/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.infrastructure.service;

import com.facebook.thrift.*;
import com.facebook.thrift.server.*;
import com.facebook.thrift.server.TThreadPoolServer.Options;
import com.facebook.thrift.transport.*;
import com.facebook.thrift.protocol.*;
import com.facebook.fb303.FacebookBase;
import com.facebook.fb303.fb_status;
import java.io.*;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import com.facebook.infrastructure.conf.DatabaseDescriptor;
import com.facebook.infrastructure.db.*;
import com.facebook.infrastructure.net.*;
import com.facebook.infrastructure.utils.*;
import org.apache.log4j.Logger;
/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class CassandraImpl extends FacebookBase implements Cassandra.Iface
{
	private static Logger logger_ = Logger.getLogger(CassandraImpl.class);
	/*
	 * Handle to the storage service to interact with the other machines in the
	 * cluster.
	 */
	StorageService storageService_;

	protected CassandraImpl(String name)
	{
		super(name);
	}
	public CassandraImpl() throws Throwable
	{
		super("Cassandra");
		// Create the instance of the storage service
		storageService_ = StorageService.instance();
	}

	/*
	 * The start function initializes the server and starts listening on the
	 * specified port
	 */
	public void start() throws Throwable
	{
		LogUtil.init();
		//LogUtil.setLogLevel("com.facebook", "DEBUG");
		// Start the storage service
		storageService_.start();
	}

	private Map<EndPoint, Message> createWriteMessages(RowMutationMessage rmMessage, Map<EndPoint, EndPoint> endpointMap) throws IOException
	{
		Map<EndPoint, Message> messageMap = new HashMap<EndPoint, Message>();
		Message message = RowMutationMessage.makeRowMutationMessage(rmMessage);

		Set<EndPoint> targets = endpointMap.keySet();
		for( EndPoint target : targets )
		{
			EndPoint hint = endpointMap.get(target);
			if ( !target.equals(hint) )
			{
				Message hintedMessage = RowMutationMessage.makeRowMutationMessage(rmMessage);
				hintedMessage.addHeader(RowMutationMessage.hint_, EndPoint.toBytes(hint) );
				logger_.debug("Sending the hint of " + target.getHost() + " to " + hint.getHost());
				messageMap.put(target, hintedMessage);
			}
			else
			{
				messageMap.put(target, message);
			}
		}
		return messageMap;
	}

	protected void insert(RowMutation rm)
	{
		// 1. Get the N nodes from storage service where the data needs to be
		// replicated
		// 2. Construct a message for read\write
		// 3. SendRR ( to all the nodes above )
		// 4. Wait for a response from atleast X nodes where X <= N
		// 5. return success

		try
		{
			logger_.debug(" insert");
			Map<EndPoint, EndPoint> endpointMap = storageService_.getNStorageEndPointMap(rm.key());
			// TODO: throw a thrift exception if we do not have N nodes
			RowMutationMessage rmMsg = new RowMutationMessage(rm);
			/* Create the write messages to be sent */
			Map<EndPoint, Message> messageMap = createWriteMessages(rmMsg, endpointMap);
			Set<EndPoint> endpoints = messageMap.keySet();
			for(EndPoint endpoint : endpoints)
			{
				MessagingService.getMessagingInstance().sendOneWay(messageMap.get(endpoint), endpoint);
			}
		}
		catch (Exception e)
		{
			logger_.info( LogUtil.throwableToString(e) );
		}
		return;
	}

	protected Row readProtocol(String tablename, String key, String columnFamily, List<String> columnNames, StorageService.ConsistencyLevel consistencyLevel) throws Exception
	{
		Row row = null;
		boolean foundLocal = false;
		EndPoint[] endpoints = storageService_.getNStorageEndPoint(key);
		for(EndPoint endPoint: endpoints)
		{
			if(endPoint.equals(StorageService.getLocalStorageEndPoint()))
			{
				foundLocal = true;
				break;
			}
		}
		if(!foundLocal)
		{
			row = strongReadProtocol(tablename, key, columnFamily, columnNames);
			return row;
		}
		else
		{
			switch ( consistencyLevel )
			{
			case WEAK:
				row = weakReadProtocol(tablename, key, columnFamily, columnNames);
				break;

			case STRONG:
				row = strongReadProtocol(tablename, key, columnFamily, columnNames);
				break;

			default:
				row = weakReadProtocol(tablename, key, columnFamily, columnNames);
				break;
			}
		}
		return row;


	}
	protected Row readProtocol(String tablename, String key, String columnFamily, int start, int count, StorageService.ConsistencyLevel consistencyLevel) throws Exception
	{
		Row row = null;
		boolean foundLocal = false;
		EndPoint[] endpoints = storageService_.getNStorageEndPoint(key);
		for(EndPoint endPoint: endpoints)
		{
			if(endPoint.equals(StorageService.getLocalStorageEndPoint()))
			{
				foundLocal = true;
				break;
			}
		}
		if(!foundLocal)
		{
			row = strongReadProtocol(tablename, key, columnFamily, start, count);
			return row;
		}
		else
		{
			switch ( consistencyLevel )
			{
			case WEAK:
				row = weakReadProtocol(tablename, key, columnFamily, start, count);
				break;

			case STRONG:
				row = strongReadProtocol(tablename, key, columnFamily, start, count);
				break;

			default:
				row = weakReadProtocol(tablename, key, columnFamily, start, count);
				break;
			}
		}
		return row;
	}

	protected Row strongReadProtocol(String tablename, String key, String columnFamily, List<String> columns) throws Exception
	{
        long startTime = System.currentTimeMillis();
		// TODO: throw a thrift exception if we do not have N nodes
		ReadMessage readMessage = new ReadMessage(tablename, key, columnFamily, columns);

        ReadMessage readMessageDigestOnly = new ReadMessage(tablename, key, columnFamily, columns);
		readMessageDigestOnly.setIsDigestQuery(true);

        Row row = doStrongReadProtocol(key, readMessage, readMessageDigestOnly);
        logger_.info("readProtocol: " + (System.currentTimeMillis() - startTime) + " ms.");
		return row;
	}

	/*
	 * This function executes the read protocol.
		// 1. Get the N nodes from storage service where the data needs to be
		// replicated
		// 2. Construct a message for read\write
		 * 3. Set one of the messages to get the data and the rest to get the digest
		// 4. SendRR ( to all the nodes above )
		// 5. Wait for a response from at least X nodes where X <= N and the data node
		 * 6. If the digest matches return the data.
		 * 7. else carry out read repair by getting data from all the nodes.
		// 5. return success
	 *
	 */
	protected Row strongReadProtocol(String tablename, String key, String columnFamily, int start, int count) throws IOException, TimeoutException
	{
        long startTime = System.currentTimeMillis();
		// TODO: throw a thrift exception if we do not have N nodes
		ReadMessage readMessage = null;
		ReadMessage readMessageDigestOnly = null;
		if( start >= 0 && count < Integer.MAX_VALUE)
		{
			readMessage = new ReadMessage(tablename, key, columnFamily, start, count);
		}
		else
		{
			readMessage = new ReadMessage(tablename, key, columnFamily);
		}
        Message message = ReadMessage.makeReadMessage(readMessage);
		if( start >= 0 && count < Integer.MAX_VALUE)
		{
			readMessageDigestOnly = new ReadMessage(tablename, key, columnFamily, start, count);
		}
		else
		{
			readMessageDigestOnly = new ReadMessage(tablename, key, columnFamily);
		}
		readMessageDigestOnly.setIsDigestQuery(true);
        Row row = doStrongReadProtocol(key, readMessage, readMessageDigestOnly);
        logger_.info("readProtocol: " + (System.currentTimeMillis() - startTime) + " ms.");
        return row;
	}

	/*
	 * This method performs the actual read from the replicas.
	 *  param @ key - key for which the data is required.
	 *  param @ readMessage - the read message to get the actual data
	 *  param @ readMessageDigest - the read message to get the digest.
	*/
	private Row doStrongReadProtocol(String key, ReadMessage readMessage, ReadMessage readMessageDigest) throws IOException, TimeoutException
	{
		Row row = null;
		Message message = ReadMessage.makeReadMessage(readMessage);
		Message messageDigestOnly = ReadMessage.makeReadMessage(readMessageDigest);

		IResponseResolver<Row> readResponseResolver = new ReadResponseResolver();
		QuorumResponseHandler<Row> quorumResponseHandler = new QuorumResponseHandler<Row>(
				DatabaseDescriptor.getReplicationFactor(),
				readResponseResolver);
		EndPoint dataPoint = storageService_.findSuitableEndPoint(key);
		List<EndPoint> endpointList = new ArrayList<EndPoint>( Arrays.asList( storageService_.getNStorageEndPoint(key) ) );
		/* Remove the local storage endpoint from the list. */
		endpointList.remove( dataPoint );
		EndPoint[] endPoints = new EndPoint[endpointList.size() + 1];
		Message messages[] = new Message[endpointList.size() + 1];

		// first message is the data Point
		endPoints[0] = dataPoint;
		messages[0] = message;

		for(int i=1; i < endPoints.length ; i++)
		{
			endPoints[i] = endpointList.get(i-1);
			messages[i] = messageDigestOnly;
		}

		try
		{
			MessagingService.getMessagingInstance().sendRR(messages, endPoints,	quorumResponseHandler);

	        long startTime2 = System.currentTimeMillis();
			row = quorumResponseHandler.get();
	        logger_.info("quorumResponseHandler: " + (System.currentTimeMillis() - startTime2)
	                + " ms.");
			if (row == null)
			{
				logger_.info("ERROR No row for this key .....: " + key);
				// TODO: throw a thrift exception
				return row;
			}
		}
		catch (DigestMismatchException ex)
		{
			IResponseResolver<Row> readResponseResolverRepair = new ReadResponseResolver();
			QuorumResponseHandler<Row> quorumResponseHandlerRepair = new QuorumResponseHandler<Row>(
					DatabaseDescriptor.getReplicationFactor(),
					readResponseResolverRepair);
			readMessage.setIsDigestQuery(false);
			logger_.info("DigestMismatchException: " + key);
            Message messageRepair = ReadMessage.makeReadMessage(readMessage);
			MessagingService.getMessagingInstance().sendRR(messageRepair, endPoints,
					quorumResponseHandlerRepair);
			try
			{
				row = quorumResponseHandlerRepair.get();
			}
			catch(DigestMismatchException dex)
			{
				logger_.warn(LogUtil.throwableToString(dex));
			}
			if (row == null)
			{
				logger_.info("ERROR No row for this key .....: " + key);
			}
		}
		return row;
	}

	protected Row weakReadProtocol(String tablename, String key, String columnFamily, List<String> columns) throws Exception
	{
		long startTime = System.currentTimeMillis();
		List<EndPoint> endpoints = storageService_.getNLiveStorageEndPoint(key);
		/* Remove the local storage endpoint from the list. */
		endpoints.remove( StorageService.getLocalStorageEndPoint() );
		// TODO: throw a thrift exception if we do not have N nodes

		Table table = Table.open( DatabaseDescriptor.getTables().get(0) );
		Row row = table.getRow(key, columnFamily, columns);

		logger_.info("Local Read Protocol: " + (System.currentTimeMillis() - startTime) + " ms.");
		/*
		 * Do the consistency checks in the background and return the
		 * non NULL row.
		 */
		if ( endpoints.size() > 0 )
			StorageService.instance().doConsistencyCheck(row, endpoints, columnFamily, columns);
		return row;
	}

	/*
	 * This function executes the read protocol locally and should be used only if consistency is not a concern.
	 * Read the data from the local disk and return if the row is NOT NULL. If the data is NULL do the read from
     * one of the other replicas (in the same data center if possible) till we get the data. In the event we get
     * the data we perform consistency checks and figure out if any repairs need to be done to the replicas.
	 */
	protected Row weakReadProtocol(String tablename, String key, String columnFamily, int start, int count) throws Exception
	{
		Row row = null;
		long startTime = System.currentTimeMillis();
		List<EndPoint> endpoints = storageService_.getNLiveStorageEndPoint(key);
		/* Remove the local storage endpoint from the list. */
		endpoints.remove( StorageService.getLocalStorageEndPoint() );
		// TODO: throw a thrift exception if we do not have N nodes

		Table table = Table.open( DatabaseDescriptor.getTables().get(0) );
		if( start >= 0 && count < Integer.MAX_VALUE)
		{
			row = table.getRow(key, columnFamily, start, count);
		}
		else
		{
			row = table.getRow(key, columnFamily);
		}

		logger_.info("Local Read Protocol: " + (System.currentTimeMillis() - startTime) + " ms.");
		/*
		 * Do the consistency checks in the background and return the
		 * non NULL row.
		 */
		StorageService.instance().doConsistencyCheck(row, endpoints, columnFamily, start, count);
		return row;
	}

    protected ColumnFamily get_cf(String tablename, String key, String columnFamily, List<String> columNames) throws TException
	{
    	ColumnFamily cfamily = null;
		try
		{
	        String[] values = RowMutation.getColumnAndColumnFamily(columnFamily);
	        // check for  values
	        if( values.length < 1 )
	        	return cfamily;
	        Row row = readProtocol(tablename, key, columnFamily, columNames, StorageService.ConsistencyLevel.WEAK);
	        if (row == null)
			{
				logger_.info("ERROR No row for this key .....: " + key);
				// TODO: throw a thrift exception
				return cfamily;
			}


			Map<String, ColumnFamily> cfMap = row.getColumnFamilies();
			if (cfMap == null || cfMap.size() == 0)
			{
				logger_	.info("ERROR ColumnFamily " + columnFamily + " map is missing.....: "
							   + "   key:" + key
								);
				// TODO: throw a thrift exception
				return cfamily;
			}
			cfamily = cfMap.get(values[0]);
			if (cfamily == null)
			{
				logger_.info("ERROR ColumnFamily " + columnFamily + " is missing.....: "
							+"   key:" + key
							+ "  ColumnFamily:" + values[0]);
				return cfamily;
			}
		}
		catch (Exception e)
		{
			logger_.info( LogUtil.throwableToString(e) );
		}
		return cfamily;
	}

    public ArrayList<column_t> get_slice(String tablename, String key, String columnFamily_column, int start, int count) throws TException
	{
		ArrayList<column_t> retlist = new ArrayList<column_t>();
        long startTime = System.currentTimeMillis();

		try
		{
	        String[] values = RowMutation.getColumnAndColumnFamily(columnFamily_column);
	        // check for  values
	        if( values.length < 1 )
	        	return retlist;

	        Row row = readProtocol(tablename, key, columnFamily_column, start, count, StorageService.ConsistencyLevel.WEAK);
			if (row == null)
			{
				logger_.info("ERROR No row for this key .....: " + key);
				// TODO: throw a thrift exception
				return retlist;
			}

			Map<String, ColumnFamily> cfMap = row.getColumnFamilies();
			if (cfMap == null || cfMap.size() == 0)
			{
				logger_	.info("ERROR ColumnFamily " + columnFamily_column + " map is missing.....: "
							   + "   key:" + key
								);
				// TODO: throw a thrift exception
				return retlist;
			}
			ColumnFamily cfamily = cfMap.get(values[0]);
			if (cfamily == null)
			{
				logger_.info("ERROR ColumnFamily " + columnFamily_column + " is missing.....: "
							+"   key:" + key
							+ "  ColumnFamily:" + values[0]);
				return retlist;
			}
			Collection<IColumn> columns = null;
			if( values.length > 1 )
			{
				// this is the super column case
				IColumn column = cfamily.getColumn(values[1]);
				if(column != null)
					columns = column.getSubColumns();
			}
			else
			{
				columns = cfamily.getAllColumns();
			}
			if (columns == null || columns.size() == 0)
			{
				logger_	.info("ERROR Columns are missing.....: "
							   + "   key:" + key
								+ "  ColumnFamily:" + values[0]);
				// TODO: throw a thrift exception
				return retlist;
			}

			for(IColumn column : columns)
			{
				column_t thrift_column = new column_t();
				thrift_column.columnName = column.name();
				thrift_column.value = new String(column.value()); // This needs to be Utf8ed
				thrift_column.timestamp = column.timestamp();
				retlist.add(thrift_column);
			}
		}
		catch (Exception e)
		{
			logger_.info( LogUtil.throwableToString(e) );
		}

        logger_.info("get_slice2: " + (System.currentTimeMillis() - startTime)
                + " ms.");

		return retlist;
	}

    public column_t get_column(String tablename, String key, String columnFamily_column) throws TException
    {
		column_t ret = null;
		try
		{
	        String[] values = RowMutation.getColumnAndColumnFamily(columnFamily_column);
	        // check for  values
	        if( values.length < 2 )
	        	return ret;
	        Row row = readProtocol(tablename, key, columnFamily_column, -1, Integer.MAX_VALUE, StorageService.ConsistencyLevel.WEAK);
			if (row == null)
			{
				logger_.info("ERROR No row for this key .....: " + key);
				// TODO: throw a thrift exception
				return ret;
			}

			Map<String, ColumnFamily> cfMap = row.getColumnFamilies();
			if (cfMap == null || cfMap.size() == 0)
			{
				logger_	.info("ERROR ColumnFamily map is missing.....: "
							   + "   key:" + key
								);
				// TODO: throw a thrift exception
				return ret;
			}
			ColumnFamily cfamily = cfMap.get(values[0]);
			if (cfamily == null)
			{
				logger_.info("ERROR ColumnFamily  is missing.....: "
							+"   key:" + key
							+ "  ColumnFamily:" + values[0]);
				return ret;
			}
			Collection<IColumn> columns = null;
			if( values.length > 2 )
			{
				// this is the super column case
				IColumn column = cfamily.getColumn(values[1]);
				if(column != null)
					columns = column.getSubColumns();
			}
			else
			{
				columns = cfamily.getAllColumns();
			}
			if (columns == null || columns.size() == 0)
			{
				logger_	.info("ERROR Columns are missing.....: "
							   + "   key:" + key
								+ "  ColumnFamily:" + values[0]);
				// TODO: throw a thrift exception
				return ret;
			}
			ret = new column_t();
			for(IColumn column : columns)
			{
				ret.columnName = column.name();
				ret.value = new String(column.value());
				ret.timestamp = column.timestamp();
			}
		}
		catch (Exception e)
		{
			logger_.info( LogUtil.throwableToString(e) );
		}
		return ret;

    }


    public int get_column_count(String tablename, String key, String columnFamily_column)
	{
    	int count = -1;
		try
		{
	        String[] values = RowMutation.getColumnAndColumnFamily(columnFamily_column);
	        // check for  values
	        if( values.length < 1 )
	        	return -1;
	        Row row = readProtocol(tablename, key, columnFamily_column, -1, Integer.MAX_VALUE, StorageService.ConsistencyLevel.WEAK);
			if (row == null)
			{
				logger_.info("ERROR No row for this key .....: " + key);
				// TODO: throw a thrift exception
				return count;
			}

			Map<String, ColumnFamily> cfMap = row.getColumnFamilies();
			if (cfMap == null || cfMap.size() == 0)
			{
				logger_	.info("ERROR ColumnFamily map is missing.....: "
							   + "   key:" + key
								);
				// TODO: throw a thrift exception
				return count;
			}
			ColumnFamily cfamily = cfMap.get(values[0]);
			if (cfamily == null)
			{
				logger_.info("ERROR ColumnFamily  is missing.....: "
							+"   key:" + key
							+ "  ColumnFamily:" + values[0]);
				return count;
			}
			Collection<IColumn> columns = null;
			if( values.length > 1 )
			{
				// this is the super column case
				IColumn column = cfamily.getColumn(values[1]);
				if(column != null)
					columns = column.getSubColumns();
			}
			else
			{
				columns = cfamily.getAllColumns();
			}
			if (columns == null || columns.size() == 0)
			{
				logger_	.info("ERROR Columns are missing.....: "
							   + "   key:" + key
								+ "  ColumnFamily:" + values[0]);
				// TODO: throw a thrift exception
				return count;
			}
			count = columns.size();
		}
		catch (Exception e)
		{
			logger_.info( LogUtil.throwableToString(e) );
		}
		return count;

	}

    public void insert(String tablename, String key, String columnFamily_column, String cellData, int timestamp)
	{

		try
		{
			RowMutation rm = new RowMutation(tablename, key.trim());
			rm.add(columnFamily_column, cellData.getBytes(), timestamp);
			insert(rm);
		}
		catch (Exception e)
		{
			logger_.debug( LogUtil.throwableToString(e) );
		}
		return;
	}
    public boolean batch_insert_blocking(batch_mutation_t batchMutation)
    {
		// 1. Get the N nodes from storage service where the data needs to be
		// replicated
		// 2. Construct a message for read\write
		// 3. SendRR ( to all the nodes above )
		// 4. Wait for a response from at least X nodes where X <= N
		// 5. return success
    	boolean result = false;
		try
		{
			logger_.warn(" batch_insert_blocking");
			IResponseResolver<Boolean> writeResponseResolver = new WriteResponseResolver();
			QuorumResponseHandler<Boolean> quorumResponseHandler = new QuorumResponseHandler<Boolean>(
					DatabaseDescriptor.getReplicationFactor(),
					writeResponseResolver);
			EndPoint[] endpoints = storageService_.getNStorageEndPoint(batchMutation.key);
			// TODO: throw a thrift exception if we do not have N nodes

			logger_.debug(" Creating the row mutation");
			RowMutation rm = new RowMutation(batchMutation.table,
					batchMutation.key.trim());
			Set<String> keys = batchMutation.cfmap.keySet();
			Iterator<String> keyIter = keys.iterator();
			while (keyIter.hasNext())
			{
				Object key = keyIter.next(); // Get the next key.
				List<column_t> list = batchMutation.cfmap.get(key);
				for (column_t columnData : list)
				{
					rm.add(key.toString() + ":" + columnData.columnName,
							columnData.value.getBytes(), columnData.timestamp);

				}
			}

			RowMutationMessage rmMsg = new RowMutationMessage(rm);
			Message message = new Message(StorageService.getLocalStorageEndPoint(),
                    StorageService.mutationStage_,
					StorageService.mutationVerbHandler_,
                    new Object[]{ rmMsg }
            );
			MessagingService.getMessagingInstance().sendRR(message, endpoints,
					quorumResponseHandler);
			logger_.debug(" Calling quorum response handler's get");
			result = quorumResponseHandler.get();

			// TODO: if the result is false that means the writes to all the
			// servers failed hence we need to throw an exception or return an
			// error back to the client so that it can take appropriate action.
		}
		catch (Exception e)
		{
			logger_.info( LogUtil.throwableToString(e) );
		}
		return result;

    }
	public void batch_insert(batch_mutation_t batchMutation)
	{
		// 1. Get the N nodes from storage service where the data needs to be
		// replicated
		// 2. Construct a message for read\write
		// 3. SendRR ( to all the nodes above )
		// 4. Wait for a response from atleast X nodes where X <= N
		// 5. return success

		try
		{
			logger_.debug(" batch_insert");
			logger_.debug(" Creating the row mutation");
			RowMutation rm = new RowMutation(batchMutation.table,
					batchMutation.key.trim());
			Set<String> keys = batchMutation.cfmap.keySet();
			Iterator<String> keyIter = keys.iterator();
			while (keyIter.hasNext())
			{
				Object key = keyIter.next(); // Get the next key.
				List<column_t> list = batchMutation.cfmap.get(key);
				for (column_t columnData : list)
				{
					rm.add(key.toString() + ":" + columnData.columnName,
							columnData.value.getBytes(), columnData.timestamp);

				}
			}
			insert(rm);
		}
		catch (Exception e)
		{
			logger_.info( LogUtil.throwableToString(e) );
		}
		return;
	}

    public void remove(String tablename, String key, String columnFamily_column)
	{
		try
		{
			RowMutation rm = new RowMutation(tablename, key.trim());
			rm.delete(columnFamily_column);
			insert(rm);
		}
		catch (Exception e)
		{
			logger_.debug( LogUtil.throwableToString(e) );
		}
		return;
	}

    public ArrayList<superColumn_t> get_slice_super(String tablename, String key, String columnFamily_superColumnName, int start, int count)
    {
		ArrayList<superColumn_t> retlist = new ArrayList<superColumn_t>();
		try
		{
	        String[] values = RowMutation.getColumnAndColumnFamily(columnFamily_superColumnName);
	        // check for  values
	        if( values.length < 1 )
	        	return retlist;
	        Row row = readProtocol(tablename, key, columnFamily_superColumnName, start, count, StorageService.ConsistencyLevel.WEAK);
			if (row == null)
			{
				logger_.info("ERROR No row for this key .....: " + key);
				// TODO: throw a thrift exception
				return retlist;
			}

			Map<String, ColumnFamily> cfMap = row.getColumnFamilies();
			if (cfMap == null || cfMap.size() == 0)
			{
				logger_	.info("ERROR ColumnFamily map is missing.....: "
							   + "   key:" + key
								);
				// TODO: throw a thrift exception
				return retlist;
			}
			ColumnFamily cfamily = cfMap.get(values[0]);
			if (cfamily == null)
			{
				logger_.info("ERROR ColumnFamily  is missing.....: "
							+"   key:" + key
							+ "  ColumnFamily:" + values[0]);
				return retlist;
			}
			Collection<IColumn> columns = cfamily.getAllColumns();
			if (columns == null || columns.size() == 0)
			{
				logger_	.info("ERROR Columns are missing.....: "
							   + "   key:" + key
								+ "  ColumnFamily:" + values[0]);
				// TODO: throw a thrift exception
				return retlist;
			}

			for(IColumn column : columns)
			{
				superColumn_t thrift_superColumn = new superColumn_t();
				thrift_superColumn.name = column.name();
				Collection<IColumn> subColumns = column.getSubColumns();
				if(subColumns.size() != 0 )
				{
					thrift_superColumn.columns = new ArrayList<column_t>();
					for( IColumn subColumn : subColumns )
					{
						column_t thrift_column = new column_t();
						thrift_column.columnName = subColumn.name();
						thrift_column.value = new String(subColumn.value());
						thrift_column.timestamp = subColumn.timestamp();
						thrift_superColumn.columns.add(thrift_column);
					}
				}
				retlist.add(thrift_superColumn);
			}
		}
		catch (Exception e)
		{
			logger_.info( LogUtil.throwableToString(e) );
		}
		return retlist;

    }

    public superColumn_t get_superColumn(String tablename, String key, String columnFamily_column)
    {
    	superColumn_t ret = null;
		try
		{
	        String[] values = RowMutation.getColumnAndColumnFamily(columnFamily_column);
	        // check for  values
	        if( values.length < 2 )
	        	return ret;

	        Row row = readProtocol(tablename, key, columnFamily_column, -1, Integer.MAX_VALUE, StorageService.ConsistencyLevel.WEAK);
			if (row == null)
			{
				logger_.info("ERROR No row for this key .....: " + key);
				// TODO: throw a thrift exception
				return ret;
			}

			Map<String, ColumnFamily> cfMap = row.getColumnFamilies();
			if (cfMap == null || cfMap.size() == 0)
			{
				logger_	.info("ERROR ColumnFamily map is missing.....: "
							   + "   key:" + key
								);
				// TODO: throw a thrift exception
				return ret;
			}
			ColumnFamily cfamily = cfMap.get(values[0]);
			if (cfamily == null)
			{
				logger_.info("ERROR ColumnFamily  is missing.....: "
							+"   key:" + key
							+ "  ColumnFamily:" + values[0]);
				return ret;
			}
			Collection<IColumn> columns = cfamily.getAllColumns();
			if (columns == null || columns.size() == 0)
			{
				logger_	.info("ERROR Columns are missing.....: "
							   + "   key:" + key
								+ "  ColumnFamily:" + values[0]);
				// TODO: throw a thrift exception
				return ret;
			}

			for(IColumn column : columns)
			{
				ret = new superColumn_t();
				ret.name = column.name();
				Collection<IColumn> subColumns = column.getSubColumns();
				if(subColumns.size() != 0 )
				{
					ret.columns = new ArrayList<column_t>();
					for(IColumn subColumn : subColumns)
					{
						column_t thrift_column = new column_t();
						thrift_column.columnName = subColumn.name();
						thrift_column.value = new String(subColumn.value());
						thrift_column.timestamp = subColumn.timestamp();
						ret.columns.add(thrift_column);
					}
				}
			}
		}
		catch (Exception e)
		{
			logger_.info( LogUtil.throwableToString(e) );
		}
		return ret;

    }

    public boolean batch_insert_superColumn_blocking(batch_mutation_super_t batchMutationSuper)
    {
    	boolean result = false;
		try
		{
			logger_.warn(" batch_insert_SuperColumn_blocking");
			logger_.debug(" Creating the row mutation");
			RowMutation rm = new RowMutation(batchMutationSuper.table,
					batchMutationSuper.key.trim());
			Set<String> keys = batchMutationSuper.cfmap.keySet();
			Iterator<String> keyIter = keys.iterator();
			while (keyIter.hasNext())
			{
				Object key = keyIter.next(); // Get the next key.
				List<superColumn_t> list = batchMutationSuper.cfmap.get(key);
				for (superColumn_t superColumnData : list)
				{
					if(superColumnData.columns.size() != 0 )
					{
						for (column_t columnData : superColumnData.columns)
						{
							rm.add(key.toString() + ":" + superColumnData.name  +":" + columnData.columnName,
									columnData.value.getBytes(), columnData.timestamp);
						}
					}
					else
					{
						rm.add(key.toString() + ":" + superColumnData.name, new byte[0], 0);
					}
				}
			}
			insert(rm);
		}
		catch (Exception e)
		{
			logger_.info( LogUtil.throwableToString(e) );
		}
		return result;

    }
    public void batch_insert_superColumn(batch_mutation_super_t batchMutationSuper)
    {
		try
		{
			logger_.debug(" batch_insert");
			logger_.debug(" Creating the row mutation");
			RowMutation rm = new RowMutation(batchMutationSuper.table,
					batchMutationSuper.key.trim());
			Set<String> keys = batchMutationSuper.cfmap.keySet();
			Iterator<String> keyIter = keys.iterator();
			while (keyIter.hasNext())
			{
				Object key = keyIter.next(); // Get the next key.
				List<superColumn_t> list = batchMutationSuper.cfmap.get(key);
				for (superColumn_t superColumnData : list)
				{
					if(superColumnData.columns.size() != 0 )
					{
						for (column_t columnData : superColumnData.columns)
						{
							rm.add(key.toString() + ":" + superColumnData.name  +":" + columnData.columnName,
									columnData.value.getBytes(), columnData.timestamp);
						}
					}
					else
					{
						rm.add(key.toString() + ":" + superColumnData.name, new byte[0], 0);
					}
				}
			}
			insert(rm);
		}
		catch (Exception e)
		{
			logger_.info( LogUtil.throwableToString(e) );
		}
		return;
    }


	public String getVersion()
	{
		return "1";
	}

	public int getStatus()
	{
		return fb_status.ALIVE;
	}

	public String getStatusDetails()
	{
		return null;
	}

	public static void main(String[] args) throws Throwable
	{
		try
		{
			CassandraImpl cassandraServer = new CassandraImpl();
			cassandraServer.start();
			Cassandra.Processor processor = new Cassandra.Processor(cassandraServer);
			// Transport
                        int port = DatabaseDescriptor.getThriftPort();
			TServerSocket tServerSocket =  new TServerSocket(port);
			 // Protocol factory
			TProtocolFactory tProtocolFactory = new TBinaryProtocol.Factory();
			 // ThreadPool Server
			Options options = new Options();
			options.minWorkerThreads = 64;
			TThreadPoolServer serverEngine = new TThreadPoolServer(processor, tServerSocket, tProtocolFactory);
			serverEngine.serve();

		}
		catch (Exception x)
		{
			System.err.println("UNCAUGHT EXCEPTION IN main()");
			x.printStackTrace();
			System.exit(1);
		}

	}

}


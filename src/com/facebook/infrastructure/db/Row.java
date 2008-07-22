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

package com.facebook.infrastructure.db;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.log4j.Logger;
import com.facebook.infrastructure.utils.CassandraUtilities;
import com.facebook.infrastructure.io.*;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class Row implements Serializable
{
    private static ICompactSerializer<Row> serializer_;
	private static Logger logger_ = Logger.getLogger(Row.class);

    static
    {
        serializer_ = new RowSerializer();
    }

    static ICompactSerializer<Row> serializer()
    {
        return serializer_;
    }

    private String key_;
    private Map<String, ColumnFamily> columnFamilies_ = new Hashtable<String, ColumnFamily>();

    private transient AtomicInteger size_ = new AtomicInteger(0);

    /* Ctor for JAXB */
    protected Row()
    {
    }

    public Row(String key)
    {
        key_ = key;
    }

    public String key()
    {
        return key_;
    }

    void key(String key)
    {
        key_ = key;
    }

    public Map<String, ColumnFamily> getColumnFamilies()
    {
        return columnFamilies_;
    }

    void addColumnFamily(ColumnFamily columnFamily)
    {
        columnFamilies_.put(columnFamily.name(), columnFamily);
        size_.addAndGet(columnFamily.size());
    }

    void removeColumnFamily(ColumnFamily columnFamily)
    {
        columnFamilies_.remove(columnFamily.name());
        int delta = (-1) * columnFamily.size();
        size_.addAndGet(delta);
    }

    int size()
    {
        return size_.get();
    }

    public boolean isEmpty()
    {
    	return ( columnFamilies_.isEmpty() );
    }

    /*
     * This is used as oldRow.merge(newRow). Basically we take the newRow
     * and merge it into the oldRow.
    */
    void merge(Row row)
    {
        Map<String, ColumnFamily> columnFamilies = row.getColumnFamilies();

        for ( Map.Entry<String, ColumnFamily> entry : columnFamilies.entrySet() )
        {
            String cfName = entry.getKey();
            ColumnFamily cf = columnFamilies_.get(cfName);
            if ( cf == null )
                columnFamilies_.put(cfName, entry.getValue());
            else
            {
                cf.merge(entry.getValue());
            }
        }
    }

    /*
     * This function will repair the current row with the input row
     * what that means is that if there are any differences between the 2 rows then
     * this fn will make the current row take the latest changes .
     */
    public void repair(Row row)
    {
        Map<String, ColumnFamily> columnFamilies = row.getColumnFamilies();

        
        for ( Map.Entry<String, ColumnFamily> entry : columnFamilies.entrySet() )
        {
            String cfName = entry.getKey();
            ColumnFamily cf = columnFamilies_.get(cfName);
            if ( cf == null )
            {
            	cf = new ColumnFamily(cfName);
                columnFamilies_.put(cfName, cf);
            }
            cf.repair(entry.getValue());
        }

    }


    /*
     * This function will calculate the difference between 2 rows
     * and return the resultant row. This assumes that the row that
     * is being submitted is a super set of the current row so
     * it only calculates additional
     * difference and does not take care of what needs to be delted from the current row to make
     * it same as the input row.
     */
    public Row diff(Row row)
    {
        Row rowDiff = new Row(key_);
    	Map<String, ColumnFamily> columnFamilies = row.getColumnFamilies();

        for ( Map.Entry<String, ColumnFamily> entry : columnFamilies.entrySet() )
        {
            String cfName = entry.getKey();
            ColumnFamily cf = columnFamilies_.get(cfName);
            ColumnFamily cfDiff = null;
            if ( cf == null )
            	rowDiff.getColumnFamilies().put(cfName, entry.getValue());
            else
            {
            	cfDiff = cf.diff(entry.getValue());
            	if(cfDiff != null)
            		rowDiff.getColumnFamilies().put(cfName, cfDiff);
            }
        }
        if(!rowDiff.getColumnFamilies().isEmpty())
        	return rowDiff;
        else
        	return null;
    }

    public Row cloneMe()
    {
    	Row row = new Row(key_);
    	row.columnFamilies_ = new HashMap<String, ColumnFamily>(columnFamilies_);
    	return row;
    }

    public byte[] digest()
    {
        long start = System.currentTimeMillis();
    	Set<String> cfamilies = columnFamilies_.keySet();
    	byte[] xorHash = new byte[0];
    	byte[] tmpHash = new byte[0];

        for (Map.Entry<String, ColumnFamily> entry : columnFamilies_.entrySet() )
    	{
            String cFamily = entry.getKey();

    		if(xorHash.length == 0)
    		{
    			xorHash = entry.getValue().digest();
    		}
    		else
    		{
    			tmpHash = entry.getValue().digest();
    			xorHash = CassandraUtilities.xor(xorHash, tmpHash);
    		}
    	}
        logger_.info("DIGEST TIME: " + (System.currentTimeMillis() - start)
                + " ms.");
    	return xorHash;
    }

    void clear()
    {
        columnFamilies_.clear();
    }
}

class RowSerializer implements ICompactSerializer<Row>
{
    public void serialize(Row row, DataOutputStream dos) throws IOException
    {
        dos.writeUTF(row.key());
        Map<String, ColumnFamily> columnFamilies = row.getColumnFamilies();
        int size = columnFamilies.size();
        dos.writeInt(size);

        if ( size > 0 )
        {
            for ( Map.Entry<String, ColumnFamily> entry : columnFamilies.entrySet() )
            {
                ColumnFamily.serializer().serialize(entry.getValue(), dos);
            }
        }
    }

    public Row deserialize(DataInputStream dis) throws IOException
    {
        String key = dis.readUTF();
        Row row = new Row(key);
        int size = dis.readInt();

        if ( size > 0 )
        {
            for ( int i = 0; i < size; ++i )
            {
                ColumnFamily cf = ColumnFamily.serializer().deserialize(dis);
                row.addColumnFamily(cf);
            }
        }
        return row;
    }
}

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

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.io.*;
import java.lang.management.ManagementFactory;
import java.math.BigInteger;
import java.net.UnknownHostException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.commons.math.linear.RealMatrix;
import org.apache.commons.math.linear.RealMatrixImpl;
import org.apache.log4j.Logger;
import com.facebook.infrastructure.analytics.AnalyticsContext;
import com.facebook.infrastructure.concurrent.*;
import com.facebook.infrastructure.conf.DatabaseDescriptor;
import com.facebook.infrastructure.db.*;
import com.facebook.infrastructure.io.DataInputBuffer;
import com.facebook.infrastructure.io.ICompactSerializer;
import com.facebook.infrastructure.dht.Range;
import com.facebook.infrastructure.net.http.HttpConnection;
import com.facebook.infrastructure.net.io.*;
import com.facebook.infrastructure.net.CompactEndPointSerializationHelper;
import com.facebook.infrastructure.net.EndPoint;
import com.facebook.infrastructure.net.IVerbHandler;
import com.facebook.infrastructure.net.Message;
import com.facebook.infrastructure.net.MessagingService;
import com.facebook.infrastructure.gms.*;
import com.facebook.infrastructure.tools.MembershipCleanerVerbHandler;
import com.facebook.infrastructure.tools.TokenUpdateVerbHandler;
import com.facebook.infrastructure.utils.*;
import com.yahoo.zookeeper.KeeperException;
import com.yahoo.zookeeper.Watcher;
import com.yahoo.zookeeper.ZooKeeper;
import com.yahoo.zookeeper.ZooDefs.Ids;
import com.yahoo.zookeeper.data.Stat;
import com.yahoo.zookeeper.proto.WatcherEvent;

/*
 * This abstraction contains the token/identifier of this node
 * on the identifier space. This token gets gossiped around.
 * This class will also maintain histograms of the load information
 * of other nodes in the cluster.
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */
public final class StorageService implements IEndPointStateChangeSubscriber, StorageServiceMBean
{
    private static Logger logger_ = Logger.getLogger(StorageService.class);
    private static final BigInteger prime_ = BigInteger.valueOf(31);
    private final static int maxKeyHashLength_ = 24;
    private final static String nodeId_ = "NODE-IDENTIFIER";
    private final static String loadAll_ = "LOAD-ALL";
    public final static String mutationStage_ = "ROW-MUTATION-STAGE";
    public final static String readStage_ = "ROW-READ-STAGE";
    public final static String mutationVerbHandler_ = "ROW-MUTATION-VERB-HANDLER";
    public final static String tokenVerbHandler_ = "TOKEN-VERB-HANDLER";
    public final static String loadVerbHandler_ = "LOAD-VERB-HANDLER";
    public final static String binaryVerbHandler_ = "BINARY-VERB-HANDLER";
    public final static String readRepairVerbHandler_ = "READ-REPAIR-VERB-HANDLER";
    public final static String readVerbHandler_ = "ROW-READ-VERB-HANDLER";
    public final static String bootStrapInitiateVerbHandler_ = "BOOTSTRAP-INITIATE-VERB-HANDLER";
    public final static String bootStrapInitiateDoneVerbHandler_ = "BOOTSTRAP-INITIATE-DONE-VERB-HANDLER";
    public final static String bootStrapTerminateVerbHandler_ = "BOOTSTRAP-TERMINATE-VERB-HANDLER";
    public final static String tokenInfoVerbHandler_ = "TOKENINFO-VERB-HANDLER";
    public final static String mbrshipCleanerVerbHandler_ = "MBRSHIP-CLEANER-VERB-HANDLER";
    public final static String bsMetadataVerbHandler_ = "BS-METADATA-VERB-HANDLER";

    public static enum ConsistencyLevel
    {
    	WEAK,
    	STRONG
    };
    
    private static StorageService instance_;
    /* Used to lock the factory for creation of StorageService instance */
    private static Lock createLock_ = new ReentrantLock();
    private static EndPoint tcpAddr_;
    private static EndPoint udpAddr_;

    public static EndPoint getLocalStorageEndPoint()
    {
        return tcpAddr_;
    }

    public static EndPoint getLocalControlEndPoint()
    {
        return udpAddr_;
    }

    public static String getHostUrl()
    {
        return "http://" + tcpAddr_.getHost() + ":" + DatabaseDescriptor.getHttpPort();
    }

    /*
     * Order preserving hash for the specified key.
    */
    public static BigInteger hash(String key)
    {
        BigInteger h = BigInteger.ZERO;
        char val[] = key.toCharArray();
        for (int i = 0; i < StorageService.maxKeyHashLength_; i++)
        {
            if( i < val.length )
                h = StorageService.prime_.multiply(h).add( BigInteger.valueOf(val[i]) );
            else
                h = StorageService.prime_.multiply(h).add( StorageService.prime_ );
        }

        return h;
    }

    /*
     * This class contains a helper method that will be used by
     * all abstractions that implement the IReplicaPlacementStrategy
     * interface.
    */
    abstract class AbstractStrategy implements IReplicaPlacementStrategy
    {
        /*
         * This method changes the ports of the endpoints from
         * the control port to the storage ports.
        */
        protected void retrofitPorts(List<EndPoint> eps)
        {
            for ( EndPoint ep : eps )
            {
                ep.setPort(DatabaseDescriptor.getStoragePort());
            }
        }

        protected EndPoint getNextAvailableEndPoint(EndPoint startPoint, List<EndPoint> topN, List<EndPoint> liveNodes)
        {
            EndPoint endPoint = null;
            Map<BigInteger, EndPoint> tokenToEndPointMap = tokenMetadata_.cloneTokenEndPointMap();
            List<BigInteger> tokens = new ArrayList<BigInteger>(tokenToEndPointMap.keySet());
            Collections.sort(tokens);
            BigInteger token = tokenMetadata_.getToken(startPoint);
            int index = Collections.binarySearch(tokens, token);
            if(index < 0)
            {
                index = (index + 1) * (-1);
                if (index >= tokens.size())
                    index = 0;
            }
            int totalNodes = tokens.size();
            int startIndex = (index+1)%totalNodes;
            for (int i = startIndex, count = 1; count < totalNodes ; ++count, i = (i+1)%totalNodes)
            {
                EndPoint tmpEndPoint = tokenToEndPointMap.get(tokens.get(i));
                if(FailureDetector.instance().isAlive(tmpEndPoint) && !topN.contains(tmpEndPoint) && !liveNodes.contains(tmpEndPoint))
                {
                    endPoint = tmpEndPoint;
                    break;
                }
            }
            return endPoint;
        }

        /*
         * This method returns the hint map. The key is the endpoint
         * on which the data is being placed and the value is the
         * endpoint which is in the top N.
         * Get the map of top N to the live nodes currently.
         */
        public Map<EndPoint, EndPoint> getHintedStorageEndPoints(BigInteger token)
        {
            List<EndPoint> liveList = new ArrayList<EndPoint>();
            Map<EndPoint, EndPoint> map = new HashMap<EndPoint, EndPoint>();
            EndPoint[] topN = getStorageEndPoints( token );

            for( int i = 0 ; i < topN.length ; i++)
            {
                if( FailureDetector.instance().isAlive(topN[i]))
                {
                    map.put(topN[i], topN[i]);
                    liveList.add(topN[i]) ;
                }
                else
                {
                    EndPoint endPoint = getNextAvailableEndPoint(topN[i], Arrays.asList(topN), liveList);
                    if(endPoint != null)
                    {
                        map.put(endPoint, topN[i]);
                        liveList.add(endPoint) ;
                    }
                    else
                    {
                        // log a warning , maybe throw an exception
                        logger_.warn("Unable to find a live Endpoint we might be out of live nodes , This is dangerous !!!!");
                    }
                }
            }
            return map;
        }

        public abstract EndPoint[] getStorageEndPoints(BigInteger token);

    }

    /*
     * This class returns the nodes responsible for a given
     * key but does not respect rack awareness. Basically
     * returns the 3 nodes that lie right next to each other
     * on the ring.
     */
    class RackUnawareStrategy extends AbstractStrategy
    {        
        public EndPoint[] getStorageEndPoints(BigInteger token)
        {
            return getStorageEndPoints(token, tokenMetadata_.cloneTokenEndPointMap());            
        }
        
        public EndPoint[] getStorageEndPoints(BigInteger token, Map<BigInteger, EndPoint> tokenToEndPointMap)
        {
            int startIndex = 0 ;
            List<EndPoint> list = new ArrayList<EndPoint>();
            int foundCount = 0;
            int N = DatabaseDescriptor.getReplicationFactor();
            List<BigInteger> tokens = new ArrayList<BigInteger>(tokenToEndPointMap.keySet());
            Collections.sort(tokens);
            int index = Collections.binarySearch(tokens, token);
            if(index < 0)
            {
                index = (index + 1) * (-1);
                if (index >= tokens.size())
                    index = 0;
            }
            int totalNodes = tokens.size();
            // Add the node at the index by default
            list.add(tokenToEndPointMap.get(tokens.get(index)));
            foundCount++;
            startIndex = (index + 1)%totalNodes;
            // If we found N number of nodes we are good. This loop will just exit. Otherwise just
            // loop through the list and add until we have N nodes.
            for (int i = startIndex, count = 1; count < totalNodes && foundCount < N; ++count, i = (i+1)%totalNodes)
            {
                if( ! list.contains(tokenToEndPointMap.get(tokens.get(i))))
                {
                    list.add(tokenToEndPointMap.get(tokens.get(i)));
                    foundCount++;
                    continue;
                }
            }
            retrofitPorts(list);
            return list.toArray(new EndPoint[0]);
        }
    }

    /*
     * This class returns the nodes responsible for a given
     * key but does respects rack awareness. It makes a best
     * effort to get a node from a different data center and
     * a node in a different rack in the same datacenter as
     * the primary.
     */
    class RackAwareStrategy extends AbstractStrategy
    {
        public EndPoint[] getStorageEndPoints(BigInteger token)
        {
            int startIndex = 0 ;
            List<EndPoint> list = new ArrayList<EndPoint>();
            boolean bDataCenter = false;
            boolean bOtherRack = false;
            int foundCount = 0;
            int N = DatabaseDescriptor.getReplicationFactor();
            Map<BigInteger, EndPoint> tokenToEndPointMap = tokenMetadata_.cloneTokenEndPointMap();
            List<BigInteger> tokens = new ArrayList<BigInteger>(tokenToEndPointMap.keySet());
            Collections.sort(tokens);
            int index = Collections.binarySearch(tokens, token);
            if(index < 0)
            {
                index = (index + 1) * (-1);
                if (index >= tokens.size())
                    index = 0;
            }
            int totalNodes = tokens.size();
            // Add the node at the index by default
            list.add(tokenToEndPointMap.get(tokens.get(index)));
            foundCount++;
            if( N == 1 )
            {
                return list.toArray(new EndPoint[0]);
            }
            startIndex = (index + 1)%totalNodes;
            for (int i = startIndex, count = 1; count < totalNodes && foundCount < N; ++count, i = (i+1)%totalNodes)
            {
                try
                {
                    // First try to find one in a different data center
                    if(!endPointSnitch_.isInSameDataCenter(tokenToEndPointMap.get(tokens.get(index)), tokenToEndPointMap.get(tokens.get(i))))
                    {
                        // If we have already found something in a diff datacenter no need to find another
                        if( !bDataCenter )
                        {
                            list.add(tokenToEndPointMap.get(tokens.get(i)));
                            bDataCenter = true;
                            foundCount++;
                        }
                        continue;
                    }
                    // Now  try to find one on a different rack
                    if(!endPointSnitch_.isOnSameRack(tokenToEndPointMap.get(tokens.get(index)), tokenToEndPointMap.get(tokens.get(i))) &&
                            endPointSnitch_.isInSameDataCenter(tokenToEndPointMap.get(tokens.get(index)), tokenToEndPointMap.get(tokens.get(i))))
                    {
                        // If we have already found something in a diff rack no need to find another
                        if( !bOtherRack )
                        {
                            list.add(tokenToEndPointMap.get(tokens.get(i)));
                            bOtherRack = true;
                            foundCount++;
                        }
                        continue;
                    }
                }
                catch (UnknownHostException e)
                {
                    logger_.debug(LogUtil.throwableToString(e));
                }

            }
            // If we found N number of nodes we are good. This loop wil just exit. Otherwise just
            // loop through the list and add until we have N nodes.
            for (int i = startIndex, count = 1; count < totalNodes && foundCount < N; ++count, i = (i+1)%totalNodes)
            {
                if( ! list.contains(tokenToEndPointMap.get(tokens.get(i))))
                {
                    list.add(tokenToEndPointMap.get(tokens.get(i)));
                    foundCount++;
                    continue;
                }
            }
            retrofitPorts(list);
            return list.toArray(new EndPoint[0]);
        }
        
        public EndPoint[] getStorageEndPoints(BigInteger token, Map<BigInteger, EndPoint> tokenToEndPointMap)
        {
            throw new UnsupportedOperationException("This operation is not currently supported");
        }
    }
    
    private enum BootstrapMode
    {
        HINT,
        FULL
    };
    
    /*
     * This class encapsulates who is the source and the
     * target of a bootstrap for a particular range.
     */
    class BootstrapSourceTarget
    {
        protected EndPoint source_;
        protected EndPoint target_;
        
        BootstrapSourceTarget(EndPoint source, EndPoint target)
        {
            source_ = source;
            target_ = target;
        }
    }
    
    /**
     * This class encapsulates the message that needs to be sent
     * to nodes that handoff data. The message contains information
     * about the node to be bootstrapped and the ranges with which
     * it needs to be bootstrapped.
    */
    protected static class BootstrapMetadataMessage
    {
        private static ICompactSerializer<BootstrapMetadataMessage> serializer_;
        static
        {
            serializer_ = new BootstrapMetadataMessageSerializer();
        }
        
        protected static ICompactSerializer<BootstrapMetadataMessage> serializer()
        {
            return serializer_;
        }
        
        protected static Message makeBootstrapMetadataMessage(BootstrapMetadataMessage bsMetadataMessage) throws IOException
        {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream( bos );
            BootstrapMetadataMessage.serializer().serialize(bsMetadataMessage, dos);
            return new Message( StorageService.getLocalStorageEndPoint(), "", StorageService.bsMetadataVerbHandler_, new Object[]{bos.toByteArray()} );            
        }        
        
        protected BootstrapMetadata[] bsMetadata_ = new BootstrapMetadata[0];
        
        BootstrapMetadataMessage(BootstrapMetadata[] bsMetadata)
        {
            bsMetadata_ = bsMetadata;
        }
    }
    
    protected static class BootstrapMetadataMessageSerializer implements ICompactSerializer<BootstrapMetadataMessage>
    {
        public void serialize(BootstrapMetadataMessage bsMetadataMessage, DataOutputStream dos) throws IOException
        {
            BootstrapMetadata[] bsMetadata = bsMetadataMessage.bsMetadata_;
            int size = (bsMetadata == null) ? 0 : bsMetadata.length;
            dos.writeInt(size);
            for ( BootstrapMetadata bsmd : bsMetadata )
            {
                BootstrapMetadata.serializer().serialize(bsmd, dos);
            }
        }

        public BootstrapMetadataMessage deserialize(DataInputStream dis) throws IOException
        {            
            int size = dis.readInt();
            BootstrapMetadata[] bsMetadata = new BootstrapMetadata[size];
            for ( int i = 0; i < size; ++i )
            {
                bsMetadata[i] = BootstrapMetadata.serializer().deserialize(dis);
            }
            return new BootstrapMetadataMessage(bsMetadata);
        }
    }
    
    /*
     * This verb handler handles the BootstrapMetadataMessage that is sent
     * by the leader to the nodes that are responsible for handing off data. 
    */
    protected static class BootstrapMetadataVerbHandler implements IVerbHandler
    {
        public void doVerb(Message message)
        {
            logger_.debug("Received a BootstrapMetadataMessage from " + message.getFrom());
            byte[] body = (byte[])message.getMessageBody()[0];
            DataInputBuffer bufIn = new DataInputBuffer();
            bufIn.reset(body, body.length);
            try
            {
                BootstrapMetadataMessage bsMetadataMessage = BootstrapMetadataMessage.serializer().deserialize(bufIn);
                BootstrapMetadata[] bsMetadata = bsMetadataMessage.bsMetadata_;
                
                /*
                 * This is for debugging purposes. Remove later.
                */
                for ( BootstrapMetadata bsmd : bsMetadata )
                {
                    logger_.debug(bsmd.toString());                                      
                }
                
                for ( BootstrapMetadata bsmd : bsMetadata )
                {
                    long startTime = System.currentTimeMillis();
                    doTransfer(bsmd.target_, bsmd.ranges_);     
                    logger_.debug("Time taken to bootstrap " + 
                            bsmd.target_ + 
                            " is " + 
                            (System.currentTimeMillis() - startTime) +
                            " msecs.");
                }
            }
            catch ( IOException ex )
            {
                logger_.info(LogUtil.throwableToString(ex));
            }
        }
        
        /*
         * This method needs to figure out the files on disk
         * locally for each range and then stream them using
         * the Bootstrap protocol to the target endpoint.
        */
        private void doTransfer(EndPoint target, List<Range> ranges) throws IOException
        {
            if ( ranges.isEmpty() )
            {
                logger_.debug("No ranges to give scram ...");
                return;
            }
            
            /* Just for debugging process - remove later */            
            for ( Range range : ranges )
            {
                StringBuilder sb = new StringBuilder("");                
                sb.append(range.toString());
                sb.append(" ");            
                logger_.debug("Beginning transfer process to " + target + " for ranges " + sb.toString());                
            }
          
            /*
             * (1) First we dump all the memtables to disk.
             * (2) Run a version of compaction which will basically
             *     put the keys in the range specified into a directory
             *     named as per the endpoint it is destined for inside the
             *     bootstrap directory.
             * (3) Handoff the data.
            */
            List<String> tables = DatabaseDescriptor.getTables();
            for ( String tName : tables )
            {
                Table table = Table.open(tName);
                logger_.debug("Flushing memtables ...");
                table.flush(false);
                logger_.debug("Forcing compaction ...");
                /* Get the counting bloom filter for each endpoint and the list of files that need to be streamed */
                List<String> fileList = new ArrayList<String>();
                BloomFilter.CountingBloomFilter cbf = table.forceCompaction(ranges, target, fileList);                
                doHandoff(cbf, target, fileList);
            }
        }

        /*
         * Stream the files in the bootstrap directory over to the
         * node being bootstrapped.
        */
        private void doHandoff(BloomFilter.CountingBloomFilter cbf, EndPoint target, List<String> fileList) throws IOException
        {
            List<File> filesList = new ArrayList<File>();
            for(String file : fileList)
            {
            	filesList.add(new File(file));
            }
            File[] files = filesList.toArray(new File[0]);
            StreamContextManager.StreamContext[] streamContexts = new StreamContextManager.StreamContext[files.length];
            int i = 0;
            for ( File file : files )
            {
                if ( file.getName().indexOf("-Data.db") != -1 )
                {
                    RequestCountSampler.Cardinality cardinality = new RequestCountSampler.Cardinality(cbf, cbf.count());
                    streamContexts[i] = new StreamContextManager.StreamContext(file.getAbsolutePath(), file.length(), cardinality);
                }
                else
                {
                    streamContexts[i] = new StreamContextManager.StreamContext(file.getAbsolutePath(), file.length());
                }
                logger_.debug("Stream context metadata " + streamContexts[i]);
                ++i;
            }
            
            if ( files.length > 0 )
            {
                /* Set up the stream manager with the files that need to streamed */
                StreamManager.instance(target).addFilesToStream(streamContexts);
                /* Send the bootstrap initiate message */
                BootstrapInitiateMessage biMessage = new BootstrapInitiateMessage(streamContexts);
                Message message = BootstrapInitiateMessage.makeBootstrapInitiateMessage(biMessage);
                logger_.debug("Sending a bootstrap initiate message to " + target + " ...");
                MessagingService.getMessagingInstance().sendOneWay(message, target);                
                logger_.debug("Waiting for transfer to " + target + " to complete");
                StreamManager.instance(target).waitForStreamCompletion();
                logger_.debug("Done with transfer to " + target);  
            }
        }
    }
    
    /**
     * This encapsulates information of the list of 
     * ranges that a target node requires in order to 
     * be bootstrapped. This will be bundled in a 
     * BootstrapMetadataMessage and sent to nodes that
     * are going to handoff the data.
    */
    protected static class BootstrapMetadata
    {
        private static ICompactSerializer<BootstrapMetadata> serializer_;
        static
        {
            serializer_ = new BootstrapMetadataSerializer();
        }
        
        protected static ICompactSerializer<BootstrapMetadata> serializer()
        {
            return serializer_;
        }
        
        protected EndPoint target_;
        protected List<Range> ranges_;
        
        BootstrapMetadata(EndPoint target, List<Range> ranges)
        {
            target_ = target;
            ranges_ = ranges;
        }
        
        public String toString()
        {
            StringBuilder sb = new StringBuilder("");
            sb.append(target_);
            sb.append("------->");
            for ( Range range : ranges_ )
            {
                sb.append(range);
                sb.append(" ");
            }
            return sb.toString();
        }
    }
    
    protected static class BootstrapMetadataSerializer implements ICompactSerializer<BootstrapMetadata>
    {
        public void serialize(BootstrapMetadata bsMetadata, DataOutputStream dos) throws IOException
        {
            CompactEndPointSerializationHelper.serialize(bsMetadata.target_, dos);
            int size = (bsMetadata.ranges_ == null) ? 0 : bsMetadata.ranges_.size();            
            dos.writeInt(size);
            
            for ( Range range : bsMetadata.ranges_ )
            {
                Range.serializer().serialize(range, dos);
            }            
        }

        public BootstrapMetadata deserialize(DataInputStream dis) throws IOException
        {            
            EndPoint target = CompactEndPointSerializationHelper.deserialize(dis);
            int size = dis.readInt();
            List<Range> ranges = (size == 0) ? null : new ArrayList<Range>();
            for( int i = 0; i < size; ++i )
            {
                ranges.add(Range.serializer().deserialize(dis));
            }            
            return new BootstrapMetadata( target, ranges );
        }
    }

    /*
     * This class handles the bootstrapping responsibilities for
     * any new endpoint.
    */
    class BootStrapper implements Runnable
    {
        private EndPoint[] targets_ = new EndPoint[0];
        private BigInteger[] tokens_ = new BigInteger[0];

        BootStrapper(EndPoint[] target, BigInteger[] token)
        {
            targets_ = target;
            tokens_ = token;
        }

        public void run()
        {
            try
            {
                logger_.debug("Beginning bootstrap process for " + targets_ + " ...");                                                               
                /* copy the token to endpoint map */
                Map<BigInteger, EndPoint> tokenToEndPointMap = tokenMetadata_.cloneTokenEndPointMap();
                /* remove the tokens associated with the endpoints being bootstrapped */                
                for ( BigInteger token : tokens_ )
                {
                    tokenToEndPointMap.remove(token);                    
                }

                Set<BigInteger> oldTokens = new HashSet<BigInteger>( tokenToEndPointMap.keySet() );
                Range[] oldRanges = getAllRanges(oldTokens);
                logger_.debug("Total number of old ranges " + oldRanges.length);
                /* 
                 * Find the ranges that are split. Maintain a mapping between
                 * the range being split and the list of subranges.
                */                
                Map<Range, List<Range>> splitRanges = getRangeSplitRangeMapping(oldRanges);                                                      
                /* Calculate the list of nodes that handle the old ranges */
                Map<Range, List<EndPoint>> oldRangeToEndPointMap = constructRangeToEndPointMap(oldRanges, tokenToEndPointMap);
                /* Mapping of split ranges to the list of endpoints responsible for the range */                
                Map<Range, List<EndPoint>> replicasForSplitRanges = new HashMap<Range, List<EndPoint>>();                                
                Set<Range> rangesSplit = splitRanges.keySet();                
                for ( Range splitRange : rangesSplit )
                {
                    replicasForSplitRanges.put( splitRange, oldRangeToEndPointMap.get(splitRange) );
                }                
                /* Remove the ranges that are split. */
                for ( Range splitRange : rangesSplit )
                {
                    oldRangeToEndPointMap.remove(splitRange);
                }
                
                /* Add the subranges of the split range to the map with the same replica set. */
                for ( Range splitRange : rangesSplit )
                {
                    List<Range> subRanges = splitRanges.get(splitRange);
                    List<EndPoint> replicas = replicasForSplitRanges.get(splitRange);
                    for ( Range subRange : subRanges )
                    {
                        /* Make sure we clone or else we are hammered. */
                        oldRangeToEndPointMap.put(subRange, new ArrayList<EndPoint>(replicas));
                    }
                }                
                
                /* Add the new token and re-calculate the range assignments */
                Collections.addAll( oldTokens, tokens_ );
                Range[] newRanges = getAllRanges(oldTokens);

                logger_.debug("Total number of new ranges " + newRanges.length);
                /* Calculate the list of nodes that handle the new ranges */
                Map<Range, List<EndPoint>> newRangeToEndPointMap = constructRangeToEndPointMap(newRanges);
                /* Calculate ranges that need to be sent and from whom to where */
                Map<Range, List<BootstrapSourceTarget>> rangesWithSourceTarget = getRangeSourceTargetInfo(oldRangeToEndPointMap, newRangeToEndPointMap);
                /* Send messages to respective folks to stream data over to the new nodes being bootstrapped */
                assignWork(rangesWithSourceTarget);                
            }
            catch ( Throwable th )
            {
                logger_.debug( LogUtil.throwableToString(th) );
            }
        }
        
        /*
         * Give a range a-------------b which is being split as
         * a-----x-----y-----b then we want a mapping from 
         * (a, b] --> (a, x], (x, y], (y, b] 
        */
        private Map<Range, List<Range>> getRangeSplitRangeMapping(Range[] oldRanges)
        {
            Map<Range, List<Range>> splitRanges = new HashMap<Range, List<Range>>();
            BigInteger[] tokens = new BigInteger[tokens_.length];
            System.arraycopy(tokens_, 0, tokens, 0, tokens.length);
            Arrays.sort(tokens);
            
            Range prevRange = null;
            BigInteger prevToken = null;
            boolean bVal = false;
            
            for ( Range oldRange : oldRanges )
            {
                if ( bVal && prevRange != null )
                {
                    bVal = false; 
                    List<Range> subRanges = splitRanges.get(prevRange);
                    if ( subRanges != null )
                        subRanges.add( new Range(prevToken, prevRange.right()) );     
                }
                
                prevRange = oldRange;
                prevToken = oldRange.left();                
                for ( BigInteger token : tokens )
                {     
                    List<Range> subRanges = splitRanges.get(oldRange);
                    if ( oldRange.contains(token) )
                    {                        
                        if ( subRanges == null )
                        {
                            subRanges = new ArrayList<Range>();
                            splitRanges.put(oldRange, subRanges);
                        }                            
                        subRanges.add( new Range(prevToken, token) );
                        prevToken = token;
                        bVal = true;
                    }
                    else
                    {
                        if ( bVal )
                        {
                            bVal = false;                                                                                
                            subRanges.add( new Range(prevToken, oldRange.right()) );                            
                        }
                    }
                }
            }
            /* This is to handle the last range being processed. */
            if ( bVal )
            {
                bVal = false; 
                List<Range> subRanges = splitRanges.get(prevRange);
                subRanges.add( new Range(prevToken, prevRange.right()) );                            
            }
            return splitRanges;
        }
        
        Map<Range, List<BootstrapSourceTarget>> getRangeSourceTargetInfo(Map<Range, List<EndPoint>> oldRangeToEndPointMap, Map<Range, List<EndPoint>> newRangeToEndPointMap)
        {
            Map<Range, List<BootstrapSourceTarget>> rangesWithSourceTarget = new HashMap<Range, List<BootstrapSourceTarget>>();
            /*
             * Basically calculate for each range the endpoints handling the
             * range in the old token set and in the new token set. Whoever
             * gets bumped out of the top N will have to hand off that range
             * to the new dude.
            */
            Set<Range> oldRangeSet = oldRangeToEndPointMap.keySet();
            for(Range range : oldRangeSet)
            {
                logger_.debug("Attempting to figure out the dudes who are bumped out for " + range + " ...");
                List<EndPoint> oldEndPoints = oldRangeToEndPointMap.get(range);
                List<EndPoint> newEndPoints = newRangeToEndPointMap.get(range);
                if ( newEndPoints != null )
                {                        
                    List<EndPoint> newEndPoints2 = new ArrayList<EndPoint>(newEndPoints);
                    for ( EndPoint newEndPoint : newEndPoints2 )
                    {
                        if ( oldEndPoints.contains(newEndPoint) )
                        {
                            oldEndPoints.remove(newEndPoint);
                            newEndPoints.remove(newEndPoint);
                        }
                    }                        
                }
                else
                {
                    logger_.warn("Trespassing - scram");
                }
                logger_.debug("Done figuring out the dudes who are bumped out for range " + range + " ...");
            }
            for ( Range range : oldRangeSet )
            {                    
                List<EndPoint> oldEndPoints = oldRangeToEndPointMap.get(range);
                List<EndPoint> newEndPoints = newRangeToEndPointMap.get(range);
                List<BootstrapSourceTarget> srcTarget = rangesWithSourceTarget.get(range);
                if ( srcTarget == null )
                {
                    srcTarget = new ArrayList<BootstrapSourceTarget>();
                    rangesWithSourceTarget.put(range, srcTarget);
                }
                int i = 0;
                for ( EndPoint oldEndPoint : oldEndPoints )
                {                        
                    srcTarget.add( new BootstrapSourceTarget(oldEndPoint, newEndPoints.get(i++)) );
                }
            }
            return rangesWithSourceTarget;
        }
     
        private Range getMyOldRange()
        {
        	Map<EndPoint, BigInteger> oldEndPointToTokenMap = tokenMetadata_.cloneEndPointTokenMap();
        	Map<BigInteger, EndPoint> oldTokenToEndPointMap = tokenMetadata_.cloneTokenEndPointMap();

        	oldEndPointToTokenMap.remove(targets_);
        	oldTokenToEndPointMap.remove(tokens_);

        	BigInteger myToken = oldEndPointToTokenMap.get(StorageService.tcpAddr_);
            List<BigInteger> allTokens = new ArrayList<BigInteger>(oldTokenToEndPointMap.keySet());
            Collections.sort(allTokens);
            int index = Collections.binarySearch(allTokens, myToken);
            /* Calculate the lhs for the range */
            BigInteger lhs = (index == 0) ? allTokens.get(allTokens.size() - 1) : allTokens.get( index - 1);
            return new Range( lhs, myToken );
        }
        
        /*
         * This method sends messages out to nodes instructing them 
         * to stream the specified ranges to specified target nodes. 
        */
        private void assignWork(Map<Range, List<BootstrapSourceTarget>> rangesWithSourceTarget) throws IOException
        {
            /*
             * Map whose key is the source node and the value is a map whose key is the
             * target and value is the list of ranges to be sent to it. 
            */
            Map<EndPoint, Map<EndPoint, List<Range>>> rangeInfo = new HashMap<EndPoint, Map<EndPoint, List<Range>>>();

            for ( Map.Entry<Range, List<BootstrapSourceTarget>> entry : rangesWithSourceTarget.entrySet() )
            {
                Range range = entry.getKey();
                List<BootstrapSourceTarget> rangeSourceTargets = entry.getValue();

                for ( BootstrapSourceTarget rangeSourceTarget : rangeSourceTargets )
                {
                    Map<EndPoint, List<Range>> targetRangeMap = rangeInfo.get(rangeSourceTarget.source_);
                    if ( targetRangeMap == null )
                    {
                        targetRangeMap = new HashMap<EndPoint, List<Range>>();
                        rangeInfo.put(rangeSourceTarget.source_, targetRangeMap);
                    }
                    List<Range> rangesToGive = targetRangeMap.get(rangeSourceTarget.target_);
                    if ( rangesToGive == null )
                    {
                        rangesToGive = new ArrayList<Range>();
                        targetRangeMap.put(rangeSourceTarget.target_, rangesToGive);
                    }
                    rangesToGive.add(range);
                }
            }


            for ( Map.Entry<EndPoint, Map<EndPoint, List<Range>>> entry : rangeInfo.entrySet() )
            {
                EndPoint source = entry.getKey();
                Map<EndPoint, List<Range>> targetRangesMap = entry.getValue();


                List<BootstrapMetadata> bsmdList = new ArrayList<BootstrapMetadata>();
                
                for ( Map.Entry<EndPoint, List<Range>> entry2 : targetRangesMap.entrySet() )
                {
                    EndPoint target = entry2.getKey();
                    List<Range> rangeForTarget = entry2.getValue();

                    BootstrapMetadata bsMetadata = new BootstrapMetadata(target, rangeForTarget);
                    bsmdList.add(bsMetadata);
                }
                
                BootstrapMetadataMessage bsMetadataMessage = new BootstrapMetadataMessage(bsmdList.toArray( new BootstrapMetadata[0] ) );
                /* Send this message to the source to do his shit. */
                Message message = BootstrapMetadataMessage.makeBootstrapMetadataMessage(bsMetadataMessage); 
                logger_.debug("Sending the BootstrapMetadataMessage to " + source);
                MessagingService.getMessagingInstance().sendOneWay(message, source);
            }
        }
    }

    public static class BootstrapInitiateDoneVerbHandler implements IVerbHandler
    {
        private static Logger logger_ = Logger.getLogger( BootstrapInitiateDoneVerbHandler.class );

        public void doVerb(Message message)
        {
            logger_.debug("Received a bootstrap initiate done message ...");            
            /* Let the Stream Manager do his thing. */
            StreamManager.instance( message.getFrom() ).start();
        }
    }

    private class ShutdownTimerTask extends TimerTask
    {
    	public void run()
    	{
    		StorageService.instance().shutdown();
    	}
    }

    /*
     * Factory method that gets an instance of the StorageService
     * class.
    */
    public static StorageService instance()
    {
        if ( instance_ == null )
        {
            StorageService.createLock_.lock();
            try
            {
                if ( instance_ == null )
                {
                    try
                    {
                        instance_ = new StorageService();
                    }
                    catch ( Throwable th )
                    {
                        logger_.error(LogUtil.throwableToString(th));
                        System.exit(1);
                    }
                }
            }
            finally
            {
                createLock_.unlock();
            }
        }
        return instance_;
    }

    /*
     * This is the endpoint snitch which depends on the network architecture. We
     * need to keep this information for each endpoint so that we make decisions
     * while doing things like replication etc.
     *
     */
    private IEndPointSnitch endPointSnitch_;
    /* Uptime of this node - we use this to determine if a bootstrap can be performed by this node */
    private long uptime_ = 0L;

    /* This abstraction maintains the token/endpoint metadata information */
    private TokenMetadata tokenMetadata_ = new TokenMetadata();
    private DBManager.StorageMetadata storageMetadata_;

    /*
     * Maintains a list of all components that need to be shutdown
     * for a clean exit.
    */
    private Set<IComponentShutdown> components_ = new HashSet<IComponentShutdown>();
    /*
     * This boolean indicates if we are in loading state. If we are then we do not want any
     * distributed algorithms w.r.t change in token state to kick in.
    */
    private boolean isLoadState_ = false;

    /*
     * This variable indicates if the local storage instance
     * has been shutdown.
    */
    private AtomicBoolean isShutdown_ = new AtomicBoolean(false);

    /* This thread pool is used to do the bootstrap for a new node */
    private ExecutorService bootStrapper_ = new DebuggableThreadPoolExecutor(1, 1,
            Integer.MAX_VALUE, TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(), new ThreadFactoryImpl(
                    "BOOT-STRAPPER"));
    
    /* This thread pool does consistency checks when the client doesn't care about consistency */
    private ExecutorService consistencyManager_;

    /* Helps determine number of keys processed in a time interval */
    private RequestCountSampler sampler_;

    /* This is the entity that tracks load information of all nodes in the cluster */
    private StorageLoadBalancer storageLoadBalancer_;
    /* We use this interface to determine where replicas need to be placed */
    private IReplicaPlacementStrategy nodePicker_;
    /* Handle to a ZooKeeper instance */
    private ZooKeeper zk_;
    
    /*
     * Registers with Management Server
     */
    private void init()
    {
        // Register this instance with JMX
        try
        {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            mbs.registerMBean(this, new ObjectName(
                    "com.facebook.infrastructure.service:type=StorageService"));
        }
        catch (Exception e)
        {
            logger_.error(LogUtil.throwableToString(e));
        }
    }

    public StorageService() throws Throwable
    {
        init();
        uptime_ = System.currentTimeMillis();
        storageLoadBalancer_ = new StorageLoadBalancer(this);
        endPointSnitch_ = new EndPointSnitch();
        
        /* register the verb handlers */
        MessagingService.getMessagingInstance().registerVerbHandlers(StorageService.tokenVerbHandler_, new TokenUpdateVerbHandler());
        MessagingService.getMessagingInstance().registerVerbHandlers(StorageService.binaryVerbHandler_, new BinaryVerbHandler());
        MessagingService.getMessagingInstance().registerVerbHandlers(StorageService.loadVerbHandler_, new LoadVerbHandler());
        MessagingService.getMessagingInstance().registerVerbHandlers(StorageService.mutationVerbHandler_, new RowMutationVerbHandler());
        MessagingService.getMessagingInstance().registerVerbHandlers(StorageService.readRepairVerbHandler_, new ReadRepairVerbHandler());
        MessagingService.getMessagingInstance().registerVerbHandlers(StorageService.readVerbHandler_, new ReadVerbHandler());
        MessagingService.getMessagingInstance().registerVerbHandlers(StorageService.bootStrapInitiateVerbHandler_, new Table.BootStrapInitiateVerbHandler());
        MessagingService.getMessagingInstance().registerVerbHandlers(StorageService.bootStrapInitiateDoneVerbHandler_, new StorageService.BootstrapInitiateDoneVerbHandler());
        MessagingService.getMessagingInstance().registerVerbHandlers(StorageService.bootStrapTerminateVerbHandler_, new StreamManager.BootstrapTerminateVerbHandler());
        MessagingService.getMessagingInstance().registerVerbHandlers(HttpConnection.httpRequestVerbHandler_, new HttpRequestVerbHandler(this) );
        MessagingService.getMessagingInstance().registerVerbHandlers(StorageService.tokenInfoVerbHandler_, new TokenInfoVerbHandler() );
        MessagingService.getMessagingInstance().registerVerbHandlers(StorageService.mbrshipCleanerVerbHandler_, new MembershipCleanerVerbHandler() );
        MessagingService.getMessagingInstance().registerVerbHandlers(StorageService.bsMetadataVerbHandler_, new BootstrapMetadataVerbHandler() );
        
        /* register the stage for the mutations */
        int threadCount = DatabaseDescriptor.getThreadsPerPool();
        consistencyManager_ = new DebuggableThreadPoolExecutor(threadCount,
        		threadCount,
                Integer.MAX_VALUE, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(), new ThreadFactoryImpl(
                        "CONSISTENCY-MANAGER"));
        
        StageManager.registerStage(StorageService.mutationStage_, new MultiThreadedStage("ROW-MUTATION", threadCount));
        StageManager.registerStage(StorageService.readStage_, new MultiThreadedStage("ROW-READ", threadCount));
        /* Stage for handling the HTTP messages. */
        StageManager.registerStage(HttpConnection.httpStage_, new SingleThreadedStage("HTTP-REQUEST"));

        if ( DatabaseDescriptor.isRackAware() )
            nodePicker_ = new RackAwareStrategy();
        else
            nodePicker_ = new RackUnawareStrategy();
    }
    
    private void reportToZookeeper() throws Throwable
    {
        try
        {
            zk_ = new ZooKeeper(DatabaseDescriptor.getZkAddress(), DatabaseDescriptor.getZkSessionTimeout(), new Watcher()
                {
                    public void process(WatcherEvent we)
                    {                    
                        String path = "/Cassandra/" + DatabaseDescriptor.getClusterName() + "/Leader";
                        String eventPath = we.getPath();
                        logger_.debug("PROCESS EVENT : " + eventPath);
                        if ( eventPath != null && (eventPath.indexOf(path) != -1) )
                        {                                                           
                            logger_.debug("Signalling the leader instance ...");
                            LeaderElector.instance().signal();                                        
                        }                                                  
                    }
                });
            
            Stat stat = zk_.exists("/", false);
            if ( stat != null )
            {
                stat = zk_.exists("/Cassandra", false);
                if ( stat == null )
                {
                    logger_.debug("Creating the Cassandra znode ...");
                    zk_.create("/Cassandra", new byte[0], Ids.OPEN_ACL_UNSAFE, 0);
                }
                
                String path = "/Cassandra/" + DatabaseDescriptor.getClusterName();
                stat = zk_.exists(path, false);
                if ( stat == null )
                {
                    logger_.debug("Creating the cluster znode " + path);
                    zk_.create(path, new byte[0], Ids.OPEN_ACL_UNSAFE, 0);
                }
                
                /* Create the Leader, Locks and Misc znode */
                stat = zk_.exists(path + "/Leader", false);
                if ( stat == null )
                {
                    logger_.debug("Creating the leader znode " + path);
                    zk_.create(path + "/Leader", new byte[0], Ids.OPEN_ACL_UNSAFE, 0);
                }
                
                stat = zk_.exists(path + "/Locks", false);
                if ( stat == null )
                {
                    logger_.debug("Creating the locks znode " + path);
                    zk_.create(path + "/Locks", new byte[0], Ids.OPEN_ACL_UNSAFE, 0);
                }
                                
                stat = zk_.exists(path + "/Misc", false);
                if ( stat == null )
                {
                    logger_.debug("Creating the misc znode " + path);
                    zk_.create(path + "/Misc", new byte[0], Ids.OPEN_ACL_UNSAFE, 0);
                }
            }
        }
        catch ( KeeperException ke )
        {
            LogUtil.throwableToString(ke);
            /* do the re-initialize again. */
            reportToZookeeper();
        }
    }
    
    protected ZooKeeper getZooKeeperHandle()
    {
        return zk_;
    }
    
    public boolean isLeader(EndPoint endpoint)
    {
        EndPoint leader = getLeader();
        return leader.equals(endpoint);
    }
    
    public EndPoint getLeader()
    {
        return LeaderElector.instance().getLeader();
    }

    public void registerComponentForShutdown(IComponentShutdown component)
    {
    	components_.add(component);
    }
    
    public void registerExternalVerbHandler(String verb, IVerbHandler verbHandler)
    {
    	MessagingService.getMessagingInstance().registerVerbHandlers(verb, verbHandler);
    }

    public void start() throws Throwable
    {
        storageMetadata_ = DBManager.instance().start();
                      
        /* Set up TCP endpoint */
        tcpAddr_ = new EndPoint(DatabaseDescriptor.getStoragePort());
        /* Set up UDP endpoint */
        udpAddr_ = new EndPoint(DatabaseDescriptor.getControlPort());
        /* Listen for application messages */
        MessagingService.getMessagingInstance().listen(tcpAddr_, false);
        /* Listen for control messages */
        MessagingService.getMessagingInstance().listenUDP(udpAddr_);
        /* Listen for HTTP messages */
        MessagingService.getMessagingInstance().listen( new EndPoint(DatabaseDescriptor.getHttpPort() ), true );
        /* start the analytics context package */
        AnalyticsContext.instance().start();
        /* report our existence to ZooKeeper instance and start the leader election service */
        // 20080716: Comment these lines out until Zookeeper intregation is complete (jeff.hammerbacher)
        //reportToZookeeper();         
        //LeaderElector.instance().start();
        /*Start the storage load balancer */
        storageLoadBalancer_.start();
        /* Register with the Gossiper for EndPointState notifications */
        Gossiper.instance().register(this);
        /*
         * Start the gossiper with the generation # retrieved from the System
         * table
         */
        Gossiper.instance().start(udpAddr_, storageMetadata_.getGeneration());
        /* Set up the request sampler */        
        sampler_ = new RequestCountSampler();
        /* Make sure this token gets gossiped around. */
        tokenMetadata_.update(storageMetadata_.getStorageId(), StorageService.tcpAddr_);
        Gossiper.instance().addApplicationState(StorageService.nodeId_, new ApplicationState(storageMetadata_.getStorageId().toString()));
    }

    public void killMe() throws Throwable
    {
        isShutdown_.set(true);
        /* 
         * Shutdown the Gossiper to stop responding/sending Gossip messages.
         * This causes other nodes to detect you as dead and starting hinting
         * data for the local endpoint. 
        */
        Gossiper.instance().shutdown();
        final long nodeDeadDetectionTime = 25000L;
        Thread.sleep(nodeDeadDetectionTime);
        /* Now perform a force flush of the table */
        String table = DatabaseDescriptor.getTables().get(0);
        Table.open(table).flush(false);
        /* Now wait for the flush to complete */
        Thread.sleep(nodeDeadDetectionTime);
        /* Shutdown all other components */
        StorageService.instance().shutdown();
    }

    public boolean isShutdown()
    {
    	return isShutdown_.get();
    }

    public void shutdown()
    {
        bootStrapper_.shutdownNow();
        /* shut down all stages */
        StageManager.shutdown();
        /* shut down the messaging service */
        MessagingService.shutdown();
        /* shut down all memtables */
        Memtable.shutdown();
        /* shut down the request count sampler */
        RequestCountSampler.shutdown();
        /* shut down the cleaner thread in FileUtils */
        FileUtils.shutdown();

        /* shut down all registered components */
        for ( IComponentShutdown component : components_ )
        {
        	component.shutdown();
        }
    }

    TokenMetadata getTokenMetadata()
    {
        return tokenMetadata_.cloneMe();
    }

    IEndPointSnitch getEndPointSnitch()
    {
    	return endPointSnitch_;
    }
    
    /*
     * Given an EndPoint this method will report if the
     * endpoint is in the same data center as the local
     * storage endpoint.
    */
    public boolean isInSameDataCenter(EndPoint endpoint) throws IOException
    {
        return endPointSnitch_.isInSameDataCenter(StorageService.tcpAddr_, endpoint);
    }
    
    /*
     * This method performs the requisite operations to make
     * sure that the N replicas are in sync. We do this in the
     * background when we do not care much about consistency.
     */
    public void doConsistencyCheck(Row row, List<EndPoint> endpoints, String columnFamily, int start, int count)
	{
		Runnable consistencySentinel = new ConsistencyManager(row.cloneMe(), endpoints, columnFamily, start, count);
		consistencyManager_.submit(consistencySentinel);
	}
    
    public void doConsistencyCheck(Row row, List<EndPoint> endpoints, String columnFamily, List<String> columns)
    {
    	Runnable consistencySentinel = new ConsistencyManager(row.cloneMe(), endpoints, columnFamily, columns);
		consistencyManager_.submit(consistencySentinel);
    }

    /*
     * This method displays all the ranges and the replicas
     * that are responsible for the individual ranges. The
     * format of this string is the following:
     *
     *  R1 : A B C
     *  R2 : D E F
     *  R3 : G H I
    */
    public String showTheRing()
    {
        StringBuilder sb = new StringBuilder();
        /* Get the token to endpoint map. */
        Map<BigInteger, EndPoint> tokenToEndPointMap = tokenMetadata_.cloneTokenEndPointMap();
        Set<BigInteger> tokens = tokenToEndPointMap.keySet();
        /* All the ranges for the tokens */
        Range[] ranges = getAllRanges(tokens);
        Map<Range, List<EndPoint>> oldRangeToEndPointMap = constructRangeToEndPointMap(ranges);


        for ( Map.Entry<Range, List<EndPoint>> entry : oldRangeToEndPointMap.entrySet() )
        {
            Range range = entry.getKey();
            List<EndPoint> replicas = entry.getValue();

            sb.append(range);
            sb.append(" : ");

            for ( EndPoint replica : replicas )
            {
                sb.append(replica);
                sb.append(" ");
            }
            sb.append(System.getProperty("line.separator"));
        }
        return sb.toString();
    }

    public Map<Range, List<EndPoint>> getRangeToEndPointMap()
    {
        /* Get the token to endpoint map. */
        Map<BigInteger, EndPoint> tokenToEndPointMap = tokenMetadata_.cloneTokenEndPointMap();
        Set<BigInteger> tokens = tokenToEndPointMap.keySet();
        /* All the ranges for the tokens */
        Range[] ranges = getAllRanges(tokens);
        Map<Range, List<EndPoint>> oldRangeToEndPointMap = constructRangeToEndPointMap(ranges);

        return oldRangeToEndPointMap;
    }

    /**
     * Construct the range to endpoint mapping based on the true view 
     * of the world. 
     * @param ranges
     * @return mapping of ranges to the replicas responsible for them.
    */
    private Map<Range, List<EndPoint>> constructRangeToEndPointMap(Range[] ranges)
    {
        logger_.debug("Constructing range to endpoint map ...");
        Map<Range, List<EndPoint>> rangeToEndPointMap = new HashMap<Range, List<EndPoint>>();
        for ( Range range : ranges )
        {
            EndPoint[] endpoints = getNStorageEndPoint(range.right());
            rangeToEndPointMap.put(range, new ArrayList<EndPoint>( Arrays.asList(endpoints) ) );
        }
        logger_.debug("Done constructing range to endpoint map ...");
        return rangeToEndPointMap;
    }
    
    /**
     * Construct the range to endpoint mapping based on the view as dictated
     * by the mapping of token to endpoints passed in. 
     * @param ranges
     * @param tokenToEndPointMap mapping of token to endpoints.
     * @return mapping of ranges to the replicas responsible for them.
    */
    private Map<Range, List<EndPoint>> constructRangeToEndPointMap(Range[] ranges, Map<BigInteger, EndPoint> tokenToEndPointMap)
    {
        logger_.debug("Constructing range to endpoint map ...");
        Map<Range, List<EndPoint>> rangeToEndPointMap = new HashMap<Range, List<EndPoint>>();
        for ( Range range : ranges )
        {
            EndPoint[] endpoints = getNStorageEndPoint(range.right(), tokenToEndPointMap);
            rangeToEndPointMap.put(range, new ArrayList<EndPoint>( Arrays.asList(endpoints) ) );
        }
        logger_.debug("Done constructing range to endpoint map ...");
        return rangeToEndPointMap;
    }
    
    /**
     * Construct a mapping from endpoint to ranges that endpoint is
     * responsible for.
     * @return the mapping from endpoint to the ranges it is responsible
     * for.
     */
    private Map<EndPoint, List<Range>> constructEndPointToRangesMap()
    {
        Map<EndPoint, List<Range>> endPointToRangesMap = new HashMap<EndPoint, List<Range>>();
        Set<EndPoint> mbrs = Gossiper.instance().getAllMembers();
        for ( EndPoint mbr : mbrs )
        {
            mbr.setPort(DatabaseDescriptor.getStoragePort());
            endPointToRangesMap.put(mbr, getRangesForEndPoint(mbr));
        }
        return endPointToRangesMap;
    }
    
    /**
     * Get the estimated disk space of the target endpoint in its
     * primary range.
     * @param target whose primary range we are interested in.
     * @return disk space of the target in the primary range.
     */
    private double getDiskSpaceForPrimaryRange(EndPoint target)
    {
        double primaryDiskSpace = 0d;
        Map<BigInteger, EndPoint> tokenToEndPointMap = tokenMetadata_.cloneTokenEndPointMap();
        Set<BigInteger> tokens = tokenToEndPointMap.keySet();
        Range[] allRanges = getAllRanges(tokens);
        Arrays.sort(allRanges);
        /* Mapping from Range to its ordered position on the ring */
        Map<Range, Integer> rangeIndex = new HashMap<Range, Integer>();
        for ( int i = 0; i < allRanges.length; ++i )
        {
            rangeIndex.put(allRanges[i], i);
        }
        /* Get the coefficients for the equations */
        List<double[]> equations = new ArrayList<double[]>();
        /* Get the endpoint to range map */
        Map<EndPoint, List<Range>> endPointToRangesMap = constructEndPointToRangesMap();

        for ( Map.Entry<EndPoint, List<Range>> entry : endPointToRangesMap.entrySet() )
        {
            EndPoint ep = entry.getKey();
            List<Range> ranges = entry.getValue();
            double[] equation = new double[allRanges.length];
            for ( Range range : ranges )
            {                
                int index = rangeIndex.get(range);
                equation[index] = 1;
            }
            equations.add(equation);
        }
        double[][] coefficients = equations.toArray( new double[0][0] );
        
        /* Get the constants which are the aggregate disk space for each endpoint */
        double[] constants = new double[allRanges.length];
        int index = 0;
        for ( EndPoint ep : endPointToRangesMap.keySet() )
        {
            /* reset the port back to control port */
            ep.setPort(DatabaseDescriptor.getControlPort());
            String lInfo = null;
            if ( ep.equals(StorageService.udpAddr_) )
                lInfo = getLoadInfo();
            else                
                lInfo = getLoadInfo(ep);
            LoadInfo li = new LoadInfo(lInfo);
            constants[index++] = FileUtils.stringToFileSize(li.diskSpace());
        }
        
        RealMatrix matrix = new RealMatrixImpl(coefficients);
        double[] solutions = matrix.solve(constants);
        Range primaryRange = getPrimaryRangeForEndPoint(target);
        primaryDiskSpace = solutions[rangeIndex.get(primaryRange)];
        return primaryDiskSpace;
    }
    
    /**
     * This is very dangerous. This is used only on the client
     * side to set up the client library. This is then used to
     * find the appropriate nodes to route the key to.
    */
    public void setTokenMetadata(TokenMetadata tokenMetadata)
    {
        tokenMetadata_ = tokenMetadata;
    }

    /**
     *  Called when there is a change in application state. In particular
     *  we are interested in new tokens as a result of a new node or an
     *  existing node moving to a new location on the ring.
    */
    public void onChange(EndPoint endpoint, EndPointState epState)
    {
        EndPoint ep = new EndPoint(endpoint.getHost(), DatabaseDescriptor.getStoragePort());
        /* node identifier for this endpoint on the identifier space */
        ApplicationState nodeIdState = epState.getApplicationState(StorageService.nodeId_);
        if (nodeIdState != null)
        {
            BigInteger newToken = new BigInteger(nodeIdState.getState());
            logger_.debug("CHANGE IN STATE FOR " + endpoint + " - has token " + nodeIdState.getState());
            BigInteger oldToken = tokenMetadata_.getToken(ep);

            if ( oldToken != null )
            {
                /*
                 * If oldToken equals the newToken then the node had crashed
                 * and is coming back up again. If oldToken is not equal to
                 * the newToken this means that the node is being relocated
                 * to another position in the ring.
                */
                if ( !oldToken.equals(newToken) )
                {
                    logger_.debug("Relocation for endpoint " + ep);
                    tokenMetadata_.update(newToken, ep);                    
                }
                else
                {
                    /*
                     * This means the node crashed and is coming back up.
                     * Deliver the hints that we have for this endpoint.
                    */
                    logger_.debug("Sending hinted data to " + ep);
                    doBootstrap(endpoint, BootstrapMode.HINT);
                }
            }
            else
            {
                /*
                 * This is a new node and we just update the token map.
                */
                tokenMetadata_.update(newToken, ep);
            }
        }
        else
        {
            /*
             * If we are here and if this node is UP and already has an entry
             * in the token map. It means that the node was behind a network partition.
            */
            if ( epState.isAlive() && tokenMetadata_.isKnownEndPoint(endpoint) )
            {
                logger_.debug("EndPoint " + ep + " just recovered from a partition. Sending hinted data.");
                doBootstrap(ep, BootstrapMode.HINT);
            }
        }

        /* Check if a bootstrap is in order */
        ApplicationState loadAllState = epState.getApplicationState(StorageService.loadAll_);
        if ( loadAllState != null )
        {
            String nodes = loadAllState.getState();
            if ( nodes != null )
            {
                doBootstrap(ep, BootstrapMode.FULL);
            }
        }
    }

    public static BigInteger generateRandomToken()
    {
	    byte[] randomBytes = new byte[24];
	    Random random = new Random();
	    for ( int i = 0 ; i < 24 ; i++)
	    {
	    randomBytes[i] = (byte)(31 + random.nextInt(256 - 31));
	    }
	    return hash(new String(randomBytes));
    }

    /**
     * This method is called by the Load Balancing module and
     * the Bootstrap module. Here we receive a Counting Bloom Filter
     * which we merge into the counter.
    */
    public void sample(RequestCountSampler.Cardinality cardinality)
    {
        sampler_.add(cardinality);
    }

    /**
     * Get the count of primary keys from the sampler.
    */
    public String getLoadInfo()
    {
        long diskSpace = FileUtils.getUsedDiskSpace();
        LoadInfo li = new LoadInfo(sampler_.count(), diskSpace);
    	return li.toString();
    }

    /**
     * Get the primary count info for this endpoint.
     * This is gossiped around and cached in the
     * StorageLoadBalancer.
    */
    public String getLoadInfo(EndPoint ep)
    {
        LoadInfo li = storageLoadBalancer_.getLoad(ep);
        return ( li == null ) ? "N/A" : li.toString();
    }
    
    /**
     * Get the endpoint that has the largest primary count.
     * @return
     */
    EndPoint getEndPointWithLargestPrimaryCount()
    {
        Set<EndPoint> allMbrs = Gossiper.instance().getAllMembers();
        Map<LoadInfo, EndPoint> loadInfoToEndPointMap = new HashMap<LoadInfo, EndPoint>();
        List<LoadInfo> lInfos = new ArrayList<LoadInfo>();
        
        for ( EndPoint mbr : allMbrs )
        {
            mbr.setPort(DatabaseDescriptor.getStoragePort());
            LoadInfo li = null;
            if ( mbr.equals(StorageService.tcpAddr_) )
            {
                li = new LoadInfo( getLoadInfo() );
                lInfos.add( li );
            }
            else
            {
                li = storageLoadBalancer_.getLoad(mbr);
                lInfos.add( li );
            }
            loadInfoToEndPointMap.put(li, mbr);
        }
        
        Collections.sort(lInfos, new LoadInfo.PrimaryCountComparator());
        return loadInfoToEndPointMap.get( lInfos.get(lInfos.size() - 1) );
    }

    /**
     * This method will sample the key into the
     * request count sampler.
     */
    public void sample(String key)
    {
    	if(isPrimary(key))
    	{
    		sampler_.sample(key);
    	}
    }

    /**
     * This method will delete the key from the
     * request count sampler.
    */
    public void delete(String key)
    {
        if ( isPrimary(key) )
        {
            sampler_.delete(key);
        }
    }

    /*
     * This method updates the token on disk and modifies the cached
     * StorageMetadata instance. This is only for the local endpoint.
    */
    public void updateToken(BigInteger token) throws IOException
    {
        /* update the token on disk */
        SystemTable.openSystemTable(SystemTable.name_).updateToken(token);
        /* Update the storageMetadata cache */
        storageMetadata_.setStorageId(token);
        /* Update the token maps */
        /* Get the old token. This needs to be removed. */
        tokenMetadata_.update(token, StorageService.tcpAddr_);
        /* Gossip this new token for the local storage instance */
        Gossiper.instance().addApplicationState(StorageService.nodeId_, new ApplicationState(token.toString()));
    }
    
    /*
     * This method removes the state associated with this endpoint
     * from the TokenMetadata instance.
     * 
     *  param@ endpoint remove the token state associated with this 
     *         endpoint.
     */
    public void removeTokenState(EndPoint endpoint) 
    {
        tokenMetadata_.remove(endpoint);
        /* Remove the state from the Gossiper */
        Gossiper.instance().removeFromMembership(endpoint);
    }
    
    /*
     * This method is invoked by the Loader process to force the
     * node to move from its current position on the token ring, to
     * a position to be determined based on the keys. This will help
     * all nodes to start off perfectly load balanced. The array passed
     * in is evaluated as follows by the loader process:
     * If there are 10 keys in the system and a totality of 5 nodes
     * then each node needs to have 2 keys i.e the array is made up
     * of every 2nd key in the total list of keys.
    */
    public void relocate(String[] keys) throws IOException
    {
    	if ( keys.length > 0 )
    	{
	        isLoadState_ = true;
	        BigInteger token = tokenMetadata_.getToken(StorageService.tcpAddr_);
	        Map<BigInteger, EndPoint> tokenToEndPointMap = tokenMetadata_.cloneTokenEndPointMap();
	        BigInteger[] tokens = tokenToEndPointMap.keySet().toArray( new BigInteger[0] );
	        Arrays.sort(tokens);
	        int index = Arrays.binarySearch(tokens, token) * (keys.length/tokens.length);
	        BigInteger newToken = hash( keys[index] );
	        /* update the token */
	        updateToken(newToken);
    	}
    }

    /*
     * This is used to indicate that this node is done
     * with the loading of data.
    */
    public void resetLoadState()
    {
        isLoadState_ = false;
    }
    
    /*
     * 
    */
    private void doBootstrap(String nodes)
    {
        String[] allNodes = nodes.split(":");
        EndPoint[] endpoints = new EndPoint[allNodes.length];
        BigInteger[] tokens = new BigInteger[allNodes.length];
        
        for ( int i = 0; i < allNodes.length; ++i )
        {
            endpoints[i] = new EndPoint( allNodes[i].trim(), DatabaseDescriptor.getStoragePort() );
            tokens[i] = tokenMetadata_.getToken(endpoints[i]);
        }
        
        /* Start the bootstrap algorithm */
        bootStrapper_.submit( new BootStrapper(endpoints, tokens) );
    }

    /*
     * Starts the bootstrap operations for the specified endpoint.
     * The name of this method is however a misnomer since it does
     * handoff of data to the specified node when it has crashed
     * and come back up, marked as alive after a network partition
     * and also when it joins the ring either as an old node being
     * relocated or as a brand new node.
    */
    private void doBootstrap(EndPoint endpoint, BootstrapMode mode)
    {
        switch ( mode )
        {
            case FULL:
                BigInteger token = tokenMetadata_.getToken(endpoint);
                bootStrapper_.submit( new BootStrapper(new EndPoint[]{endpoint}, new BigInteger[]{token}) );
                break;

            case HINT:
                /* Deliver the hinted data to this endpoint. */
                HintedHandOffManager.instance().deliverHints(endpoint);
                break;

            default:
                break;
        }
    }

    /* These methods belong to the MBean interface */

    public long getRequestHandled()
    {
        return sampler_.count();
    }

    public String getToken(EndPoint ep)
    {
        EndPoint ep2 = new EndPoint(ep.getHost(), DatabaseDescriptor.getStoragePort());
        BigInteger token = tokenMetadata_.getToken(ep2);
        return ( token == null ) ? BigInteger.ZERO.toString() : token.toString();
    }

    public String getToken()
    {
        return tokenMetadata_.getToken(StorageService.tcpAddr_).toString();
    }
    
    public void updateToken(String token)
    {
        try
        {
            updateToken(new BigInteger(token));
        }
        catch ( IOException ex )
        {
            logger_.debug(LogUtil.throwableToString(ex));
        }
    }

    public String getLiveNodes()
    {
        return stringify(Gossiper.instance().getLiveMembers());
    }

    public String getUnreachableNodes()
    {
        return stringify(Gossiper.instance().getUnreachableMembers());
    }

    /* Helper for the MBean interface */
    private String stringify(Set<EndPoint> eps)
    {
        StringBuilder sb = new StringBuilder("");
        for (EndPoint ep : eps)
        {
            sb.append(ep);
            sb.append(" ");
        }
        return sb.toString();
    }

    public void loadAll(String nodes)
    {        
        // Gossiper.instance().addApplicationState(StorageService.loadAll_, new ApplicationState(nodes));
        doBootstrap(nodes);
    }
    
    public String getAppropriateToken(int count)
    {
        BigInteger token = BootstrapAndLbHelper.getTokenBasedOnPrimaryCount(count);
        return token.toString();
    }

    /* End of MBean interface methods */

    /*
     * This method returns the predecessor of the endpoint ep on the identifier
     * space.
     */
    EndPoint getPredecessor(EndPoint ep)
    {
        BigInteger token = tokenMetadata_.getToken(ep);
        Map<BigInteger, EndPoint> tokenToEndPointMap = tokenMetadata_.cloneTokenEndPointMap();
        List<BigInteger> tokens = new ArrayList<BigInteger>(tokenToEndPointMap.keySet());
        Collections.sort(tokens);
        int index = Collections.binarySearch(tokens, token);
        EndPoint predecessor = (index == 0) ? tokenToEndPointMap.get(tokens
                .get(tokens.size() - 1)) : tokenToEndPointMap.get(tokens
                .get(--index));
        return predecessor;
    }

    /*
     * This method returns the successor of the endpoint ep on the identifier
     * space.
     */
    EndPoint getSuccessor(EndPoint ep)
    {
        BigInteger token = tokenMetadata_.getToken(ep);
        Map<BigInteger, EndPoint> tokenToEndPointMap = tokenMetadata_.cloneTokenEndPointMap();
        List<BigInteger> tokens = new ArrayList<BigInteger>(tokenToEndPointMap.keySet());
        Collections.sort(tokens);
        int index = Collections.binarySearch(tokens, token);
        EndPoint predecessor = (index == (tokens.size() - 1)) ? tokenToEndPointMap
                .get(tokens.get(0))
                : tokenToEndPointMap.get(tokens.get(++index));
        return predecessor;
    }

    /**
     * This method returns the range handled by this node.
     */
    Range getMyRange()
    {
        BigInteger myToken = tokenMetadata_.getToken(StorageService.tcpAddr_);
        Map<BigInteger, EndPoint> tokenToEndPointMap = tokenMetadata_.cloneTokenEndPointMap();
        List<BigInteger> allTokens = new ArrayList<BigInteger>(tokenToEndPointMap.keySet());
        Collections.sort(allTokens);
        int index = Collections.binarySearch(allTokens, myToken);
        /* Calculate the lhs for the range */
        BigInteger lhs = (index == 0) ? allTokens.get(allTokens.size() - 1) : allTokens.get( index - 1);
        return new Range( lhs, myToken );
    }
    
    /**
     * Get the primary range for the specified endpoint.
     * @param ep endpoint we are interested in.
     * @return range for the specified endpoint.
     */
    Range getPrimaryRangeForEndPoint(EndPoint ep)
    {
        BigInteger right = tokenMetadata_.getToken(ep);
        EndPoint predecessor = getPredecessor(ep);
        BigInteger left = tokenMetadata_.getToken(predecessor);
        return new Range(left, right);
    }
    
    /**
     * Get all ranges an endpoint is responsible for.
     * @param ep endpoint we are interested in.
     * @return ranges for the specified endpoint.
     */
    List<Range> getRangesForEndPoint(EndPoint ep)
    {
        List<Range> ranges = new ArrayList<Range>();
        ranges.add( getPrimaryRangeForEndPoint(ep) );
        
        EndPoint predecessor = ep;
        int count = DatabaseDescriptor.getReplicationFactor() - 1;
        for ( int i = 0; i < count; ++i )
        {
            predecessor = getPredecessor(predecessor);
            ranges.add( getPrimaryRangeForEndPoint(predecessor) );
        }
        
        return ranges;
    }
    
    /**
     * Get all ranges that span the ring.
    */
    Range[] getAllRanges(Set<BigInteger> tokens)
    {
        List<Range> ranges = new ArrayList<Range>();
        List<BigInteger> allTokens = new ArrayList<BigInteger>(tokens);
        Collections.sort(allTokens);
        int size = allTokens.size();
        for ( int i = 1; i < size; ++i )
        {
            Range range = new Range( allTokens.get(i - 1), allTokens.get(i) );
            ranges.add(range);
        }
        Range range = new Range( allTokens.get(size - 1), allTokens.get(0) );
        ranges.add(range);
        return ranges.toArray( new Range[0] );
    }

    /**
     * This method returns the endpoint that is responsible for storing the
     * specified key.
     *
     * param @ key - key for which we need to find the endpoint
     * return value - the endpoint responsible for this key
     */
    public EndPoint getPrimary(String key)
    {
        EndPoint endpoint = StorageService.tcpAddr_;
        BigInteger token = hash(key);
        Map<BigInteger, EndPoint> tokenToEndPointMap = tokenMetadata_.cloneTokenEndPointMap();
        List<BigInteger> tokens = new ArrayList<BigInteger>(tokenToEndPointMap.keySet());
        if (!tokens.isEmpty())
        {
            Collections.sort(tokens);
            int index = Collections.binarySearch(tokens, token);
            if (index >= 0)
            {
                /*
                 * retrieve the endpoint based on the token at this index in the
                 * tokens list
                 */
                endpoint = tokenToEndPointMap.get(tokens.get(index));
            }
            else
            {
                index = (index + 1) * (-1);
                if (index < tokens.size())
                    endpoint = tokenToEndPointMap.get(tokens.get(index));
                else
                    endpoint = tokenToEndPointMap.get(tokens.get(0));
            }
        }
        return endpoint;
    }

    /**
     * This method determines whether the local endpoint is the
     * primary for the given key.
     * @param key
     * @return true if the local endpoint is the primary replica.
    */
    public boolean isPrimary(String key)
    {
        EndPoint endpoint = getPrimary(key);
        return StorageService.tcpAddr_.equals(endpoint);
    }
    
    /**
     * This method determines whether the target endpoint is the
     * primary for the given key.
     * @param key
     * @param target the target enpoint 
     * @return true if the local endpoint is the primary replica.
    */
    public boolean isPrimary(String key, EndPoint target)
    {
        EndPoint endpoint = getPrimary(key);
        return target.equals(endpoint);
    }
    
    /**
     * This method determines whether the local endpoint is the
     * seondary replica for the given key.
     * @param key
     * @return true if the local endpoint is the secondary replica.
     */
    public boolean isSecondary(String key)
    {
        EndPoint[] topN = getNStorageEndPoint(key);
        if ( topN.length < DatabaseDescriptor.getReplicationFactor() )
            return false;
        return topN[1].equals(StorageService.tcpAddr_);
    }
    
    /**
     * This method determines whether the local endpoint is the
     * seondary replica for the given key.
     * @param key
     * @return true if the local endpoint is the tertiary replica.
     */
    public boolean isTertiary(String key)
    {
        EndPoint[] topN = getNStorageEndPoint(key);
        if ( topN.length < DatabaseDescriptor.getReplicationFactor() )
            return false;
        return topN[2].equals(StorageService.tcpAddr_);
    }
    
    /**
     * This method determines if the local endpoint is
     * in the topN of N nodes passed in.
    */
    public boolean isInTopN(String key)
    {
    	EndPoint[] topN = getNStorageEndPoint(key);
        return Arrays.asList(topN).contains( StorageService.tcpAddr_ );
    }
    
    /**
     * This method returns the N endpoints that are responsible for storing the
     * specified key i.e for replication.
     *
     * param @ key - key for which we need to find the endpoint return value -
     * the endpoint responsible for this key
     */
    public EndPoint[] getNStorageEndPoint(String key)
    {
        BigInteger token = hash(key);
        return nodePicker_.getStorageEndPoints(token);
    }
    
    
    /**
     * This method attempts to return N endpoints that are responsible for storing the
     * specified key i.e for replication.
     *
     * param @ key - key for which we need to find the endpoint return value -
     * the endpoint responsible for this key
     */
    public List<EndPoint> getNLiveStorageEndPoint(String key)
    {
    	List<EndPoint> liveEps = new ArrayList<EndPoint>();
    	EndPoint[] endpoints = getNStorageEndPoint(key);
    	
    	for ( EndPoint endpoint : endpoints )
    	{
    		if ( FailureDetector.instance().isAlive(endpoint) )
    			liveEps.add(endpoint);
    	}
    	
    	return liveEps;
    }

    /**
     * This method returns the N endpoints that are responsible for storing the
     * specified key i.e for replication.
     *
     * param @ key - key for which we need to find the endpoint return value -
     * the endpoint responsible for this key
     */
    public Map<EndPoint, EndPoint> getNStorageEndPointMap(String key)
    {
        BigInteger token = hash(key);
        return nodePicker_.getHintedStorageEndPoints(token);
    }

    /**
     * This method returns the N endpoints that are responsible for storing the
     * specified key i.e for replication. But it makes sure that the N endpoints
     * that are returned are live as reported by the FD. It returns the hint information
     * if some nodes in the top N are not live.
     *
     * param @ key - key for which we need to find the endpoint return value -
     * the endpoint responsible for this key
     */
    public Map<EndPoint, EndPoint> getNHintedStorageEndPoint(String key)
    {
        BigInteger token = hash(key);
        return nodePicker_.getHintedStorageEndPoints(token);
    }

    /**
     * This method returns the N endpoints that are responsible for storing the
     * specified token i.e for replication.
     *
     * param @ token - position on the ring
     */
    public EndPoint[] getNStorageEndPoint(BigInteger token)
    {
        return nodePicker_.getStorageEndPoints(token);
    }
    
    /**
     * This method returns the N endpoints that are responsible for storing the
     * specified token i.e for replication and are based on the token to endpoint 
     * mapping that is passed in.
     *
     * param @ token - position on the ring
     * param @ tokens - w/o the following tokens in the token list
     */
    protected EndPoint[] getNStorageEndPoint(BigInteger token, Map<BigInteger, EndPoint> tokenToEndPointMap)
    {
        return nodePicker_.getStorageEndPoints(token, tokenToEndPointMap);
    }

    /**
     * This method returns the N endpoints that are responsible for storing the
     * specified key i.e for replication. But it makes sure that the N endpoints
     * that are returned are live as reported by the FD. It returns the hint information
     * if some nodes in the top N are not live.
     *
     * param @ token - position on the ring
     */
    public Map<EndPoint, EndPoint> getNHintedStorageEndPoint(BigInteger token)
    {
        return nodePicker_.getHintedStorageEndPoints(token);
    }
    
    /**
     * This function finds the most suitable endpoint given a key.
     * It checks for loclity and alive test.
     */
	protected EndPoint findSuitableEndPoint(String key) throws IOException
	{
		EndPoint[] endpoints = getNStorageEndPoint(key);
		for(EndPoint endPoint: endpoints)
		{
			if(endPoint.equals(StorageService.getLocalStorageEndPoint()))
			{
				return endPoint;
			}
		}
		int j = 0;
		for ( ; j < endpoints.length; ++j )
		{
			if ( StorageService.instance().isInSameDataCenter(endpoints[j]) && FailureDetector.instance().isAlive(endpoints[j]) )
			{
				logger_.debug("EndPoint " + endpoints[j] + " is in the same data center as local storage endpoint.");
				return endpoints[j];
			}
		}
		// We have tried to be really nice but looks like there are no servers 
		// in the local data center that are alive and can service this request so 
		// just send it to the first alive guy and see if we get anything.
		j = 0;
		for ( ; j < endpoints.length; ++j )
		{
			if ( FailureDetector.instance().isAlive(endpoints[j]) )
			{
				logger_.debug("EndPoint " + endpoints[j] + " is alive so get data from it.");
				return endpoints[j];
			}
		}
		return null;
		
	}
}

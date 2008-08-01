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

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.facebook.infrastructure.concurrent.DebuggableThreadPoolExecutor;
import com.facebook.infrastructure.concurrent.ThreadFactoryImpl;
import com.facebook.infrastructure.conf.DatabaseDescriptor;
import com.facebook.infrastructure.db.FileUtils;
import com.facebook.infrastructure.db.SystemTable;
import com.facebook.infrastructure.db.Table;
import com.facebook.infrastructure.gms.*;
import com.facebook.infrastructure.io.DataInputBuffer;
import com.facebook.infrastructure.io.DataOutputBuffer;
import com.facebook.infrastructure.io.ICompactSerializer;
import com.facebook.infrastructure.io.ICompactSerializer2;
import com.facebook.infrastructure.io.IFileReader;
import com.facebook.infrastructure.io.IFileWriter;
import com.facebook.infrastructure.io.SequenceFile;
import com.facebook.infrastructure.utils.BitSet;
import com.facebook.infrastructure.utils.BloomFilter;
import com.facebook.infrastructure.utils.CassandraUtilities;
import com.facebook.infrastructure.utils.LogUtil;

/*
 * This class is used to get approximate count for the
 * number of distinct keys processed in a given time
 * interval. This information is then deemed the load
 * information for the node and is gossiped around in
 * order to make good load balancing decisions.
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public final class RequestCountSampler
{
    private static Logger logger_ = Logger.getLogger(RequestCountSampler.class);
    /* Gossip load after every 5 mins. */
    private static final long threshold_ = 5 * 60 * 1000L;
    /* This thread pool is used to persist the cardinality to disk. */
    private static ExecutorService cFlusher_ = new DebuggableThreadPoolExecutor(1,
            1,
            Integer.MAX_VALUE,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(),
            new ThreadFactoryImpl("CARDINALITY-FLUSHER")
            );
    final static String loadInfo_= "LOAD-INFORMATION";

    public static void shutdown()
    {
    	cFlusher_.shutdownNow();
    }
    
    public static class Cardinality
    {
        private final static int max_ = 64*1024*1024;
        private final static int bitsPerElement_ = 4;
        private static ICompactSerializer<Cardinality> serializer_;

        static
        {
            serializer_ = new CardinalitySerializer();
        }

        public static ICompactSerializer<Cardinality> serializer()
        {
            return serializer_;
        }
        
        public static Cardinality alter(Cardinality counter)
        {
            Cardinality alteredCounter = new Cardinality();
            alteredCounter.bloomFilter_.merge(counter.bloomFilter_);
            alteredCounter.count_ = counter.count_;
            return alteredCounter;
        }

        /* Bloom keeps track of the elements processed so far. */
        private BloomFilter.CountingBloomFilter bloomFilter_;
        /* Keeps track of the number of elements processed so far. */
        private int count_ = 0;

        public Cardinality()
        {
            bloomFilter_ = new BloomFilter.CountingBloomFilter(max_, bitsPerElement_);
        }

        public Cardinality(BloomFilter.CountingBloomFilter cbf)
        {
            bloomFilter_ = cbf;
        }

        public Cardinality(BloomFilter.CountingBloomFilter cbf, int count)
        {
            bloomFilter_ = cbf;
            count_ = count;
        }

        synchronized Cardinality cloneMe()
        {
            BloomFilter.CountingBloomFilter cbf = bloomFilter_.cloneMe();
            return new Cardinality(cbf, count_);
        }

        synchronized Cardinality shallowCopy()
        {
            BloomFilter.CountingBloomFilter cbf = bloomFilter_.shallowCopy();
            return new Cardinality(cbf, count_);
        }

        synchronized void add(String key)
        {
            if ( !bloomFilter_.isPresent(key) )
            {
                bloomFilter_.add(key);
                ++count_;
            }
        }

        synchronized void add(RequestCountSampler.Cardinality cardinality)
        {
            bloomFilter_.merge(cardinality.bloomFilter_);
            count_ += cardinality.count_;
        }

        synchronized void remove(String key)
        {
            boolean bVal = bloomFilter_.delete(key);
            if ( bVal )
                --count_;
        }

        public int count()
        {
            return count_;
        }
    }

    public static class CardinalitySerializer implements ICompactSerializer<Cardinality>
    {
        /*
         * The following methods are used for compact representation
         * of BloomFilter. This is essential, since we want to determine
         * the size of the serialized Bloom Filter blob before it is
         * populated armed with the knowledge of how many elements are
         * going to reside in it.
         */

        public void serialize(Cardinality cardinality, DataOutput dos)
                throws IOException
        {
            /* write out the CountinBloomFilter */
            BloomFilter.CountingBloomFilter.serializer().serialize(cardinality.bloomFilter_, dos);
            /* write the count of the # of keys */
            dos.writeInt(cardinality.count_);
        }

        public RequestCountSampler.Cardinality deserialize(DataInputStream dis) throws IOException
        {
            /* read the BloomFilter */
            BloomFilter.CountingBloomFilter cbf = BloomFilter.CountingBloomFilter.serializer().deserialize(dis);
            /* read the count */
            int count = dis.readInt();
            return new Cardinality(cbf, count);
        }
    }

    private Cardinality counter_;
    private CardinalityFlusher cardinalityFlusher_;
    private IFileWriter writer_;
    private long startTime_ = System.currentTimeMillis();

    private class CardinalityFlusher implements Runnable
    {
        private Cardinality counterToPersist_;

        void counter(Cardinality counter)
        {
            counterToPersist_ = counter;
        }

        public void run()
        {
            /* persist "Cardinality" to disk */
            String table = DatabaseDescriptor.getTables().get(0);

            try
            {
                if ( writer_ == null )
                {
                    String file = getFileName();
                    writer_ = SequenceFile.writer(file);
                }
                writer_.seek(0L);
                DataOutputBuffer bufOut = new DataOutputBuffer();
                Cardinality.serializer().serialize(counterToPersist_, bufOut);
                logger_.debug("Persisting the cardinality to disk ...");
                writer_.append(table, bufOut);
            }
            catch ( IOException ex )
            {
                logger_.warn(LogUtil.throwableToString(ex) );
            }
        }
    }

    RequestCountSampler() throws IOException
    {
        String file = getFileName();
        load();
        cardinalityFlusher_ = new CardinalityFlusher();
        diseminateLoadInfo();
    }

    private String getFileName()
    {
        String table = DatabaseDescriptor.getTables().get(0);
        return DatabaseDescriptor.getMetadataDirectory() + System.getProperty("file.separator") + table + "-Cardinality.db";
    }

    private void load() throws IOException
    {
        String file = getFileName();
        File f = new File(file);

        if ( f.exists() )
        {
            IFileReader reader = SequenceFile.reader(file);
            DataOutputBuffer bufOut = new DataOutputBuffer();
            DataInputBuffer bufIn = new DataInputBuffer();

            while ( !reader.isEOF() )
            {
                /* Read the metadata info. */
                reader.next(bufOut);
                bufIn.reset(bufOut.getData(), bufOut.getLength());

                /* The key is the table name */
                String key = bufIn.readUTF();
                /* read the size of the data we ignore this value */
                bufIn.readInt();
                counter_ = Cardinality.serializer().deserialize(bufIn);
                if ( DatabaseDescriptor.isAlterCardinality() )
                {
                    counter_ = Cardinality.alter(counter_);
                }
                break;
            }
        }
        else
        {
            counter_ = new Cardinality();
        }
    }

    /*
     * Add the key passed in to the Counting Bloom Filter
     * instance of the Cardinality class.
    */
    void sample(String key)
    {
        counter_.add(key);
        long current = System.currentTimeMillis();
        /* Calculate the request count and gossip it every "threshold" period */
        if ( (current - startTime_) >= RequestCountSampler.threshold_ )
        {
            startTime_ = System.currentTimeMillis();
            diseminateLoadInfo();
            /* Clone the cardinality class and submit it to the flusher. */
            Cardinality counter = counter_.shallowCopy();
            cardinalityFlusher_.counter(counter);
            RequestCountSampler.cFlusher_.execute(cardinalityFlusher_);
        }
    }
    
    /**
     * Diseminate the count information via Gossip.
    */
    void diseminateLoadInfo()
    {                
        long diskSpace = FileUtils.getUsedDiskSpace();                
        LoadInfo lInfo = new LoadInfo(count(), diskSpace);
        logger_.debug("Disseminating load info ...");
        Gossiper.instance().addApplicationState(RequestCountSampler.loadInfo_, new ApplicationState(lInfo.toString()));
    }

    /*
     * Delete the key passed in from the Counting Bloom Filter
     * instance of the Cardinality class.
    */
    void delete(String key)
    {
        counter_.remove(key);
    }

    /*
     * Loop through the bitset and determine the number
     * of bits that have been flipped to one. This tells
     * us the approximate number of keys that have been
     * processed. Approximate because we could have two
     * distinct keys whose intValue() could be the same.
     * I need to determine the probability of such an
     * occurance.
    */
    int count()
    {
        return counter_.count();
    }

    /**
     * This method is called to update the cardinality of the
     * of this node on load balance/bootstrap operation.
    */
    void add(RequestCountSampler.Cardinality cardinality)
    {
        counter_.add(cardinality);
    }

    public static void main(String[] args) throws Throwable
    {
        RequestCountSampler sampler = new RequestCountSampler();
        for ( int i = 0; i < 1024; ++i )
        {
            sampler.sample(Integer.valueOf(i).toString());
        }
        sampler.delete(Integer.valueOf(2).toString());
        System.out.println(sampler.count());
    }
}

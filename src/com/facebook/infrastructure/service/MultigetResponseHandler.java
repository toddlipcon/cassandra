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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

import com.facebook.infrastructure.net.*;
import com.facebook.infrastructure.conf.DatabaseDescriptor;
import org.apache.log4j.Logger;

public class MultigetResponseHandler<T>
{
    private static Logger logger_ = Logger.getLogger(MultigetResponseHandler.class);

    private final BlockingQueue<PerKeyHandler> doneHandlers_ = new LinkedBlockingQueue<PerKeyHandler>();

    private final Map<String, PerKeyHandler> handlers_ = new HashMap<String, PerKeyHandler>();
    
    private final Map<String, T> results_ = new HashMap<String, T>();

    private IResponseResolver<T> responseResolver_;

    private final Set<String> needsRepair_ = new HashSet<String>();

    public MultigetResponseHandler(IResponseResolver<T> responseResolver)
    {
        responseResolver_ = responseResolver;
    }

    public IAsyncCallback createCallback(final String key)
    {
        final PerKeyHandler pkh = new PerKeyHandler(key);
        handlers_.put(key, pkh);
        return pkh;
    }

    public Map<String, T> get() throws TimeoutException
    {
        logger_.debug("----- MRH.get()");

        long startTime = System.currentTimeMillis();
        long endTime = System.currentTimeMillis() + DatabaseDescriptor.getRpcTimeout();
        long remainingTime = 1;

        int numDone = 0;
        int totalNum = handlers_.size();

        while (numDone < totalNum && remainingTime >= 0)
        {
            remainingTime = endTime - System.currentTimeMillis();
            logger_.debug("Remaining time: " + String.valueOf(remainingTime));

            PerKeyHandler doneHandler;
            try
            {
                doneHandler = doneHandlers_.poll(remainingTime, TimeUnit.MILLISECONDS);
                if (doneHandler == null)
                    break; // timed out
            }
            catch (InterruptedException ie)
            {
                continue; // keep trying if we have more time
            }

            logger_.debug("Another response handler finished: " + doneHandler.key_);
            numDone++;

            // We have enough data to resolve this key
            try
            {
                List<Message> messages = doneHandler.responses_;
                for (Message m : messages)
                {
                    MessagingService.removeRegisteredCallback( m.getMessageId() );
                }

                T resolved = responseResolver_.resolve( doneHandler.responses_ );
                logger_.debug("Resolved: " + doneHandler.key_);

                results_.put( doneHandler.key_, resolved );
            }
            catch ( DigestMismatchException dme ) {
                logger_.debug("Needs repair: " + doneHandler.key_);

                needsRepair_.add( doneHandler.key_ );
            }
        }

        return results_;
    }

    private class PerKeyHandler  implements IAsyncCallback
    {
        private String key_;

        private Lock lock_ = new ReentrantLock();
        private ArrayList<Message> responses_ = new ArrayList<Message>();
        
        /* We've received a majority */
        private AtomicBoolean done_ = new AtomicBoolean(false);

        private int responseCount_ = 0;

        public PerKeyHandler(String key)
        {
            key_ = key;
        }

        public void response(Message message)
        {
            lock_.lock();
            try
            {
                int majority = (responseCount_ >> 1) + 1;
                if ( !done_.get() )
                {
                    responses_.add( message );
                    if ( responses_.size() >= majority && responseResolver_.isDataPresent(responses_))
                    {
                        done_.set(true);
                        while (true)
                        {
                            try
                            {
                                doneHandlers_.put(this);
                            } catch (InterruptedException ie)
                            {
                                continue;
                            }
                            break;
                        }
                    }
                }
            }
            finally
            {
                lock_.unlock();
            }
        }
    }
}

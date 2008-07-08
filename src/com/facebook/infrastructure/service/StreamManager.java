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

import java.io.File;
import java.io.IOException;
import java.util.*;
import org.apache.log4j.Logger;

import com.facebook.infrastructure.conf.DatabaseDescriptor;
import com.facebook.infrastructure.db.FileUtils;
import com.facebook.infrastructure.io.DataInputBuffer;
import com.facebook.infrastructure.net.EndPoint;
import com.facebook.infrastructure.net.IVerbHandler;
import com.facebook.infrastructure.net.Message;
import com.facebook.infrastructure.net.MessagingService;
import com.facebook.infrastructure.net.io.StreamContextManager;
import com.facebook.infrastructure.service.StorageService.BootstrapInitiateDoneVerbHandler;
import com.facebook.infrastructure.utils.LogUtil;

/*
 * This class manages the streaming of multiple files 
 * one after the other. 
*/
class StreamManager
{   
    private static Logger logger_ = Logger.getLogger( StreamManager.class );
    
    public static class BootstrapTerminateVerbHandler implements IVerbHandler
    {
        private static Logger logger_ = Logger.getLogger( BootstrapInitiateDoneVerbHandler.class );

        public void doVerb(Message message)
        {
            byte[] body = (byte[])message.getMessageBody()[0];
            DataInputBuffer bufIn = new DataInputBuffer();
            bufIn.reset(body, body.length);

            try
            {
                StreamContextManager.StreamStatusMessage streamStatusMessage = StreamContextManager.StreamStatusMessage.serializer().deserialize(bufIn);
                StreamContextManager.StreamStatus streamStatus = streamStatusMessage.getStreamStatus();
                                                               
                switch( streamStatus.getAction() )
                {
                    case DELETE:                              
                        StreamManager.instance(message.getFrom()).finish(streamStatus.getFile());
                        break;

                    case STREAM:
                        logger_.debug("Need to re-stream file " + streamStatus.getFile());
                        StreamManager.instance(message.getFrom()).repeat();
                        break;

                    default:
                        break;
                }
            }
            catch ( IOException ex )
            {
                logger_.info(LogUtil.throwableToString(ex));
            }
        }
    }
    
    private static Map<EndPoint, StreamManager> streamManagers_ = new HashMap<EndPoint, StreamManager>();
    
    public static StreamManager instance(EndPoint to)
    {
        StreamManager streamManager = streamManagers_.get(to);
        if ( streamManager == null )
        {
            streamManager = new StreamManager(to);
            streamManagers_.put(to, streamManager);
        }
        return streamManager;
    }
    
    private List<File> filesToStream_ = new ArrayList<File>();
    private EndPoint to_;
    private long totalBytesToStream_ = 0L;
    
    private StreamManager(EndPoint to)
    {
        to_ = to;        
    }
    
    void addFilesToStream(StreamContextManager.StreamContext[] streamContexts)
    {
        for ( StreamContextManager.StreamContext streamContext : streamContexts )
        {
            logger_.debug("Adding file " + streamContext.getTargetFile() + " to be streamed.");
            filesToStream_.add( new File( streamContext.getTargetFile() ) );
            totalBytesToStream_ += streamContext.getExpectedBytes();
        }
    }
    
    void start()
    {
        if ( filesToStream_.size() > 0 )
        {
            File file = filesToStream_.get(0);
            logger_.debug("Streaming file " + file + " ...");
            MessagingService.getMessagingInstance().stream(file.getAbsolutePath(), 0L, file.length(), StorageService.getLocalStorageEndPoint(), to_);
        }
    }
    
    void repeat()
    {
        if ( filesToStream_.size() > 0 )
            start();
    }
    
    void finish(String file) throws IOException
    {
        File f = new File(file);
        logger_.debug("Deleting file " + file + " after streaming " + f.length() + "/" + totalBytesToStream_ + " bytes.");
        FileUtils.delete(file);
        filesToStream_.remove(0);
        if ( filesToStream_.size() > 0 )
            start();
        else
        {
            synchronized(this)
            {
                logger_.debug("Signalling that streaming is done for " + to_);
                notifyAll();
            }
        }
    }
    
    public synchronized void waitForStreamCompletion()
    {
        try
        {
            wait();
        }
        catch(InterruptedException ex)
        {
            logger_.warn(LogUtil.throwableToString(ex));
        }
    }
}

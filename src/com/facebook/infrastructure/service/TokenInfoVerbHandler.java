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

import org.apache.log4j.Logger;

import com.facebook.infrastructure.net.EndPoint;
import com.facebook.infrastructure.net.IVerbHandler;
import com.facebook.infrastructure.net.Message;
import com.facebook.infrastructure.net.MessagingService;
/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class TokenInfoVerbHandler implements IVerbHandler
{
    private static Logger logger_ = Logger.getLogger( TokenInfoVerbHandler.class );
    
    public void doVerb(Message message)
    {
        EndPoint from = message.getFrom();
        logger_.info("Received a token download request from " + from);
        /* Get the token metadata map and send it over. */
        Message response = message.getReply(StorageService.getLocalStorageEndPoint(), new Object[]{StorageService.instance().getTokenMetadata()});
        logger_.info("Sending the token download response to " + from);
        MessagingService.getMessagingInstance().sendOneWay(response, from);
    }
}

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

import java.math.BigInteger;
import java.util.*;

import org.apache.log4j.Logger;
import com.facebook.infrastructure.net.EndPoint;
import com.facebook.infrastructure.conf.DatabaseDescriptor;
import com.facebook.infrastructure.db.*;
import com.facebook.infrastructure.gms.*;
import java.net.UnknownHostException;

/*
 * This class returns the nodes responsible for a given
 * key but does not respect rack awareness. Basically
 * returns the 3 nodes that lie right next to each other
 * on the ring.
 */
class RackUnawareStrategy extends AbstractStrategy
{        
    public EndPoint[] getStorageEndPoints(BigInteger token, TokenMetadata tmd)
    {
        int N = DatabaseDescriptor.getReplicationFactor();

        int foundCount = 0;
        List<EndPoint> list = new ArrayList<EndPoint>();

        TokenMetadata.RingIterator iter = tmd.getRingIterator(token);
        while( iter.hasNext() )
        {
            EndPoint ep = iter.next().getValue();
            if( ! list.contains( ep ) ) // TODO is this check necessary?
            {
                list.add( ep );
                if (++foundCount == N)
                    break;
            }
        }
        retrofitPorts(list);
        return list.toArray(new EndPoint[0]);
    }
}


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
import java.net.UnknownHostException;

import com.facebook.infrastructure.net.EndPoint;
import com.facebook.infrastructure.conf.DatabaseDescriptor;
import com.facebook.infrastructure.db.*;
import com.facebook.infrastructure.gms.*;
import com.facebook.infrastructure.utils.*;
import org.apache.log4j.Logger;

/*
 * This class returns the nodes responsible for a given
 * key but does respects rack awareness. It makes a best
 * effort to get a node from a different data center and
 * a node in a different rack in the same datacenter as
 * the primary.
 */
class RackAwareStrategy extends AbstractStrategy
{
    private static Logger logger_ = Logger.getLogger(RackAwareStrategy.class);
    private IEndPointSnitch endPointSnitch_;


    public RackAwareStrategy(IEndPointSnitch snitch)
    {
        endPointSnitch_ = snitch;
    }

    public EndPoint[] getStorageEndPoints(BigInteger token, TokenMetadata tmd)
    {
        int N = DatabaseDescriptor.getReplicationFactor();

        List<EndPoint> list = new ArrayList<EndPoint>();

        Iterator<Map.Entry<BigInteger, EndPoint>> iter = tmd.getRingIterator(token);
        EndPoint primaryEp = iter.next().getValue();
        list.add(primaryEp);

        int foundCount = 1;
        boolean bDataCenter = false;
        boolean bOtherRack = false;

        while ( iter.hasNext() && !bDataCenter && !bOtherRack && foundCount < N)
        {
            Map.Entry<BigInteger, EndPoint> entry = iter.next();
            EndPoint thisEp = entry.getValue();

            try {
                // First try to find one in a different data center
                if( !endPointSnitch_.isInSameDataCenter(primaryEp, thisEp) )
                {
                    // If we have already found something in a diff datacenter no need to find another
                    if( !bDataCenter )
                    {
                        list.add( thisEp );
                        bDataCenter = true;
                        foundCount++;
                    }
                    continue;
                }
                    
                // Now  try to find one on a different rack
                if( !endPointSnitch_.isOnSameRack(primaryEp, thisEp) &&
                    endPointSnitch_.isInSameDataCenter(primaryEp, thisEp) )
                {
                    // If we have already found something in a diff rack no need to find another
                    if( !bOtherRack )
                    {
                        list.add( thisEp );
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
            
        // If we haven't yet found enough nodes, start iterating again at the beginning of the ring
        // taking any token regardless of location
        if (foundCount < N)
        {
            iter = tmd.getRingIterator(token);
            while ( iter.hasNext() && foundCount < N )
            {
                Map.Entry<BigInteger, EndPoint> entry = iter.next();
                EndPoint thisEp = entry.getValue();

                if( ! list.contains( thisEp ) ) {
                    list.add( thisEp );
                    foundCount++;
                }
            }
        }

        if (foundCount < N)
        {
            logger_.warn("Could not find enough RackAware endpoints for token " + String.valueOf(token));
        }

        retrofitPorts(list);
        return list.toArray(new EndPoint[0]);
    }
        
    public EndPoint[] getStorageEndPoints(BigInteger token, Map<BigInteger, EndPoint> tokenToEndPointMap)
    {
        throw new UnsupportedOperationException("This operation is not currently supported");
    }
}

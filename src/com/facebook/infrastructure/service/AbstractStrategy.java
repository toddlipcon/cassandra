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
 * This class contains a helper method that will be used by
 * all abstractions that implement the IReplicaPlacementStrategy
 * interface.
*/
abstract class AbstractStrategy implements IReplicaPlacementStrategy
{
    private static Logger logger_ = Logger.getLogger(AbstractStrategy.class);

    /*
     * This method changes the ports of the endpoints from
     * the control port to the storage ports.
     *
     * TODO: this mutability is bad! There might be a serious race here now
     * that there aren't clones when reading TokenMetadata
    */
    protected void retrofitPorts(List<EndPoint> eps)
    {
        for ( EndPoint ep : eps )
        {
            ep.setPort(DatabaseDescriptor.getStoragePort());
        }
    }

    protected EndPoint getNextAvailableEndPoint(EndPoint startPoint, TokenMetadata tmd,
                                                List<EndPoint> topN, List<EndPoint> liveNodes)
    {
        EndPoint endPoint = null;

        TokenMetadata.RingIterator iter = tmd.getRingIterator(startPoint);
        while( iter.hasNext() )
        {
            EndPoint tmpEndPoint = iter.next().getValue();
            if(FailureDetector.instance().isAlive(tmpEndPoint) &&
               !topN.contains(tmpEndPoint) &&
               !liveNodes.contains(tmpEndPoint))
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
    public Map<EndPoint, EndPoint> getHintedStorageEndPoints(BigInteger token, TokenMetadata tmd)
    {
        List<EndPoint> liveList = new ArrayList<EndPoint>();
        Map<EndPoint, EndPoint> map = new HashMap<EndPoint, EndPoint>();
        EndPoint[] topN = getStorageEndPoints( token, tmd );

        for( int i = 0 ; i < topN.length ; i++)
        {
            if( FailureDetector.instance().isAlive(topN[i]))
            {
                map.put(topN[i], topN[i]);
                liveList.add(topN[i]) ;
            }
            else
            {
                EndPoint endPoint = getNextAvailableEndPoint(topN[i], tmd, Arrays.asList(topN), liveList);
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

    public abstract EndPoint[] getStorageEndPoints(BigInteger token, TokenMetadata tmd);

}

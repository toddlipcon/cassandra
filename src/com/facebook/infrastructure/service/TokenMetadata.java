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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.facebook.infrastructure.dht.Range;
import com.facebook.infrastructure.net.EndPoint;

/**
 * Immutable class representing the state of the token ring.
 */
public class TokenMetadata
{
    /* Maintains token to endpoint map of every node in the cluster. */
    private final TreeMap<BigInteger, EndPoint> tokenToEndPointMap_;;

    /* Maintains a reverse index of endpoint to token in the cluster. */
    private final Map<EndPoint, BigInteger> endPointToTokenMap_;

    /*
     * For JAXB purposes.
    */
    public TokenMetadata()
    {
        tokenToEndPointMap_ = new TreeMap<BigInteger, EndPoint>();
        endPointToTokenMap_ = new HashMap<EndPoint, BigInteger>();
    }

    public TokenMetadata(TreeMap<BigInteger, EndPoint> tokenToEndPointMap, Map<EndPoint, BigInteger> endPointToTokenMap)
    {
        tokenToEndPointMap_ = tokenToEndPointMap;
        endPointToTokenMap_ = Collections.unmodifiableMap(endPointToTokenMap);
    }

    /**
     * Return a new TokenMetadata object reflecting a new token for the given
     * endpoint.
     */
    public TokenMetadata update(BigInteger token, EndPoint endpoint)
    {
        TreeMap<BigInteger, EndPoint> newTTEP =
            new TreeMap<BigInteger, EndPoint>(tokenToEndPointMap_);
        Map<EndPoint, BigInteger> newEPTT =
            new HashMap<EndPoint, BigInteger>(endPointToTokenMap_);

        BigInteger oldToken = endPointToTokenMap_.get(endpoint);
        if ( oldToken != null )
            newTTEP.remove(oldToken);
        newTTEP.put(token, endpoint);
        newEPTT.put(endpoint, token);

        return new TokenMetadata(newTTEP, newEPTT);
    }

    /**
     * Remove the entries in the two maps.
     * @param endpoint
     */
    public TokenMetadata remove(EndPoint endpoint)
    {
        TreeMap<BigInteger, EndPoint> newTTEP =
            new TreeMap<BigInteger, EndPoint>(tokenToEndPointMap_);
        Map<EndPoint, BigInteger> newEPTT =
            new HashMap<EndPoint, BigInteger>(endPointToTokenMap_);

        BigInteger oldToken = endPointToTokenMap_.get(endpoint);
        if ( oldToken != null )
            newTTEP.remove(oldToken);
        newEPTT.remove(endpoint);

        return new TokenMetadata(newTTEP, newEPTT);
    }

    BigInteger getToken(EndPoint endpoint)
    {
        return endPointToTokenMap_.get(endpoint);
    }


    /**
     * Return the highest token that is less than the given token
     */
    BigInteger getTokenBefore(BigInteger token)
    {
        BigInteger k = tokenToEndPointMap_.lowerKey(token);

        if (k != null)
            return k;

        // Wrap around
        return tokenToEndPointMap_.lastKey();
    }

    /**
     * Return the endpoint that has the highest token less than this ep's token
     */
    EndPoint getTokenBefore(EndPoint ep)
    {
        BigInteger token = endPointToTokenMap_.get( ep );
        Map.Entry<BigInteger, EndPoint> entry = tokenToEndPointMap_.lowerEntry( token );
        if ( entry != null )
            return entry.getValue();
        return tokenToEndPointMap_.lastEntry().getValue(); // wrap
    }

    /**
     * Return the lowest token that is greater than the given token
     */
    BigInteger getTokenAfter(BigInteger token)
    {
        BigInteger k = tokenToEndPointMap_.higherKey(token);

        if (k != null)
            return k;

        // Wrap around
        return tokenToEndPointMap_.firstKey();
    }

    /**
     * Return the endpoint that has the lowest token greater than this ep's token
     */
    EndPoint getTokenAfter(EndPoint ep)
    {
        BigInteger token = endPointToTokenMap_.get( ep );
        Map.Entry<BigInteger, EndPoint> entry = tokenToEndPointMap_.higherEntry( token );
        if ( entry != null )
            return entry.getValue();
        return tokenToEndPointMap_.firstEntry().getValue(); // wrap
    }    

    /**
     * Return the index of the node in the ring that handles this token
     */
    int getTokenIndex(BigInteger token)
    {
        // Find keys strictly less than token
        SortedMap<BigInteger, EndPoint> head = tokenToEndPointMap_.headMap(token);

        // If the last key less than token is the last key of the map total, then
        // we are greater than all keys in the map. Therefore the lowest node in the
        // ring is responsible.
        if ( !head.isEmpty() && head.lastKey().equals( tokenToEndPointMap_.lastKey() ) )
        {
            return 0;
        }
        // otherwise return the number of elems in the map smaller than token
        return head.size();
    }

    /**
     * Return the number of endpoints in the ring
     */
    int getRingSize()
    {
        return tokenToEndPointMap_.size();
    }

    /**
     * Return a list of Ranges starting with (token 0, token 1) and ending
     * with (token N-1, token 0)
     */
    Range[] getRanges() {
        List<Range> ranges = new ArrayList<Range>();
        BigInteger prevToken = null;
        for ( BigInteger token : tokenToEndPointMap_.keySet() )
        {
            if ( prevToken != null )
            {
                ranges.add( new Range(prevToken, token) );
            }
            prevToken = token;
        }

        Range wrapRange = new Range(tokenToEndPointMap_.lastKey(), tokenToEndPointMap_.firstKey());
        ranges.add( wrapRange );

        return ranges.toArray( new Range[0] );
    }

    /**
     * Returns the endpoint with the lowest token that is >= the given token,
     * or wrap around to the beginning of the ring.
     */
    EndPoint getResponsibleEndPoint(BigInteger token) {
        BigInteger responsible = (getResponsibleToken(token));
        return tokenToEndPointMap_.get(responsible);
    }

    /**
     * Given a token that may or may not be exactly equal to one of the endpoint's
     * tokens, returns the token of the endpoint that is responsible for storing
     * this token.
     *
     * @throws NoSuchElementException if there are no known tokens
     */
    BigInteger getResponsibleToken(BigInteger token) {
        if ( tokenToEndPointMap_.isEmpty() )
        {
            throw new NoSuchElementException("No tokens in ring!");
        }

        // find smallest key >= token
        BigInteger ctoken = tokenToEndPointMap_.ceilingKey(token);

        if (ctoken != null)
            return ctoken;

        return tokenToEndPointMap_.firstKey();
    }

    boolean isKnownEndPoint(EndPoint ep)
    {
        return endPointToTokenMap_.containsKey(ep);
    }

    /*
     * Returns a safe clone of tokenToEndPointMap_.
     * This doesn't have to truly clone since the map is immutable
    */
    public SortedMap<BigInteger, EndPoint> cloneTokenEndPointMap()
    {
        return tokenToEndPointMap_;
    }

    public Set<BigInteger> getSortedTokens() {
        return tokenToEndPointMap_.keySet();
    }

    /*
     * Returns a safe clone of endPointTokenMap_.
     * This doesn't have to truly clone since the map is immutable
    */
    public Map<EndPoint, BigInteger> cloneEndPointTokenMap()
    {
        return endPointToTokenMap_;
    }

    public RingIterator getRingIterator(BigInteger startToken)
    {
        return new RingIterator(startToken);
    }

    public RingIterator getRingIterator(EndPoint startPoint)
    {
        return new RingIterator( endPointToTokenMap_.get(startPoint) );
    }

    /**
     * Exposes an iterator that traverses the TokenMetadata in a ring
     * starting at a given key
     */
    public class RingIterator implements Iterator<Map.Entry<BigInteger, EndPoint>>{
        private final Iterator<Map.Entry<BigInteger, EndPoint>> tailIter_;
        private final Iterator<Map.Entry<BigInteger, EndPoint>> headIter_;

        public RingIterator(BigInteger startToken) {
            tailIter_ = tokenToEndPointMap_.tailMap(startToken).entrySet().iterator();
            headIter_ = tokenToEndPointMap_.headMap(startToken).entrySet().iterator();
        }

        public boolean hasNext() {
            return tailIter_.hasNext() || headIter_.hasNext();
        }

        public Map.Entry<BigInteger, EndPoint> next() throws NoSuchElementException {
            if( tailIter_.hasNext() )
                return tailIter_.next();
            return headIter_.next();
        }

        public void remove() {
            throw new UnsupportedOperationException("TokenMetadata is immutable");
        }
    }

    

    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        for ( Map.Entry<EndPoint, BigInteger> entry : endPointToTokenMap_.entrySet() )
        {
            sb.append(entry.getKey());
            sb.append(":");
            sb.append(entry.getValue());
            sb.append(System.getProperty("line.separator"));
        }

        return sb.toString();
    }
}

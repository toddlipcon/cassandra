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

package com.facebook.infrastructure.gms;

import java.util.Random;

import com.facebook.infrastructure.utils.BitSet;


/**
 * Implementation of a PureRandomNumber generator. Use this class cautiously. Not
 * for general purpose use. Currently this is used by the Gossiper to choose a random 
 * endpoint to Gossip to.
 * 
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

class PureRandom extends Random
{
    private BitSet bs_ = new BitSet();
    private int lastUb_;
    
    PureRandom()
    {
        super();
    }
    
    public int nextInt(int ub)
    {
        // If we're going from a big upper bound to a smaller one,
        // reclaim space. TODO is this actually worth doing?
        if ( ub < lastUb_ && ub < bs_.size() )
        {
            bs_.clear(ub, bs_.size());
            bs_ = (BitSet)bs_.clone(); // trims
        }
        lastUb_ = ub;
        
        // If we've already used all the bits once, start again
        // with a new bitset
        if ( bs_.cardinality() == ub )
        {
            bs_.clear();
        }
        int value = super.nextInt(ub);
        while ( bs_.get(value) )
        {
            value = super.nextInt(ub);
        }
        bs_.set(value);
        return value;
    }
}

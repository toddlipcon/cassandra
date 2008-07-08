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
        if ( lastUb_ != ub )
        {
            BitSet bs = new BitSet();
            bs.or(bs_);
            bs_.clear();
            bs_ = bs_;
            lastUb_ = ub;
        }
        
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
    
    public static void main(String[] args) throws Throwable
    {
        Random pr = new PureRandom();
        long startTime = System.currentTimeMillis();
        for ( int k = 0; k < 24; ++k )
        {
            for ( int i = 1; i < 5; ++i )
            {
                for ( int j = 0; j < i; ++j )
                    System.out.println(pr.nextInt(i));
                System.out.println("----------------------------------------");
            }
        }
        System.out.println("Time taken : " + (System.currentTimeMillis() - startTime));
    }
}

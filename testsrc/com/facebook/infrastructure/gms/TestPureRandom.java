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

import junit.framework.TestCase;
import java.util.Random;

public class TestPureRandom extends TestCase
{
    public void testRandom()
    {
        Random pr = new PureRandom();
        long startTime = System.currentTimeMillis();
        for ( int k = 0; k < 24; ++k )
        {
            for ( int i = 1; i < 5; ++i )
            {
                for ( int j = 0; j < i * 2; ++j )
                    System.out.println(pr.nextInt(i));
            }
        }
    }

    /**
     * This test exposed a bug that the previous test didn't
     */
    public void testMore()
    {
        Random pr = new PureRandom();
        int ubs[] = new int[] { 2, 3, 1};

        for (int ub : ubs)
        {
            System.out.println("UB: " + String.valueOf(ub));
            for (int j = 0; j < 10; j++)
            {
                int junk = pr.nextInt(ub);
                // Do something with junk so JVM doesn't optimize away
                assertTrue(junk >= 0 && junk < ub);
            }
        }
    }
}
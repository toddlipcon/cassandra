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

package com.facebook.infrastructure.db;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import org.apache.log4j.Logger;
import com.facebook.infrastructure.concurrent.DebuggableThreadPoolExecutor;
import com.facebook.infrastructure.concurrent.ThreadFactoryImpl;
import com.facebook.infrastructure.utils.LogUtil;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class FastHashMap<K,V> implements Map<K,V>
{ 
    private static Logger logger_ = Logger.getLogger( FastHashMap.class );
    private static Map<String, ExecutorService> apartments_ = new HashMap<String, ExecutorService>();
    
    public static <K,V> Map<K,V> instance(String name)
    {
        if ( apartments_.get(name) == null )
        {
            apartments_.put(name, new DebuggableThreadPoolExecutor( 1,
                    1,
                    Integer.MAX_VALUE,
                    TimeUnit.SECONDS,
                    new LinkedBlockingQueue<Runnable>(),
                    new ThreadFactoryImpl("FAST-HASH-MAP-POOL")
                    ));
        }
        return new FastHashMap<K,V>(name);
    }
    
    class Getter implements Callable<V>
    {
        private Object k_;
        
        Getter(Object k)
        {
            k_ = k;
        }
        
        public V call()
        {
            return map_.get(k_);
        }
    }
    
    class Putter implements Callable<V>
    {
        private K k_;
        private V v_;
        
        Putter(K k, V v)
        {
            k_ = k;
            v_ = v;
        }
        
        public V call()
        {
            V v = map_.get(k_);
            map_.put(k_, v_);
            return v;
        }
    }
    
    class Remover implements Callable<V>
    {
        private Object k_;
        
        Remover(Object k)
        {
            k_ = k;
        }
        
        public V call()
        {
            return map_.remove(k_);
        }
    }
    
    private String name_;
    private Map<K,V> map_ = new HashMap<K,V>();
    
    FastHashMap(String name)
    {
        name_ = name;
    }
    
    public V get(Object k)
    {
        V result = null;
        Future<V> futurePtr = apartments_.get(name_).submit(new Getter(k));
        try
        {
            result = futurePtr.get();
        }
        catch ( InterruptedException ex )
        {
            logger_.debug(LogUtil.throwableToString(ex));
        }
        catch ( ExecutionException ex2 )
        {
            logger_.debug(LogUtil.throwableToString(ex2));
        }
        return result;
    }
    
    public V put(K k, V v)
    {
        V result = null;
        Future<V> futurePtr = apartments_.get(name_).submit(new Putter(k, v));
        try
        {
            result = futurePtr.get();
        }
        catch ( InterruptedException ex )
        {
            logger_.debug(LogUtil.throwableToString(ex));
        }
        catch ( ExecutionException ex2 )
        {
            logger_.debug(LogUtil.throwableToString(ex2));
        }
        return result;
    }
    
    public V remove(Object k)
    {
        V result = null;
        Future<V> futurePtr = apartments_.get(name_).submit(new Remover(k));
        try
        {
            result = futurePtr.get();
        }
        catch ( InterruptedException ex )
        {
            logger_.debug(LogUtil.throwableToString(ex));
        }
        catch ( ExecutionException ex2 )
        {
            logger_.debug(LogUtil.throwableToString(ex2));
        }
        return result;
    }
    
    public Set<K> keySet()
    {
        return map_.keySet();
    }
    
    public Collection<V> values()
    {
        return map_.values();
    }
    
    public int size()
    {
        return map_.size();
    }
    
    public void putAll(Map<? extends K,? extends V> m)
    {
        throw new UnsupportedOperationException("This operation is not supported in the FastHashMap.");
    }
    
    public boolean isEmpty()
    {
        return map_.isEmpty();
    }
    
    public void clear()
    {
        map_.clear();
    }
    
    public boolean containsKey(Object k)
    {
        return map_.containsKey(k);
    }
    
    public boolean containsValue(Object v)
    {
        return map_.containsValue(v);
    }
    
    public Set<Map.Entry<K,V>> entrySet()
    {
        return map_.entrySet();
    }
    
    public boolean equals(Object o)
    {
        return map_.equals(o);
    }
    
    public int hashCode()
    {
        return map_.hashCode();
    }
    
    public static void main(String[] args) throws Throwable
    {
        Map<String, String> map = FastHashMap.instance("CF");
        map.put("Avinash", "Avinash");
        map.put("Srinivas", "Srinivas");
        map.put("Meena", "Meena");
        
        System.out.println(map.get("Avinash"));
        System.out.println(map.get("Srinivas"));
        System.out.println(map.get("Meena"));
    }
}

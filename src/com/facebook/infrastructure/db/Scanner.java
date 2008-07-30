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

import java.util.*;
import java.io.IOException;

import com.facebook.infrastructure.conf.DatabaseDescriptor;

/*
 * This class is used to loop through a retrieved column family
 * to get all columns in Iterator style. Usage is as follows:
 * Scanner scanner = new Scanner("table");
 * scanner.fetchColumnfamily("column-family");
 * scanner.lookup("key");
 * 
 * while ( scanner.hasNext() )
 * {
 *     Column column = scanner.next();
 *     // Do something with the column
 * }
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class Scanner
{
    private String table_;
    private String columnFamily_;
    private Set<IColumn> columns_ = new HashSet<IColumn>();
    
    public Scanner(String table)
    {
        table_ = table;
    }
    
    public void fetchColumnFamily(String cf) throws IOException
    {
        columnFamily_ = cf;        
    }
    
    public Iterator<IColumn> lookup(String key) throws ColumnFamilyNotDefinedException, IOException
    {
        if ( columnFamily_ != null )
        {
            Table table = Table.open(table_);
            ColumnFamily columnFamily = table.get(key, columnFamily_);
            if ( columnFamily != null )
            {
                Collection<IColumn> columns = columnFamily.getSortedColumns();
                columns_.addAll(columns);
            }
        }
        return columns_.iterator();
    }
    
    public boolean hasNext()
    {
        return columns_.iterator().hasNext();
    }
    
    public IColumn next()
    {
        return columns_.iterator().next();
    }
    
    public void remove()
    {
        columns_.iterator().remove();
    }
    
    public static void main(String[] args) throws Throwable
    {
        DatabaseDescriptor.init();
        Table table = Table.open("Test");    
        Random random = new Random();
        byte[] bytes = new byte[1024];        
        
        String key = new Integer(10).toString();
        RowMutation rm = new RowMutation("Test", key);
        random.nextBytes(bytes);
        rm.add("ColumnFamily:Column", bytes);
        rm.add("ColumnFamily2:Column", bytes);
        rm.apply();
        
        Scanner scanner = new Scanner("Test");
        scanner.fetchColumnFamily("ColumnFamily");
        Iterator<IColumn> it = scanner.lookup(key);
        while ( it.hasNext() )
        {
            IColumn column = it.next();
            System.out.println(column.name());
            System.out.println(column.value());
        }
    }
}

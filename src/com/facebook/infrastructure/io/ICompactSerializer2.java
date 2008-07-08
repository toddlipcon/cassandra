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

package com.facebook.infrastructure.io;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import com.facebook.infrastructure.utils.BloomFilter;

/**
 * This interface is an extension of the ICompactSerializer which allows for partial deserialization
 * of a type.
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public interface ICompactSerializer2<T> extends ICompactSerializer<T>
{   
	/**
     * Returns an instance of an IColumn which contains only the 
     * columns that are required. This is specified in the <i>columnNames</i>
     * argument.
     * 
     * @param dis DataInputStream from which we need to deserialize.
     * @param columnNames list of items that are required.
     * @throws IOException
     * @return type which contains the specified items.
	*/
	public T deserialize(DataInputStream dis, List<String> columnNames) throws IOException;
    
    /**
     * This method is used to deserialize just the specified field from 
     * the serialized stream.
     * 
     * @param dis DataInputStream from which we need to deserialize.
     * @param name name of the desired field.
     * @param count count of the number of fields required.
     * @throws IOException
     * @return the deserialized type.
    */
	public T deserialize(DataInputStream dis, String name, int count) throws IOException;
    
    /**
     * This method is used to deserialize just the specified field from 
     * the serialized stream.
     * 
     * @param dis DataInputStream from which we need to deserialize.
     * @param startPosition start from the field in this position.
     * @param count count of the number of fields required.
     * @throws IOException
     * @return the deserialized type.
    */    
    public T deserialize(DataInputStream dis, int startPosition, int count) throws IOException;
    
    /**
     * 
     * @param dis
     * @throws IOException
     */
    public void skip(DataInputStream dis) throws IOException;
}

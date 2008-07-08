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

package com.facebook.infrastructure.net.io;

import java.io.IOException;

import com.facebook.infrastructure.io.DataInputBuffer;
import com.facebook.infrastructure.io.DataOutputBuffer;
import com.facebook.infrastructure.net.Message;
/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class FastSerializer implements ISerializer
{ 
    public byte[] serialize(Message message) throws IOException
    {
        DataOutputBuffer buffer = new DataOutputBuffer();
        Message.serializer().serialize(message, buffer);
        return buffer.getData();
    }
    
    public Message deserialize(byte[] bytes) throws IOException
    {
        DataInputBuffer bufIn = new DataInputBuffer();
        bufIn.reset(bytes, bytes.length);
        return Message.serializer().deserialize(bufIn);
    }
}

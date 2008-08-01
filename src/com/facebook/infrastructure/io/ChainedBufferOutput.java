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

import java.io.IOException;
import java.io.DataOutput;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;


public class ChainedBufferOutput implements DataOutput, IWritable
{
    private List<IWritable> buffers_ = new ArrayList<IWritable>();
    private int chainedLen_ = 0;
    private DataOutputBuffer curBuffer_ = null;


    public void write(byte[] b) throws IOException
    {
        write(b, 0, b.length);
    }

    public void write(byte[] b, int off, int len) throws IOException
    {
        appendCurBuffer();
        addToChain( new BytesWritable(b, off, len) );
    }

    public void write(int b) throws IOException
    {
        ensureBuffer().write(b);
    }

    public void writeBoolean(boolean v) throws IOException
    {
        ensureBuffer().writeBoolean(v);
    }

    public void writeByte(int v) throws IOException
    {
        ensureBuffer().writeByte(v);
    }

    public void writeBytes(String s) throws IOException
    {
        ensureBuffer().writeBytes(s);
    }

    public void writeChar(int v) throws IOException
    {
        ensureBuffer().writeChar(v);
    }

    public void writeChars(String s) throws IOException
    {
        ensureBuffer().writeChars(s);
    }

    public void writeDouble(double v) throws IOException
    {
        ensureBuffer().writeDouble(v);
    }

    public void writeFloat(float v) throws IOException
    {
        ensureBuffer().writeFloat(v);
    }

    public void writeInt(int v) throws IOException
    {
        ensureBuffer().writeInt(v);
    }

    public void writeLong(long v) throws IOException
    {
        ensureBuffer().writeLong(v);
    }

    public void writeShort(int v) throws IOException
    {
        ensureBuffer().writeShort(v);
    }

    public void writeUTF(String str) throws IOException
    {
        ensureBuffer().writeUTF(str);
    }

    private DataOutputBuffer ensureBuffer()
    {
        if( curBuffer_ == null )
        {
            curBuffer_ = new DataOutputBuffer();
        }
        return curBuffer_;
    }

    private void appendCurBuffer()
    {
        if (curBuffer_ != null)
        {
            addToChain( curBuffer_ );
            curBuffer_ = null;
        }
    }

    private void addToChain( IWritable writable )
    {
        chainedLen_ += writable.getLength();
        buffers_.add( writable );
    }

    /* IWritable implementation */

    public void writeTo(DataOutput out) throws IOException
    {
        for ( IWritable writable : buffers_ )
        {
            writable.writeTo(out);
        }
        if( curBuffer_ != null )
            curBuffer_.writeTo(out);
    }

    public void putTo(ByteBuffer out) throws IOException
    {
        for ( IWritable writable : buffers_ )
        {
            writable.putTo(out);
        }
        if( curBuffer_ != null )
            curBuffer_.putTo(out);
    }

    public int getLength()
    {
        int len = chainedLen_;
        if( curBuffer_ != null )
            len += curBuffer_.getLength();
        return len;
    }

    private static class BytesWritable implements IWritable
    {
        byte[] bytes_;
        int off_;
        int len_;

        public BytesWritable(byte[] b, int off, int len)
        {
            bytes_ = b;
            off_ = off;
            len_ = len;
        }

        public void writeTo(DataOutput out) throws IOException
        {
            out.write(bytes_, off_, len_);
        }

        public void putTo(ByteBuffer buf) throws IOException
        {
            buf.put(bytes_, off_, len_);
        }

        public int getLength()
        {
            return len_;
        }
    }

}

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

import java.io.*;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.log4j.Logger;

import com.facebook.infrastructure.conf.DatabaseDescriptor;
import com.facebook.infrastructure.db.RowMutation;
import com.facebook.infrastructure.io.IndexHelper.ColumnPositionInfo;
import com.facebook.infrastructure.utils.LogUtil;

/**
 * This class writes key/value pairs seqeuntially to disk. It is
 * also used to read sequentially from disk. However one could
 * jump to random positions to read data from the file. This class
 * also has many implementations of the IFileWriter and IFileReader
 * interfaces which are exposed through factory methods.
 *
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com ) & Karthik Ranganathan ( kranganathan@facebook.com )
 */

public class SequenceFile
{
    public static abstract class AbstractWriter implements IFileWriter
    {
        protected String filename_;

        AbstractWriter(String filename)
        {
            filename_ = filename;
        }

        public String getFileName()
        {
            return filename_;
        }

        public long lastModified()
        {
            File file = new File(filename_);
            return file.lastModified();
        }
    }

    public static class Writer extends AbstractWriter
    {
        private RandomAccessFile file_;

        Writer(String filename) throws IOException
        {
            super(filename);
            File file = new File(filename);
            boolean isNewFile = false;
            if ( !file.exists() )
            {
                file.createNewFile();
            }
            file_ = new RandomAccessFile(file, "rw");
        }

        public long getCurrentPosition() throws IOException
        {
            return file_.getFilePointer();
        }

        public void seek(long position) throws IOException
        {
            file_.seek(position);
        }

        public void append(DataOutputBuffer keyBuffer, DataOutputBuffer buffer) throws IOException
        {
            int keyBufLength = keyBuffer.getLength();
            if ( keyBuffer == null || keyBufLength == 0 )
                throw new IllegalArgumentException("Key cannot be NULL or of zero length.");

            file_.seek(file_.getFilePointer());
            file_.writeInt(keyBufLength);
            file_.write(keyBuffer.getData(), 0, keyBufLength);

            int length = buffer.getLength();
            file_.writeInt(length);
            file_.write(buffer.getData(), 0, length);
        }

        public void append(String key, DataOutputBuffer buffer) throws IOException
        {
            if ( key == null )
                throw new IllegalArgumentException("Key cannot be NULL.");

            file_.seek(file_.getFilePointer());
            file_.writeUTF(key);
            int length = buffer.getLength();
            file_.writeInt(length);
            file_.write(buffer.getData(), 0, length);
        }

        public void append(String key, byte[] value) throws IOException
        {
            if ( key == null )
                throw new IllegalArgumentException("Key cannot be NULL.");

            file_.seek(file_.getFilePointer());
            file_.writeUTF(key);
            file_.writeInt(value.length);
            file_.write(value);
        }

        public void append(String key, long value) throws IOException
        {
            if ( key == null )
                throw new IllegalArgumentException("Key cannot be NULL.");

            file_.seek(file_.getFilePointer());
            file_.writeUTF(key);
            file_.writeLong(value);
        }

        /**
         * Be extremely careful while using this API. This currently
         * used to write the commit log header in the commit logs.
         * If not used carefully it could completely screw up reads
         * of other key/value pairs that are written.
         * @param bytes the bytes to write
        */
        public long writeDirect(byte[] bytes) throws IOException
        {
            file_.write(bytes);
            return file_.getFilePointer();
        }

        public void close() throws IOException
        {
            file_.close();
        }

        public void close(byte[] footer, int size) throws IOException
        {
            file_.writeUTF(SequenceFile.marker_);
            file_.writeInt(size);
            file_.write(footer);
            close();
        }

        public String getFileName()
        {
            return filename_;
        }

        public long getFileSize() throws IOException
        {
            return file_.length();
        }
    }

    public static class BufferWriter extends AbstractWriter
    {
        private BufferedRandomAccessFile file_;
        private long position_ = 0L;

        BufferWriter(String filename, int size) throws IOException
        {
            super(filename);
            File file = new File(filename);
            file_ = new BufferedRandomAccessFile(file, "rw", size);
            if ( !file.exists() )
            {
                file.createNewFile();
            }
        }

        public long getCurrentPosition() throws IOException
        {
            return file_.getFilePointer();
        }

        public void seek(long position) throws IOException
        {
            file_.seek(position);
        }

        public void append(DataOutputBuffer keyBuffer, DataOutputBuffer buffer) throws IOException
        {
            int keyBufLength = keyBuffer.getLength();
            if ( keyBuffer == null || keyBufLength == 0 )
                throw new IllegalArgumentException("Key cannot be NULL or of zero length.");

            file_.seek(file_.getFilePointer());
            file_.writeInt(keyBufLength);
            file_.write(keyBuffer.getData(), 0, keyBufLength);

            int length = buffer.getLength();
            file_.writeInt(length);
            file_.write(buffer.getData(), 0, length);
        }

        public void append(String key, DataOutputBuffer buffer) throws IOException
        {
            if ( key == null )
                throw new IllegalArgumentException("Key cannot be NULL.");

            file_.seek(file_.getFilePointer());
            file_.writeUTF(key);
            int length = buffer.getLength();
            file_.writeInt(length);
            file_.write(buffer.getData(), 0, length);
        }

        public void append(String key, byte[] value) throws IOException
        {
            if ( key == null )
                throw new IllegalArgumentException("Key cannot be NULL.");

            file_.seek(file_.getFilePointer());
            file_.writeUTF(key);
            file_.writeInt(value.length);
            file_.write(value);
        }

        public void append(String key, long value) throws IOException
        {
            if ( key == null )
                throw new IllegalArgumentException("Key cannot be NULL.");

            file_.seek(file_.getFilePointer());
            file_.writeUTF(key);
            file_.writeLong(value);
        }

        /**
         * Be extremely careful while using this API. This currently
         * used to write the commit log header in the commit logs.
         * If not used carefully it could completely screw up reads
         * of other key/value pairs that are written.
         * @param bytes the bytes to write
        */
        public long writeDirect(byte[] bytes) throws IOException
        {
            file_.write(bytes);
            return file_.getFilePointer();
        }

        public void close() throws IOException
        {
            file_.close();
        }

        public void close(byte[] footer, int size) throws IOException
        {
            file_.writeUTF(SequenceFile.marker_);
            file_.writeInt(size);
            file_.write(footer);
            close();
        }

        public String getFileName()
        {
            return filename_;
        }

        public long getFileSize() throws IOException
        {
            return file_.length();
        }
    }

    public static class ConcurrentWriter extends AbstractWriter
    {
        private FileChannel fc_;

        public ConcurrentWriter(String filename) throws IOException
        {
            super(filename);
            RandomAccessFile raf = new RandomAccessFile(filename, "rw");
            fc_ = raf.getChannel();
        }

        public long getCurrentPosition() throws IOException
        {
            return fc_.position();
        }

        public void seek(long position) throws IOException
        {
            fc_.position(position);
        }

        public void append(DataOutputBuffer keyBuffer, DataOutputBuffer buffer) throws IOException
        {
            int keyBufLength = keyBuffer.getLength();
            if ( keyBuffer == null || keyBufLength == 0 )
                throw new IllegalArgumentException("Key cannot be NULL or of zero length.");

            /* Size allocated "int" for key length + key + "int" for data length + data */
            int length = buffer.getLength();
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect( 4 + keyBufLength + 4 + length );
            byteBuffer.putInt(keyBufLength);
            byteBuffer.put(keyBuffer.getData(), 0, keyBufLength);
            byteBuffer.putInt(length);
            byteBuffer.put(buffer.getData(), 0, length);
            byteBuffer.flip();
            fc_.write(byteBuffer);
        }

        public void append(String key, DataOutputBuffer buffer) throws IOException
        {
            if ( key == null )
                throw new IllegalArgumentException("Key cannot be NULL.");

            int length = buffer.getLength();
            /* Size allocated : utfPrefix_ + key length + "int" for data size + data */
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect( SequenceFile.utfPrefix_ + key.length() + 4 + length);
            SequenceFile.writeUTF(byteBuffer, key);
            byteBuffer.putInt(length);
            byteBuffer.put(buffer.getData(), 0, length);
            byteBuffer.flip();
            fc_.write(byteBuffer);
        }

        public void append(String key, byte[] value) throws IOException
        {
            if ( key == null )
                throw new IllegalArgumentException("Key cannot be NULL.");

            /* Size allocated key length + "int" for data size + data */
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(utfPrefix_ + key.length() + 4 + value.length);
            SequenceFile.writeUTF(byteBuffer, key);
            byteBuffer.putInt(value.length);
            byteBuffer.put(value);
            byteBuffer.flip();
            fc_.write(byteBuffer);
        }

        public void append(String key, long value) throws IOException
        {
            if ( key == null )
                throw new IllegalArgumentException("Key cannot be NULL.");

            /* Size allocated key length + a long */
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(SequenceFile.utfPrefix_ + key.length() + 8);
            SequenceFile.writeUTF(byteBuffer, key);
            byteBuffer.putLong(value);
            byteBuffer.flip();
            fc_.write(byteBuffer);
        }

        /*
         * Be extremely careful while using this API. This currently
         * used to write the commit log header in the commit logs.
         * If not used carefully it could completely screw up reads
         * of other key/value pairs that are written.
        */
        public long writeDirect(byte[] bytes) throws IOException
        {
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(bytes.length);
            byteBuffer.put(bytes);
            byteBuffer.flip();
            fc_.write(byteBuffer);
            return fc_.position();
        }

        public void close() throws IOException
        {
            fc_.close();
        }

        public void close(byte[] footer, int size) throws IOException
        {
            /* Size is marker length + "int" for size + footer data */
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect( utfPrefix_ + SequenceFile.marker_.length() + 4 + footer.length);
            SequenceFile.writeUTF(byteBuffer, SequenceFile.marker_);
            byteBuffer.putInt(size);
            byteBuffer.put(footer);
            byteBuffer.flip();
            fc_.write(byteBuffer);
            close();
        }

        public String getFileName()
        {
            return filename_;
        }

        public long getFileSize() throws IOException
        {
            return fc_.size();
        }
    }

    public static class FastConcurrentWriter extends AbstractWriter
    {
        private FileChannel fc_;
        private MappedByteBuffer buffer_;

        public FastConcurrentWriter(String filename, int size) throws IOException
        {
            super(filename);
            fc_ = new RandomAccessFile(filename, "rw").getChannel();
            buffer_ = fc_.map( FileChannel.MapMode.READ_WRITE, 0, size );
            buffer_.load();
        }

        void unmap(final Object buffer)
        {
            AccessController.doPrivileged( new PrivilegedAction<MappedByteBuffer>()
                                        {
                                            public MappedByteBuffer run()
                                            {
                                                try
                                                {
                                                    Method getCleanerMethod = buffer.getClass().getMethod("cleaner", new Class[0]);
                                                    getCleanerMethod.setAccessible(true);
                                                    sun.misc.Cleaner cleaner = (sun.misc.Cleaner)getCleanerMethod.invoke(buffer,new Object[0]);
                                                    cleaner.clean();
                                                }
                                                catch(Throwable e)
                                                {
                                                    logger_.warn( LogUtil.throwableToString(e) );
                                                }
                                                return null;
                                            }
                                        });
        }


        public long getCurrentPosition() throws IOException
        {
            return buffer_.position();
        }

        public void seek(long position) throws IOException
        {
            buffer_.position((int)position);
        }

        public void append(DataOutputBuffer keyBuffer, DataOutputBuffer buffer) throws IOException
        {
            int keyBufLength = keyBuffer.getLength();
            if ( keyBuffer == null || keyBufLength == 0 )
                throw new IllegalArgumentException("Key cannot be NULL or of zero length.");

            int length = buffer.getLength();
            buffer_.putInt(keyBufLength);
            buffer_.put(keyBuffer.getData(), 0, keyBufLength);
            buffer_.putInt(length);
            buffer_.put(buffer.getData(), 0, length);
        }

        public void append(String key, DataOutputBuffer buffer) throws IOException
        {
            if ( key == null )
                throw new IllegalArgumentException("Key cannot be NULL.");

            int length = buffer.getLength();
            SequenceFile.writeUTF(buffer_, key);
            buffer_.putInt(length);
            buffer_.put(buffer.getData(), 0, length);
        }

        public void append(String key, byte[] value) throws IOException
        {
            if ( key == null )
                throw new IllegalArgumentException("Key cannot be NULL.");

            SequenceFile.writeUTF(buffer_, key);
            buffer_.putInt(value.length);
            buffer_.put(value);
        }

        public void append(String key, long value) throws IOException
        {
            if ( key == null )
                throw new IllegalArgumentException("Key cannot be NULL.");

            SequenceFile.writeUTF(buffer_, key);
            buffer_.putLong(value);
        }

        /*
         * Be extremely careful while using this API. This currently
         * used to write the commit log header in the commit logs.
         * If not used carefully it could completely screw up reads
         * of other key/value pairs that are written.
        */
        public long writeDirect(byte[] bytes) throws IOException
        {
            buffer_.put(bytes);
            return buffer_.position();
        }

        public void close() throws IOException
        {
            buffer_.flip();
            buffer_.force();
            unmap(buffer_);
            fc_.truncate(buffer_.limit());
        }

        public void close(byte[] footer, int size) throws IOException
        {
            SequenceFile.writeUTF(buffer_, SequenceFile.marker_);
            buffer_.putInt(size);
            buffer_.put(footer);
            close();
        }

        public String getFileName()
        {
            return filename_;
        }

        public long getFileSize() throws IOException
        {
            return buffer_.position();
        }
    }

    public static abstract class AbstractReader implements IFileReader
    {
        private static final short utfPrefix_ = 2;
    	protected RandomAccessFile file_;
        protected String filename_;

        AbstractReader(String filename)
        {
            filename_ = filename;
        }

        public String getFileName()
        {
            return filename_;
        }

        /**
         * Return the position of the given key from the block index.
         * @param key the key whose offset is to be extracted from the current block index
         */
        public long getPositionFromBlockIndex(String key) throws IOException
        {
            long position = -1L;
            /* read the block key. */
            String blockIndexKey = file_.readUTF();
            if ( !blockIndexKey.equals(SSTable.blockIndexKey_) )
                throw new IOException("Unexpected position to be reading the block index from.");
            /* read the size of the block index */
            int size = file_.readInt();

            /* Read the entire block index. */
            byte[] bytes = new byte[size];
            file_.readFully(bytes);

            DataInputBuffer bufIn = new DataInputBuffer();
            bufIn.reset(bytes, bytes.length);

            /* Number of keys in the block. */
            int keys = bufIn.readInt();
            for ( int i = 0; i < keys; ++i )
            {
                String keyInBlock = bufIn.readUTF();
                if ( keyInBlock.equals(key) )
                {
                    position = bufIn.readLong();
                    break;
                }
                else
                {
                    /*
                     * This is not the key we are looking for. So read its position
                     * and the size of the data associated with it. This was strored
                     * as the BlockMetadata.
                    */
                    bufIn.readLong();
                    bufIn.readLong();
                }
            }

            return position;
        }

        /**
         * Return the block index metadata for a given key.
         */
        public SSTable.BlockMetadata getBlockMetadata(String key) throws IOException
        {
            SSTable.BlockMetadata blockMetadata = SSTable.BlockMetadata.NULL;
            /* read the block key. */
            String blockIndexKey = file_.readUTF();
            if ( !blockIndexKey.equals(SSTable.blockIndexKey_) )
                throw new IOException("Unexpected position to be reading the block index from.");
            /* read the size of the block index */
            int size = file_.readInt();

            /* Read the entire block index. */
            byte[] bytes = new byte[size];
            file_.readFully(bytes);

            DataInputBuffer bufIn = new DataInputBuffer();
            bufIn.reset(bytes, bytes.length);

            /* Number of keys in the block. */
            int keys = bufIn.readInt();
            for ( int i = 0; i < keys; ++i )
            {
                String keyInBlock = bufIn.readUTF();
                if ( keyInBlock.equals(key) )
                {
                    long position = bufIn.readLong();
                    long dataSize = bufIn.readLong();
                    blockMetadata = new SSTable.BlockMetadata(position, dataSize);
                    break;
                }
                else
                {
                    /*
                     * This is not the key we are looking for. So read its position
                     * and the size of the data associated with it. This was strored
                     * as the BlockMetadata.
                    */
                    bufIn.readLong();
                    bufIn.readLong();
                }
            }

            return blockMetadata;
        }

        /**
         * This function seeks to the position where the key data is present in the file
         * in order to get the buffer cache populated with the key-data. This is done as
         * a hint before the user actually queries the data.
         * @param key the key whose data is being touched
         * @param fData
         */
        public long touch(String key, boolean fData) throws IOException
        {
            long bytesRead = -1L;
            if ( isEOF() )
                return bytesRead;

            long startPosition = file_.getFilePointer();
            String keyInDisk = file_.readUTF();
            if ( keyInDisk != null )
            {
                /*
                 * If key on disk is greater than requested key
                 * we can bail out since we exploit the property
                 * of the SSTable format.
                */
                if ( keyInDisk.compareTo(key) > 0 )
                    return bytesRead;

                /*
                 * If we found the key then we populate the buffer that
                 * is passed in. If not then we skip over this key and
                 * position ourselves to read the next one.
                */
                int dataSize = file_.readInt();
                if ( keyInDisk.equals(key) )
                {
                    /* return 0L to signal the key has been touched. */
                    bytesRead = 0L;
                    return bytesRead;
                }
                else
                {
                    /* skip over data portion */
                    file_.seek(dataSize + file_.getFilePointer());
                }

                long endPosition = file_.getFilePointer();
                bytesRead = endPosition - startPosition;
            }

            return bytesRead;
        }

        /**
         * This method dumps the next key/value into the DataOuputStream
         * passed in. Always use this method to query for application
         * specific data as it will have indexes.
         *
         * @param key key we are interested in.
         * @param dos DataOutputStream that needs to be filled.
         * @param cf the IColumn we want to read
         * @return total number of bytes read/considered
        */
        public long next(String key, DataOutputBuffer bufOut, String cf) throws IOException
        {
    		String[] values = RowMutation.getColumnAndColumnFamily(cf);
    		String columnFamilyName = values[0];
    		// String columnName = (values[1] == null || values[1].length() == 0)?null:values[1];
    		String columnName = (values.length == 1) ? null : values[1];

            long bytesRead = -1L;
            if ( isEOF() )
                return bytesRead;

            long startPosition = file_.getFilePointer();
            String keyInDisk = file_.readUTF();
            if ( keyInDisk != null )
            {
                /*
                 * If key on disk is greater than requested key
                 * we can bail out since we exploit the property
                 * of the SSTable format.
                */
                if ( keyInDisk.compareTo(key) > 0 )
                    return bytesRead;

                /*
                 * If we found the key then we populate the buffer that
                 * is passed in. If not then we skip over this key and
                 * position ourselves to read the next one.
                */
                int dataSize = file_.readInt();
                if ( keyInDisk.equals(key) )
                {
                    /* write the key into buffer */
                    bufOut.writeUTF( keyInDisk );

                    /* if there is no column indexing enabled on this column then there are no indexes for it */
                    if(!DatabaseDescriptor.isNameIndexEnabled(columnFamilyName))
                    {
                    	/* write the data size */
                    	bufOut.writeInt(dataSize);
	                    /* write the data into buffer, except the boolean we have read */
	                    bufOut.write(file_, dataSize);
                    }
                    /* if we need to read the all the columns do not read the column indexes */
                    else if(columnName == null)
                    {
                    	int bytesSkipped = IndexHelper.skip(file_);
	                    /*
	                     * read the correct number of bytes for the column family and
	                     * write data into buffer
	                    */
                    	dataSize -= bytesSkipped;
                    	/* write the data size */
                    	bufOut.writeInt(dataSize);
	                    /* write the data into buffer, except the boolean we have read */
	                    bufOut.write(file_, dataSize);
                    }
                    else
                    {
                    	/* check if we have an index */
                        boolean hasColumnIndexes = file_.readBoolean();
                        int totalBytesRead = 1;
                        List<ColumnPositionInfo> columnIndexList = null;
                        /* if we do then deserialize the index */
                        if(hasColumnIndexes)
                        {
                        	columnIndexList = new ArrayList<IndexHelper.ColumnPositionInfo>();
                        	/* read the index */
                        	totalBytesRead += IndexHelper.deserializeIndex(file_, columnIndexList);
                        }
                    	dataSize -= totalBytesRead;

                        /* read the column family name */
                        String cfName = file_.readUTF();
                        dataSize -= (utfPrefix_ + cfName.length());

                        /* read if this cf is marked for delete */
                        boolean markedForDelete = file_.readBoolean();
                        dataSize -= 1;

                        /* read the total number of columns */
                        int totalNumCols = file_.readInt();
                        dataSize -= 4;

                        /* get the column range we have to read */
                        IndexHelper.ColumnPositionInfo columnRange = IndexHelper.getColumnRangeFromIndex(columnName, columnIndexList, dataSize, totalNumCols);

                		/* seek to the correct offset to the data, and calculate the data size */
                        file_.skipBytes(columnRange.start());
                        dataSize = columnRange.end() - columnRange.start();

                        /*
                         * write the number of columns in the column family we are returning:
                         * 	dataSize that we are reading +
                         * 	length of column family name +
                         * 	one booleanfor deleted or not +
                         * 	one int for number of columns
                        */
                        bufOut.writeInt(dataSize + utfPrefix_+cfName.length() + 4 + 1);
                        /* write the column family name */
                        bufOut.writeUTF(cfName);
                        /* write if this cf is marked for delete */
                        bufOut.writeBoolean(markedForDelete);
                        /* write number of columns */
                        bufOut.writeInt(columnRange.numColumns());
                        /* now write the columns */
                        bufOut.write(file_, dataSize);
                    }
                }
                else
                {
                    /* skip over data portion */
                	file_.seek(dataSize + file_.getFilePointer());
                }

                long endPosition = file_.getFilePointer();
                bytesRead = endPosition - startPosition;
            }

            return bytesRead;
        }

        /**
         * This method dumps the next key/value into the DataOuputStream
         * passed in. Always use this method to query for application
         * specific data as it will have indexes.
         *
         * @param key key we are interested in.
         * @param dos DataOutputStream that needs to be filled.
         * @param cfName The name of the column family only without the ":"
         * @param columnNames The list of columns in the cfName column family that we want to return
         * @return total number of bytes read/considered
         *
        */
        public long next(String key, DataOutputBuffer bufOut, String cf, List<String> columnNames) throws IOException
        {
        	String[] values = RowMutation.getColumnAndColumnFamily(cf);
    		String columnFamilyName = values[0];

            long bytesRead = -1L;
            if ( isEOF() )
                return bytesRead;

            long startPosition = file_.getFilePointer();
            String keyInDisk = file_.readUTF();
            if ( keyInDisk != null )
            {
                /*
                 * If key on disk is greater than requested key
                 * we can bail out since we exploit the property
                 * of the SSTable format.
                */
                if ( keyInDisk.compareTo(key) > 0 )
                    return bytesRead;

                /*
                 * If we found the key then we populate the buffer that
                 * is passed in. If not then we skip over this key and
                 * position ourselves to read the next one.
                */
                int dataSize = file_.readInt();
                if ( keyInDisk.equals(key) )
                {
                    /* write the key into buffer */
                    bufOut.writeUTF( keyInDisk );

                    /* if there is no column indexing enabled on this column then there are no indexes for it */
                    if(!DatabaseDescriptor.isNameIndexEnabled(columnFamilyName))
                    {
                    	/* write the data size */
                    	bufOut.writeInt(dataSize);
	                    /* write the data into buffer, except the boolean we have read */
	                    bufOut.write(file_, dataSize);
                    }
                    /* if we need to read the all the columns do not read the column indexes */
                    else if(columnNames == null || columnNames.isEmpty() )
                    {
                    	int bytesSkipped = IndexHelper.skip(file_);
	                    /*
	                     * read the correct number of bytes for the column family and
	                     * write data into buffer
	                    */
                    	dataSize -= bytesSkipped;
                    	/* write the data size */
                    	bufOut.writeInt(dataSize);
	                    /* write the data into buffer, except the boolean we have read */
	                    bufOut.write(file_, dataSize);
                    }
                    else
                    {
                    	/* check if we have an index */
                        boolean hasColumnIndexes = file_.readBoolean();
                        int totalBytesRead = 1;
                        List<ColumnPositionInfo> columnIndexList = null;
                        /* if we do then deserialize the index */
                        if(hasColumnIndexes)
                        {
                        	columnIndexList = new ArrayList<IndexHelper.ColumnPositionInfo>();
                        	/* read the index */
                        	totalBytesRead += IndexHelper.deserializeIndex(file_, columnIndexList);
                        }
                    	dataSize -= totalBytesRead;

                        /* read the column family name */
                        String cfName = file_.readUTF();
                        dataSize -= (utfPrefix_ + cfName.length());

                        /* read if this cf is marked for delete */
                        boolean markedForDelete = file_.readBoolean();
                        dataSize -= 1;

                        /* read the total number of columns */
                        int totalNumCols = file_.readInt();
                        dataSize -= 4;

                        // TODO: this is name sorted - but eventually this should be sorted by the same criteria as the col index
                        /* sort the required list of columns */
                        Collections.sort(columnNames);
                        /* get the various column ranges we have to read */
                        List<IndexHelper.ColumnPositionInfo> columnRangeList = IndexHelper.getMultiColumnRangesFromIndex(columnNames, columnIndexList, dataSize, totalNumCols);

                        /* calculate the data size */
                        int numColsReturned = 0;
                        int dataSizeReturned = 0;
                        for(ColumnPositionInfo colRange : columnRangeList)
                        {
                        	numColsReturned += colRange.numColumns();
                        	dataSizeReturned += colRange.end() - colRange.start();
                        }

                        /*
                         * write the number of columns in the column family we are returning:
                         * 	dataSize that we are reading +
                         * 	length of column family name +
                         * 	one booleanfor deleted or not +
                         * 	one int for number of columns
                        */
                        bufOut.writeInt(dataSizeReturned + utfPrefix_+cfName.length() + 4 + 1);
                        /* write the column family name */
                        bufOut.writeUTF(cfName);
                        /* write if this cf is marked for delete */
                        bufOut.writeBoolean(markedForDelete);
                        /* write number of columns */
                        bufOut.writeInt(numColsReturned);
                        int prevPosition = 0;
                        /* now write all the columns we are required to write */
                        for(ColumnPositionInfo colRange : columnRangeList)
                        {
                            /* seek to the correct offset to the data */
                            file_.skipBytes(colRange.start() - prevPosition);
                        	bufOut.write(file_, colRange.end() - colRange.start());
                        	prevPosition = colRange.end();
                        }
                    }
                }
                else
                {
                    /* skip over data portion */
                	file_.seek(dataSize + file_.getFilePointer());
                }

                long endPosition = file_.getFilePointer();
                bytesRead = endPosition - startPosition;
            }

            return bytesRead;
        }

        /**
         * This method dumps the next key/value into the DataOuputStream
         * passed in.
         *
         * @param dos DataOutputStream that needs to be filled.
         * @param columnFamilyName the column family whose value is to be returned
         * @return total number of bytes read/considered
        */
        public long next(DataOutputBuffer bufOut, String columnFamilyName) throws IOException
        {
    		long bytesRead = -1L;
            if ( isEOF() )
                return bytesRead;

            long startPosition = file_.getFilePointer();
            String key = file_.readUTF();
            if ( key != null )
            {
                /* write the key into buffer */
                bufOut.writeUTF( key );

                int dataSize = file_.readInt();

                if(DatabaseDescriptor.isNameIndexEnabled(columnFamilyName))
                {
	                /* skip index file */
	            	int bytesSkipped = IndexHelper.skip(file_);
	                /*
	                 * read the correct number of bytes for the column family and
	                 * write data into buffer
	                */
	            	dataSize -= bytesSkipped;
                }
                /* write data size into buffer */
                bufOut.writeInt(dataSize);
                /* write the data into buffer */
                bufOut.write(file_, dataSize);
                long endPosition = file_.getFilePointer();
                bytesRead = endPosition - startPosition;
            }

            return bytesRead;
        }

        /**
         * This method dumps the next key/value into the DataOuputStream
         * passed in.
         *
         * @param dos - DataOutputStream that needs to be filled.
         * @return total number of bytes read/considered
         */
        public long next(DataOutputBuffer bufOut) throws IOException
        {
            long bytesRead = -1L;
            if ( isEOF() )
                return bytesRead;

            long startPosition = file_.getFilePointer();
            String key = file_.readUTF();
            if ( key != null )
            {
                /* write the key into buffer */
                bufOut.writeUTF( key );

                int dataSize = file_.readInt();
                /* write data size into buffer */
                bufOut.writeInt(dataSize);
                /* write the data into buffer */
                bufOut.write(file_, dataSize);
                long endPosition = file_.getFilePointer();
                bytesRead = endPosition - startPosition;
            }

            return bytesRead;
        }

        /**
         * This method dumps the next key/value into the DataOuputStream
         * passed in.
         *
         * @param key - key we are interested in.
         * @param dos - DataOutputStream that needs to be filled.
         * @return total number of bytes read/considered
         */
        public long next(String key, DataOutputBuffer bufOut) throws IOException
        {
            long bytesRead = -1L;
            if ( isEOF() )
                return bytesRead;

            long startPosition = file_.getFilePointer();
            String keyInDisk = file_.readUTF();
            if ( keyInDisk != null )
            {
                /*
                 * If key on disk is greater than requested key
                 * we can bail out since we exploit the property
                 * of the SSTable format.
                */
                if ( keyInDisk.compareTo(key) > 0 )
                    return bytesRead;

                /*
                 * If we found the key then we populate the buffer that
                 * is passed in. If not then we skip over this key and
                 * position ourselves to read the next one.
                */
                int dataSize = file_.readInt();
                if ( keyInDisk.equals(key) )
                {
                    /* write the key into buffer */
                    bufOut.writeUTF( keyInDisk );
                    /* write data size into buffer */
                    bufOut.writeInt(dataSize);
                    /* write the data into buffer */
                    bufOut.write(file_, dataSize);
                }
                else
                {
                    /* skip over data portion */
                	file_.seek(dataSize + file_.getFilePointer());
                }

                long endPosition = file_.getFilePointer();
                bytesRead = endPosition - startPosition;
            }

            return bytesRead;
        }
    }

    public static class Reader extends AbstractReader
    {
        Reader(String filename) throws FileNotFoundException
        {
            super(filename);
            file_ = new RandomAccessFile(filename, "r");
        }

        public long getEOF() throws IOException
        {
            return file_.length();
        }

        public long getCurrentPosition() throws IOException
        {
            return file_.getFilePointer();
        }

        public boolean isHealthyFileDescriptor() throws IOException
        {
            return file_.getFD().valid();
        }

        public void seek(long position) throws IOException
        {
            file_.seek(position);
        }

        public boolean isEOF() throws IOException
        {
            return ( getCurrentPosition() == getEOF() );
        }

        /**
         * Be extremely careful while using this API. This currently
         * used to read the commit log header from the commit logs.
         * Treat this as an internal API.
         * @param bytes read from the buffer into the this array
        */
        public void readDirect(byte[] bytes) throws IOException
        {
            file_.readFully(bytes);
        }

        public void close() throws IOException
        {
            file_.close();
        }
    }

    public static class BufferReader extends AbstractReader
    {
        private long position_ = 0L;

        BufferReader(String filename, int size) throws FileNotFoundException
        {
            super(filename);
            file_ = new BufferedRandomAccessFile(filename, "r", size);
        }

        public long getEOF() throws IOException
        {
            return file_.length();
        }

        public long getCurrentPosition() throws IOException
        {
            return file_.getFilePointer();
        }

        public boolean isHealthyFileDescriptor() throws IOException
        {
            return file_.getFD().valid();
        }

        public void seek(long position) throws IOException
        {
            file_.seek(position);
        }

        public boolean isEOF() throws IOException
        {
            return ( getCurrentPosition() == getEOF() );
        }

        /**
         * Be extremely careful while using this API. This currently
         * used to read the commit log header from the commit logs.
         * Treat this as an internal API.
         * @param bytes read from the buffer into the this array
        */
        public void readDirect(byte[] bytes) throws IOException
        {
            file_.readFully(bytes);
        }

        public void close() throws IOException
        {
            file_.close();
        }
    }

    public static class MemoryMappedReader extends AbstractReader
    {
        private MappedByteBuffer buffer_;

        MemoryMappedReader(String filename, long start, long end) throws IOException
        {
            super(filename);
            map(start, end);
        }

        public void map() throws IOException
        {
            RandomAccessFile file = new RandomAccessFile(filename_, "rw");
            try
            {
                buffer_ = file.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, file.length() );
                buffer_.load();
            }
            finally
            {
                file.close();
            }
        }

        public void map(long start, long end) throws IOException
        {
            if ( start < 0 || end < 0 || end < start )
                throw new IllegalArgumentException("Invalid values for start and end.");

            RandomAccessFile file = new RandomAccessFile(filename_, "rw");
            try
            {
                if ( end == 0 )
                    end = file.length();
                buffer_ = file.getChannel().map(FileChannel.MapMode.READ_ONLY, start, end);
                buffer_.load();
            }
            finally
            {
                file.close();
            }
        }

        void unmap(final Object buffer)
        {
            AccessController.doPrivileged( new PrivilegedAction<MappedByteBuffer>()
                    {
                public MappedByteBuffer run()
                {
                    try
                    {
                        Method getCleanerMethod = buffer.getClass().getMethod("cleaner", new Class[0]);
                        getCleanerMethod.setAccessible(true);
                        sun.misc.Cleaner cleaner = (sun.misc.Cleaner)getCleanerMethod.invoke(buffer,new Object[0]);
                        cleaner.clean();
                    }
                    catch(Throwable e)
                    {
                        logger_.debug( LogUtil.throwableToString(e) );
                    }
                    return null;
                }
                    });
        }

        public void seek(long position) throws IOException
        {
            buffer_.position((int)position);
        }

        public long getCurrentPosition() throws IOException
        {
            return buffer_.position();
        }

        /**
         * Be extremely careful while using this API. This currently
         * used to read the commit log header from the commit logs.
         * Treat this as an internal API.
         * @param bytes read from the buffer into the this array
         */
        public void readDirect(byte[] bytes) throws IOException
        {
            buffer_.get(bytes);
        }

        public boolean isEOF()
        {
            return ( buffer_.remaining() == 0 );
        }

        public long getEOF() throws IOException
        {
            return buffer_.limit();
        }

        public void close() throws IOException
        {
            unmap(buffer_);
        }

        public boolean isHealthyFileDescriptor()
        {
            throw new UnsupportedOperationException("This operation is unsupported for the MemoryMappedReader");
        }

        /**
         * This method dumps the next key/value into the DataOuputStream
         * passed in. Always use this method to query for application
         * specific data as it will have indexes.
         *
         * param @ key - key we are interested in.
         * param @ dos - DataOutputStream that needs to be filled.
         * param @ readColIndexes - if column indexes are present, read
         *                          only the column indexes if this parameter
         *                          is true. If not, read the whole column
         *                          family including the column indexes.
        */
        public long next(String key, DataOutputBuffer bufOut, String cf) throws IOException
        {
            String[] values = RowMutation.getColumnAndColumnFamily(cf);
            String columnFamilyName = values[0];
            // String columnName = (values[1] == null || values[1].length() == 0)?null:values[1];
            String columnName = (values.length == 1) ? null : values[1];

            long bytesRead = -1L;
            if ( isEOF() )
                return bytesRead;

            long startPosition = buffer_.position();
            String keyInDisk = SequenceFile.readUTF(buffer_);
            if ( keyInDisk != null )
            {
                /*
                 * If key on disk is greater than requested key
                 * we can bail out since we exploit the property
                 * of the SSTable format.
                */
                if ( keyInDisk.compareTo(key) > 0 )
                    return bytesRead;

                /*
                 * If we found the key then we populate the buffer that
                 * is passed in. If not then we skip over this key and
                 * position ourselves to read the next one.
                */
                int dataSize = buffer_.getInt();
                if ( keyInDisk.equals(key) )
                {
                    /* write the key into buffer */
                    bufOut.writeUTF( keyInDisk );

                    /* if there is no column indexing enabled on this column then there are no indexes for it */
                    if(!DatabaseDescriptor.isNameIndexEnabled(columnFamilyName))
                    {
                        /* write the data size */
                        bufOut.writeInt(dataSize);
                        /* write the data into buffer, except the boolean we have read */
                        bufOut.write(buffer_, dataSize);
                    }
                    /* if we need to read the all the columns do not read the column indexes */
                    else if(columnName == null)
                    {
                        int bytesSkipped = IndexHelper.skip(buffer_);
                        /*
                         * read the correct number of bytes for the column family and
                         * write data into buffer
                        */
                        dataSize -= bytesSkipped;
                        /* write the data size */
                        bufOut.writeInt(dataSize);
                        /* write the data into buffer, except the boolean we have read */
                        bufOut.write(buffer_, dataSize);
                    }
                    else
                    {
                        /* check if we have an index */
                        boolean hasColumnIndexes = file_.readBoolean();
                        int totalBytesRead = 1;
                        List<ColumnPositionInfo> columnIndexList = null;
                        /* if we do then deserialize the index */
                        if(hasColumnIndexes)
                        {
                            columnIndexList = new ArrayList<IndexHelper.ColumnPositionInfo>();
                            /* read the index */
                            totalBytesRead += IndexHelper.deserializeIndex(buffer_, columnIndexList);
                        }
                        dataSize -= totalBytesRead;

                        /* read the column family name */
                        String cfName = SequenceFile.readUTF(buffer_);
                        dataSize -= (utfPrefix_ + cfName.length());

                        /* read if this cf is marked for delete */
                        boolean markedForDelete = file_.readBoolean();
                        dataSize -= 1;

                        /* read the total number of columns */
                        int totalNumCols = buffer_.getInt();
                        dataSize -= 4;

                        /* get the column range we have to read */
                        IndexHelper.ColumnPositionInfo columnRange = IndexHelper.getColumnRangeFromIndex(columnName, columnIndexList, dataSize, totalNumCols);

                        /* seek to the correct offset to the data, and calculate the data size */
                        buffer_.position(columnRange.start());
                        dataSize = columnRange.end() - columnRange.start();

                        /*
                         * write the number of columns in the column family we are returning:
                         *  dataSize that we are reading +
                         *  length of column family name +
                         *  one booleanfor deleted or not +
                         *  one int for number of columns
                        */
                        bufOut.writeInt(dataSize + utfPrefix_+cfName.length() + 4 + 1);
                        /* write the column family name */
                        bufOut.writeUTF(cfName);
                        /* write if this cf is marked for delete */
                        bufOut.writeBoolean(markedForDelete);
                        /* write number of columns */
                        bufOut.writeInt(columnRange.numColumns());
                        /* now write the columns */
                        bufOut.write(buffer_, dataSize);
                    }
                }
                else
                {
                    /* skip over data portion */
                    seek(dataSize + buffer_.position());
                }

                long endPosition = buffer_.position();
                bytesRead = endPosition - startPosition;
            }

            return bytesRead;
        }

        /**
         * This method dumps the next key/value into the DataOuputStream
         * passed in. Always use this method to query for application
         * specific data as it will have indexes.
         *
         * param @ key - key we are interested in.
         * param @ dos - DataOutputStream that needs to be filled.
         * param @ cfName - The name of the column family only without the ":"
         * param @ columnNames - The list of columns in the cfName column family
         *                       that we want to return
         *
        */
        public long next(String key, DataOutputBuffer bufOut, String cf, List<String> columnNames) throws IOException
        {
            String[] values = RowMutation.getColumnAndColumnFamily(cf);
            String columnFamilyName = values[0];

            long bytesRead = -1L;
            if ( isEOF() )
                return bytesRead;

            long startPosition = buffer_.position();
            String keyInDisk = SequenceFile.readUTF(buffer_);
            if ( keyInDisk != null )
            {
                /*
                 * If key on disk is greater than requested key
                 * we can bail out since we exploit the property
                 * of the SSTable format.
                */
                if ( keyInDisk.compareTo(key) > 0 )
                    return bytesRead;

                /*
                 * If we found the key then we populate the buffer that
                 * is passed in. If not then we skip over this key and
                 * position ourselves to read the next one.
                */
                int dataSize = buffer_.getInt();
                if ( keyInDisk.equals(key) )
                {
                    /* write the key into buffer */
                    bufOut.writeUTF( keyInDisk );

                    /* if there is no column indexing enabled on this column then there are no indexes for it */
                    if(!DatabaseDescriptor.isNameIndexEnabled(columnFamilyName))
                    {
                        /* write the data size */
                        bufOut.writeInt(dataSize);
                        /* write the data into buffer, except the boolean we have read */
                        bufOut.write(buffer_, dataSize);
                    }
                    /* if we need to read the all the columns do not read the column indexes */
                    else if(columnNames == null || columnNames.isEmpty() )
                    {
                        int bytesSkipped = IndexHelper.skip(buffer_);
                        /*
                         * read the correct number of bytes for the column family and
                         * write data into buffer
                        */
                        dataSize -= bytesSkipped;
                        /* write the data size */
                        bufOut.writeInt(dataSize);
                        /* write the data into buffer, except the boolean we have read */
                        bufOut.write(buffer_, dataSize);
                    }
                    else
                    {
                        /* check if we have an index */
                        boolean hasColumnIndexes = SequenceFile.readBoolean(buffer_);
                        int totalBytesRead = 1;
                        List<ColumnPositionInfo> columnIndexList = null;
                        /* if we do then deserialize the index */
                        if(hasColumnIndexes)
                        {
                            columnIndexList = new ArrayList<IndexHelper.ColumnPositionInfo>();
                            /* read the index */
                            totalBytesRead += IndexHelper.deserializeIndex(buffer_, columnIndexList);
                        }
                        dataSize -= totalBytesRead;

                        /* read the column family name */
                        String cfName = SequenceFile.readUTF(buffer_);
                        dataSize -= (utfPrefix_ + cfName.length());

                        /* read if this cf is marked for delete */
                        boolean markedForDelete = SequenceFile.readBoolean(buffer_);
                        dataSize -= 1;

                        /* read the total number of columns */
                        int totalNumCols = buffer_.getInt();
                        dataSize -= 4;

                        // TODO: this is name sorted - but eventually this should be sorted by the same criteria as the col index
                        /* sort the required list of columns */
                        Collections.sort(columnNames);
                        /* get the various column ranges we have to read */
                        List<IndexHelper.ColumnPositionInfo> columnRangeList = IndexHelper.getMultiColumnRangesFromIndex(columnNames, columnIndexList, dataSize, totalNumCols);

                        /* calculate the data size */
                        int numColsReturned = 0;
                        int dataSizeReturned = 0;
                        for(ColumnPositionInfo colRange : columnRangeList)
                        {
                            numColsReturned += colRange.numColumns();
                            dataSizeReturned += colRange.end() - colRange.start();
                        }

                        /*
                         * write the number of columns in the column family we are returning:
                         *  dataSize that we are reading +
                         *  length of column family name +
                         *  one booleanfor deleted or not +
                         *  one int for number of columns
                        */
                        bufOut.writeInt(dataSizeReturned + utfPrefix_+cfName.length() + 4 + 1);
                        /* write the column family name */
                        bufOut.writeUTF(cfName);
                        /* write if this cf is marked for delete */
                        bufOut.writeBoolean(markedForDelete);
                        /* write number of columns */
                        bufOut.writeInt(numColsReturned);
                        int prevPosition = 0;
                        /* now write all the columns we are required to write */
                        for(ColumnPositionInfo colRange : columnRangeList)
                        {
                            /* seek to the correct offset to the data */
                            file_.skipBytes(colRange.start() - prevPosition);
                            bufOut.write(buffer_, colRange.end() - colRange.start());
                            prevPosition = colRange.end();
                        }
                    }
                }
                else
                {
                    /* skip over data portion */
                    seek(dataSize + buffer_.position());
                }

                long endPosition = buffer_.position();
                bytesRead = endPosition - startPosition;
            }

            return bytesRead;
        }

        /**
         * This method dumps the next key/value into the DataOuputStream
         * passed in.
         *
         * param @ dos - DataOutputStream that needs to be filled.
        */
        public long next(DataOutputBuffer bufOut, String columnFamilyName) throws IOException
        {
            long bytesRead = -1L;
            if ( isEOF() )
                return bytesRead;

            long startPosition = buffer_.position();
            String key = SequenceFile.readUTF(buffer_);
            if ( key != null )
            {
                /* write the key into buffer */
                bufOut.writeUTF( key );

                int dataSize = buffer_.getInt();

                if(DatabaseDescriptor.isNameIndexEnabled(columnFamilyName))
                {
                    /* skip index file */
                    int bytesSkipped = IndexHelper.skip(buffer_);
                    /*
                     * read the correct number of bytes for the column family and
                     * write data into buffer
                    */
                    dataSize -= bytesSkipped;
                }
                /* write data size into buffer */
                bufOut.writeInt(dataSize);
                /* write the data into buffer */
                bufOut.write(buffer_, dataSize);
                long endPosition = buffer_.position();
                bytesRead = endPosition - startPosition;
            }

            return bytesRead;
        }

        /*
         * This method dumps the next key/value into the DataOuputStream
         * passed in.
         *
         * param @ dos - DataOutputStream that needs to be filled.
        */
        public long next(DataOutputBuffer bufOut) throws IOException
        {
            long bytesRead = -1L;
            if ( isEOF() )
                return bytesRead;

            long startPosition = buffer_.position();
            String key = SequenceFile.readUTF(buffer_);
            if ( key != null )
            {
                /* write the key into buffer */
                bufOut.writeUTF( key );

                int dataSize = buffer_.getInt();
                /* write data size into buffer */
                bufOut.writeInt(dataSize);
                /* write the data into buffer */
                bufOut.write(buffer_, dataSize);
                long endPosition = buffer_.position();
                bytesRead = endPosition - startPosition;
            }

            return bytesRead;
        }

        /**
         * This method dumps the next key/value into the DataOuputStream
         * passed in.
         *
         * param @ key - key we are interested in.
         * param @ dos - DataOutputStream that needs to be filled.
        */
        public long next(String key, DataOutputBuffer bufOut) throws IOException
        {
            long bytesRead = -1L;
            if ( isEOF() )
                return bytesRead;

            long startPosition = buffer_.position();
            String keyInDisk = SequenceFile.readUTF(buffer_);
            if ( keyInDisk != null )
            {
                /*
                 * If key on disk is greater than requested key
                 * we can bail out since we exploit the property
                 * of the SSTable format.
                */
                if ( keyInDisk.compareTo(key) > 0 )
                    return bytesRead;

                /*
                 * If we found the key then we populate the buffer that
                 * is passed in. If not then we skip over this key and
                 * position ourselves to read the next one.
                */
                int dataSize = buffer_.getInt();
                if ( keyInDisk.equals(key) )
                {
                    /* write the key into buffer */
                    bufOut.writeUTF( keyInDisk );
                    /* write data size into buffer */
                    bufOut.writeInt(dataSize);
                    /* write the data into buffer */
                    bufOut.write(buffer_, dataSize);
                }
                else
                {
                    /* skip over data portion */
                    seek(dataSize + buffer_.position());
                }

                long endPosition = buffer_.position();
                bytesRead = endPosition - startPosition;
            }

            return bytesRead;
        }
    }

    private static Logger logger_ = Logger.getLogger( SequenceFile.class ) ;
    public static final short utfPrefix_ = 2;
    static final String marker_ = "Bloom-Filter";

    public static IFileWriter writer(String filename) throws IOException
    {
        return new Writer(filename);
    }

    public static IFileWriter bufferedWriter(String filename, int size) throws IOException
    {
        return new BufferWriter(filename, size);
    }

    public static IFileWriter concurrentWriter(String filename) throws IOException
    {
        return new ConcurrentWriter(filename);
    }

    public static IFileWriter fastWriter(String filename, int size) throws IOException
    {
        return new FastConcurrentWriter(filename, size);
    }

    public static IFileReader reader(String filename) throws FileNotFoundException
    {
        return new Reader(filename);
    }

    public static IFileReader bufferedReader(String filename, int size) throws IOException
    {
        return new BufferReader(filename, size);
    }

    public static IFileReader memoryMappedReader(String filename, long start, long end) throws IOException
    {
        return new MemoryMappedReader(filename, start, end);
    }

    public static boolean readBoolean(ByteBuffer buffer)
    {
        return ( buffer.get() == 1 ? true : false );
    }

    /**
     * Efficiently writes a UTF8 string to the buffer.
     * Assuming all Strings that are passed in have length
     * that can be represented as a short i.e length of the
     * string is <= 65535
     * @param buffer buffer to write the serialize version into
     * @param str string to serialize
    */
    private static void writeUTF(ByteBuffer buffer, String str)
    {
        int strlen = str.length();
        int utflen = 0;
        int c, count = 0;

        /* use charAt instead of copying String to char array */
        for (int i = 0; i < strlen; i++)
        {
            c = str.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F))
            {
                utflen++;
            }
            else if (c > 0x07FF)
            {
                utflen += 3;
            }
            else
            {
                utflen += 2;
            }
        }

        byte[] bytearr = new byte[utflen + 2];
        bytearr[count++] = (byte) ((utflen >>> 8) & 0xFF);
        bytearr[count++] = (byte) ((utflen >>> 0) & 0xFF);

        int i = 0;
        for (i = 0; i < strlen; i++)
        {
            c = str.charAt(i);
            if (!((c >= 0x0001) && (c <= 0x007F)))
                break;
            bytearr[count++] = (byte) c;
        }

        for (; i < strlen; i++)
        {
            c = str.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F))
            {
                bytearr[count++] = (byte) c;

            }
            else if (c > 0x07FF)
            {
                bytearr[count++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
                bytearr[count++] = (byte) (0x80 | ((c >> 6) & 0x3F));
                bytearr[count++] = (byte) (0x80 | ((c >> 0) & 0x3F));
            }
            else
            {
                bytearr[count++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
                bytearr[count++] = (byte) (0x80 | ((c >> 0) & 0x3F));
            }
        }
        buffer.put(bytearr, 0, utflen + 2);
    }

    /**
     * Read a UTF8 string from a serialized buffer.
     * @param buffer buffer from which a UTF8 string is read
     * @return a Java String
    */
    private static String readUTF(ByteBuffer in) throws IOException
    {
        int utflen = in.getShort();
        byte[] bytearr = new byte[utflen];
        char[] chararr = new char[utflen];

        int c, char2, char3;
        int count = 0;
        int chararr_count = 0;

        in.get(bytearr, 0, utflen);

        while (count < utflen)
        {
            c = (int) bytearr[count] & 0xff;
            if (c > 127)
                break;
            count++;
            chararr[chararr_count++] = (char) c;
        }

        while (count < utflen)
        {
            c = (int) bytearr[count] & 0xff;
            switch (c >> 4)
            {
            case 0:
            case 1:
            case 2:
            case 3:
            case 4:
            case 5:
            case 6:
            case 7:
                /* 0xxxxxxx */
                count++;
                chararr[chararr_count++] = (char) c;
                break;
            case 12:
            case 13:
                /* 110x xxxx 10xx xxxx */
                count += 2;
                if (count > utflen)
                    throw new UTFDataFormatException(
                    "malformed input: partial character at end");
                char2 = (int) bytearr[count - 1];
                if ((char2 & 0xC0) != 0x80)
                    throw new UTFDataFormatException(
                            "malformed input around byte " + count);
                chararr[chararr_count++] = (char) (((c & 0x1F) << 6) | (char2 & 0x3F));
                break;
            case 14:
                /* 1110 xxxx 10xx xxxx 10xx xxxx */
                count += 3;
                if (count > utflen)
                    throw new UTFDataFormatException(
                    "malformed input: partial character at end");
                char2 = (int) bytearr[count - 2];
                char3 = (int) bytearr[count - 1];
                if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80))
                    throw new UTFDataFormatException(
                            "malformed input around byte " + (count - 1));
                chararr[chararr_count++] = (char) (((c & 0x0F) << 12)
                        | ((char2 & 0x3F) << 6) | ((char3 & 0x3F) << 0));
                break;
            default:
                /* 10xx xxxx, 1111 xxxx */
                throw new UTFDataFormatException("malformed input around byte "
                        + count);
            }
        }
        // The number of chars produced may be less than utflen
        return new String(chararr, 0, chararr_count);
    }
}

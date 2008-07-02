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

import java.io.DataInput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.facebook.infrastructure.io.SSTable.KeyPositionInfo;

/**
 * Provides helper to serialize, deserialize and use column indexes.
 * Author : Karthik Ranganathan ( kranganathan@facebook.com )
 */

public class IndexHelper
{
	/**
	 * Serializes a column index to a data output stream
	 * @param indexSizeInBytes Size of index to be written
	 * @param columnIndexList List of column index entries as objects
	 * @param dos the output stream into which the column index is to be written
	 * @throws IOException
	 */
	public static void serialize(int indexSizeInBytes, List<ColumnPositionInfo> columnIndexList, DataOutputStream dos) throws IOException
	{
		/* if we have no data to index, the write that there is no index present */
		if(indexSizeInBytes == 0 || columnIndexList == null || columnIndexList.size() == 0)
		{
			dos.writeBoolean(false);
		}
		else
		{
	        /* write if we are storing a column index */
	    	dos.writeBoolean(true);

	    	/* write the size of the index */
	    	dos.writeInt(indexSizeInBytes);
	        for( ColumnPositionInfo colPosInfo : columnIndexList )
	        {
	        	/* write the column name */
	            dos.writeUTF(colPosInfo.key());
	            /* write the relative offset */
	            dos.writeInt((int)colPosInfo.position());
	            /* write the number of columns in this bucket */
	            dos.writeInt((int)colPosInfo.numColumns());
	        }
		}
	}

	/**
	 * Skip the index and return the number of bytes read.
	 * @param file the data input from which the index should be skipped
	 * @return number of bytes read from the data input
	 * @throws IOException
	 */
	public static int skip(DataInput file) throws IOException
	{
        /* read if the file has column indexes */
		boolean hasColumnIndexes = file.readBoolean();
        int totalBytesRead = 1;

        if(hasColumnIndexes)
        {
    		/* read only the column index list */
    		int columnIndexSize = file.readInt();
    		totalBytesRead += 4;

            /* skip the column index data */
    		file.skipBytes(columnIndexSize);
    		totalBytesRead += columnIndexSize;
        }

        return totalBytesRead;
	}

	/**
	 * Skip the index and return the number of bytes read.
	 * @param buffer the byte buffer from which the index should be skipped
	 * @return number of bytes read from the data input
	 * @throws IOException
	 */
    public static int skip(ByteBuffer buffer) throws IOException
    {
        /* read if the file has column indexes */
        boolean hasColumnIndexes = ( buffer.get() == 1 ) ? true : false;
        int totalBytesRead = 1;

        if(hasColumnIndexes)
        {
            /* read only the column index list */
            int columnIndexSize = buffer.getInt();
            totalBytesRead += 4;

            /* skip the column index data */
            buffer.position(columnIndexSize);
            totalBytesRead += columnIndexSize;
        }

        return totalBytesRead;
    }

    /**
     * Deserialize the index into a structure and return the number of bytes read.
     * @param file Input from which the serialized form of the index is read
     * @param columnIndexList the structure which is filled in with the deserialized index
     * @return number of bytes read from the input
     * @throws IOException
     */
	static int deserializeIndex(RandomAccessFile file, List<ColumnPositionInfo> columnIndexList) throws IOException
	{
		/* read only the column index list */
		int columnIndexSize = file.readInt();
		int totalBytesRead = 4;

		/* read the indexes into a separate buffer */
		DataOutputBuffer indexOut = new DataOutputBuffer();
        /* write the data into buffer */
		indexOut.write(file, columnIndexSize);
		totalBytesRead += columnIndexSize;

		/* now deserialize the index list */
        DataInputBuffer indexIn = new DataInputBuffer();
        indexIn.reset(indexOut.getData(), indexOut.getLength());
        String columnName;
        int position;
        int numCols;
        while(indexIn.available() > 0)
        {
        	columnName = indexIn.readUTF();
        	position = indexIn.readInt();
        	numCols = indexIn.readInt();
        	columnIndexList.add(new ColumnPositionInfo(columnName, position, numCols));
        }

		return totalBytesRead;
	}

	/**
	 * Deserialize the index into a structure and return the number of bytes read.
	 * @param buffer Input from which the serialized form of the index is read
	 * @param columnIndexList columnIndexList the structure which is filled in with the deserialized index
	 * @return number of bytes read from the input
	 * @throws IOException
	 */
    static int deserializeIndex(ByteBuffer buffer, List<ColumnPositionInfo> columnIndexList) throws IOException
    {
        /* read only the column index list */
        int columnIndexSize = buffer.getInt();
        int totalBytesRead = 4;

        /* read the indexes into a separate buffer */
        DataOutputBuffer indexOut = new DataOutputBuffer();
        /* write the data into buffer */
        indexOut.write(buffer, columnIndexSize);
        totalBytesRead += columnIndexSize;

        /* now deserialize the index list */
        DataInputBuffer indexIn = new DataInputBuffer();
        indexIn.reset(indexOut.getData(), indexOut.getLength());
        String columnName;
        int position;
        int numCols;
        while(indexIn.available() > 0)
        {
            columnName = indexIn.readUTF();
            position = indexIn.readInt();
            numCols = indexIn.readInt();
            columnIndexList.add(new ColumnPositionInfo(columnName, position, numCols));
        }

        return totalBytesRead;
    }

    /**
     * Returns the range in which a given column falls in the index
     * @param column The column whose range needs to be found
     * @param columnIndexList the in-memory representation of the column index
     * @param dataSize the total size of the data
     * @param totalNumCols total number of columns
     * @return an object describing a subrange in which the column is serialized
     */
	static ColumnPositionInfo getColumnRangeFromIndex(String column, List<ColumnPositionInfo> columnIndexList, int dataSize, int totalNumCols)
	{
		/* if column indexes were not present for this column family, the handle accordingly */
		if(columnIndexList == null)
		{
			return (new ColumnPositionInfo(0, dataSize, totalNumCols));
		}

		/* find the offset for the column */
        int size = columnIndexList.size();
        long start = 0;
        long end = dataSize;
        int numColumns = 0;
        int index = Collections.binarySearch(columnIndexList, new KeyPositionInfo(column));
        if ( index < 0 )
        {
            /* We are here which means that the requested column is not an index. */
            index = (++index)*(-1);
        }
        else
        {
        	++index;
        }

        /* calculate the starting offset from which we have to read */
        start = (index == 0) ? 0 : columnIndexList.get(index - 1).position();

        if( index < size )
        {
        	end = columnIndexList.get(index).position();
            numColumns = columnIndexList.get(index).numColumns();
        }
        else
        {
        	end = dataSize;
        	int totalColsIndexed = 0;
	        for( ColumnPositionInfo colPosInfo : columnIndexList )
	        {
	        	totalColsIndexed += colPosInfo.numColumns();
	        }
            numColumns = totalNumCols - totalColsIndexed;
        }

        return (new ColumnPositionInfo((int)start, (int)end, numColumns));
	}

	/**
	 * Returns the sub-ranges that contain the list of columns in columnNames.
	 * @param columnNames The list of columns whose subranges need to be found
	 * @param columnIndexList the deserialized column indexes
	 * @param dataSize the total size of data
	 * @param totalNumCols the total number of columns
	 * @return a list of subranges which contain all the columns in columnNames
	 */
	static List<ColumnPositionInfo> getMultiColumnRangesFromIndex(List<String> columnNames, List<ColumnPositionInfo> columnIndexList, int dataSize, int totalNumCols)
	{
		List<ColumnPositionInfo> columnPosInfoList = new ArrayList<ColumnPositionInfo>();
		ColumnPositionInfo colPosInfo = null;
		Map<Integer,Integer> startPositions  = new HashMap<Integer,Integer>();

		for(String column : columnNames)
		{
			colPosInfo = getColumnRangeFromIndex(column, columnIndexList, dataSize, totalNumCols);

			if( colPosInfo != null && startPositions.get(colPosInfo.start()) == null )
			{
				columnPosInfoList.add(colPosInfo);
				startPositions.put(colPosInfo.start(), colPosInfo.end());
			}
		}

		return columnPosInfoList;
	}

	/**
	 * A helper class to keep track of column positions.
	 */
	public static class ColumnPositionInfo extends KeyPositionInfo
	{
		private int start_;
		private int end_;
		private int numColumns_;

		public ColumnPositionInfo(String key, long position, int numColumns)
		{
			super(key, position);
			numColumns_ = numColumns;
		}

		public ColumnPositionInfo(int start, int end, int numColumns)
		{
			super("");
			start_ = start;
			end_ = end;
			numColumns_ = numColumns;
		}

		public int start()
		{
			return start_;
		}

		public int end()
		{
			return end_;
		}

		public int numColumns()
		{
			return numColumns_;
		}

		boolean equals(ColumnPositionInfo colPosInfo)
		{
			return (colPosInfo.start() == this.start());
		}
	}
}

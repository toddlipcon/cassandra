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

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.facebook.infrastructure.conf.DatabaseDescriptor;
import com.facebook.infrastructure.io.IndexHelper;
import com.facebook.infrastructure.io.SSTable.KeyPositionInfo;
import com.facebook.infrastructure.utils.CassandraUtilities;

/**
 * Help to create an index for a column family based on size of columns
 * Author : Karthik Ranganathan ( kranganathan@facebook.com )
 */

public class ColumnIndexer
{
	/**
	 * Given a column family this, function creates an in-memory structure that represents the
	 * column index for the column family, and subsequently writes it to disk.
	 * @param columnFamily Column family to create index for
	 * @param dos data output stream
	 * @throws IOException
	 */
    public static void serialize(ColumnFamily columnFamily, DataOutputStream dos) throws IOException
	{
        /* we are going to write column indexes */
        int numColumns = 0;
        int position = 0;
        int indexSizeInBytes = 0;
        int sizeSummarized = 0;
        Collection<IColumn> columns = columnFamily.getAllColumns();

        /*
         * Maintains a list of KeyPositionInfo objects for the columns in this
         * column family. The key is the column name and the position is the
         * relative offset of that column name from the start of the list.
         * We do this so that we don't read all the columns into memory.
        */
        List<IndexHelper.ColumnPositionInfo> columnIndexList = new ArrayList<IndexHelper.ColumnPositionInfo>();
        IndexHelper.ColumnPositionInfo columnPositionInfo;

		/*
		 * add per column indexing at the this column level only if indicated in the
         * options and if the number of columns is greater than the threshold.
		*/
        if(DatabaseDescriptor.isNameIndexEnabled(columnFamily.name()))
        {
	        /* column offsets at the right thresholds into the index map. */
	        for ( IColumn column : columns )
	        {
	        	/* if we hit the column index size that we have to index after, go ahead and index it */
	        	if(position - sizeSummarized >= DatabaseDescriptor.getColumnIndexSize())
	        	{
	        		columnPositionInfo = new IndexHelper.ColumnPositionInfo(column.name(), position, numColumns);
	        		columnIndexList.add(columnPositionInfo);
	        		/*
	        		 * we will be writing this object as a UTF8 string and two ints,
	        		 * so calculate the size accordingly. Note that we store the string
	        		 * as UTF-8 encoded, so when we calculate the length, it should be
	            	 * converted to UTF-8.
	            	 */
	        		indexSizeInBytes += CassandraUtilities.getUTF8Length(columnPositionInfo.key()) + IColumn.UtfPrefix_ + 4 + 4;
	        		sizeSummarized = position;
	        		numColumns = 0;
	        	}
	            position += column.serializedSize();
	            ++numColumns;
	        }
	        /* write the column index list */
	    	IndexHelper.serialize(indexSizeInBytes, columnIndexList, dos);
        }
	}


}

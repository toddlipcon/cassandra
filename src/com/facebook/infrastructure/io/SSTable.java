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
import java.nio.channels.FileChannel;
import java.util.*;

import org.apache.log4j.Logger;

import com.facebook.infrastructure.conf.DatabaseDescriptor;
import com.facebook.infrastructure.utils.*;

/**
 * This class is built on top of the SequenceFile. It stores
 * data on disk in sorted fashion. However the sorting is upto
 * the application. This class expects keys to be handed to it
 * in sorted order. SSTable is broken up into blocks where each
 * block contains 128 keys. At the end of every block the block 
 * index is written which contains the offsets to the keys in the
 * block. SSTable also maintains an index file to which every 128th 
 * key is written with a pointer to the block index which is the block 
 * that actually contains the key. This index file is then read and 
 * maintained in memory. SSTable is append only and immutable.
 * 
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */
public class SSTable
{
    private static Logger logger_ = Logger.getLogger(SSTable.class);
    /* use this as a monitor to lock when loading index. */
    private static Object indexLoadLock_ = new Object();
    /* Every 128th key is an index. */
    private static final int indexInterval_ = 128;
    /* Key associated with block index written to disk */
    public static final String blockIndexKey_ = "BLOCK-INDEX";
    /* Required extension for temporary files created during compactions. */
    public static final String temporaryFile_ = "tmp";
    
    /*
     * This map has the SSTable as key and a BloomFilter as value. This
     * BloomFilter will tell us if a key/column pair is in the SSTable.
     * If not we can avoid scanning it.
     */
    private static Map<String, BloomFilter> bfs_ = new Hashtable<String, BloomFilter>();
    /* Maintains a touched set of keys */
    private static LinkedHashMap<String, Long> touchCache_ = new TouchedKeyCache(DatabaseDescriptor.getTouchKeyCacheSize());
    
    /*
     * Section of a file that needs to be scanned
     * is represented by this class.
    */
    private static class Coordinate
    {
    	long start_;
    	long end_;
    	
    	Coordinate(long start, long end)
    	{
    		start_ = start;
    		end_ = end;
    	}
    }
    
    /*
     * This class holds the position of a key in a block
     * and the size of the data associated with this key. 
    */
    protected static class BlockMetadata
    {
        protected static final BlockMetadata NULL = new BlockMetadata(-1L, -1L);
        
        long position_;
        long size_;
        
        BlockMetadata(long position, long size)
        {
            position_ = position;
            size_ = size;
        }
    }
    
    /**
     * This abstraction provides LRU semantics for the keys that are 
     * "touched". Currently it holds the offset of the key in a data
     * file. May change to hold a reference to a IFileReader which
     * memory maps the key and its associated data on a touch.
    */
    private static class TouchedKeyCache extends LinkedHashMap<String, Long>
    {
        private final int capacity_;
        
        TouchedKeyCache(int capacity)
        {
            super(capacity + 1, 1.1f, true);
            capacity_ = capacity;
        }
        
        protected boolean removeEldestEntry(Map.Entry<String, Long> entry)
        {
            return ( size() > capacity_ );
        }
    }
  
    /**
     * This class is a simple container for the file name
     * and the offset within the file we are interested in.
     * 
     * @author alakshman
     *
     */
    private static class FilePositionInfo 
    {
        private String file_;
        private long position_;
        
        FilePositionInfo(String file, long position)
        {
            file_ = file;
            position_ = position;
        }
        
        String file()
        {
            return file_;
        }
        
        long position()
        {
            return position_;
        }
    }
    
    /**
     * This is a simple container for the index Key and its corresponding position
     * in the data file. Binary search is performed on a list of these objects
     * to lookup keys within the SSTable data file.
    */
    public static class KeyPositionInfo implements Comparable<KeyPositionInfo>
    {
        private String key_;
        private long position_;

        public KeyPositionInfo(String key)
        {
            key_ = key;
        }

        public KeyPositionInfo(String key, long position)
        {
            this(key);
            position_ = position;
        }

        public String key()
        {
            return key_;
        }

        public long position()
        {
            return position_;
        }

        public int compareTo(KeyPositionInfo kPosInfo)
        {
            return key_.compareTo(kPosInfo.key_);
        }

        public String toString()
        {
        	return key_ + ":" + position_;
        }
    }
    
    public static int indexInterval()
    {
    	return indexInterval_;
    }
    
    /*
     * Maintains a list of KeyPositionInfo objects per SSTable file loaded.
     * We do this so that we don't read the index file into memory multiple
     * times.
    */
    private static Map<String, List<KeyPositionInfo>> indexMetadataMap_ = new Hashtable<String, List<KeyPositionInfo>>();
    
    /** 
     * This method deletes both the specified data file
     * and the associated index file
     *
     * @param dataFile - data file associated with the SSTable
    */
    public static void delete(String dataFile)
    {
        String iFile = getIndexFileName(dataFile);
        /* remove the cached index table from memory */
        indexMetadataMap_.remove(iFile);

        File file2 = new File(iFile);
        if ( file2.exists() )
        /* delete the index file */
        if (file2.delete())
        {            
            logger_.info("** Deleted " + file2.getName() + " **");
        }   
        else
        {            
            logger_.error("Failed to delete " + file2.getName());
        }
        File file = new File(dataFile);
        if ( file.exists() )
            /* delete the data file */
			if (file.delete())
			{			    
			    logger_.info("** Deleted " + file.getName() + " **");
			}
			else
			{			  
              logger_.error("Failed to delete " + file2.getName());
			}
    }

    private static String getIndexFileName(String dataFile)
    {
        File file = new File(dataFile);
        String dFile = file.getName();
        StringTokenizer st = new StringTokenizer(dFile, "-");
        String iFile = "";
        int i = 0;
        int count = st.countTokens();
        while ( st.hasMoreElements() )
        {
            iFile = iFile + (String)st.nextElement() + "-";
            if ( ++i == count - 1 )
                break;
        }

        iFile += "Index.db";
        iFile = file.getParent() + System.getProperty("file.separator") + iFile;
        return iFile;
    }

    public static int getApproximateKeyCount( List<String> dataFiles)
    {
    	int count = 0 ;

    	for(String dataFile : dataFiles )
    	{
    		String indexFile = getIndexFileName(dataFile);
    		List<KeyPositionInfo> index = indexMetadataMap_.get(indexFile);
    		if (index != null )
    		{
    			int indexKeyCount = index.size();
    			count = count + (indexKeyCount+1) * indexInterval_ ;
    	        logger_.debug("index size for bloom filter calc for file  : " + dataFile + "   : " + count);
    		}
    	}

    	return count;
    }

    /**
     * Get all indexed keys in the SSTable.
    */
    public static List<String> getIndexedKeys()
    {
        List<KeyPositionInfo> keyPositionInfos = new ArrayList<KeyPositionInfo>();

        for ( List<KeyPositionInfo> infos : indexMetadataMap_.values() )
        {
            keyPositionInfos.addAll( infos );
        }

        List<String> indexedKeys = new ArrayList<String>();
        for ( KeyPositionInfo keyPositionInfo : keyPositionInfos )
        {
            indexedKeys.add(keyPositionInfo.key_);
        }

        Collections.sort(indexedKeys);
        return indexedKeys;
    }
    
    /**
     * Intialize the index files and also cache the Bloom Filters
     * associated with these files.
    */
    public static void onStart(List<String> filenames) throws IOException
    {
        for ( String filename : filenames )
        {
            SSTable ssTable = new SSTable(filename);
            ssTable.close();
        }
    }

    /**
     * Stores the Bloom Filter associated with the given file.
    */
    public static void storeBloomFilter(String filename, BloomFilter bf)
    {
        bfs_.put(filename, bf);
    }

    /**
     * Removes the bloom filter associated with the specified file.
    */
    public static void removeAssociatedBloomFilter(String filename)
    {
        bfs_.remove(filename);
    }

    /**
     * Determines if the given key is in the specified file. If the
     * key is not present then we skip processing this file.
    */
    public static boolean isKeyInFile(String key, String filename)
    {
        boolean bVal = false;
        BloomFilter bf = bfs_.get(filename);
        if ( bf != null )
        {
            bVal = bf.isPresent(key);
        }
        return bVal;
    }
    
    public static void punchAHole(String file, long startOffset, long endOffset) throws IOException
    {
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        FileChannel source = raf.getChannel();
        source.position(startOffset);
        
        RandomAccessFile raf2 = new RandomAccessFile(file, "rw");
        FileChannel target = raf2.getChannel();        
        
        target.transferTo(endOffset, (target.size() - endOffset), source);
        source.truncate(source.position());
        source.close();
        target.close();
    }
    
    public static boolean isIndexFile(String file)
    {
        return ( file.indexOf("-Index.db") != -1 );
    }
    
    public static boolean isDataFile(String file)
    {
        return ( file.indexOf("-Data.db") != -1 );
    }

    private String indexFile_;
    private String dataFile_;
    private IFileWriter indexWriter_;
    private IFileWriter dataWriter_;
    private String lastWrittenKey_;
    private int indexKeysWritten_ = 0;
    /* Holds the keys and their respective positions in a block */
    private SortedMap<String, BlockMetadata> blockIndex_ = new TreeMap<String, BlockMetadata>();

    /*
     * This ctor basically gets passed in the full path name
     * of the data file associated with this SSTable. Use this
     * ctor to read the data in this file.
    */
    public SSTable(String dataFileName) throws FileNotFoundException, IOException
    {
        indexFile_ = SSTable.getIndexFileName(dataFileName);
        dataFile_ = dataFileName;
        init();
    }

    /*
     * This ctor is used for writing data into the SSTable. Use this
     * version to write to the SSTable.
    */
    public SSTable(String directory, String filename) throws IOException
    {
        indexFile_ = directory + System.getProperty("file.separator") + filename + "-Index.db";
        dataFile_ = directory + System.getProperty("file.separator") + filename + "-Data.db";
        initWriters();
        init();
    }

    private void initWriters() throws IOException
    {
        indexWriter_ = SequenceFile.writer(indexFile_);
        dataWriter_ = SequenceFile.bufferedWriter(dataFile_, 32*1024*1024);
    }

    private void loadIndexFile() throws FileNotFoundException, IOException
    {
        IFileReader indexReader = SequenceFile.reader(indexFile_);
        indexMetadataMap_.put(indexFile_, new ArrayList<KeyPositionInfo>());
        DataOutputBuffer bufOut = new DataOutputBuffer();
        DataInputBuffer bufIn = new DataInputBuffer();
        int i = 0;
        /* Read the keys in the index file into memory */
        try
        {
            while ( !indexReader.isEOF() )
            {
                bufOut.reset();
                long bytesRead = indexReader.next(bufOut);
                if ( bytesRead != -1 )
                {
                    /* retreive the key and its position in the data file */
                    bufIn.reset(bufOut.getData(), bufOut.getLength());
                    String key = bufIn.readUTF();
                    if ( key.equals(SequenceFile.marker_) )
                    {
                        /*
                         * We are now reading the serialized Bloom Filter. We read
                         * the length and then pass the bufIn to the serializer of
                         * the BloomFilter. We then store the Bloom filter in the
                         * map. However if the Bloom Filter already exists then we
                         * need not read the rest of the file.
                        */
                        if ( bfs_.get(dataFile_) == null )
                        {
                            bufIn.readInt();
                            if ( bfs_.get(dataFile_) == null )
                            	bfs_.put(dataFile_, BloomFilter.serializer().deserialize(bufIn));
                        }
                        break;
                    }
                    /* reading the size of the value */
                    bufIn.readInt();
                    /* we actually don't care about the size because
                     * we know wrote a long for the position.
                    */
                    byte[] longPos = new byte[8];
                    bufIn.readFully(longPos);
                    long position = BasicUtilities.byteArrayToLong(longPos);
                    /*
                     * this should be in sorted order since the MapFile doesn't
                     * tolerate oout of order writes.
                    */
                    indexMetadataMap_.get(indexFile_).add( new KeyPositionInfo(key, position) );
                }
            }
        }
        catch( IOException ex )
        {
        	logger_.warn(LogUtil.throwableToString(ex));
        }
        finally
        {
            indexReader.close();
        }
    }

    private boolean isIndexFileLoadable()
    {
        boolean bVal = false;
        File file = new File(indexFile_);

        if ( file.length() > 0 && (indexMetadataMap_.get(indexFile_) == null) )
            bVal = true;
        return bVal;
    }

    private void init() throws FileNotFoundException, IOException
    {
        /*
         * this is to prevent multiple threads from
         * loading the same index files multiple times
         * into memory.
        */
        synchronized( indexLoadLock_ )
        {
            if ( isIndexFileLoadable() )
            {
                long start = System.nanoTime();
                loadIndexFile();
                logger_.debug("INDEX LOAD TIME: " + (System.nanoTime() - start)/1000 + " ms.");
            }
        }
    }

    private String getFile(String name) throws IOException
    {
        File file = new File(name);
        if ( file.exists() )
            return file.getAbsolutePath();
        throw new IOException("File " + name + " was not found on disk.");
    }

    public String getIndexFileLocation() throws IOException
    {
        return getFile(indexFile_);
    }

    public String getDataFileLocation() throws IOException
    {
        return getFile(dataFile_);
    }

    public long lastModified()
    {
        return dataWriter_.lastModified();
    }
    
    /*
     * Seeks to the specified key on disk.
     * @param fData if set, will read the data off disk to ensure it is in buffer cache
    */
    public void touch(String key, boolean fData) throws IOException
    {
        if ( touchCache_.containsKey(key) )
            return;
        
        IFileReader dataReader = SequenceFile.reader(dataFile_); 
        try
        {
            Coordinate fileCoordinate = getCoordinates(key, dataReader);
            /* Get offset of key from block Index */
            dataReader.seek(fileCoordinate.end_);
            BlockMetadata blockMetadata = dataReader.getBlockMetadata(key);
            if ( blockMetadata.position_ != -1L )
            {
                touchCache_.put(dataFile_ + ":" + key, blockMetadata.position_);                  
            } 
            
            if ( fData )
            {
                /* Read the data associated with this key and pull it into the Buffer Cache */
                if ( blockMetadata.position_ != -1L )
                {
                    dataReader.seek(blockMetadata.position_);
                    DataOutputBuffer bufOut = new DataOutputBuffer();
                    dataReader.next(bufOut);
                    bufOut.reset();
                    logger_.debug("Finished the touch of the key to pull it into buffer cache.");
                }
            }
        }
        finally
        {
            if ( dataReader != null )
                dataReader.close();
        }
    }

    private long beforeAppend(String key) throws IOException
    {
    	if(key == null )
            throw new IOException("Keys must not be null.");
        if ( lastWrittenKey_ != null && key.compareTo(lastWrittenKey_) <= 0 )
        {
            logger_.info("Last written key : " + lastWrittenKey_);
            logger_.info("Current key : " + key);
            logger_.info("Writing into file " + dataFile_);
            throw new IOException("Keys must be written in ascending order.");
        }
        long currentPosition = (lastWrittenKey_ == null) ? 0 : dataWriter_.getCurrentPosition();
        return currentPosition;
    }

    private void afterAppend(String key, long position, long size) throws IOException
    {
        ++indexKeysWritten_;
        lastWrittenKey_ = key;
        blockIndex_.put(key, new BlockMetadata(position, size));
        if ( indexKeysWritten_ == indexInterval_ )
        {
            dumpBlockIndex();        	
            indexKeysWritten_ = 0;
        }                
    }
    
    private void dumpBlockIndex() throws IOException
    {
        DataOutputBuffer bufOut = new DataOutputBuffer();
        
        /* Number of keys in this block */
        bufOut.writeInt(blockIndex_.size());
        for ( Map.Entry<String, BlockMetadata> entry : blockIndex_.entrySet() )
        {            
            bufOut.writeUTF(entry.getKey());
            BlockMetadata blockMetadata = entry.getValue();
            bufOut.writeLong(blockMetadata.position_);
            bufOut.writeLong(blockMetadata.size_);
        }
        
        /* 
         * Record the position where we start writing the block index. This is will be
         * used as the position of the lastWrittenKey in the block in the index file
        */
        long position = dataWriter_.getCurrentPosition();
        /* Write out the block index. */
        dataWriter_.append(SSTable.blockIndexKey_, bufOut);
        blockIndex_.clear();
        /* Write an entry into the Index file */
        indexWriter_.append(lastWrittenKey_, BasicUtilities.longToByteArray(position));
        /* Load this index into the in memory index map */
        List<KeyPositionInfo> keyPositionInfos = SSTable.indexMetadataMap_.get(indexFile_);
        if ( keyPositionInfos == null )
        {
        	keyPositionInfos = new ArrayList<KeyPositionInfo>();
        	SSTable.indexMetadataMap_.put(indexFile_, keyPositionInfos);
        }
        keyPositionInfos.add(new KeyPositionInfo(lastWrittenKey_, position));
    }

    public void append(String key, DataOutputBuffer buffer) throws IOException
    {
        long currentPosition = beforeAppend(key);
        dataWriter_.append(key, buffer);
        afterAppend(key, currentPosition, buffer.getLength());
    }

    public void append(String key, byte[] value) throws IOException
    {
        long currentPosition = beforeAppend(key);
        dataWriter_.append(key, value);
        afterAppend(key, currentPosition, value.length );
    }

    private Coordinate getCoordinates(String key, IFileReader dataReader) throws IOException
    {
    	List<KeyPositionInfo> indexInfo = indexMetadataMap_.get(indexFile_);
    	int size = (indexInfo == null) ? 0 : indexInfo.size();
    	long start = 0L;
    	long end = dataReader.getEOF();
        if ( size > 0 )
        {
            int index = Collections.binarySearch(indexInfo, new KeyPositionInfo(key));
            if ( index < 0 )
            {
                /*
                 * We are here which means that the requested
                 * key is not an index.
                */
                index = (++index)*(-1);
                /*
                 * This means key is not present at all. Hence
                 * a scan is in order.
                */
                start = (index == 0) ? 0 : indexInfo.get(index - 1).position();
                if ( index < size )
                {
                    end = indexInfo.get(index).position();
                }
                else
                {
                    /* This is the Block Index in the file. */
                    end = start;
                }
            }
            else
            {
                /*
                 * If we are here that means the key is in the index file
                 * and we can retrieve it w/o a scan. In reality we would
                 * like to have a retreive(key, fromPosition) but for now
                 * we use scan(start, start + 1) - a hack.
                */
                start = indexInfo.get(index).position();                
                end = start;
            }
        }
        else
        {
            /*
             * We are here which means there are less than
             * 128 keys in the system and hence our only recourse
             * is a linear scan from start to finish. Automatically
             * use memory mapping since we have a huge file and very
             * few keys.
            */
            end = dataReader.getEOF();
        }  
        
        return new Coordinate(start, end);
    }
    
    public DataInputBuffer next(String key, String cf, List<String> cNames) throws IOException
    {
    	DataInputBuffer bufIn = null;
        IFileReader dataReader = SequenceFile.reader(dataFile_);
        try
        {
        	Coordinate fileCoordinate = getCoordinates(key, dataReader);

            /*
             * we have the position we have to read from in order to get the
             * column family, get the column family and column(s) needed.
            */        	
            bufIn = getData(dataReader, key, cf, cNames, fileCoordinate);
        }
        finally
        {
            if ( dataReader != null )
                dataReader.close();
        }
        return bufIn;
    }
    
    public DataInputBuffer next(String key, String columnName) throws IOException
    {
        DataInputBuffer bufIn = null;
        IFileReader dataReader = SequenceFile.reader(dataFile_);
        
        try
        {
        	Coordinate fileCoordinate = getCoordinates(key, dataReader);
            /*
             * we have the position we have to read from in order to get the
             * column family, get the column family and column(s) needed.
            */            
            bufIn = getData(dataReader, key, columnName, fileCoordinate);
        }
        finally
        {
            if ( dataReader != null )
                dataReader.close();
        }
        return bufIn;
    }
    
    long getSeekPosition(String key, long start)
    {
        Long seekStart = touchCache_.get(dataFile_ + ":" + key);
        if( seekStart != null)
        {
            return seekStart;
        }
        return start;
    }
    
    /*
     * Get the data for the key from the position passed in. 
    */
    private DataInputBuffer getData(IFileReader dataReader, String key, String column, Coordinate section) throws IOException
    {
        DataOutputBuffer bufOut = new DataOutputBuffer();
        DataInputBuffer bufIn = new DataInputBuffer();
                  
        /* Goto the Block Index */
        dataReader.seek(section.end_);
        long position = dataReader.getPositionFromBlockIndex(key);
        
        if ( position == -1L )
            return bufIn;
        
        dataReader.seek(position);
        dataReader.next(key, bufOut, column);
        if ( bufOut.getLength() > 0 )
        {                        
            bufIn.reset(bufOut.getData(), bufOut.getLength());
            /* read the key even though we do not use it */
            bufIn.readUTF();
            bufIn.readInt();            
        }
        return bufIn;
    }
    
    private DataInputBuffer getData(IFileReader dataReader, String key, String cf, List<String> columns, Coordinate section) throws IOException
    {
        DataOutputBuffer bufOut = new DataOutputBuffer();
        DataInputBuffer bufIn = new DataInputBuffer();
                  
        /* Goto the Block Index */
        dataReader.seek(section.end_);
        long position = dataReader.getPositionFromBlockIndex(key);
        
        if ( position == -1L )
            return bufIn;
        
        dataReader.seek(position);
        dataReader.next(key, bufOut, cf, columns);
        if ( bufOut.getLength() > 0 )
        {                        
            bufIn.reset(bufOut.getData(), bufOut.getLength());
            /* read the key even though we do not use it */
            bufIn.readUTF();
            bufIn.readInt();            
        }
        return bufIn;
    }

    /*
     * This method scans for the key in the data file in
     * the range specified via the start and the end params.
     *
     *  param @ key - key whose value we are interested in.
     *  param @ start - scan from this position
     *  param @ end - scan till this position
    */
    private DataInputBuffer scan(IFileReader dataReader, String key, long start, long end) throws IOException
    {
        DataOutputBuffer bufOut = new DataOutputBuffer();
        DataInputBuffer bufIn = new DataInputBuffer();
        long bufPtr = start;

        long startTime = System.nanoTime();
        /* Seek to start position */
        dataReader.seek(start);
        while ( bufPtr < end )
        {
            bufOut.reset();
            long bytesRead = dataReader.next(key, bufOut);
            /* If we hit EOF we bail */
            if ( bytesRead == -1 )
                break;

            if ( bufOut.getLength() > 0 )
            {
                bufIn.reset(bufOut.getData(), bufOut.getLength());
                /* read the key even though we do not use it */
                bufIn.readUTF();
                bufIn.readInt();
                break;
            }
            bufPtr += bytesRead;
        }

        logger_.debug("SCAN TIME for key " + key + " in file " + dataFile_ + " : " + (System.nanoTime() - startTime)/1000 + " ms.");
        return bufIn;
    }

    /*
     * This method scans for the key in the data file in
     * the range specified via the start and the end params.
     *
     *  param @ key - key whose value we are interested in.
     *  param @ start - scan from this position
     *  param @ end - scan till this position
     *  param @ cf - name of the actual column with ":" separation
    */
    private DataInputBuffer scan(IFileReader dataReader, String key, long start, long end, String column) throws IOException
    {
        DataOutputBuffer bufOut = new DataOutputBuffer();
        DataInputBuffer bufIn = new DataInputBuffer();
        long bufPtr = start;

        long startTime = System.nanoTime();
        long seekStart = getSeekPosition(key, start);        
        /* Seek to start position */
        dataReader.seek(seekStart);
        while ( bufPtr < end )
        {
            bufOut.reset();
            long bytesRead = dataReader.next(key, bufOut, column);
            /* If we hit EOF we bail */
            if ( bytesRead == -1 )
                break;

            if ( bufOut.getLength() > 0 )
            {
                logger_.debug("SCAN TIME for key " + key + " in file " + dataFile_ + " : " + (System.nanoTime() - startTime)/1000 + " ms." + " total region : " + (end -start) + "  Region scanned : " + (bufPtr -start));
                startTime = System.nanoTime();
                bufIn.reset(bufOut.getData(), bufOut.getLength());
                /* read the key even though we do not use it */
                bufIn.readUTF();
                bufIn.readInt();
                break;
            }
            bufPtr += bytesRead;
        }
        logger_.debug("READ TIME for key " + key + " in file " + dataFile_ + " : " + (System.nanoTime() - startTime)/1000 + " ms.");
        return bufIn;
    }
    
    /*
     * This method scans for the key in the data file in
     * the range specified via the start and the end params.
     *
     *  param @ dataReader - Data reader to read from file.
     *  param @ key - key whose value we are interested in.
     *  param @ start - scan from this position
     *  param @ end - scan till this position
     *  param @ cf - name of the actual column family w/o ":" separation
     *  param @ cNames - names of the columns in the above column family
    */
    private DataInputBuffer scan(IFileReader dataReader, String key, long start, long end, String cf, List<String> cNames) throws IOException
    {
    	DataOutputBuffer bufOut = new DataOutputBuffer();
        DataInputBuffer bufIn = new DataInputBuffer();
        long bufPtr = start;

        long startTime = System.nanoTime();
        long seekStart = getSeekPosition(key, start);
        /* Seek to start position */
        dataReader.seek(seekStart);
        while ( bufPtr < end )
        {
            bufOut.reset();
            long bytesRead = dataReader.next(key, bufOut, cf, cNames);
            /* If we hit EOF we bail */
            if ( bytesRead == -1 )
                break;

            if ( bufOut.getLength() > 0 )
            {
            	
                logger_.debug("SCAN TIME for key " + key + " in file " + dataFile_ + " : " + (System.nanoTime() - startTime)/1000 + " ms.");
                startTime = System.nanoTime();
                bufIn.reset(bufOut.getData(), bufOut.getLength());
                /* read the key even though we do not use it */
                bufIn.readUTF();
                bufIn.readInt();
                break;
            }
            bufPtr += bytesRead;
        }
        logger_.debug("READ TIME for key " + key + " in file " + dataFile_ + " : " + (System.nanoTime() - startTime)/1000 + " ms.");
        return bufIn;
    }

    private List<byte[]> scan(IFileReader dataReader, String startKey, String endKey, long startPosition, long endPosition) throws IOException
    {
        List<byte[]> values = new ArrayList<byte[]>();
        long bufPtr = startPosition;
        boolean startGathering = false;
        DataOutputBuffer bufOut = new DataOutputBuffer();
        DataInputBuffer bufIn = new DataInputBuffer();
        try
        {
            while ( bufPtr < endPosition )
            {
                bufOut.reset();
                long bytesRead = dataReader.next(bufOut);
                bufIn.reset(bufOut.getData(), bufOut.getLength());
                String keyInDisk = bufIn.readUTF();
                /*
                 * If key on disk is greater than requested key
                 * we can bail out since we exploit the property
                 * of the SSTable.
                */
                if ( keyInDisk.compareTo(startKey) > 0 )
                    break;

                if ( keyInDisk.equals(startKey) )
                {
                    startGathering = true;
                }
                if ( startGathering )
                {
                    int dataSize = bufIn.readInt();
                    byte[] bytes = new byte[dataSize];
                    bufIn.readFully(bytes);
                    values.add(bytes);
                }
                if ( keyInDisk.equals(endKey) )
                    break;
                bufPtr += bytesRead;
            }
        }
        finally
        {
            dataReader.close();
        }
        return values;
    }

    /*
     * Given a key we are interested in this method gets the
     * closest index before the key on disk.
     *
     *  param @ key - key we are interested in.
     *  return position of the closest index before the key
     *  on disk or -1 if this key is not on disk.
    */
    private long getClosestIndexPositionToKeyOnDisk(String key)
    {
        long position = -1L;
        List<KeyPositionInfo> indexInfo = indexMetadataMap_.get(indexFile_);
        int size = indexInfo.size();
        int index = Collections.binarySearch(indexInfo, new KeyPositionInfo(key));
        if ( index < 0 )
        {
            /*
             * We are here which means that the requested
             * key is not an index.
            */
            index = (++index)*(-1);
            /* this means key is not present at all */
            if ( index >= size )
                return position;
            /* a scan is in order. */
            position = (index == 0) ? 0 : indexInfo.get(index - 1).position();
        }
        else
        {
            /*
             * If we are here that means the key is in the index file
             * and we can retrieve it w/o a scan. In reality we would
             * like to have a retreive(key, fromPosition) but for now
             * we use scan(start, start + 1) - a hack.
            */
            position = indexInfo.get(index).position();
        }
        return position;
    }

    /*
     * This method is used to execute range queries against the
     * data file. It is upto the caller to make sure that the keys
     * are contiguous. Values associated with keys in the range
     * [start, end) are retreived.
     *
     * param @ start - startKey key in the range.
     * param @ end - endKey key in the range
    */
    List<byte[]> range(String startKey, String endKey) throws IOException
    {
        List<byte[]> values = new ArrayList<byte[]>();
        IFileReader dataReader = SequenceFile.reader(dataFile_);
        long startPosition = getClosestIndexPositionToKeyOnDisk(startKey);
        /* startKey is not on disk */
        if ( startPosition == -1 )
            return values;
        long endPosition = getClosestIndexPositionToKeyOnDisk(endKey);
        endPosition = (endPosition == -1) ? dataReader.getEOF() : endPosition;
        values = scan(dataReader, startKey, endKey, startPosition, endPosition);
        return values;
    }

    public void close() throws IOException
    {
        close( new byte[0], 0 );
    }

    public void close(BloomFilter bf) throws IOException
    {
        /* Any remnants in the blockIndex should be dumped */
        dumpBlockIndex();
    	/* reset the buffer and serialize the Bloom Filter. */
        DataOutputBuffer bufOut = new DataOutputBuffer();
        BloomFilter.serializer().serialize(bf, bufOut);
        byte[] bytes = new byte[bufOut.getLength()];
        System.arraycopy(bufOut.getData(), 0, bytes, 0, bytes.length);
        close(bytes, bytes.length);
        bufOut.close();
    }

    /**
     * Renames a temporray sstable file to a valid data and index file
     */
    public void closeRename(BloomFilter bf) throws IOException
    {
    	closeRename(bf, null);
    }
    
    public void closeRename(BloomFilter bf, List<String> files) throws IOException
    {
        close( bf);
        String dataFileName = dataFile_.replace("-" + temporaryFile_,"");
        String indexFileName = indexFile_.replace("-" + temporaryFile_,"");
        File dataFile = new File(dataFile_);
        dataFile.renameTo(new File(dataFileName));
        File indexFile = new File(indexFile_);
        indexFile.renameTo(new File(indexFileName));
        dataFile_ = dataFileName;
        /* Now repair the in memory index associated with the old name */
        List<KeyPositionInfo> keyPositionInfos = SSTable.indexMetadataMap_.remove(indexFile_);      
        indexFile_ = indexFileName;             
        SSTable.indexMetadataMap_.put(indexFile_, keyPositionInfos);
        if ( files != null )
        {
            files.add(indexFileName);
            files.add(dataFileName);
        }
    }
    
    private void close(byte[] footer, int size) throws IOException
    {
        if ( size > 0 )
        {
            if ( indexWriter_ != null )
                indexWriter_.close(footer, size);
        }
        else
        {
            if ( indexWriter_ != null )
                indexWriter_.close();
        }

        if ( dataWriter_ != null )
            dataWriter_.close();
    }

    public void dumpIndex(String fileName) throws Throwable
    {
    	loadIndexFile();
    	List<KeyPositionInfo> indexInfo = indexMetadataMap_.get(indexFile_);
    	RandomAccessFile raf = new RandomAccessFile(fileName, "rw");
    	for(KeyPositionInfo kPosition : indexInfo)
    	{
    		raf.writeUTF(kPosition.toString());
    		raf.writeUTF(System.getProperty("line.separator"));
    	}
    	raf.close();
    }
}

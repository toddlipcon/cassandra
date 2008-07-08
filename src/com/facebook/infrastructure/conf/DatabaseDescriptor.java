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

package com.facebook.infrastructure.conf;

import java.util.*;
import java.io.*;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import com.facebook.infrastructure.db.ColumnFamily;
import com.facebook.infrastructure.db.FileUtils;
import com.facebook.infrastructure.utils.XMLUtils;


/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com ) & Karthik Ranganathan ( kranganathan@facebook.com )
 */

public class DatabaseDescriptor
{
    private static int storagePort_ = 7000;
    private static int controlPort_ = 7001;
    private static int httpPort_ = 7002;
    private static String clusterName_ = "Test";
    private static int replicationFactor_ = 3;
    private static long rpcTimeoutInMillis_ = 2000;
    private static Set<String> seeds_ = new HashSet<String>();
    private static String multicastAddr_ = "230.0.0.1";
    private static String metadataDirectory_;
    /* Keeps the list of Ganglia servers to contact */
    private static String[] gangliaServers_ ;
    /* Keeps the list of data file directories */
    private static String[] dataFileDirectories_;
    /* Current index into the above list of directories */
    private static int currentIndex_ = 0;
    private static String stagingFileDirectory_;
    private static String logFileDirectory_;
    private static String bootstrapFileDirectory_;
    private static int logRotationThreshold_ = 128*1024*1024;
    private static boolean fastSync_ = false;
    private static boolean rackAware_ = false;
    private static int threadsPerPool_ = 4;
    private static List<String> tables_ = new ArrayList<String>();
    private static Set<String> applicationColumnFamilies_ = new HashSet<String>();
    private static Map<String, String> cfToColumnTypeMap_ = new HashMap<String, String>();
    private static Map<String, String> cfToIndexPropertyMap_ = new HashMap<String, String>();
    /* if the size of columns or super-columns are more than this, indexing will kick in */
    private static int columnIndexSizeInKB_;
    /* Size of touch key cache */
    private static int touchKeyCacheSize_ = 1024;
    /* Number of hours to keep a memtable in memory */
    private static int memtableLifetime_ = 6;
    /* Address of ZooKeeper cell */
    private static String zkAddress_;
    /* Zookeeper session timeout. */
    private static int zkSessionTimeout_ = 30000;
    /* Should cardinality be altered. */
    private static boolean isCardinalityAltered_ = true;

    public static Map<String, Set<String>> init(String filePath) throws Throwable
    {
        /* Read the configuration file to retrieve DB related properties. */
        String file = filePath + System.getProperty("file.separator") + "storage-conf.xml";
        return initInternal(file);
    }

    public static Map<String, Set<String>> init() throws Throwable
    {
        /* Read the configuration file to retrieve DB related properties. */
        String file = System.getProperty("storage-config") + System.getProperty("file.separator") + "storage-conf.xml";
        return initInternal(file);
    }

    public static Map<String, Set<String>> initInternal(String file) throws Throwable
    {
        String os = System.getProperty("os.name");
        XMLUtils xmlUtils = new XMLUtils(file);
        Node rootNode = xmlUtils.getRequestedNode("/Storage");

        /* Cluster Name */
        clusterName_ = xmlUtils.getNodeValue(rootNode, "ClusterName");

        /* Multicast channel */
        multicastAddr_ = xmlUtils.getNodeValue(rootNode, "MulticastChannel");

        /* Ganglia servers contact list */
        gangliaServers_ = xmlUtils.getNodeValues(rootNode, "GangliaServers/GangliaServer");

        /* ZooKeeper's address */
        zkAddress_ = xmlUtils.getNodeValue(rootNode, "ZookeeperAddress");

        /* Zookeeper's session timeout */
        String zkSessionTimeout = xmlUtils.getNodeValue(rootNode, "ZookeeperSessionTimeout");
        if ( zkSessionTimeout != null )
            zkSessionTimeout_ = Integer.parseInt(zkSessionTimeout);

        /* Data replication factor */
        String replicationFactor = xmlUtils.getNodeValue(rootNode, "ReplicationFactor");
        if ( replicationFactor != null )
        	replicationFactor_ = Integer.parseInt(replicationFactor);

        /* RPC Timeout */
        String rpcTimeoutInMillis = xmlUtils.getNodeValue(rootNode, "RpcTimeoutInMillis");
        if ( rpcTimeoutInMillis != null )
        	rpcTimeoutInMillis_ = Integer.parseInt(rpcTimeoutInMillis);

        /* Thread per pool */
        String threadsPerPool = xmlUtils.getNodeValue(rootNode, "ThreadsPerPool");
        if ( threadsPerPool != null )
            threadsPerPool_ = Integer.parseInt(threadsPerPool);

        /* TCP port on which the storage system listens */
        String port = xmlUtils.getNodeValue(rootNode, "StoragePort");
        if ( port != null )
            storagePort_ = Integer.parseInt(port);

        /* UDP port for control messages */
        port = xmlUtils.getNodeValue(rootNode, "ControlPort");
        if ( port != null )
            controlPort_ = Integer.parseInt(port);

        /* HTTP port for HTTP messages */
        port = xmlUtils.getNodeValue(rootNode, "HttpPort");
        if ( port != null )
            httpPort_ = Integer.parseInt(port);

        /* Touch Key Cache Size */
        String touchKeyCacheSize = xmlUtils.getNodeValue(rootNode, "TouchKeyCacheSize");
        if ( touchKeyCacheSize != null )
            touchKeyCacheSize_ = Integer.parseInt(touchKeyCacheSize);

        /* Number of days to keep the memtable around w/o flushing */
        String lifetime = xmlUtils.getNodeValue(rootNode, "MemtableLifetimeInDays");
        if ( lifetime != null )
            memtableLifetime_ = Integer.parseInt(lifetime);


        /* read the size at which we should do column indexes */
        String columnIndexSizeInKB = xmlUtils.getNodeValue(rootNode, "ColumnIndexSizeInKB");
        if(columnIndexSizeInKB == null)
        {
        	columnIndexSizeInKB_ = 64;
        }
        else
        {
        	columnIndexSizeInKB_ = Integer.parseInt(columnIndexSizeInKB);
        }

        /* metadata directory */
        metadataDirectory_ = xmlUtils.getNodeValue(rootNode, "MetadataDirectory");
        if ( metadataDirectory_ != null )
            FileUtils.createDirectory(metadataDirectory_);
        else
        {
            if ( os.equals("Linux") )
            {
                metadataDirectory_ = "/var/storage/system";
            }
        }

        /* data file directory */
        dataFileDirectories_ = xmlUtils.getNodeValues(rootNode, "DataFileDirectories/DataFileDirectory");
        if ( dataFileDirectories_.length > 0 )
        {
        	for ( String dataFileDirectory : dataFileDirectories_ )
        		FileUtils.createDirectory(dataFileDirectory);
        }
        else
        {
            if ( os.equals("Linux") )
            {
                dataFileDirectories_ = new String[]{"/var/storage/data"};
            }
        }

        /* bootstrap file directory */
        bootstrapFileDirectory_ = xmlUtils.getNodeValue(rootNode, "BootstrapFileDirectory");
        if ( bootstrapFileDirectory_ != null )
            FileUtils.createDirectory(bootstrapFileDirectory_);
        else
        {
            if ( os.equals("Linux") )
            {
                bootstrapFileDirectory_ = "/var/storage/bootstrap";
            }
        }

        /* bootstrap file directory */
        stagingFileDirectory_ = xmlUtils.getNodeValue(rootNode, "StagingFileDirectory");
        if ( stagingFileDirectory_ != null )
            FileUtils.createDirectory(stagingFileDirectory_);
        else
        {
            if ( os.equals("Linux") )
            {
                stagingFileDirectory_ = "/var/storage/staging";
            }
        }

        /* commit log directory */
        logFileDirectory_ = xmlUtils.getNodeValue(rootNode, "CommitLogDirectory");
        if ( logFileDirectory_ != null )
            FileUtils.createDirectory(logFileDirectory_);
        else
        {
            if ( os.equals("Linux") )
            {
                logFileDirectory_ = "/var/storage/commitlog";
            }
        }

        /* threshold after which commit log should be rotated. */
        String value = xmlUtils.getNodeValue(rootNode, "CommitLogRotationThresholdInMB");
        if ( value != null)
            logRotationThreshold_ = Integer.parseInt(value) * 1024 * 1024;

        /* fast sync option */
        value = xmlUtils.getNodeValue(rootNode, "CommitLogFastSync");
        if ( value != null )
            fastSync_ = Boolean.parseBoolean(value);


        /* Rack Aware option */
        value = xmlUtils.getNodeValue(rootNode, "RackAware");
        if ( value != null )
            rackAware_ = Boolean.parseBoolean(value);

        Map<String, Set<String>> tableToColumnFamilyMap = new HashMap<String, Set<String>>();
        /* Read the table related stuff from config */
        NodeList tables = xmlUtils.getRequestedNodeList(rootNode, "/Storage/Tables/Table");
        int size = tables.getLength();
        for ( int i = 0; i < size; ++i )
        {
            Node table = tables.item(i);
            /* parsing out the table name */
            String tName = xmlUtils.getAttributeValue(table, "Name");
            tables_.add(tName);
            tableToColumnFamilyMap.put(tName, new HashSet<String>());

            NodeList columnFamilies = xmlUtils.getRequestedNodeList(table, "ColumnFamily");
            int size2 = columnFamilies.getLength();

            for ( int j = 0; j < size2; ++j )
            {
                Node columnFamily = columnFamilies.item(j);
                String cName = columnFamily.getChildNodes().item(0).getNodeValue();
                /* squirrel away the application column families */
                applicationColumnFamilies_.add(cName);
                /* Parse out the column type */
                String columnType = xmlUtils.getAttributeValue(columnFamily, "ColumnType");
                columnType = ColumnFamily.getColumnType(columnType);
                cfToColumnTypeMap_.put(cName, columnType);
                /* Parse out the column family index property */
                String columnIndexProperty = xmlUtils.getAttributeValue(columnFamily, "Index");
                String columnIndexType = ColumnFamily.getColumnIndexProperty(columnIndexProperty);
                cfToIndexPropertyMap_.put(cName, columnIndexType);
                tableToColumnFamilyMap.get(tName).add(cName);
            }
        }

        /* Load the seeds for node contact points */
        String[] seeds = xmlUtils.getNodeValues(rootNode, "Seeds/Seed");
        for( int i = 0; i < seeds.length; ++i )
        {
            seeds_.add( seeds[i] );
        }
        return tableToColumnFamilyMap;
    }

    public static String getZkAddress()
    {
        return zkAddress_;
    }

    public static int getZkSessionTimeout()
    {
        return zkSessionTimeout_;
    }

    public static String getMulticastChannel()
    {
        return multicastAddr_;
    }

    public static int getColumnIndexSize()
    {
    	return columnIndexSizeInKB_ * 1024;
    }

    public static boolean isAlterCardinality()
    {
        return isCardinalityAltered_;
    }

    public static int getMemtableLifetime()
    {
      return memtableLifetime_;
    }

    public static String getClusterName()
    {
        return clusterName_;
    }

    public static boolean isApplicationColumnFamily(String columnFamily)
    {
        return applicationColumnFamilies_.contains(columnFamily);
    }

    public static int getTouchKeyCacheSize()
    {
        return touchKeyCacheSize_;
    }

    public static String getGangliaServers()
    {
    	StringBuilder sb = new StringBuilder();
    	for ( int i = 0; i < gangliaServers_.length; ++i )
    	{
    		sb.append(gangliaServers_[i]);
    		if ( i != (gangliaServers_.length - 1) )
    			sb.append(", ");
    	}
    	return sb.toString();
    }

    public static String getColumnType(String cfName)
    {
    	return cfToColumnTypeMap_.get(cfName);
    }

    public static boolean isNameIndexEnabled(String cfName)
    {
    	return "Name".equals(cfToIndexPropertyMap_.get(cfName));
    }

    public static List<String> getTables()
    {
        return tables_;
    }

    public static void  setTables(String table)
    {
        tables_.add(table);
    }

    public static int getStoragePort()
    {
        return storagePort_;
    }

    public static int getControlPort()
    {
        return controlPort_;
    }

    public static int getHttpPort()
    {
        return httpPort_;
    }

    public static int getReplicationFactor()
    {
        return replicationFactor_;
    }

    public static long getRpcTimeout()
    {
        return rpcTimeoutInMillis_;
    }

    public static int getThreadsPerPool()
    {
        return threadsPerPool_;
    }

    public static String getMetadataDirectory()
    {
        return metadataDirectory_;
    }

    public static void setMetadataDirectory(String metadataDirectory)
    {
        metadataDirectory_ = metadataDirectory_;
    }

    public static String[] getAllDataFileLocations()
    {
        return dataFileDirectories_;
    }

    public static String getDataFileLocation()
    {
    	String dataFileDirectory = dataFileDirectories_[currentIndex_];
        return dataFileDirectory;
    }
    public static String getCompactionFileLocation()
    {
    	String dataFileDirectory = dataFileDirectories_[currentIndex_];
    	currentIndex_ = (currentIndex_ + 1 )%dataFileDirectories_.length ;
        return dataFileDirectory;
    }

    public static String getBootstrapFileLocation()
    {
        return bootstrapFileDirectory_;
    }

    public static void setBootstrapFileLocation(String bfLocation)
    {
        bootstrapFileDirectory_ = bfLocation;
    }

    public static String getStagingFileLocation()
    {
        return stagingFileDirectory_;
    }

    public static void setStagingFileLocation(String stagingLocation)
    {
        stagingFileDirectory_ = stagingLocation;
    }

    public static int getLogFileSizeThreshold()
    {
        return logRotationThreshold_;
    }

    public static String getLogFileLocation()
    {
        return logFileDirectory_;
    }

    public static void setLogFileLocation(String logLocation)
    {
        logFileDirectory_ = logLocation;
    }

    public static boolean isFastSync()
    {
        return fastSync_;
    }

    public static boolean isRackAware()
    {
        return rackAware_;
    }

    public static Set<String> getSeeds()
    {
        return seeds_;
    }

    public static String getColumnFamilyType(String cfName)
    {
    	return cfToColumnTypeMap_.get(cfName);
    }

    /*
     * Loop through all the disks to see which disk has the max free space
     * return the disk with max free space for compactions. If the size of the expected
     * compacted file is greater than the max disk space available return null, we cannot
     * do compaction in this case.
     */
    public static String getCompactionFileLocation(long expectedCompactedFileSize)
    {
      long maxFreeDisk = 0;
      int maxDiskIndex = 0;
      String dataFileDirectory = null;
      for ( int i = 0 ; i < dataFileDirectories_.length ; i++ )
      {
        File f = new File(dataFileDirectories_[i]);
        if( maxFreeDisk < f.getUsableSpace())
        {
          maxFreeDisk = f.getUsableSpace();
          maxDiskIndex = i;
        }
      }
      // Load factor of 0.9 we do not want to use the entire disk that is too risky.
      maxFreeDisk = (long)(0.9 * maxFreeDisk);
      if( expectedCompactedFileSize < maxFreeDisk )
      {
        dataFileDirectory = dataFileDirectories_[maxDiskIndex];
        currentIndex_ = (maxDiskIndex + 1 )%dataFileDirectories_.length ;
      }
      else
      {
        currentIndex_ = maxDiskIndex;
      }
        return dataFileDirectory;
    }
}

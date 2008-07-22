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
import java.io.*;

import org.apache.log4j.Logger;

import com.facebook.infrastructure.conf.DatabaseDescriptor;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class RecoveryManager
{
    private static RecoveryManager instance_;
    private static Logger logger_ = Logger.getLogger(RecoveryManager.class);
    
    synchronized static RecoveryManager instance() throws IOException
    {
        if ( instance_ == null )
            instance_ = new RecoveryManager();
        return instance_;
    }
    
    public void doRecovery() throws IOException
    {
        String directory = DatabaseDescriptor.getLogFileLocation();
        File file = new File(directory);
        File[] files = file.listFiles();
        /* Maintains a mapping of table name to a list of commit log files */
        Map<String, List<File>> tableToCommitLogs = new HashMap<String, List<File>>();
        
        for (File f : files)
        {
            String table = CommitLog.getTableName(f.getName());
            List<File> clogs = tableToCommitLogs.get(table);
            if ( clogs == null )
            {
                clogs = new ArrayList<File>();
                tableToCommitLogs.put(table, clogs);
            }
            clogs.add(f);
        }
        
        recoverEachTable(tableToCommitLogs);
        FileUtils.delete(files);
    }
    
    private void recoverEachTable(Map<String, List<File>> tableToCommitLogs) throws IOException
    {
        Comparator<File> fCmp = new FileUtils.FileComparator();

        for ( Map.Entry<String, List<File>> entry : tableToCommitLogs.entrySet() )
        {
            String table = entry.getKey();
            List<File> clogs = entry.getValue();
            Collections.sort(clogs, fCmp);
            CommitLog clog = new CommitLog(table, true);
            clog.recover(clogs);
        }
    }
    
    public static void main(String[] args) throws Throwable
    {
        DatabaseDescriptor.init();
        long start = System.currentTimeMillis();
        RecoveryManager rm = RecoveryManager.instance();
        rm.doRecovery();  
        logger_.debug( "Time taken : " + (System.currentTimeMillis() - start) + " ms.");
    }
}

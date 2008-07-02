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

package com.facebook.infrastructure.service;

import java.util.Comparator;

import com.facebook.infrastructure.db.FileUtils;

class LoadInfo
{
    protected static class PrimaryCountComparator implements Comparator<LoadInfo>
    {
        public int compare(LoadInfo li, LoadInfo li2)
        {
            if ( li == null || li2 == null )
                throw new IllegalArgumentException("Cannot pass in values that are NULL.");
            return (li.count_ - li2.count_);
        }
    }
    
    protected static class DiskSpaceComparator implements Comparator<LoadInfo>
    {
        public int compare(LoadInfo li, LoadInfo li2)
        {
            if ( li == null || li2 == null )
                throw new IllegalArgumentException("Cannot pass in values that are NULL.");
            
            double space = FileUtils.stringToFileSize(li.diskSpace_);
            double space2 = FileUtils.stringToFileSize(li2.diskSpace_);
            return (int)(space - space2);
        }
    }
    
    private int count_;
    private String diskSpace_;
    
    LoadInfo(int count, long diskSpace)
    {
        count_ = count;
        diskSpace_ = FileUtils.stringifyFileSize(diskSpace);
    }
    
    LoadInfo(String loadInfo)
    {
        String[] peices = loadInfo.split(":");
        count_ = Integer.parseInt(peices[0]);
        diskSpace_ = peices[1];
    }
    
    int count()
    {
        return count_;
    }
    
    String diskSpace()
    {
        return diskSpace_;
    }
    
    public String toString()
    {
        StringBuilder sb = new StringBuilder("");
        sb.append(count_);
        sb.append(":");
        sb.append(diskSpace_);
        return sb.toString();
    }
}

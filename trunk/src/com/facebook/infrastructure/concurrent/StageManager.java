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

package com.facebook.infrastructure.concurrent;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * This class manages all stages that exist within a process. The application registers
 * and de-registers stages with this abstraction. Any component that has the <i>ID</i> 
 * associated with a stage can obtain a handle to actual stage.
 * 
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class StageManager
{
    private static Map<String, IStage > stageQueues_ = new HashMap<String, IStage>();

    /**
     * Register a stage with the StageManager
     * @param stageName stage name.
     * @param stage stage for the respective message types.
     */
    public static void registerStage(String stageName, IStage stage)
    {
        stageQueues_.put(stageName, stage);
    }

    /**
     * Retrieve a stage from the StageManager
     * @param stageName name of the stage to be retrieved.
    */
    public static IStage getStage(String stageName)
    {
        return stageQueues_.get(stageName);
    }

    /**
     * Deregister a stage from StageManager
     * @param stageName stage name.
     */
    public static void deregisterStage(String stageName)
    {
        stageQueues_.remove(stageName);
    }

    /**
     * This method gets the number of tasks on the
     * stage's internal queue.
     * @param stage name of the stage
     * @return
     */
    public static long getStageTaskCount(String stage)
    {
        return stageQueues_.get(stage).getTaskCount();
    }

    /**
     * This method shuts down all registered stages.
     */
    public static void shutdown()
    {
        Set<String> stages = stageQueues_.keySet();
        for ( String stage : stages )
        {
            IStage registeredStage = stageQueues_.get(stage);
            registeredStage.shutdown();
        }
    }
}

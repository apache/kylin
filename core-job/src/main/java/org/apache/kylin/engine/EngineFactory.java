/*
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

package org.apache.kylin.engine;

import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ImplementationSwitch;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.metadata.model.IEngineAware;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;

public class EngineFactory {

    private static ImplementationSwitch<IBatchCubingEngine> batchEngines;
    static {
        Map<Integer, String> impls = KylinConfig.getInstanceFromEnv().getJobEngines();
        batchEngines = new ImplementationSwitch<>(impls, IBatchCubingEngine.class);
    }

    public static IBatchCubingEngine batchEngine(IEngineAware aware) {
        return batchEngines.get(aware.getEngineType());
    }

    /** Mark deprecated to indicate for test purpose only */
    @Deprecated
    public static IJoinedFlatTableDesc getJoinedFlatTableDesc(CubeDesc cubeDesc) {
        return batchEngine(cubeDesc).getJoinedFlatTableDesc(cubeDesc);
    }
    
    public static IJoinedFlatTableDesc getJoinedFlatTableDesc(CubeSegment newSegment) {
        return batchEngine(newSegment).getJoinedFlatTableDesc(newSegment);
    }
    
    /** Build a new cube segment, typically its time range appends to the end of current cube. */
    public static DefaultChainedExecutable createBatchCubingJob(CubeSegment newSegment, String submitter) {
        return batchEngine(newSegment).createBatchCubingJob(newSegment, submitter);
    }

    /** Merge multiple small segments into a big one. */
    public static DefaultChainedExecutable createBatchMergeJob(CubeSegment mergeSegment, String submitter) {
        return batchEngine(mergeSegment).createBatchMergeJob(mergeSegment, submitter);
    }

}

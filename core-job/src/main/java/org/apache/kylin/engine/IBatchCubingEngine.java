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

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;

public interface IBatchCubingEngine {
    
    /** Mark deprecated to indicate for test purpose only */
    @Deprecated
    public IJoinedFlatTableDesc getJoinedFlatTableDesc(CubeDesc cubeDesc);
    
    public IJoinedFlatTableDesc getJoinedFlatTableDesc(CubeSegment newSegment);

    /** Build a new cube segment, typically its time range appends to the end of current cube. */
    public DefaultChainedExecutable createBatchCubingJob(CubeSegment newSegment, String submitter, Integer priorityOffset);

    /** Merge multiple small segments into a big one. */
    public DefaultChainedExecutable createBatchMergeJob(CubeSegment mergeSegment, String submitter);

    /** Optimize a segment based on the cuboid recommend list produced by the cube planner. */
    public DefaultChainedExecutable createBatchOptimizeJob(CubeSegment optimizeSegment, String submitter);

    public Class<?> getSourceInterface();

    public Class<?> getStorageInterface();
}

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
import org.apache.kylin.engine.mr.MRBatchCubingEngine;
import org.apache.kylin.job.execution.DefaultChainedExecutable;

public class BuildEngineFactory {
    
    private static final IBatchCubingEngine defaultBatch = new MRBatchCubingEngine();
    
    /** Build a new cube segment, typically its time range appends to the end of current cube. */
    public static DefaultChainedExecutable createBatchCubingJob(CubeSegment newSegment, String submitter) {
        return defaultBatch.createBatchCubingJob(newSegment, submitter);
    }
    
    /** Merge multiple small segments into a big one. */
    public static DefaultChainedExecutable createBatchMergeJob(CubeSegment mergeSegment, String submitter) {
        return defaultBatch.createBatchMergeJob(mergeSegment, submitter);
    }
    

}

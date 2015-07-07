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

package org.apache.kylin.engine.mr;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.IBatchCubeBuilder;
import org.apache.kylin.job.execution.ChainedExecutable;

public class MRCubeBuilder implements IBatchCubeBuilder {

    @Override
    public ChainedExecutable createBuildJob(CubeSegment newSegment) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ChainedExecutable createMergeJob(CubeSegment mergeSegment) {
        // TODO Auto-generated method stub
        return null;
    }

    /** Build a new segment and merge it with existing segment into one step. Deprecated, only needed by Holistic Distinct Count. */
    public ChainedExecutable createBuildAndMergeJob(CubeSegment appendSegment, CubeSegment mergeSegment) {
        return null;
    }
    

}

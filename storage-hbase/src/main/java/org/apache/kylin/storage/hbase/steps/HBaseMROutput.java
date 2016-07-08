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

package org.apache.kylin.storage.hbase.steps;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.IMROutput;
import org.apache.kylin.job.execution.DefaultChainedExecutable;

public class HBaseMROutput implements IMROutput {

    @Override
    public IMRBatchCubingOutputSide getBatchCubingOutputSide(final CubeSegment seg) {
        return new IMRBatchCubingOutputSide() {
            HBaseMRSteps steps = new HBaseMRSteps(seg);

            @Override
            public void addStepPhase3_BuildCube(DefaultChainedExecutable jobFlow, String cuboidRootPath) {
                steps.addSaveCuboidToHTableSteps(jobFlow, cuboidRootPath);
            }

            @Override
            public void addStepPhase4_Cleanup(DefaultChainedExecutable jobFlow) {
                steps.addCubingGarbageCollectionSteps(jobFlow);
            }
        };
    }

    @Override
    public IMRBatchMergeOutputSide getBatchMergeOutputSide(final CubeSegment seg) {
        return new IMRBatchMergeOutputSide() {
            HBaseMRSteps steps = new HBaseMRSteps(seg);

            @Override
            public void addStepPhase2_BuildCube(DefaultChainedExecutable jobFlow, String cuboidRootPath) {
                steps.addSaveCuboidToHTableSteps(jobFlow, cuboidRootPath);
            }

            @Override
            public void addStepPhase3_Cleanup(DefaultChainedExecutable jobFlow) {
                steps.addMergingGarbageCollectionSteps(jobFlow);
            }
        };
    }
}

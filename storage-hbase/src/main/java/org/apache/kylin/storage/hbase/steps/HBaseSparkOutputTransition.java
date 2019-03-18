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

import java.util.List;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.spark.ISparkOutput;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This "Transition" impl generates cuboid files and then convert to HFile.
 * The additional step slows down build process, but the gains is merge
 * can read from HDFS instead of over HBase region server. See KYLIN-1007.
 * 
 * This is transitional because finally we want to merge from HTable snapshot.
 * However multiple snapshots as MR input is only supported by HBase 1.x.
 * Before most users upgrade to latest HBase, they can only use this transitional
 * cuboid file solution.
 */
public class HBaseSparkOutputTransition implements ISparkOutput {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(HBaseSparkOutputTransition.class);

    @Override
    public ISparkBatchCubingOutputSide getBatchCubingOutputSide(final CubeSegment seg) {
        final HBaseJobSteps steps = new HBaseSparkSteps(seg);

        return new ISparkBatchCubingOutputSide() {

            @Override
            public void addStepPhase2_BuildDictionary(DefaultChainedExecutable jobFlow) {
                jobFlow.addTask(steps.createCreateHTableStep(jobFlow.getId()));
            }

            @Override
            public void addStepPhase3_BuildCube(DefaultChainedExecutable jobFlow) {
                jobFlow.addTask(steps.createConvertCuboidToHfileStep(jobFlow.getId()));
                jobFlow.addTask(steps.createBulkLoadStep(jobFlow.getId()));
            }

            @Override
            public void addStepPhase4_Cleanup(DefaultChainedExecutable jobFlow) {
                steps.addCubingGarbageCollectionSteps(jobFlow);
            }

        };
    }

    @Override
    public ISparkBatchMergeOutputSide getBatchMergeOutputSide(final CubeSegment seg) {
        return new ISparkBatchMergeOutputSide() {
            HBaseSparkSteps steps = new HBaseSparkSteps(seg);

            @Override
            public void addStepPhase1_MergeDictionary(DefaultChainedExecutable jobFlow) {
                jobFlow.addTask(steps.createCreateHTableStep(jobFlow.getId()));
            }

            @Override
            public void addStepPhase2_BuildCube(CubeSegment seg, List<CubeSegment> mergingSegments,
                    DefaultChainedExecutable jobFlow) {
                jobFlow.addTask(steps.createConvertCuboidToHfileStep(jobFlow.getId()));
                jobFlow.addTask(steps.createBulkLoadStep(jobFlow.getId()));
            }

            @Override
            public void addStepPhase3_Cleanup(DefaultChainedExecutable jobFlow) {
                steps.addMergingGarbageCollectionSteps(jobFlow);
            }

        };
    }

    public ISparkBatchOptimizeOutputSide getBatchOptimizeOutputSide(final CubeSegment seg) {
        return null;
    }
}
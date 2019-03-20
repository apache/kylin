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

package org.apache.kylin.source.hive;

import com.google.common.collect.Lists;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.engine.flink.IFlinkInput;
import org.apache.kylin.job.JoinedFlatTable;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.ISegment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * The implementation of {@link IFlinkInput} based on Hive.
 */
public class HiveFlinkInput extends HiveInputBase implements IFlinkInput {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(HiveFlinkInput.class);

    @Override
    public IFlinkBatchCubingInputSide getBatchCubingInputSide(IJoinedFlatTableDesc flatDesc) {
        return new BatchCubingInputSide(flatDesc);
    }

    @Override
    public IFlinkBatchMergeInputSide getBatchMergeInputSide(ISegment seg) {
        return new IFlinkBatchMergeInputSide() {
            @Override
            public void addStepPhase1_MergeDictionary(DefaultChainedExecutable jobFlow) {
                // doing nothing
            }
        };
    }

    public class BatchCubingInputSide extends BaseBatchCubingInputSide implements IFlinkBatchCubingInputSide {

        List<String> hiveViewIntermediateTables = Lists.newArrayList();

        public BatchCubingInputSide(IJoinedFlatTableDesc flatDesc) {
            super(flatDesc);
        }

        protected void addStepPhase1_DoMaterializeLookupTable(DefaultChainedExecutable jobFlow) {
            final String hiveInitStatements = JoinedFlatTable.generateHiveInitStatements(flatTableDatabase);
            final String jobWorkingDir = getJobWorkingDir(jobFlow, hdfsWorkingDir);

            AbstractExecutable task = createLookupHiveViewMaterializationStep(hiveInitStatements, jobWorkingDir,
                    flatDesc, hiveViewIntermediateTables, jobFlow.getId());
            if (task != null) {
                jobFlow.addTask(task);
            }
        }

        @Override
        public void addStepPhase4_Cleanup(DefaultChainedExecutable jobFlow) {
            final String jobWorkingDir = getJobWorkingDir(jobFlow, hdfsWorkingDir);

            GarbageCollectionStep step = new GarbageCollectionStep();
            step.setName(ExecutableConstants.STEP_NAME_HIVE_CLEANUP);
            step.setIntermediateTables(Collections.singletonList(getIntermediateTableIdentity()));
            step.setExternalDataPaths(Collections.singletonList(JoinedFlatTable.getTableDir(flatDesc, jobWorkingDir)));
            step.setHiveViewIntermediateTableIdentities(StringUtil.join(hiveViewIntermediateTables, ","));
            jobFlow.addTask(step);
        }

    }

}

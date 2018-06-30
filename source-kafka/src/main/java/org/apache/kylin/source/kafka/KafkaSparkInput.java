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
package org.apache.kylin.source.kafka;

import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.engine.spark.ISparkInput;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.ISegment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class KafkaSparkInput extends KafkaInputBase implements ISparkInput {

    private static final Logger logger = LoggerFactory.getLogger(KafkaSparkInput.class);
    private CubeSegment cubeSegment;

    @Override
    public ISparkInput.ISparkBatchCubingInputSide getBatchCubingInputSide(IJoinedFlatTableDesc flatDesc) {
        this.cubeSegment = (CubeSegment) flatDesc.getSegment();
        return new BatchCubingInputSide(cubeSegment, flatDesc);
    }

    @Override
    public ISparkBatchMergeInputSide getBatchMergeInputSide(ISegment seg) {
        return new KafkaSparkBatchMergeInputSide((CubeSegment) seg);
    }

    public static class BatchCubingInputSide implements ISparkBatchCubingInputSide {

        final JobEngineConfig conf;
        final CubeSegment seg;
        private CubeDesc cubeDesc;
        private KylinConfig config;
        protected IJoinedFlatTableDesc flatDesc;
        protected String hiveTableDatabase;
        final private List<String> intermediateTables = Lists.newArrayList();
        final private List<String> intermediatePaths = Lists.newArrayList();
        private String cubeName;

        public BatchCubingInputSide(CubeSegment seg, IJoinedFlatTableDesc flatDesc) {
            this.conf = new JobEngineConfig(KylinConfig.getInstanceFromEnv());
            this.config = seg.getConfig();
            this.flatDesc = flatDesc;
            this.hiveTableDatabase = config.getHiveDatabaseForIntermediateTable();
            this.seg = seg;
            this.cubeDesc = seg.getCubeDesc();
            this.cubeName = seg.getCubeInstance().getName();
        }

        @Override
        public void addStepPhase1_CreateFlatTable(DefaultChainedExecutable jobFlow) {

            boolean onlyOneTable = cubeDesc.getModel().getLookupTables().size() == 0;
            final String baseLocation = getJobWorkingDir(jobFlow);
            if (onlyOneTable) {
                // directly use flat table location
                final String intermediateFactTable = flatDesc.getTableName();
                final String tableLocation = baseLocation + "/" + intermediateFactTable;
                jobFlow.addTask(createSaveKafkaDataStep(jobFlow.getId(), tableLocation, seg));
                intermediatePaths.add(tableLocation);
            } else {
                final String mockFactTableName = MetadataConstants.KYLIN_INTERMEDIATE_PREFIX + cubeName.toLowerCase()
                        + "_" + seg.getUuid().replaceAll("-", "_") + "_fact";
                jobFlow.addTask(createSaveKafkaDataStep(jobFlow.getId(), baseLocation + "/" + mockFactTableName, seg));
                jobFlow.addTask(createFlatTable(hiveTableDatabase, mockFactTableName, baseLocation, cubeName, cubeDesc,
                        flatDesc, intermediateTables, intermediatePaths));
            }
        }

        protected String getJobWorkingDir(DefaultChainedExecutable jobFlow) {
            return JobBuilderSupport.getJobWorkingDir(config.getHdfsWorkingDirectory(), jobFlow.getId());
        }

        @Override
        public void addStepPhase4_Cleanup(DefaultChainedExecutable jobFlow) {
            jobFlow.addTask(createGCStep(intermediateTables, intermediatePaths));

        }
    }

    class KafkaSparkBatchMergeInputSide implements ISparkBatchMergeInputSide {

        private CubeSegment cubeSegment;

        KafkaSparkBatchMergeInputSide(CubeSegment cubeSegment) {
            this.cubeSegment = cubeSegment;
        }

        @Override
        public void addStepPhase1_MergeDictionary(DefaultChainedExecutable jobFlow) {
            jobFlow.addTask(createMergeOffsetStep(jobFlow.getId(), cubeSegment));
        }
    }

}

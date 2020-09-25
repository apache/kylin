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
import org.apache.kylin.source.kafka.model.StreamCubeFactTableDesc;
import org.apache.kylin.engine.mr.IInput;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.MapReduceExecutable;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.JoinedFlatTable;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.source.hive.CreateFlatHiveTableStep;
import org.apache.kylin.source.hive.GarbageCollectionStep;
import org.apache.kylin.source.kafka.hadoop.KafkaFlatTableJob;
import org.apache.kylin.source.kafka.job.MergeOffsetStep;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

import static org.apache.kylin.job.util.FlatTableSqlQuoteUtils.quoteTableIdentity;

public class KafkaInputBase {

    public static class BaseBatchCubingInputSide implements IInput.IBatchCubingInputSide {

        final JobEngineConfig conf;
        final CubeSegment seg;
        private CubeDesc cubeDesc;
        private KylinConfig config;
        protected IJoinedFlatTableDesc flatDesc;
        protected String hiveTableDatabase;
        final private List<String> intermediateTables = Lists.newArrayList();
        final private List<String> intermediatePaths = Lists.newArrayList();
        private String cubeName;

        public BaseBatchCubingInputSide(CubeSegment seg, IJoinedFlatTableDesc flatDesc) {
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
                // sink stream data as a mock fact table, and then join it with dimension tables
                final StreamCubeFactTableDesc streamFactDesc = new StreamCubeFactTableDesc(cubeDesc, seg, flatDesc);
                jobFlow.addTask(createSaveKafkaDataStep(jobFlow.getId(), baseLocation + "/" + streamFactDesc.getTableName(), seg));
                jobFlow.addTask(createFlatTable(hiveTableDatabase, baseLocation, cubeName,
                        streamFactDesc, intermediateTables, intermediatePaths));
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

    public static  class BaseBatchMergeInputSide implements IInput.IBatchMergeInputSide {

        private CubeSegment cubeSegment;

        BaseBatchMergeInputSide(CubeSegment cubeSegment) {
            this.cubeSegment = cubeSegment;
        }

        @Override
        public void addStepPhase1_MergeDictionary(DefaultChainedExecutable jobFlow) {
            jobFlow.addTask(createMergeOffsetStep(jobFlow.getId(), cubeSegment));
        }
    }

    protected static AbstractExecutable createMergeOffsetStep(String jobId, CubeSegment cubeSegment) {

        final MergeOffsetStep result = new MergeOffsetStep();
        result.setName("Merge offset step");

        CubingExecutableUtil.setCubeName(cubeSegment.getCubeInstance().getName(), result.getParams());
        CubingExecutableUtil.setSegmentId(cubeSegment.getUuid(), result.getParams());
        CubingExecutableUtil.setCubingJobId(jobId, result.getParams());
        return result;
    }

    protected static MapReduceExecutable createSaveKafkaDataStep(String jobId, String location, CubeSegment seg) {
        MapReduceExecutable result = new MapReduceExecutable();
        result.setName("Save data from Kafka");
        result.setMapReduceJobClass(KafkaFlatTableJob.class);
        JobBuilderSupport jobBuilderSupport = new JobBuilderSupport(seg, "system");
        StringBuilder cmd = new StringBuilder();
        jobBuilderSupport.appendMapReduceParameters(cmd);
        JobBuilderSupport.appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());
        JobBuilderSupport.appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, location);
        JobBuilderSupport.appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_ID, seg.getUuid());
        JobBuilderSupport.appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME,
                "Kylin_Save_Kafka_Data_" + seg.getRealization().getName() + "_Step");

        result.setMapReduceParams(cmd.toString());
        return result;
    }

    protected static AbstractExecutable createFlatTable(final String hiveTableDatabase,
                                                        final String baseLocation, final String cubeName,
                                                        final StreamCubeFactTableDesc streamFactDesc, final List<String> intermediateTables,
                                                        final List<String> intermediatePaths) {
        final IJoinedFlatTableDesc flatDesc = streamFactDesc.getFlatTableDesc();

        final String hiveInitStatements = JoinedFlatTable.generateHiveInitStatements(hiveTableDatabase);

        final String dropFactTableHql = JoinedFlatTable.generateDropTableStatement(streamFactDesc);
        // the table inputformat is sequence file
        final String createFactTableHql = JoinedFlatTable.generateCreateTableStatement(streamFactDesc, baseLocation,
                JoinedFlatTable.SEQUENCEFILE);

        final String dropTableHql = JoinedFlatTable.generateDropTableStatement(flatDesc);
        final String createTableHql = JoinedFlatTable.generateCreateTableStatement(flatDesc, baseLocation);
        String insertDataHqls = JoinedFlatTable.generateInsertDataStatement(flatDesc);
        insertDataHqls = insertDataHqls.replace(
                quoteTableIdentity(flatDesc.getDataModel().getRootFactTable(), null) + " ",
                quoteTableIdentity(hiveTableDatabase, streamFactDesc.getTableName(), null) + " ");

        CreateFlatHiveTableStep step = new CreateFlatHiveTableStep();
        CubingExecutableUtil.setCubeName(cubeName, step.getParams());
        step.setInitStatement(hiveInitStatements);
        step.setCreateTableStatement(
                dropFactTableHql + createFactTableHql + dropTableHql + createTableHql + insertDataHqls);
        step.setName(ExecutableConstants.STEP_NAME_CREATE_FLAT_HIVE_TABLE);

        intermediateTables.add(flatDesc.getTableName());
        intermediateTables.add(streamFactDesc.getTableName());
        intermediatePaths.add(baseLocation + "/" + flatDesc.getTableName());
        intermediatePaths.add(baseLocation + "/" + streamFactDesc.getTableName());
        return step;
    }

    protected static AbstractExecutable createGCStep(List<String> intermediateTables, List<String> intermediatePaths) {
        GarbageCollectionStep step = new GarbageCollectionStep();
        step.setName(ExecutableConstants.STEP_NAME_HIVE_CLEANUP);
        step.setIntermediateTables(intermediateTables);
        step.setExternalDataPaths(intermediatePaths);

        return step;
    }
}

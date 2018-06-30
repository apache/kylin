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

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.MapReduceExecutable;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.JoinedFlatTable;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.source.hive.CreateFlatHiveTableStep;
import org.apache.kylin.source.hive.GarbageCollectionStep;
import org.apache.kylin.source.kafka.hadoop.KafkaFlatTableJob;
import org.apache.kylin.source.kafka.job.MergeOffsetStep;

public class KafkaInputBase {

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

    protected static AbstractExecutable createFlatTable(final String hiveTableDatabase, final String mockFactTableName,
            final String baseLocation, final String cubeName, final CubeDesc cubeDesc,
            final IJoinedFlatTableDesc flatDesc, final List<String> intermediateTables,
            final List<String> intermediatePaths) {
        final String hiveInitStatements = JoinedFlatTable.generateHiveInitStatements(hiveTableDatabase);

        final IJoinedFlatTableDesc mockfactDesc = new IJoinedFlatTableDesc() {

            @Override
            public String getTableName() {
                return mockFactTableName;
            }

            @Override
            public DataModelDesc getDataModel() {
                return cubeDesc.getModel();
            }

            @Override
            public List<TblColRef> getAllColumns() {
                return flatDesc.getFactColumns();
            }

            @Override
            public List<TblColRef> getFactColumns() {
                return null;
            }

            @Override
            public int getColumnIndex(TblColRef colRef) {
                return 0;
            }

            @Override
            public SegmentRange getSegRange() {
                return null;
            }

            @Override
            public TblColRef getDistributedBy() {
                return null;
            }

            @Override
            public TblColRef getClusterBy() {
                return null;
            }

            @Override
            public ISegment getSegment() {
                return null;
            }

            @Override
            public boolean useAlias() {
                return false;
            }
        };
        final String dropFactTableHql = JoinedFlatTable.generateDropTableStatement(mockfactDesc);
        // the table inputformat is sequence file
        final String createFactTableHql = JoinedFlatTable.generateCreateTableStatement(mockfactDesc, baseLocation,
                JoinedFlatTable.SEQUENCEFILE);

        final String dropTableHql = JoinedFlatTable.generateDropTableStatement(flatDesc);
        final String createTableHql = JoinedFlatTable.generateCreateTableStatement(flatDesc, baseLocation);
        String insertDataHqls = JoinedFlatTable.generateInsertDataStatement(flatDesc);
        insertDataHqls = insertDataHqls.replace(flatDesc.getDataModel().getRootFactTableName() + " ",
                mockFactTableName + " ");

        CreateFlatHiveTableStep step = new CreateFlatHiveTableStep();
        CubingExecutableUtil.setCubeName(cubeName, step.getParams());
        step.setInitStatement(hiveInitStatements);
        step.setCreateTableStatement(
                dropFactTableHql + createFactTableHql + dropTableHql + createTableHql + insertDataHqls);
        step.setName(ExecutableConstants.STEP_NAME_CREATE_FLAT_HIVE_TABLE);

        intermediateTables.add(flatDesc.getTableName());
        intermediateTables.add(mockFactTableName);
        intermediatePaths.add(baseLocation + "/" + flatDesc.getTableName());
        intermediatePaths.add(baseLocation + "/" + mockFactTableName);
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

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

package org.apache.kylin.tool.metrics.systemcube;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.ModelDimensionDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metrics.lib.impl.RecordEvent;
import org.apache.kylin.metrics.lib.impl.TimePropertyEnum;
import org.apache.kylin.metrics.property.JobPropertyEnum;
import org.apache.kylin.metrics.property.QuerySparkExecutionEnum;
import org.apache.kylin.metrics.property.QuerySparkJobEnum;
import org.apache.kylin.metrics.property.QuerySparkStageEnum;
import org.apache.kylin.tool.metrics.systemcube.def.MetricsSinkDesc;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

public class ModelCreator {

    public static final Serializer<DataModelDesc> MODELDESC_SERIALIZER = new JsonSerializer<>(DataModelDesc.class);

    public static void main(String[] args) throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();

        DataModelDesc kylinModel = generateKylinModelForMetricsQuery("ADMIN", config, new MetricsSinkDesc());
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(buf);
        MODELDESC_SERIALIZER.serialize(kylinModel, dout);
        dout.close();
        buf.close();
        System.out.println(buf.toString("UTF-8"));
    }

    public static PartitionDesc getPartitionDesc(String tableName) {
        PartitionDesc partitionDesc = new PartitionDesc();

        partitionDesc.setPartitionDateColumn(tableName + "." + TimePropertyEnum.DAY_DATE.toString());
        partitionDesc.setPartitionTimeColumn(tableName + "." + TimePropertyEnum.DAY_TIME.toString());
        return partitionDesc;
    }

    public static DataModelDesc generateKylinModelForMetricsQuery(String owner, KylinConfig kylinConfig,
            MetricsSinkDesc sinkDesc) {
        String tableName = sinkDesc.getTableNameForMetrics(kylinConfig.getKylinMetricsSubjectQueryExecution());
        return generateKylinModel(owner, tableName, getDimensionsForMetricsQueryExecution(),
                getMeasuresForMetricsQueryExecution(), getPartitionDesc(tableName));
    }

    public static DataModelDesc generateKylinModelForMetricsQueryCube(String owner, KylinConfig kylinConfig,
            MetricsSinkDesc sinkDesc) {
        String tableName = sinkDesc.getTableNameForMetrics(kylinConfig.getKylinMetricsSubjectQuerySparkJob());
        return generateKylinModel(owner, tableName, getDimensionsForMetricsQuerySparkJob(),
                getMeasuresForMetricsQuerySparkJob(), getPartitionDesc(tableName));
    }

    public static DataModelDesc generateKylinModelForMetricsQueryRPC(String owner, KylinConfig kylinConfig,
            MetricsSinkDesc sinkDesc) {
        String tableName = sinkDesc.getTableNameForMetrics(kylinConfig.getKylinMetricsSubjectQuerySparkStage());
        return generateKylinModel(owner, tableName, getDimensionsForMetricsQuerySparkStage(),
                getMeasuresForMetricsQuerySparkStage(), getPartitionDesc(tableName));
    }

    public static DataModelDesc generateKylinModelForMetricsJob(String owner, KylinConfig kylinConfig,
            MetricsSinkDesc sinkDesc) {
        String tableName = sinkDesc.getTableNameForMetrics(kylinConfig.getKylinMetricsSubjectJob());
        return generateKylinModel(owner, tableName, getDimensionsForMetricsJob(), getMeasuresForMetricsJob(),
                getPartitionDesc(tableName));
    }

    public static DataModelDesc generateKylinModelForMetricsJobException(String owner, KylinConfig kylinConfig,
            MetricsSinkDesc sinkDesc) {
        String tableName = sinkDesc.getTableNameForMetrics(kylinConfig.getKylinMetricsSubjectJobException());
        return generateKylinModel(owner, tableName, getDimensionsForMetricsJobException(),
                getMeasuresForMetricsJobException(), getPartitionDesc(tableName));
    }

    public static List<String> getDimensionsForMetricsQueryExecution() {
        List<String> result = Lists.newLinkedList();
        result.add(RecordEvent.RecordReserveKeyEnum.HOST.toString());
        result.add(QuerySparkExecutionEnum.USER.toString());
        result.add(QuerySparkExecutionEnum.PROJECT.toString());
        result.add(QuerySparkExecutionEnum.REALIZATION.toString());
        result.add(QuerySparkExecutionEnum.REALIZATION_TYPE.toString());
        result.add(QuerySparkExecutionEnum.CUBOID_IDS.toString());
        result.add(QuerySparkExecutionEnum.TYPE.toString());
        result.add(QuerySparkExecutionEnum.EXCEPTION.toString());
        result.add(QuerySparkExecutionEnum.SPARDER_NAME.toString());
        result.add(QuerySparkExecutionEnum.QUERY_ID.toString());
        result.add(QuerySparkExecutionEnum.START_TIME.toString());
        result.add(QuerySparkExecutionEnum.END_TIME.toString());

        result.addAll(getTimeDimensionsForMetrics());
        return result;
    }

    public static List<String> getMeasuresForMetricsQueryExecution() {
        List<String> result = Lists.newLinkedList();
        result.add(QuerySparkExecutionEnum.ID_CODE.toString());
        result.add(QuerySparkExecutionEnum.TIME_COST.toString());
        result.add(QuerySparkExecutionEnum.TOTAL_SCAN_COUNT.toString());
        result.add(QuerySparkExecutionEnum.TOTAL_SCAN_BYTES.toString());
        result.add(QuerySparkExecutionEnum.RESULT_COUNT.toString());
        result.add(QuerySparkExecutionEnum.EXECUTION_DURATION.toString());
        result.add(QuerySparkExecutionEnum.RESULT_SIZE.toString());
        result.add(QuerySparkExecutionEnum.EXECUTOR_DESERIALIZE_TIME.toString());
        result.add(QuerySparkExecutionEnum.EXECUTOR_DESERIALIZE_CPU_TIME.toString());
        result.add(QuerySparkExecutionEnum.EXECUTOR_RUN_TIME.toString());
        result.add(QuerySparkExecutionEnum.EXECUTOR_CPU_TIME.toString());
        result.add(QuerySparkExecutionEnum.JVM_GC_TIME.toString());
        result.add(QuerySparkExecutionEnum.RESULT_SERIALIZATION_TIME.toString());
        result.add(QuerySparkExecutionEnum.MEMORY_BYTE_SPILLED.toString());
        result.add(QuerySparkExecutionEnum.DISK_BYTES_SPILLED.toString());
        result.add(QuerySparkExecutionEnum.PEAK_EXECUTION_MEMORY.toString());
        return result;
    }

    public static List<String> getDimensionsForMetricsQuerySparkJob() {
        List<String> result = Lists.newLinkedList();
        result.add(RecordEvent.RecordReserveKeyEnum.HOST.toString());
        result.add(QuerySparkJobEnum.QUERY_ID.toString());
        result.add(QuerySparkJobEnum.EXECUTION_ID.toString());
        result.add(QuerySparkJobEnum.JOB_ID.toString());
        result.add(QuerySparkJobEnum.PROJECT.toString());
        result.add(QuerySparkJobEnum.START_TIME.toString());
        result.add(QuerySparkJobEnum.END_TIME.toString());
        result.add(QuerySparkJobEnum.IF_SUCCESS.toString());

        result.addAll(getTimeDimensionsForMetrics());
        return result;
    }

    public static List<String> getMeasuresForMetricsQuerySparkJob() {
        List<String> result = Lists.newLinkedList();
        result.add(QuerySparkJobEnum.RESULT_SIZE.toString());
        result.add(QuerySparkJobEnum.EXECUTOR_DESERIALIZE_TIME.toString());
        result.add(QuerySparkJobEnum.EXECUTOR_DESERIALIZE_CPU_TIME.toString());
        result.add(QuerySparkJobEnum.EXECUTOR_RUN_TIME.toString());
        result.add(QuerySparkJobEnum.EXECUTOR_CPU_TIME.toString());
        result.add(QuerySparkJobEnum.JVM_GC_TIME.toString());
        result.add(QuerySparkJobEnum.RESULT_SERIALIZATION_TIME.toString());
        result.add(QuerySparkJobEnum.MEMORY_BYTE_SPILLED.toString());
        result.add(QuerySparkJobEnum.DISK_BYTES_SPILLED.toString());
        result.add(QuerySparkJobEnum.PEAK_EXECUTION_MEMORY.toString());

        return result;
    }

    public static List<String> getDimensionsForMetricsQuerySparkStage() {
        List<String> result = Lists.newLinkedList();
        result.add(RecordEvent.RecordReserveKeyEnum.HOST.toString());
        result.add(QuerySparkStageEnum.QUERY_ID.toString());
        result.add(QuerySparkStageEnum.EXECUTION_ID.toString());
        result.add(QuerySparkStageEnum.JOB_ID.toString());
        result.add(QuerySparkStageEnum.STAGE_ID.toString());
        result.add(QuerySparkStageEnum.SUBMIT_TIME.toString());
        result.add(QuerySparkStageEnum.PROJECT.toString());
        result.add(QuerySparkStageEnum.REALIZATION.toString());
        result.add(QuerySparkStageEnum.CUBOID_ID.toString());
        result.add(QuerySparkStageEnum.IF_SUCCESS.toString());

        result.addAll(getTimeDimensionsForMetrics());
        return result;
    }

    public static List<String> getMeasuresForMetricsQuerySparkStage() {
        List<String> result = Lists.newLinkedList();
        result.add(QuerySparkStageEnum.RESULT_SIZE.toString());
        result.add(QuerySparkStageEnum.EXECUTOR_DESERIALIZE_TIME.toString());
        result.add(QuerySparkStageEnum.EXECUTOR_DESERIALIZE_CPU_TIME.toString());
        result.add(QuerySparkStageEnum.EXECUTOR_RUN_TIME.toString());
        result.add(QuerySparkStageEnum.EXECUTOR_CPU_TIME.toString());
        result.add(QuerySparkStageEnum.JVM_GC_TIME.toString());
        result.add(QuerySparkStageEnum.RESULT_SERIALIZATION_TIME.toString());
        result.add(QuerySparkStageEnum.MEMORY_BYTE_SPILLED.toString());
        result.add(QuerySparkStageEnum.DISK_BYTES_SPILLED.toString());
        result.add(QuerySparkStageEnum.PEAK_EXECUTION_MEMORY.toString());

        return result;
    }

    public static List<String> getDimensionsForMetricsJob() {
        List<String> result = Lists.newLinkedList();
        result.add(JobPropertyEnum.USER.toString());
        result.add(JobPropertyEnum.PROJECT.toString());
        result.add(JobPropertyEnum.CUBE.toString());
        result.add(JobPropertyEnum.TYPE.toString());
        result.add(JobPropertyEnum.ALGORITHM.toString());

        result.addAll(getTimeDimensionsForMetrics());
        return result;
    }

    public static List<String> getMeasuresForMetricsJob() {
        List<String> result = Lists.newLinkedList();
        result.add(JobPropertyEnum.BUILD_DURATION.toString());
        result.add(JobPropertyEnum.SOURCE_SIZE.toString());
        result.add(JobPropertyEnum.CUBE_SIZE.toString());
        result.add(JobPropertyEnum.PER_BYTES_TIME_COST.toString());
        result.add(JobPropertyEnum.WAIT_RESOURCE_TIME.toString());

        result.add(JobPropertyEnum.STEP_DURATION_DISTINCT_COLUMNS.toString());
        result.add(JobPropertyEnum.STEP_DURATION_DICTIONARY.toString());
        result.add(JobPropertyEnum.STEP_DURATION_INMEM_CUBING.toString());
        result.add(JobPropertyEnum.STEP_DURATION_HFILE_CONVERT.toString());

        return result;
    }

    public static List<String> getDimensionsForMetricsJobException() {
        List<String> result = Lists.newLinkedList();
        result.add(JobPropertyEnum.USER.toString());
        result.add(JobPropertyEnum.PROJECT.toString());
        result.add(JobPropertyEnum.CUBE.toString());
        result.add(JobPropertyEnum.TYPE.toString());
        result.add(JobPropertyEnum.ALGORITHM.toString());
        result.add(JobPropertyEnum.EXCEPTION.toString());

        result.addAll(getTimeDimensionsForMetrics());
        return result;
    }

    public static List<String> getMeasuresForMetricsJobException() {
        List<String> result = Lists.newLinkedList();
        result.add(JobPropertyEnum.ID_CODE.toString());

        return result;
    }

    public static List<String> getTimeDimensionsForMetrics() {
        List<String> result = Lists.newLinkedList();
        result.add(RecordEvent.RecordReserveKeyEnum.TIME.toString());
        result.add(TimePropertyEnum.YEAR.toString());
        result.add(TimePropertyEnum.MONTH.toString());
        result.add(TimePropertyEnum.WEEK_BEGIN_DATE.toString());
        result.add(TimePropertyEnum.DAY_TIME.toString());
        result.add(TimePropertyEnum.DAY_DATE.toString());
        result.add(TimePropertyEnum.TIME_HOUR.toString());
        result.add(TimePropertyEnum.TIME_MINUTE.toString());

        return result;
    }

    public static DataModelDesc generateKylinModel(String owner, String tableName, List<String> dimensions,
            List<String> measures, PartitionDesc partitionDesc) {
        ModelDimensionDesc modelDimensionDesc = new ModelDimensionDesc();
        modelDimensionDesc.setTable(tableName);
        modelDimensionDesc.setColumns(dimensions.toArray(new String[dimensions.size()]));

        DataModelDesc kylinModel = new DataModelDesc();
        kylinModel.setName(tableName.replace('.', '_'));
        kylinModel.setOwner(owner);
        kylinModel.setDescription("");
        kylinModel.setLastModified(0L);
        kylinModel.setRootFactTableName(tableName);
        kylinModel.setJoinTables(new JoinTableDesc[0]);
        kylinModel.setDimensions(Lists.newArrayList(modelDimensionDesc));
        kylinModel.setMetrics(measures.toArray(new String[measures.size()]));
        kylinModel.setFilterCondition("");
        kylinModel.setPartitionDesc(partitionDesc);
        kylinModel.setCapacity(DataModelDesc.RealizationCapacity.SMALL);
        kylinModel.updateRandomUuid();

        return kylinModel;
    }
}

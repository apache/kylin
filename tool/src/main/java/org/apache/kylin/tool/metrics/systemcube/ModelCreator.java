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
import org.apache.kylin.metrics.lib.SinkTool;
import org.apache.kylin.metrics.lib.impl.RecordEvent;
import org.apache.kylin.metrics.lib.impl.TimePropertyEnum;
import org.apache.kylin.metrics.property.JobPropertyEnum;
import org.apache.kylin.metrics.property.QueryCubePropertyEnum;
import org.apache.kylin.metrics.property.QueryPropertyEnum;
import org.apache.kylin.metrics.property.QueryRPCPropertyEnum;
import org.apache.kylin.tool.metrics.systemcube.util.HiveSinkTool;

import com.google.common.collect.Lists;

public class ModelCreator {

    public static final Serializer<DataModelDesc> MODELDESC_SERIALIZER = new JsonSerializer<>(DataModelDesc.class);

    public static void main(String[] args) throws Exception {
        //        KylinConfig.setSandboxEnvIfPossible();
        KylinConfig config = KylinConfig.getInstanceFromEnv();

        DataModelDesc kylinModel = generateKylinModelForMetricsQuery("ADMIN", config, new HiveSinkTool());
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
            SinkTool sinkTool) {
        String tableName = sinkTool.getTableNameForMetrics(kylinConfig.getKylinMetricsSubjectQuery());
        return generateKylinModel(owner, tableName, getDimensionsForMetricsQuery(), getMeasuresForMetricsQuery(),
                getPartitionDesc(tableName));
    }

    public static DataModelDesc generateKylinModelForMetricsQueryCube(String owner, KylinConfig kylinConfig,
            SinkTool sinkTool) {
        String tableName = sinkTool.getTableNameForMetrics(kylinConfig.getKylinMetricsSubjectQueryCube());
        return generateKylinModel(owner, tableName, getDimensionsForMetricsQueryCube(),
                getMeasuresForMetricsQueryCube(), getPartitionDesc(tableName));
    }

    public static DataModelDesc generateKylinModelForMetricsQueryRPC(String owner, KylinConfig kylinConfig,
            SinkTool sinkTool) {
        String tableName = sinkTool.getTableNameForMetrics(kylinConfig.getKylinMetricsSubjectQueryRpcCall());
        return generateKylinModel(owner, tableName, getDimensionsForMetricsQueryRPC(), getMeasuresForMetricsQueryRPC(),
                getPartitionDesc(tableName));
    }

    public static DataModelDesc generateKylinModelForMetricsJob(String owner, KylinConfig kylinConfig,
            SinkTool sinkTool) {
        String tableName = sinkTool.getTableNameForMetrics(kylinConfig.getKylinMetricsSubjectJob());
        return generateKylinModel(owner, tableName, getDimensionsForMetricsJob(), getMeasuresForMetricsJob(),
                getPartitionDesc(tableName));
    }

    public static DataModelDesc generateKylinModelForMetricsJobException(String owner, KylinConfig kylinConfig,
            SinkTool sinkTool) {
        String tableName = sinkTool.getTableNameForMetrics(kylinConfig.getKylinMetricsSubjectJobException());
        return generateKylinModel(owner, tableName, getDimensionsForMetricsJobException(),
                getMeasuresForMetricsJobException(), getPartitionDesc(tableName));
    }

    public static List<String> getDimensionsForMetricsQuery() {
        List<String> result = Lists.newLinkedList();
        result.add(RecordEvent.RecordReserveKeyEnum.HOST.toString());
        result.add(QueryPropertyEnum.USER.toString());
        result.add(QueryPropertyEnum.PROJECT.toString());
        result.add(QueryPropertyEnum.REALIZATION.toString());
        result.add(QueryPropertyEnum.REALIZATION_TYPE.toString());
        result.add(QueryPropertyEnum.TYPE.toString());
        result.add(QueryPropertyEnum.EXCEPTION.toString());

        result.addAll(getTimeDimensionsForMetrics());
        return result;
    }

    public static List<String> getMeasuresForMetricsQuery() {
        List<String> result = Lists.newLinkedList();
        result.add(QueryPropertyEnum.ID_CODE.toString());
        result.add(QueryPropertyEnum.TIME_COST.toString());
        result.add(QueryPropertyEnum.CALCITE_RETURN_COUNT.toString());
        result.add(QueryPropertyEnum.STORAGE_RETURN_COUNT.toString());
        result.add(QueryPropertyEnum.AGGR_FILTER_COUNT.toString());

        return result;
    }

    public static List<String> getDimensionsForMetricsQueryCube() {
        List<String> result = Lists.newLinkedList();
        result.add(RecordEvent.RecordReserveKeyEnum.HOST.toString());
        result.add(QueryCubePropertyEnum.PROJECT.toString());
        result.add(QueryCubePropertyEnum.CUBE.toString());
        result.add(QueryCubePropertyEnum.SEGMENT.toString());
        result.add(QueryCubePropertyEnum.CUBOID_SOURCE.toString());
        result.add(QueryCubePropertyEnum.CUBOID_TARGET.toString());
        result.add(QueryCubePropertyEnum.FILTER_MASK.toString());
        result.add(QueryCubePropertyEnum.IF_MATCH.toString());
        result.add(QueryCubePropertyEnum.IF_SUCCESS.toString());

        result.addAll(getTimeDimensionsForMetrics());
        return result;
    }

    public static List<String> getMeasuresForMetricsQueryCube() {
        List<String> result = Lists.newLinkedList();
        result.add(QueryCubePropertyEnum.WEIGHT_PER_HIT.toString());
        result.add(QueryCubePropertyEnum.CALL_COUNT.toString());
        result.add(QueryCubePropertyEnum.TIME_SUM.toString());
        result.add(QueryCubePropertyEnum.TIME_MAX.toString());
        result.add(QueryCubePropertyEnum.SKIP_COUNT.toString());
        result.add(QueryCubePropertyEnum.SCAN_COUNT.toString());
        result.add(QueryCubePropertyEnum.RETURN_COUNT.toString());
        result.add(QueryCubePropertyEnum.AGGR_FILTER_COUNT.toString());
        result.add(QueryCubePropertyEnum.AGGR_COUNT.toString());

        return result;
    }

    public static List<String> getDimensionsForMetricsQueryRPC() {
        List<String> result = Lists.newLinkedList();
        result.add(RecordEvent.RecordReserveKeyEnum.HOST.toString());
        result.add(QueryRPCPropertyEnum.PROJECT.toString());
        result.add(QueryRPCPropertyEnum.REALIZATION.toString());
        result.add(QueryRPCPropertyEnum.RPC_SERVER.toString());
        result.add(QueryRPCPropertyEnum.EXCEPTION.toString());

        result.addAll(getTimeDimensionsForMetrics());
        return result;
    }

    public static List<String> getMeasuresForMetricsQueryRPC() {
        List<String> result = Lists.newLinkedList();
        result.add(QueryRPCPropertyEnum.CALL_TIME.toString());
        result.add(QueryRPCPropertyEnum.RETURN_COUNT.toString());
        result.add(QueryRPCPropertyEnum.SCAN_COUNT.toString());
        result.add(QueryRPCPropertyEnum.SKIP_COUNT.toString());
        result.add(QueryRPCPropertyEnum.AGGR_FILTER_COUNT.toString());
        result.add(QueryRPCPropertyEnum.AGGR_COUNT.toString());

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

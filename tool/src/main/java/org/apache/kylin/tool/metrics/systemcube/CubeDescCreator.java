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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.model.AggregationGroup;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.DimensionDesc;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.cube.model.HBaseColumnFamilyDesc;
import org.apache.kylin.cube.model.HBaseMappingDesc;
import org.apache.kylin.cube.model.RowKeyColDesc;
import org.apache.kylin.cube.model.RowKeyDesc;
import org.apache.kylin.cube.model.SelectRule;
import org.apache.kylin.dimension.DictionaryDimEnc;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.measure.percentile.PercentileMeasureType;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.IEngineAware;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metrics.lib.impl.RecordEvent;
import org.apache.kylin.metrics.lib.impl.TimePropertyEnum;
import org.apache.kylin.metrics.property.JobPropertyEnum;
import org.apache.kylin.metrics.property.QueryCubePropertyEnum;
import org.apache.kylin.metrics.property.QueryPropertyEnum;
import org.apache.kylin.metrics.property.QueryRPCPropertyEnum;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.shaded.com.google.common.collect.Sets;
import org.apache.kylin.tool.metrics.systemcube.def.MetricsSinkDesc;

public class CubeDescCreator {

    public static CubeDesc generateKylinCubeDescForMetricsQuery(KylinConfig config, MetricsSinkDesc sinkDesc) {
        String tableName = sinkDesc.getTableNameForMetrics(config.getKylinMetricsSubjectQuery());

        //Set for dimensions
        List<String> dimensions = ModelCreator.getDimensionsForMetricsQuery();
        dimensions.remove(TimePropertyEnum.DAY_TIME.toString());
        dimensions.remove(RecordEvent.RecordReserveKeyEnum.TIME.toString());

        List<DimensionDesc> dimensionDescList = Lists.newArrayListWithExpectedSize(dimensions.size());
        for (String dimensionName : dimensions) {
            dimensionDescList.add(getDimensionDesc(tableName, dimensionName));
        }

        //Set for measures
        List<String> measures = ModelCreator.getMeasuresForMetricsQuery();
        measures.remove(QueryPropertyEnum.ID_CODE.toString());
        List<MeasureDesc> measureDescList = Lists.newArrayListWithExpectedSize(measures.size() * 2 + 1 + 1);

        List<Pair<String, String>> measureTypeList = HiveTableCreator.getHiveColumnsForMetricsQuery();
        Map<String, String> measureTypeMap = Maps.newHashMapWithExpectedSize(measureTypeList.size());
        for (Pair<String, String> entry : measureTypeList) {
            measureTypeMap.put(entry.getFirst(), entry.getSecond());
        }
        measureDescList.add(getMeasureCount());
        measureDescList.add(getMeasureMin(QueryPropertyEnum.TIME_COST.toString(),
                measureTypeMap.get(QueryPropertyEnum.TIME_COST.toString())));
        for (String measure : measures) {
            measureDescList.add(getMeasureSum(measure, measureTypeMap.get(measure)));
            measureDescList.add(getMeasureMax(measure, measureTypeMap.get(measure)));
        }
        measureDescList.add(getMeasureHLL(QueryPropertyEnum.ID_CODE.toString()));
        measureDescList.add(getMeasurePercentile(QueryPropertyEnum.TIME_COST.toString()));

        //Set for row key
        RowKeyColDesc[] rowKeyColDescs = new RowKeyColDesc[dimensionDescList.size()];
        int idx = getTimeRowKeyColDesc(tableName, rowKeyColDescs);
        rowKeyColDescs[idx] = getRowKeyColDesc(tableName, QueryPropertyEnum.USER.toString(), idx + 1);
        idx++;
        rowKeyColDescs[idx] = getRowKeyColDesc(tableName, QueryPropertyEnum.PROJECT.toString(), idx + 1);
        idx++;
        rowKeyColDescs[idx] = getRowKeyColDesc(tableName, QueryPropertyEnum.REALIZATION.toString(), idx + 1);
        idx++;
        rowKeyColDescs[idx] = getRowKeyColDesc(tableName, QueryPropertyEnum.REALIZATION_TYPE.toString(), idx + 1);
        idx++;
        rowKeyColDescs[idx] = getRowKeyColDesc(tableName, QueryPropertyEnum.EXCEPTION.toString(), idx + 1);
        idx++;
        rowKeyColDescs[idx] = getRowKeyColDesc(tableName, QueryPropertyEnum.TYPE.toString(), idx + 1);
        idx++;
        rowKeyColDescs[idx] = getRowKeyColDesc(tableName, RecordEvent.RecordReserveKeyEnum.HOST.toString(), idx + 1);
        idx++;

        RowKeyDesc rowKeyDesc = new RowKeyDesc();
        rowKeyDesc.setRowkeyColumns(rowKeyColDescs);

        //Set for aggregation group
        String[][] hierarchy_dims = new String[2][];
        hierarchy_dims[0] = getTimeHierarchy();
        hierarchy_dims[1] = new String[2];
        hierarchy_dims[1][0] = QueryPropertyEnum.REALIZATION_TYPE.toString();
        hierarchy_dims[1][1] = QueryPropertyEnum.REALIZATION.toString();
        for (int i = 0; i < hierarchy_dims.length; i++) {
            hierarchy_dims[i] = refineColumnWithTable(tableName, hierarchy_dims[i]);
        }

        SelectRule selectRule = new SelectRule();
        selectRule.mandatoryDims = new String[0];
        selectRule.hierarchyDims = hierarchy_dims;
        selectRule.jointDims = new String[0][0];

        AggregationGroup aggGroup = new AggregationGroup();
        aggGroup.setIncludes(refineColumnWithTable(tableName, dimensions));
        aggGroup.setSelectRule(selectRule);

        //Set for hbase mapping
        HBaseMappingDesc hBaseMapping = new HBaseMappingDesc();
        hBaseMapping.setColumnFamily(getHBaseColumnFamily(measureDescList));

        return generateKylinCubeDesc(tableName, sinkDesc.getStorageType(), dimensionDescList, measureDescList,
                rowKeyDesc, aggGroup, hBaseMapping, sinkDesc.getCubeDescOverrideProperties());
    }

    public static CubeDesc generateKylinCubeDescForMetricsQueryCube(KylinConfig config, MetricsSinkDesc sinkDesc) {
        String tableName = sinkDesc.getTableNameForMetrics(config.getKylinMetricsSubjectQueryCube());

        //Set for dimensions
        List<String> dimensions = ModelCreator.getDimensionsForMetricsQueryCube();
        dimensions.remove(TimePropertyEnum.DAY_TIME.toString());
        dimensions.remove(RecordEvent.RecordReserveKeyEnum.TIME.toString());
        dimensions.remove(RecordEvent.RecordReserveKeyEnum.HOST.toString());
        dimensions.remove(QueryCubePropertyEnum.PROJECT.toString());

        List<DimensionDesc> dimensionDescList = Lists.newArrayListWithExpectedSize(dimensions.size());
        for (String dimensionName : dimensions) {
            dimensionDescList.add(getDimensionDesc(tableName, dimensionName));
        }

        //Set for measures
        List<String> measures = ModelCreator.getMeasuresForMetricsQueryCube();
        List<MeasureDesc> measureDescList = Lists.newArrayListWithExpectedSize(measures.size() * 2);

        List<Pair<String, String>> measureTypeList = HiveTableCreator.getHiveColumnsForMetricsQueryCube();
        Map<String, String> measureTypeMap = Maps.newHashMapWithExpectedSize(measureTypeList.size());
        for (Pair<String, String> entry : measureTypeList) {
            measureTypeMap.put(entry.getFirst(), entry.getSecond());
        }
        measureDescList.add(getMeasureCount());
        for (String measure : measures) {
            measureDescList.add(getMeasureSum(measure, measureTypeMap.get(measure)));
            if (!measure.equals(QueryCubePropertyEnum.WEIGHT_PER_HIT.toString())) {
                measureDescList.add(getMeasureMax(measure, measureTypeMap.get(measure)));
            }
        }

        //Set for row key
        RowKeyColDesc[] rowKeyColDescs = new RowKeyColDesc[dimensionDescList.size()];
        int idx = getTimeRowKeyColDesc(tableName, rowKeyColDescs);
        rowKeyColDescs[idx] = getRowKeyColDesc(tableName, QueryCubePropertyEnum.CUBE.toString(), idx + 1);
        idx++;
        rowKeyColDescs[idx] = getRowKeyColDesc(tableName, QueryCubePropertyEnum.SEGMENT.toString(), idx + 1);
        idx++;
        rowKeyColDescs[idx] = getRowKeyColDesc(tableName, QueryCubePropertyEnum.CUBOID_SOURCE.toString(), idx + 1);
        idx++;
        rowKeyColDescs[idx] = getRowKeyColDesc(tableName, QueryCubePropertyEnum.CUBOID_TARGET.toString(), idx + 1);
        idx++;
        rowKeyColDescs[idx] = getRowKeyColDesc(tableName, QueryCubePropertyEnum.FILTER_MASK.toString(), idx + 1);
        idx++;
        rowKeyColDescs[idx] = getRowKeyColDesc(tableName, QueryCubePropertyEnum.IF_MATCH.toString(), idx + 1);
        idx++;
        rowKeyColDescs[idx] = getRowKeyColDesc(tableName, QueryCubePropertyEnum.IF_SUCCESS.toString(), idx + 1);
        idx++;

        RowKeyDesc rowKeyDesc = new RowKeyDesc();
        rowKeyDesc.setRowkeyColumns(rowKeyColDescs);

        //Set for aggregation group
        String[] mandatory_dims = new String[] { QueryCubePropertyEnum.CUBE.toString() };
        mandatory_dims = refineColumnWithTable(tableName, mandatory_dims);

        String[][] hierarchy_dims = new String[1][];
        hierarchy_dims[0] = getTimeHierarchy();
        for (int i = 0; i < hierarchy_dims.length; i++) {
            hierarchy_dims[i] = refineColumnWithTable(tableName, hierarchy_dims[i]);
        }

        String[][] joint_dims = new String[1][];
        joint_dims[0] = new String[] { QueryCubePropertyEnum.CUBOID_SOURCE.toString(),
                QueryCubePropertyEnum.CUBOID_TARGET.toString() };
        for (int i = 0; i < joint_dims.length; i++) {
            joint_dims[i] = refineColumnWithTable(tableName, joint_dims[i]);
        }

        SelectRule selectRule = new SelectRule();
        selectRule.mandatoryDims = mandatory_dims;
        selectRule.hierarchyDims = hierarchy_dims;
        selectRule.jointDims = joint_dims;

        AggregationGroup aggGroup = new AggregationGroup();
        aggGroup.setIncludes(refineColumnWithTable(tableName, dimensions));
        aggGroup.setSelectRule(selectRule);

        //Set for hbase mapping
        HBaseMappingDesc hBaseMapping = new HBaseMappingDesc();
        hBaseMapping.setColumnFamily(getHBaseColumnFamily(measureDescList));

        return generateKylinCubeDesc(tableName, sinkDesc.getStorageType(), dimensionDescList, measureDescList,
                rowKeyDesc, aggGroup, hBaseMapping, sinkDesc.getCubeDescOverrideProperties());
    }

    public static CubeDesc generateKylinCubeDescForMetricsQueryRPC(KylinConfig config, MetricsSinkDesc sinkDesc) {
        String tableName = sinkDesc.getTableNameForMetrics(config.getKylinMetricsSubjectQueryRpcCall());

        //Set for dimensions
        List<String> dimensions = ModelCreator.getDimensionsForMetricsQueryRPC();
        dimensions.remove(TimePropertyEnum.DAY_TIME.toString());
        dimensions.remove(RecordEvent.RecordReserveKeyEnum.TIME.toString());

        List<DimensionDesc> dimensionDescList = Lists.newArrayListWithExpectedSize(dimensions.size());
        for (String dimensionName : dimensions) {
            dimensionDescList.add(getDimensionDesc(tableName, dimensionName));
        }

        //Set for measures
        List<String> measures = ModelCreator.getMeasuresForMetricsQueryRPC();
        List<MeasureDesc> measureDescList = Lists.newArrayListWithExpectedSize(measures.size() * 2 + 1 + 1);

        List<Pair<String, String>> measureTypeList = HiveTableCreator.getHiveColumnsForMetricsQueryRPC();
        Map<String, String> measureTypeMap = Maps.newHashMapWithExpectedSize(measureTypeList.size());
        for (Pair<String, String> entry : measureTypeList) {
            measureTypeMap.put(entry.getFirst(), entry.getSecond());
        }
        measureDescList.add(getMeasureCount());
        for (String measure : measures) {
            measureDescList.add(getMeasureSum(measure, measureTypeMap.get(measure)));
            measureDescList.add(getMeasureMax(measure, measureTypeMap.get(measure)));
        }
        measureDescList.add(getMeasurePercentile(QueryRPCPropertyEnum.CALL_TIME.toString()));

        //Set for row key
        RowKeyColDesc[] rowKeyColDescs = new RowKeyColDesc[dimensionDescList.size()];
        int idx = getTimeRowKeyColDesc(tableName, rowKeyColDescs);
        rowKeyColDescs[idx] = getRowKeyColDesc(tableName, QueryRPCPropertyEnum.PROJECT.toString(), idx + 1);
        idx++;
        rowKeyColDescs[idx] = getRowKeyColDesc(tableName, QueryRPCPropertyEnum.REALIZATION.toString(), idx + 1);
        idx++;
        rowKeyColDescs[idx] = getRowKeyColDesc(tableName, QueryRPCPropertyEnum.RPC_SERVER.toString(), idx + 1);
        idx++;
        rowKeyColDescs[idx] = getRowKeyColDesc(tableName, RecordEvent.RecordReserveKeyEnum.HOST.toString(), idx + 1);
        idx++;
        rowKeyColDescs[idx] = getRowKeyColDesc(tableName, QueryRPCPropertyEnum.EXCEPTION.toString(), idx + 1);
        idx++;

        RowKeyDesc rowKeyDesc = new RowKeyDesc();
        rowKeyDesc.setRowkeyColumns(rowKeyColDescs);

        //Set for aggregation group
        String[][] hierarchy_dims = new String[1][];
        hierarchy_dims[0] = getTimeHierarchy();
        for (int i = 0; i < hierarchy_dims.length; i++) {
            hierarchy_dims[i] = refineColumnWithTable(tableName, hierarchy_dims[i]);
        }

        SelectRule selectRule = new SelectRule();
        selectRule.mandatoryDims = new String[0];
        selectRule.hierarchyDims = hierarchy_dims;
        selectRule.jointDims = new String[0][0];

        AggregationGroup aggGroup = new AggregationGroup();
        aggGroup.setIncludes(refineColumnWithTable(tableName, dimensions));
        aggGroup.setSelectRule(selectRule);

        //Set for hbase mapping
        HBaseMappingDesc hBaseMapping = new HBaseMappingDesc();
        hBaseMapping.setColumnFamily(getHBaseColumnFamily(measureDescList));

        return generateKylinCubeDesc(tableName, sinkDesc.getStorageType(), dimensionDescList, measureDescList,
                rowKeyDesc, aggGroup, hBaseMapping, sinkDesc.getCubeDescOverrideProperties());
    }

    public static CubeDesc generateKylinCubeDescForMetricsJob(KylinConfig config, MetricsSinkDesc sinkDesc) {
        String tableName = sinkDesc.getTableNameForMetrics(config.getKylinMetricsSubjectJob());

        //Set for dimensions
        List<String> dimensions = ModelCreator.getDimensionsForMetricsJob();
        dimensions.remove(TimePropertyEnum.DAY_TIME.toString());
        dimensions.remove(RecordEvent.RecordReserveKeyEnum.TIME.toString());
        dimensions.remove(RecordEvent.RecordReserveKeyEnum.HOST.toString());

        List<DimensionDesc> dimensionDescList = Lists.newArrayListWithExpectedSize(dimensions.size());
        for (String dimensionName : dimensions) {
            dimensionDescList.add(getDimensionDesc(tableName, dimensionName));
        }

        //Set for measures
        List<String> measures = ModelCreator.getMeasuresForMetricsJob();
        List<MeasureDesc> measureDescList = Lists.newArrayListWithExpectedSize((measures.size() - 4) * 3 + 1 + 1 + 4);

        Set<String> stepDuration = Sets.newHashSet();
        stepDuration.add(JobPropertyEnum.STEP_DURATION_DISTINCT_COLUMNS.toString());
        stepDuration.add(JobPropertyEnum.STEP_DURATION_DICTIONARY.toString());
        stepDuration.add(JobPropertyEnum.STEP_DURATION_INMEM_CUBING.toString());
        stepDuration.add(JobPropertyEnum.STEP_DURATION_HFILE_CONVERT.toString());

        List<Pair<String, String>> measureTypeList = HiveTableCreator.getHiveColumnsForMetricsJob();
        Map<String, String> measureTypeMap = Maps.newHashMapWithExpectedSize(measureTypeList.size());
        for (Pair<String, String> entry : measureTypeList) {
            measureTypeMap.put(entry.getFirst(), entry.getSecond());
        }
        measureDescList.add(getMeasureCount());
        for (String measure : measures) {
            measureDescList.add(getMeasureSum(measure, measureTypeMap.get(measure)));
            measureDescList.add(getMeasureMax(measure, measureTypeMap.get(measure)));
            if (!stepDuration.contains(measure)) {
                measureDescList.add(getMeasureMin(measure, measureTypeMap.get(measure)));
            }
        }
        measureDescList.add(getMeasurePercentile(JobPropertyEnum.BUILD_DURATION.toString()));

        //Set for row key
        RowKeyColDesc[] rowKeyColDescs = new RowKeyColDesc[dimensionDescList.size()];
        int idx = getTimeRowKeyColDesc(tableName, rowKeyColDescs);
        rowKeyColDescs[idx] = getRowKeyColDesc(tableName, JobPropertyEnum.USER.toString(), idx + 1);
        idx++;
        rowKeyColDescs[idx] = getRowKeyColDesc(tableName, JobPropertyEnum.PROJECT.toString(), idx + 1);
        idx++;
        rowKeyColDescs[idx] = getRowKeyColDesc(tableName, JobPropertyEnum.CUBE.toString(), idx + 1);
        idx++;
        rowKeyColDescs[idx] = getRowKeyColDesc(tableName, JobPropertyEnum.ALGORITHM.toString(), idx + 1);
        idx++;
        rowKeyColDescs[idx] = getRowKeyColDesc(tableName, JobPropertyEnum.TYPE.toString(), idx + 1);
        idx++;

        RowKeyDesc rowKeyDesc = new RowKeyDesc();
        rowKeyDesc.setRowkeyColumns(rowKeyColDescs);

        //Set for aggregation group
        String[][] hierarchy_dims = new String[1][];
        hierarchy_dims[0] = getTimeHierarchy();
        for (int i = 0; i < hierarchy_dims.length; i++) {
            hierarchy_dims[i] = refineColumnWithTable(tableName, hierarchy_dims[i]);
        }

        SelectRule selectRule = new SelectRule();
        selectRule.mandatoryDims = new String[0];
        selectRule.hierarchyDims = hierarchy_dims;
        selectRule.jointDims = new String[0][0];

        AggregationGroup aggGroup = new AggregationGroup();
        aggGroup.setIncludes(refineColumnWithTable(tableName, dimensions));
        aggGroup.setSelectRule(selectRule);

        //Set for hbase mapping
        HBaseMappingDesc hBaseMapping = new HBaseMappingDesc();
        hBaseMapping.setColumnFamily(getHBaseColumnFamily(measureDescList));

        return generateKylinCubeDesc(tableName, sinkDesc.getStorageType(), dimensionDescList, measureDescList,
                rowKeyDesc, aggGroup, hBaseMapping, sinkDesc.getCubeDescOverrideProperties());
    }

    public static CubeDesc generateKylinCubeDescForMetricsJobException(KylinConfig config, MetricsSinkDesc sinkDesc) {
        String tableName = sinkDesc.getTableNameForMetrics(config.getKylinMetricsSubjectJobException());

        //Set for dimensions
        List<String> dimensions = ModelCreator.getDimensionsForMetricsJobException();
        dimensions.remove(TimePropertyEnum.DAY_TIME.toString());
        dimensions.remove(RecordEvent.RecordReserveKeyEnum.TIME.toString());
        dimensions.remove(RecordEvent.RecordReserveKeyEnum.HOST.toString());

        List<DimensionDesc> dimensionDescList = Lists.newArrayListWithExpectedSize(dimensions.size());
        for (String dimensionName : dimensions) {
            dimensionDescList.add(getDimensionDesc(tableName, dimensionName));
        }

        //Set for measures
        List<String> measures = ModelCreator.getMeasuresForMetricsJobException();
        measures.remove(JobPropertyEnum.ID_CODE.toString());
        List<MeasureDesc> measureDescList = Lists.newArrayListWithExpectedSize(1);

        measureDescList.add(getMeasureCount());

        //Set for row key
        RowKeyColDesc[] rowKeyColDescs = new RowKeyColDesc[dimensionDescList.size()];
        int idx = getTimeRowKeyColDesc(tableName, rowKeyColDescs);
        rowKeyColDescs[idx] = getRowKeyColDesc(tableName, JobPropertyEnum.USER.toString(), idx + 1);
        idx++;
        rowKeyColDescs[idx] = getRowKeyColDesc(tableName, JobPropertyEnum.PROJECT.toString(), idx + 1);
        idx++;
        rowKeyColDescs[idx] = getRowKeyColDesc(tableName, JobPropertyEnum.CUBE.toString(), idx + 1);
        idx++;
        rowKeyColDescs[idx] = getRowKeyColDesc(tableName, JobPropertyEnum.ALGORITHM.toString(), idx + 1);
        idx++;
        rowKeyColDescs[idx] = getRowKeyColDesc(tableName, JobPropertyEnum.TYPE.toString(), idx + 1);
        idx++;
        rowKeyColDescs[idx] = getRowKeyColDesc(tableName, JobPropertyEnum.EXCEPTION.toString(), idx + 1);
        idx++;

        RowKeyDesc rowKeyDesc = new RowKeyDesc();
        rowKeyDesc.setRowkeyColumns(rowKeyColDescs);

        //Set for aggregation group
        String[][] hierarchy_dims = new String[1][];
        hierarchy_dims[0] = getTimeHierarchy();
        for (int i = 0; i < hierarchy_dims.length; i++) {
            hierarchy_dims[i] = refineColumnWithTable(tableName, hierarchy_dims[i]);
        }

        SelectRule selectRule = new SelectRule();
        selectRule.mandatoryDims = new String[0];
        selectRule.hierarchyDims = hierarchy_dims;
        selectRule.jointDims = new String[0][0];

        AggregationGroup aggGroup = new AggregationGroup();
        aggGroup.setIncludes(refineColumnWithTable(tableName, dimensions));
        aggGroup.setSelectRule(selectRule);

        //Set for hbase mapping
        HBaseMappingDesc hBaseMapping = new HBaseMappingDesc();
        hBaseMapping.setColumnFamily(getHBaseColumnFamily(measureDescList));

        return generateKylinCubeDesc(tableName, sinkDesc.getStorageType(), dimensionDescList, measureDescList,
                rowKeyDesc, aggGroup, hBaseMapping, sinkDesc.getCubeDescOverrideProperties());
    }

    public static CubeDesc generateKylinCubeDesc(String tableName, int storageType,
            List<DimensionDesc> dimensionDescList, List<MeasureDesc> measureDescList, RowKeyDesc rowKeyDesc,
            AggregationGroup aggGroup, HBaseMappingDesc hBaseMapping, Map<String, String> overrideProperties) {
        CubeDesc desc = new CubeDesc();
        desc.setName(tableName.replace('.', '_'));
        desc.setModelName(tableName.replace('.', '_'));
        desc.setDescription("");
        desc.setLastModified(0L);
        desc.setDimensions(dimensionDescList);
        desc.setMeasures(measureDescList);
        desc.setRowkey(rowKeyDesc);
        desc.setHbaseMapping(hBaseMapping);
        desc.setNotifyEmailList(Lists.<String> newArrayList());
        desc.setNotifyDingTalkList(Lists.<String> newArrayList());
        desc.setStatusNeedNotify(Lists.newArrayList(JobStatusEnum.ERROR.toString()));
        desc.setAutoMergeTimeRanges(new long[] { 86400000L, 604800000L, 2419200000L, 7776000000L, 31104000000L });
        desc.setEngineType(IEngineAware.ID_MR_V2);
        desc.setStorageType(storageType);
        desc.setAggregationGroups(Lists.newArrayList(aggGroup));
        desc.getOverrideKylinProps().putAll(overrideProperties);
        desc.updateRandomUuid();
        return desc;
    }

    public static HBaseColumnFamilyDesc[] getHBaseColumnFamily(List<MeasureDesc> measureDescList) {
        List<String> normalMeasureList = Lists.newLinkedList();
        List<String> largeMeasureList = Lists.newLinkedList();
        for (MeasureDesc measureDesc : measureDescList) {
            if (measureDesc.getFunction().isCountDistinct()
                    || measureDesc.getFunction().getExpression().equals(PercentileMeasureType.FUNC_PERCENTILE)) {
                largeMeasureList.add(measureDesc.getName());
            } else {
                normalMeasureList.add(measureDesc.getName());
            }
        }
        List<HBaseColumnFamilyDesc> columnFamilyDescList = Lists.newLinkedList();
        int idx = 1;
        if (normalMeasureList.size() > 0) {
            HBaseColumnDesc columnDesc = new HBaseColumnDesc();
            columnDesc.setQualifier("M");
            columnDesc.setMeasureRefs(normalMeasureList.toArray(new String[normalMeasureList.size()]));
            HBaseColumnFamilyDesc columnFamilyDesc = new HBaseColumnFamilyDesc();
            columnFamilyDesc.setName("F" + idx++);
            columnFamilyDesc.setColumns(new HBaseColumnDesc[] { columnDesc });

            columnFamilyDescList.add(columnFamilyDesc);
        }
        for (String largeMeasure : largeMeasureList) {
            HBaseColumnDesc columnDesc = new HBaseColumnDesc();
            columnDesc.setQualifier("M");
            columnDesc.setMeasureRefs(new String[] { largeMeasure });
            HBaseColumnFamilyDesc columnFamilyDesc = new HBaseColumnFamilyDesc();
            columnFamilyDesc.setName("F" + idx++);
            columnFamilyDesc.setColumns(new HBaseColumnDesc[] { columnDesc });

            columnFamilyDescList.add(columnFamilyDesc);
        }

        return columnFamilyDescList.toArray(new HBaseColumnFamilyDesc[columnFamilyDescList.size()]);
    }

    public static String[] getTimeHierarchy() {
        String[] result = new String[4];
        result[0] = TimePropertyEnum.YEAR.toString();
        result[1] = TimePropertyEnum.MONTH.toString();
        result[2] = TimePropertyEnum.WEEK_BEGIN_DATE.toString();
        result[3] = TimePropertyEnum.DAY_DATE.toString();
        return result;
    }

    public static String[] refineColumnWithTable(String tableName, List<String> columns) {
        String[] dimensions = new String[columns.size()];
        for (int i = 0; i < dimensions.length; i++) {
            dimensions[i] = tableName.substring(tableName.lastIndexOf(".") + 1) + "." + columns.get(i);
        }
        return dimensions;
    }

    public static String[] refineColumnWithTable(String tableName, String[] columns) {
        String[] dimensions = new String[columns.length];
        for (int i = 0; i < dimensions.length; i++) {
            dimensions[i] = tableName.substring(tableName.lastIndexOf(".") + 1) + "." + columns[i];
        }
        return dimensions;
    }

    public static int getTimeRowKeyColDesc(String tableName, RowKeyColDesc[] rowKeyColDescs) {
        int idx = 0;
        rowKeyColDescs[idx] = getRowKeyColDesc(tableName, TimePropertyEnum.DAY_DATE.toString(), idx + 1);
        idx++;
        rowKeyColDescs[idx] = getRowKeyColDesc(tableName, TimePropertyEnum.WEEK_BEGIN_DATE.toString(), idx + 1);
        idx++;
        rowKeyColDescs[idx] = getRowKeyColDesc(tableName, TimePropertyEnum.MONTH.toString(), idx + 1);
        idx++;
        rowKeyColDescs[idx] = getRowKeyColDesc(tableName, TimePropertyEnum.YEAR.toString(), idx + 1);
        idx++;
        rowKeyColDescs[idx] = getRowKeyColDesc(tableName, TimePropertyEnum.TIME_HOUR.toString(), idx + 1);
        idx++;
        rowKeyColDescs[idx] = getRowKeyColDesc(tableName, TimePropertyEnum.TIME_MINUTE.toString(), idx + 1);
        idx++;
        return idx;
    }

    public static RowKeyColDesc getRowKeyColDesc(String tableName, String column, int id) {
        RowKeyColDesc rowKeyColDesc = new RowKeyColDesc();
        rowKeyColDesc.setIndex(Integer.toString(id));
        rowKeyColDesc.setColumn(tableName.substring(tableName.lastIndexOf(".") + 1) + "." + column);
        rowKeyColDesc.setEncoding(DictionaryDimEnc.ENCODING_NAME);
        rowKeyColDesc.setShardBy(false);
        return rowKeyColDesc;
    }

    public static DimensionDesc getDimensionDesc(String tableName, String dimension) {
        DimensionDesc dimensionDesc = new DimensionDesc();
        dimensionDesc.setName(dimension);
        dimensionDesc.setTable(tableName.substring(tableName.lastIndexOf(".") + 1));
        dimensionDesc.setColumn(dimension);
        return dimensionDesc;
    }

    public static MeasureDesc getMeasureCount() {
        ParameterDesc parameterDesc = new ParameterDesc();
        parameterDesc.setValue("1");
        parameterDesc.setType(FunctionDesc.PARAMETER_TYPE_CONSTANT);

        FunctionDesc function = new FunctionDesc();
        function.setExpression(FunctionDesc.FUNC_COUNT);
        function.setParameter(parameterDesc);
        function.setReturnType(HiveTableCreator.HiveTypeEnum.HBIGINT.toString());

        MeasureDesc result = new MeasureDesc();
        result.setName("_COUNT_");
        result.setFunction(function);
        return result;
    }

    public static MeasureDesc getMeasureSum(String column, String dataType) {
        ParameterDesc parameterDesc = new ParameterDesc();
        parameterDesc.setValue(column);
        parameterDesc.setType(FunctionDesc.PARAMETER_TYPE_COLUMN);

        FunctionDesc function = new FunctionDesc();
        function.setExpression(FunctionDesc.FUNC_SUM);
        function.setParameter(parameterDesc);
        function.setReturnType(dataType.equals(HiveTableCreator.HiveTypeEnum.HDOUBLE.toString())
                ? HiveTableCreator.HiveTypeEnum.HDECIMAL.toString()
                : dataType);

        MeasureDesc result = new MeasureDesc();
        result.setName(column + "_SUM");
        result.setFunction(function);
        return result;
    }

    public static MeasureDesc getMeasureMax(String column, String dataType) {
        ParameterDesc parameterDesc = new ParameterDesc();
        parameterDesc.setValue(column);
        parameterDesc.setType(FunctionDesc.PARAMETER_TYPE_COLUMN);

        FunctionDesc function = new FunctionDesc();
        function.setExpression(FunctionDesc.FUNC_MAX);
        function.setParameter(parameterDesc);
        function.setReturnType(dataType);

        MeasureDesc result = new MeasureDesc();
        result.setName(column + "_MAX");
        result.setFunction(function);
        return result;
    }

    public static MeasureDesc getMeasureMin(String column, String dataType) {
        ParameterDesc parameterDesc = new ParameterDesc();
        parameterDesc.setValue(column);
        parameterDesc.setType(FunctionDesc.PARAMETER_TYPE_COLUMN);

        FunctionDesc function = new FunctionDesc();
        function.setExpression(FunctionDesc.FUNC_MIN);
        function.setParameter(parameterDesc);
        function.setReturnType(dataType);

        MeasureDesc result = new MeasureDesc();
        result.setName(column + "_MIN");
        result.setFunction(function);
        return result;
    }

    public static MeasureDesc getMeasureHLL(String column) {
        ParameterDesc parameterDesc = new ParameterDesc();
        parameterDesc.setValue(column);
        parameterDesc.setType(FunctionDesc.PARAMETER_TYPE_COLUMN);

        FunctionDesc function = new FunctionDesc();
        function.setExpression(FunctionDesc.FUNC_COUNT_DISTINCT);
        function.setParameter(parameterDesc);
        function.setReturnType("hllc12");

        MeasureDesc result = new MeasureDesc();
        result.setName(column + "_HLL");
        result.setFunction(function);
        return result;
    }

    public static MeasureDesc getMeasurePercentile(String column) {
        ParameterDesc parameterDesc = new ParameterDesc();
        parameterDesc.setValue(column);
        parameterDesc.setType(FunctionDesc.PARAMETER_TYPE_COLUMN);

        FunctionDesc function = new FunctionDesc();
        function.setExpression(PercentileMeasureType.FUNC_PERCENTILE);
        function.setParameter(parameterDesc);
        function.setReturnType("percentile(100)");

        MeasureDesc result = new MeasureDesc();
        result.setName(column + "_PERCENTILE");
        result.setFunction(function);
        return result;
    }
}

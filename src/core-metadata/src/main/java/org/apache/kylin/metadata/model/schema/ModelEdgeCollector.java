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
package org.apache.kylin.metadata.model.schema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.cube.cuboid.NAggregationGroup;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.util.ComputedColumnUtil;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.Maps;

import io.kyligence.kap.guava20.shaded.common.graph.Graph;
import io.kyligence.kap.guava20.shaded.common.graph.MutableGraph;
import io.kyligence.kap.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Lists;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

@RequiredArgsConstructor
class ModelEdgeCollector {

    @NonNull
    private IndexPlan indexPlan;

    @NonNull
    private MutableGraph<SchemaNode> graph;

    private NDataModel model;
    private Map<Integer, NDataModel.NamedColumn> effectiveNamedColumns;
    private Map<String, NDataModel.NamedColumn> nameColumnIdMap;
    private ImmutableBiMap<Integer, TblColRef> effectiveCols;
    private ImmutableBiMap<Integer, NDataModel.Measure> effectiveMeasures;
    private Map<Integer, String> modelColumnMeasureIdNameMap = new HashMap<>();

    public Graph<SchemaNode> collect() {
        model = indexPlan.getModel();
        effectiveNamedColumns = model.getEffectiveNamedColumns();
        effectiveCols = model.getEffectiveCols();
        effectiveMeasures = model.getEffectiveMeasures();
        nameColumnIdMap = effectiveNamedColumns.entrySet().stream()
                .collect(Collectors.toMap(e -> e.getValue().getAliasDotColumn(), Map.Entry::getValue));

        modelColumnMeasureIdNameMap.putAll(model.getAllMeasures().stream().filter(measure -> !measure.isTomb())
                .collect(Collectors.toMap(NDataModel.Measure::getId, NDataModel.Measure::getName)));
        modelColumnMeasureIdNameMap.putAll(model.getAllNamedColumns().stream()
                .collect(Collectors.toMap(NDataModel.NamedColumn::getId, NDataModel.NamedColumn::getAliasDotColumn)));

        collectModelColumns();
        collectModelSignificant();
        collectDimensionAndMeasure();

        collectIndex(indexPlan.getWhitelistLayouts(), SchemaNodeType.WHITE_LIST_INDEX, Lists.newArrayList());
        collectIndex(indexPlan.getToBeDeletedIndexes().stream().flatMap(index -> index.getLayouts().stream())
                .collect(Collectors.toList()), SchemaNodeType.TO_BE_DELETED_INDEX, Lists.newArrayList());
        collectIndex(indexPlan.getRuleBaseLayouts(), SchemaNodeType.RULE_BASED_INDEX, indexPlan.getAggShardByColumns());

        collectAggGroup();
        collectIndexPlan();
        return graph;
    }

    private void collectModelColumns() {
        val ccs = model.getComputedColumnDescs().stream()
                .collect(Collectors.toMap(ComputedColumnDesc::getColumnName, Function.identity()));
        effectiveCols.forEach((id, tblColRef) -> {
            val namedColumn = effectiveNamedColumns.get(id);
            if (tblColRef.getColumnDesc().isComputedColumn()) {
                return;
            }
            graph.putEdge(SchemaNode.ofTableColumn(tblColRef.getColumnDesc()),
                    SchemaNode.ofModelColumn(namedColumn, model.getAlias()));
        });
        effectiveCols.forEach((id, tblColRef) -> {
            if (!tblColRef.getColumnDesc().isComputedColumn()) {
                return;
            }
            val namedColumn = effectiveNamedColumns.get(id);
            val cc = ccs.get(tblColRef.getName());
            val ccNode = SchemaNode.ofModelCC(cc, model.getAlias(), model.getRootFactTableName());
            collectExprWithModel(cc.getExpression(), ccNode);
            graph.putEdge(ccNode, SchemaNode.ofModelColumn(namedColumn, model.getAlias()));
        });
    }

    private void collectModelSignificant() {
        if (model.getPartitionDesc() != null && model.getPartitionDesc().getPartitionDateColumnRef() != null) {
            val colRef = model.getPartitionDesc().getPartitionDateColumnRef();
            val nameColumn = nameColumnIdMap.get(colRef.getAliasDotName());
            graph.putEdge(SchemaNode.ofModelColumn(nameColumn, model.getAlias()),
                    SchemaNode.ofPartition(model.getPartitionDesc(), model.getAlias()));
        }

        if (model.isMultiPartitionModel()) {
            val colRefs = model.getMultiPartitionDesc().getColumnRefs();
            val multiplePartitionNode = SchemaNode.ofMultiplePartition(model.getMultiPartitionDesc(), model.getAlias());
            for (TblColRef colRef : colRefs) {
                val nameColumn = nameColumnIdMap.get(colRef.getAliasDotName());
                graph.putEdge(SchemaNode.ofModelColumn(nameColumn, model.getAlias()), multiplePartitionNode);
            }
        }

        // fact table
        graph.putEdge(SchemaNode.ofTable(model.getRootFactTable()),
                SchemaNode.ofModelFactTable(model.getRootFactTable(), model.getAlias()));

        for (JoinTableDesc joinTable : model.getJoinTables()) {
            // dim table
            graph.putEdge(SchemaNode.ofTable(joinTable.getTableRef()),
                    SchemaNode.ofModelDimensionTable(joinTable.getTableRef(), model.getAlias()));

            for (int i = 0; i < joinTable.getJoin().getPrimaryKey().length; i++) {
                SchemaNode join = SchemaNode.ofJoin(joinTable, joinTable.getJoin().getFKSide(),
                        joinTable.getJoin().getPKSide(), joinTable.getJoin(), model.getAlias());

                String fkCol = joinTable.getJoin().getForeignKey()[i];
                val fkNameColumn = nameColumnIdMap.get(fkCol);
                graph.putEdge(SchemaNode.ofModelColumn(fkNameColumn, model.getAlias()), join);

                String pkCol = joinTable.getJoin().getPrimaryKey()[i];
                val pkNameColumn = nameColumnIdMap.get(pkCol);
                graph.putEdge(SchemaNode.ofModelColumn(pkNameColumn, model.getAlias()), join);
            }
        }

        if (StringUtils.isNotEmpty(model.getFilterCondition())) {
            collectExprWithModel(model.getFilterCondition(),
                    SchemaNode.ofFilter(model.getAlias(), model.getFilterCondition()));
        }
    }

    private void collectDimensionAndMeasure() {
        model.getEffectiveDimensions().forEach((id, dimension) -> {
            val nameColumn = nameColumnIdMap.get(dimension.getAliasDotName());
            graph.putEdge(SchemaNode.ofModelColumn(nameColumn, model.getAlias()),
                    SchemaNode.ofDimension(nameColumn, model.getAlias()));
        });
        model.getEffectiveMeasures().forEach((id, measure) -> {
            val params = measure.getFunction().getParameters();
            SchemaNode measureNode = SchemaNode.ofMeasure(measure, model.getAlias());
            graph.addNode(measureNode);
            if (CollectionUtils.isEmpty(params)) {
                return;
            }
            for (ParameterDesc param : params) {
                if (param.isConstant()) {
                    continue;
                }
                val colRef = param.getColRef();
                val nameColumn = nameColumnIdMap.get(colRef.getAliasDotName());
                graph.putEdge(SchemaNode.ofModelColumn(nameColumn, model.getAlias()), measureNode);
            }
        });
    }

    private void collectIndex(List<LayoutEntity> allLayouts, SchemaNodeType type, List<Integer> aggShardByColumns) {
        val colNodes = Maps.<Integer, SchemaNode> newHashMap();
        for (LayoutEntity layout : allLayouts) {
            val indexNode = SchemaNode.ofIndex(type, layout, model, modelColumnMeasureIdNameMap,
                    type == SchemaNodeType.RULE_BASED_INDEX ? aggShardByColumns : null);
            if (layout.getIndex().isTableIndex()) {
                for (Integer col : layout.getColOrder()) {
                    val namedColumn = effectiveNamedColumns.get(col);
                    graph.putEdge(SchemaNode.ofModelColumn(namedColumn, model.getAlias()), indexNode);
                }
                continue;
            }
            for (Integer col : layout.getColOrder()) {
                colNodes.computeIfAbsent(col, id -> {
                    if (col < NDataModel.MEASURE_ID_BASE) {
                        val namedColumn = effectiveNamedColumns.get(col);
                        return SchemaNode.ofDimension(namedColumn, model.getAlias());
                    } else {
                        NDataModel.Measure measure = effectiveMeasures.get(col);
                        if (measure == null) {
                            return null;
                        }
                        return SchemaNode.ofMeasure(measure, model.getAlias());
                    }
                });

                val colSchemaNode = colNodes.get(col);
                if (colSchemaNode == null) {
                    continue;
                }

                graph.putEdge(colSchemaNode, indexNode);
            }
        }
    }

    private void collectExprWithModel(String expr, SchemaNode target) {
        val pairs = ComputedColumnUtil.ExprIdentifierFinder.getExprIdentifiers(expr);
        for (Pair<String, String> pair : pairs) {
            graph.putEdge(SchemaNode.ofModelColumn(nameColumnIdMap.get(pair.getFirst() + "." + pair.getSecond()),
                    model.getAlias()), target);
        }
    }

    private void collectAggGroup() {
        val ruleBasedIndex = indexPlan.getRuleBasedIndex();
        if (ruleBasedIndex == null) {
            return;
        }
        int index = 0;
        for (NAggregationGroup aggGroup : ruleBasedIndex.getAggregationGroups()) {
            val aggGroupNode = SchemaNodeType.AGG_GROUP.withKey(model.getAlias() + "/" + index);
            for (Integer col : aggGroup.getIncludes()) {
                val namedColumn = effectiveNamedColumns.get(col);
                graph.putEdge(SchemaNode.ofDimension(namedColumn, model.getAlias()), aggGroupNode);

            }
            for (Integer measure : aggGroup.getMeasures()) {
                graph.putEdge(SchemaNode.ofMeasure(effectiveMeasures.get(measure), model.getAlias()), aggGroupNode);
            }
            index++;
        }
    }

    private void collectIndexPlan() {
        val aggShardNode = SchemaNodeType.INDEX_AGG_SHARD.withKey(model.getAlias());
        collectAggExpertColumns(indexPlan.getAggShardByColumns(), aggShardNode);

        val aggPartitionNode = SchemaNodeType.INDEX_AGG_EXTEND_PARTITION.withKey(model.getAlias());
        collectAggExpertColumns(indexPlan.getExtendPartitionColumns(), aggPartitionNode);
    }

    private void collectAggExpertColumns(List<Integer> cols, SchemaNode node) {
        if (CollectionUtils.isEmpty(cols)) {
            return;
        }
        for (Integer col : cols) {
            val namedColumn = effectiveNamedColumns.get(col);
            graph.putEdge(SchemaNode.ofModelColumn(namedColumn, model.getAlias()), node);
        }
        for (LayoutEntity layout : indexPlan.getRuleBaseLayouts()) {
            if (layout.getColOrder().containsAll(indexPlan.getAggShardByColumns())) {
                graph.putEdge(node, SchemaNode.ofIndex(SchemaNodeType.RULE_BASED_INDEX, layout, model,
                        modelColumnMeasureIdNameMap, null));
            }
        }
    }

}

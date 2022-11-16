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

import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang.ArrayUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.RuleBasedIndex;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.guava20.shaded.common.graph.Graph;
import io.kyligence.kap.guava20.shaded.common.graph.Graphs;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.val;

@Data
public class AffectedModelContext {

    @NonNull
    private final Set<SchemaNode> updatedNodes;
    private final Set<SchemaNode> shouldDeleteNodes;
    private final String project;
    private final String modelId;
    private final String modelAlias;

    private final boolean isBroken;
    @Getter
    private Set<Long> updatedLayouts = Sets.newHashSet();
    @Getter
    private Set<Long> shouldDeleteLayouts = Sets.newHashSet();
    @Getter
    private Set<Long> addLayouts = Sets.newHashSet();

    private Set<Integer> dimensions = Sets.newHashSet();
    private Set<Integer> measures = Sets.newHashSet();
    private Set<Integer> columns = Sets.newHashSet();
    private Set<String> computedColumns = Sets.newHashSet();

    private Map<NDataModel.Measure, NDataModel.Measure> updateIdMeasureMap = Maps.newHashMap();
    private Map<Integer, NDataModel.Measure> updateMeasureMap = Maps.newHashMap();

    public AffectedModelContext(String project, String modelId, Set<SchemaNode> updatedNodes, boolean isDelete) {
        this(project, modelId, updatedNodes, Sets.newHashSet(), isDelete);
    }

    public AffectedModelContext(String project, String modelId, Set<SchemaNode> updatedNodes,
            Set<Pair<NDataModel.Measure, NDataModel.Measure>> updateMeasures, boolean isDelete) {
        this.project = project;
        this.modelId = modelId;
        this.modelAlias = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .getDataModelDesc(modelId).getAlias();
        this.updatedNodes = updatedNodes;
        this.updateIdMeasureMap = updateMeasures.stream().collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));
        this.updateMeasureMap = updateMeasures.stream()
                .collect(Collectors.toMap(pair -> pair.getFirst().getId(), Pair::getSecond));
        this.shouldDeleteNodes = calcShouldDeletedNodes(isDelete);
        if (isDelete) {
            isBroken = updatedNodes.stream().anyMatch(SchemaNode::isCauseModelBroken);
        } else {
            isBroken = false;
        }
        updatedLayouts = filterIndexFromNodes(updatedNodes);
        shouldDeleteLayouts = filterIndexFromNodes(shouldDeleteNodes);
        columns = shouldDeleteNodes.stream().filter(node -> node.getType() == SchemaNodeType.MODEL_COLUMN)
                .map(node -> Integer.parseInt(node.getDetail())).collect(Collectors.toSet());
        dimensions = shouldDeleteNodes.stream().filter(node -> node.getType() == SchemaNodeType.MODEL_DIMENSION)
                .map(node -> Integer.parseInt(node.getDetail())).collect(Collectors.toSet());
        measures = shouldDeleteNodes.stream().filter(node -> node.getType() == SchemaNodeType.MODEL_MEASURE)
                .map(node -> Integer.parseInt(node.getDetail())).collect(Collectors.toSet());
        computedColumns = shouldDeleteNodes.stream().filter(node -> node.getType() == SchemaNodeType.MODEL_CC)
                .map(SchemaNode::getDetail).collect(Collectors.toSet());
    }

    public AffectedModelContext(String project, IndexPlan originIndexPlan, Set<SchemaNode> updatedNodes,
            Set<Pair<NDataModel.Measure, NDataModel.Measure>> updateMeasures, boolean isDelete) {
        this(project, originIndexPlan.getId(), updatedNodes, updateMeasures, isDelete);
        val indexPlanCopy = originIndexPlan.copy();
        shrinkIndexPlan(indexPlanCopy);
        val originLayoutIds = originIndexPlan.getAllLayouts().stream().map(LayoutEntity::getId)
                .collect(Collectors.toSet());
        addLayouts = indexPlanCopy.getAllLayouts().stream().map(LayoutEntity::getId)
                .filter(id -> !originLayoutIds.contains(id)).collect(Collectors.toSet());
    }

    public void shrinkIndexPlan(IndexPlan indexPlan) {
        UnaryOperator<Integer[]> dimFilter = input -> Stream.of(input).filter(i -> !dimensions.contains(i))
                .toArray(Integer[]::new);
        UnaryOperator<Integer[]> meaFilter = input -> Stream.of(input).map(i -> {
            if (updateMeasureMap.containsKey(i)) {
                i = updateMeasureMap.get(i).getId();
            }
            return i;
        }).filter(i -> !measures.contains(i)).toArray(Integer[]::new);
        indexPlan.removeLayouts(shouldDeleteLayouts, true, true);

        val overrideIndexes = Maps.newHashMap(indexPlan.getIndexPlanOverrideIndexes());
        columns.forEach(overrideIndexes::remove);
        indexPlan.setIndexPlanOverrideIndexes(overrideIndexes);

        if (indexPlan.getDictionaries() != null) {
            indexPlan.setDictionaries(indexPlan.getDictionaries().stream().filter(d -> !columns.contains(d.getId()))
                    .collect(Collectors.toList()));
        }

        if (indexPlan.getRuleBasedIndex() == null) {
            return;
        }
        val rule = JsonUtil.deepCopyQuietly(indexPlan.getRuleBasedIndex(), RuleBasedIndex.class);
        rule.setLayoutIdMapping(Lists.newArrayList());
        rule.setDimensions(
                rule.getDimensions().stream().filter(d -> !dimensions.contains(d)).collect(Collectors.toList()));
        rule.setMeasures(rule.getMeasures().stream().map(i -> {
            if (updateMeasureMap.containsKey(i)) {
                i = updateMeasureMap.get(i).getId();
            }
            return i;
        }).filter(i -> !measures.contains(i)).collect(Collectors.toList()));
        val newAggGroups = rule.getAggregationGroups().stream().peek(group -> {
            group.setIncludes(dimFilter.apply(group.getIncludes()));
            group.setMeasures(meaFilter.apply(group.getMeasures()));
            group.getSelectRule().mandatoryDims = dimFilter.apply(group.getSelectRule().mandatoryDims);
            group.getSelectRule().hierarchyDims = Stream.of(group.getSelectRule().hierarchyDims).map(dimFilter)
                    .filter(dims -> dims.length > 0).toArray(Integer[][]::new);
            group.getSelectRule().jointDims = Stream.of(group.getSelectRule().jointDims).map(dimFilter)
                    .filter(dims -> dims.length > 0).toArray(Integer[][]::new);
        }).filter(group -> ArrayUtils.isNotEmpty(group.getIncludes())).collect(Collectors.toList());

        rule.setAggregationGroups(newAggGroups);
        indexPlan.setRuleBasedIndex(rule);
        if (updatedNodes.contains(SchemaNodeType.INDEX_AGG_SHARD.withKey(indexPlan.getModelAlias()))) {
            indexPlan.setAggShardByColumns(Lists.newArrayList());
        }
        if (updatedNodes.contains(SchemaNodeType.INDEX_AGG_EXTEND_PARTITION.withKey(indexPlan.getModelAlias()))) {
            indexPlan.setExtendPartitionColumns(Lists.newArrayList());
        }

    }

    Predicate<SchemaNode> deletableIndexPredicate = node -> node.getType() == SchemaNodeType.WHITE_LIST_INDEX
            || node.getType() == SchemaNodeType.RULE_BASED_INDEX
            || node.getType() == SchemaNodeType.TO_BE_DELETED_INDEX;

    private Set<Long> filterIndexFromNodes(Set<SchemaNode> nodes) {
        return nodes.stream().filter(deletableIndexPredicate).map(node -> Long.parseLong(node.getDetail()))
                .collect(Collectors.toSet());
    }

    private Set<SchemaNode> calcShouldDeletedNodes(boolean isDelete) {
        Set<SchemaNode> result = Sets.newHashSet();

        if (isDelete) {
            return updatedNodes;
        }

        NDataModel model = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .getDataModelDesc(modelId);
        Graph<SchemaNode> schemaNodeGraph = SchemaUtil.dependencyGraph(project, model);

        updatedNodes.stream().filter(node -> node.getType() == SchemaNodeType.MODEL_MEASURE)
                .filter(node -> !updateMeasureMap.containsKey(Integer.parseInt(node.getDetail()))).forEach(node -> {
                    // add should delete measure
                    result.add(node);
                    // add should delete measure affected index schema nodes
                    Graphs.reachableNodes(schemaNodeGraph, node).stream().filter(deletableIndexPredicate)
                            .forEach(result::add);
                });

        // add updated measure affect index schema nodes
        updateIdMeasureMap.keySet().forEach(originalMeasure -> {
            Graphs.reachableNodes(schemaNodeGraph, SchemaNode.ofMeasure(originalMeasure, modelAlias)).stream()
                    .filter(deletableIndexPredicate).forEach(result::add);
        });

        return result;
    }

    public Set<String> getMeasuresKey() {
        return shouldDeleteNodes.stream()
                .filter(node -> node.getType() == SchemaNodeType.MODEL_MEASURE
                        && measures.contains(Integer.parseInt(node.getDetail())))
                .map(SchemaNode::getKey).collect(Collectors.toSet());
    }

    public Set<String> getDimensionsKey() {
        return shouldDeleteNodes.stream()
                .filter(node -> node.getType() == SchemaNodeType.MODEL_DIMENSION
                        && dimensions.contains(Integer.parseInt(node.getDetail())))
                .map(SchemaNode::getKey).collect(Collectors.toSet());
    }
}

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

package org.apache.kylin.rec;

import static java.util.stream.Collectors.groupingBy;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Ordering;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.OlapContext;
import org.apache.kylin.query.util.QueryModelPriorities;
import org.apache.kylin.rec.common.AccelerateInfo;
import org.apache.kylin.rec.model.AbstractJoinRule;
import org.apache.kylin.rec.model.GreedyModelTreesBuilder;
import org.apache.kylin.rec.model.ModelTree;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ModelSelectProposer extends AbstractProposer {

    public static final String NO_MODEL_MATCH_PENDING_MSG = "No model matches the SQL. Please add a model matches the SQL before attempting to accelerate this query.";
    public static final String CC_ACROSS_MODELS_PENDING_MSG = "No model matches the SQL. Please add a model that contains all the computed columns used in the query.";
    private final NDataModelManager dataModelManager;
    private final AbstractJoinRule joinSelectOptRule;

    public ModelSelectProposer(AbstractContext proposeContext) {
        super(proposeContext);
        dataModelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        joinSelectOptRule = AbstractJoinRule.getInstance();
    }

    @Override
    public void execute() {
        List<AbstractContext.ModelContext> modelContexts = proposeContext.getModelContexts();
        if (CollectionUtils.isEmpty(modelContexts)) {
            log.warn("Something wrong happened in the preceding step of sql analysis. "
                    + "Cannot continue auto-modeling without modelTrees.");
            return;
        }

        val allSubModelContexts = Lists.<AbstractContext.ModelContext> newArrayList();
        Map<String, AbstractContext.ModelContext> selectedModel = Maps.newHashMap();
        selectModelForModelContext(modelContexts, allSubModelContexts, selectedModel);
        if (CollectionUtils.isNotEmpty(allSubModelContexts)) {
            selectModelForModelContext(allSubModelContexts, Lists.newArrayList(), selectedModel);
        }

        proposeContext.handleExceptionAfterModelSelect();
    }

    private void selectModelForModelContext(List<AbstractContext.ModelContext> modelContexts,
            List<AbstractContext.ModelContext> allSubModelContexts,
            Map<String, AbstractContext.ModelContext> selectedModel) {
        val modelContextIterator = modelContexts.listIterator();
        Set<AbstractContext.ModelContext> mergedModelContexts = Sets.newHashSet();
        while (modelContextIterator.hasNext()) {
            val modelContext = modelContextIterator.next();
            ModelTree modelTree = modelContext.getModelTree();
            NDataModel model = selectExistedModel(modelTree, modelContext);

            if (model == null) {
                if (CollectionUtils.isEmpty(proposeContext.getOriginModels())) {
                    continue;
                }
                val subModelContexts = splitModelContext(modelContext);
                if (subModelContexts.size() > 1) {
                    modelContextIterator.remove();
                    subModelContexts.forEach(modelContextIterator::add);
                    allSubModelContexts.addAll(subModelContexts);
                }
                continue;
            }

            if (selectedModel.containsKey(model.getUuid()) && !modelContext.isSnapshotSelected()
                    && !modelContext.getModelTree().isHasModelPropertiesHint()) {
                AbstractContext.ModelContext anotherModelContext = selectedModel.get(model.getUuid());
                if (!anotherModelContext.getModelTree().isHasModelPropertiesHint()) {
                    AbstractContext.ModelContext newModelContext = new GreedyModelTreesBuilder.TreeBuilder(
                            model.getRootFactTable().getTableDesc(),
                            NTableMetadataManager.getInstance(dataModelManager.getConfig(), project).getAllTablesMap(),
                            proposeContext).mergeModelContext(proposeContext, modelContext, anotherModelContext);
                    setModelContextModel(newModelContext, model);
                    mergedModelContexts.add(modelContext);
                    mergedModelContexts.add(anotherModelContext);
                    modelContextIterator.add(newModelContext);
                    selectedModel.put(model.getUuid(), newModelContext);
                    continue;
                }
            }
            // found matched, then use it
            setModelContextModel(modelContext, model);
            if (!modelContext.isSnapshotSelected()) {
                selectedModel.put(model.getUuid(), modelContext);
            }
        }
        modelContexts.removeAll(mergedModelContexts);
    }

    private void setModelContextModel(AbstractContext.ModelContext modelContext, NDataModel model) {
        modelContext.setOriginModel(model);
        NDataModel targetModel = dataModelManager.copyBySerialization(model);
        initModel(targetModel);
        targetModel.getComputedColumnDescs().forEach(cc -> modelContext.getUsedCC().put(cc.getExpression(), cc));
        modelContext.setTargetModel(targetModel);
    }

    private List<AbstractContext.ModelContext> splitModelContext(AbstractContext.ModelContext modelContext) {
        val sqlOlapContextMap = modelContext.getModelTree().getOlapContexts().stream()
                .collect(groupingBy(OlapContext::getSql));
        if (sqlOlapContextMap.size() == 1) {
            return Lists.newArrayList(modelContext);
        }

        val subModelContexts = Lists.<AbstractContext.ModelContext> newArrayList();
        val sqlModelContextMap = Maps.<String, AbstractContext.ModelContext> newHashMap();
        val sqlSelectedModelMap = Maps.<NDataModel, List<String>> newHashMap();

        Map<String, Collection<OlapContext>> noneSelected = Maps.newHashMap();
        sqlOlapContextMap.forEach((key, value) -> {
            Map<String, Collection<OlapContext>> map = Maps.newHashMap();
            map.put(key, value);
            val sqlModelContext = buildModelContext(map).get(0);
            val selectedModel = selectExistedModel(sqlModelContext.getModelTree(), sqlModelContext);
            if (selectedModel == null) {
                noneSelected.put(key, value);
            } else {
                sqlSelectedModelMap.putIfAbsent(selectedModel, Lists.newArrayList());
                sqlSelectedModelMap.get(selectedModel).add(key);
                sqlModelContextMap.put(key, sqlModelContext);
            }
        });

        // merge non-selected model contexts
        subModelContexts.addAll(buildModelContext(noneSelected));

        // merge selected model contexts by model id
        sqlSelectedModelMap.forEach((model, sqls) -> {
            Map<String, Collection<OlapContext>> map = Maps.newHashMap();
            if (sqls.size() == 1) {
                subModelContexts.add(sqlModelContextMap.get(sqls.get(0)));
            } else {
                for (String sql : sqls) {
                    map.putIfAbsent(sql, Lists.newArrayList());
                    map.get(sql).addAll(sqlOlapContextMap.get(sql));
                }
                subModelContexts.addAll(buildModelContext(map));
            }
        });

        return subModelContexts;
    }

    private List<AbstractContext.ModelContext> buildModelContext(Map<String, Collection<OlapContext>> groupedOlapMap) {
        return new GreedyModelTreesBuilder(KylinConfig.getInstanceFromEnv(), project, proposeContext) //
                .build(groupedOlapMap, null) //
                .stream() //
                .filter(modelTree -> !modelTree.getOlapContexts().isEmpty()) //
                .map(proposeContext::createModelContext) //
                .collect(Collectors.toList());
    }

    private void initModel(NDataModel modelDesc) {
        modelDesc.init(KylinConfig.getInstanceFromEnv(), project, Lists.newArrayList());
    }

    private Comparator<NDataModel> modelSorter(ModelTree modelTree) {
        Map<String, Integer> modelPriorities = Maps.newHashMap();
        if (modelTree != null && modelTree.getOlapContexts() != null && !modelTree.getOlapContexts().isEmpty()) {
            String[] priorities = QueryModelPriorities
                    .getModelPrioritiesFromComment(modelTree.getOlapContexts().iterator().next().getSql());
            for (int i = 0; i < priorities.length; i++) {
                modelPriorities.put(priorities[i], i);
            }
        }
        Comparator<NDataModel> sqlHintSorter = Comparator.comparingInt(
                m -> modelPriorities.getOrDefault(m.getAlias().toUpperCase(Locale.ROOT), Integer.MAX_VALUE));

        Comparator<NDataModel> joinSorter = (m1, m2) -> {
            List<JoinTableDesc> joinTables2 = m2.getJoinTables() == null ? Lists.newArrayList() : m2.getJoinTables();
            List<JoinTableDesc> joinTables1 = m1.getJoinTables() == null ? Lists.newArrayList() : m1.getJoinTables();
            List<JoinTableDesc> filteredJoinTables1 = joinTables1.stream()
                    .filter(joinTable -> joinTable.getJoin().isJoinWithFactTable(m1.getRootFactTableName()))
                    .collect(Collectors.toList());
            List<JoinTableDesc> filteredJoinTables2 = joinTables2.stream()
                    .filter(joinTable -> joinTable.getJoin().isJoinWithFactTable(m2.getRootFactTableName()))
                    .collect(Collectors.toList());
            return Integer.compare(filteredJoinTables2.size(), filteredJoinTables1.size());
        };
        Comparator<NDataModel> modifiedSorter = Comparator.comparing(NDataModel::getCreateTime).reversed();
        Comparator<NDataModel> aliasSorter = Comparator.comparing(NDataModel::getAlias).reversed();
        return Ordering.from(sqlHintSorter).compound(joinSorter).compound(modifiedSorter).compound(aliasSorter);
    }

    private NDataModel selectExistedModel(ModelTree modelTree, AbstractContext.ModelContext modelContext) {
        List<NDataModel> originModels = proposeContext.getOriginModels();
        originModels.sort(modelSorter(modelTree));
        for (NDataModel model : originModels) {
            List<OlapContext> retainedOlapContexts = retainCapableOlapContexts(model,
                    Lists.newArrayList(modelTree.getOlapContexts()));
            if (retainedOlapContexts.isEmpty()) {
                continue;
            }

            boolean match = proposeContext instanceof SmartContext //
                    ? modelTree.hasSameSubGraph(model)
                    : modelTree.isExactlyMatch(model, proposeContext.isPartialMatch(),
                            proposeContext.isPartialMatchNonEqui());

            if (match) {
                List<OlapContext> disabledList = modelTree.getOlapContexts().stream()
                        .filter(context -> !retainedOlapContexts.contains(context)).collect(Collectors.toList());
                disabledList.forEach(context -> {
                    AccelerateInfo accelerateInfo = new AccelerateInfo();
                    accelerateInfo.setPendingMsg(CC_ACROSS_MODELS_PENDING_MSG);
                    proposeContext.getAccelerateInfoMap().put(context.getSql(), accelerateInfo);
                });
                modelTree.getOlapContexts().clear();
                modelTree.getOlapContexts().addAll(retainedOlapContexts);
                modelContext.setSnapshotSelected(false);
                return model;
            } else if (proposeContext.isCanCreateNewModel() && joinSelectOptRule.isCompatible(model, modelTree)) {
                return model;
            }

            val project = modelContext.getProposeContext().getProject();
            val config = modelContext.getProposeContext().getSmartConfig().getKylinConfig();
            if (!(proposeContext instanceof SmartContext) && matchSnapshot(config, project, modelTree)) {
                modelContext.setSnapshotSelected(true);
                return model;
            }
        }
        return null;
    }

    private List<OlapContext> retainCapableOlapContexts(NDataModel model, List<OlapContext> olapContexts) {
        Set<ColumnDesc> ccColDesc = filterTblColRefOfCC(model.getEffectiveCols().values());
        Iterator<OlapContext> iterator = olapContexts.iterator();
        while (iterator.hasNext()) {
            OlapContext context = iterator.next();
            Set<ColumnDesc> ccColDescInCtx = filterTblColRefOfCC(context.getAllColumns());
            if (!ccColDesc.containsAll(ccColDescInCtx)) {
                iterator.remove();
            }
        }
        return olapContexts;
    }

    private Set<ColumnDesc> filterTblColRefOfCC(Set<TblColRef> tableColRefSet) {
        if (CollectionUtils.isEmpty(tableColRefSet)) {
            return Sets.newHashSet();
        }
        return tableColRefSet.stream() //
                .map(TblColRef::getColumnDesc) //
                .filter(ColumnDesc::isComputedColumn) //
                .collect(Collectors.toSet());
    }

    public static boolean matchSnapshot(KylinConfig config, String project, ModelTree modelTree) {
        if (!modelTree.getJoins().isEmpty() || modelTree.getRootFactTable().isIncrementLoading())
            return false;

        val modelTreeRootTable = modelTree.getRootFactTable().getIdentity();

        NTableMetadataManager tableManager = NTableMetadataManager.getInstance(config, project);
        return StringUtils.isNotEmpty(tableManager.getTableDesc(modelTreeRootTable).getLastSnapshotPath());
    }

    @Override
    public String getIdentifierName() {
        return "ModelSelectProposer";
    }
}

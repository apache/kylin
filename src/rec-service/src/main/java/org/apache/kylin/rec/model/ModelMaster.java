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

package org.apache.kylin.rec.model;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.HashBiMap;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.model.AntiFlatChecker;
import org.apache.kylin.metadata.model.ColExcludedChecker;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.graph.JoinsGraph;
import org.apache.kylin.query.util.ComputedColumnRewriter;
import org.apache.kylin.query.util.QueryAliasMatchInfo;
import org.apache.kylin.rec.AbstractContext;
import org.apache.kylin.rec.ModelOptProposer;
import org.apache.kylin.rec.ModelReuseContext;
import org.apache.kylin.rec.SmartContext;
import org.apache.kylin.rec.query.AbstractQueryRunner;
import org.apache.kylin.rec.query.QueryRunnerBuilder;
import org.apache.kylin.rec.util.CubeUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ModelMaster {

    private final AbstractContext.ModelContext modelContext;
    private final ProposerProvider proposerProvider;
    private final String project;

    public ModelMaster(AbstractContext.ModelContext modelContext) {
        this.modelContext = modelContext;
        this.proposerProvider = ProposerProvider.create(modelContext);
        this.project = modelContext.getProposeContext().getProject();
    }

    NDataModel proposeInitialModel() {
        NDataModel dataModel = new NDataModel();
        dataModel.setRootFactTableName(modelContext.getModelTree().getRootFactTable().getIdentity());
        dataModel.setDescription(StringUtils.EMPTY);
        dataModel.setFilterCondition(StringUtils.EMPTY);
        dataModel.setComputedColumnDescs(Lists.newArrayList());
        dataModel.setProject(project);

        FunctionDesc countStar = CubeUtils.newCountStarFuncDesc(dataModel);
        NDataModel.Measure countStarMeasure = CubeUtils.newMeasure(countStar, "COUNT_ALL", NDataModel.MEASURE_ID_BASE);
        dataModel.setAllMeasures(Lists.newArrayList(countStarMeasure));
        log.info("Initialized a new model({}) for no compatible one to use.", dataModel.getId());
        return dataModel;
    }

    public NDataModel proposeJoins(NDataModel dataModel) {
        if (modelContext.getProposeContext() instanceof ModelReuseContext) {
            ModelReuseContext context = (ModelReuseContext) modelContext.getProposeContext();
            if (!context.isCanCreateNewModel()) {
                Preconditions.checkState(dataModel != null, ModelOptProposer.NO_COMPATIBLE_MODEL_MSG);
                return dataModel;
            }
        }

        log.info("Start proposing join relations.");
        if (dataModel == null) {
            dataModel = proposeInitialModel();
        }
        dataModel = proposerProvider.getJoinProposer().propose(dataModel);
        log.info("Proposing join relations completed successfully.");
        return dataModel;
    }

    public NDataModel proposeScope(NDataModel dataModel) {
        log.info("Start proposing dimensions and measures.");
        dataModel = proposerProvider.getScopeProposer().propose(dataModel);
        log.info("Proposing dimensions and measures completed successfully.");
        return dataModel;
    }

    public NDataModel proposeComputedColumn(NDataModel dataModel) {
        log.info("Start proposing computed columns.");
        initAntiFlatChecker(modelContext, dataModel);
        initExcludedChecker(modelContext);
        boolean turnOnCC = modelContext.getProposeContext().getKapConfig().getKylinConfig()
                .isConvertExpressionToCcEnabled();
        if (!turnOnCC) {
            log.warn("The feature of proposing computed column in Kyligence Enterprise has been turned off.");
            return dataModel;
        }

        List<ComputedColumnDesc> originalCCs = Lists.newArrayList(dataModel.getComputedColumnDescs());
        try {
            dataModel = proposerProvider.getComputedColumnProposer().propose(dataModel);
            if (modelContext.isNeedUpdateCC()) {
                // New CC detected, need to rebuild ModelContext regarding new coming CC
                log.info("Start using proposed computed columns to update the model({})", dataModel.getId());
                updateContextWithCC(dataModel);
            }
            log.info("Proposing computed column completed successfully.");
        } catch (Exception e) {
            log.error("Propose failed, will discard new computed columns.", e);
            dataModel.setComputedColumnDescs(originalCCs);
        }
        return dataModel;
    }

    private void initAntiFlatChecker(AbstractContext.ModelContext modelContext, NDataModel dataModel) {
        if (modelContext.getAntiFlatChecker() == null) {
            AntiFlatChecker checker = new AntiFlatChecker(dataModel.getJoinTables(), dataModel);
            modelContext.setAntiFlatChecker(checker);
        }
    }

    private void initExcludedChecker(AbstractContext.ModelContext modelContext) {
        if (modelContext.getExcludedChecker() == null) {
            KylinConfig kylinConfig = modelContext.getProposeContext().getSmartConfig().getKylinConfig();
            ColExcludedChecker excludedChecker = new ColExcludedChecker(kylinConfig, project,
                    modelContext.getTargetModel());
            modelContext.setExcludedChecker(excludedChecker);
        }
    }

    public NDataModel shrinkComputedColumn(NDataModel dataModel) {
        return proposerProvider.getShrinkComputedColumnProposer().propose(dataModel);
    }

    private void updateContextWithCC(NDataModel dataModel) {
        List<String> originQueryList = Lists.newArrayList();
        modelContext.getModelTree().getOlapContexts().stream() //
                .filter(context -> !StringUtils.isEmpty(context.getSql())) //
                .forEach(context -> originQueryList.add(context.getSql()));
        if (originQueryList.isEmpty()) {
            log.warn("Failed to replace cc expression in original sql with proposed computed columns, "
                    + "early termination of the method of updateContextWithCC");
            return;
        }

        // Rebuild modelTrees and find match one to replace original
        KylinConfig kylinConfig = modelContext.getProposeContext().getSmartConfig().getKylinConfig();
        try (AbstractQueryRunner extractor = new QueryRunnerBuilder(project, kylinConfig,
                originQueryList.toArray(new String[0])).of(Lists.newArrayList(dataModel)).build()) {
            log.info("Start to rebuild modelTrees after replace cc expression with cc name.");
            extractor.execute();
            final AbstractContext proposeContext = modelContext.getProposeContext();
            List<ModelTree> modelTrees = new GreedyModelTreesBuilder(kylinConfig, project, proposeContext) //
                    .build(extractor.filterNonModelViewOlapContexts(), null);
            ModelTree updatedModelTree = null;
            for (ModelTree modelTree : modelTrees) {
                boolean match = proposeContext instanceof SmartContext //
                        ? modelTree.hasSameSubGraph(dataModel)
                        : modelTree.isExactlyMatch(dataModel, proposeContext.isPartialMatch(),
                                proposeContext.isPartialMatchNonEqui());
                if (match) {
                    updatedModelTree = modelTree;
                    break;
                }
            }
            if (updatedModelTree == null) {
                return;
            }

            // Update context info
            this.modelContext.setModelTree(updatedModelTree);
            updateOlapCtxWithCC(updatedModelTree, dataModel);
            log.info("Rebuild modelTree successfully.");
        } catch (InterruptedException e) {
            log.warn("Interrupted!!", e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.warn("NModelMaster.updateContextWithCC failed to update model tree", e);
        }
    }

    private void updateOlapCtxWithCC(ModelTree modelTree, NDataModel model) {
        boolean matchPartial = this.modelContext.getProposeContext().isPartialMatch();
        modelTree.getOlapContexts().forEach(context -> {
            JoinsGraph joinsGraph = context.getJoinsGraph() == null
                    ? new JoinsGraph(context.getFirstTableScan().getTableRef(), context.getJoins())
                    : context.getJoinsGraph();
            Map<String, String> matches = joinsGraph.matchAlias(model.getJoinsGraph(), matchPartial);
            if (matches == null || matches.isEmpty()) {
                return;
            }
            Set<String> cols = modelContext.getExcludedChecker().filterRelatedExcludedColumn(model);
            QueryAliasMatchInfo matchInfo = new QueryAliasMatchInfo(HashBiMap.create(matches), null);
            matchInfo.getExcludedColumns().addAll(cols);
            ComputedColumnRewriter.rewriteCcInnerCol(context, model, matchInfo);
        });
    }
}

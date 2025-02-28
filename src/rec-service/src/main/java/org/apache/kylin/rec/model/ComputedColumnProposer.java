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

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.spark.utils.ComputedColumnEvalUtil;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.metadata.model.util.ComputedColumnUtil;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.recommendation.candidate.RawRecItem;
import org.apache.kylin.metadata.recommendation.entity.CCRecItemV2;
import org.apache.kylin.metadata.recommendation.util.RawRecUtil;
import org.apache.kylin.query.relnode.OlapContext;
import org.apache.kylin.query.relnode.TableColRefWithRel;
import org.apache.kylin.query.util.PushDownUtil;
import org.apache.kylin.rec.AbstractContext;
import org.apache.kylin.rec.AbstractContext.ModelContext;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ComputedColumnProposer extends AbstractModelProposer {

    ComputedColumnProposer(AbstractContext.ModelContext modelContext) {
        super(modelContext);
    }

    @Override
    protected void execute(NDataModel dataModel) {
        log.trace("Propose computed column for model({})", dataModel.getId());
        long startTime = System.currentTimeMillis();

        // pre-init to construct join-tree
        initModel(dataModel);
        Set<String> ccSuggestions = collectLatentCCSuggestions(modelContext, dataModel);
        transferStatusOfNeedUpdateCC(ccSuggestions);
        List<ComputedColumnDesc> newValidCCList = transferToComputedColumn(dataModel, ccSuggestions);
        evaluateTypeOfComputedColumns(dataModel, newValidCCList);
        cleanInvalidComputedColumnsInModel(dataModel);
        collectCCRecommendations(newValidCCList);

        log.info("Propose ComputedColumns successfully completed in {} s. Valid ComputedColumns on model({}) are: {}.",
                (System.currentTimeMillis() - startTime) / 1000, dataModel.getId(), dataModel.getComputedColumnNames());
    }

    private Set<String> collectLatentCCSuggestions(ModelContext modelContext, NDataModel dataModel) {
        Set<String> ccSuggestions = Sets.newHashSet();
        ModelTree modelTree = modelContext.getModelTree();
        KylinConfig kylinConfig = getModelContext().getProposeContext().getKapConfig().getKylinConfig();
        boolean partialMatch = kylinConfig.isQueryMatchPartialInnerJoinModel();
        boolean nonEquiPartialMatch = kylinConfig.partialMatchNonEquiJoins();
        for (OlapContext ctx : modelTree.getOlapContexts()) {
            // fix models to update alias
            Map<String, String> matchingAlias = ctx.matchJoins(dataModel, partialMatch, nonEquiPartialMatch);
            ctx.fixModel(dataModel, matchingAlias);
            Set<TblColRef> innerColumns = collectInnerColumns(ctx);
            innerColumns.removeIf(col -> !col.isInnerColumn());
            innerColumns.removeIf(col -> modelContext.getExcludedChecker().isExcludedCol(col));
            innerColumns.removeIf(col -> modelContext.getAntiFlatChecker().isCCOfAntiLookup(col));
            ccSuggestions.addAll(translateToSuggestions(innerColumns, matchingAlias));
            ctx.unfixModel();
        }

        log.info("Proposed computed column candidates {} for model [{}] successfully", ccSuggestions,
                dataModel.getId());
        return ccSuggestions;
    }

    private void transferStatusOfNeedUpdateCC(Set<String> latentCCSuggestions) {
        if (!latentCCSuggestions.isEmpty()) {
            modelContext.setNeedUpdateCC(true);
        }
    }

    private List<ComputedColumnDesc> transferToComputedColumn(NDataModel dataModel, Set<String> ccSuggestions) {
        List<ComputedColumnDesc> validCCs = Lists.newArrayList();
        if (ccSuggestions.isEmpty()) {
            return validCCs;
        }

        List<NDataModel> otherModels = getOtherModels(dataModel);
        KylinConfig projectConfig = NProjectManager.getProjectConfig(project);
        boolean onlyReuseUserDefinedCC = projectConfig.onlyReuseUserDefinedCC();
        for (String ccSuggestion : ccSuggestions) {
            ComputedColumnDesc ccDesc = modelContext.getUsedCC().get(ccSuggestion);
            // In general, cc expressions in the SQL statements should have been replaced in transformers,
            // however, it could not be replaced when meets some corner cases(#11411). As a result, it will
            // lead to add the same CC more than once and fail to accelerate current sql statements.
            if (ccDesc != null || onlyReuseUserDefinedCC) {
                continue;
            }
            ccDesc = new ComputedColumnDesc();
            ccDesc.setTableIdentity(dataModel.getRootFactTable().getTableIdentity());
            ccDesc.setTableAlias(dataModel.getRootFactTableAlias());
            ccDesc.setComment("Auto suggested from: " + ccSuggestion);
            ccDesc.setDatatype("ANY"); // resolve data type later
            ccDesc.setExpression(ccSuggestion);

            String innerExpression = PushDownUtil.massageComputedColumn(dataModel, project, ccDesc, null);
            ccDesc.setInnerExpression(innerExpression);

            String content = RawRecUtil.getContent(dataModel.getProject(), dataModel.getUuid(),
                    ccDesc.getUniqueContent(), RawRecItem.RawRecType.COMPUTED_COLUMN);
            String ccMd5 = RawRecUtil.computeMD5(content);

            String contentWithoutModelID = RawRecUtil.getContent(dataModel.getProject(), null,
                    ccDesc.getUniqueContent(), RawRecItem.RawRecType.COMPUTED_COLUMN);
            String ccMd5WithoutModelID = RawRecUtil.computeMD5(contentWithoutModelID);

            String newCCName = ComputedColumnUtil.shareCCNameAcrossModel(ccDesc, dataModel, otherModels);
            if (newCCName != null) {
                ccDesc.setColumnName(newCCName);
            } else {
                String name = ComputedColumnUtil.uniqueCCName(ccMd5WithoutModelID);
                ccDesc.setColumnName(name);
            }

            ccDesc.setUuid(ccMd5);
            dataModel.getComputedColumnDescs().add(ccDesc);

            if (ComputedColumnEvalUtil.resolveCCName(ccDesc, dataModel, otherModels)) {
                validCCs.add(ccDesc);
                modelContext.getUsedCC().put(ccDesc.getExpression(), ccDesc);
            } else {
                // For example: If a malformed computed column expression of
                // `CASE(IN($3, 'Auction', 'FP-GTC'), 'Auction', $3)`
                // has been added to the model, we should discard it, otherwise, all suggestions after this round
                // cannot resolve ccName for model initialization failed.
                dataModel.getComputedColumnDescs().remove(ccDesc);
                log.debug("Malformed computed column {} has been removed from the model {}.", //
                        ccDesc, dataModel.getUuid());
            }
        }
        return validCCs;
    }

    /**
     * There are three kind of CC need remove:
     * 1. invalid CC: something wrong happened in resolving name
     * 2. unsupported CC: something wrong happened in inferring type
     * 3. the type of CC is ANY: something unlikely thing happened in inferring type
     */
    private void cleanInvalidComputedColumnsInModel(NDataModel dataModel) {
        dataModel.getComputedColumnDescs().removeIf(cc -> cc.getDatatype().equals("ANY"));
    }

    private Set<TblColRef> collectInnerColumns(OlapContext context) {
        Set<TblColRef> usedCols = Sets.newHashSet();
        usedCols.addAll(context.getAllColumns());

        context.getAggregations().stream() // collect inner columns from agg metrics
                .filter(agg -> CollectionUtils.isNotEmpty(agg.getParameters()))
                .forEach(agg -> usedCols.addAll(agg.getColRefs()));
        // collect inner columns from group keys
        usedCols.addAll(getGroupByInnerColumns(context));
        // collect inner columns from filter keys
        if (modelContext.getProposeContext().getSmartConfig().enableComputedColumnOnFilterKeySuggestion()) {
            usedCols.addAll(getFilterInnerColumns(context));
        }
        return usedCols;
    }

    private void collectCCRecommendations(List<ComputedColumnDesc> computedColumns) {
        if (modelContext.getProposeContext().skipCollectRecommendations() || CollectionUtils.isEmpty(computedColumns)) {
            return;
        }

        computedColumns.forEach(cc -> {
            CCRecItemV2 item = new CCRecItemV2();
            item.setCc(cc);
            item.setCreateTime(System.currentTimeMillis());
            item.setUuid(cc.getUuid());
            item.setUniqueContent(cc.getUniqueContent());
            modelContext.getCcRecItemMap().putIfAbsent(item.getUuid(), item);
        });
    }

    protected Set<String> translateToSuggestions(Set<TblColRef> innerColumns, Map<String, String> matchingAlias) {
        Set<String> candidates = Sets.newHashSet();
        for (TblColRef col : innerColumns) {
            String parserDesc = col.getParserDescription();
            if (parserDesc == null) {
                continue;
            }
            parserDesc = matchingAlias.entrySet().stream()
                    .map(entry -> (Function<String, String>) s -> s.replaceAll(entry.getKey(), entry.getValue()))
                    .reduce(Function.identity(), Function::andThen).apply(parserDesc);
            log.trace(parserDesc);
            candidates.add(parserDesc);
        }
        return candidates;
    }

    private Collection<TblColRef> getFilterInnerColumns(OlapContext context) {
        Collection<TblColRef> resultSet = new HashSet<>();
        for (TableColRefWithRel innerColRefWithRel : context.getInnerFilterColumns()) {
            TblColRef innerColRef = innerColRefWithRel.getTblColRef();
            Set<TblColRef> filterSourceColumns = innerColRef.getSourceColumns();
            if (!innerColRef.getSourceColumns().isEmpty()
                    && checkColumnsMinCardinality(filterSourceColumns, modelContext.getProposeContext().getSmartConfig()
                            .getComputedColumnOnFilterKeySuggestionMinCardinality())) {

                // if the inner filter column contains columns from group keys
                // and the inner filter column also appears in the select clause,
                // the the CC replacement will produce a wrong aggregation form.
                // eg.
                // BEFORE: select expr(a) from tbl where expr(a) > 100 group by a
                // AFTER: select CC_1 from tbl where CC_1 > 100 group by a
                // Thus we add a simple check here to ensure that inner filter column
                // does not contain columns from group by clause
                // see: https://github.com/Kyligence/KAP/issues/14072
                // remove this once issue #14072 is fixed
                filterSourceColumns.retainAll(context.getGroupByColumns());
                if (filterSourceColumns.isEmpty()) {
                    resultSet.add(innerColRef);
                }
            }
        }
        return resultSet;
    }

    private Collection<TblColRef> getGroupByInnerColumns(OlapContext context) {
        Collection<TblColRef> resultSet = new HashSet<>();
        for (TableColRefWithRel groupByColRefWithRel : context.getInnerGroupByColumns()) {
            TblColRef groupByColRef = groupByColRefWithRel.getTblColRef();
            Set<TblColRef> groupSourceColumns = groupByColRef.getSourceColumns();
            if (!groupByColRef.getSourceColumns().isEmpty()
                    && checkColumnsMinCardinality(groupSourceColumns, modelContext.getProposeContext().getSmartConfig()
                            .getComputedColumnOnGroupKeySuggestionMinCardinality())) {
                resultSet.add(groupByColRef);
            }
        }
        return resultSet;
    }

    /**
     * check and ensure that the cardinality of input cols is greater than or equal to the minCardinality
     * If the cardinality of a column is missing, return config "computed-column.suggestion.enabled-if-no-sampling"
     * @param colRefs TblColRef set
     * @param minCardinality minCardinality
     */
    private boolean checkColumnsMinCardinality(Collection<TblColRef> colRefs, long minCardinality) {
        for (TblColRef colRef : colRefs) {
            long colCardinality = getColumnCardinality(colRef);
            if (colCardinality == -1) {
                return this.getModelContext().getProposeContext().getSmartConfig().needProposeCcIfNoSampling();
            }
            if (colCardinality >= minCardinality) {
                return true;
            }
            minCardinality = minCardinality / colCardinality;
        }
        return false;
    }

    private long getColumnCardinality(TblColRef colRef) {
        NTableMetadataManager nTableMetadataManager = NTableMetadataManager
                .getInstance(KylinConfig.getInstanceFromEnv(), project);
        TableExtDesc.ColumnStats columnStats = TableExtDesc.ColumnStats.getColumnStats(nTableMetadataManager, colRef);
        return columnStats == null ? -1 : columnStats.getCardinality();
    }

    private void evaluateTypeOfComputedColumns(NDataModel dataModel, List<ComputedColumnDesc> validCCs) {
        if (modelContext.getProposeContext().isSkipEvaluateCC() || validCCs.isEmpty()) {
            return;
        }
        ComputedColumnEvalUtil.evaluateExprAndTypeBatch(dataModel, validCCs);
    }

    private List<NDataModel> getOtherModels(NDataModel dataModel) {
        List<NDataModel> otherModels = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project) //
                .listAllModels().stream() //
                .filter(m -> !m.getUuid().equals(dataModel.getUuid())) //
                .collect(Collectors.toList());
        otherModels.addAll(//
                getModelContext().getProposeContext() //
                        .getModelContexts().stream() //
                        .filter(modelContext -> modelContext != getModelContext()) //
                        .map(ModelContext::getTargetModel) //
                        .filter(Objects::nonNull) //
                        .collect(Collectors.toList()) //
        );
        return otherModels;
    }

    public static class ComputedColumnProposerOfModelReuseContext extends ComputedColumnProposer {

        ComputedColumnProposerOfModelReuseContext(ModelContext modelContext) {
            super(modelContext);
        }

        @Override
        protected Set<String> translateToSuggestions(Set<TblColRef> usedCols, Map<String, String> matchingAlias) {
            Set<String> candidates = super.translateToSuggestions(usedCols, matchingAlias);
            for (TblColRef col : usedCols) {
                // if create totally new model, it won't reuse CC. So it need to be create its own new ComputedCols.
                if (col.getColumnDesc().isComputedColumn()) {
                    try {
                        candidates.add(CalciteParser.transformDoubleQuote(col.getExpressionInSourceDB()));
                    } catch (Exception e) {
                        log.warn("fail to acquire formatted cc from expr {}", col.getExpressionInSourceDB());
                    }
                }
            }
            return candidates;
        }
    }
}

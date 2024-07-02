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

import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModel.ColumnStatus;
import org.apache.kylin.metadata.model.NDataModel.Measure;
import org.apache.kylin.metadata.model.NDataModel.NamedColumn;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.recommendation.entity.DimensionRecItemV2;
import org.apache.kylin.metadata.recommendation.entity.MeasureRecItemV2;
import org.apache.kylin.metadata.recommendation.util.RawRecUtil;
import org.apache.kylin.query.relnode.OlapContext;
import org.apache.kylin.rec.AbstractContext;
import org.apache.kylin.rec.common.AccelerateInfo;
import org.apache.kylin.rec.exception.PendingException;
import org.apache.kylin.rec.index.IndexSuggester;
import org.apache.kylin.rec.util.CubeUtils;

import lombok.extern.slf4j.Slf4j;

/**
 * Define Dimensions and Measures from SQLs
 */
@Slf4j
public class QueryScopeProposer extends AbstractModelProposer {

    QueryScopeProposer(AbstractContext.ModelContext modelContext) {
        super(modelContext);
    }

    @Override
    protected void execute(NDataModel dataModel) {
        log.trace("Propose scope for model [{}]", dataModel.getId());
        KylinConfig kylinConfig = getModelContext().getProposeContext().getKapConfig().getKylinConfig();
        boolean partialMatch = kylinConfig.isQueryMatchPartialInnerJoinModel();
        boolean nonEquiPartialMatch = kylinConfig.partialMatchNonEquiJoins();

        // Load from context
        ScopeBuilder scopeBuilder = new ScopeBuilder(dataModel, getModelContext());
        ModelTree modelTree = modelContext.getModelTree();
        for (OlapContext ctx : modelTree.getOlapContexts()) {
            if (!isValidOlapContext(ctx)) {
                continue;
            }

            // When injecting the columns and measures of OlapContext into the scopeBuilder fails,
            // discard this OlapContext and set blocking cause to the related SQL.
            try {
                Map<String, String> aliasMap = ctx.matchJoins(dataModel, partialMatch, nonEquiPartialMatch);
                ctx.fixModel(dataModel, aliasMap);
                scopeBuilder.injectAllTableColumns(ctx);
                scopeBuilder.injectCandidateMeasure(ctx);
                scopeBuilder.injectCandidateColumns(ctx);
            } catch (Exception e) {
                scopeBuilder.inheritAllNameColumn();
                Map<String, AccelerateInfo> accelerateInfoMap = modelContext.getProposeContext().getAccelerateInfoMap();
                AccelerateInfo accelerateInfo = accelerateInfoMap.get(ctx.getSql());
                Preconditions.checkNotNull(accelerateInfo);
                if (e instanceof PendingException) {
                    accelerateInfo.setPendingMsg(e.getMessage());
                } else {
                    accelerateInfo.setFailedCause(e);
                }
            } finally {
                ctx.unfixModel();
            }
        }

        scopeBuilder.build();
    }

    protected static class ScopeBuilder {

        private final Map<String, ComputedColumnDesc> ccMap;
        private final NDataModel dataModel;
        private final AbstractContext.ModelContext modelContext;
        // column_identity <====> NamedColumn
        Map<String, NDataModel.NamedColumn> candidateNamedColumns = Maps.newLinkedHashMap();
        Map<FunctionDesc, NDataModel.Measure> candidateMeasures = Maps.newLinkedHashMap();
        Set<TblColRef> dimensionAsMeasureColumns = Sets.newHashSet();
        Set<TblColRef> allTableColumns = Sets.newHashSet();
        JoinTableDesc[] joins = new JoinTableDesc[0];

        private Set<String> newCcUuids;
        private int maxColId = -1;
        private int maxMeasureId = NDataModel.MEASURE_ID_BASE - 1;

        protected ScopeBuilder(NDataModel dataModel, AbstractContext.ModelContext modelContext) {
            this.dataModel = dataModel;
            this.modelContext = modelContext;

            // Inherit from old model
            inheritCandidateNamedColumns(dataModel);
            inheritCandidateMeasures(dataModel);
            inheritJoinTables(dataModel);

            /* ccMap used for recording all computed columns for generate uniqueFlag of RecItemV2 */
            ccMap = dataModel.getCcMap();
            newCcUuids = modelContext.getCcRecItemMap().values().stream().map(item -> item.getCc().getUuid())
                    .collect(Collectors.toSet());
        }

        private void inheritCandidateNamedColumns(NDataModel dataModel) {
            List<NamedColumn> allNamedColumns = dataModel.getAllNamedColumns();
            for (NamedColumn column : allNamedColumns) {
                maxColId = Math.max(maxColId, column.getId());
                // Forward compatibility, ensure col name is unique
                if (!column.isExist()) {
                    continue;
                }
                column.setName(column.getName());
                candidateNamedColumns.put(column.getAliasDotColumn(), column);
            }
        }

        private void inheritCandidateMeasures(NDataModel dataModel) {
            List<Measure> measures = dataModel.getAllMeasures();
            for (NDataModel.Measure measure : measures) {
                maxMeasureId = Math.max(maxMeasureId, measure.getId());
                if (measure.isTomb()) {
                    continue;
                }
                candidateMeasures.put(measure.getFunction(), measure);
            }
        }

        private void inheritJoinTables(NDataModel dataModel) {
            this.joins = dataModel.getJoinTables().toArray(new JoinTableDesc[0]);
        }

        private void injectAllTableColumns(OlapContext ctx) {
            ctx.getAllTableScans().forEach(tableScan -> allTableColumns.addAll(tableScan.getTableRef().getColumns()));
        }

        private void injectCandidateColumns(OlapContext olapContext) {

            // add all table columns of the ctx to allColumns,
            // use TreeSet can get a steady test result in different circumstances
            Set<TblColRef> allColumns = new TreeSet<>(Comparator.comparing(TblColRef::getIdentity));
            allColumns.addAll(allTableColumns);

            // set status for all columns and put them into candidate named columns
            Map<String, TblColRef> fKAsDimensionMap = modelContext.collectFkDimensionMap(olapContext);
            allColumns.forEach(tblColRef -> {
                ColumnStatus status;
                boolean canTreatAsDim = canTblColRefTreatAsDimension(fKAsDimensionMap, tblColRef)
                        || canTblColRefTreatAsDimension(olapContext, tblColRef);
                boolean isNewDimension = canTreatAsDim;
                if (candidateNamedColumns.containsKey(tblColRef.getIdentity())) {
                    NamedColumn namedColumn = candidateNamedColumns.get(tblColRef.getIdentity());
                    boolean existingDimension = namedColumn.isDimension();
                    status = existingDimension || canTreatAsDim ? ColumnStatus.DIMENSION : ColumnStatus.EXIST;
                    isNewDimension = !existingDimension && canTreatAsDim;
                    namedColumn.setStatus(status);
                } else {
                    status = canTreatAsDim ? ColumnStatus.DIMENSION : ColumnStatus.EXIST;
                    final NamedColumn column = transferToNamedColumn(tblColRef, status);
                    candidateNamedColumns.put(tblColRef.getIdentity(), column);
                }

                if (isNewDimension) {
                    addDimRecommendation(candidateNamedColumns.get(tblColRef.getIdentity()), tblColRef);
                }
            });
        }

        private void inheritAllNameColumn() {
            Set<TblColRef> allColumns = Sets.newTreeSet(Comparator.comparing(TblColRef::getIdentity));
            allColumns.addAll(allTableColumns);
            allColumns.forEach(tblColRef -> {
                if (!candidateNamedColumns.containsKey(tblColRef.getIdentity())) {
                    final NamedColumn column = transferToNamedColumn(tblColRef, ColumnStatus.EXIST);
                    candidateNamedColumns.put(tblColRef.getIdentity(), column);
                }
            });
        }

        private void addDimRecommendation(NamedColumn column, TblColRef tblColRef) {
            if (modelContext.getProposeContext().skipCollectRecommendations()) {
                return;
            }

            String uniqueContent = RawRecUtil.dimensionUniqueContent(tblColRef, ccMap, newCcUuids);
            if (modelContext.getUniqueContentToFlag().containsKey(uniqueContent)) {
                return;
            }

            DimensionRecItemV2 item = new DimensionRecItemV2(column, tblColRef, uniqueContent);
            modelContext.getDimensionRecItemMap().putIfAbsent(item.getUuid(), item);
        }

        private void injectCandidateMeasure(OlapContext ctx) {
            preCheckBeforeInjectMeasures(ctx);
            ctx.getAggregations().forEach(agg -> {
                log.debug("aggregation from OlapContext is: {}", agg);
                Set<String> paramNames = Sets.newLinkedHashSet();
                agg.getParameters().forEach(parameterDesc -> {
                    String identity = parameterDesc.getColRef().getIdentity();
                    paramNames.add(identity.replace(".", "_"));
                });
                boolean isNewMeasure = false;
                if (!candidateMeasures.containsKey(agg)) {
                    boolean isValidMeasure = CubeUtils.isValidMeasure(agg);
                    if (isValidMeasure) {
                        FunctionDesc fun = copyFunctionDesc(agg);
                        String name = String.format(Locale.ROOT, "%s_%s", fun.getExpression(),
                                String.join("_", paramNames));
                        NDataModel.Measure measure = CubeUtils.newMeasure(fun, name, ++maxMeasureId);
                        candidateMeasures.put(fun, measure);
                        isNewMeasure = true;
                    } else if (agg.canAnsweredByDimensionAsMeasure()) {
                        dimensionAsMeasureColumns.addAll(agg.getSourceColRefs());
                    } else if (paramNames.stream().anyMatch(param -> param.startsWith(TblColRef.UNKNOWN_ALIAS))) {
                        throw new PendingException(IndexSuggester.OTHER_UNSUPPORTED_MEASURE + agg);
                    }
                } else if (candidateMeasures.get(agg).isTomb()) {
                    String name = String.format(Locale.ROOT, "%s_%s", agg.getExpression(),
                            String.join("_", paramNames));
                    Measure measure = CubeUtils.newMeasure(agg, name, ++maxMeasureId);
                    candidateMeasures.put(agg, measure);
                    isNewMeasure = true;
                }

                if (isNewMeasure) {
                    Measure measure = candidateMeasures.get(agg);
                    addMeasureRecommendation(measure);
                }
            });
        }

        private void preCheckBeforeInjectMeasures(OlapContext ctx) {
            for (FunctionDesc agg : ctx.getAggregations()) {
                if (agg.canAnsweredByDimensionAsMeasure()) {
                    continue;
                }
                if (modelContext.getAntiFlatChecker().isMeasureOfAntiLookup(agg)) {
                    throw new PendingException(IndexSuggester.MEASURE_ON_ANTI_FLATTEN_LOOKUP + agg);
                } else if (modelContext.getExcludedChecker().isExcludedMeasure(agg)) {
                    throw new PendingException(IndexSuggester.MEASURE_OF_EXCLUDED_COLUMNS + agg);
                }
            }
        }

        private void addMeasureRecommendation(Measure measure) {
            if (modelContext.getProposeContext().skipCollectRecommendations()) {
                return;
            }

            String uniqueContent = RawRecUtil.measureUniqueContent(measure, ccMap, newCcUuids);
            if (modelContext.getUniqueContentToFlag().containsKey(uniqueContent)) {
                return;
            }

            MeasureRecItemV2 item = new MeasureRecItemV2();
            item.setMeasure(measure);
            item.setCreateTime(System.currentTimeMillis());
            item.setUniqueContent(uniqueContent);
            item.setUuid(String.format(Locale.ROOT, "measure_%s", RandomUtil.randomUUIDStr()));
            modelContext.getMeasureRecItemMap().putIfAbsent(item.getUuid(), item);
        }

        private void build() {

            // 1. publish all measures
            List<Measure> measures = Lists.newArrayList(candidateMeasures.values());
            NDataModel.checkDuplicateMeasure(measures);
            dataModel.setAllMeasures(measures);

            // 2. publish all named columns
            List<NamedColumn> namedColumns = Lists.newArrayList(candidateNamedColumns.values());
            NDataModel.changeNameIfDup(namedColumns);

            NDataModel.checkDuplicateColumn(namedColumns);
            dataModel.setAllNamedColumns(namedColumns);
        }

        private ParameterDesc copyParameterDesc(ParameterDesc param) {
            ParameterDesc newParam = new ParameterDesc();
            newParam.setType(param.getType());
            if (param.isColumnType()) {
                newParam.setValue(param.getColRef().getIdentity());
            } else {
                newParam.setValue(param.getValue());
            }
            return newParam;
        }

        private FunctionDesc copyFunctionDesc(FunctionDesc orig) {
            TblColRef paramColRef = orig.getParameters().get(0).getColRef();
            List<ParameterDesc> newParams = Lists.newArrayList();
            orig.getParameters().forEach(parameterDesc -> newParams.add(copyParameterDesc(parameterDesc)));
            return CubeUtils.newFunctionDesc(dataModel, orig.getExpression(), newParams,
                    paramColRef == null ? null : paramColRef.getDatatype());
        }

        private boolean canTblColRefTreatAsDimension(OlapContext ctx, TblColRef tblColRef) {
            if (modelContext.getAntiFlatChecker().isColOfAntiLookup(tblColRef)) {
                return false;
            }
            if (modelContext.getExcludedChecker().isExcludedCol(tblColRef)) {
                return false;
            }
            if (ctx.getSQLDigest().isRawQuery) {
                return ctx.getAllColumns().contains(tblColRef);
            } else {
                return ctx.getFilterColumns().contains(tblColRef) || ctx.getGroupByColumns().contains(tblColRef)
                        || ctx.getSubqueryJoinParticipants().contains(tblColRef)
                        || dimensionAsMeasureColumns.contains(tblColRef);
            }
        }

        private boolean canTblColRefTreatAsDimension(Map<String, TblColRef> fKAsDimensionMap, TblColRef tblColRef) {
            return fKAsDimensionMap.containsKey(tblColRef.getCanonicalName());
        }

        protected NamedColumn transferToNamedColumn(TblColRef colRef, ColumnStatus status) {
            NamedColumn col = new NamedColumn();
            col.setName(colRef.getName());
            col.setAliasDotColumn(colRef.getIdentity());
            col.setId(++maxColId);
            col.setStatus(status);
            return col;
        }
    }
}

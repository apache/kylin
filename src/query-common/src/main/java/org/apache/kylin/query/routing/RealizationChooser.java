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

package org.apache.kylin.query.routing;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.exception.KylinTimeoutException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.logging.SetLogCategory;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.common.util.SetThreadName;
import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.HashMultimap;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableMap;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Multimap;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.cube.cuboid.NLayoutCandidate;
import org.apache.kylin.metadata.cube.cuboid.NLookupCandidate;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.FusionModelManager;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.model.graph.JoinsGraph;
import org.apache.kylin.metadata.project.NProjectLoader;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.HybridRealization;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
import org.apache.kylin.metadata.realization.NoStreamingRealizationFoundException;
import org.apache.kylin.metadata.realization.RealizationRuntimeException;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPContextProp;
import org.apache.kylin.query.relnode.OLAPTableScan;
import org.apache.kylin.query.util.RelAggPushDownUtil;
import org.apache.kylin.storage.StorageContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.ttl.TtlRunnable;

import lombok.val;

public class RealizationChooser {

    private static final Logger logger = LoggerFactory.getLogger(RealizationChooser.class);
    private static final ExecutorService selectCandidateService = new ThreadPoolExecutor(
            KylinConfig.getInstanceFromEnv().getQueryRealizationChooserThreadCoreNum(),
            KylinConfig.getInstanceFromEnv().getQueryRealizationChooserThreadMaxNum(), 60L, TimeUnit.SECONDS,
            new SynchronousQueue<>(), new NamedThreadFactory("RealChooser"), new ThreadPoolExecutor.CallerRunsPolicy());

    private RealizationChooser() {
    }

    // select models for given contexts, return realization candidates for each context
    public static void selectLayoutCandidate(List<OLAPContext> contexts) {
        // try different model for different context
        for (OLAPContext ctx : contexts) {
            if (ctx.isConstantQueryWithAggregations()) {
                continue;
            }
            attemptSelectCandidate(ctx);
            Preconditions.checkNotNull(ctx.realization);
        }
    }

    public static void multiThreadSelectLayoutCandidate(List<OLAPContext> contexts) {
        List<Future<?>> futureList = Lists.newArrayList();
        try {
            // try different model for different context
            CountDownLatch latch = new CountDownLatch(contexts.size());
            for (OLAPContext ctx : contexts) {
                TtlRunnable r = Objects.requireNonNull(TtlRunnable.get(() -> selectCandidate0(latch, ctx)));
                Future<?> future = selectCandidateService.submit(r);
                futureList.add(future);
            }
            latch.await();
            for (Future<?> future : futureList) {
                future.get();
            }
        } catch (ExecutionException e) {
            if (e.getCause() instanceof NoRealizationFoundException) {
                throw (NoRealizationFoundException) e.getCause();
            } else if (e.getCause() instanceof NoStreamingRealizationFoundException) {
                throw (NoStreamingRealizationFoundException) e.getCause();
            } else {
                throw new RealizationRuntimeException("unexpected error when choose layout", e);
            }
        } catch (InterruptedException e) {
            for (Future<?> future : futureList) {
                future.cancel(true);
            }
            QueryContext.current().getQueryTagInfo().setTimeout(true);
            Thread.currentThread().interrupt();
            throw new KylinTimeoutException("The query exceeds the set time limit of "
                    + KylinConfig.getInstanceFromEnv().getQueryTimeoutSeconds()
                    + "s. Current step: Realization chooser. ");
        }
    }

    private static void selectCandidate0(CountDownLatch latch, OLAPContext ctx) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        String queryId = QueryContext.current().getQueryId();
        try (KylinConfig.SetAndUnsetThreadLocalConfig ignored0 = KylinConfig.setAndUnsetThreadLocalConfig(kylinConfig);
                SetThreadName ignored1 = new SetThreadName(Thread.currentThread().getName() + " QueryId %s", queryId);
                SetLogCategory ignored2 = new SetLogCategory("query")) {

            String project = ctx.olapSchema.getProjectName();
            NTableMetadataManager.getInstance(kylinConfig, project);
            NDataModelManager.getInstance(kylinConfig, project);
            NDataflowManager.getInstance(kylinConfig, project);
            NIndexPlanManager.getInstance(kylinConfig, project);
            NProjectLoader.updateCache(project);

            if (!ctx.isConstantQueryWithAggregations()) {
                ctx.realizationCheck = new RealizationCheck();
                attemptSelectCandidate(ctx);
                Preconditions.checkNotNull(ctx.realization);
            }
        } catch (KylinTimeoutException e) {
            logger.error("realization chooser thread task interrupted due to query [{}] timeout", queryId);
        } finally {
            NProjectLoader.removeCache();
            latch.countDown();
        }
    }

    @VisibleForTesting
    public static void attemptSelectCandidate(OLAPContext context) {
        // Step 1. get model through matching fact table with query
        Multimap<NDataModel, IRealization> modelMap = makeOrderedModelMap(context);
        checkNoRealizationFound(context, modelMap);

        logger.info("Context join graph: {}", context.getJoinsGraph());

        // Step 2.1 try to exactly match model
        List<Candidate> candidates = trySelectCandidates(context, modelMap, false, false);

        // Step 2.2 try to partial match model
        if (CollectionUtils.isEmpty(candidates)) {
            String project = context.olapSchema.getProjectName();
            KylinConfig projectConfig = NProjectManager.getProjectConfig(project);
            boolean partialMatch = projectConfig.isQueryMatchPartialInnerJoinModel();
            boolean nonEquiPartialMatch = projectConfig.partialMatchNonEquiJoins();
            if (partialMatch || nonEquiPartialMatch) {
                candidates = trySelectCandidates(context, modelMap, partialMatch, nonEquiPartialMatch);
                context.storageContext.setPartialMatchModel(CollectionUtils.isNotEmpty(candidates));
            }
        }

        if (candidates.isEmpty()) {
            checkNoRealizationWithStreaming(context);
            RelAggPushDownUtil.registerUnmatchedJoinDigest(context.getTopNode());
            throw new NoRealizationFoundException("No realization found for " + toErrorMsg(context));
        }

        // Step 3. find the lowest-cost candidate
        QueryRouter.sortCandidates(context.olapSchema.getProjectName(), candidates);
        logger.trace("Cost Sorted Realizations {}", candidates);
        Candidate candidate = candidates.get(0);
        restoreOLAPContextProps(context, candidate.getRewrittenCtx());
        context.fixModel(candidate.getRealization().getModel(), candidate.getMatchedJoinsGraphAliasMap());
        adjustForCapabilityInfluence(candidate, context);

        context.realization = candidate.realization;
        if (candidate.getCapability().isVacant()) {
            QueryContext.current().getQueryTagInfo().setVacant(true);
            NLayoutCandidate layoutCandidate = (NLayoutCandidate) candidate.capability.getSelectedCandidate();
            context.storageContext.setCandidate(layoutCandidate);
            context.storageContext.setLayoutId(layoutCandidate.getLayoutEntity().getId());
            context.storageContext.setEmptyLayout(true);
            return;
        }

        if (candidate.capability.getSelectedCandidate() instanceof NLookupCandidate) {
            boolean useSnapshot = context.isFirstTableLookupTableInModel(context.realization.getModel());
            context.storageContext.setUseSnapshot(useSnapshot);
        } else {
            Set<TblColRef> dimensions = Sets.newHashSet();
            Set<FunctionDesc> metrics = Sets.newHashSet();
            buildDimensionsAndMetrics(context, dimensions, metrics);
            buildStorageContext(context, dimensions, metrics, candidate);
            buildSecondStorageEnabled(context.getSQLDigest());
            fixContextForTableIndexAnswerNonRawQuery(context);
        }
    }

    private static void checkNoRealizationFound(OLAPContext context, Multimap<NDataModel, IRealization> modelMap) {
        if (!modelMap.isEmpty()) {
            return;
        }
        checkNoRealizationWithStreaming(context);
        RelAggPushDownUtil.registerUnmatchedJoinDigest(context.getTopNode());
        throw new NoRealizationFoundException("No model found for " + toErrorMsg(context));
    }

    private static List<Candidate> trySelectCandidates(OLAPContext context, Multimap<NDataModel, IRealization> modelMap,
            boolean partialMatch, boolean nonEquiPartialMatch) {
        List<Candidate> candidates = Lists.newArrayList();
        for (NDataModel model : modelMap.keySet()) {
            // preserve the props of OLAPContext may be modified
            OLAPContextProp preservedOlapProps = preservePropsBeforeRewrite(context);
            Map<String, String> matchedAliasMap = matchJoins(model, context, partialMatch, nonEquiPartialMatch);
            Set<IRealization> realizations = Sets.newHashSet(modelMap.get(model));
            List<Candidate> list = selectRealizations(model, context, matchedAliasMap, realizations);

            // discard the props of OLAPContext modified by rewriteCcInnerCol
            restoreOLAPContextProps(context, preservedOlapProps);

            // The matchJoin() method has the potential to optimize the JoinsGraph, 
            // therefore we perform a check on ready segments at this point.
            if (!hasReadySegments(model)) {
                logger.info("Exclude this model {} because there are no ready segments", model.getAlias());
                continue;
            }

            if (CollectionUtils.isNotEmpty(list)) {
                candidates.addAll(list);
                logger.info("context & model({}/{}/{}) match info: {}", context.olapSchema.getProjectName(),
                        model.getUuid(), model.getAlias(), true);
            }
        }
        return candidates;
    }

    private static void checkNoRealizationWithStreaming(OLAPContext context) {
        String projectName = context.olapSchema.getProjectName();
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        NTableMetadataManager tableManager = NTableMetadataManager.getInstance(kylinConfig, projectName);
        for (OLAPTableScan tableScan : context.allTableScans) {
            TableDesc tableDesc = tableManager.getTableDesc(tableScan.getTableName());
            if (tableDesc.getSourceType() == ISourceAware.ID_STREAMING) {
                throw new NoStreamingRealizationFoundException(ServerErrorCode.STREAMING_MODEL_NOT_FOUND,
                        MsgPicker.getMsg().getNoStreamingModelFound());
            }
        }
    }

    private static List<Candidate> selectRealizations(NDataModel model, OLAPContext context,
            Map<String, String> matchedGraphAliasMap, Set<IRealization> realizations) {
        // skip selection
        if (MapUtils.isEmpty(matchedGraphAliasMap)) {
            return Lists.newArrayList();
        }

        context.fixModel(model, matchedGraphAliasMap);
        preprocessSpecialAggregations(context);
        List<Candidate> candidates = Lists.newArrayListWithCapacity(realizations.size());
        for (IRealization real : realizations) {
            Candidate candidate = selectRealization(context, real, matchedGraphAliasMap);
            if (candidate != null) {
                candidates.add(candidate);
                logger.trace("Model {} QueryRouter matched", model);
            } else {
                logger.trace("Model {} failed in QueryRouter matching", model);
            }
        }
        context.setNeedToManyDerived(needToManyDerived(model));
        context.unfixModel();
        return candidates;
    }

    public static Candidate selectRealization(OLAPContext olapContext, IRealization realization,
            Map<String, String> matchedJoinGraphAliasMap) {
        if (!realization.isOnline()) {
            logger.warn("Realization {} is not ready", realization);
            return null;
        }

        Candidate candidate = new Candidate(realization, olapContext, matchedJoinGraphAliasMap);
        logger.info("Find candidates by table {} and project={} : {}", olapContext.firstTableScan.getTableName(),
                olapContext.olapSchema.getProjectName(), candidate);

        QueryRouter.applyRules(candidate);

        if (!candidate.getCapability().isCapable()) {
            return null;
        }

        candidate.setRewrittenCtx(preserveRewriteProps(olapContext));
        logger.info("The realizations remaining: {}, and the final chosen one for current olap context {} is {}",
                candidate.realization.getCanonicalName(), olapContext.id, candidate.realization.getCanonicalName());
        return candidate;
    }

    static OLAPContextProp preserveRewriteProps(OLAPContext rewrittenOLAContext) {
        return preservePropsBeforeRewrite(rewrittenOLAContext);
    }

    static OLAPContextProp preservePropsBeforeRewrite(OLAPContext oriOLAPContext) {
        OLAPContextProp preserved = new OLAPContextProp(-1);
        preserved.allColumns = Sets.newHashSet(oriOLAPContext.allColumns);
        preserved.setSortColumns(Lists.newArrayList(oriOLAPContext.getSortColumns()));
        preserved.setInnerGroupByColumns(Sets.newHashSet(oriOLAPContext.getInnerGroupByColumns()));
        preserved.setGroupByColumns(Sets.newLinkedHashSet(oriOLAPContext.getGroupByColumns()));
        preserved.setInnerFilterColumns(Sets.newHashSet(oriOLAPContext.getInnerFilterColumns()));
        for (FunctionDesc agg : oriOLAPContext.aggregations) {
            preserved.getReservedMap().put(agg,
                    FunctionDesc.newInstance(agg.getExpression(), agg.getParameters(), agg.getReturnType()));
        }

        return preserved;
    }

    static void restoreOLAPContextProps(OLAPContext oriOLAPContext, OLAPContextProp preservedOLAPContext) {
        oriOLAPContext.allColumns = preservedOLAPContext.allColumns;
        oriOLAPContext.setSortColumns(preservedOLAPContext.getSortColumns());
        oriOLAPContext.aggregations.forEach(agg -> {
            if (preservedOLAPContext.getReservedMap().containsKey(agg)) {
                final FunctionDesc functionDesc = preservedOLAPContext.getReservedMap().get(agg);
                agg.setExpression(functionDesc.getExpression());
                agg.setParameters(functionDesc.getParameters());
                agg.setReturnType(functionDesc.getReturnType());
            }
        });
        oriOLAPContext.setGroupByColumns(preservedOLAPContext.getGroupByColumns());
        oriOLAPContext.setInnerGroupByColumns(preservedOLAPContext.getInnerGroupByColumns());
        oriOLAPContext.setInnerFilterColumns(preservedOLAPContext.getInnerFilterColumns());
        oriOLAPContext.resetSQLDigest();
    }

    private static boolean needToManyDerived(NDataModel model) {
        return model.getJoinTables().stream().anyMatch(JoinTableDesc::isDerivedToManyJoinRelation);
    }

    private static boolean hasReadySegments(NDataModel model) {
        val dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), model.getProject())
                .getDataflow(model.getUuid());
        if (model.isFusionModel()) {
            FusionModelManager fusionModelManager = FusionModelManager.getInstance(KylinConfig.getInstanceFromEnv(),
                    model.getProject());
            String batchId = fusionModelManager.getFusionModel(model.getFusionId()).getBatchModel().getUuid();
            val batchDataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), model.getProject())
                    .getDataflow(batchId);
            return dataflow.hasReadySegments() || batchDataflow.hasReadySegments();
        }
        return dataflow.hasReadySegments();
    }

    public static void fixContextForTableIndexAnswerNonRawQuery(OLAPContext context) {
        if (context.realization.getConfig().isUseTableIndexAnswerNonRawQuery()
                && !context.storageContext.isEmptyLayout() && context.isAnsweredByTableIndex()) {
            if (!context.aggregations.isEmpty()) {
                List<FunctionDesc> aggregations = context.aggregations;
                HashSet<TblColRef> needDimensions = Sets.newHashSet();
                for (FunctionDesc aggregation : aggregations) {
                    List<ParameterDesc> parameters = aggregation.getParameters();
                    for (ParameterDesc aggParameter : parameters) {
                        needDimensions.addAll(aggParameter.getColRef().getSourceColumns());
                    }
                }
                context.storageContext.getDimensions().addAll(needDimensions);
                context.aggregations.clear();
            }
            if (context.getSQLDigest().aggregations != null) {
                context.getSQLDigest().aggregations.clear();
            }
            if (context.storageContext.getMetrics() != null) {
                context.storageContext.getMetrics().clear();
            }
        }
    }

    private static void adjustForCapabilityInfluence(Candidate chosen, OLAPContext olapContext) {
        CapabilityResult capability = chosen.getCapability();

        for (CapabilityResult.CapabilityInfluence inf : capability.influences) {

            if (inf instanceof CapabilityResult.DimensionAsMeasure) {
                FunctionDesc functionDesc = ((CapabilityResult.DimensionAsMeasure) inf).getMeasureFunction();
                functionDesc.setDimensionAsMetric(true);
                addToContextGroupBy(functionDesc.getSourceColRefs(), olapContext);
                olapContext.resetSQLDigest();
                olapContext.getSQLDigest();
                logger.info("Adjust DimensionAsMeasure for {}", functionDesc);
            } else {

                MeasureDesc involvedMeasure = inf.getInvolvedMeasure();
                if (involvedMeasure == null)
                    continue;

                involvedMeasure.getFunction().getMeasureType().adjustSqlDigest(involvedMeasure,
                        olapContext.getSQLDigest());
            }
        }
    }

    private static void addToContextGroupBy(Collection<TblColRef> colRefs, OLAPContext context) {
        for (TblColRef col : colRefs) {
            if (!col.isInnerColumn() && context.belongToContextTables(col))
                context.getGroupByColumns().add(col);
        }
    }

    private static void preprocessSpecialAggregations(OLAPContext context) {
        if (CollectionUtils.isEmpty(context.aggregations))
            return;
        Iterator<FunctionDesc> it = context.aggregations.iterator();
        while (it.hasNext()) {
            FunctionDesc func = it.next();
            if (FunctionDesc.FUNC_GROUPING.equalsIgnoreCase(func.getExpression())) {
                it.remove();
            } else if (FunctionDesc.FUNC_INTERSECT_COUNT.equalsIgnoreCase(func.getExpression())) {
                TblColRef col = func.getColRefs().get(1);
                context.getGroupByColumns().add(col);
            }
        }
    }

    private static void buildSecondStorageEnabled(SQLDigest sqlDigest) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        if (kylinConfig.getSecondStorageQueryPushdownLimit() <= 0)
            return;

        if (sqlDigest.isRawQuery && sqlDigest.limit > kylinConfig.getSecondStorageQueryPushdownLimit()) {
            QueryContext.current().setRetrySecondStorage(false);
        }
    }

    private static void buildStorageContext(OLAPContext olapContext, Set<TblColRef> dimensions,
            Set<FunctionDesc> metrics, Candidate candidate) {
        boolean isBatchQuery = !(olapContext.realization instanceof HybridRealization)
                && !olapContext.realization.isStreaming();
        if (isBatchQuery) {
            buildBatchStorageContext(olapContext.storageContext, dimensions, metrics, candidate);
        } else {
            buildHybridStorageContext(olapContext.storageContext, dimensions, metrics, candidate);
        }
    }

    private static void buildBatchStorageContext(StorageContext storageContext, Set<TblColRef> dimensions,
            Set<FunctionDesc> metrics, Candidate candidate) {
        val layoutCandidate = (NLayoutCandidate) candidate.getCapability().getSelectedCandidate();
        val prunedSegments = candidate.getQueryableSeg().getBatchSegments();
        val prunedPartitions = candidate.getPrunedPartitions();
        if (layoutCandidate.isEmptyCandidate()) {
            storageContext.setLayoutId(-1L);
            storageContext.setEmptyLayout(true);
            logger.info("The context {}, chose empty layout", storageContext.getCtxId());
            return;
        }
        LayoutEntity layout = layoutCandidate.getLayoutEntity();
        storageContext.setCandidate(layoutCandidate);
        storageContext.setDimensions(dimensions);
        storageContext.setMetrics(metrics);
        storageContext.setLayoutId(layout.getId());
        storageContext.setPrunedSegments(prunedSegments);
        storageContext.setPrunedPartitions(prunedPartitions);
        logger.info("The context {}, chosen model: {}, its join: {}, layout: {}, dimensions: {}, measures: {}, " //
                + "segments: {}", storageContext.getCtxId(), layout.getModel().getAlias(),
                layout.getModel().getJoinsGraph(), layout.getId(), layout.getOrderedDimensions(),
                layout.getOrderedMeasures(), candidate.getQueryableSeg().getPrunedSegmentIds(true));
    }

    private static void buildHybridStorageContext(StorageContext context, Set<TblColRef> dimensions,
            Set<FunctionDesc> metrics, Candidate candidate) {
        context.setPrunedStreamingSegments(candidate.getQueryableSeg().getStreamingSegments());
        NLayoutCandidate streamingCandidate = (NLayoutCandidate) candidate.getCapability()
                .getSelectedStreamingCandidate();
        context.setStreamingCandidate(streamingCandidate);
        if (streamingCandidate == null || streamingCandidate.isEmptyCandidate()) {
            context.setStreamingLayoutId(-1L);
        } else {
            context.setStreamingLayoutId(streamingCandidate.getLayoutEntity().getId());
        }

        List<NDataSegment> prunedSegments = candidate.getQueryableSeg().getBatchSegments();
        NLayoutCandidate batchCandidate = (NLayoutCandidate) candidate.getCapability().getSelectedCandidate();
        if ((batchCandidate == null && streamingCandidate == NLayoutCandidate.EMPTY)
                || (streamingCandidate == null && batchCandidate == NLayoutCandidate.EMPTY)) {
            throw new NoStreamingRealizationFoundException(ServerErrorCode.STREAMING_MODEL_NOT_FOUND,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getNoStreamingModelFound()));
        }

        if (batchCandidate == NLayoutCandidate.EMPTY && streamingCandidate == NLayoutCandidate.EMPTY) {
            context.setLayoutId(-1L);
            context.setStreamingLayoutId(-1L);
            context.setEmptyLayout(true);
            logger.info("The context {}, chose empty layout", context.getCtxId());
            return;
        }

        if (differentTypeofIndex(batchCandidate, streamingCandidate)) {
            context.setLayoutId(null);
            context.setStreamingLayoutId(null);
            context.setEmptyLayout(true);
            logger.error("The index types of streaming model and batch model are different, "
                    + "and this situation is not supported yet.");
            throw new NoStreamingRealizationFoundException(ServerErrorCode.STREAMING_MODEL_NOT_FOUND,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getNoStreamingModelFound()));
        }

        NDataModel model = candidate.getRealization().getModel();

        context.setCandidate(batchCandidate);
        context.setDimensions(dimensions);
        context.setMetrics(metrics);
        context.setLayoutId(batchCandidate == null ? -1L : batchCandidate.getLayoutEntity().getId());
        context.setPrunedSegments(prunedSegments);
        context.setPrunedPartitions(candidate.getPrunedPartitions());
        if (batchCandidate != null && !batchCandidate.isEmptyCandidate()) {
            LayoutEntity layout = batchCandidate.getLayoutEntity();
            logger.info("The context {}, chosen model: {}, its join: {}, " //
                    + "batch layout: {}, batch layout dimensions: {}, " //
                    + "batch layout measures: {}, batch segments: {}", context.getCtxId(), model.getAlias(),
                    model.getJoinsGraph(), layout.getId(), layout.getOrderedDimensions(), layout.getOrderedMeasures(),
                    candidate.getQueryableSeg().getPrunedSegmentIds(true));
        }

        if (streamingCandidate != null && !streamingCandidate.isEmptyCandidate()) {
            LayoutEntity cuboidLayout = streamingCandidate.getLayoutEntity();
            logger.info("The context {}, chosen model: {}, its join: {}, " //
                    + "streaming layout: {}, streaming layout dimensions: {},  " //
                    + "streaming layout measures: {}, streaming segments: {}", context.getCtxId(), model.getAlias(),
                    model.getJoinsGraph(), cuboidLayout.getId(), cuboidLayout.getOrderedDimensions(),
                    cuboidLayout.getOrderedMeasures(), candidate.getQueryableSeg().getPrunedSegmentIds(false));
        }
    }

    private static boolean differentTypeofIndex(NLayoutCandidate batchLayout, NLayoutCandidate streamLayout) {
        if (batchLayout == null || batchLayout.isEmptyCandidate()) {
            return false;
        }
        if (streamLayout == null || streamLayout.isEmptyCandidate()) {
            return false;
        }
        return batchLayout.getLayoutEntity().getIndex().isTableIndex() != streamLayout.getLayoutEntity().getIndex()
                .isTableIndex();
    }

    private static void buildDimensionsAndMetrics(OLAPContext context, Collection<TblColRef> dimensions,
            Collection<FunctionDesc> metrics) {
        SQLDigest sqlDigest = context.getSQLDigest();
        IRealization realization = context.realization;
        for (FunctionDesc func : sqlDigest.aggregations) {
            if (!func.isDimensionAsMetric() && !func.isGrouping()) {
                // use the FunctionDesc from cube desc as much as possible, that has more info such as HLLC precision

                if (FunctionDesc.FUNC_INTERSECT_COUNT.equalsIgnoreCase(func.getExpression())) {
                    realization.getMeasures().stream()
                            .filter(measureDesc -> measureDesc.getFunction().getReturnType().equals("bitmap") && func
                                    .getParameters().get(0).equals(measureDesc.getFunction().getParameters().get(0)))
                            .forEach(measureDesc -> metrics.add(measureDesc.getFunction()));
                    dimensions.add(func.getParameters().get(1).getColRef());
                } else if (FunctionDesc.FUNC_BITMAP_UUID.equalsIgnoreCase(func.getExpression())
                        || FunctionDesc.FUNC_BITMAP_BUILD.equalsIgnoreCase(func.getExpression())) {
                    realization.getMeasures().stream()
                            .filter(measureDesc -> measureDesc.getFunction().getReturnType().equals("bitmap") && func
                                    .getParameters().get(0).equals(measureDesc.getFunction().getParameters().get(0)))
                            .forEach(measureDesc -> metrics.add(measureDesc.getFunction()));
                } else {
                    FunctionDesc aggrFuncFromDataflowDesc = realization.findAggrFunc(func);
                    metrics.add(aggrFuncFromDataflowDesc);
                }
            } else if (func.isDimensionAsMetric()) {
                FunctionDesc funcUsedDimenAsMetric = findAggrFuncFromRealization(func, realization);
                dimensions.addAll(funcUsedDimenAsMetric.getColRefs());

                Set<TblColRef> groupbyCols = Sets.newLinkedHashSet(sqlDigest.groupbyColumns);
                groupbyCols.addAll(funcUsedDimenAsMetric.getColRefs());
                sqlDigest.groupbyColumns = Lists.newArrayList(groupbyCols);
            }
        }

        if (sqlDigest.isRawQuery) {
            dimensions.addAll(sqlDigest.allColumns);
        } else {
            dimensions.addAll(sqlDigest.groupbyColumns);
            dimensions.addAll(sqlDigest.filterColumns);
        }
    }

    private static FunctionDesc findAggrFuncFromRealization(FunctionDesc aggrFunc, IRealization realization) {
        for (MeasureDesc measure : realization.getMeasures()) {
            if (measure.getFunction().equals(aggrFunc))
                return measure.getFunction();
        }
        return aggrFunc;
    }

    private static String toErrorMsg(OLAPContext ctx) {
        StringBuilder buf = new StringBuilder("OLAPContext");
        RealizationCheck checkResult = ctx.realizationCheck;
        for (List<RealizationCheck.IncapableReason> reasons : checkResult.getModelIncapableReasons().values()) {
            for (RealizationCheck.IncapableReason reason : reasons) {
                buf.append(", ").append(reason);
            }
        }
        buf.append(", ").append(ctx.firstTableScan);
        for (JoinDesc join : ctx.joins)
            buf.append(", ").append(join);
        return buf.toString();
    }

    public static Map<String, String> matchJoins(NDataModel model, OLAPContext ctx, boolean partialMatchInnerJoin,
            boolean partialMatchNonEquiJoin) {
        Map<String, String> matchedAliasMap = Maps.newHashMap();
        TableRef firstTable = ctx.firstTableScan.getTableRef();
        boolean matched;

        if (ctx.isFirstTableLookupTableInModel(model)) {
            // one lookup table, the matchedAliasMap was set to the mapping of tableAlias => modelAlias
            String modelAlias = model.findFirstTable(firstTable.getTableIdentity()).getAlias();
            matchedAliasMap = ImmutableMap.of(firstTable.getAlias(), modelAlias);
            matched = true;
            logger.info("Context fact table {} matched lookup table in model {}", ctx.firstTableScan.getTableName(),
                    model);
        } else if (ctx.joins.size() != ctx.allTableScans.size() - 1) {
            // has hanging tables
            ctx.realizationCheck.addModelIncapableReason(model,
                    RealizationCheck.IncapableReason.MODEL_BAD_JOIN_SEQUENCE);
            return new HashMap<>();
        } else {
            // normal join graph match
            if (ctx.getJoinsGraph() == null) {
                ctx.setJoinsGraph(new JoinsGraph(firstTable, ctx.joins));
            }
            matched = ctx.getJoinsGraph().match(model.getJoinsGraph(), matchedAliasMap, partialMatchInnerJoin,
                    partialMatchNonEquiJoin);
            if (!matched) {
                KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
                if (kylinConfig.isJoinMatchOptimizationEnabled()) {
                    logger.info(
                            "Query match join with join match optimization mode, trying to match with newly rewrite join graph.");
                    ctx.matchJoinWithFilterTransformation();
                    ctx.matchJoinWithEnhancementTransformation();
                    matched = ctx.getJoinsGraph().match(model.getJoinsGraph(), matchedAliasMap, partialMatchInnerJoin,
                            partialMatchNonEquiJoin);
                    logger.info("Match result for match join with join match optimization mode is: {}", matched);
                }
                logger.debug("Context join graph missed model {}, model join graph {}", model, model.getJoinsGraph());
                logger.debug("Missed match nodes - Context {}, Model {}",
                        ctx.getJoinsGraph().unmatched(model.getJoinsGraph()),
                        model.getJoinsGraph().unmatched(ctx.getJoinsGraph()));
            }
        }

        if (!matched) {
            ctx.realizationCheck.addModelIncapableReason(model, RealizationCheck.IncapableReason.MODEL_UNMATCHED_JOIN);
            return new HashMap<>();
        }
        ctx.realizationCheck.addCapableModel(model, matchedAliasMap);
        return matchedAliasMap;
    }

    private static Multimap<NDataModel, IRealization> makeOrderedModelMap(OLAPContext context) {
        if (context.getModelAlias() != null) {
            logger.info("The query is bounded to a certain model({}/{}).", context.olapSchema.getProjectName(),
                    context.getModelAlias());
        }

        KylinConfig kylinConfig = context.olapSchema.getConfig();
        String project = context.olapSchema.getProjectName();
        String factTable = context.firstTableScan.getOlapTable().getTableName();
        Set<IRealization> realizations = NProjectManager.getInstance(kylinConfig).getRealizationsByTable(project,
                factTable);

        final Multimap<NDataModel, IRealization> mapModelToRealizations = HashMultimap.create();
        boolean streamingEnabled = kylinConfig.streamingEnabled();
        for (IRealization real : realizations) {
            boolean skip = false;
            if (!real.isOnline()) {
                skip = true;
                logger.warn("Offline model({}/{}) with fact table {} cannot be queried.", project, real, factTable);
            } else if (isModelViewBounded(context, real)) {
                skip = true;
            } else if (omitFusionModel(streamingEnabled, real)) {
                skip = true;
                logger.info("Fusion model({}/{}) is skipped.", project, real.getUuid());
            }
            if (skip) {
                continue;
            }
            mapModelToRealizations.put(real.getModel(), real);
        }

        if (mapModelToRealizations.isEmpty()) {
            logger.error("No realization found for project {} with fact table {}", project, factTable);
        }

        return mapModelToRealizations;
    }

    /**
     * context is bound to a certain model (by model view)
     */
    private static boolean isModelViewBounded(OLAPContext context, IRealization realization) {
        return context.getModelAlias() != null
                && !StringUtils.equalsIgnoreCase(realization.getModel().getAlias(), context.getModelAlias());
    }

    private static boolean omitFusionModel(boolean turnOnStreaming, IRealization real) {
        return !turnOnStreaming && real.getModel().isFusionModel();
    }
}

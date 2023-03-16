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

import static org.apache.kylin.common.exception.ServerErrorCode.STREAMING_MODEL_NOT_FOUND;

import java.util.ArrayList;
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
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.exception.KylinTimeoutException;
import org.apache.kylin.common.logging.SetLogCategory;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.common.util.SetThreadName;
import org.apache.kylin.metadata.cube.cuboid.NLayoutCandidate;
import org.apache.kylin.metadata.cube.cuboid.NLookupCandidate;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.cube.realization.HybridRealization;
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
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.realization.CapabilityResult;
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
import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.HashMultimap;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableMap;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Multimap;
import org.apache.kylin.guava30.shaded.common.collect.Sets;

import lombok.val;

public class RealizationChooser {

    private static final Logger logger = LoggerFactory.getLogger(RealizationChooser.class);
    private static ExecutorService selectCandidateService = new ThreadPoolExecutor(
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
            ctx.realizationCheck = new RealizationCheck();
            attemptSelectCandidate(ctx);
            Preconditions.checkNotNull(ctx.realization);
        }
    }

    public static void multiThreadSelectLayoutCandidate(List<OLAPContext> contexts) {
        ArrayList<Future<?>> futureList = Lists.newArrayList();
        try {
            KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            String project = QueryContext.current().getProject();
            String queryId = QueryContext.current().getQueryId();
            // try different model for different context
            CountDownLatch latch = new CountDownLatch(contexts.size());
            for (OLAPContext ctx : contexts) {
                Future<?> future = selectCandidateService.submit(Objects.requireNonNull(TtlRunnable.get(() -> {
                    try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig
                            .setAndUnsetThreadLocalConfig(kylinConfig);
                            SetThreadName ignored = new SetThreadName(Thread.currentThread().getName() + " QueryId %s",
                                    queryId);
                            SetLogCategory logCategory = new SetLogCategory("query")) {
                        if (project != null) {
                            NTableMetadataManager.getInstance(kylinConfig, project);
                            NDataModelManager.getInstance(kylinConfig, project);
                            NDataflowManager.getInstance(kylinConfig, project);
                            NIndexPlanManager.getInstance(kylinConfig, project);
                            NProjectLoader.updateCache(project);
                        }
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
                })));
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

    @VisibleForTesting
    public static void attemptSelectCandidate(OLAPContext context) {
        if (context.getModelAlias() != null) {
            logger.info("context is bound to model {}", context.getModelAlias());
        }
        context.setHasSelected(true);
        // Step 1. get model through matching fact table with query
        Multimap<NDataModel, IRealization> modelMap = makeOrderedModelMap(context);
        if (modelMap.size() == 0) {
            checkNoRealizationWithStreaming(context);
            RelAggPushDownUtil.registerUnmatchedJoinDigest(context.getTopNode());
            throw new NoRealizationFoundException("No model found for " + toErrorMsg(context));
        }
        logger.trace("Models matched fact table {}: {}", context.firstTableScan.getTableName(), modelMap.values());
        List<Candidate> candidates = Lists.newArrayList();
        Map<NDataModel, Map<String, String>> model2AliasMap = Maps.newHashMap();

        // Step 2.1 try to exactly match model
        logger.info("Context join graph: {}", context.getJoinsGraph());
        for (NDataModel model : modelMap.keySet()) {
            OLAPContextProp preservedOLAPContext = QueryRouter.preservePropsBeforeRewrite(context);
            List<Candidate> candidate = selectRealizationFromModel(model, context, false, false, modelMap,
                    model2AliasMap);
            if (candidate != null && !candidate.isEmpty()) {
                candidates.addAll(candidate);
                logger.info("context & model({}, {}) match info: {}", model.getUuid(), model.getAlias(), true);
            }
            // discard the props of OLAPContext modified by rewriteCcInnerCol
            QueryRouter.restoreOLAPContextProps(context, preservedOLAPContext);
        }

        // Step 2.2 if no exactly model and user config to try partial model match, then try partial match model
        if (CollectionUtils.isEmpty(candidates) && (partialMatchInnerJoin() || partialMatchNonEquiJoin())) {
            for (NDataModel model : modelMap.keySet()) {
                OLAPContextProp preservedOLAPContext = QueryRouter.preservePropsBeforeRewrite(context);
                List<Candidate> candidate = selectRealizationFromModel(model, context, partialMatchInnerJoin(),
                        partialMatchNonEquiJoin(), modelMap, model2AliasMap);
                if (candidate != null) {
                    candidates.addAll(candidate);
                }
                // discard the props of OLAPContext modified by rewriteCcInnerCol
                QueryRouter.restoreOLAPContextProps(context, preservedOLAPContext);
            }
            context.storageContext.setPartialMatchModel(CollectionUtils.isNotEmpty(candidates));
        }

        // Step 3. find the lowest-cost candidate
        sortCandidate(context, candidates);
        logger.trace("Cost Sorted Realizations {}", candidates);
        if (!candidates.isEmpty()) {
            Candidate selectedCandidate = candidates.get(0);
            QueryRouter.restoreOLAPContextProps(context, selectedCandidate.getRewrittenCtx());
            context.fixModel(selectedCandidate.getRealization().getModel(),
                    model2AliasMap.get(selectedCandidate.getRealization().getModel()));
            adjustForCapabilityInfluence(selectedCandidate, context);

            context.realization = selectedCandidate.realization;
            if (selectedCandidate.capability.getSelectedCandidate() instanceof NLookupCandidate) {
                context.storageContext
                        .setUseSnapshot(context.isFirstTableLookupTableInModel(context.realization.getModel()));
            } else {
                Set<TblColRef> dimensions = Sets.newHashSet();
                Set<FunctionDesc> metrics = Sets.newHashSet();
                boolean isBatchQuery = !(context.realization instanceof HybridRealization)
                        && !context.realization.isStreaming();
                buildDimensionsAndMetrics(context.getSQLDigest(), dimensions, metrics, context.realization);
                buildStorageContext(context.storageContext, dimensions, metrics, selectedCandidate, isBatchQuery);
                buildSecondStorageEnabled(context.getSQLDigest());
                fixContextForTableIndexAnswerNonRawQuery(context);
            }
            return;
        }

        checkNoRealizationWithStreaming(context);
        RelAggPushDownUtil.registerUnmatchedJoinDigest(context.getTopNode());
        throw new NoRealizationFoundException("No realization found for " + toErrorMsg(context));
    }

    private static void sortCandidate(OLAPContext context, List<Candidate> candidates) {
        ProjectInstance projectInstance = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getProject(context.olapSchema.getProjectName());
        if (projectInstance.getConfig().useTableIndexAnswerSelectStarEnabled() && context.getSQLDigest().isRawQuery) {
            candidates.sort(Candidate.COMPARATOR_TABLE_INDEX);
        } else {
            candidates.sort(Candidate.COMPARATOR);
        }
    }

    private static void checkNoRealizationWithStreaming(OLAPContext context) {
        String projectName = context.olapSchema.getProjectName();
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        NTableMetadataManager tableManager = NTableMetadataManager.getInstance(kylinConfig, projectName);
        for (OLAPTableScan tableScan : context.allTableScans) {
            TableDesc tableDesc = tableManager.getTableDesc(tableScan.getTableName());
            if (tableDesc.getSourceType() == ISourceAware.ID_STREAMING) {
                throw new NoStreamingRealizationFoundException(STREAMING_MODEL_NOT_FOUND,
                        MsgPicker.getMsg().getNoStreamingModelFound());
            }
        }
    }

    private static List<Candidate> selectRealizationFromModel(NDataModel model, OLAPContext context,
            boolean isPartialMatch, boolean isPartialMatchNonEquiJoin, Multimap<NDataModel, IRealization> modelMap,
            Map<NDataModel, Map<String, String>> model2AliasMap) {
        final Map<String, String> map = matchJoins(model, context, isPartialMatch, isPartialMatchNonEquiJoin);
        if (MapUtils.isEmpty(map)) {
            return new ArrayList<>();
        }
        context.fixModel(model, map);
        model2AliasMap.put(model, map);

        preprocessOlapCtx(context);
        // check ready segments
        if (!hasReadySegments(model)) {
            context.unfixModel();
            logger.info("Exclude this model {} because there are no ready segments", model.getAlias());
            return new ArrayList<>();
        }

        Set<IRealization> realizations = Sets.newHashSet(modelMap.get(model));
        List<Candidate> candidates = Lists.newArrayListWithCapacity(realizations.size());
        for (IRealization real : realizations) {
            Candidate candidate = QueryRouter.selectRealization(context, real, model2AliasMap.get(model));
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

    private static void preprocessOlapCtx(OLAPContext context) {
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

    private static void buildStorageContext(StorageContext context, Set<TblColRef> dimensions,
            Set<FunctionDesc> metrics, Candidate candidate, boolean isBatchQuery) {
        if (isBatchQuery) {
            buildBatchStorageContext(context, dimensions, metrics, candidate);
        } else {
            buildStreamingStorageContext(context, dimensions, metrics, candidate);
        }
    }

    private static void buildBatchStorageContext(StorageContext context, Set<TblColRef> dimensions,
            Set<FunctionDesc> metrics, Candidate candidate) {
        val layoutCandidate = (NLayoutCandidate) candidate.getCapability().getSelectedCandidate();
        val prunedSegments = candidate.getPrunedSegments();
        val prunedPartitions = candidate.getPrunedPartitions();
        if (layoutCandidate.isEmptyCandidate()) {
            context.setLayoutId(-1L);
            context.setEmptyLayout(true);
            logger.info("for context {}, chose empty layout", context.getCtxId());
            return;
        }
        LayoutEntity cuboidLayout = layoutCandidate.getLayoutEntity();
        context.setCandidate(layoutCandidate);
        context.setDimensions(dimensions);
        context.setMetrics(metrics);
        context.setLayoutId(cuboidLayout.getId());
        context.setPrunedSegments(prunedSegments);
        context.setPrunedPartitions(prunedPartitions);
        val segmentIds = prunedSegments.stream().map(NDataSegment::getId).collect(Collectors.toList());
        logger.info(
                "for context {}, chosen model: {}, its join: {}, layout: {}, dimensions: {}, measures: {}, segments: {}",
                context.getCtxId(), cuboidLayout.getModel().getAlias(), cuboidLayout.getModel().getJoinsGraph(),
                cuboidLayout.getId(), cuboidLayout.getOrderedDimensions(), cuboidLayout.getOrderedMeasures(),
                segmentIds);

    }

    private static void buildStreamingStorageContext(StorageContext context, Set<TblColRef> dimensions,
            Set<FunctionDesc> metrics, Candidate candidate) {
        context.setPrunedStreamingSegments(candidate.getPrunedStreamingSegments());
        val layoutStreamingCandidate = (NLayoutCandidate) candidate.getCapability().getSelectedStreamingCandidate();
        context.setStreamingCandidate(layoutStreamingCandidate);
        if (layoutStreamingCandidate == null || layoutStreamingCandidate.isEmptyCandidate()) {
            context.setStreamingLayoutId(-1L);
        } else {
            context.setStreamingLayoutId(layoutStreamingCandidate.getLayoutEntity().getId());
        }

        List<NDataSegment> prunedSegments = candidate.getPrunedSegments();
        NLayoutCandidate layoutCandidate = (NLayoutCandidate) candidate.getCapability().getSelectedCandidate();

        val prunedPartitions = candidate.getPrunedPartitions();
        if ((layoutCandidate == null && layoutStreamingCandidate == NLayoutCandidate.EMPTY)
                || (layoutStreamingCandidate == null && layoutCandidate == NLayoutCandidate.EMPTY)) {
            throw new NoStreamingRealizationFoundException(STREAMING_MODEL_NOT_FOUND,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getNoStreamingModelFound()));
        }

        if (layoutCandidate == NLayoutCandidate.EMPTY && layoutStreamingCandidate == NLayoutCandidate.EMPTY) {
            context.setLayoutId(-1L);
            context.setStreamingLayoutId(-1L);
            context.setEmptyLayout(true);
            logger.info("for context {}, chose empty layout", context.getCtxId());
            return;
        }

        // TODO: support the case when the type of streaming index and batch index is different
        if (differentTypeofIndex(layoutCandidate, layoutStreamingCandidate)) {
            context.setLayoutId(null);
            context.setStreamingLayoutId(null);
            context.setEmptyLayout(true);
            logger.error("The case when the type of stream and batch index different is not supported yet.");
            throw new NoStreamingRealizationFoundException(STREAMING_MODEL_NOT_FOUND,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getNoStreamingModelFound()));
        }

        NDataModel model = candidate.getRealization().getModel();

        context.setCandidate(layoutCandidate);
        context.setDimensions(dimensions);
        context.setMetrics(metrics);
        context.setLayoutId(layoutCandidate == null ? -1L : layoutCandidate.getLayoutEntity().getId());
        context.setPrunedSegments(prunedSegments);
        context.setPrunedPartitions(prunedPartitions);
        if (layoutCandidate != null && !layoutCandidate.isEmptyCandidate()) {
            LayoutEntity cuboidLayout = layoutCandidate.getLayoutEntity();
            val segmentIds = prunedSegments.stream().map(NDataSegment::getId).collect(Collectors.toList());
            logger.info(
                    "for context {}, chosen model: {}, its join: {}, batch layout: {}, batch layout dimensions: {}, "
                            + "batch layout measures: {}, batch segments: {}",
                    context.getCtxId(), model.getAlias(), model.getJoinsGraph(), cuboidLayout.getId(),
                    cuboidLayout.getOrderedDimensions(), cuboidLayout.getOrderedMeasures(), segmentIds);
        }
        if (layoutStreamingCandidate != null && !layoutStreamingCandidate.isEmptyCandidate()) {
            LayoutEntity cuboidLayout = layoutStreamingCandidate.getLayoutEntity();
            val segmentIds = candidate.getPrunedStreamingSegments().stream().map(NDataSegment::getId)
                    .collect(Collectors.toList());
            logger.info(
                    "for context {}, chosen model: {}, its join: {}, streaming layout: {}, streaming layout dimensions: {}, "
                            + "streaming layout measures: {}, streaming segments: {}",
                    context.getCtxId(), model.getAlias(), model.getJoinsGraph(), cuboidLayout.getId(),
                    cuboidLayout.getOrderedDimensions(), cuboidLayout.getOrderedMeasures(), segmentIds);
        }
    }

    private static boolean differentTypeofIndex(NLayoutCandidate batchLayout, NLayoutCandidate streamLayout) {
        if (batchLayout == null || batchLayout.isEmptyCandidate()) {
            return false;
        }
        if (streamLayout == null || streamLayout.isEmptyCandidate()) {
            return false;
        }
        if (batchLayout.getLayoutEntity().getIndex().isTableIndex() != streamLayout.getLayoutEntity().getIndex()
                .isTableIndex()) {
            return true;
        }
        return false;
    }

    private static void buildDimensionsAndMetrics(SQLDigest sqlDigest, Collection<TblColRef> dimensions,
            Collection<FunctionDesc> metrics, IRealization realization) {
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

    public static Map<String, String> matchJoins(NDataModel model, OLAPContext ctx, boolean partialMatch,
            boolean partialMatchNonEquiJoin) {
        Map<String, String> matchUp = Maps.newHashMap();
        TableRef firstTable = ctx.firstTableScan.getTableRef();
        boolean matched;

        if (ctx.isFirstTableLookupTableInModel(model)) {
            // one lookup table
            String modelAlias = model.findFirstTable(firstTable.getTableIdentity()).getAlias();
            matchUp = ImmutableMap.of(firstTable.getAlias(), modelAlias);
            matched = true;
            logger.info("Context fact table {} matched lookup table in model {}", ctx.firstTableScan.getTableName(),
                    model);
        } else if (ctx.joins.size() != ctx.allTableScans.size() - 1) {
            // has hanging tables
            ctx.realizationCheck.addModelIncapableReason(model,
                    RealizationCheck.IncapableReason.create(RealizationCheck.IncapableType.MODEL_BAD_JOIN_SEQUENCE));
            return new HashMap<>();
        } else {
            // normal big joins
            if (ctx.getJoinsGraph() == null) {
                ctx.setJoinsGraph(new JoinsGraph(firstTable, ctx.joins));
            }
            matched = ctx.getJoinsGraph().match(model.getJoinsGraph(), matchUp, partialMatch, partialMatchNonEquiJoin);
            if (!matched) {
                KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
                if (kylinConfig.isJoinMatchOptimizationEnabled()) {
                    logger.info(
                            "Query match join with join match optimization mode, trying to match with newly rewrite join graph.");
                    ctx.matchJoinWithFilterTransformation();
                    ctx.matchJoinWithEnhancementTransformation();
                    matched = ctx.getJoinsGraph().match(model.getJoinsGraph(), matchUp, partialMatch,
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
            ctx.realizationCheck.addModelIncapableReason(model,
                    RealizationCheck.IncapableReason.create(RealizationCheck.IncapableType.MODEL_UNMATCHED_JOIN));
            return new HashMap<>();
        }
        ctx.realizationCheck.addCapableModel(model, matchUp);
        return matchUp;
    }

    public static Map<String, String> matchJoins(NDataModel model, OLAPContext ctx) {
        return matchJoins(model, ctx, partialMatchInnerJoin(), partialMatchNonEquiJoin());
    }

    private static Multimap<NDataModel, IRealization> makeOrderedModelMap(OLAPContext context) {
        KylinConfig kylinConfig = context.olapSchema.getConfig();
        String projectName = context.olapSchema.getProjectName();
        String factTableName = context.firstTableScan.getOlapTable().getTableName();
        Set<IRealization> realizations = NProjectManager.getInstance(kylinConfig).getRealizationsByTable(projectName,
                factTableName);

        final Multimap<NDataModel, IRealization> mapModelToRealizations = HashMultimap.create();
        boolean streamingEnabled = kylinConfig.streamingEnabled();
        for (IRealization real : realizations) {
            if (!real.isReady() || isModelViewBounded(context, real) || omitFusionModel(streamingEnabled, real)) {
                if (!real.isReady()) {
                    context.realizationCheck.addIncapableCube(real,
                            RealizationCheck.IncapableReason.create(RealizationCheck.IncapableType.CUBE_NOT_READY));
                    logger.warn("Realization {} is not ready for project {} with fact table {}", real, projectName,
                            factTableName);
                }
                continue;
            }
            mapModelToRealizations.put(real.getModel(), real);
        }

        if (mapModelToRealizations.isEmpty()) {
            logger.error("No realization found for project {} with fact table {}", projectName, factTableName);
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

    private static boolean partialMatchInnerJoin() {
        return getProjectConfig().isQueryMatchPartialInnerJoinModel();
    }

    private static boolean partialMatchNonEquiJoin() {
        return getProjectConfig().partialMatchNonEquiJoins();
    }

    private static KylinConfig getProjectConfig() {
        String project = QueryContext.current().getProject();
        try {
            if (project != null) {
                return NProjectManager.getProjectConfig(project);
            }
        } catch (Exception e) {
            logger.error("Failed to get config of project<{}> when matching partial inner join model. {}", //
                    project, e.getMessage());
        }
        return KylinConfig.getInstanceFromEnv();
    }

}

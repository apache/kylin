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

import org.apache.calcite.rex.RexNode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.constant.LogConstant;
import org.apache.kylin.common.exception.KylinTimeoutException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.logging.SetLogCategory;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.common.util.SetThreadName;
import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;
import org.apache.kylin.guava30.shaded.common.collect.HashMultimap;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Multimap;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.cube.cuboid.NLayoutCandidate;
import org.apache.kylin.metadata.cube.cuboid.NLookupCandidate;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.FusionModelManager;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.NProjectLoader;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.HybridRealization;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
import org.apache.kylin.metadata.realization.NoStreamingRealizationFoundException;
import org.apache.kylin.metadata.realization.RealizationRuntimeException;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.query.relnode.OlapContext;
import org.apache.kylin.query.relnode.OlapContextProp;
import org.apache.kylin.query.relnode.OlapFilterRel;
import org.apache.kylin.query.relnode.OlapTableScan;
import org.apache.kylin.query.util.RelAggPushDownUtil;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.util.FilterConditionExpander;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.ttl.TtlRunnable;

public class RealizationChooser {

    private static final Logger logger = LoggerFactory.getLogger(RealizationChooser.class);
    private static final ExecutorService selectCandidateService = new ThreadPoolExecutor(
            KylinConfig.getInstanceFromEnv().getQueryRealizationChooserThreadCoreNum(),
            KylinConfig.getInstanceFromEnv().getQueryRealizationChooserThreadMaxNum(), 60L, TimeUnit.SECONDS,
            new SynchronousQueue<>(), new NamedThreadFactory("RealChooser"), new ThreadPoolExecutor.CallerRunsPolicy());

    private RealizationChooser() {
    }

    // select models for given contexts, return realization candidates for each context
    public static void selectLayoutCandidate(String project, List<OlapContext> contexts) {
        if (!NProjectManager.getProjectConfig(project).isRealizationChooserUsingMultiThread()) {
            contexts.forEach(RealizationChooser::attemptSelectCandidate);
            return;
        }

        // use multi-threads to select candidates
        List<Future<?>> futureList = Lists.newArrayList();
        try {
            CountDownLatch latch = new CountDownLatch(contexts.size());
            for (OlapContext ctx : contexts) {
                TtlRunnable r = Objects.requireNonNull(TtlRunnable.get(() -> selectCandidate0(project, latch, ctx)));
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

    private static void selectCandidate0(String project, CountDownLatch latch, OlapContext ctx) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        String queryId = QueryContext.current().getQueryId();
        try (KylinConfig.SetAndUnsetThreadLocalConfig ignored0 = KylinConfig.setAndUnsetThreadLocalConfig(kylinConfig);
                SetThreadName ignored1 = new SetThreadName(Thread.currentThread().getName() + " QueryId %s", queryId);
                SetLogCategory ignored2 = new SetLogCategory(LogConstant.QUERY_CATEGORY)) {

            NTableMetadataManager.getInstance(kylinConfig, project);
            NDataModelManager.getInstance(kylinConfig, project);
            NDataflowManager.getInstance(kylinConfig, project);
            NIndexPlanManager.getInstance(kylinConfig, project);
            NProjectLoader.updateCache(project);

            // select candidate for OlapContext
            attemptSelectCandidate(ctx);
        } catch (KylinTimeoutException e) {
            logger.error("realization chooser thread task interrupted due to query [{}] timeout", queryId);
        } finally {
            NProjectLoader.removeCache();
            latch.countDown();
        }
    }

    @VisibleForTesting
    public static void attemptSelectCandidate(OlapContext context) {
        if (context.isInvalidContext()) {
            context.markInvalid();
            return;
        } else if (context.getAllTableScans().isEmpty() || context.isConstantQueryWithAggregations()) {
            return;
        }

        NLookupCandidate.Policy policy = context.deduceLookupTableType();
        if (policy != NLookupCandidate.Policy.NONE && policy != NLookupCandidate.Policy.AGG_THEN_INTERNAL_TABLE
                && policy != NLookupCandidate.Policy.AGG_THEN_SNAPSHOT) {
            matchLookupCandidate(context, policy);
            return;
        }

        // Step 1. filter qualified models by firstTable of OlapContext.
        String project = context.getOlapSchema().getProject();
        KylinConfig olapConfig = context.getOlapSchema().getConfig();
        Multimap<NDataModel, IRealization> modelMap = filterQualifiedModelMap(context);
        checkNoRealizationFound(context, modelMap, policy);

        // Step 2.1 try to exactly match model
        List<Candidate> candidates = trySelectCandidates(context, modelMap, false, false);

        // Step 2.2 try to partial match model
        if (CollectionUtils.isEmpty(candidates)) {
            boolean partialMatch = olapConfig.isQueryMatchPartialInnerJoinModel();
            boolean nonEquiPartialMatch = olapConfig.partialMatchNonEquiJoins();
            if (partialMatch || nonEquiPartialMatch) {
                candidates = trySelectCandidates(context, modelMap, partialMatch, nonEquiPartialMatch);
                context.getStorageContext().setPartialMatch(CollectionUtils.isNotEmpty(candidates));
            }
        }

        if (candidates.isEmpty()) {
            if (isLookupCandidateMatched(context, policy)) {
                return;
            }
            checkNoRealizationWithStreaming(context);
            RelAggPushDownUtil.registerUnmatchedJoinDigest(context.getTopNode());
            throw new NoRealizationFoundException("No realization found for " + context.incapableMsg());
        }

        // Step 3. find the lowest-cost candidate
        QueryRouter.sortCandidates(project, candidates);
        logger.trace("Cost Sorted Realizations {}", candidates);
        Candidate candidate = candidates.get(0);
        restoreOlapContextProps(context, candidate.getRewrittenCtx());
        context.fixModel(candidate.getRealization().getModel(), candidate.getMatchedJoinsGraphAliasMap());
        adjustForCapabilityInfluence(candidate, context);

        context.setRealization(candidate.getRealization());
        if (candidate.getCapability().isVacant()) {
            QueryContext.current().getQueryTagInfo().setVacant(true);
            NLayoutCandidate layoutCandidate = (NLayoutCandidate) candidate.capability.getSelectedCandidate();
            context.getStorageContext().setBatchCandidate(layoutCandidate);
            context.getStorageContext().setDataSkipped(true);
            return;
        }

        Set<TblColRef> dimensions = Sets.newHashSet();
        Set<FunctionDesc> metrics = Sets.newHashSet();
        buildDimensionsAndMetrics(context, dimensions, metrics);
        buildStorageContext(context, dimensions, metrics, candidate);
        if (!QueryContext.current().isForModeling()) {
            fixContextForTableIndexAnswerNonRawQuery(context);
        }
    }

    public static boolean isLookupCandidateMatched(OlapContext context, NLookupCandidate.Policy policy) {
        if (policy == NLookupCandidate.Policy.AGG_THEN_INTERNAL_TABLE) {
            matchLookupCandidate(context, NLookupCandidate.Policy.INTERNAL_TABLE);
            return true;
        } else if (policy == NLookupCandidate.Policy.AGG_THEN_SNAPSHOT) {
            matchLookupCandidate(context, NLookupCandidate.Policy.SNAPSHOT);
            return true;
        }
        return false;
    }

    private static void matchLookupCandidate(OlapContext context, NLookupCandidate.Policy policy) {
        NLookupCandidate lookupCandidate = new NLookupCandidate(context.getSQLDigest().getFactTable(), policy);
        CapabilityResult result = new CapabilityResult();
        result.setCapable(true);
        result.setCandidate(false, lookupCandidate);
        result.setCost(lookupCandidate.getCost());
        lookupCandidate.setCapabilityResult(result);
        context.getStorageContext().setLookupCandidate(lookupCandidate);
    }

    private static void checkNoRealizationFound(OlapContext context, Multimap<NDataModel, IRealization> modelMap,
            NLookupCandidate.Policy policy) {
        if (!modelMap.isEmpty() || isLookupCandidateMatched(context, policy)) {
            return;
        }
        checkNoRealizationWithStreaming(context);
        RelAggPushDownUtil.registerUnmatchedJoinDigest(context.getTopNode());
        throw new NoRealizationFoundException("No model found for " + context.incapableMsg());
    }

    private static List<Candidate> trySelectCandidates(OlapContext context, Multimap<NDataModel, IRealization> modelMap,
            boolean partialMatch, boolean nonEquiPartialMatch) {
        List<Candidate> candidates = Lists.newArrayList();
        for (NDataModel model : modelMap.keySet()) {
            Map<String, String> matchedAliasMap = context.matchJoins(model, partialMatch, nonEquiPartialMatch);
            if (MapUtils.isEmpty(matchedAliasMap)) {
                continue;
            }

            // preserve the props of OlapContext may be modified
            OlapContextProp preservedOlapProps = preservePropsBeforeRewrite(context);
            List<Candidate> list = selectRealizations(model, context, matchedAliasMap, modelMap.get(model));
            // discard the props of OlapContext modified by rewriteCcInnerCol
            restoreOlapContextProps(context, preservedOlapProps);

            if (CollectionUtils.isNotEmpty(list)) {
                candidates.addAll(list);
                logger.info("context & model({}/{}/{}) match info: {}", context.getOlapSchema().getProject(),
                        model.getUuid(), model.getAlias(), true);
            }
        }
        return candidates;
    }

    private static void checkNoRealizationWithStreaming(OlapContext context) {
        String projectName = context.getOlapSchema().getProject();
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        NTableMetadataManager tableManager = NTableMetadataManager.getInstance(kylinConfig, projectName);
        for (OlapTableScan tableScan : context.getAllTableScans()) {
            TableDesc tableDesc = tableManager.getTableDesc(tableScan.getTableName());
            if (tableDesc.getSourceType() == ISourceAware.ID_STREAMING) {
                throw new NoStreamingRealizationFoundException(ServerErrorCode.STREAMING_MODEL_NOT_FOUND,
                        MsgPicker.getMsg().getNoStreamingModelFound());
            }
        }
    }

    private static List<Candidate> selectRealizations(NDataModel model, OlapContext context,
            Map<String, String> matchedGraphAliasMap, Collection<IRealization> realizations) {
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

    public static Candidate selectRealization(OlapContext olapContext, IRealization realization,
            Map<String, String> matchedJoinGraphAliasMap) {
        if (!realization.isOnline()) {
            logger.warn("Realization {} is not ready", realization);
            return null;
        }

        Candidate candidate = new Candidate(realization, olapContext, matchedJoinGraphAliasMap);
        logger.info("Find candidates by table {} and project={} : {}", olapContext.getFirstTableScan().getTableName(),
                olapContext.getOlapSchema().getProject(), candidate);

        for (OlapFilterRel filterRel : olapContext.getAllFilterRels()) {
            List<RexNode> filterConditions = new FilterConditionExpander(olapContext, filterRel)
                    .convert(filterRel.getCondition());
            olapContext.getExpandedFilterConditions().addAll(filterConditions);
        }

        QueryRouter.applyRules(candidate);

        if (!candidate.getCapability().isCapable()) {
            return null;
        }

        logger.info("The realizations remaining: {}, and the final chosen one for current olap context {} is {}",
                candidate.getRealization().getCanonicalName(), olapContext.getId(),
                candidate.getRealization().getCanonicalName());
        return candidate;
    }

    static OlapContextProp preservePropsBeforeRewrite(OlapContext oriOlapContext) {
        OlapContextProp preserved = new OlapContextProp(-1);
        preserved.setAllColumns(Sets.newHashSet(oriOlapContext.getAllColumns()));
        preserved.setSortColumns(Lists.newArrayList(oriOlapContext.getSortColumns()));
        preserved.setInnerGroupByColumns(Sets.newHashSet(oriOlapContext.getInnerGroupByColumns()));
        preserved.setGroupByColumns(Sets.newLinkedHashSet(oriOlapContext.getGroupByColumns()));
        preserved.setInnerFilterColumns(Sets.newHashSet(oriOlapContext.getInnerFilterColumns()));
        for (FunctionDesc agg : oriOlapContext.getAggregations()) {
            preserved.getReservedMap().put(agg,
                    FunctionDesc.newInstance(agg.getExpression(), agg.getParameters(), agg.getReturnType()));
        }

        return preserved;
    }

    static void restoreOlapContextProps(OlapContext oriOlapContext, OlapContextProp preservedOlapContext) {
        oriOlapContext.setAllColumns(preservedOlapContext.getAllColumns());
        oriOlapContext.setSortColumns(preservedOlapContext.getSortColumns());
        // By creating a new hashMap, the containsKey method can obtain appropriate results.
        // This is necessary because the aggregations have changed during the query matching process,
        // therefore changed hash of the same object would be put into different bucket of the LinkedHashMap.
        Map<FunctionDesc, FunctionDesc> map = Maps.newHashMap(preservedOlapContext.getReservedMap());
        oriOlapContext.getAggregations().forEach(agg -> {
            if (map.containsKey(agg)) {
                final FunctionDesc functionDesc = map.get(agg);
                agg.setExpression(functionDesc.getExpression());
                agg.setParameters(functionDesc.getParameters());
                agg.setReturnType(functionDesc.getReturnType());
            }
        });
        oriOlapContext.setGroupByColumns(preservedOlapContext.getGroupByColumns());
        oriOlapContext.setInnerGroupByColumns(preservedOlapContext.getInnerGroupByColumns());
        oriOlapContext.setInnerFilterColumns(preservedOlapContext.getInnerFilterColumns());
        oriOlapContext.resetSQLDigest();
    }

    private static boolean needToManyDerived(NDataModel model) {
        return model.getJoinTables().stream().anyMatch(JoinTableDesc::isDerivedToManyJoinRelation);
    }

    private static boolean hasReadySegments(NDataModel model) {
        if (QueryContext.current().isDryRun()) {
            return true;
        }
        KylinConfig kylinConfig = model.getConfig();
        String project = model.getProject();
        NDataflow dataflow = NDataflowManager.getInstance(kylinConfig, project).getDataflow(model.getUuid());
        if (model.isFusionModel()) {
            FusionModelManager fusionModelManager = FusionModelManager.getInstance(kylinConfig, project);
            String batchId = fusionModelManager.getFusionModel(model.getFusionId()).getBatchModel().getUuid();
            NDataflow batchDataflow = NDataflowManager.getInstance(kylinConfig, project).getDataflow(batchId);
            return dataflow.hasReadySegments() || batchDataflow.hasReadySegments();
        }
        return dataflow.hasReadySegments();
    }

    public static void fixContextForTableIndexAnswerNonRawQuery(OlapContext context) {
        if (context.getRealization().getConfig().isUseTableIndexAnswerNonRawQuery()
                && !context.getStorageContext().isDataSkipped() && context.isAnsweredByTableIndex()) {
            if (!context.getAggregations().isEmpty()) {
                List<FunctionDesc> aggregations = context.getAggregations();
                HashSet<TblColRef> needDimensions = Sets.newHashSet();
                for (FunctionDesc aggregation : aggregations) {
                    List<ParameterDesc> parameters = aggregation.getParameters();
                    for (ParameterDesc aggParameter : parameters) {
                        needDimensions.addAll(aggParameter.getColRef().getSourceColumns());
                    }
                }
                context.getStorageContext().getDimensions().addAll(needDimensions);
                context.getAggregations().clear();
            }
            if (context.getSQLDigest().getAggregations() != null) {
                context.getSQLDigest().getAggregations().clear();
            }
            if (context.getStorageContext().getMetrics() != null) {
                context.getStorageContext().getMetrics().clear();
            }
        }
    }

    private static void adjustForCapabilityInfluence(Candidate chosen, OlapContext olapContext) {
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

    private static void addToContextGroupBy(Collection<TblColRef> colRefs, OlapContext context) {
        for (TblColRef col : colRefs) {
            if (!col.isInnerColumn() && context.belongToContextTables(col))
                context.getGroupByColumns().add(col);
        }
    }

    private static void preprocessSpecialAggregations(OlapContext context) {
        if (CollectionUtils.isEmpty(context.getAggregations()))
            return;
        Iterator<FunctionDesc> it = context.getAggregations().iterator();
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

    private static void buildStorageContext(OlapContext olapContext, Set<TblColRef> dimensions,
            Set<FunctionDesc> metrics, Candidate candidate) {
        boolean isBatchQuery = !(olapContext.getRealization() instanceof HybridRealization)
                && !olapContext.getRealization().isStreaming();
        if (isBatchQuery) {
            buildBatchStorageContext(olapContext.getStorageContext(), dimensions, metrics, candidate);
        } else {
            buildHybridStorageContext(olapContext.getStorageContext(), dimensions, metrics, candidate);
        }
    }

    private static void buildBatchStorageContext(StorageContext storageContext, Set<TblColRef> dimensions,
            Set<FunctionDesc> metrics, Candidate candidate) {
        NLayoutCandidate layoutCandidate = (NLayoutCandidate) candidate.getCapability().getSelectedCandidate();
        layoutCandidate.setPrunedSegments(candidate.getQueryableSeg().getBatchSegments());
        if (layoutCandidate.isEmpty()) {
            storageContext.setDataSkipped(true);
            logger.info("The context({}) matches the batch-model with empty storage.", storageContext.getCtxId());
            return;
        }
        storageContext.setBatchCandidate(layoutCandidate);
        storageContext.setDimensions(dimensions);
        storageContext.setMetrics(metrics);
        storageContext.setPrunedPartitions(candidate.getPrunedPartitions());
        LayoutEntity layout = layoutCandidate.getLayoutEntity();
        logger.info("The context {}, chosen model: {}, its join: {}, layout: {}, dimensions: {}, measures: {}, " //
                + "segments: {}", storageContext.getCtxId(), layout.getModel().getAlias(),
                layout.getModel().getJoinsGraph(), layout.getId(), layout.getOrderedDimensions(),
                layout.getOrderedMeasures(), candidate.getQueryableSeg().getPrunedSegmentIds(true));
    }

    private static void buildHybridStorageContext(StorageContext storageContext, Set<TblColRef> dimensions,
            Set<FunctionDesc> metrics, Candidate candidate) {

        storageContext.setDimensions(dimensions);
        storageContext.setMetrics(metrics);
        storageContext.setPrunedPartitions(candidate.getPrunedPartitions());

        NLayoutCandidate streamCandidate = (NLayoutCandidate) candidate.getCapability().getSelectedStreamCandidate();
        NLayoutCandidate batchCandidate = (NLayoutCandidate) candidate.getCapability().getSelectedCandidate();

        boolean noCandidate = batchCandidate.isEmpty() && streamCandidate.isEmpty();
        if (candidate.getCapability().isCapable() && noCandidate) {
            storageContext.setDataSkipped(true);
            logger.info("The context({}) matches the hybrid-model with empty storage.", storageContext.getCtxId());
            return;
        }

        // check hybrid info
        if (noCandidate || differentTypeofIndex(batchCandidate, streamCandidate)) {
            throw new NoStreamingRealizationFoundException(ServerErrorCode.STREAMING_MODEL_NOT_FOUND,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getNoStreamingModelFound()));
        }

        // set streaming layoutCandidate
        streamCandidate.setPrunedSegments(candidate.getQueryableSeg().getStreamingSegments());
        storageContext.setStreamCandidate(streamCandidate);
        // set batch layoutCandidate
        batchCandidate.setPrunedSegments(candidate.getQueryableSeg().getBatchSegments());
        storageContext.setBatchCandidate(batchCandidate);

        // log
        NDataModel model = candidate.getRealization().getModel();
        if (!batchCandidate.isEmpty()) {
            LayoutEntity layout = batchCandidate.getLayoutEntity();
            logger.info("The context {}, chosen model: {}, its join: {}, " //
                    + "batch layout: {}, batch layout dimensions: {}, " //
                    + "batch layout measures: {}, batch segments: {}", storageContext.getCtxId(), model.getAlias(),
                    model.getJoinsGraph(), layout.getId(), layout.getOrderedDimensions(), layout.getOrderedMeasures(),
                    candidate.getQueryableSeg().getPrunedSegmentIds(true));
        }

        if (!streamCandidate.isEmpty()) {
            LayoutEntity layout = streamCandidate.getLayoutEntity();
            logger.info("The context {}, chosen model: {}, its join: {}, " //
                    + "streaming layout: {}, streaming layout dimensions: {},  " //
                    + "streaming layout measures: {}, streaming segments: {}", storageContext.getCtxId(),
                    model.getAlias(), model.getJoinsGraph(), layout.getId(), layout.getOrderedDimensions(),
                    layout.getOrderedMeasures(), candidate.getQueryableSeg().getPrunedSegmentIds(false));
        }
    }

    private static boolean differentTypeofIndex(NLayoutCandidate batchLayout, NLayoutCandidate streamLayout) {
        return !batchLayout.isEmpty() && !streamLayout.isEmpty()
                && batchLayout.isTableIndex() != streamLayout.isTableIndex();
    }

    private static void buildDimensionsAndMetrics(OlapContext context, Collection<TblColRef> dimensions,
            Collection<FunctionDesc> metrics) {
        SQLDigest sqlDigest = context.getSQLDigest();
        IRealization realization = context.getRealization();
        for (FunctionDesc func : sqlDigest.getAggregations()) {
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

                Set<TblColRef> groupByCols = Sets.newLinkedHashSet(sqlDigest.getGroupByColumns());
                groupByCols.addAll(funcUsedDimenAsMetric.getColRefs());
                sqlDigest.setGroupByColumns(Lists.newArrayList(groupByCols));
            }
        }

        if (sqlDigest.isRawQuery) {
            dimensions.addAll(sqlDigest.getAllColumns());
        } else {
            dimensions.addAll(sqlDigest.getGroupByColumns());
            dimensions.addAll(sqlDigest.getFilterColumns());
        }
    }

    private static FunctionDesc findAggrFuncFromRealization(FunctionDesc aggrFunc, IRealization realization) {
        for (MeasureDesc measure : realization.getMeasures()) {
            if (measure.getFunction().equals(aggrFunc))
                return measure.getFunction();
        }
        return aggrFunc;
    }

    private static Multimap<NDataModel, IRealization> filterQualifiedModelMap(OlapContext context) {
        Multimap<NDataModel, IRealization> multimap = HashMultimap.create();
        KylinConfig olapConfig = context.getOlapSchema().getConfig();
        String project = context.getOlapSchema().getProject();
        String boundedModel = context.getBoundedModelAlias();
        if (boundedModel != null) {
            logger.info("The query is bounded to a certain model({}/{}).", project, boundedModel);
        }

        // link all qualified models
        String tableName = context.getFirstTableScan().getOlapTable().getTableName();
        boolean streamingEnabled = olapConfig.isStreamingEnabled();
        for (IRealization real : NProjectManager.getRealizations(olapConfig, project, tableName)) {
            NDataModel model = real.getModel();
            if (!real.isOnline() || !context.isBoundedModel(model) || skipFusionModel(streamingEnabled, model)) {
                continue;
            }
            multimap.put(model, real);
        }

        if (multimap.isEmpty()) {
            logger.warn("No realization found for project {} with fact table {}", project, tableName);
            return multimap;
        }

        // Remove models without ready segments
        List<Map.Entry<NDataModel, IRealization>> noReadySegEntries = multimap.entries().stream()
                .filter(entry -> !hasReadySegments(entry.getKey())).collect(Collectors.toList());
        multimap.entries().removeAll(noReadySegEntries);

        // Filter models in modelPriority comments
        String[] modelPriorities = QueryContext.current().getModelPriorities();
        if (olapConfig.useOnlyModelsInPriorities() && modelPriorities.length > 0) {
            Set<String> modelSet = Sets.newHashSet(modelPriorities);
            multimap.entries().removeIf(entry -> !modelSet.contains(StringUtils.upperCase(entry.getKey().getAlias())));
            List<String> usedModels = multimap.keys().stream().map(NDataModel::getAlias).collect(Collectors.toList());
            logger.info("Use only models in priorities: {}", usedModels);
        }
        return multimap;
    }

    private static boolean skipFusionModel(boolean turnOnStreaming, NDataModel model) {
        boolean b = !turnOnStreaming && model.isFusionModel();
        if (b) {
            logger.debug("Fusion model({}/{}) is skipped.", model.getProject(), model.getUuid());
        }
        return b;
    }
}

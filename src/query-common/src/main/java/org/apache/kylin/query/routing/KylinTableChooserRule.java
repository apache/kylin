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

import static org.apache.kylin.query.routing.QueryLayoutChooser.chooseBestLayoutCandidate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSimplify;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.exception.KylinTimeoutException;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.collect.BiMap;
import org.apache.kylin.guava30.shaded.common.collect.HashBiMap;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Range;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.cube.cuboid.ChooserContext;
import org.apache.kylin.metadata.cube.cuboid.IndexMatcher;
import org.apache.kylin.metadata.cube.cuboid.NLayoutCandidate;
import org.apache.kylin.metadata.cube.cuboid.NLookupCandidate;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataLayoutDetails;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.IRealizationCandidate;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.query.exception.UserStopQueryException;
import org.apache.kylin.query.relnode.OlapContext;
import org.apache.kylin.query.relnode.OlapContextProp;
import org.apache.kylin.query.util.ComputedColumnRewriter;
import org.apache.kylin.query.util.QueryAliasMatchInfo;
import org.apache.kylin.query.util.QueryInterruptChecker;
import org.apache.kylin.query.util.RexUtils;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KylinTableChooserRule extends PruningRule {

    @Override
    public void apply(Candidate candidate) {
        if (!isStorageMatch(candidate) || candidate.getCapability() != null) {
            return;
        }

        OlapContextProp propsBeforeRewrite = RealizationChooser.preservePropsBeforeRewrite(candidate.getCtx());
        CapabilityResult capabilityResult;

        if (!candidate.getRealization().getModel().getComputedColumnDescs().isEmpty()) {
            rewriteComputedColumns(candidate);
            candidate.getCtx().resetSQLDigest();
            capabilityResult = getCapabilityResult(candidate);
            candidate.recordRewrittenCtxProps();
        } else {
            RealizationChooser.restoreOlapContextProps(candidate.getCtx(), propsBeforeRewrite);
            candidate.getCtx().resetSQLDigest();
            capabilityResult = getCapabilityResult(candidate);
            candidate.recordRewrittenCtxProps();
        }

        candidate.setCapability(capabilityResult);
    }

    @Override
    public boolean isStorageMatch(Candidate candidate) {
        return candidate.getRealization().getModel().getStorageType().isV3Storage();
    }

    private void rewriteComputedColumns(Candidate candidate) {
        BiMap<String, String> aliasMapping = HashBiMap.create(candidate.getMatchedJoinsGraphAliasMap());
        ComputedColumnRewriter.rewriteCcInnerCol(candidate.getCtx(), candidate.getRealization().getModel(),
                new QueryAliasMatchInfo(aliasMapping, null));
    }

    private CapabilityResult getCapabilityResult(Candidate candidate) {
        IRealization realization = candidate.getRealization();
        SQLDigest sqlDigest = candidate.getCtx().getSQLDigest();
        CapabilityResult capability = check((NDataflow) realization, candidate.getCtx(), sqlDigest);

        // The matching process may modify the dimensions and measures info of the  OlapContext,
        // so we need these properties to be recorded in the candidate's rewrittenCtx. It is important
        // that once the OlapContext matched an index, no further matching will be performed.
        candidate.recordRewrittenCtxProps();
        return capability;
    }

    public CapabilityResult check(NDataflow dataflow, OlapContext olapContext, SQLDigest digest) {
        logMatchingLayout(dataflow, digest);
        CapabilityResult result = new CapabilityResult();

        if (digest.isLimitPrecedesAggr()) {
            logLimitPrecedesAggr(dataflow);
            result.incapableCause = CapabilityResult.IncapableCause
                    .create(CapabilityResult.IncapableType.LIMIT_PRECEDE_AGGR);
            return result;
        }

        IRealizationCandidate chosenCandidate = null;
        String factTableOfQuery = digest.getFactTable();
        String modelFactTable = dataflow.getModel().getQueryCompatibleFactTable(factTableOfQuery);

        if (digest.getJoinDescs().isEmpty() && !modelFactTable.equals(factTableOfQuery)) {
            log.trace("Snapshot dataflow matching");
            chosenCandidate = tryMatchLookup(dataflow, digest, result);
            if (chosenCandidate != null) {
                log.info("Matched table {} snapshot in dataflow {} ", factTableOfQuery, dataflow);
            }
        } else {
            NLayoutCandidate candidateAndInfluence = matchNormalDataflow(dataflow, digest, olapContext);
            if (candidateAndInfluence != null) {
                chosenCandidate = candidateAndInfluence;
                result.influences.addAll(candidateAndInfluence.getCapabilityResult().influences);
                log.info("Matched layout {} snapshot in dataflow {} ", chosenCandidate, dataflow);
            }
        }

        setResult(result, dataflow, chosenCandidate);
        return result;
    }

    private static void logMatchingLayout(NDataflow dataflow, SQLDigest digest) {
        log.info("Matching Layout in dataflow {}, SQL digest {}", dataflow, digest);
    }

    private static void logLimitPrecedesAggr(NDataflow dataflow) {
        log.info("Exclude NDataflow {} because there's limit preceding aggregation", dataflow);
    }

    private static void setResult(CapabilityResult result, NDataflow dataflow, IRealizationCandidate chosenCandidate) {
        if (chosenCandidate != null) {
            result.setCapable(true);
            result.setCandidate(dataflow.isStreaming(), chosenCandidate);
            result.setCost(chosenCandidate.getCost());
        } else {
            result.setCapable(false);
        }
    }

    private NLayoutCandidate matchNormalDataflow(NDataflow dataflow, SQLDigest digest, OlapContext ctx) {
        log.trace("Normal dataflow matching");
        List<NDataLayoutDetails> fragments = dataflow.listAllLayoutDetails();
        NLayoutCandidate candidateAndInfluence = selectLayoutCandidate(dataflow, fragments, digest, ctx);

        if (candidateAndInfluence != null) {
            log.info("Matched layout {} snapshot in dataflow {} ", candidateAndInfluence, dataflow);
            return candidateAndInfluence;
        }
        return candidateAndInfluence;
    }

    private static List<Pair<LayoutEntity, NDataLayoutDetails>> getCommonLayouts(NDataflow dataflow,
            List<NDataLayoutDetails> fragments) {
        return fragments.stream().filter(f -> dataflow.getIndexPlan().getLayoutEntity(f.getLayoutId()) != null).map(
                fragment -> Pair.newPair(dataflow.getIndexPlan().getLayoutEntity(fragment.getLayoutId()), fragment))
                .collect(Collectors.toList());
    }

    private List<Pair<LayoutEntity, NDataLayoutDetails>> preCheckAndPruneLayouts(NDataflow dataflow,
            OlapContext olapContext, List<Pair<LayoutEntity, NDataLayoutDetails>> layouts) {
        KylinConfig projectConfig = NProjectManager.getProjectConfig(dataflow.getProject());
        if (!projectConfig.isHeterogeneousSegmentEnabled()) {
            return layouts;
        }

        // pruner layouts by partition column and dataformat
        PartitionDesc partitionCol = getPartitionDesc(dataflow, olapContext);
        if (isFullBuildModel(partitionCol)) {
            log.info("No partition column or partition column format is null.");
            return layouts;
        }

        // pruner layouts by simplify sql filter
        val relOptCluster = olapContext.getFirstTableScan().getCluster();
        val rexSimplify = new RexSimplify(relOptCluster.getRexBuilder(), RelOptPredicateList.EMPTY, true,
                relOptCluster.getPlanner().getExecutor());
        RexNode simplifiedFilter = rexSimplify.simplifyAnds(olapContext.getExpandedFilterConditions());
        if (simplifiedFilter.isAlwaysFalse()) {
            log.info("SQL filter condition is always false, pruning all layouts");
            olapContext.getStorageContext().setFilterCondAlwaysFalse(true);
            return Lists.newArrayList();
        }

        // pruner layout by customized scene optimize
        TblColRef partition = partitionCol.getPartitionDateColumnRef();

        if (simplifiedFilter.isAlwaysTrue()) {
            log.info("SQL filter condition is always true, pruning no layout");
            return layouts;
        }

        return pruneLayouts(dataflow, olapContext, rexSimplify, partition, simplifiedFilter, layouts);
    }

    private List<Pair<LayoutEntity, NDataLayoutDetails>> pruneLayouts(NDataflow dataflow, OlapContext olapContext,
            RexSimplify rexSimplify, TblColRef partitionColRef, RexNode simplifiedFilter,
            List<Pair<LayoutEntity, NDataLayoutDetails>> layouts) {
        if (CollectionUtils.isEmpty(olapContext.getExpandedFilterConditions())
                || CollectionUtils.isEmpty(olapContext.getFilterColumns())) {
            log.info("There is no filter for pruning layouts.");
            return layouts;
        }

        RexInputRef partitionColInputRef = olapContext.getFilterColumns().contains(partitionColRef)
                ? RexUtils.transformColumn2RexInputRef(partitionColRef, olapContext.getAllTableScans())
                : null;
        String partitionDateFormat = getPartitionDesc(dataflow, olapContext).getPartitionDateFormat();

        List<Pair<LayoutEntity, NDataLayoutDetails>> choosedLayouts = new ArrayList<>();
        for (Pair<LayoutEntity, NDataLayoutDetails> layoutEntity : layouts) {
            try {
                QueryInterruptChecker.checkQueryCanceledOrThreadInterrupted(
                        "Interrupted during pruning layouts by filter!", "pruning layouts by filter");

                val fragment = layoutEntity.getSecond();
                fragment.getFragmentRangeSet().asRanges().forEach(range -> {
                    List<RexNode> allPredicates = new ArrayList<>();
                    allPredicates.addAll(collectPartitionPredicates(rexSimplify.rexBuilder, range, partitionColInputRef,
                            partitionDateFormat, partitionColRef.getType()));
                    RelOptPredicateList predicateList = RelOptPredicateList.of(rexSimplify.rexBuilder, allPredicates);

                    RexNode simplifiedWithPredicates = rexSimplify.withPredicates(predicateList)
                            .simplify(simplifiedFilter);
                    if (!simplifiedWithPredicates.isAlwaysFalse()) {
                        choosedLayouts.add(layoutEntity);
                    }
                });

            } catch (InterruptedException ie) {
                log.error(String.format(Locale.ROOT, "Interrupted on pruning layout from %s!",
                        layoutEntity.getFirst().toString()), ie);
                Thread.currentThread().interrupt();
                throw new KylinRuntimeException(ie);
            } catch (UserStopQueryException | KylinTimeoutException e) {
                log.error(
                        String.format(Locale.ROOT, "Stop pruning layout from %s!", layoutEntity.getFirst().toString()),
                        e);
                throw e;
            } catch (Exception ex) {
                log.warn(String.format(Locale.ROOT, "To skip the exception on pruning layout %s!",
                        layoutEntity.getFirst().toString()), ex);
                choosedLayouts.add(layoutEntity);
            }
        }
        log.info("Pruned out {} layouts of {}.", (layouts.size() - choosedLayouts.size()), layouts.size());

        return choosedLayouts;
    }

    private List<RexNode> collectPartitionPredicates(RexBuilder rexBuilder, Range<Long> range,
            RexInputRef partitionColInputRef, String dateFormat, DataType partitionColType) {
        if (partitionColInputRef == null) {
            return Collections.emptyList();
        }

        List<RexNode> allPredicts = Lists.newArrayList();
        long fragmentStartTs = range.lowerEndpoint();
        long fragmentEndTs = range.upperEndpoint();
        String formattedStart = DateFormat.formatToDateStr(fragmentStartTs, dateFormat);
        String formattedEnd = DateFormat.formatToDateStr(fragmentEndTs, dateFormat);
        String start = checkAndReformatDateType(formattedStart, fragmentStartTs, partitionColType);
        String end = checkAndReformatDateType(formattedEnd, fragmentEndTs, partitionColType);
        List<RexNode> rexNodes = transformValue2RexCall(rexBuilder, partitionColInputRef, partitionColType, start, end,
                false);
        allPredicts.addAll(rexNodes);

        return allPredicts;
    }

    private NLayoutCandidate selectLayoutCandidate(NDataflow dataflow, List<NDataLayoutDetails> fragments,
            SQLDigest sqlDigest, OlapContext ctx) {
        if (CollectionUtils.isEmpty(fragments)) {
            log.info("There is no data range to answer sql");
            return NLayoutCandidate.ofEmptyCandidate();
        }

        ChooserContext chooserContext = new ChooserContext(sqlDigest, dataflow);
        if (chooserContext.isIndexMatchersInvalid()) {
            return null;
        }

        List<Pair<LayoutEntity, NDataLayoutDetails>> commonLayouts = getCommonLayouts(dataflow, fragments);
        commonLayouts = preCheckAndPruneLayouts(dataflow, ctx, commonLayouts);
        log.info("Matching dataflow with layout num: {}", commonLayouts.size());
        List<NLayoutCandidate> candidates = collectAllLayoutCandidates(chooserContext, commonLayouts);
        return chooseBestLayoutCandidate(dataflow, sqlDigest, chooserContext, candidates, "selectLayoutCandidate");
    }

    public List<NLayoutCandidate> collectAllLayoutCandidates(ChooserContext chooserContext,
            List<Pair<LayoutEntity, NDataLayoutDetails>> dataLayouts) {
        return dataLayouts.stream().filter(pair -> isMatched(chooserContext, pair.getFirst()))
                .map(pair -> createCandidate(chooserContext, pair)).collect(Collectors.toList());
    }

    private boolean isMatched(ChooserContext chooserContext, LayoutEntity layout) {
        IndexMatcher.MatchResult matchResult = chooserContext.getTableIndexMatcher().match(layout);
        if (!matchResult.isMatched()) {
            matchResult = chooserContext.getAggIndexMatcher().match(layout);
        }
        return matchResult.isMatched();
    }

    private NLayoutCandidate createCandidate(ChooserContext chooserContext,
            Pair<LayoutEntity, NDataLayoutDetails> pair) {
        LayoutEntity layout = pair.getFirst();
        NLayoutCandidate candidate = new NLayoutCandidate(layout, pair.getSecond());
        IndexMatcher.MatchResult matchResult = chooserContext.getTableIndexMatcher().match(layout);
        if (!matchResult.isMatched()) {
            matchResult = chooserContext.getAggIndexMatcher().match(layout);
        }
        CapabilityResult tempResult = new CapabilityResult(matchResult);
        if (!matchResult.getNeedDerive().isEmpty()) {
            candidate.setDerivedToHostMap(matchResult.getNeedDerive());
            candidate.setDerivedLookups(candidate.getDerivedToHostMap().keySet().stream()
                    .map(i -> chooserContext.convertToRef(i).getTable()).collect(Collectors.toSet()));
        }
        long size = pair.getSecond().getSizeInBytes();
        candidate.setCost(size * (tempResult.influences.size() + matchResult.getInfluenceFactor()));
        candidate.setCapabilityResult(tempResult);
        return candidate;
    }

    private static IRealizationCandidate tryMatchLookup(NDataflow dataflow, SQLDigest digest, CapabilityResult result) {
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(dataflow.getConfig(), dataflow.getProject());
        if (dataflow.getLatestReadySegment() == null
                || StringUtils.isEmpty(tableMgr.getTableDesc(digest.getFactTable()).getLastSnapshotPath())) {
            log.info("Exclude NDataflow {} because snapshot of table {} does not exist", dataflow,
                    digest.getFactTable());
            result.incapableCause = CapabilityResult.IncapableCause
                    .create(CapabilityResult.IncapableType.NOT_EXIST_SNAPSHOT);
            result.setCapable(false);
            return null;
        }

        NDataModel model = getModel(dataflow);
        Set<TblColRef> colsOfSnapShot = Sets.newHashSet(model.findFirstTable(digest.getFactTable()).getColumns());
        Collection<TblColRef> unmatchedCols = Sets.newHashSet(digest.getAllColumns());
        unmatchedCols.removeAll(colsOfSnapShot);

        if (!unmatchedCols.isEmpty()) {
            log.info("Exclude NDataflow {} because unmatched dimensions [{}] in Snapshot", dataflow, unmatchedCols);
            result.incapableCause = CapabilityResult.IncapableCause.unmatchedDimensions(unmatchedCols);
            return null;
        } else {
            return new NLookupCandidate(digest.getFactTable(), NLookupCandidate.getDerivedPolicy(dataflow.getConfig()));
        }
    }

    private static NDataModel getModel(NDataflow dataflow) {
        NDataModel model = dataflow.getModel();
        if (model.isFusionModel()) {
            model = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), model.getProject())
                    .getDataModelDesc(model.getFusionId());
        }
        return model;
    }
}

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

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.exception.KylinTimeoutException;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.guava30.shaded.common.collect.BiMap;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.cube.cuboid.NLayoutCandidate;
import org.apache.kylin.metadata.cube.model.DimensionRangeInfo;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.query.exception.UserStopQueryException;
import org.apache.kylin.query.relnode.OlapContext;
import org.apache.kylin.query.util.QueryInterruptChecker;
import org.apache.kylin.query.util.RexUtils;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SegmentPruningRule extends PruningRule {

    @Override
    public void apply(Candidate candidate) {
        if (!isStorageMatch(candidate)) {
            return;
        }

        List<IRealization> realizations = candidate.getRealization().getRealizations();
        for (IRealization realization : realizations) {
            NDataflow df = (NDataflow) realization;
            Segments<NDataSegment> prunedSegments = pruneSegments(df, candidate.getCtx());
            candidate.setPrunedSegments(prunedSegments, df);
        }
        if (CollectionUtils.isEmpty(candidate.getQueryableSeg().getBatchSegments())
                && CollectionUtils.isEmpty(candidate.getQueryableSeg().getStreamingSegments())) {
            log.info("{}({}/{}): there is no queryable segments to answer this query.", this.getClass().getName(),
                    candidate.getRealization().getProject(), candidate.getRealization().getCanonicalName());
            CapabilityResult capability = new CapabilityResult();
            capability.setCapable(true);
            capability.setSelectedCandidate(NLayoutCandidate.ofEmptyCandidate());
            capability.setSelectedStreamCandidate(NLayoutCandidate.ofEmptyCandidate());
            candidate.setCapability(capability);
        }
    }

    @Override
    public boolean isStorageMatch(Candidate candidate) {
        return candidate.getRealization().getModel().getStorageType().isV1Storage();
    }

    public Segments<NDataSegment> pruneSegments(NDataflow dataflow, OlapContext olapContext) {
        Segments<NDataSegment> allReadySegments = dataflow.getQueryableSegments();
        KylinConfig projectConfig = NProjectManager.getProjectConfig(dataflow.getProject());
        if (!projectConfig.isHeterogeneousSegmentEnabled()) {
            return allReadySegments;
        }

        // pruner segment by partition column and dataformat
        PartitionDesc partitionCol = getPartitionDesc(dataflow, olapContext);
        if (isFullBuildModel(partitionCol)) {
            log.info("No partition column or partition column format is null.");
            return allReadySegments;
        }

        // pruner segment by simplify sql filter
        val relOptCluster = olapContext.getFirstTableScan().getCluster();
        val rexSimplify = new RexSimplify(relOptCluster.getRexBuilder(), RelOptPredicateList.EMPTY, true,
                relOptCluster.getPlanner().getExecutor());
        RexNode simplifiedFilter = rexSimplify.simplifyAnds(olapContext.getExpandedFilterConditions());
        if (simplifiedFilter.isAlwaysFalse()) {
            log.info("SQL filter condition is always false, pruning all ready segments");
            olapContext.getStorageContext().setFilterCondAlwaysFalse(true);
            return Segments.empty();
        }

        // pruner segment by customized scene optimize
        TblColRef partition = partitionCol.getPartitionDateColumnRef();
        if (canPruneSegmentsForMaxMeasure(dataflow, olapContext, partition)) {
            return selectSegmentsForMaxMeasure(dataflow);
        }

        if (simplifiedFilter.isAlwaysTrue()) {
            log.info("SQL filter condition is always true, pruning no segment");
            return allReadySegments;
        }

        // prune segments by dimensions
        List<TblColRef> dimColRefs = Lists.newArrayList();
        if (projectConfig.isQueryDimensionRangeFilterEnabled()) {
            BiMap<Integer, TblColRef> effectiveDims = dataflow.getIndexPlan().getEffectiveDimCols();
            List<TblColRef> collect = olapContext.getFilterColumns().stream() //
                    .filter(tblColRef -> !tblColRef.equals(partition) && effectiveDims.containsValue(tblColRef))
                    .collect(Collectors.toList());
            dimColRefs.addAll(collect);
        }
        return prune(dataflow, olapContext, rexSimplify, partition, dimColRefs, simplifiedFilter);
    }

    private Segments<NDataSegment> prune(NDataflow dataflow, OlapContext olapContext, RexSimplify rexSimplify,
            TblColRef partitionColRef, List<TblColRef> dimTblColRefs, RexNode simplifiedFilter) {
        if (CollectionUtils.isEmpty(olapContext.getExpandedFilterConditions())
                || CollectionUtils.isEmpty(olapContext.getFilterColumns())) {
            log.info("There is no filter for pruning segments.");
            return dataflow.getQueryableSegments();
        }

        // When the expression node of filter condition is too complex, Calcite takes too long to simplify,
        // do not prune segments here, hand it over to Spark
        if (RelOptUtil.conjunctions(RexUtil.toCnf(rexSimplify.rexBuilder, 100, simplifiedFilter)).size() > dataflow
                .getConfig().getMaxFilterConditionCnt()) {
            return dataflow.getQueryableSegments();
        }

        Segments<NDataSegment> selectedSegments = new Segments<>();
        RexInputRef partitionColInputRef = olapContext.getFilterColumns().contains(partitionColRef)
                ? RexUtils.transformColumn2RexInputRef(partitionColRef, olapContext.getAllTableScans())
                : null;
        String partitionDateFormat = getPartitionDesc(dataflow, olapContext).getPartitionDateFormat();
        Map<TblColRef, RexInputRef> dimTblInputRefMap = dimTblColRefs.stream().collect(Collectors.toMap(tcr -> tcr,
                tcr -> RexUtils.transformColumn2RexInputRef(tcr, olapContext.getAllTableScans())));

        for (NDataSegment segment : dataflow.getQueryableSegments()) {
            try {
                QueryInterruptChecker.checkQueryCanceledOrThreadInterrupted(
                        "Interrupted during pruning segments by filter!", "pruning segments by filter");

                // evaluate partition column range [startTime, endTime) and normal dimension range [min, max]
                List<RexNode> allPredicates = Lists.newArrayList();
                allPredicates.addAll(collectPartitionPredicates(rexSimplify.rexBuilder, segment, partitionColInputRef,
                        partitionDateFormat, partitionColRef.getType()));
                allPredicates.addAll(collectDimensionPredicates(rexSimplify.rexBuilder, segment, dimTblInputRefMap));
                RelOptPredicateList predicateList = RelOptPredicateList.of(rexSimplify.rexBuilder, allPredicates);

                // To improve this simplification after fixed https://olapio.atlassian.net/browse/KE-42295
                RexNode simplifiedWithPredicates = rexSimplify.withPredicates(predicateList).simplify(simplifiedFilter);
                if (!simplifiedWithPredicates.isAlwaysFalse()) {
                    selectedSegments.add(segment);
                }
            } catch (InterruptedException ie) {
                log.error(String.format(Locale.ROOT, "Interrupted on pruning segments from %s!", segment.toString()),
                        ie);
                Thread.currentThread().interrupt();
                throw new KylinRuntimeException(ie);
            } catch (UserStopQueryException | KylinTimeoutException e) {
                log.error(String.format(Locale.ROOT, "Stop pruning segments from %s!", segment.toString()), e);
                throw e;
            } catch (Exception ex) {
                log.warn(String.format(Locale.ROOT, "To skip the exception on pruning segment %s!", segment.toString()),
                        ex);
                selectedSegments.add(segment);
            }
        }

        log.info("Scan segments {}/{} after time partition and dimension range pruning by[{}]", selectedSegments.size(),
                dataflow.getQueryableSegments().size(), simplifiedFilter);
        return selectedSegments;
    }

    private Segments<NDataSegment> selectSegmentsForMaxMeasure(NDataflow dataflow) {
        Segments<NDataSegment> selectedSegments = new Segments<>();
        long days = dataflow.getConfig().getMaxMeasureSegmentPrunerBeforeDays();
        // segment was sorted
        Segments<NDataSegment> allReadySegments = dataflow.getQueryableSegments();
        long maxDt = allReadySegments.getLatestReadySegment().getTSRange().getEnd();
        long minDt = maxDt - DateUtils.MILLIS_PER_DAY * days;
        for (int i = allReadySegments.size() - 1; i >= 0; i--) {
            if (allReadySegments.get(i).getTSRange().getEnd() > minDt) {
                selectedSegments.add(allReadySegments.get(i));
            } else {
                break;
            }
        }
        log.info("Scan segment size: {} after max measure segment pruner. The before days: {}. Passed on segment: {}",
                selectedSegments.size(), days,
                selectedSegments.stream().map(ISegment::getName).collect(Collectors.joining(",")));
        return selectedSegments;
    }

    private boolean canPruneSegmentsForMaxMeasure(NDataflow dataflow, OlapContext olapContext,
            TblColRef partitionColRef) {
        if (dataflow.getConfig().getMaxMeasureSegmentPrunerBeforeDays() < 0) {
            return false;
        }

        if (CollectionUtils.isNotEmpty(olapContext.getGroupByColumns())
                && !olapContext.getGroupByColumns().stream().allMatch(partitionColRef::equals)) {
            return false;
        }

        if (CollectionUtils.isEmpty(olapContext.getAggregations())) {
            return false;
        }

        for (FunctionDesc agg : olapContext.getAggregations()) {
            if (FunctionDesc.FUNC_MAX.equalsIgnoreCase(agg.getExpression())
                    && !partitionColRef.equals(agg.getParameters().get(0).getColRef())) {
                return false;
            }
            if (!FunctionDesc.FUNC_MAX.equalsIgnoreCase(agg.getExpression())
                    && CollectionUtils.isNotEmpty(agg.getParameters())) {
                return false;
            }
        }

        return true;
    }

    private List<RexNode> collectDimensionPredicates(RexBuilder rexBuilder, NDataSegment dataSegment,
            Map<TblColRef, RexInputRef> dimTblInputRefMap) {
        Map<String, DimensionRangeInfo> dimRangeInfoMap = dataSegment.getDimensionRangeInfoMap();
        if (dimRangeInfoMap.isEmpty()) {
            return Collections.emptyList();
        }

        List<RexNode> allPredicts = Lists.newArrayList();
        BiMap<TblColRef, Integer> inverse = dataSegment.getDataflow().getIndexPlan().getEffectiveDimCols().inverse();
        dimTblInputRefMap.forEach(((tblColRef, inputRef) -> {
            int index = inverse.get(tblColRef);
            DimensionRangeInfo dimRangeInfo = dimRangeInfoMap.get(String.valueOf(index));
            if (Objects.isNull(dimRangeInfo)) {
                log.warn("There is no values for the column[{}] in this segment.", tblColRef.getName());
                return;
            }

            List<RexNode> rexNodes = transformValue2RexCall(rexBuilder, inputRef, tblColRef.getType(),
                    dimRangeInfo.getMin(), dimRangeInfo.getMax(), true);
            allPredicts.addAll(rexNodes);
        }));
        return allPredicts;
    }

    private List<RexNode> collectPartitionPredicates(RexBuilder rexBuilder, NDataSegment segment,
            RexInputRef partitionColInputRef, String dateFormat, DataType partitionColType) {
        if (partitionColInputRef == null) {
            return Collections.emptyList();
        }

        List<RexNode> allPredicts = Lists.newArrayList();
        String start;
        String end;
        if (segment.isOffsetCube()) {
            start = DateFormat.formatToDateStr(segment.getKSRange().getStart(), dateFormat);
            end = DateFormat.formatToDateStr(segment.getKSRange().getEnd(), dateFormat);
        } else {
            long segmentStartTs = segment.getTSRange().getStart();
            long segmentEndTs = segment.getTSRange().getEnd();
            String formattedStart = DateFormat.formatToDateStr(segmentStartTs, dateFormat);
            String formattedEnd = DateFormat.formatToDateStr(segmentEndTs, dateFormat);
            start = checkAndReformatDateType(formattedStart, segmentStartTs, partitionColType);
            end = checkAndReformatDateType(formattedEnd, segmentEndTs, partitionColType);
        }
        List<RexNode> rexNodes = transformValue2RexCall(rexBuilder, partitionColInputRef, partitionColType, start, end,
                segment.getDataflow().isStreaming());
        allPredicts.addAll(rexNodes);
        return allPredicts;
    }
}

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

package org.apache.kylin.metadata.cube.cuboid;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinTimeoutException;
import org.apache.kylin.metadata.model.DeriveInfo;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.MetadataExtension;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.project.NProjectManager;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;

import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NQueryLayoutChooser {

    private NQueryLayoutChooser() {
    }

    public static NLayoutCandidate selectPartialLayoutCandidate(NDataflow dataflow, List<NDataSegment> prunedSegments,
            SQLDigest sqlDigest) {

        NLayoutCandidate candidate = null;
        List<NDataSegment> toRemovedSegments = Lists.newArrayList();
        for (NDataSegment segment : prunedSegments) {
            if (candidate == null) {
                candidate = selectLayoutCandidate(dataflow, Lists.newArrayList(segment), sqlDigest);
                if (candidate == null) {
                    toRemovedSegments.add(segment);
                }
            } else if (segment.getSegDetails().getLayoutById(candidate.getLayoutEntity().getId()) == null) {
                toRemovedSegments.add(segment);
            }
        }
        prunedSegments.removeAll(toRemovedSegments);
        return candidate;
    }

    public static NLayoutCandidate selectLayoutCandidate(NDataflow dataflow, List<NDataSegment> prunedSegments,
            SQLDigest sqlDigest) {

        if (CollectionUtils.isEmpty(prunedSegments)) {
            log.info("There is no segment to answer sql");
            return NLayoutCandidate.EMPTY;
        }
        List<NLayoutCandidate> candidates = new ArrayList<>();
        val commonLayouts = getLayoutsFromSegments(prunedSegments, dataflow);
        val model = dataflow.getModel();
        log.info("Matching dataflow with seg num: {} layout num: {}", prunedSegments.size(), commonLayouts.size());
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        Set<String> excludedTables = MetadataExtension.getFactory().getQueryExcludedTablesExtension()
                .getExcludedTables(config, model.getProject());
        boolean isReplaceCount = config.isReplaceColCountWithCountStar();
        val indexPlan = dataflow.getIndexPlan();
        val chooserContext = new ChooserContext(model);
        val aggIndexMatcher = new AggIndexMatcher(sqlDigest, chooserContext, excludedTables, isReplaceCount);
        val tableIndexMatcher = new TableIndexMatcher(sqlDigest, chooserContext, excludedTables,
                dataflow.getConfig().isUseTableIndexAnswerNonRawQuery());
        // bail out if both agg index are invalid
        // invalid matcher may caused by
        // 1. cc col is not present in the model
        // 2. dynamic params ? present in query like select sum(col/?) from ...,
        //    see org.apache.kylin.query.DynamicQueryTest.testDynamicParamOnAgg
        if (!aggIndexMatcher.valid() && !tableIndexMatcher.valid()) {
            return null;
        }
        for (NDataLayout dataLayout : commonLayouts) {
            log.trace("Matching layout {}", dataLayout);
            CapabilityResult tempResult = new CapabilityResult();
            // check indexEntity
            IndexEntity indexEntity = indexPlan.getIndexEntity(dataLayout.getIndexId());
            LayoutEntity layout = indexPlan.getLayoutEntity(dataLayout.getLayoutId());
            log.trace("Matching indexEntity {}", indexEntity);

            var matchResult = tableIndexMatcher.match(layout);
            if (!matchResult.isMatched()) {
                matchResult = aggIndexMatcher.match(layout);
            }
            if (!matchResult.isMatched()) {
                log.trace("Matching failed");
                continue;
            }

            NLayoutCandidate candidate = new NLayoutCandidate(layout);
            tempResult.influences = matchResult.getInfluences();
            candidate.setCost(dataLayout.getRows() * (tempResult.influences.size() + 1.0));
            if (!matchResult.getNeedDerive().isEmpty()) {
                candidate.setDerivedToHostMap(matchResult.getNeedDerive());
                candidate.setDerivedTableSnapshots(candidate.getDerivedToHostMap().keySet().stream()
                        .map(i -> chooserContext.convertToRef(i).getTable()).collect(Collectors.toSet()));
            }
            candidate.setCapabilityResult(tempResult);
            candidates.add(candidate);
        }

        if (Thread.interrupted()) {
            throw new KylinTimeoutException("The query exceeds the set time limit of "
                    + KylinConfig.getInstanceFromEnv().getQueryTimeoutSeconds() + "s. Current step: Layout chooser. ");
        }

        log.info("Matched candidates num : {}", candidates.size());
        if (candidates.isEmpty()) {
            return null;
        }
        sortCandidates(candidates, chooserContext, sqlDigest);
        return candidates.get(0);
    }

    private static Collection<NDataLayout> getLayoutsFromSegments(List<NDataSegment> segments, NDataflow dataflow) {
        val projectInstance = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getProject(dataflow.getProject());
        if (!projectInstance.getConfig().isHeterogeneousSegmentEnabled()) {
            return dataflow.getLatestReadySegment().getLayoutsMap().values();
        }

        val commonLayouts = Maps.<Long, NDataLayout> newHashMap();
        if (CollectionUtils.isEmpty(segments)) {
            return commonLayouts.values();
        }

        for (int i = 0; i < segments.size(); i++) {
            val dataSegment = segments.get(i);
            val layoutIdMapToDataLayout = dataSegment.getLayoutsMap();
            if (i == 0) {
                commonLayouts.putAll(layoutIdMapToDataLayout);
            } else {
                commonLayouts.keySet().retainAll(layoutIdMapToDataLayout.keySet());
            }
        }

        return commonLayouts.values();
    }

    private static void sortCandidates(List<NLayoutCandidate> candidates, ChooserContext chooserContext,
            SQLDigest sqlDigest) {
        final Set<TblColRef> filterColSet = ImmutableSet.copyOf(sqlDigest.filterColumns);
        final List<TblColRef> filterCols = Lists.newArrayList(filterColSet);
        val filterColIds = filterCols.stream().sorted(ComparatorUtils.filterColComparator(chooserContext))
                .map(col -> chooserContext.getTblColMap().get(col)).collect(Collectors.toList());

        final Set<TblColRef> nonFilterColSet = sqlDigest.isRawQuery ? sqlDigest.allColumns.stream()
                .filter(colRef -> colRef.getFilterLevel() == TblColRef.FilterColEnum.NONE).collect(Collectors.toSet())
                : sqlDigest.groupbyColumns.stream()
                        .filter(colRef -> colRef.getFilterLevel() == TblColRef.FilterColEnum.NONE)
                        .collect(Collectors.toSet());
        final List<TblColRef> nonFilterColumns = Lists.newArrayList(nonFilterColSet);
        nonFilterColumns.sort(ComparatorUtils.nonFilterColComparator());
        val nonFilterColIds = nonFilterColumns.stream().map(col -> chooserContext.getTblColMap().get(col))
                .collect(Collectors.toList());

        Ordering<NLayoutCandidate> ordering = Ordering //
                .from(derivedLayoutComparator()).compound(rowSizeComparator()) // L1 comparator, compare cuboid rows
                .compound(filterColumnComparator(filterColIds, chooserContext)) // L2 comparator, order filter columns
                .compound(dimensionSizeComparator()) // the lower dimension the best
                .compound(measureSizeComparator()) // L3 comparator, order size of cuboid columns
                .compound(nonFilterColumnComparator(nonFilterColIds)); // L4 comparator, order non-filter columns
        candidates.sort(ordering);
    }

    private static Comparator<NLayoutCandidate> derivedLayoutComparator() {
        return (layoutCandidate1, layoutCandidate2) -> {
            if (layoutCandidate1.getDerivedToHostMap().isEmpty() && !layoutCandidate2.getDerivedToHostMap().isEmpty()) {
                return -1;
            } else if (!layoutCandidate1.getDerivedToHostMap().isEmpty()
                    && layoutCandidate2.getDerivedToHostMap().isEmpty()) {
                return 1;
            }

            return 0;
        };
    }

    private static Comparator<NLayoutCandidate> rowSizeComparator() {
        return Comparator.comparingDouble(NLayoutCandidate::getCost);
    }

    private static Comparator<NLayoutCandidate> dimensionSizeComparator() {
        return Comparator.comparingInt(candidate -> candidate.getLayoutEntity().getOrderedDimensions().size());
    }

    private static Comparator<NLayoutCandidate> measureSizeComparator() {
        return Comparator.comparingInt(candidate -> candidate.getLayoutEntity().getOrderedMeasures().size());
    }

    /**
     * compare filters in SQL with layout dims
     * 1. choose the layout if its shardby column is found in filters
     * 2. otherwise compare position of filter columns appear in the layout dims
     */
    private static Comparator<NLayoutCandidate> filterColumnComparator(List<Integer> sortedFilters,
            ChooserContext chooserContext) {
        return Ordering.from(shardByComparator(sortedFilters)).compound(colComparator(sortedFilters));
    }

    private static Comparator<NLayoutCandidate> nonFilterColumnComparator(List<Integer> sortedNonFilters) {
        return colComparator(sortedNonFilters);
    }

    /**
     * compare filters with dim pos in layout, filter columns are sorted by filter type and selectivity (cardinality)
     */
    private static Comparator<NLayoutCandidate> colComparator(List<Integer> sortedCols) {
        return (layoutCandidate1, layoutCandidate2) -> {
            List<Integer> position1 = getColumnsPos(layoutCandidate1, sortedCols);
            List<Integer> position2 = getColumnsPos(layoutCandidate2, sortedCols);
            Iterator<Integer> iter1 = position1.iterator();
            Iterator<Integer> iter2 = position2.iterator();

            while (iter1.hasNext() && iter2.hasNext()) {
                int i1 = iter1.next();
                int i2 = iter2.next();

                int c = i1 - i2;
                if (c != 0)
                    return c;
            }

            return 0;
        };
    }

    /**
     * compare filter columns with shardby columns in layouts
     * 1. check if shardby columns appears in filters
     * 2. if both layout has shardy columns in filters, compare the filter type and selectivity (cardinality)
     */
    private static Comparator<NLayoutCandidate> shardByComparator(List<Integer> columns) {
        return (candidate1, candidate2) -> {
            int shardByCol1Idx = Integer.MAX_VALUE;
            List<Integer> shardByCols1 = candidate1.getLayoutEntity().getShardByColumns();
            if (CollectionUtils.isNotEmpty(shardByCols1)) {
                int tmpCol = shardByCols1.get(0);
                for (int i = 0; i < columns.size(); i++) {
                    if (columns.get(i) == tmpCol) {
                        shardByCol1Idx = i;
                        break;
                    }
                }
            }

            int shardByCol2Idx = Integer.MAX_VALUE;
            List<Integer> shardByCols2 = candidate2.getLayoutEntity().getShardByColumns();
            if (CollectionUtils.isNotEmpty(shardByCols2)) {
                int tmpCol = shardByCols2.get(0);
                for (int i = 0; i < columns.size(); i++) {
                    if (columns.get(i) == tmpCol) {
                        shardByCol2Idx = i;
                        break;
                    }
                }
            }

            return shardByCol1Idx - shardByCol2Idx;
        };
    }

    private static List<Integer> getColumnsPos(final NLayoutCandidate candidate, List<Integer> sortedColumns) {

        List<Integer> positions = Lists.newArrayList();
        for (Integer col : sortedColumns) {
            DeriveInfo deriveInfo = candidate.getDerivedToHostMap().get(col);
            if (deriveInfo == null) {
                positions.add(getDimsIndexInLayout(col, candidate));
            } else {
                for (Integer hostColId : deriveInfo.columns) {
                    positions.add(getDimsIndexInLayout(hostColId, candidate));
                }
            }
        }
        return positions;
    }

    private static int getDimsIndexInLayout(Integer id, final NLayoutCandidate candidate) {
        //get dimension
        return id == null ? -1 : candidate.getLayoutEntity().getColOrder().indexOf(id);
    }
}

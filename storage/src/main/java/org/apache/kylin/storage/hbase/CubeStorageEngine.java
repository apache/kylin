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

package org.apache.kylin.storage.hbase;

import java.util.*;

import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.storage.hbase.coprocessor.observer.ObserverEnabler;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.kylin.storage.StorageContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.kylin.common.persistence.HBaseConnection;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.RowValueDecoder;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.cube.model.HBaseMappingDesc;
import org.apache.kylin.cube.model.CubeDesc.DeriveInfo;
import org.apache.kylin.dict.lookup.LookupStringTable;
import org.apache.kylin.storage.IStorageEngine;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter.FilterOperatorEnum;
import org.apache.kylin.metadata.tuple.ITupleIterator;

/**
 * @author xjiang, yangli9
 */
public class CubeStorageEngine implements IStorageEngine {

    private static final Logger logger = LoggerFactory.getLogger(CubeStorageEngine.class);

    private static final int MERGE_KEYRANGE_THRESHOLD = 7;
    private static final long MEM_BUDGET_PER_QUERY = 3L * 1024 * 1024 * 1024; // 3G

    private final CubeInstance cubeInstance;
    private final CubeDesc cubeDesc;

    public CubeStorageEngine(CubeInstance cube) {
        this.cubeInstance = cube;
        this.cubeDesc = cube.getDescriptor();
    }

    @Override
    public ITupleIterator search(StorageContext context, SQLDigest sqlDigest) {

        Collection<TblColRef> groups = sqlDigest.groupbyColumns;
        TupleFilter filter = sqlDigest.filter;

        // build dimension & metrics
        Collection<TblColRef> dimensions = new HashSet<TblColRef>();
        Collection<FunctionDesc> metrics = new HashSet<FunctionDesc>();
        buildDimensionsAndMetrics(dimensions, metrics, sqlDigest);

        // all dimensions = groups + others
        Set<TblColRef> others = Sets.newHashSet(dimensions);
        others.removeAll(groups);

        // expand derived
        Set<TblColRef> derivedPostAggregation = Sets.newHashSet();
        Set<TblColRef> groupsD = expandDerived(groups, derivedPostAggregation);
        Set<TblColRef> othersD = expandDerived(others, derivedPostAggregation);
        othersD.removeAll(groupsD);
        derivedPostAggregation.removeAll(groups);

        // identify cuboid
        Set<TblColRef> dimensionsD = Sets.newHashSet();
        dimensionsD.addAll(groupsD);
        dimensionsD.addAll(othersD);
        Cuboid cuboid = identifyCuboid(dimensionsD);
        context.setCuboid(cuboid);

        // isExactAggregation? meaning: tuples returned from storage requires no further aggregation in query engine
        Set<TblColRef> singleValuesD = findSingleValueColumns(filter);
        boolean isExactAggregation = isExactAggregation(cuboid, groups, othersD, singleValuesD, derivedPostAggregation);
        context.setExactAggregation(isExactAggregation);

        // translate filter for scan range and compose returning groups for coprocessor, note:
        // - columns on non-evaluatable filter have to return
        // - columns on loosened filter (due to derived translation) have to return
        Set<TblColRef> groupsCopD = Sets.newHashSet(groupsD);
        groupsCopD.addAll(context.getOtherMandatoryColumns()); // TODO: this is tricky, to generalize
        collectNonEvaluable(filter, groupsCopD);
        TupleFilter filterD = translateDerived(filter, groupsCopD);

        // flatten to OR-AND filter, (A AND B AND ..) OR (C AND D AND ..) OR ..
        TupleFilter flatFilter = flattenToOrAndFilter(filterD);

        // translate filter into segment scan ranges
        List<HBaseKeyRange> scans = buildScanRanges(flatFilter, dimensionsD);

        // check involved measures, build value decoder for each each family:column
        List<RowValueDecoder> valueDecoders = translateAggregation(cubeDesc.getHBaseMapping(), metrics, context);

        setThreshold(dimensionsD, valueDecoders, context); // set cautious threshold to prevent out of memory
        setCoprocessor(groupsCopD, valueDecoders, context); // enable coprocessor if beneficial
        setLimit(filter, context);

        HConnection conn = HBaseConnection.get(context.getConnUrl());
        return new SerializedHBaseTupleIterator(conn, scans, cubeInstance, dimensionsD, filterD, groupsCopD, valueDecoders, context);
    }

    private void buildDimensionsAndMetrics(Collection<TblColRef> dimensions, Collection<FunctionDesc> metrics, SQLDigest sqlDigest) {

        for (FunctionDesc func : sqlDigest.aggregations) {
            if (!func.isDimensionAsMetric()) {
                metrics.add(func);
            }
        }

        for (TblColRef column : sqlDigest.allColumns) {
            // skip measure columns
            if (sqlDigest.metricColumns.contains(column)) {
                continue;
            }
            dimensions.add(column);
        }
    }

    private Cuboid identifyCuboid(Set<TblColRef> dimensions) {
        long cuboidID = 0;
        for (TblColRef column : dimensions) {
            int index = cubeDesc.getRowkey().getColumnBitIndex(column);
            cuboidID |= 1L << index;
        }
        return Cuboid.findById(cubeDesc, cuboidID);
    }

    private boolean isExactAggregation(Cuboid cuboid, Collection<TblColRef> groups, Set<TblColRef> othersD, Set<TblColRef> singleValuesD, Set<TblColRef> derivedPostAggregation) {
        boolean exact = true;

        if (cuboid.requirePostAggregation()) {
            exact = false;
            logger.info("exactAggregation is false because cuboid " + cuboid.getInputID() + "=> " + cuboid.getId());
        }

        // derived aggregation is bad, unless expanded columns are already in group by
        if (groups.containsAll(derivedPostAggregation) == false) {
            exact = false;
            logger.info("exactAggregation is false because derived column require post aggregation: " + derivedPostAggregation);
        }

        // other columns (from filter) is bad, unless they are ensured to have single value
        if (singleValuesD.containsAll(othersD) == false) {
            exact = false;
            logger.info("exactAggregation is false because some column not on group by: " + othersD //
                    + " (single value column: " + singleValuesD + ")");
        }

        if (exact) {
            logger.info("exactAggregation is true");
        }
        return exact;
    }

    private Set<TblColRef> expandDerived(Collection<TblColRef> cols, Set<TblColRef> derivedPostAggregation) {
        Set<TblColRef> expanded = Sets.newHashSet();
        for (TblColRef col : cols) {
            if (cubeDesc.isDerived(col)) {
                DeriveInfo hostInfo = cubeDesc.getHostInfo(col);
                for (TblColRef hostCol : hostInfo.columns) {
                    expanded.add(hostCol);
                    if (hostInfo.isOneToOne == false)
                        derivedPostAggregation.add(hostCol);
                }
            } else {
                expanded.add(col);
            }
        }
        return expanded;
    }

    @SuppressWarnings("unchecked")
    private Set<TblColRef> findSingleValueColumns(TupleFilter filter) {
        Collection<? extends TupleFilter> toCheck;
        if (filter instanceof CompareTupleFilter) {
            toCheck = Collections.singleton(filter);
        } else if (filter instanceof LogicalTupleFilter && filter.getOperator() == FilterOperatorEnum.AND) {
            toCheck = filter.getChildren();
        } else {
            return (Set<TblColRef>) Collections.EMPTY_SET;
        }

        Set<TblColRef> result = Sets.newHashSet();
        for (TupleFilter f : toCheck) {
            if (f instanceof CompareTupleFilter) {
                CompareTupleFilter compFilter = (CompareTupleFilter) f;
                // is COL=const ?
                if (compFilter.getOperator() == FilterOperatorEnum.EQ && compFilter.getValues().size() == 1 && compFilter.getColumn() != null) {
                    result.add(compFilter.getColumn());
                }
            }
        }

        // expand derived
        Set<TblColRef> resultD = Sets.newHashSet();
        for (TblColRef col : result) {
            if (cubeDesc.isDerived(col)) {
                DeriveInfo hostInfo = cubeDesc.getHostInfo(col);
                if (hostInfo.isOneToOne) {
                    for (TblColRef hostCol : hostInfo.columns) {
                        resultD.add(hostCol);
                    }
                }
                //if not one2one, it will be pruned
            } else {
                resultD.add(col);
            }
        }
        return resultD;
    }

    private void collectNonEvaluable(TupleFilter filter, Set<TblColRef> collector) {
        if (filter == null)
            return;

        if (filter.isEvaluable()) {
            for (TupleFilter child : filter.getChildren())
                collectNonEvaluable(child, collector);
        } else {
            collectColumnsRecursively(filter, collector);
        }
    }

    private void collectColumnsRecursively(TupleFilter filter, Set<TblColRef> collector) {
        if (filter instanceof ColumnTupleFilter) {
            collectColumns(((ColumnTupleFilter) filter).getColumn(), collector);
        }
        for (TupleFilter child : filter.getChildren()) {
            collectColumnsRecursively(child, collector);
        }
    }

    private void collectColumns(TblColRef col, Set<TblColRef> collector) {
        if (cubeDesc.isDerived(col)) {
            DeriveInfo hostInfo = cubeDesc.getHostInfo(col);
            for (TblColRef h : hostInfo.columns)
                collector.add(h);
        } else {
            collector.add(col);
        }
    }

    @SuppressWarnings("unchecked")
    private TupleFilter translateDerived(TupleFilter filter, Set<TblColRef> collector) {
        if (filter == null)
            return filter;

        if (filter instanceof CompareTupleFilter) {
            return translateDerivedInCompare((CompareTupleFilter) filter, collector);
        }

        List<TupleFilter> children = (List<TupleFilter>) filter.getChildren();
        List<TupleFilter> newChildren = Lists.newArrayListWithCapacity(children.size());
        boolean modified = false;
        for (TupleFilter child : children) {
            TupleFilter translated = translateDerived(child, collector);
            newChildren.add(translated);
            if (child != translated)
                modified = true;
        }
        if (modified) {
            filter = replaceChildren(filter, newChildren);
        }
        return filter;
    }

    private TupleFilter replaceChildren(TupleFilter filter, List<TupleFilter> newChildren) {
        if (filter instanceof LogicalTupleFilter) {
            LogicalTupleFilter r = new LogicalTupleFilter(filter.getOperator());
            r.addChildren(newChildren);
            return r;
        } else
            throw new IllegalStateException("Cannot replaceChildren on " + filter);
    }

    private TupleFilter translateDerivedInCompare(CompareTupleFilter compf, Set<TblColRef> collector) {
        if (compf.getColumn() == null || compf.getValues().isEmpty())
            return compf;

        TblColRef derived = compf.getColumn();
        if (cubeDesc.isDerived(derived) == false)
            return compf;

        DeriveInfo hostInfo = cubeDesc.getHostInfo(derived);
        CubeManager cubeMgr = CubeManager.getInstance(this.cubeInstance.getConfig());
        CubeSegment seg = cubeInstance.getLatestReadySegment();
        LookupStringTable lookup = cubeMgr.getLookupTable(seg, hostInfo.dimension);
        Pair<TupleFilter, Boolean> translated = DerivedFilterTranslator.translate(lookup, hostInfo, compf);
        TupleFilter translatedFilter = translated.getFirst();
        boolean loosened = translated.getSecond();
        if (loosened) {
            collectColumnsRecursively(compf, collector);
        }
        return translatedFilter;
    }

    private List<RowValueDecoder> translateAggregation(HBaseMappingDesc hbaseMapping, Collection<FunctionDesc> metrics, //
            StorageContext context) {
        Map<HBaseColumnDesc, RowValueDecoder> codecMap = Maps.newHashMap();
        for (FunctionDesc aggrFunc : metrics) {
            Collection<HBaseColumnDesc> hbCols = hbaseMapping.findHBaseColumnByFunction(aggrFunc);
            if (hbCols.isEmpty()) {
                throw new IllegalStateException("can't find HBaseColumnDesc for function " + aggrFunc.getFullExpression());
            }
            HBaseColumnDesc bestHBCol = null;
            int bestIndex = -1;
            for (HBaseColumnDesc hbCol : hbCols) {
                bestHBCol = hbCol;
                bestIndex = hbCol.findMeasureIndex(aggrFunc);
                MeasureDesc measure = hbCol.getMeasures()[bestIndex];
                // criteria for holistic measure: Exact Aggregation && Exact Cuboid
                if (measure.isHolisticCountDistinct() && context.isExactAggregation()) {
                    logger.info("Holistic count distinct chosen for " + aggrFunc);
                    break;
                }
            }

            RowValueDecoder codec = codecMap.get(bestHBCol);
            if (codec == null) {
                codec = new RowValueDecoder(bestHBCol);
                codecMap.put(bestHBCol, codec);
            }
            codec.setIndex(bestIndex);
        }
        return new ArrayList<RowValueDecoder>(codecMap.values());
    }

    private TupleFilter flattenToOrAndFilter(TupleFilter filter) {
        if (filter == null)
            return null;

        TupleFilter flatFilter = filter.flatFilter();

        // normalize to OR-AND filter
        if (flatFilter.getOperator() == FilterOperatorEnum.AND) {
            LogicalTupleFilter f = new LogicalTupleFilter(FilterOperatorEnum.OR);
            f.addChild(flatFilter);
            flatFilter = f;
        }

        if (flatFilter.getOperator() != FilterOperatorEnum.OR)
            throw new IllegalStateException();

        return flatFilter;
    }

    private List<HBaseKeyRange> buildScanRanges(TupleFilter flatFilter, Collection<TblColRef> dimensionColumns) {

        List<HBaseKeyRange> result = Lists.newArrayList();

        // build row key range for each cube segment
        for (CubeSegment cubeSeg : cubeInstance.getSegments(SegmentStatusEnum.READY)) {

            // consider derived (lookup snapshot), filter on dimension may
            // differ per segment
            List<Collection<ColumnValueRange>> orAndDimRanges = translateToOrAndDimRanges(flatFilter, cubeSeg);
            if (orAndDimRanges == null) { // has conflict
                continue;
            }

            List<HBaseKeyRange> scanRanges = Lists.newArrayListWithCapacity(orAndDimRanges.size());
            for (Collection<ColumnValueRange> andDimRanges : orAndDimRanges) {
                HBaseKeyRange rowKeyRange = new HBaseKeyRange(dimensionColumns, andDimRanges, cubeSeg, cubeDesc);
                scanRanges.add(rowKeyRange);
            }

            List<HBaseKeyRange> mergedRanges = mergeOverlapRanges(scanRanges);
            mergedRanges = mergeTooManyRanges(mergedRanges);
            result.addAll(mergedRanges);
        }

        dropUnhitSegments(result);

        return result;
    }

    private List<Collection<ColumnValueRange>> translateToOrAndDimRanges(TupleFilter flatFilter, CubeSegment cubeSegment) {
        List<Collection<ColumnValueRange>> result = Lists.newArrayList();

        if (flatFilter == null) {
            result.add(Collections.<ColumnValueRange> emptyList());
            return result;
        }

        for (TupleFilter andFilter : flatFilter.getChildren()) {
            if (andFilter.getOperator() != FilterOperatorEnum.AND) {
                throw new IllegalStateException("Filter should be AND instead of " + andFilter);
            }

            Collection<ColumnValueRange> andRanges = translateToAndDimRanges(andFilter.getChildren(), cubeSegment);

            result.add(andRanges);
        }

        return preprocessConstantConditions(result);
    }

    private List<Collection<ColumnValueRange>> preprocessConstantConditions(List<Collection<ColumnValueRange>> orAndRanges) {
        boolean globalAlwaysTrue = false;
        Iterator<Collection<ColumnValueRange>> iterator = orAndRanges.iterator();
        while (iterator.hasNext()) {
            Collection<ColumnValueRange> andRanges = iterator.next();
            Iterator<ColumnValueRange> iterator2 = andRanges.iterator();
            boolean hasAlwaysFalse = false;
            while (iterator2.hasNext()) {
                ColumnValueRange range = iterator2.next();
                if (range.satisfyAll())
                    iterator2.remove();
                else if (range.satisfyNone())
                    hasAlwaysFalse = true;
            }
            if (hasAlwaysFalse) {
                iterator.remove();
            } else if (andRanges.isEmpty()) {
                globalAlwaysTrue = true;
                break;
            }
        }
        if (globalAlwaysTrue) {
            orAndRanges.clear();
            orAndRanges.add(Collections.<ColumnValueRange> emptyList());
        }
        return orAndRanges;
    }

    private Collection<ColumnValueRange> translateToAndDimRanges(List<? extends TupleFilter> andFilters, CubeSegment cubeSegment) {
        Map<TblColRef, ColumnValueRange> rangeMap = new HashMap<TblColRef, ColumnValueRange>();
        for (TupleFilter filter : andFilters) {
            if ((filter instanceof CompareTupleFilter) == false) {
                continue;
            }

            CompareTupleFilter comp = (CompareTupleFilter) filter;
            if (comp.getColumn() == null) {
                continue;
            }

            @SuppressWarnings("unchecked")
            ColumnValueRange range = new ColumnValueRange(comp.getColumn(), (Collection<String>) comp.getValues(), comp.getOperator());
            andMerge(range, rangeMap);

        }
        return rangeMap.values();
    }

    private void andMerge(ColumnValueRange range, Map<TblColRef, ColumnValueRange> rangeMap) {
        ColumnValueRange columnRange = rangeMap.get(range.getColumn());
        if (columnRange == null) {
            rangeMap.put(range.getColumn(), range);
        } else {
            columnRange.andMerge(range);
        }
    }

    private List<HBaseKeyRange> mergeOverlapRanges(List<HBaseKeyRange> keyRanges) {
        if (keyRanges.size() <= 1) {
            return keyRanges;
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Merging key range from " + keyRanges.size());
        }

        // sort ranges by start key
        Collections.sort(keyRanges);

        // merge the overlap range
        List<HBaseKeyRange> mergedRanges = new LinkedList<HBaseKeyRange>();
        int beginIndex = 0;
        byte[] maxStopKey = keyRanges.get(0).getStopKey();
        for (int index = 0; index < keyRanges.size(); index++) {
            HBaseKeyRange keyRange = keyRanges.get(index);
            if (Bytes.compareTo(maxStopKey, keyRange.getStartKey()) < 0) {
                // merge the current key ranges
                HBaseKeyRange mergedRange = mergeKeyRange(keyRanges, beginIndex, index - 1);
                mergedRanges.add(mergedRange);
                // start new merge
                beginIndex = index;
            }
            if (Bytes.compareTo(maxStopKey, keyRange.getStopKey()) < 0) {
                // update the stop key
                maxStopKey = keyRange.getStopKey();
            }
        }
        // merge last range
        HBaseKeyRange mergedRange = mergeKeyRange(keyRanges, beginIndex, keyRanges.size() - 1);
        mergedRanges.add(mergedRange);
        if (logger.isDebugEnabled()) {
            logger.debug("Merging key range to " + mergedRanges.size());
        }
        return mergedRanges;
    }

    private HBaseKeyRange mergeKeyRange(List<HBaseKeyRange> keyRanges, int from, int to) {
        HBaseKeyRange keyRange = keyRanges.get(from);
        int mergeSize = to - from + 1;
        if (mergeSize > 1) {
            // merge range from mergeHeader to i - 1
            CubeSegment cubeSegment = keyRange.getCubeSegment();
            Cuboid cuboid = keyRange.getCuboid();
            byte[] startKey = keyRange.getStartKey();
            byte[] stopKey = keyRange.getStopKey();
            long partitionColumnStartDate = Long.MAX_VALUE;
            long partitionColumnEndDate = 0;
            List<Pair<byte[], byte[]>> newFuzzyKeys = new ArrayList<Pair<byte[], byte[]>>(mergeSize);
            List<Collection<ColumnValueRange>> newFlatOrAndFilter = Lists.newLinkedList();

            boolean hasNonFuzzyRange = false;
            for (int k = from; k <= to; k++) {
                HBaseKeyRange nextRange = keyRanges.get(k);
                hasNonFuzzyRange = hasNonFuzzyRange || nextRange.getFuzzyKeys().isEmpty();
                newFuzzyKeys.addAll(nextRange.getFuzzyKeys());
                newFlatOrAndFilter.addAll(nextRange.getFlatOrAndFilter());
                if (Bytes.compareTo(stopKey, nextRange.getStopKey()) < 0) {
                    stopKey = nextRange.getStopKey();
                }
                if (nextRange.getPartitionColumnStartDate() > 0 && nextRange.getPartitionColumnStartDate() < partitionColumnStartDate) {
                    partitionColumnStartDate = nextRange.getPartitionColumnStartDate();
                }
                if (nextRange.getPartitionColumnEndDate() < Long.MAX_VALUE && nextRange.getPartitionColumnEndDate() > partitionColumnEndDate) {
                    partitionColumnEndDate = nextRange.getPartitionColumnEndDate();
                }
            }

            // if any range is non-fuzzy, then all fuzzy keys must be cleared
            if (hasNonFuzzyRange) {
                newFuzzyKeys.clear();
            }

            partitionColumnStartDate = (partitionColumnStartDate == Long.MAX_VALUE) ? 0 : partitionColumnStartDate;
            partitionColumnEndDate = (partitionColumnEndDate == 0) ? Long.MAX_VALUE : partitionColumnEndDate;
            keyRange = new HBaseKeyRange(cubeSegment, cuboid, startKey, stopKey, newFuzzyKeys, newFlatOrAndFilter, partitionColumnStartDate, partitionColumnEndDate);
        }
        return keyRange;
    }

    private List<HBaseKeyRange> mergeTooManyRanges(List<HBaseKeyRange> keyRanges) {
        if (keyRanges.size() < MERGE_KEYRANGE_THRESHOLD) {
            return keyRanges;
        }
        // TODO: check the distance between range. and merge the large distance range
        List<HBaseKeyRange> mergedRanges = new LinkedList<HBaseKeyRange>();
        HBaseKeyRange mergedRange = mergeKeyRange(keyRanges, 0, keyRanges.size() - 1);
        mergedRanges.add(mergedRange);
        return mergedRanges;
    }

    private void dropUnhitSegments(List<HBaseKeyRange> scans) {
        if (cubeDesc.getModel().getPartitionDesc().isPartitioned()) {
            Iterator<HBaseKeyRange> iterator = scans.iterator();
            while (iterator.hasNext()) {
                HBaseKeyRange scan = iterator.next();
                if (scan.hitSegment() == false) {
                    iterator.remove();
                }
            }
        }
    }

    private void setThreshold(Collection<TblColRef> dimensions, List<RowValueDecoder> valueDecoders, StorageContext context) {
        if (RowValueDecoder.hasMemHungryCountDistinct(valueDecoders) == false) {
            return;
        }

        int rowSizeEst = dimensions.size() * 3;
        for (RowValueDecoder decoder : valueDecoders) {
            MeasureDesc[] measures = decoder.getMeasures();
            BitSet projectionIndex = decoder.getProjectionIndex();
            for (int i = projectionIndex.nextSetBit(0); i >= 0; i = projectionIndex.nextSetBit(i + 1)) {
                FunctionDesc func = measures[i].getFunction();
                rowSizeEst += func.getReturnDataType().getSpaceEstimate();
            }
        }

        long rowEst = MEM_BUDGET_PER_QUERY / rowSizeEst;
        context.setThreshold((int) rowEst);
    }

    private void setLimit(TupleFilter filter, StorageContext context) {
        boolean goodAggr = context.isExactAggregation();
        boolean goodFilter = filter == null || (TupleFilter.isEvaluableRecursively(filter) && context.isCoprocessorEnabled());
        boolean goodSort = context.hasSort() == false;
        if (goodAggr && goodFilter && goodSort) {
            logger.info("Enable limit " + context.getLimit());
            context.enableLimit();
        }
    }

    private void setCoprocessor(Set<TblColRef> groupsCopD, List<RowValueDecoder> valueDecoders, StorageContext context) {
        ObserverEnabler.enableCoprocessorIfBeneficial(cubeInstance, groupsCopD, valueDecoders, context);
    }

}

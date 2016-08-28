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

package org.apache.kylin.storage.hbase.cube.v1;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hbase.client.HConnection;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.ShardingHash;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.RawQueryLastHacker;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.cube.model.HBaseMappingDesc;
import org.apache.kylin.cube.model.RowKeyDesc;
import org.apache.kylin.cube.model.CubeDesc.DeriveInfo;
import org.apache.kylin.dict.lookup.LookupStringTable;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter.FilterOperatorEnum;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.storage.IStorageQuery;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.hbase.HBaseConnection;
import org.apache.kylin.storage.hbase.cube.v1.coprocessor.observer.ObserverEnabler;
import org.apache.kylin.storage.hbase.steps.RowValueDecoder;
import org.apache.kylin.storage.translate.ColumnValueRange;
import org.apache.kylin.storage.translate.DerivedFilterTranslator;
import org.apache.kylin.storage.translate.HBaseKeyRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

@SuppressWarnings("unused")
public class CubeStorageQuery implements IStorageQuery {

    private static final Logger logger = LoggerFactory.getLogger(CubeStorageQuery.class);

    private static final int MERGE_KEYRANGE_THRESHOLD = 100;

    private final CubeInstance cubeInstance;
    private final CubeDesc cubeDesc;
    private final String uuid;

    public CubeStorageQuery(CubeInstance cube) {
        this.cubeInstance = cube;
        this.cubeDesc = cube.getDescriptor();
        this.uuid = cube.getUuid();
    }

    @Override
    public ITupleIterator search(StorageContext context, SQLDigest sqlDigest, TupleInfo returnTupleInfo) {
        //cope with queries with no aggregations
        RawQueryLastHacker.hackNoAggregations(sqlDigest, cubeDesc);

        // Customized measure taking effect: e.g. allow custom measures to help raw queries
        notifyBeforeStorageQuery(sqlDigest);

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

        // identify cuboid
        Set<TblColRef> dimensionsD = Sets.newHashSet();
        dimensionsD.addAll(groupsD);
        dimensionsD.addAll(othersD);
        Cuboid cuboid = identifyCuboid(dimensionsD, metrics);
        context.setCuboid(cuboid);

        // isExactAggregation? meaning: tuples returned from storage requires no further aggregation in query engine
        Set<TblColRef> singleValuesD = findSingleValueColumns(filter);
        boolean isExactAggregation = isExactAggregation(cuboid, groupsD, othersD, singleValuesD, derivedPostAggregation);
        context.setExactAggregation(isExactAggregation);

        // translate filter for scan range and compose returning groups for coprocessor, note:
        // - columns on non-evaluatable filter have to return
        // - columns on loosened filter (due to derived translation) have to return
        Set<TblColRef> groupsCopD = Sets.newHashSet(groupsD);
        collectNonEvaluable(filter, groupsCopD);
        TupleFilter filterD = translateDerived(filter, groupsCopD);

        // translate filter into segment scan ranges
        List<HBaseKeyRange> scans = buildScanRanges(flattenToOrAndFilter(filterD), dimensionsD);

        // check involved measures, build value decoder for each each family:column
        List<RowValueDecoder> valueDecoders = translateAggregation(cubeDesc.getHbaseMapping(), metrics, context);

        // memory hungry distinct count are pushed down to coprocessor, no need to set threshold any more
        setThreshold(dimensionsD, valueDecoders, context); // set cautious threshold to prevent out of memory
        setCoprocessor(groupsCopD, valueDecoders, context); // enable coprocessor if beneficial
        setLimit(filter, context);

        HConnection conn = HBaseConnection.get(context.getConnUrl());

        // notice we're passing filterD down to storage instead of flatFilter
        return new SerializedHBaseTupleIterator(conn, scans, cubeInstance, dimensionsD, filterD, groupsCopD, valueDecoders, context, returnTupleInfo);
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

    private Cuboid identifyCuboid(Set<TblColRef> dimensions, Collection<FunctionDesc> metrics) {
        for (FunctionDesc metric : metrics) {
            if (metric.getMeasureType().onlyAggrInBaseCuboid())
                return Cuboid.getBaseCuboid(cubeDesc);
        }

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
            if (cubeDesc.hasHostColumn(col)) {
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
            if (cubeDesc.isExtendedColumn(col)) {
                throw new CubeDesc.CannotFilterExtendedColumnException(col);
            }

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
        if (filter == null)
            return;

        if (filter instanceof ColumnTupleFilter) {
            collectColumns(((ColumnTupleFilter) filter).getColumn(), collector);
        }
        for (TupleFilter child : filter.getChildren()) {
            collectColumnsRecursively(child, collector);
        }
    }

    private void collectColumns(TblColRef col, Set<TblColRef> collector) {
        if (cubeDesc.isExtendedColumn(col)) {
            throw new CubeDesc.CannotFilterExtendedColumnException(col);
        }

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
        if (cubeDesc.isExtendedColumn(derived)) {
            throw new CubeDesc.CannotFilterExtendedColumnException(derived);
        }
        if (cubeDesc.isDerived(derived) == false) {
            return compf;
        }

        DeriveInfo hostInfo = cubeDesc.getHostInfo(derived);
        CubeManager cubeMgr = CubeManager.getInstance(this.cubeInstance.getConfig());
        CubeSegment seg = cubeInstance.getLatestReadySegment();
        LookupStringTable lookup = cubeMgr.getLookupTable(seg, hostInfo.dimension);
        Pair<TupleFilter, Boolean> translated = DerivedFilterTranslator.translate(lookup, hostInfo, compf);
        TupleFilter translatedFilter = translated.getFirst();
        boolean loosened = translated.getSecond();
        if (loosened) {
            collectColumnsRecursively(translatedFilter, collector);
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
                bestIndex = hbCol.findMeasure(aggrFunc);
                // we used to prefer specific measure over another (holistic distinct count), now it's gone
                break;
            }

            RowValueDecoder codec = codecMap.get(bestHBCol);
            if (codec == null) {
                codec = new RowValueDecoder(bestHBCol);
                codecMap.put(bestHBCol, codec);
            }
            codec.setProjectIndex(bestIndex);
        }
        return new ArrayList<RowValueDecoder>(codecMap.values());
    }

    //check TupleFilter.flatFilter's comment
    private TupleFilter flattenToOrAndFilter(TupleFilter filter) {
        if (filter == null)
            return null;

        // core
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

        logger.info("Current cubeInstance is " + cubeInstance + " with " + cubeInstance.getSegments().size() + " segs in all");
        List<CubeSegment> segs = cubeInstance.getSegments(SegmentStatusEnum.READY);
        logger.info("READY segs count: " + segs.size());

        // build row key range for each cube segment
        StringBuilder sb = new StringBuilder("hbasekeyrange trace: ");
        for (CubeSegment cubeSeg : segs) {

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

            //log
            sb.append(scanRanges.size() + "=(mergeoverlap)>");

            List<HBaseKeyRange> mergedRanges = mergeOverlapRanges(scanRanges);

            //log
            sb.append(mergedRanges.size() + "=(mergetoomany)>");

            mergedRanges = mergeTooManyRanges(mergedRanges);

            //log
            sb.append(mergedRanges.size() + ",");

            result.addAll(mergedRanges);
        }
        logger.info(sb.toString());

        logger.info("hbasekeyrange count: " + result.size());

        dropUnhitSegments(result);
        logger.info("hbasekeyrange count after dropping unhit :" + result.size());

        //TODO: should use LazyRowKeyEncoder.getRowKeysDifferentShards like CubeHBaseRPC, not do so because v1 query engine is retiring. not worth changing it
        if (cubeDesc.isEnableSharding()) {
            result = duplicateRangeByShard(result);
        }
        logger.info("hbasekeyrange count after dropping duplicatebyshard :" + result.size());

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

            if (andRanges != null) {
                result.add(andRanges);
            }
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

    // return empty collection to mean true; return null to mean false
    @SuppressWarnings("unchecked")
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

            ColumnValueRange range = new ColumnValueRange(comp.getColumn(), (Collection<String>) comp.getValues(), comp.getOperator());
            andMerge(range, rangeMap);
        }

        // a little pre-evaluation to remove invalid EQ/IN values and round start/end according to dictionary
        RowKeyDesc rowkey = cubeSegment.getCubeDesc().getRowkey();
        Iterator<ColumnValueRange> it = rangeMap.values().iterator();
        while (it.hasNext()) {
            ColumnValueRange range = it.next();
            if (rowkey.isUseDictionary(range.getColumn())) {
                range.preEvaluateWithDict((Dictionary<String>) cubeSegment.getDictionary(range.getColumn()));
            }
            if (range.satisfyAll())
                it.remove();
            else if (range.satisfyNone())
                return null;
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

            TreeSet<Pair<byte[], byte[]>> newFuzzyKeys = new TreeSet<>(new Comparator<Pair<byte[], byte[]>>() {
                @Override
                public int compare(Pair<byte[], byte[]> o1, Pair<byte[], byte[]> o2) {
                    int partialResult = Bytes.compareTo(o1.getFirst(), o2.getFirst());
                    if (partialResult != 0) {
                        return partialResult;
                    } else {
                        return Bytes.compareTo(o1.getSecond(), o2.getSecond());
                    }
                }
            });
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
            keyRange = new HBaseKeyRange(cubeSegment, cuboid, startKey, stopKey, Lists.newArrayList(newFuzzyKeys), newFlatOrAndFilter, partitionColumnStartDate, partitionColumnEndDate);
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

    private List<HBaseKeyRange> duplicateRangeByShard(List<HBaseKeyRange> scans) {
        List<HBaseKeyRange> ret = Lists.newArrayList();

        for (HBaseKeyRange scan : scans) {
            CubeSegment segment = scan.getCubeSegment();

            byte[] startKey = scan.getStartKey();
            byte[] stopKey = scan.getStopKey();

            short cuboidShardNum = segment.getCuboidShardNum(scan.getCuboid().getId());
            short cuboidShardBase = segment.getCuboidBaseShard(scan.getCuboid().getId());
            for (short i = 0; i < cuboidShardNum; ++i) {
                short newShard = ShardingHash.normalize(cuboidShardBase, i, segment.getTotalShards(scan.getCuboid().getId()));
                byte[] newStartKey = duplicateKeyAndChangeShard(newShard, startKey);
                byte[] newStopKey = duplicateKeyAndChangeShard(newShard, stopKey);
                HBaseKeyRange newRange = new HBaseKeyRange(segment, scan.getCuboid(), newStartKey, newStopKey, //
                        scan.getFuzzyKeys(), scan.getFlatOrAndFilter(), scan.getPartitionColumnStartDate(), scan.getPartitionColumnEndDate());
                ret.add(newRange);
            }
        }

        Collections.sort(ret, new Comparator<HBaseKeyRange>() {
            @Override
            public int compare(HBaseKeyRange o1, HBaseKeyRange o2) {
                return Bytes.compareTo(o1.getStartKey(), o2.getStartKey());
            }
        });

        return ret;
    }

    private byte[] duplicateKeyAndChangeShard(short newShard, byte[] bytes) {
        byte[] ret = Arrays.copyOf(bytes, bytes.length);
        BytesUtil.writeShort(newShard, ret, 0, RowConstants.ROWKEY_SHARDID_LEN);
        return ret;
    }

    private void setThreshold(Collection<TblColRef> dimensions, List<RowValueDecoder> valueDecoders, StorageContext context) {
        if (RowValueDecoder.hasMemHungryMeasures(valueDecoders) == false) {
            return;
        }

        int rowSizeEst = dimensions.size() * 3;
        for (RowValueDecoder decoder : valueDecoders) {
            MeasureDesc[] measures = decoder.getMeasures();
            BitSet projectionIndex = decoder.getProjectionIndex();
            for (int i = projectionIndex.nextSetBit(0); i >= 0; i = projectionIndex.nextSetBit(i + 1)) {
                FunctionDesc func = measures[i].getFunction();
                // FIXME getStorageBytesEstimate() is not appropriate as here we want size in memory (not in storage)
                rowSizeEst += func.getReturnDataType().getStorageBytesEstimate();
            }
        }

        long rowEst = this.cubeInstance.getConfig().getQueryMemBudget() / rowSizeEst;
        if (rowEst > 0) {
            logger.info("Memory budget is set to: " + rowEst);
            context.setThreshold((int) rowEst);
        } else {
            logger.info("Memory budget is not set.");
        }
    }

    private void setLimit(TupleFilter filter, StorageContext context) {
        boolean goodAggr = context.isExactAggregation();
        boolean goodFilter = filter == null || (TupleFilter.isEvaluableRecursively(filter) && context.isCoprocessorEnabled());
        boolean goodSort = !context.hasSort();
        if (goodAggr && goodFilter && goodSort) {
            logger.info("Enable limit " + context.getLimit());
            context.enableLimit();
        }
    }

    private void setCoprocessor(Set<TblColRef> groupsCopD, List<RowValueDecoder> valueDecoders, StorageContext context) {
        ObserverEnabler.enableCoprocessorIfBeneficial(cubeInstance, groupsCopD, valueDecoders, context);
    }

    private void notifyBeforeStorageQuery(SQLDigest sqlDigest) {

        Map<String, List<MeasureDesc>> map = Maps.newHashMap();
        for (MeasureDesc measure : cubeDesc.getMeasures()) {
            MeasureType<?> measureType = measure.getFunction().getMeasureType();

            String key = measureType.getClass().getCanonicalName();
            List<MeasureDesc> temp = null;
            if ((temp = map.get(key)) != null) {
                temp.add(measure);
            } else {
                map.put(key, Lists.<MeasureDesc> newArrayList(measure));
            }
        }

        for (List<MeasureDesc> sublist : map.values()) {
            sublist.get(0).getFunction().getMeasureType().adjustSqlDigest(sublist, sqlDigest);
        }
    }

}

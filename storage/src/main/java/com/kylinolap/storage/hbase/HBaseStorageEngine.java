/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kylinolap.storage.hbase;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.kylinolap.common.persistence.HBaseConnection;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.cube.CubeSegment;
import com.kylinolap.cube.CubeSegmentStatusEnum;
import com.kylinolap.cube.cuboid.Cuboid;
import com.kylinolap.cube.kv.RowValueDecoder;
import com.kylinolap.dict.lookup.LookupStringTable;
import com.kylinolap.metadata.model.cube.*;
import com.kylinolap.metadata.model.cube.CubeDesc.DeriveInfo;
import com.kylinolap.storage.IStorageEngine;
import com.kylinolap.storage.StorageContext;
import com.kylinolap.storage.filter.CompareTupleFilter;
import com.kylinolap.storage.filter.ExtractTupleFilter;
import com.kylinolap.storage.filter.LogicalTupleFilter;
import com.kylinolap.storage.filter.TupleFilter;
import com.kylinolap.storage.filter.TupleFilter.FilterOperatorEnum;
import com.kylinolap.storage.tuple.TupleIterator;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author xjiang
 */
public class HBaseStorageEngine implements IStorageEngine {

    private static final Logger logger = LoggerFactory.getLogger(HBaseStorageEngine.class);

    private static final int MERGE_KEYRANGE_THRESHOLD = 7;
    private static final long MEM_BUDGET_PER_QUERY = 3L * 1024 * 1024 * 1024; // 3G

    private final CubeInstance cubeInstance;
    private final CubeDesc cubeDesc;

    public HBaseStorageEngine(CubeInstance cube) {
        this.cubeInstance = cube;
        this.cubeDesc = cube.getDescriptor();
    }

    @Override
    public TupleIterator search(Collection<TblColRef> dimensions, TupleFilter filter,
                                Collection<TblColRef> groups, Collection<FunctionDesc> metrics, StorageContext context) {

        // flatten to OR-AND filter, (A AND B AND ..) OR (C AND D AND ..) OR ..
        TupleFilter flatFilter = flattenToOrAndFilter(filter);

        // translate filter into segment scan ranges
        List<HBaseKeyRange> scans = translateFilter(flatFilter, dimensions);

        // post process filter: remove unused segment & set limit
        postProcessFilter(scans, flatFilter, context);

        // check involved measures, build value decoder for each each family:column
        List<RowValueDecoder> valueDecoders =
                translateAggregation(cubeDesc.getHBaseMapping(), metrics, scans, context);

        setThreshold(dimensions, valueDecoders, context);

        HConnection conn = HBaseConnection.get(context.getConnUrl());
        return new SerializedHBaseTupleIterator(conn, scans, cubeInstance, dimensions, filter, groups,
                valueDecoders, context);
    }

    private void setThreshold(Collection<TblColRef> dimensions, List<RowValueDecoder> valueDecoders,
                              StorageContext context) {
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

    private List<RowValueDecoder> translateAggregation(HBaseMappingDesc hbaseMapping,
                                                       Collection<FunctionDesc> aggregations, List<HBaseKeyRange> scans, StorageContext context) {
        Map<HBaseColumnDesc, RowValueDecoder> codecMap = Maps.newHashMap();
        for (FunctionDesc aggrFunc : aggregations) {
            Collection<HBaseColumnDesc> hbCols = hbaseMapping.findHBaseColumnByFunction(aggrFunc);
            if (hbCols.isEmpty()) {
                throw new IllegalStateException("can't find HBaseColumnDesc for function "
                        + aggrFunc.getFullExpression());
            }
            HBaseColumnDesc bestHBCol = null;
            int bestIndex = -1;
            for (HBaseColumnDesc hbCol : hbCols) {
                bestHBCol = hbCol;
                bestIndex = hbCol.findMeasureIndex(aggrFunc);
                MeasureDesc measure = hbCol.getMeasures()[bestIndex];
                // criteria for holistic measure: Exact Aggregation && Exact Cuboid
                if (measure.isHolisticCountDistinct() && context.requireNoPostAggregation()) {
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

    private List<HBaseKeyRange> translateFilter(TupleFilter flatFilter, Collection<TblColRef> dimensionColumns) {

        List<HBaseKeyRange> result = Lists.newArrayList();

        // build row key range for each cube segment
        for (CubeSegment cubeSeg : cubeInstance.getSegments(CubeSegmentStatusEnum.READY)) {

            // consider derived (lookup snapshot), filter on dimension may differ per segment
            List<Collection<ColumnValueRange>> orAndDimRanges =
                    translateToOrAndDimRanges(flatFilter, cubeSeg);
            if (orAndDimRanges == null) { // has conflict
                continue;
            }

            List<HBaseKeyRange> scanRanges = Lists.newArrayListWithCapacity(orAndDimRanges.size());
            for (Collection<ColumnValueRange> andDimRanges : orAndDimRanges) {
                HBaseKeyRange rowKeyRange =
                        new HBaseKeyRange(dimensionColumns, andDimRanges, cubeSeg, cubeDesc);
                scanRanges.add(rowKeyRange);
            }

            List<HBaseKeyRange> mergedRanges = mergeOverlapRanges(scanRanges);
            mergedRanges = mergeTooManyRanges(mergedRanges);
            result.addAll(mergedRanges);
        }

        return result;
    }

    private List<Collection<ColumnValueRange>> translateToOrAndDimRanges(TupleFilter flatFilter,
                                                                         CubeSegment cubeSegment) {
        List<Collection<ColumnValueRange>> result = Lists.newArrayList();

        if (flatFilter == null) {
            result.add(Collections.<ColumnValueRange>emptyList());
            return result;
        }

        for (TupleFilter andFilter : flatFilter.getChildren()) {
            if (andFilter.getOperator() != FilterOperatorEnum.AND) {
                throw new IllegalStateException("Filter should be AND instead of " + andFilter);
            }

            Collection<ColumnValueRange> andRanges =
                    translateToAndDimRanges(andFilter.getChildren(), cubeSegment);

            result.add(andRanges);
        }

        return preprocessConstantConditions(result);
    }

    private List<Collection<ColumnValueRange>> preprocessConstantConditions(
            List<Collection<ColumnValueRange>> orAndRanges) {
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
            orAndRanges.add(Collections.<ColumnValueRange>emptyList());
        }
        return orAndRanges;
    }

    private Collection<ColumnValueRange> translateToAndDimRanges(List<? extends TupleFilter> andFilters,
                                                                 CubeSegment cubeSegment) {
        Map<TblColRef, ColumnValueRange> rangeMap = new HashMap<TblColRef, ColumnValueRange>();
        for (TupleFilter filter : andFilters) {
            if (filter instanceof ExtractTupleFilter) {
                continue;
            }

            CompareTupleFilter comp = (CompareTupleFilter) filter;
            if (comp.getColumn() == null) {
                continue;
            }

            ColumnValueRange range =
                    new ColumnValueRange(comp.getColumn(), comp.getValues(), comp.getOperator());
            TblColRef column = comp.getColumn();
            if (cubeDesc.isDerived(column)) {
                DeriveInfo hostInfo = cubeDesc.getHostInfo(column);
                ColumnValueRange[] hostRanges = mapDerivedToHostRanges(cubeSegment, range, hostInfo);
                for (ColumnValueRange hostRange : hostRanges) {
                    andMerge(hostRange, rangeMap);
                }
            } else {
                andMerge(range, rangeMap);
            }

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

    private ColumnValueRange[] mapDerivedToHostRanges(CubeSegment cubeSegment, ColumnValueRange derivedRange,
                                                      DeriveInfo hostInfo) {

        TblColRef[] hostCols = hostInfo.columns;
        ColumnValueRange[] result = new ColumnValueRange[hostCols.length];

        switch (hostInfo.type) {
            case LOOKUP:
                CubeManager cubeMgr = CubeManager.getInstance(this.cubeInstance.getConfig());
                LookupStringTable lookup = cubeMgr.getLookupTable(cubeSegment, hostInfo.dimension);
                TblColRef[] pkCols = hostInfo.dimension.getJoin().getPrimaryKeyColumns();
                for (int i = 0; i < hostCols.length; i++) {
                    TblColRef hostCol = hostCols[i];
                    TblColRef pkCol = pkCols[i];
                    if (derivedRange.getEqualValues() != null) {
                        Set<String> values =
                                lookup.mapValues(derivedRange.getColumn().getName(),
                                        derivedRange.getEqualValues(), pkCol.getName());
                        result[i] = new ColumnValueRange(hostCol, values, FilterOperatorEnum.IN);
                    } else {
                        Pair<String, String> pair =
                                lookup.mapRange(derivedRange.getColumn().getName(), derivedRange.getBeginValue(),
                                        derivedRange.getEndValue(), pkCol.getName());
                        if (pair == null)
                            result[i] = new ColumnValueRange(hostCol, "1", "0", null); // always false
                        else
                            result[i] = new ColumnValueRange(hostCol, pair.getFirst(), pair.getSecond(), null);
                    }
                }
                break;
            case PK_FK:
                assert hostCols.length == 1;
                result[0] =
                        new ColumnValueRange(hostCols[0], derivedRange.getBeginValue(),
                                derivedRange.getEndValue(), derivedRange.getEqualValues());
                break;
            default:
                throw new IllegalArgumentException();
        }

        return result;
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

            for (int k = from; k <= to; k++) {
                HBaseKeyRange nextRange = keyRanges.get(k);
                newFuzzyKeys.addAll(nextRange.getFuzzyKeys());
                newFlatOrAndFilter.addAll(nextRange.getFlatOrAndFilter());
                if (Bytes.compareTo(stopKey, nextRange.getStopKey()) < 0) {
                    stopKey = nextRange.getStopKey();
                }
                if (nextRange.getPartitionColumnStartDate() > 0
                        && nextRange.getPartitionColumnStartDate() < partitionColumnStartDate) {
                    partitionColumnStartDate = nextRange.getPartitionColumnStartDate();
                }
                if (nextRange.getPartitionColumnEndDate() < Long.MAX_VALUE
                        && nextRange.getPartitionColumnEndDate() > partitionColumnEndDate) {
                    partitionColumnEndDate = nextRange.getPartitionColumnEndDate();
                }
            }

            partitionColumnStartDate =
                    (partitionColumnStartDate == Long.MAX_VALUE) ? 0 : partitionColumnStartDate;
            partitionColumnEndDate = (partitionColumnEndDate == 0) ? Long.MAX_VALUE : partitionColumnEndDate;
            keyRange =

                    new HBaseKeyRange(cubeSegment, cuboid, startKey, stopKey, newFuzzyKeys,
                            newFlatOrAndFilter, partitionColumnStartDate, partitionColumnEndDate);
        }
        return keyRange;
    }

    private List<HBaseKeyRange> mergeTooManyRanges(List<HBaseKeyRange> keyRanges) {
        if (keyRanges.size() < MERGE_KEYRANGE_THRESHOLD) {
            return keyRanges;
        }
        //TODO: check the distance between range. and merge the large distance range
        List<HBaseKeyRange> mergedRanges = new LinkedList<HBaseKeyRange>();
        HBaseKeyRange mergedRange = mergeKeyRange(keyRanges, 0, keyRanges.size() - 1);
        mergedRanges.add(mergedRange);
        return mergedRanges;
    }

    private void postProcessFilter(List<HBaseKeyRange> scans, TupleFilter flatFilter, StorageContext context) {
        if (cubeDesc.getCubePartitionDesc().getPartitionDateColumn() != null) {
            Iterator<HBaseKeyRange> iterator = scans.iterator();
            while (iterator.hasNext()) {
                HBaseKeyRange scan = iterator.next();
                if (scan.hitSegment() == false) {
                    iterator.remove();
                }
            }
        }

        if (!scans.isEmpty()) {
            // note all scans have the same cuboid, to avoid dedup
            Cuboid cuboid = scans.get(0).getCuboid();
            context.addCuboid(cuboid);
            //TODO: we don't need to check filter after enable hbase coproccessor
            if (!cuboid.requirePostAggregation() && context.isExactAggregation() && flatFilter == null
                    && !context.hasSort()) {
                logger.info("Enable limit " + context.getLimit());
                context.enableLimit();
            }
        }

    }
}

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

package org.apache.kylin.storage.gtrecord;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.common.FuzzyValueCombination;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.gridtable.CubeGridTable;
import org.apache.kylin.cube.gridtable.CuboidToGridTableMapping;
import org.apache.kylin.cube.gridtable.RecordComparators;
import org.apache.kylin.cube.gridtable.ScanRangePlannerBase;
import org.apache.kylin.cube.gridtable.SegmentGTStartAndEnd;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRange;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.GTScanRequestBuilder;
import org.apache.kylin.gridtable.GTUtil;
import org.apache.kylin.gridtable.IGTComparator;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.StorageContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class CubeScanRangePlanner extends ScanRangePlannerBase {

    private static final Logger logger = LoggerFactory.getLogger(CubeScanRangePlanner.class);

    protected int maxScanRanges;
    protected int maxFuzzyKeys;

    //non-GT
    protected CubeSegment cubeSegment;
    protected CubeDesc cubeDesc;
    protected Cuboid cuboid;

    protected StorageContext context;

    public CubeScanRangePlanner(CubeSegment cubeSegment, Cuboid cuboid, TupleFilter filter, Set<TblColRef> dimensions, Set<TblColRef> groupbyDims, //
            Collection<FunctionDesc> metrics, StorageContext context) {
        this.context = context;

        this.maxScanRanges = KylinConfig.getInstanceFromEnv().getQueryStorageVisitScanRangeMax();
        this.maxFuzzyKeys = KylinConfig.getInstanceFromEnv().getQueryScanFuzzyKeyMax();

        this.cubeSegment = cubeSegment;
        this.cubeDesc = cubeSegment.getCubeDesc();
        this.cuboid = cuboid;

        Set<TblColRef> filterDims = Sets.newHashSet();
        TupleFilter.collectColumns(filter, filterDims);

        this.gtInfo = CubeGridTable.newGTInfo(cubeSegment, cuboid.getId());
        CuboidToGridTableMapping mapping = cuboid.getCuboidToGridTableMapping();

        IGTComparator comp = gtInfo.getCodeSystem().getComparator();
        //start key GTRecord compare to start key GTRecord
        this.rangeStartComparator = RecordComparators.getRangeStartComparator(comp);
        //stop key GTRecord compare to stop key GTRecord
        this.rangeEndComparator = RecordComparators.getRangeEndComparator(comp);
        //start key GTRecord compare to stop key GTRecord
        this.rangeStartEndComparator = RecordComparators.getRangeStartEndComparator(comp);

        //replace the constant values in filter to dictionary codes 
        this.gtFilter = GTUtil.convertFilterColumnsAndConstants(filter, gtInfo, mapping.getCuboidDimensionsInGTOrder(), groupbyDims);

        this.gtDimensions = mapping.makeGridTableColumns(dimensions);
        this.gtAggrGroups = mapping.makeGridTableColumns(replaceDerivedColumns(groupbyDims, cubeSegment.getCubeDesc()));
        this.gtAggrMetrics = mapping.makeGridTableColumns(metrics);
        this.gtAggrFuncs = mapping.makeAggrFuncs(metrics);

        if (cubeSegment.getModel().getPartitionDesc().isPartitioned()) {
            int index = mapping.getIndexOf(cubeSegment.getModel().getPartitionDesc().getPartitionDateColumnRef());
            if (index >= 0) {
                SegmentGTStartAndEnd segmentGTStartAndEnd = new SegmentGTStartAndEnd(cubeSegment, gtInfo);
                this.gtStartAndEnd = segmentGTStartAndEnd.getSegmentStartAndEnd(index);
                this.isPartitionColUsingDatetimeEncoding = segmentGTStartAndEnd.isUsingDatetimeEncoding(index);
                this.gtPartitionCol = gtInfo.colRef(index);
            }
        }

    }

    /**
     * constrcut GTScanRangePlanner with incomplete information. only be used for UT  
     * @param info
     * @param gtStartAndEnd
     * @param gtPartitionCol
     * @param gtFilter
     */
    public CubeScanRangePlanner(GTInfo info, Pair<ByteArray, ByteArray> gtStartAndEnd, TblColRef gtPartitionCol, TupleFilter gtFilter) {

        this.maxScanRanges = KylinConfig.getInstanceFromEnv().getQueryStorageVisitScanRangeMax();
        this.maxFuzzyKeys = KylinConfig.getInstanceFromEnv().getQueryScanFuzzyKeyMax();

        this.gtInfo = info;

        IGTComparator comp = gtInfo.getCodeSystem().getComparator();
        //start key GTRecord compare to start key GTRecord
        this.rangeStartComparator = RecordComparators.getRangeStartComparator(comp);
        //stop key GTRecord compare to stop key GTRecord
        this.rangeEndComparator = RecordComparators.getRangeEndComparator(comp);
        //start key GTRecord compare to stop key GTRecord
        this.rangeStartEndComparator = RecordComparators.getRangeStartEndComparator(comp);

        this.gtFilter = gtFilter;
        this.gtStartAndEnd = gtStartAndEnd;
        this.gtPartitionCol = gtPartitionCol;
    }

    public GTScanRequest planScanRequest() {
        GTScanRequest scanRequest;
        List<GTScanRange> scanRanges = this.planScanRanges();
        if (scanRanges != null && scanRanges.size() != 0) {
            GTScanRequestBuilder builder = new GTScanRequestBuilder().setInfo(gtInfo).setRanges(scanRanges).setDimensions(gtDimensions).//
                    setAggrGroupBy(gtAggrGroups).setAggrMetrics(gtAggrMetrics).setAggrMetricsFuncs(gtAggrFuncs).setFilterPushDown(gtFilter).//
                    setAllowStorageAggregation(context.isNeedStorageAggregation()).setAggCacheMemThreshold(cubeSegment.getCubeInstance().getConfig().getQueryCoprocessorMemGB()).//
                    setStorageScanRowNumThreshold(context.getThreshold());

            if (context.getFinalPushDownLimit() != Integer.MAX_VALUE)
                builder.setStoragePushDownLimit(context.getFinalPushDownLimit());

            scanRequest = builder.createGTScanRequest();
        } else {
            scanRequest = null;
        }
        return scanRequest;
    }

    /**
     * Overwrite this method to provide smarter storage visit plans
     * @return
     */
    public List<GTScanRange> planScanRanges() {
        TupleFilter flatFilter = flattenToOrAndFilter(gtFilter);

        List<Collection<ColumnRange>> orAndDimRanges = translateToOrAndDimRanges(flatFilter);

        List<GTScanRange> scanRanges = Lists.newArrayListWithCapacity(orAndDimRanges.size());
        for (Collection<ColumnRange> andDimRanges : orAndDimRanges) {
            GTScanRange scanRange = newScanRange(andDimRanges);
            if (scanRange != null)
                scanRanges.add(scanRange);
        }

        List<GTScanRange> mergedRanges = mergeOverlapRanges(scanRanges);
        mergedRanges = mergeTooManyRanges(mergedRanges, maxScanRanges);

        return mergedRanges;
    }

    private Set<TblColRef> replaceDerivedColumns(Set<TblColRef> input, CubeDesc cubeDesc) {
        Set<TblColRef> ret = Sets.newHashSet();
        for (TblColRef col : input) {
            if (cubeDesc.hasHostColumn(col)) {
                for (TblColRef host : cubeDesc.getHostInfo(col).columns) {
                    ret.add(host);
                }
            } else {
                ret.add(col);
            }
        }
        return ret;
    }

    protected GTScanRange newScanRange(Collection<ColumnRange> andDimRanges) {
        GTRecord pkStart = new GTRecord(gtInfo);
        GTRecord pkEnd = new GTRecord(gtInfo);
        Map<Integer, Set<ByteArray>> fuzzyValues = Maps.newHashMap();

        List<GTRecord> fuzzyKeys;

        for (ColumnRange range : andDimRanges) {
            if (gtPartitionCol != null && range.column.equals(gtPartitionCol)) {
                int beginCompare = rangeStartEndComparator.comparator.compare(range.begin, gtStartAndEnd.getSecond());
                int endCompare = rangeStartEndComparator.comparator.compare(gtStartAndEnd.getFirst(), range.end);

                if ((isPartitionColUsingDatetimeEncoding && endCompare <= 0 && beginCompare < 0) || (!isPartitionColUsingDatetimeEncoding && endCompare <= 0 && beginCompare <= 0)) {
                    //segment range is [Closed,Open), but segmentStartAndEnd.getSecond() might be rounded when using dict encoding, so use <= when has equals in condition. 
                } else {
                    logger.debug("Pre-check partition col filter failed, partitionColRef {}, segment start {}, segment end {}, range begin {}, range end {}", //
                            gtPartitionCol, makeReadable(gtStartAndEnd.getFirst()), makeReadable(gtStartAndEnd.getSecond()), makeReadable(range.begin), makeReadable(range.end));
                    return null;
                }
            }

            int col = range.column.getColumnDesc().getZeroBasedIndex();
            if (!gtInfo.getPrimaryKey().get(col))
                continue;

            pkStart.set(col, range.begin);
            pkEnd.set(col, range.end);

            if (range.valueSet != null && !range.valueSet.isEmpty()) {
                fuzzyValues.put(col, range.valueSet);
            }
        }

        fuzzyKeys =

                buildFuzzyKeys(fuzzyValues);
        return new GTScanRange(pkStart, pkEnd, fuzzyKeys);
    }

    private List<GTRecord> buildFuzzyKeys(Map<Integer, Set<ByteArray>> fuzzyValueSet) {
        ArrayList<GTRecord> result = Lists.newArrayList();

        if (fuzzyValueSet.isEmpty())
            return result;

        // debug/profiling purpose
        if (BackdoorToggles.getDisableFuzzyKey()) {
            logger.info("The execution of this query will not use fuzzy key");
            return result;
        }

        List<Map<Integer, ByteArray>> fuzzyValueCombinations = FuzzyValueCombination.calculate(fuzzyValueSet, maxFuzzyKeys);

        for (Map<Integer, ByteArray> fuzzyValue : fuzzyValueCombinations) {

            //            BitSet bitSet = new BitSet(gtInfo.getColumnCount());
            //            for (Map.Entry<Integer, ByteArray> entry : fuzzyValue.entrySet()) {
            //                bitSet.set(entry.getKey());
            //            }
            GTRecord fuzzy = new GTRecord(gtInfo);
            for (Map.Entry<Integer, ByteArray> entry : fuzzyValue.entrySet()) {
                fuzzy.set(entry.getKey(), entry.getValue());
            }

            result.add(fuzzy);
        }
        return result;
    }

    protected List<GTScanRange> mergeOverlapRanges(List<GTScanRange> ranges) {
        if (ranges.size() <= 1) {
            return ranges;
        }

        // sort ranges by start key
        Collections.sort(ranges, new Comparator<GTScanRange>() {
            @Override
            public int compare(GTScanRange a, GTScanRange b) {
                return rangeStartComparator.compare(a.pkStart, b.pkStart);
            }
        });

        // merge the overlap range
        List<GTScanRange> mergedRanges = new ArrayList<GTScanRange>();
        int mergeBeginIndex = 0;
        GTRecord mergeEnd = ranges.get(0).pkEnd;
        for (int index = 1; index < ranges.size(); index++) {
            GTScanRange range = ranges.get(index);

            // if overlap, swallow it
            if (rangeStartEndComparator.compare(range.pkStart, mergeEnd) <= 0) {
                mergeEnd = rangeEndComparator.max(mergeEnd, range.pkEnd);
                continue;
            }

            // not overlap, split here
            GTScanRange mergedRange = mergeKeyRange(ranges.subList(mergeBeginIndex, index));
            mergedRanges.add(mergedRange);

            // start new split
            mergeBeginIndex = index;
            mergeEnd = range.pkEnd;
        }

        // don't miss the last range
        GTScanRange mergedRange = mergeKeyRange(ranges.subList(mergeBeginIndex, ranges.size()));
        mergedRanges.add(mergedRange);

        return mergedRanges;
    }

    private GTScanRange mergeKeyRange(List<GTScanRange> ranges) {
        GTScanRange first = ranges.get(0);
        if (ranges.size() == 1)
            return first;

        GTRecord start = first.pkStart;
        GTRecord end = first.pkEnd;
        List<GTRecord> newFuzzyKeys = new ArrayList<GTRecord>();

        boolean hasNonFuzzyRange = false;
        for (GTScanRange range : ranges) {
            hasNonFuzzyRange = hasNonFuzzyRange || range.fuzzyKeys.isEmpty();
            newFuzzyKeys.addAll(range.fuzzyKeys);
            end = rangeEndComparator.max(end, range.pkEnd);
        }

        // if any range is non-fuzzy, then all fuzzy keys must be cleared
        // also too many fuzzy keys will slow down HBase scan
        if (hasNonFuzzyRange || newFuzzyKeys.size() > maxFuzzyKeys) {
            newFuzzyKeys.clear();
        }

        return new GTScanRange(start, end, newFuzzyKeys);
    }

    protected List<GTScanRange> mergeTooManyRanges(List<GTScanRange> ranges, int maxRanges) {
        if (ranges.size() <= maxRanges) {
            return ranges;
        }

        // TODO: check the distance between range and merge the large distance range
        List<GTScanRange> result = new ArrayList<GTScanRange>(1);
        GTScanRange mergedRange = mergeKeyRange(ranges);
        result.add(mergedRange);
        return result;
    }

    public int getMaxScanRanges() {
        return maxScanRanges;
    }

    public void setMaxScanRanges(int maxScanRanges) {
        this.maxScanRanges = maxScanRanges;
    }

}

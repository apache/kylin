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

package org.apache.kylin.gridtable;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.common.FuzzyValueCombination;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter.FilterOperatorEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class GTScanRangePlanner {

    private static final Logger logger = LoggerFactory.getLogger(GTScanRangePlanner.class);

    private static final int MAX_HBASE_FUZZY_KEYS = 100;

    final private GTInfo info;
    final private Pair<ByteArray, ByteArray> segmentStartAndEnd;
    final private TblColRef partitionColRef;

    final private RecordComparator rangeStartComparator;
    final private RecordComparator rangeEndComparator;
    final private RecordComparator rangeStartEndComparator;

    /**
     * @param info
     * @param segmentStartAndEnd in GT encoding
     * @param partitionColRef the TblColRef in GT
     */
    public GTScanRangePlanner(GTInfo info, Pair<ByteArray, ByteArray> segmentStartAndEnd, TblColRef partitionColRef) {

        this.info = info;
        this.segmentStartAndEnd = segmentStartAndEnd;
        this.partitionColRef = partitionColRef;

        IGTComparator comp = info.codeSystem.getComparator();

        //start key GTRecord compare to start key GTRecord
        this.rangeStartComparator = getRangeStartComparator(comp);
        //stop key GTRecord compare to stop key GTRecord
        this.rangeEndComparator = getRangeEndComparator(comp);
        //start key GTRecord compare to stop key GTRecord
        this.rangeStartEndComparator = getRangeStartEndComparator(comp);
    }

    // return empty list meaning filter is always false
    public List<GTScanRange> planScanRanges(TupleFilter filter) {
        return planScanRanges(filter, Integer.MAX_VALUE);
    }

    // return empty list meaning filter is always false
    public List<GTScanRange> planScanRanges(TupleFilter filter, int maxRanges) {

        TupleFilter flatFilter = flattenToOrAndFilter(filter);

        List<Collection<ColumnRange>> orAndDimRanges = translateToOrAndDimRanges(flatFilter);

        List<GTScanRange> scanRanges = Lists.newArrayListWithCapacity(orAndDimRanges.size());
        for (Collection<ColumnRange> andDimRanges : orAndDimRanges) {
            GTScanRange scanRange = newScanRange(andDimRanges);
            if (scanRange != null)
                scanRanges.add(scanRange);
        }

        List<GTScanRange> mergedRanges = mergeOverlapRanges(scanRanges);
        mergedRanges = mergeTooManyRanges(mergedRanges, maxRanges);

        return mergedRanges;
    }

    private String makeReadable(ByteArray byteArray) {
        if (byteArray == null) {
            return null;
        } else {
            return byteArray.toReadableText();
        }
    }

    private GTScanRange newScanRange(Collection<ColumnRange> andDimRanges) {
        GTRecord pkStart = new GTRecord(info);
        GTRecord pkEnd = new GTRecord(info);
        Map<Integer, Set<ByteArray>> fuzzyValues = Maps.newHashMap();

        List<GTRecord> fuzzyKeys;

        for (ColumnRange range : andDimRanges) {
            if (partitionColRef != null && range.column.equals(partitionColRef)) {
                if (rangeStartEndComparator.comparator.compare(segmentStartAndEnd.getFirst(), range.end) <= 0 //
                        && rangeStartEndComparator.comparator.compare(range.begin, segmentStartAndEnd.getSecond()) < 0) {
                    //segment range is [Closed,Open)
                } else {
                    logger.debug("Pre-check partition col filter failed, partitionColRef {}, segment start {}, segment end {}, range begin {}, range end {}",//
                            new Object[] { partitionColRef, makeReadable(segmentStartAndEnd.getFirst()), makeReadable(segmentStartAndEnd.getSecond()), makeReadable(range.begin), makeReadable(range.end) });
                    return null;
                }
            }

            int col = range.column.getColumnDesc().getZeroBasedIndex();
            if (!info.primaryKey.get(col))
                continue;

            pkStart.set(col, range.begin);
            pkEnd.set(col, range.end);

            if (range.valueSet != null && !range.valueSet.isEmpty()) {
                fuzzyValues.put(col, range.valueSet);
            }
        }

        fuzzyKeys = buildFuzzyKeys(fuzzyValues);

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

        List<Map<Integer, ByteArray>> fuzzyValueCombinations = FuzzyValueCombination.calculate(fuzzyValueSet, MAX_HBASE_FUZZY_KEYS);

        for (Map<Integer, ByteArray> fuzzyValue : fuzzyValueCombinations) {

            BitSet bitSet = new BitSet(info.getColumnCount());
            for (Map.Entry<Integer, ByteArray> entry : fuzzyValue.entrySet()) {
                bitSet.set(entry.getKey());
            }
            GTRecord fuzzy = new GTRecord(info, new ImmutableBitSet(bitSet));
            for (Map.Entry<Integer, ByteArray> entry : fuzzyValue.entrySet()) {
                fuzzy.set(entry.getKey(), entry.getValue());
            }

            result.add(fuzzy);
        }
        return result;
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

    private List<Collection<ColumnRange>> translateToOrAndDimRanges(TupleFilter flatFilter) {
        List<Collection<ColumnRange>> result = Lists.newArrayList();

        if (flatFilter == null) {
            result.add(Collections.<ColumnRange> emptyList());
            return result;
        }

        for (TupleFilter andFilter : flatFilter.getChildren()) {
            if (andFilter.getOperator() != FilterOperatorEnum.AND)
                throw new IllegalStateException("Filter should be AND instead of " + andFilter);

            Collection<ColumnRange> andRanges = translateToAndDimRanges(andFilter.getChildren());
            if (andRanges != null) {
                result.add(andRanges);
            }
        }

        return preEvaluateConstantConditions(result);
    }

    private Collection<ColumnRange> translateToAndDimRanges(List<? extends TupleFilter> andFilters) {
        Map<TblColRef, ColumnRange> rangeMap = new HashMap<TblColRef, ColumnRange>();
        for (TupleFilter filter : andFilters) {
            if ((filter instanceof CompareTupleFilter) == false) {
                if (filter instanceof ConstantTupleFilter && !filter.evaluate(null, null)) {
                    return null;
                } else {
                    continue;
                }
            }

            CompareTupleFilter comp = (CompareTupleFilter) filter;
            if (comp.getColumn() == null) {
                continue;
            }

            @SuppressWarnings("unchecked")
            ColumnRange newRange = new ColumnRange(comp.getColumn(), (Set<ByteArray>) comp.getValues(), comp.getOperator());
            ColumnRange existing = rangeMap.get(newRange.column);
            if (existing == null) {
                rangeMap.put(newRange.column, newRange);
            } else {
                existing.andMerge(newRange);
            }
        }
        return rangeMap.values();
    }

    private List<Collection<ColumnRange>> preEvaluateConstantConditions(List<Collection<ColumnRange>> orAndRanges) {
        boolean globalAlwaysTrue = false;
        Iterator<Collection<ColumnRange>> iterator = orAndRanges.iterator();
        while (iterator.hasNext()) {
            Collection<ColumnRange> andRanges = iterator.next();
            Iterator<ColumnRange> iterator2 = andRanges.iterator();
            boolean hasAlwaysFalse = false;
            while (iterator2.hasNext()) {
                ColumnRange range = iterator2.next();
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
        // return empty OR list means global false
        // return an empty AND collection inside OR list means global true
        if (globalAlwaysTrue) {
            orAndRanges.clear();
            orAndRanges.add(Collections.<ColumnRange> emptyList());
        }
        return orAndRanges;
    }

    private List<GTScanRange> mergeOverlapRanges(List<GTScanRange> ranges) {
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
        if (hasNonFuzzyRange || newFuzzyKeys.size() > MAX_HBASE_FUZZY_KEYS) {
            newFuzzyKeys.clear();
        }

        return new GTScanRange(start, end, newFuzzyKeys);
    }

    private List<GTScanRange> mergeTooManyRanges(List<GTScanRange> ranges, int maxRanges) {
        if (ranges.size() <= maxRanges) {
            return ranges;
        }

        // TODO: check the distance between range and merge the large distance range
        List<GTScanRange> result = new ArrayList<GTScanRange>(1);
        GTScanRange mergedRange = mergeKeyRange(ranges);
        result.add(mergedRange);
        return result;
    }

    private class ColumnRange {
        private TblColRef column;
        private ByteArray begin = ByteArray.EMPTY;
        private ByteArray end = ByteArray.EMPTY;
        private Set<ByteArray> valueSet;

        public ColumnRange(TblColRef column, Set<ByteArray> values, FilterOperatorEnum op) {
            this.column = column;

            switch (op) {
            case EQ:
            case IN:
                valueSet = new HashSet<ByteArray>(values);
                refreshBeginEndFromEquals();
                break;
            case LT:
            case LTE:
                end = rangeEndComparator.comparator.max(values);
                break;
            case GT:
            case GTE:
                begin = rangeStartComparator.comparator.min(values);
                break;
            case NEQ:
            case NOTIN:
            case ISNULL:
            case ISNOTNULL:
                // let Optiq filter it!
                break;
            default:
                throw new UnsupportedOperationException(op.name());
            }
        }

        void copy(TblColRef column, ByteArray beginValue, ByteArray endValue, Set<ByteArray> equalValues) {
            this.column = column;
            this.begin = beginValue;
            this.end = endValue;
            this.valueSet = equalValues;
        }

        private void refreshBeginEndFromEquals() {
            if (valueSet.isEmpty()) {
                begin = ByteArray.EMPTY;
                end = ByteArray.EMPTY;
            } else {
                begin = rangeStartComparator.comparator.min(valueSet);
                end = rangeEndComparator.comparator.max(valueSet);
            }
        }

        public boolean satisfyAll() {
            return begin.array() == null && end.array() == null; // the NEQ case
        }

        public boolean satisfyNone() {
            if (valueSet != null) {
                return valueSet.isEmpty();
            } else if (begin.array() != null && end.array() != null) {
                return info.codeSystem.getComparator().compare(begin, end) > 0;
            } else {
                return false;
            }
        }

        public void andMerge(ColumnRange another) {
            assert this.column.equals(another.column);

            if (another.satisfyAll()) {
                return;
            }

            if (this.satisfyAll()) {
                copy(another.column, another.begin, another.end, another.valueSet);
                return;
            }

            if (this.valueSet != null && another.valueSet != null) {
                this.valueSet.retainAll(another.valueSet);
                refreshBeginEndFromEquals();
                return;
            }

            if (this.valueSet != null) {
                this.valueSet = filter(this.valueSet, another.begin, another.end);
                refreshBeginEndFromEquals();
                return;
            }

            if (another.valueSet != null) {
                this.valueSet = filter(another.valueSet, this.begin, this.end);
                refreshBeginEndFromEquals();
                return;
            }

            this.begin = rangeStartComparator.comparator.max(this.begin, another.begin);
            this.end = rangeEndComparator.comparator.min(this.end, another.end);
        }

        private Set<ByteArray> filter(Set<ByteArray> equalValues, ByteArray beginValue, ByteArray endValue) {
            Set<ByteArray> result = Sets.newHashSetWithExpectedSize(equalValues.size());
            for (ByteArray v : equalValues) {
                if (rangeStartEndComparator.comparator.compare(beginValue, v) <= 0 && rangeStartEndComparator.comparator.compare(v, endValue) <= 0) {
                    result.add(v);
                }
            }
            return equalValues;
        }

        public String toString() {
            if (valueSet == null) {
                return column.getName() + " between " + begin + " and " + end;
            } else {
                return column.getName() + " in " + valueSet;
            }
        }
    }

    public static abstract class ComparatorEx<T> implements Comparator<T> {

        public T min(Collection<T> v) {
            if (v.size() <= 0) {
                return null;
            }

            Iterator<T> iterator = v.iterator();
            T min = iterator.next();
            while (iterator.hasNext()) {
                min = min(min, iterator.next());
            }
            return min;
        }

        public T max(Collection<T> v) {
            if (v.size() <= 0) {
                return null;
            }

            Iterator<T> iterator = v.iterator();
            T max = iterator.next();
            while (iterator.hasNext()) {
                max = max(max, iterator.next());
            }
            return max;
        }

        public T min(T a, T b) {
            return compare(a, b) <= 0 ? a : b;
        }

        public T max(T a, T b) {
            return compare(a, b) >= 0 ? a : b;
        }

        public boolean between(T v, T start, T end) {
            return compare(start, v) <= 0 && compare(v, end) <= 0;
        }
    }

    public static RecordComparator getRangeStartComparator(final IGTComparator comp) {
        return new RecordComparator(new ComparatorEx<ByteArray>() {
            @Override
            public int compare(ByteArray a, ByteArray b) {
                if (a.array() == null) {
                    if (b.array() == null) {
                        return 0;
                    } else {
                        return -1;
                    }
                } else if (b.array() == null) {
                    return 1;
                } else {
                    return comp.compare(a, b);
                }
            }
        });
    }

    public static RecordComparator getRangeEndComparator(final IGTComparator comp) {
        return new RecordComparator(new ComparatorEx<ByteArray>() {
            @Override
            public int compare(ByteArray a, ByteArray b) {
                if (a.array() == null) {
                    if (b.array() == null) {
                        return 0;
                    } else {
                        return 1;
                    }
                } else if (b.array() == null) {
                    return -1;
                } else {
                    return comp.compare(a, b);
                }
            }
        });
    }

    public static RecordComparator getRangeStartEndComparator(final IGTComparator comp) {
        return new AsymmetricRecordComparator(new ComparatorEx<ByteArray>() {
            @Override
            public int compare(ByteArray a, ByteArray b) {
                if (a.array() == null || b.array() == null) {
                    return -1;
                } else {
                    return comp.compare(a, b);
                }
            }
        });
    }

    private static class RecordComparator extends ComparatorEx<GTRecord> {
        final ComparatorEx<ByteArray> comparator;

        RecordComparator(ComparatorEx<ByteArray> byteComparator) {
            this.comparator = byteComparator;
        }

        @Override
        public int compare(GTRecord a, GTRecord b) {
            assert a.info == b.info;
            assert a.maskForEqualHashComp() == b.maskForEqualHashComp();
            ImmutableBitSet mask = a.maskForEqualHashComp();

            int comp;
            for (int i = 0; i < mask.trueBitCount(); i++) {
                int c = mask.trueBitAt(i);
                comp = comparator.compare(a.cols[c], b.cols[c]);
                if (comp != 0)
                    return comp;
            }
            return 0; // equals
        }
    }

    /**
     * asymmetric means compare(a,b) > 0 does not cause compare(b,a) < 0 
     * so min max functions will not be supported
     */
    private static class AsymmetricRecordComparator extends RecordComparator {

        AsymmetricRecordComparator(ComparatorEx<ByteArray> byteComparator) {
            super(byteComparator);
        }

        public GTRecord min(Collection<GTRecord> v) {
            throw new UnsupportedOperationException();
        }

        public GTRecord max(Collection<GTRecord> v) {
            throw new UnsupportedOperationException();
        }

        public GTRecord min(GTRecord a, GTRecord b) {
            throw new UnsupportedOperationException();
        }

        public GTRecord max(GTRecord a, GTRecord b) {
            throw new UnsupportedOperationException();
        }

        public boolean between(GTRecord v, GTRecord start, GTRecord end) {
            throw new UnsupportedOperationException();
        }
    }
}

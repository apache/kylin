package org.apache.kylin.storage.gridtable;

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

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.IFilterCodeSystem;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter.FilterOperatorEnum;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class GTScanRangePlanner {

    private static final int MAX_HBASE_FUZZY_KEYS = 100;

    final private GTInfo info;
    final private ComparatorEx<ByteArray> byteUnknownIsSmaller;
    final private ComparatorEx<ByteArray> byteUnknownIsBigger;
    final private ComparatorEx<GTRecord> recordUnknownIsSmaller;
    final private ComparatorEx<GTRecord> recordUnknownIsBigger;

    public GTScanRangePlanner(GTInfo info) {
        this.info = info;

        IFilterCodeSystem<ByteArray> cs = info.codeSystem.getFilterCodeSystem();
        this.byteUnknownIsSmaller = byteComparatorTreatsUnknownSmaller(cs);
        this.byteUnknownIsBigger = byteComparatorTreatsUnknownBigger(cs);
        this.recordUnknownIsSmaller = recordComparatorTreatsUnknownSmaller(cs);
        this.recordUnknownIsBigger = recordComparatorTreatsUnknownBigger(cs);
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
            scanRanges.add(scanRange);
        }

        List<GTScanRange> mergedRanges = mergeOverlapRanges(scanRanges);
        mergedRanges = mergeTooManyRanges(mergedRanges, maxRanges);

        return mergedRanges;
    }

    private GTScanRange newScanRange(Collection<ColumnRange> andDimRanges) {
        GTRecord pkStart = new GTRecord(info);
        GTRecord pkEnd = new GTRecord(info);
        List<GTRecord> hbaseFuzzyKeys = Lists.newArrayList();

        for (ColumnRange range : andDimRanges) {
            int col = range.column.getColumn().getZeroBasedIndex();
            if (info.primaryKey.get(col) == false)
                continue;

            pkStart.set(col, range.begin);
            pkEnd.set(col, range.end);

            if (range.equals != null) {
                BitSet fuzzyMask = new BitSet();
                fuzzyMask.set(col);
                for (ByteArray v : range.equals) {
                    GTRecord fuzzy = new GTRecord(info);
                    fuzzy.set(col, v);
                    fuzzy.maskForEqualHashComp(fuzzyMask);
                    hbaseFuzzyKeys.add(fuzzy);
                }
            }
        }

        return new GTScanRange(pkStart, pkEnd, hbaseFuzzyKeys);
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
            result.add(andRanges);
        }

        return preEvaluateConstantConditions(result);
    }

    private Collection<ColumnRange> translateToAndDimRanges(List<? extends TupleFilter> andFilters) {
        Map<TblColRef, ColumnRange> rangeMap = new HashMap<TblColRef, ColumnRange>();
        for (TupleFilter filter : andFilters) {
            if ((filter instanceof CompareTupleFilter) == false) {
                continue;
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
                return recordUnknownIsSmaller.compare(a.pkStart, b.pkStart);
            }
        });

        // merge the overlap range
        List<GTScanRange> mergedRanges = new ArrayList<GTScanRange>();
        int mergeBeginIndex = 0;
        GTRecord mergeEnd = ranges.get(0).pkEnd;
        for (int index = 0; index < ranges.size(); index++) {
            GTScanRange range = ranges.get(index);

            // if overlap, swallow it
            if (recordUnknownIsSmaller.min(range.pkStart, mergeEnd) == range.pkStart //
                    || recordUnknownIsBigger.max(mergeEnd, range.pkStart) == mergeEnd) {
                mergeEnd = recordUnknownIsBigger.max(mergeEnd, range.pkEnd);
                continue;
            }

            // not overlap, split here
            GTScanRange mergedRange = mergeKeyRange(ranges.subList(mergeBeginIndex, index));
            mergedRanges.add(mergedRange);

            // start new split
            mergeBeginIndex = index;
            mergeEnd = recordUnknownIsBigger.max(mergeEnd, range.pkEnd);
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
            hasNonFuzzyRange = hasNonFuzzyRange || range.hbaseFuzzyKeys.isEmpty();
            newFuzzyKeys.addAll(range.hbaseFuzzyKeys);
            end = recordUnknownIsBigger.max(end, range.pkEnd);
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
        private ByteArray begin = new ByteArray();
        private ByteArray end = new ByteArray();
        private Set<ByteArray> equals;

        public ColumnRange(TblColRef column, Set<ByteArray> values, FilterOperatorEnum op) {
            this.column = column;

            switch (op) {
            case EQ:
            case IN:
                equals = new HashSet<ByteArray>(values);
                refreshBeginEndFromEquals();
                break;
            case LT:
            case LTE:
                end = byteUnknownIsBigger.max(values);
                break;
            case GT:
            case GTE:
                begin = byteUnknownIsSmaller.min(values);
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
            this.equals = equalValues;
        }

        private void refreshBeginEndFromEquals() {
            this.begin = byteUnknownIsSmaller.min(this.equals);
            this.end = byteUnknownIsBigger.max(this.equals);
        }

        public boolean satisfyAll() {
            return begin.array() == null && end.array() == null; // the NEQ case
        }

        public boolean satisfyNone() {
            if (equals != null) {
                return equals.isEmpty();
            } else if (begin.array() != null && end.array() != null) {
                return info.codeSystem.getFilterCodeSystem().compare(begin, end) > 0;
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
                copy(another.column, another.begin, another.end, another.equals);
                return;
            }

            if (this.equals != null && another.equals != null) {
                this.equals.retainAll(another.equals);
                refreshBeginEndFromEquals();
                return;
            }

            if (this.equals != null) {
                this.equals = filter(this.equals, another.begin, another.end);
                refreshBeginEndFromEquals();
                return;
            }

            if (another.equals != null) {
                this.equals = filter(another.equals, this.begin, this.end);
                refreshBeginEndFromEquals();
                return;
            }

            this.begin = byteUnknownIsSmaller.max(this.begin, another.begin);
            this.end = byteUnknownIsBigger.min(this.end, another.end);
        }

        private Set<ByteArray> filter(Set<ByteArray> equalValues, ByteArray beginValue, ByteArray endValue) {
            Set<ByteArray> result = Sets.newHashSetWithExpectedSize(equalValues.size());
            for (ByteArray v : equalValues) {
                if (byteUnknownIsSmaller.compare(beginValue, v) <= 0 && byteUnknownIsBigger.compare(v, endValue) <= 0) {
                    result.add(v);
                }
            }
            return equalValues;
        }

        public String toString() {
            if (equals == null) {
                return column.getName() + " between " + begin + " and " + end;
            } else {
                return column.getName() + " in " + equals;
            }
        }
    }

    public static abstract class ComparatorEx<T> implements Comparator<T> {

        public T min(Collection<T> v) {
            if (v.size() < 0) {
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
            if (v.size() < 0) {
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

    public static ComparatorEx<ByteArray> byteComparatorTreatsUnknownSmaller(final IFilterCodeSystem<ByteArray> cs) {
        return new ComparatorEx<ByteArray>() {
            @Override
            public int compare(ByteArray a, ByteArray b) {
                if (a.array() == null)
                    return -1;
                else if (b.array() == null)
                    return 1;
                else
                    return cs.compare(a, b);
            }
        };
    }

    public static ComparatorEx<ByteArray> byteComparatorTreatsUnknownBigger(final IFilterCodeSystem<ByteArray> cs) {
        return new ComparatorEx<ByteArray>() {
            @Override
            public int compare(ByteArray a, ByteArray b) {
                if (a.array() == null)
                    return 1;
                else if (b.array() == null)
                    return -1;
                else
                    return cs.compare(a, b);
            }
        };
    }

    public static ComparatorEx<GTRecord> recordComparatorTreatsUnknownSmaller(IFilterCodeSystem<ByteArray> cs) {
        return new RecordComparator(byteComparatorTreatsUnknownSmaller(cs));
    }

    public static ComparatorEx<GTRecord> recordComparatorTreatsUnknownBigger(IFilterCodeSystem<ByteArray> cs) {
        return new RecordComparator(byteComparatorTreatsUnknownBigger(cs));
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
            BitSet mask = a.maskForEqualHashComp();

            int comp = 0;
            for (int i = mask.nextSetBit(0); i >= 0; i = mask.nextSetBit(i + 1)) {
                comp = comparator.compare(a.cols[i], b.cols[i]);
                if (comp != 0)
                    return comp;
            }
            return 0; // equals
        }
    }
}

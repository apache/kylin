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

package org.apache.kylin.cube.gridtable;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public abstract class ScanRangePlannerBase {

    //GT 
    protected TupleFilter gtFilter;
    protected GTInfo gtInfo;
    protected Pair<ByteArray, ByteArray> gtStartAndEnd;
    protected TblColRef gtPartitionCol;
    protected ImmutableBitSet gtDimensions;
    protected ImmutableBitSet gtAggrGroups;
    protected ImmutableBitSet gtAggrMetrics;
    protected String[] gtAggrFuncs;
    protected boolean isPartitionColUsingDatetimeEncoding = true;

    protected RecordComparator rangeStartComparator;
    protected RecordComparator rangeEndComparator;
    protected RecordComparator rangeStartEndComparator;

    public abstract GTScanRequest planScanRequest();

    protected TupleFilter flattenToOrAndFilter(TupleFilter filter) {
        if (filter == null)
            return null;

        TupleFilter flatFilter = filter.flatFilter();

        // normalize to OR-AND filter
        if (flatFilter.getOperator() == TupleFilter.FilterOperatorEnum.AND) {
            LogicalTupleFilter f = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.OR);
            f.addChild(flatFilter);
            flatFilter = f;
        }

        if (flatFilter.getOperator() != TupleFilter.FilterOperatorEnum.OR)
            throw new IllegalStateException();

        return flatFilter;
    }

    protected List<Collection<ColumnRange>> translateToOrAndDimRanges(TupleFilter flatFilter) {
        List<Collection<ColumnRange>> result = Lists.newArrayList();

        if (flatFilter == null) {
            result.add(Collections.<ColumnRange> emptyList());
            return result;
        }

        for (TupleFilter andFilter : flatFilter.getChildren()) {
            if (andFilter.getOperator() != TupleFilter.FilterOperatorEnum.AND)
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

    public class ColumnRange {
        public TblColRef column;
        public ByteArray begin = ByteArray.EMPTY;
        public ByteArray end = ByteArray.EMPTY;
        public Set<ByteArray> valueSet;
        public boolean isBoundryInclusive;

        public ColumnRange(TblColRef column, Set<ByteArray> values, TupleFilter.FilterOperatorEnum op) {
            this.column = column;

            //TODO: the treatment is un-precise
            if (op == TupleFilter.FilterOperatorEnum.EQ || op == TupleFilter.FilterOperatorEnum.IN || op == TupleFilter.FilterOperatorEnum.LTE || op == TupleFilter.FilterOperatorEnum.GTE) {
                isBoundryInclusive = true;
            }

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
                return gtInfo.getCodeSystem().getComparator().compare(begin, end) > 0;
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
            this.isBoundryInclusive |= another.isBoundryInclusive;
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

    protected String makeReadable(ByteArray byteArray) {
        if (byteArray == null) {
            return null;
        } else {
            return byteArray.toReadableText();
        }
    }

}

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

package org.apache.kylin.storage.druid.read.filter;

import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.InDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.ordering.StringComparator;

import java.util.List;

public class RangeSets {

    public static RangeSet<RangeValue> from(DimFilter filter, StringComparator comparator) {
        if (filter instanceof SelectorDimFilter) {
            SelectorDimFilter select = (SelectorDimFilter) filter;
            RangeValue value = new RangeValue(select.getValue(), comparator);
            return of(Range.singleton(value));
        }
        if (filter instanceof BoundDimFilter) {
            BoundDimFilter bound = (BoundDimFilter) filter;
            BoundType lowerType = bound.isLowerStrict() ? BoundType.OPEN : BoundType.CLOSED;
            BoundType upperType = bound.isUpperStrict() ? BoundType.OPEN : BoundType.CLOSED;
            RangeValue lowerValue = bound.hasLowerBound() ? new RangeValue(bound.getLower(), comparator) : null;
            RangeValue upperValue = bound.hasUpperBound() ? new RangeValue(bound.getUpper(), comparator) : null;

            Range range = null;
            if (bound.hasLowerBound() && bound.hasUpperBound()) {
                range = Range.range(lowerValue, lowerType, upperValue, upperType);
            } else if (bound.hasLowerBound()) {
                range = Range.downTo(lowerValue, lowerType);
            } else if (bound.hasUpperBound()) {
                range = Range.upTo(upperValue, upperType);
            }
            return of(range);
        }
        if (filter instanceof InDimFilter) {
            InDimFilter in = (InDimFilter) filter;
            RangeSet<RangeValue> result = TreeRangeSet.create();
            for (String value : in.getValues()) {
                result.add(Range.singleton(new RangeValue(value, comparator)));
            }
            return result;
        }
        return null;
    }

    public static BoundDimFilter rangeToFilter(RangeKey key, Range<RangeValue> range) {
        return new BoundDimFilter(
                key.getDimension(),
                range.hasLowerBound() ? range.lowerEndpoint().getValue() : null,
                range.hasUpperBound() ? range.upperEndpoint().getValue() : null,
                range.hasLowerBound() && range.lowerBoundType() == BoundType.OPEN,
                range.hasUpperBound() && range.upperBoundType() == BoundType.OPEN,
                null,
                key.getExtractionFn(),
                key.getComparator()
        );
    }

    public static DimFilter rangeSetsToFilter(RangeKey key, RangeSet<RangeValue> rangeSet) {
        if (rangeSet.isEmpty()) {
            return MoreDimFilters.ALWAYS_FALSE;
        }

        List<String> values = Lists.newArrayList();
        List<DimFilter> filters = Lists.newArrayList();

        for (Range<RangeValue> range : rangeSet.asRanges()) {
            if (!range.hasLowerBound() && !range.hasUpperBound()) {
                return MoreDimFilters.ALWAYS_TRUE;
            }
            if (range.hasLowerBound() && range.hasUpperBound() && range.lowerEndpoint().equals(range.upperEndpoint())) {
                values.add(range.lowerEndpoint().getValue()); // range match single value
            } else {
                filters.add(rangeToFilter(key, range));
            }
        }

        if (!values.isEmpty()) {
            filters.add(new InDimFilter(key.getDimension(), values, key.getExtractionFn()));
        }

        return MoreDimFilters.or(filters);
    }

    public static <T extends Comparable<T>> RangeSet<T> of(Range<T> range) {
        RangeSet<T> set = TreeRangeSet.create();
        set.add(range);
        return set;
    }

    /**
     * Intersects a set of rangeSets, or returns empty set if the set is empty.
     */
    public static <T extends Comparable<T>> RangeSet<T> intersect(final Iterable<RangeSet<T>> rangeSets) {
        RangeSet<T> rangeSet = null;
        for (final RangeSet<T> set : rangeSets) {
            if (rangeSet == null) {
                rangeSet = TreeRangeSet.create();
                rangeSet.addAll(set);
            } else {
                rangeSet.removeAll(set.complement());
            }
        }
        if (rangeSet == null) {
            return TreeRangeSet.create();
        }
        return rangeSet;
    }

    /**
     * Unions a set of rangeSets, or returns empty set if the set is empty.
     */
    public static <T extends Comparable<T>> RangeSet<T> union(final Iterable<RangeSet<T>> rangeSets) {
        final RangeSet<T> rangeSet = TreeRangeSet.create();
        for (RangeSet<T> set : rangeSets) {
            rangeSet.addAll(set);
        }
        return rangeSet;
    }
}

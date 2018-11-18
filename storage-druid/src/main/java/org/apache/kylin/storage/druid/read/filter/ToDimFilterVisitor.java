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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.RangeSet;
import org.apache.kylin.metadata.filter.BuiltInFunctionTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilterVisitor2;
import org.apache.kylin.metadata.filter.TupleFilterVisitor2Adaptor;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.druid.DruidSchema;
import org.apache.kylin.storage.druid.NameMapping;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.InDimFilter;
import io.druid.query.filter.LikeDimFilter;
import io.druid.query.filter.NotDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.ordering.StringComparator;

public class ToDimFilterVisitor implements TupleFilterVisitor2<DimFilter> {

    private final NameMapping mapping;
    private final List<TblColRef> dimensions;

    private Map<String, StringComparator> comparators = Maps.newHashMap();

    public ToDimFilterVisitor(NameMapping mapping, List<TblColRef> dimensions) {
        this.mapping = mapping;
        this.dimensions = dimensions;
    }

    private StringComparator getComparator(DimFilter filter) {
        String dimension = MoreDimFilters.getDimension(filter);
        return comparators.get(dimension);
    }

    private DimFilter makeCompareFilter(TblColRef column, TupleFilter.FilterOperatorEnum op, Set<?> values, Object firstValue, ExtractionFn extractionFn) {
        final String dimension = mapping.getDimFieldName(column);
        StringComparator comparator = comparators.get(dimension);
        if (comparator == null) {
            comparator = DruidSchema.dimensionComparator(column.getType());
            comparators.put(dimension, comparator);
        }

        // FIXME toString only works for string, float, double, int, long?
        String stringValue = null;
        if (firstValue != null) {
            stringValue = firstValue.toString();
        }
        final Set<String> stringSet = new HashSet<>();
        for (Object value : values) {
            stringSet.add(value.toString());
        }

        switch (op) {
        case EQ:
            return new SelectorDimFilter(dimension, stringValue, extractionFn);
        case NEQ:
            return new NotDimFilter(new SelectorDimFilter(dimension, stringValue, extractionFn));
        case LT:
            return new BoundDimFilter(dimension, null, stringValue, null, true, null, extractionFn, comparator);
        case LTE:
            return new BoundDimFilter(dimension, null, stringValue, null, false, null, extractionFn, comparator);
        case GT:
            return new BoundDimFilter(dimension, stringValue, null, true, null, null, extractionFn, comparator);
        case GTE:
            return new BoundDimFilter(dimension, stringValue, null, false, null, null, extractionFn, comparator);
        case ISNULL:
            return new SelectorDimFilter(dimension, null, null);
        case ISNOTNULL:
            return new NotDimFilter(new SelectorDimFilter(dimension, null, null));
        case IN:
            return new InDimFilter(dimension, stringSet, extractionFn);
        case NOTIN:
            return new NotDimFilter(new InDimFilter(dimension, stringSet, extractionFn));
        default:
            throw new AssertionError("Illegal op for CompareTupleFilter: " + op);
        }
    }

    @Override
    public DimFilter visitColumnCompare(CompareTupleFilter originFilter, TblColRef column, TupleFilter.FilterOperatorEnum op, Set<?> values, Object firstValue) {
        assert dimensions.contains(column);
        assert !values.isEmpty() || op == TupleFilter.FilterOperatorEnum.ISNOTNULL ||  op == TupleFilter.FilterOperatorEnum.ISNULL;

        return makeCompareFilter(column, op, values, firstValue, null);
    }

    @Override
    public DimFilter visitColumnLike(BuiltInFunctionTupleFilter originFilter, TblColRef column, String pattern, boolean reversed) {
        assert dimensions.contains(column);

        ExtractionFn extractionFn = null;
        if (originFilter.getColumnContainerFilter() instanceof BuiltInFunctionTupleFilter) {
            // nested case: lower(c) like 'x'
            extractionFn = FunctionConverters.convert((BuiltInFunctionTupleFilter) originFilter.getColumnContainerFilter());
            if (extractionFn == null) {
                return visitUnsupported(originFilter);
            }
        }

        String dimension = mapping.getDimFieldName(column);
        LikeDimFilter likeDimFilter = new LikeDimFilter(dimension, pattern, null, extractionFn);
        return reversed ? new NotDimFilter(likeDimFilter) : likeDimFilter;
    }

    @Override
    public DimFilter visitColumnFunction(CompareTupleFilter originFilter, BuiltInFunctionTupleFilter function, TupleFilter.FilterOperatorEnum op, Set<?> values, Object firstValue) {
        TblColRef column = function.getColumn();
        assert dimensions.contains(column);

        ExtractionFn extractionFn = FunctionConverters.convert(function);
        if (extractionFn == null) {
            return visitUnsupported(originFilter);
        }

        return makeCompareFilter(column, op, values, firstValue, extractionFn);
    }

    @Override
    public DimFilter visitAnd(LogicalTupleFilter originFilter, List<? extends TupleFilter> children, TupleFilterVisitor2Adaptor<DimFilter> adaptor) {
        List<DimFilter> childFilters = Lists.newArrayListWithCapacity(children.size());
        for (TupleFilter child : children) {
            childFilters.add(child.accept(adaptor));
        }
        return simplify(childFilters, false);
    }

    @Override
    public DimFilter visitOr(LogicalTupleFilter originFilter, List<? extends TupleFilter> children, TupleFilterVisitor2Adaptor<DimFilter> adaptor) {
        List<DimFilter> childFilters = Lists.newArrayListWithCapacity(children.size());
        for (TupleFilter child : children) {
            childFilters.add(child.accept(adaptor));
        }
        return simplify(childFilters, true);
    }

    private DimFilter simplify(List<DimFilter> children, boolean disjunction) {
        List<DimFilter> newChildren = Lists.newArrayList(children);

        ListMultimap<RangeKey, DimFilter> columnToFilter = ArrayListMultimap.create();
        for (DimFilter child : children) {
            RangeKey key = RangeKey.from(child, getComparator(child));
            if (key != null) {
                columnToFilter.put(key, child);
            }
        }

        // try to simplify filters on each column
        for (RangeKey key : columnToFilter.keySet()) {
            List<DimFilter> filters = columnToFilter.get(key);
            if (filters.size() == 1) {
                continue; // can't be simplified any more
            }
            // determine value range
            List<RangeSet<RangeValue>> rangeSets = Lists.newArrayList();
            for (DimFilter filter : filters) {
                rangeSets.add(RangeSets.from(filter, getComparator(filter)));
            }
            RangeSet<RangeValue> finalRangeSet = disjunction ? RangeSets.union(rangeSets) : RangeSets.intersect(rangeSets);

            // convert range set back to filter
            DimFilter newFilter = RangeSets.rangeSetsToFilter(key, finalRangeSet);
            if (newFilter == MoreDimFilters.ALWAYS_TRUE && disjunction) {
                return newFilter; // or short-circuit
            }
            if (newFilter == MoreDimFilters.ALWAYS_FALSE && !disjunction) {
                return newFilter; // and short-circuit
            }
            int leafCount = MoreDimFilters.leafCount(newFilter);
            if (leafCount < filters.size()) { // found simplification
                newChildren.removeAll(filters);
                newChildren.add(newFilter);
            }
        }

        return disjunction ? MoreDimFilters.or(newChildren) : MoreDimFilters.and(newChildren);
    }

    @Override
    public DimFilter visitNot(LogicalTupleFilter originFilter, TupleFilter child, TupleFilterVisitor2Adaptor<DimFilter> adaptor) {
        DimFilter childFilter = child.accept(adaptor);
        return MoreDimFilters.not(childFilter);
    }

    @Override
    public DimFilter visitConstant(ConstantTupleFilter originFilter) {
        // only happens on root filter
        if (originFilter == ConstantTupleFilter.TRUE) {
            return MoreDimFilters.ALWAYS_TRUE;
        }
        if (originFilter == ConstantTupleFilter.FALSE) {
            return MoreDimFilters.ALWAYS_FALSE;
        }
        throw new AssertionError("other constant filter");
    }

    @Override
    public DimFilter visitUnsupported(TupleFilter originFilter) {
        throw new UnsupportedFilterException(originFilter.toString());
    }
}

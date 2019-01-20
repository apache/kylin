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

package org.apache.kylin.storage.parquet.cube;

import com.google.common.collect.Lists;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.dict.lookup.ILookupTable;
import org.apache.kylin.metadata.filter.BuiltInFunctionTupleFilter;
import org.apache.kylin.metadata.filter.CaseTupleFilter;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.FilterCodeSystemFactory;
import org.apache.kylin.metadata.filter.IFilterCodeSystem;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilterVisitor2;
import org.apache.kylin.metadata.filter.TupleFilterVisitor2Adaptor;
import org.apache.kylin.metadata.filter.TupleFilters;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.IEvaluatableTuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Bottom up traverse filter tree, translate derived column filter to host column filter.
 */
public class TranslateDerivedVisitor implements TupleFilterVisitor2<TupleFilter> {
    private final CubeDesc desc;
    private final LookupTableCache lookupCache;
    private final Set<TblColRef> unevaluatableColumnCollector;

    public TranslateDerivedVisitor(CubeDesc desc, LookupTableCache lookupCache, Set<TblColRef> unevaluatableColumnCollector) {
        this.desc = desc;
        this.lookupCache = lookupCache;
        this.unevaluatableColumnCollector = unevaluatableColumnCollector;
    }

    private void checkNotFilteringOnExtendedColumn(TblColRef column) {
        if (desc.isExtendedColumn(column)) {
            throw new CubeDesc.CannotFilterExtendedColumnException(column);
        }
    }

    private List<String[]> collectAllMatchingHostRecords(TblColRef derivedColumn, CubeDesc.DeriveInfo hostInfo, DerivedColumnMatcher matcher) {
        int derivedIndex = derivedColumn.getColumnDesc().getZeroBasedIndex();

        TblColRef[] pkCols = hostInfo.join.getPrimaryKeyColumns();
        int[] pkIndices = new int[pkCols.length];
        for (int i = 0; i < pkCols.length; i++) {
            pkIndices[i] = pkCols[i].getColumnDesc().getZeroBasedIndex();
        }

        List<String[]> result = new ArrayList<>();
        ILookupTable lookup = lookupCache.get(hostInfo.join);

        for (String[] row : lookup) {
            if (matcher.match(row[derivedIndex])) {
                String[] hostValues = new String[pkIndices.length];
                for (int i = 0; i < pkIndices.length; i++) {
                    hostValues[i] = row[pkIndices[i]];
                }
                result.add(hostValues);
            }
        }
        return result;
    }

    private TupleFilter newHostColumnFilter(TblColRef[] hostColumns, List<String[]> hostRecords) {
        if (hostRecords.size() > desc.getConfig().getParquetDerivedInThreshold()) {
            throw new UnsupportedFilterException("too many rows matching derived column filter");
        }

        if (hostRecords.isEmpty()) {
            return ConstantTupleFilter.FALSE;
        }

        if (hostColumns.length == 1) {
            List<String> values = Lists.newArrayListWithExpectedSize(hostRecords.size());
            for (String[] records : hostRecords) {
                values.add(records[0]);
            }
            return TupleFilters.compare(hostColumns[0], TupleFilter.FilterOperatorEnum.IN, values);
        }

        List<TupleFilter> ands = new ArrayList<>();
        for (String[] hostValues : hostRecords) {
            List<TupleFilter> equals = Lists.newArrayListWithCapacity(hostColumns.length);
            for (int i = 0; i < hostColumns.length; i++) {
                equals.add(TupleFilters.eq(hostColumns[i], hostValues[i]));
            }
            ands.add(TupleFilters.and(equals));
        }
        return TupleFilters.or(ands);
    }

    @Override
    public TupleFilter visitColumnCompare(final CompareTupleFilter originFilter, final TblColRef column, TupleFilter.FilterOperatorEnum op, Set<?> values, Object firstValue) {
        checkNotFilteringOnExtendedColumn(column);
        if (!desc.isDerived(column)) {
            return originFilter;
        }

        CubeDesc.DeriveInfo hostInfo = desc.getHostInfo(column);
        TblColRef[] hostCols = hostInfo.columns;

        if (hostInfo.type == CubeDesc.DeriveType.PK_FK) {
            return TupleFilters.compare(hostCols[0], op, values);
        }

        // collect rows that matches filter
        List<String[]> hostRecords = collectAllMatchingHostRecords(column, hostInfo, new DerivedColumnMatcher() {
            SingleColumnTuple tuple = new SingleColumnTuple(column);
            IFilterCodeSystem codeSystem = FilterCodeSystemFactory.getFilterCodeSystem(column.getColumnDesc().getType());

            @Override
            public boolean match(String value) {
                tuple.value = value;
                return originFilter.evaluate(tuple, codeSystem);
            }
        });

        // translate to host column filter
        return newHostColumnFilter(hostCols, hostRecords);
    }

    @Override
    public TupleFilter visitCaseCompare(CompareTupleFilter originFilter, TupleFilter.FilterOperatorEnum op, Set<?> values, Object firstValue, TupleFilterVisitor2Adaptor<TupleFilter> adaptor) {
        CaseTupleFilter caseFilter = (CaseTupleFilter)originFilter.getChildren().get(0);
        TupleFilter first = visitCase(caseFilter, caseFilter.getChildren(), caseFilter.getWhenFilters(), caseFilter.getThenFilters(), adaptor);
        ConstantTupleFilter second = new ConstantTupleFilter(values);

        CompareTupleFilter compareFilter = new CompareTupleFilter(op);
        compareFilter.addChild(first);
        compareFilter.addChild(second);

        return compareFilter;
    }

    @Override
    public TupleFilter visitColumnLike(final BuiltInFunctionTupleFilter originFilter, TblColRef column, final String pattern, final boolean reversed) {
        checkNotFilteringOnExtendedColumn(column);
        if (!desc.isDerived(column)) {
            return originFilter;
        }

        CubeDesc.DeriveInfo hostInfo = desc.getHostInfo(column);
        TblColRef[] hostCols = hostInfo.columns;

        if (hostInfo.type == CubeDesc.DeriveType.PK_FK && originFilter.getColumnContainerFilter() instanceof ColumnTupleFilter) {
            return TupleFilters.like(hostCols[0], pattern, reversed);
        }

        final BuiltInFunctionTupleFilter newFilter = new BuiltInFunctionTupleFilter(originFilter.getName());
        newFilter.addChild(new ColumnTupleFilter(column));
        newFilter.addChild(originFilter.getConstantTupleFilter());
        newFilter.setReversed(reversed);

        List<String[]> hostRecords = collectAllMatchingHostRecords(column, hostInfo, new DerivedColumnMatcher() {
            @Override
            public boolean match(String value) {
                try {
                    if (originFilter.getColumnContainerFilter() instanceof BuiltInFunctionTupleFilter) {
                        // lower(derived) like 'x'
                        BuiltInFunctionTupleFilter columnFunction = (BuiltInFunctionTupleFilter) originFilter.getColumnContainerFilter();
                        value = Objects.toString(invokeFunction(columnFunction, value));
                    }
                    boolean matched = (Boolean) newFilter.invokeFunction(value);
                    return reversed ? !matched : matched;

                } catch (Exception e) {
                    throw new UnsupportedFilterException("Failed to evaluate LIKE expression: " + e.getMessage());
                }
            }
        });

        return newHostColumnFilter(hostCols, hostRecords);
    }

    @Override
    public TupleFilter visitColumnFunction(CompareTupleFilter originFilter, final BuiltInFunctionTupleFilter function, TupleFilter.FilterOperatorEnum op, Set<?> values, Object firstValue) {
        final TblColRef column = function.getColumn();
        checkNotFilteringOnExtendedColumn(column);
        if (!desc.isDerived(column)) {
            return originFilter;
        }

        CubeDesc.DeriveInfo hostInfo = desc.getHostInfo(column);
        TblColRef[] hostCols = hostInfo.columns;

        final CompareTupleFilter newFilter = new CompareTupleFilter(op);
        newFilter.addChild(new ColumnTupleFilter(column));
        newFilter.addChild(new ConstantTupleFilter(values));

        // collect rows that matches filter
        List<String[]> hostRecords = collectAllMatchingHostRecords(column, hostInfo, new DerivedColumnMatcher() {
            SingleColumnTuple tuple = new SingleColumnTuple(column);
            IFilterCodeSystem codeSystem = FilterCodeSystemFactory.getFilterCodeSystem(column.getColumnDesc().getType());

            @Override
            public boolean match(String value) {
                tuple.value = Objects.toString(invokeFunction(function, value));
                return newFilter.evaluate(tuple, codeSystem);
            }
        });

        // translate to host column filter
        return newHostColumnFilter(hostCols, hostRecords);
    }

    private Object invokeFunction(BuiltInFunctionTupleFilter function, String value) {
        // special case for extract(timeUnit from derived)
        if ("EXTRACT".equals(function.getName())) {
            if (value == null) {
                return null;
            }
            ConstantTupleFilter constantTupleFilter = function.getConstantTupleFilter();
            String timeUnit = (String) constantTupleFilter.getValues().iterator().next();
            return TimeUtil.extract(timeUnit, value);
        }

        if (!function.isValid()) {
            throw new UnsupportedOperationException("Function '" + function.getName() + "' is not supported");
        }

        try {
            return function.invokeFunction(value);
        } catch (Exception e) {
            throw new UnsupportedFilterException("Function '" + function.getName() + "' failed to execute: " + e.getMessage());
        }
    }

    @Override
    public TupleFilter visitAnd(LogicalTupleFilter originFilter, List<? extends TupleFilter> children, TupleFilterVisitor2Adaptor<TupleFilter> adaptor) {
        List<TupleFilter> newChildren = Lists.newArrayList();
        for (TupleFilter child : children) {
            TupleFilter newChild = child.accept(adaptor);
            newChildren.add(newChild);
        }
        return TupleFilters.and(newChildren);
    }

    @Override
    public TupleFilter visitOr(LogicalTupleFilter originFilter, List<? extends TupleFilter> children, TupleFilterVisitor2Adaptor<TupleFilter> adaptor) {
        List<TupleFilter> newChildren = Lists.newArrayList();
        for (TupleFilter child : children) {
            TupleFilter newChild = child.accept(adaptor);
            newChildren.add(newChild);
        }
        return TupleFilters.or(newChildren);
    }

    @Override
    public TupleFilter visitNot(LogicalTupleFilter originFilter, TupleFilter child, TupleFilterVisitor2Adaptor<TupleFilter> adaptor) {
        TupleFilter newChild = child.accept(adaptor);
        return TupleFilters.not(newChild);
    }

    @Override
    public TupleFilter visitCase(CaseTupleFilter originFilter, List<? extends TupleFilter> children, List<? extends TupleFilter> whenFilter, List<? extends TupleFilter> thenFilter, TupleFilterVisitor2Adaptor<TupleFilter> adaptor) {
        List<TupleFilter> newChildren = Lists.newArrayList();
        for (TupleFilter child : children) {
            TupleFilter newChild = child.accept(adaptor);
            newChildren.add(newChild);
        }
        CaseTupleFilter caseFilter = new CaseTupleFilter();
        caseFilter.addChildren(newChildren);
        return caseFilter;
    }

    @Override
    public TupleFilter visitColumn(ColumnTupleFilter originFilter, TblColRef column) {
        return originFilter;
    }

    @Override
    public TupleFilter visitSecondColumnCompare(CompareTupleFilter originFilter) {
        unevaluatableColumnCollector.add(originFilter.getColumn());
        return ConstantTupleFilter.TRUE;
    }

    @Override
    public TupleFilter visitConstant(ConstantTupleFilter originFilter) {
        return originFilter;
    }

    @Override
    public TupleFilter visitUnsupported(TupleFilter originFilter) {
        TupleFilter.collectColumns(originFilter, unevaluatableColumnCollector);
        return ConstantTupleFilter.TRUE;
    }

    interface DerivedColumnMatcher {
        boolean match(String value);
    }

    private static class SingleColumnTuple implements IEvaluatableTuple {
        TblColRef col;
        String value;

        SingleColumnTuple(TblColRef col) {
            this.col = col;
        }

        @Override
        public Object getValue(TblColRef col) {
            if (this.col.equals(col))
                return value;
            else
                throw new IllegalArgumentException("unexpected column " + col);
        }

    }
}

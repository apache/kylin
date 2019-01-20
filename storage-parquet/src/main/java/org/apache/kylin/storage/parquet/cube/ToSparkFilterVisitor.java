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

import org.apache.kylin.metadata.filter.BuiltInFunctionTupleFilter;
import org.apache.kylin.metadata.filter.CaseTupleFilter;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilterVisitor2;
import org.apache.kylin.metadata.filter.TupleFilterVisitor2Adaptor;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.parquet.NameMapping;
import org.apache.spark.sql.Column;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.dayofmonth;
import static org.apache.spark.sql.functions.dayofweek;
import static org.apache.spark.sql.functions.dayofyear;
import static org.apache.spark.sql.functions.hour;
import static org.apache.spark.sql.functions.length;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.lower;
import static org.apache.spark.sql.functions.minute;
import static org.apache.spark.sql.functions.month;
import static org.apache.spark.sql.functions.not;
import static org.apache.spark.sql.functions.quarter;
import static org.apache.spark.sql.functions.second;
import static org.apache.spark.sql.functions.substring;
import static org.apache.spark.sql.functions.upper;
import static org.apache.spark.sql.functions.weekofyear;
import static org.apache.spark.sql.functions.when;
import static org.apache.spark.sql.functions.year;

import java.util.List;
import java.util.Set;

public class ToSparkFilterVisitor implements TupleFilterVisitor2<Column> {
    private final NameMapping nameMapping;
    private final List<TblColRef> dimensions;

    public ToSparkFilterVisitor(NameMapping nameMapping, List<TblColRef> dimensions) {
        this.nameMapping = nameMapping;
        this.dimensions = dimensions;
    }

    @Override
    public Column visitColumnCompare(CompareTupleFilter originFilter, TblColRef column, TupleFilter.FilterOperatorEnum op, Set values, Object firstValue) {
        assert dimensions.contains(column);
        assert !values.isEmpty() || op == TupleFilter.FilterOperatorEnum.ISNOTNULL ||  op == TupleFilter.FilterOperatorEnum.ISNULL;

        String columnName = nameMapping.getDimFieldName(column);

        return makeCompareFilter(col(columnName), op, values, firstValue);
    }

    @Override
    public Column visitCaseCompare(CompareTupleFilter originFilter, TupleFilter.FilterOperatorEnum op, Set<?> values, Object firstValue, TupleFilterVisitor2Adaptor<Column> adaptor) {
        CaseTupleFilter caseFilter = (CaseTupleFilter)originFilter.getChildren().get(0);
        Column caseColumn = visitCase(caseFilter, caseFilter.getChildren(), caseFilter.getWhenFilters(), caseFilter.getThenFilters(), adaptor);

        return makeCompareFilter(caseColumn, op, values, firstValue);
    }

    @Override
    public Column visitColumnLike(BuiltInFunctionTupleFilter originFilter, TblColRef column, String pattern, boolean reversed) {
        assert dimensions.contains(column);

        Column functionColumn = null;
        // lower(c) like 'x'
        if (originFilter.getColumnContainerFilter() instanceof BuiltInFunctionTupleFilter) {
            functionColumn = convertFunction((BuiltInFunctionTupleFilter)originFilter.getColumnContainerFilter(), col(nameMapping.getDimFieldName(column)));
        } else {
            functionColumn = col(nameMapping.getDimFieldName(column));
        }

        if (functionColumn == null) {
            return visitUnsupported(originFilter);
        }

        functionColumn = functionColumn.like(pattern);

        return reversed ? not(functionColumn) : functionColumn;
    }

    @Override
    public Column visitNot(LogicalTupleFilter originFilter, TupleFilter child, TupleFilterVisitor2Adaptor<Column> adaptor) {
        return not(child.accept(adaptor));
    }

    @Override
    public Column visitCase(CaseTupleFilter originFilter, List<? extends TupleFilter> children, List<? extends TupleFilter> whenFilter, List<? extends TupleFilter> thenFilter, TupleFilterVisitor2Adaptor<Column> adaptor) {
        Column caseColumn = when(whenFilter.get(0).accept(adaptor), thenFilter.get(0).accept(adaptor));
        for (int i = 1; i < originFilter.getWhenFilters().size() - 1; i++) {
            caseColumn = caseColumn.when(whenFilter.get(i).accept(adaptor), thenFilter.get(i).accept(adaptor));
        }
        caseColumn = caseColumn.otherwise(children.get(children.size() - 1).accept(adaptor));

        return caseColumn;
    }

    @Override
    public Column visitColumn(ColumnTupleFilter originFilter, TblColRef column) {
        return col(nameMapping.getDimFieldName(column));
    }

    @Override
    public Column visitSecondColumnCompare(CompareTupleFilter originFilter) {
        return lit(true);
    }

    @Override
    public Column visitConstant(ConstantTupleFilter originFilter) {
        if (originFilter == ConstantTupleFilter.TRUE || (originFilter.getValues().size() == 1 && "true".equals(originFilter.getValues().iterator().next()))) {
            return lit(true);
        }
        if (originFilter == ConstantTupleFilter.FALSE || (originFilter.getValues().size() == 1 && "false".equals(originFilter.getValues().iterator().next()))) {
            return lit(false);
        }

        if (originFilter.getValues().size() == 1) {
            return lit(originFilter.getValues().iterator().next());
        } else {
            return lit(originFilter.getValues().toArray());
        }
    }

    @Override
    public Column visitUnsupported(TupleFilter originFilter) {
        return lit(true);
    }

    @Override
    public Column visitOr(LogicalTupleFilter originFilter, List<? extends TupleFilter> children, TupleFilterVisitor2Adaptor<Column> adaptor) {
        assert children.size() > 1;

        Column addColumn = children.get(0).accept(adaptor);
        for (int i = 1; i < children.size(); i++) {
            addColumn = addColumn.or(children.get(i).accept(adaptor));
        }
        return addColumn;
    }

    @Override
    public Column visitAnd(LogicalTupleFilter originFilter, List<? extends TupleFilter> children, TupleFilterVisitor2Adaptor<Column> adaptor) {
        assert children.size() > 1;

        Column addColumn = children.get(0).accept(adaptor);
        for (int i = 1; i < children.size(); i++) {
            addColumn = addColumn.and(children.get(i).accept(adaptor));
        }
        return addColumn;
    }

    @Override
    public Column visitColumnFunction(CompareTupleFilter originFilter, BuiltInFunctionTupleFilter function, TupleFilter.FilterOperatorEnum op, Set values, Object firstValue) {
        assert dimensions.contains(function.getColumn());

        Column column = convertFunctionFilter(function);

        return makeCompareFilter(column, op, values, firstValue);
    }

    private Column convertFunctionFilter(BuiltInFunctionTupleFilter parent) {
        TupleFilter columnContainer = parent.getColumnContainerFilter();
        Column child = null;
        if (columnContainer instanceof BuiltInFunctionTupleFilter) {
            child = convertFunctionFilter((BuiltInFunctionTupleFilter)columnContainer);
        }
        // parent = innerest, child = null
        if (child == null) {
            return convertFunction(parent, col(nameMapping.getDimFieldName(parent.getColumn())));
        } else {
            return convertFunction(parent, child);
        }
    }

    private Column convertFunction(BuiltInFunctionTupleFilter filter, Column column) {
        switch (filter.getName()) {
            case "UPPER":
                return upper(column);
            case "LOWER":
                return lower(column);
            case "CHAR_LENGTH":
                return length(column);
            case "SUBSTRING":
                List<? extends TupleFilter> children = filter.getChildren();
                assert children.size() == 3;
                int pos = Integer.valueOf((String)children.get(1).getValues().iterator().next());
                int len = Integer.valueOf((String)children.get(2).getValues().iterator().next());
                return substring(column, pos, len);
            case "EXTRACT": {
                ConstantTupleFilter constantTupleFilter = filter.getConstantTupleFilter();
                String timeUnit = (String) constantTupleFilter.getValues().iterator().next();
                switch (timeUnit) {
                    case "YEAR":
                        return year(column);
                    case "QUARTER":
                        return quarter(column);
                    case "MONTH":
                        return month(column);
                    case "WEEK":
                        return weekofyear(column);
                    case "DOY":
                        return dayofyear(column);
                    case "DAY":
                        return dayofmonth(column);
                    case "DOW":
                        return dayofweek(column);
                    case "HOUR":
                        return hour(column);
                    case "MINUTE":
                        return minute(column);
                    case "SECOND":
                        return second(column);
                    default:
                        throw new IllegalArgumentException("unsupported timeunit: " + timeUnit);
                }
            }
            case "||":
                String con = (String)filter.getConstantTupleFilter().getValues().iterator().next();
                if (filter.getColPosition() == 0) {
                    return concat(column, lit(con));
                } else {
                    return concat(lit(con), column);
                }
            default:
                throw new IllegalArgumentException("unsupported function: " + filter.getName());

        }
    }

    private Column makeCompareFilter(Column column, TupleFilter.FilterOperatorEnum op, Set values, Object firstValue) {
        switch (op) {
            case EQ:
                return column.equalTo(firstValue);
            case NEQ:
                return column.notEqual(firstValue);
            case GT:
                return column.gt(firstValue);
            case LT:
                return column.lt(firstValue);
            case GTE:
                return column.geq(firstValue);
            case LTE:
                return column.leq(firstValue);
            case ISNULL:
                return column.isNull();
            case ISNOTNULL:
                return column.isNotNull();
            case IN:
                return column.isin(values.toArray());
            case NOTIN:
                return not(column.isin(values.toArray()));
            default:
                throw new IllegalArgumentException("Illegal column compare op: " + op);
        }
    }
}

























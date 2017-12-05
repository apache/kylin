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

package org.apache.kylin.metadata.filter;

import org.apache.kylin.metadata.filter.TupleFilter.FilterOperatorEnum;
import org.apache.kylin.metadata.model.TblColRef;

import java.util.Set;

public class TupleFilterVisitor2Adaptor<R> implements TupleFilterVisitor<R> {
    private final TupleFilterVisitor2<R> visitor;

    public TupleFilterVisitor2Adaptor(TupleFilterVisitor2<R> visitor) {
        this.visitor = visitor;
    }

    @Override
    public R visitCase(CaseTupleFilter filter) {
        throw new UnsupportedOperationException("visitCase");
    }

    @Override
    public R visitColumn(ColumnTupleFilter filter) {
        // leaf node like column should never be visited
        throw new UnsupportedOperationException("visitColumn");
    }

    @Override
    public R visitCompare(CompareTupleFilter filter) {
        TblColRef col = filter.getColumn();
        FunctionTupleFilter function = filter.getFunction();
        Set<?> values = filter.getValues();

        // TODO consider case when filter.secondColumn != null

        if (col != null && (!values.isEmpty() || filter.getOperator() == FilterOperatorEnum.ISNOTNULL || filter.getOperator() == FilterOperatorEnum.ISNULL)) {
            return visitor.visitColumnCompare(filter, col, filter.operator, values, filter.getFirstValue());
        }

        if (function instanceof BuiltInFunctionTupleFilter) {
            BuiltInFunctionTupleFilter functionFilter = (BuiltInFunctionTupleFilter) function;
            if (functionFilter.getColumn() != null && !values.isEmpty()) {
                return visitor.visitColumnFunction(filter, functionFilter, filter.operator, values, filter.getFirstValue());
            }
        }

        // TODO consider MassInTupleFilter?
        return visitor.visitUnsupported(filter);
    }

    @Override
    public R visitConstant(ConstantTupleFilter filter) {
        if (filter == ConstantTupleFilter.TRUE || filter == ConstantTupleFilter.FALSE) {
            return visitor.visitConstant(filter);
        }
        throw new AssertionError("visitConstant"); // should never traverse a non-root constant filter
    }

    @Override
    public R visitDynamic(DynamicTupleFilter filter) {
        throw new UnsupportedOperationException("visitDynamic");
    }

    @Override
    public R visitFunction(FunctionTupleFilter filter) {
        if (!(filter instanceof BuiltInFunctionTupleFilter)) {
            return visitor.visitUnsupported(filter);
        }
        BuiltInFunctionTupleFilter filter2 = (BuiltInFunctionTupleFilter) filter;
        if ("LIKE".equals(filter2.name) && filter2.getColumn() != null && filter2.getConstantTupleFilter() != null) {
            String pattern = (String) filter2.getConstantTupleFilter().getValues().iterator().next();
            return visitor.visitColumnLike(filter2, filter2.getColumn(), pattern, filter2.isReversed());
        }
        return visitor.visitUnsupported(filter);
    }

    @Override
    public R visitLogical(LogicalTupleFilter filter) {
        if (filter.operator == FilterOperatorEnum.AND) {
            return visitor.visitAnd(filter, filter.getChildren(), this);
        }
        if (filter.operator == FilterOperatorEnum.OR) {
            return visitor.visitOr(filter, filter.getChildren(), this);
        }
        if (filter.operator == FilterOperatorEnum.NOT) {
            return visitor.visitNot(filter, filter.getChildren().get(0), this);
        }
        throw new AssertionError("Illegal operator for LogicalTupleFilter: " + filter.operator);
    }

    @Override
    public R visitExtract(ExtractTupleFilter filter) {
        throw new UnsupportedOperationException("visitExtract");
    }

    @Override
    public R visitUnsupported(UnsupportedTupleFilter filter) {
        return visitor.visitUnsupported(filter);
    }
}

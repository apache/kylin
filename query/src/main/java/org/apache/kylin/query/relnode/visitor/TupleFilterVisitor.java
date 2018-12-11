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

package org.apache.kylin.query.relnode.visitor;

import java.math.BigDecimal;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.util.NlsString;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.metadata.filter.CaseTupleFilter;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.DynamicTupleFilter;
import org.apache.kylin.metadata.filter.ExtractTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.UnsupportedTupleFilter;
import org.apache.kylin.metadata.filter.function.Functions;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.ColumnRowType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class TupleFilterVisitor extends RexVisitorImpl<TupleFilter> {

    final ColumnRowType inputRowType;

    public TupleFilterVisitor(ColumnRowType inputRowType) {
        super(true);
        this.inputRowType = inputRowType;
    }

    @Override
    public TupleFilter visitCall(RexCall call) {
        TupleFilter filter = null;
        SqlOperator op = call.getOperator();
        switch (op.getKind()) {
        case AND:
            filter = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.AND);
            break;
        case OR:
            filter = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.OR);
            break;
        case NOT:
            filter = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.NOT);
            break;
        case EQUALS:
            filter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.EQ);
            break;
        case GREATER_THAN:
            filter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.GT);
            break;
        case LESS_THAN:
            filter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.LT);
            break;
        case GREATER_THAN_OR_EQUAL:
            filter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.GTE);
            break;
        case LESS_THAN_OR_EQUAL:
            filter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.LTE);
            break;
        case NOT_EQUALS:
            filter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.NEQ);
            break;
        case IS_NULL:
            filter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.ISNULL);
            break;
        case IS_NOT_NULL:
            filter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.ISNOTNULL);
            break;
        case CAST:
        case REINTERPRET:
            // NOTE: use child directly
            break;
        case CASE:
            filter = new CaseTupleFilter();
            break;
        case OTHER:
            if (op.getName().equalsIgnoreCase("extract_date")) {
                filter = new ExtractTupleFilter(TupleFilter.FilterOperatorEnum.EXTRACT);
            } else {
                filter = Functions.getFunctionTupleFilter(op.getName());
            }
            break;
        case LIKE:
        case OTHER_FUNCTION:
            filter = Functions.getFunctionTupleFilter(op.getName());
            break;
        case PLUS:
        case MINUS:
        case TIMES:
        case DIVIDE:
            TupleFilter f = dealWithTrivialExpr(call);
            if (f != null) {
                // is a trivial expr
                return f;
            }
            filter = new UnsupportedTupleFilter(TupleFilter.FilterOperatorEnum.UNSUPPORTED);
            break;
        default:
            filter = new UnsupportedTupleFilter(TupleFilter.FilterOperatorEnum.UNSUPPORTED);
        }

        for (RexNode operand : call.operands) {
            TupleFilter childFilter = operand.accept(this);
            if (filter == null) {
                filter = cast(childFilter, call.type);
            } else {
                filter.addChild(childFilter);
            }
        }

        if (op.getKind() == SqlKind.OR) {
            CompareTupleFilter inFilter = mergeToInClause(filter);
            if (inFilter != null) {
                filter = inFilter;
            }
        } else if (op.getKind() == SqlKind.NOT) {
            assert (filter.getChildren().size() == 1);
            filter = filter.getChildren().get(0).reverse();
        }
        return filter;
    }

    //KYLIN-2597 - Deal with trivial expression in filters like x = 1 + 2
    private TupleFilter dealWithTrivialExpr(RexCall call) {
        ImmutableList<RexNode> operators = call.operands;
        if (operators.size() != 2) {
            return null;
        }

        BigDecimal left = null;
        BigDecimal right = null;
        for (RexNode rexNode : operators) {
            if (!(rexNode instanceof RexLiteral)) {
                return null;// only trivial expr with constants
            }

            RexLiteral temp = (RexLiteral) rexNode;
            if (temp.getType().getFamily() != SqlTypeFamily.NUMERIC || !(temp.getValue() instanceof BigDecimal)) {
                return null;// only numeric constants now
            }

            if (left == null) {
                left = (BigDecimal) temp.getValue();
            } else {
                right = (BigDecimal) temp.getValue();
            }
        }

        Preconditions.checkNotNull(left);
        Preconditions.checkNotNull(right);

        switch (call.op.getKind()) {
        case PLUS:
            return new ConstantTupleFilter(left.add(right).toString());
        case MINUS:
            return new ConstantTupleFilter(left.subtract(right).toString());
        case TIMES:
            return new ConstantTupleFilter(left.multiply(right).toString());
        case DIVIDE:
            return new ConstantTupleFilter(left.divide(right).toString());
        default:
            return null;
        }
    }

    private TupleFilter cast(TupleFilter filter, RelDataType type) {
        if ((filter instanceof ConstantTupleFilter) == false) {
            return filter;
        }

        ConstantTupleFilter constFilter = (ConstantTupleFilter) filter;

        if (type.getFamily() == SqlTypeFamily.DATE || type.getFamily() == SqlTypeFamily.DATETIME
                || type.getFamily() == SqlTypeFamily.TIMESTAMP) {
            List<String> newValues = Lists.newArrayList();
            for (Object v : constFilter.getValues()) {
                if (v == null)
                    newValues.add(null);
                else
                    newValues.add(String.valueOf(DateFormat.stringToMillis(v.toString())));
            }
            constFilter = new ConstantTupleFilter(newValues);
        }
        return constFilter;
    }

    private CompareTupleFilter mergeToInClause(TupleFilter filter) {
        List<? extends TupleFilter> children = filter.getChildren();
        TblColRef inColumn = null;
        List<Object> inValues = new LinkedList<Object>();
        Map<String, Object> dynamicVariables = new HashMap<>();
        for (TupleFilter child : children) {
            if (child.getOperator() == TupleFilter.FilterOperatorEnum.EQ) {
                CompareTupleFilter compFilter = (CompareTupleFilter) child;
                TblColRef column = compFilter.getColumn();
                if (inColumn == null) {
                    inColumn = column;
                }

                if (column == null || !column.equals(inColumn)) {
                    return null;
                }
                inValues.addAll(compFilter.getValues());
                dynamicVariables.putAll(compFilter.getVariables());
            } else {
                return null;
            }
        }

        children.clear();

        CompareTupleFilter inFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.IN);
        inFilter.addChild(new ColumnTupleFilter(inColumn));
        inFilter.addChild(new ConstantTupleFilter(inValues));
        inFilter.getVariables().putAll(dynamicVariables);
        return inFilter;
    }

    @Override
    public TupleFilter visitLocalRef(RexLocalRef localRef) {
        throw new UnsupportedOperationException("local ref:" + localRef);
    }

    @Override
    public TupleFilter visitInputRef(RexInputRef inputRef) {
        TblColRef column = inputRowType.getColumnByIndex(inputRef.getIndex());
        ColumnTupleFilter filter = new ColumnTupleFilter(column);
        return filter;
    }

    @SuppressWarnings("unused")
    private String normToTwoDigits(int i) {
        if (i < 10)
            return "0" + i;
        else
            return "" + i;
    }

    @Override
    public TupleFilter visitLiteral(RexLiteral literal) {
        String strValue = null;
        Object literalValue = literal.getValue();
        if (literalValue instanceof NlsString) {
            strValue = ((NlsString) literalValue).getValue();
        } else if (literalValue instanceof GregorianCalendar) {
            GregorianCalendar g = (GregorianCalendar) literalValue;
            strValue = Long.toString(g.getTimeInMillis());
        } else if (literalValue instanceof TimeUnitRange) {
            // Extract(x from y) in where clause
            strValue = ((TimeUnitRange) literalValue).name();
        } else if (literalValue == null) {
            strValue = null;
        } else {
            strValue = literalValue.toString();
        }
        TupleFilter filter = new ConstantTupleFilter(strValue);
        return filter;
    }

    @Override
    public TupleFilter visitDynamicParam(RexDynamicParam dynamicParam) {
        String name = dynamicParam.getName();
        TupleFilter filter = new DynamicTupleFilter(name);
        return filter;
    }
}

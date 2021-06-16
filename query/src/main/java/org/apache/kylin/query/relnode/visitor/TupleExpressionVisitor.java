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

import java.util.List;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlCastFunction;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.calcite.util.NlsString;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.expression.BinaryTupleExpression;
import org.apache.kylin.metadata.expression.CaseTupleExpression;
import org.apache.kylin.metadata.expression.ColumnTupleExpression;
import org.apache.kylin.metadata.expression.NumberTupleExpression;
import org.apache.kylin.metadata.expression.RexCallTupleExpression;
import org.apache.kylin.metadata.expression.StringTupleExpression;
import org.apache.kylin.metadata.expression.TupleExpression;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.FilterOptimizeTransformer;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.ColumnRowType;
import org.apache.kylin.query.util.RexUtil;

import com.google.common.collect.Lists;

public class TupleExpressionVisitor extends RexVisitorImpl<TupleExpression> {
    final ColumnRowType inputRowType;
    final boolean ifVerify;

    public TupleExpressionVisitor(ColumnRowType inputRowType, boolean ifVerify) {
        super(true);
        this.inputRowType = inputRowType;
        this.ifVerify = ifVerify;
    }

    @Override
    public TupleExpression visitCall(RexCall call) {
        SqlOperator op = call.getOperator();
        if (op instanceof SqlCastFunction) {
            return call.getOperands().get(0).accept(this);
        } else if (op instanceof SqlUserDefinedFunction) {
            if (op.getName().equals("QUARTER")) {
                return visitFirstRexInputRef(call);
            }
        }

        TupleExpression tupleExpression;
        switch (op.getKind()) {
        case PLUS:
            tupleExpression = getBinaryTupleExpression(call, TupleExpression.ExpressionOperatorEnum.PLUS);
            break;
        case MINUS:
            tupleExpression = getBinaryTupleExpression(call, TupleExpression.ExpressionOperatorEnum.MINUS);
            break;
        case TIMES:
            tupleExpression = getBinaryTupleExpression(call, TupleExpression.ExpressionOperatorEnum.MULTIPLE);
            break;
        case DIVIDE:
            tupleExpression = getBinaryTupleExpression(call, TupleExpression.ExpressionOperatorEnum.DIVIDE);
            break;
        case CASE:
            tupleExpression = getCaseTupleExpression(call);
            break;
        default:
            tupleExpression = getRexCallTupleExpression(call);
        }
        if (ifVerify) {
            tupleExpression.verify();
        }
        return tupleExpression;
    }

    private BinaryTupleExpression getBinaryTupleExpression(RexCall call, TupleExpression.ExpressionOperatorEnum op) {
        assert call.operands.size() == 2;
        TupleExpression left = call.operands.get(0).accept(this);
        TupleExpression right = call.operands.get(1).accept(this);
        BinaryTupleExpression tuple = new BinaryTupleExpression(op, Lists.newArrayList(left, right));
        tuple.setDigest(call.toString());
        return tuple;
    }

    private CaseTupleExpression getCaseTupleExpression(RexCall call) {
        List<Pair<TupleFilter, TupleExpression>> whenList = Lists
                .newArrayListWithExpectedSize(call.operands.size() / 2);
        TupleExpression elseExpr = null;

        TupleFilterVisitor filterVistor = new TupleFilterVisitor(inputRowType);
        for (int i = 0; i < call.operands.size() - 1; i += 2) {
            if (call.operands.get(i) instanceof RexCall) {
                RexCall whenCall = (RexCall) call.operands.get(i);
                CompareTupleFilter.CompareResultType compareResultType = RexUtil.getCompareResultType(whenCall);
                if (compareResultType == CompareTupleFilter.CompareResultType.AlwaysTrue) {
                    elseExpr = call.operands.get(i + 1).accept(this);
                    break;
                } else if (compareResultType == CompareTupleFilter.CompareResultType.AlwaysFalse) {
                    continue;
                }
                TupleFilter whenFilter = whenCall.accept(filterVistor);
                whenFilter = new FilterOptimizeTransformer().transform(whenFilter);

                TupleExpression thenExpr = call.operands.get(i + 1).accept(this);
                whenList.add(new Pair<>(whenFilter, thenExpr));
            }
        }
        if (elseExpr == null && call.operands.size() % 2 == 1) {
            RexNode elseNode = call.operands.get(call.operands.size() - 1);
            if (!(elseNode instanceof RexLiteral && ((RexLiteral) elseNode).getValue() == null)) {
                elseExpr = elseNode.accept(this);
            }
        }
        CaseTupleExpression tuple = new CaseTupleExpression(whenList, elseExpr);
        tuple.setDigest(call.toString());
        return tuple;
    }

    private RexCallTupleExpression getRexCallTupleExpression(RexCall call) {
        List<TupleExpression> children = Lists.newArrayListWithExpectedSize(call.getOperands().size());
        for (RexNode rexNode : call.operands) {
            children.add(rexNode.accept(this));
        }
        RexCallTupleExpression tuple = new RexCallTupleExpression(children);
        tuple.setDigest(call.toString());
        return tuple;
    }

    @Override
    public TupleExpression visitLocalRef(RexLocalRef localRef) {
        throw new UnsupportedOperationException("local ref:" + localRef);
    }

    @Override
    public TupleExpression visitInputRef(RexInputRef inputRef) {
        int index = inputRef.getIndex();
        // check it for rewrite count
        if (index < inputRowType.size()) {
            TblColRef column = inputRowType.getColumnByIndex(index);
            TupleExpression tuple;
            if (column.getSubTupleExps() != null) {
                tuple = new RexCallTupleExpression(column.getSubTupleExps());
            } else {
                tuple = new ColumnTupleExpression(column);
            }
            tuple.setDigest(inputRef.toString());
            return tuple;
        } else {
            throw new IllegalStateException("Can't find " + inputRef + " from child columnrowtype");
        }
    }

    public TupleExpression visitFirstRexInputRef(RexCall call) {
        for (RexNode operand : call.getOperands()) {
            if (operand instanceof RexInputRef) {
                return visitInputRef((RexInputRef) operand);
            }
            if (operand instanceof RexCall) {
                TupleExpression r = visitFirstRexInputRef((RexCall) operand);
                if (r != null)
                    return r;
            }
        }
        return null;
    }

    @Override
    public TupleExpression visitLiteral(RexLiteral literal) {
        TupleExpression tuple;
        Object value = literal.getValue();
        if (value instanceof Number) {
            tuple = new NumberTupleExpression(value);
        } else {
            if (value == null) {
                tuple = new StringTupleExpression(null);
            } else if (value instanceof NlsString) {
                tuple = new StringTupleExpression(((NlsString) value).getValue());
            } else {
                tuple = new StringTupleExpression(value.toString());
            }
        }
        tuple.setDigest(literal.toString());
        return tuple;
    }
}

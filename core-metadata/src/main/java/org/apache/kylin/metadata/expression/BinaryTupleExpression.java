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

package org.apache.kylin.metadata.expression;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.kylin.common.util.DecimalUtil;
import org.apache.kylin.exception.QueryOnCubeException;
import org.apache.kylin.metadata.filter.IFilterCodeSystem;
import org.apache.kylin.metadata.tuple.IEvaluatableTuple;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

public class BinaryTupleExpression extends TupleExpression {

    public BinaryTupleExpression(ExpressionOperatorEnum op) {
        this(op, Lists.<TupleExpression> newArrayListWithExpectedSize(2));
    }

    public BinaryTupleExpression(ExpressionOperatorEnum op, List<TupleExpression> exprs) {
        super(op, exprs);

        boolean opGood = (op == ExpressionOperatorEnum.PLUS || op == ExpressionOperatorEnum.MINUS
                || op == ExpressionOperatorEnum.MULTIPLE || op == ExpressionOperatorEnum.DIVIDE);
        if (opGood == false)
            throw new IllegalArgumentException("Unsupported operator " + op);
    }

    @Override
    public boolean ifForDynamicColumn() {
        return ifAbleToPushDown();
    }

    @Override
    public void verify() {
        switch (operator) {
        case MULTIPLE:
            verifyMultiply();
            break;
        case DIVIDE:
            verifyDivide();
            break;
        default:
        }
    }

    private void verifyMultiply() {
        if (ExpressionColCollector.collectMeasureColumns(getLeft()).size() > 0 //
                && ExpressionColCollector.collectMeasureColumns(getRight()).size() > 0) {
            throw new QueryOnCubeException(
                    "That both of the two sides of the BinaryTupleExpression own columns is not supported for "
                            + operator.toString());
        }
    }

    private void verifyDivide() {
        if (ExpressionColCollector.collectMeasureColumns(getRight()).size() > 0) {
            throw new QueryOnCubeException(
                    "That the right side of the BinaryTupleExpression owns columns is not supported for "
                            + operator.toString());
        }
    }

    @Override
    public BigDecimal calculate(IEvaluatableTuple tuple, IFilterCodeSystem<?> cs) {
        assert children.size() == 2;
        BigDecimal left = DecimalUtil.toBigDecimal(getLeft().calculate(tuple, cs));
        if (left == null)
            return null;
        BigDecimal right = DecimalUtil.toBigDecimal(getRight().calculate(tuple, cs));
        if (right == null)
            return null;
        switch (operator) {
        case PLUS:
            return left.add(right);
        case MINUS:
            return left.subtract(right);
        case MULTIPLE:
            return left.multiply(right);
        case DIVIDE:
            return left.divide(right);
        default:
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public TupleExpression accept(ExpressionVisitor visitor) {
        return visitor.visitBinary(this);
    }

    @Override
    public void serialize(IFilterCodeSystem<?> cs, ByteBuffer buffer) {
    }

    @Override
    public void deserialize(IFilterCodeSystem<?> cs, ByteBuffer buffer) {
    }

    public TupleExpression getLeft() {
        return children.get(0);
    }

    public TupleExpression getRight() {
        return children.get(1);
    }

    public String toString() {
        return operator.toString() + "(" + getLeft().toString() + "," + getRight().toString() + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        BinaryTupleExpression that = (BinaryTupleExpression) o;

        if (operator != that.operator)
            return false;
        return children.equals(that.children);
    }

    @Override
    public int hashCode() {
        int result = operator != null ? operator.hashCode() : 0;
        result = 31 * result + (children != null ? children.hashCode() : 0);
        return result;
    }
}

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

import org.apache.kylin.exception.QueryOnCubeException;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.filter.IFilterCodeSystem;
import org.apache.kylin.metadata.tuple.IEvaluatableTuple;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

public class BinaryTupleExpression extends TupleExpression {

    public BinaryTupleExpression(DataType dataType, ExpressionOperatorEnum op) {
        this(dataType, op, Lists.<TupleExpression> newArrayListWithExpectedSize(2));
    }

    public BinaryTupleExpression(ExpressionOperatorEnum op, TupleExpression left, TupleExpression right) {
        this(referDataType(op, left, right), op, Lists.newArrayList(left, right));
    }

    public BinaryTupleExpression(DataType dataType, ExpressionOperatorEnum op, List<TupleExpression> exprs) {
        super(dataType, op, exprs);

        checkBinaryOp(op, exprs);
    }

    @Override
    public void addChild(TupleExpression child) {
        assert child.getDataType().isNumberFamily();
        super.addChild(child);
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
    public Object calculate(IEvaluatableTuple tuple, IFilterCodeSystem<?> cs) {
        Object left = referValue(getLeft().calculate(tuple, cs));
        if (left == null)
            return null;
        Object right = referValue(getRight().calculate(tuple, cs));
        if (right == null)
            return null;

        if (dataType.isDecimal()) {
            BigDecimal leftV = (BigDecimal) left;
            BigDecimal rightV = (BigDecimal) right;
            switch (operator) {
            case PLUS:
                return leftV.add(rightV);
            case MINUS:
                return leftV.subtract(rightV);
            case MULTIPLE:
                return leftV.multiply(rightV);
            case DIVIDE:
                return leftV.divide(rightV);
            default:
                throw new UnsupportedOperationException();
            }
        } else if (dataType.isIntegerFamily()) {
            long leftV = (long) left;
            long rightV = (long) right;
            switch (operator) {
            case PLUS:
                return leftV + rightV;
            case MINUS:
                return leftV - rightV;
            case MULTIPLE:
                return leftV * rightV;
            case DIVIDE:
                return leftV / rightV;
            default:
                throw new UnsupportedOperationException();
            }
        } else {
            double leftV = (double) left;
            double rightV = (double) right;
            switch (operator) {
            case PLUS:
                return leftV + rightV;
            case MINUS:
                return leftV - rightV;
            case MULTIPLE:
                return leftV * rightV;
            case DIVIDE:
                return leftV / rightV;
            default:
                throw new UnsupportedOperationException();
            }
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

    private static void checkBinaryOp(ExpressionOperatorEnum op, List<TupleExpression> exprs) {
        boolean opGood = (op == ExpressionOperatorEnum.PLUS || op == ExpressionOperatorEnum.MINUS
                || op == ExpressionOperatorEnum.MULTIPLE || op == ExpressionOperatorEnum.DIVIDE);
        if (!opGood)
            throw new IllegalArgumentException("Unsupported operator " + op);
    }

    public static DataType referDataType(ExpressionOperatorEnum op, TupleExpression left, TupleExpression right) {
        checkBinaryOp(op, Lists.newArrayList(left, right));

        DataType dataType = TupleExpression.referDataType(left.getDataType(), right.getDataType());
        if (op == ExpressionOperatorEnum.DIVIDE && dataType.isIntegerFamily()) {
            dataType = DataType.getType("double");
        }
        return dataType;
    }
}
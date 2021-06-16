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

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.filter.IFilterCodeSystem;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

public class TupleExpressionSerializer {

    private static final Logger logger = LoggerFactory.getLogger(TupleExpressionSerializer.class);

    public interface Decorator {
        TblColRef mapCol(TblColRef col);

        TupleExpression convertInnerExpression(TupleExpression expression);

        TupleFilter convertInnerFilter(TupleFilter filter);
    }

    private static class Serializer implements ExpressionVisitor {
        private final Decorator decorator;
        private final IFilterCodeSystem<?> cs;
        private final ByteBuffer buffer;

        private Serializer(Decorator decorator, IFilterCodeSystem<?> cs, ByteBuffer buffer) {
            this.decorator = decorator;
            this.cs = cs;
            this.buffer = buffer;
        }

        @Override
        public TupleExpression visitNumber(NumberTupleExpression numExpr) {
            serializeExpression(0, numExpr, buffer, cs);
            return numExpr;
        }

        public TupleExpression visitString(StringTupleExpression strExpr) {
            serializeExpression(0, strExpr, buffer, cs);
            return strExpr;
        }

        @Override
        public TupleExpression visitColumn(ColumnTupleExpression colExpr) {
            if (decorator != null) {
                colExpr = new ColumnTupleExpression(decorator.mapCol(colExpr.getColumn()));
            }
            serializeExpression(0, colExpr, buffer, cs);
            return colExpr;
        }

        @Override
        public TupleExpression visitBinary(BinaryTupleExpression binaryExpr) {
            // serialize expression+true
            serializeExpression(1, binaryExpr, buffer, cs);
            // serialize children
            TupleExpression left = binaryExpr.getLeft().accept(this);
            TupleExpression right = binaryExpr.getRight().accept(this);
            // serialize none
            serializeExpression(-1, binaryExpr, buffer, cs);
            return decorator == null ? binaryExpr
                    : new BinaryTupleExpression(binaryExpr.getOperator(), Lists.newArrayList(left, right));
        }

        @Override
        public TupleExpression visitCaseCall(CaseTupleExpression caseExpr) {
            if (decorator != null) {
                List<Pair<TupleFilter, TupleExpression>> whenList = Lists
                        .newArrayListWithExpectedSize(caseExpr.getWhenList().size());
                for (Pair<TupleFilter, TupleExpression> entry : caseExpr.getWhenList()) {
                    TupleFilter filter = decorator.convertInnerFilter(entry.getFirst());
                    TupleExpression whenEntry = decorator.convertInnerExpression(entry.getSecond());
                    whenList.add(new Pair<>(filter, whenEntry));
                }

                TupleExpression elseExpr = caseExpr.getElseExpr();
                if (elseExpr != null) {
                    elseExpr = decorator.convertInnerExpression(elseExpr);
                }
                caseExpr = new CaseTupleExpression(whenList, elseExpr);
            }

            serializeExpression(0, caseExpr, buffer, cs);
            return caseExpr;
        }

        @Override
        public TupleExpression visitRexCall(RexCallTupleExpression rexCallExpr) {
            throw new UnsupportedOperationException();
        }

        @Override
        public TupleExpression visitNone(NoneTupleExpression noneExpr) {
            return noneExpr;
        }
    }

    private static final int BUFFER_SIZE = 65536;
    private static final Map<Integer, TupleExpression.ExpressionOperatorEnum> ID_OP_MAP = Maps.newHashMap();

    static {
        for (TupleExpression.ExpressionOperatorEnum op : TupleExpression.ExpressionOperatorEnum.values()) {
            ID_OP_MAP.put(op.getValue(), op);
        }
    }

    public static byte[] serialize(TupleExpression rootExpr, IFilterCodeSystem<?> cs) {
        return serialize(rootExpr, null, cs);
    }

    public static byte[] serialize(TupleExpression rootExpr, Decorator decorator, IFilterCodeSystem<?> cs) {
        ByteBuffer buffer;
        int bufferSize = BUFFER_SIZE;
        while (true) {
            try {
                buffer = ByteBuffer.allocate(bufferSize);
                Serializer serializer = new Serializer(decorator, cs, buffer);
                rootExpr.accept(serializer);
                break;
            } catch (BufferOverflowException e) {
                logger.info("Buffer size {} cannot hold the expression, resizing to 4 times", bufferSize);
                bufferSize *= 4;
            }
        }
        byte[] result = new byte[buffer.position()];
        System.arraycopy(buffer.array(), 0, result, 0, buffer.position());
        return result;
    }

    private static void serializeExpression(int flag, TupleExpression expr, ByteBuffer buffer,
                                            IFilterCodeSystem<?> cs) {
        if (flag < 0) {
            BytesUtil.writeVInt(-1, buffer);
        } else {
            int opVal = expr.getOperator().getValue();
            BytesUtil.writeVInt(opVal, buffer);
            expr.serialize(cs, buffer);
            BytesUtil.writeVInt(flag, buffer);
        }
    }

    public static TupleExpression deserialize(byte[] bytes, IFilterCodeSystem<?> cs) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        TupleExpression rootTuple = null;
        Stack<TupleExpression> parentStack = new Stack<>();
        while (buffer.hasRemaining()) {
            int opVal = BytesUtil.readVInt(buffer);
            if (opVal < 0) {
                parentStack.pop();
                continue;
            }

            // deserialize expression
            TupleExpression tuple = createTupleExpression(opVal);
            tuple.deserialize(cs, buffer);

            if (rootTuple == null) {
                // push root to stack
                rootTuple = tuple;
                parentStack.push(tuple);
                BytesUtil.readVInt(buffer);
                continue;
            }

            // add expression to parent
            TupleExpression parentExpression = parentStack.peek();
            if (parentExpression != null) {
                parentExpression.addChild(tuple);
            }

            // push expression to stack or not based on having children or not
            int hasChild = BytesUtil.readVInt(buffer);
            if (hasChild == 1) {
                parentStack.push(tuple);
            }
        }
        return rootTuple;
    }

    private static TupleExpression createTupleExpression(int opVal) {
        TupleExpression.ExpressionOperatorEnum op = ID_OP_MAP.get(opVal);
        if (op == null) {
            throw new IllegalStateException("operator value is " + opVal);
        }
        TupleExpression tuple = null;
        switch (op) {
        case PLUS:
        case MINUS:
        case MULTIPLE:
        case DIVIDE:
            tuple = new BinaryTupleExpression(op);
            break;
        case NUMBER:
            tuple = new NumberTupleExpression(null);
            break;
        case STRING:
            tuple = new StringTupleExpression(null);
            break;
        case COLUMN:
            tuple = new ColumnTupleExpression(null);
            break;
        case CASE:
            tuple = new CaseTupleExpression(null, null);
            break;
        default:
            throw new IllegalStateException("Error ExpressionOperatorEnum: " + op.getValue());
        }
        return tuple;
    }
}

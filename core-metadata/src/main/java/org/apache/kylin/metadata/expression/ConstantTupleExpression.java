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
import java.util.Collections;
import java.util.Objects;

import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.filter.IFilterCodeSystem;
import org.apache.kylin.metadata.tuple.IEvaluatableTuple;

public class ConstantTupleExpression extends TupleExpression {

    public final static ConstantTupleExpression ZERO = new ConstantTupleExpression(0);

    private Object value;

    public ConstantTupleExpression(Object value) {
        this(referDataType(value), value);
    }

    public ConstantTupleExpression(DataType dataType, Object value) {
        super(dataType, ExpressionOperatorEnum.CONSTANT, Collections.<TupleExpression> emptyList());
        this.value = referValue(value);
    }

    public Object getValue() {
        return value;
    }

    @Override
    public void verify() {
    }

    @Override
    public Object calculate(IEvaluatableTuple tuple, IFilterCodeSystem<?> cs) {
        return value;
    }

    @Override
    public TupleExpression accept(ExpressionVisitor visitor) {
        return visitor.visitConstant(this);
    }

    @Override
    public void serialize(IFilterCodeSystem<?> cs, ByteBuffer buffer) {
        serializer.serialize(value, buffer);
    }

    @Override
    public void deserialize(IFilterCodeSystem<?> cs, ByteBuffer buffer) {
        value = serializer.deserialize(buffer);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ConstantTupleExpression that = (ConstantTupleExpression) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    public static DataType referDataType(Object value) {
        if (value instanceof Number) {
            if (value instanceof Short || value instanceof Integer || value instanceof Long) {
                return DataType.getType("bigint");
            } else if (value instanceof BigDecimal) {
                return DataType.getType("decimal");
            } else {
                return DataType.getType("double");
            }
        } else {
            return DataType.getType("varchar");
        }
    }
}
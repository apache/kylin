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

import org.apache.kylin.common.util.DecimalUtil;
import org.apache.kylin.metadata.datatype.BigDecimalSerializer;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.filter.IFilterCodeSystem;
import org.apache.kylin.metadata.tuple.IEvaluatableTuple;

public class NumberTupleExpression extends TupleExpression {

    public static final BigDecimalSerializer serializer = new BigDecimalSerializer(DataType.getType("decimal"));

    private BigDecimal value;

    public NumberTupleExpression(Object value) {
        this(DecimalUtil.toBigDecimal(value));
    }

    public NumberTupleExpression(BigDecimal value) {
        super(ExpressionOperatorEnum.NUMBER, Collections.<TupleExpression> emptyList());
        this.value = value;
    }

    @Override
    public void verify() {
    }

    @Override
    public BigDecimal calculate(IEvaluatableTuple tuple, IFilterCodeSystem<?> cs) {
        return value;
    }

    @Override
    public TupleExpression accept(ExpressionVisitor visitor) {
        return visitor.visitNumber(this);
    }

    @Override
    public void serialize(IFilterCodeSystem<?> cs, ByteBuffer buffer) {
        serializer.serialize(value, buffer);
    }

    @Override
    public void deserialize(IFilterCodeSystem<?> cs, ByteBuffer buffer) {
        value = serializer.deserialize(buffer);
    }

    public BigDecimal getValue() {
        return value;
    }

    public String toString() {
        return value.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        NumberTupleExpression that = (NumberTupleExpression) o;

        return value != null ? value.equals(that.value) : that.value == null;

    }

    @Override
    public int hashCode() {
        return value != null ? value.hashCode() : 0;
    }
}

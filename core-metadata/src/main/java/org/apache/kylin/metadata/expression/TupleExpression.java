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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import org.apache.kylin.common.util.DecimalUtil;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.apache.kylin.metadata.filter.IFilterCodeSystem;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.IEvaluatableTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TupleExpression {
    static final Logger logger = LoggerFactory.getLogger(TupleExpression.class);

    public enum ExpressionOperatorEnum {
        PLUS(0, "+"), MINUS(1, "-"), MULTIPLE(2, "*"), DIVIDE(3, "/"), //
        CASE(10, "Case"), //
        COLUMN(20, "InputRef"), CONSTANT(21, "Constant"), //
        REXCALL(30, "RexCall"), NONE(31, "NONE");

        private final int value;
        private final String name;

        private ExpressionOperatorEnum(int value, String name) {
            this.value = value;
            this.name = name;
        }

        public String toString() {
            return name;
        }

        public int getValue() {
            return value;
        }
    }

    protected final ExpressionOperatorEnum operator;
    protected final List<TupleExpression> children;
    protected final DataType dataType;
    protected final DataTypeSerializer serializer;
    protected String digest;
    protected Boolean ifAbleToPushDown = null;

    protected TupleExpression(DataType dataType, ExpressionOperatorEnum op, List<TupleExpression> exprs) {
        this.dataType = dataType;
        this.serializer = dataType == null || TblColRef.InnerDataTypeEnum.contains(dataType.getName())
                || dataType.equals(DataType.ANY) ? null : DataTypeSerializer.create(dataType);
        this.operator = op;
        this.children = exprs;
    }

    protected boolean ifAbleToPushDown() {
        if (ifAbleToPushDown == null) {
            for (TupleExpression child : children) {
                ifAbleToPushDown = child.ifAbleToPushDown();
                if (!ifAbleToPushDown) {
                    break;
                }
            }
            if (ifAbleToPushDown == null) {
                ifAbleToPushDown = true;
            }
        }
        return ifAbleToPushDown;
    }

    public boolean ifForDynamicColumn() {
        return false;
    }

    public abstract void verify();

    public abstract Object calculate(IEvaluatableTuple tuple, IFilterCodeSystem<?> cs);

    public abstract TupleExpression accept(ExpressionVisitor visitor);

    public abstract void serialize(IFilterCodeSystem<?> cs, ByteBuffer buffer);

    public abstract void deserialize(IFilterCodeSystem<?> cs, ByteBuffer buffer);

    public DataType getDataType() {
        return dataType;
    }

    public ExpressionOperatorEnum getOperator() {
        return operator;
    }

    public void setDigest(String digest) {
        this.digest = digest;
    }

    public String getDigest() {
        return digest;
    }

    public boolean hasChildren() {
        return children != null && !children.isEmpty();
    }

    public List<? extends TupleExpression> getChildren() {
        return Collections.unmodifiableList(children);
    }

    public void addChild(TupleExpression child) {
        children.add(child);
    }

    public Object referValue(Object value) {
        return referValue(value, dataType);
    }

    public static DataType referDataType(DataType dt1, DataType dt2) {
        if (dt1 == null || dt2 == null) {
            DataType dt = dt1 == null ? dt2 : dt1;
            if (dt == null) {
                return null;
            }
            if (dt.isNumberFamily()) {
                if (dt.isIntegerFamily()) {
                    return DataType.getType("bigint");
                } else if (dt.isDecimal()) {
                    return DataType.getType("decimal");
                } else {
                    return DataType.getType("double");
                }
            } else {
                return dt;
            }
        }

        if (dt1.isNumberFamily() && dt2.isNumberFamily()) {
            if (dt1.isIntegerFamily() && dt2.isIntegerFamily()) {
                return DataType.getType("bigint");
            } else if (dt1.isDecimal() || dt2.isDecimal()) {
                return DataType.getType("decimal");
            } else {
                return DataType.getType("double");
            }
        } else {
            assert dt1.equals(dt2);
            return dt1;
        }
    }

    public static Object referValue(Object value, DataType dt) {
        if (value == null) {
            return null;
        }
        if (dt != null && dt.isNumberFamily()) {
            Number number = (Number) value;
            if (dt.isIntegerFamily()) {
                return number.longValue();
            } else if (dt.isDecimal()) {
                return DecimalUtil.toBigDecimal(number);
            } else {
                return number.doubleValue();
            }
        } else {
            return value;
        }
    }
}
/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

 
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

package org.apache.kylin.engine.spark.metadata.cube.model;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.kylin.engine.spark.metadata.cube.model.tool.NonEquiJoinConditionComparator;
import org.apache.kylin.engine.spark.metadata.cube.model.tool.TypedLiteralConverter;

import java.io.Serializable;
import java.util.Arrays;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class NonEquiJoinCondition implements Serializable {

    @JsonProperty("type")
    private NonEquiJoinConditionType type;

    @JsonProperty("data_type")
    private DataType dataType; // data type of the corresponding rex node

    @JsonProperty("op")
    private SqlKind op; // kind of the operator

    @JsonProperty("op_name")
    private String opName; // name of the operator

    @JsonProperty("operands")
    private NonEquiJoinCondition[] operands = new NonEquiJoinCondition[0]; // nested operands

    @JsonProperty("value")
    private String value; // literal or column identity at leaf node

    private TblColRef colRef; // set at runtime with model init

    @JsonProperty("expr")
    private String expr;

    public NonEquiJoinCondition() {
    }

    public NonEquiJoinCondition(SqlOperator op, NonEquiJoinCondition[] operands, RelDataType dataType) {
        this(op.getName(), op.getKind(), operands, new DataType(dataType));
    }

    public NonEquiJoinCondition(RexLiteral value, RelDataType dataType) {
        this(TypedLiteralConverter.typedLiteralToString(value), new DataType(dataType));
    }

    public NonEquiJoinCondition(TblColRef tblColRef, RelDataType dataType) {
        this(tblColRef, new DataType(dataType));
    }

    public NonEquiJoinCondition(String opName, SqlKind op, NonEquiJoinCondition[] operands, DataType dataType) {
        this.opName = opName;
        this.op = op;
        this.operands = operands;
        this.type = NonEquiJoinConditionType.EXPRESSION;
        this.dataType = dataType;
    }

    public NonEquiJoinCondition(String value, DataType dataType) {
        this.op = SqlKind.LITERAL;
        this.type = NonEquiJoinConditionType.LITERAL;
        this.value = value;
        this.dataType = dataType;
    }

    public NonEquiJoinCondition(TblColRef tblColRef, DataType dataType) {
        this.op = SqlKind.INPUT_REF;
        this.type = NonEquiJoinConditionType.COLUMN;
        this.value = tblColRef.getIdentity();
        this.colRef = tblColRef;
        this.dataType = dataType;
    }

    public Object getTypedValue() {
        return TypedLiteralConverter.stringValueToTypedValue(value, dataType);
    }

    public NonEquiJoinCondition copy() {
        NonEquiJoinCondition condCopy = new NonEquiJoinCondition();
        condCopy.type = type;
        condCopy.dataType = dataType;
        condCopy.op = op;
        condCopy.opName = opName;
        condCopy.operands = Arrays.copyOf(operands, operands.length);
        condCopy.value = value;
        condCopy.colRef = colRef;
        condCopy.expr = expr;
        return condCopy;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(op);
        sb.append("(");
        for (NonEquiJoinCondition input : operands) {
            sb.append(input.toString());
            sb.append(", ");
        }
        if (type == NonEquiJoinConditionType.LITERAL) {
            sb.append(value);
        } else if (type == NonEquiJoinConditionType.COLUMN) {
            if (colRef != null) {
                sb.append(colRef.getColumnWithTableAndSchema());
            } else {
                sb.append(value);
            }
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public boolean equals(Object obj) {
        return obj != null &&
                obj instanceof NonEquiJoinCondition &&
                NonEquiJoinConditionComparator.equals(this, (NonEquiJoinCondition) obj);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + type.hashCode();
        result = prime * result + dataType.hashCode();
        result = prime * result + op.hashCode();
        // consider opName only SqlKind OTHER
        if ((op == SqlKind.OTHER || op == SqlKind.OTHER_FUNCTION) && opName != null) {
            result = prime * result + opName.hashCode();
        }
        for (NonEquiJoinCondition operand : operands) {
            result = prime * result + operand.hashCode();
        }

        if (type == NonEquiJoinConditionType.LITERAL) {
            result = prime * result + value.hashCode();
        } else if (type == NonEquiJoinConditionType.COLUMN) {
            if (colRef != null) {
                result = prime * result + colRef.hashCode();
            } else {
                result = prime * result + value.hashCode();
            }
        }
        return result;
    }

    public NonEquiJoinConditionType getType() {
        return this.type;
    }

    public DataType getDataType() {
        return this.dataType;
    }

    public SqlKind getOp() {
        return this.op;
    }

    public String getOpName() {
        return this.opName;
    }

    public NonEquiJoinCondition[] getOperands() {
        return this.operands;
    }

    public String getValue() {
        return this.value;
    }

    public TblColRef getColRef() {
        return this.colRef;
    }

    public String getExpr() {
        return this.expr;
    }

    public void setType(NonEquiJoinConditionType type) {
        this.type = type;
    }

    public void setDataType(DataType dataType) {
        this.dataType = dataType;
    }

    public void setOp(SqlKind op) {
        this.op = op;
    }

    public void setOpName(String opName) {
        this.opName = opName;
    }

    public void setOperands(NonEquiJoinCondition[] operands) {
        this.operands = operands;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public void setColRef(TblColRef colRef) {
        this.colRef = colRef;
    }

    public void setExpr(String expr) {
        this.expr = expr;
    }
}

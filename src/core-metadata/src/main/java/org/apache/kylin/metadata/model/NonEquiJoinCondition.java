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

package org.apache.kylin.metadata.model;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.kylin.metadata.model.tool.NonEquiJoinConditionComparator;
import org.apache.kylin.metadata.model.tool.TypedLiteralConverter;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.guava30.shaded.common.collect.Sets;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class NonEquiJoinCondition implements Serializable {

    @Getter
    @Setter
    @JsonProperty("type")
    private NonEquiJoinConditionType type;

    @Getter
    @Setter
    @JsonProperty("data_type")
    private DataType dataType; // data type of the corresponding rex node

    @Getter
    @Setter
    @JsonProperty("op")
    private SqlKind op; // kind of the operator

    @Getter
    @Setter
    @JsonProperty("op_name")
    private String opName; // name of the operator

    @Getter
    @Setter
    @JsonProperty("operands")
    private NonEquiJoinCondition[] operands = new NonEquiJoinCondition[0]; // nested operands

    @Getter
    @Setter
    @JsonProperty("value")
    private String value; // literal or column identity at leaf node

    @Getter
    @Setter
    private TblColRef colRef; // set at runtime with model init

    @Getter
    @Setter
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
        return obj != null && obj instanceof NonEquiJoinCondition
                && NonEquiJoinConditionComparator.equals(this, (NonEquiJoinCondition) obj);
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

    public Set<String> getAllReferencingColumns() {
        return doGetAllReferencingColumns(this);
    }

    private Set<String> doGetAllReferencingColumns(NonEquiJoinCondition cond) {
        if (cond.type == NonEquiJoinConditionType.COLUMN) {
            return Sets.newHashSet(cond.colRef.getIdentity());
        } else if (cond.type == NonEquiJoinConditionType.EXPRESSION) {
            return Arrays.stream(cond.operands).map(this::doGetAllReferencingColumns).flatMap(Collection::stream)
                    .collect(Collectors.toSet());
        } else {
            return new HashSet<>();
        }
    }

    @Setter
    @Getter
    @NoArgsConstructor
    public static final class SimplifiedNonEquiJoinCondition implements Serializable {

        private static final long serialVersionUID = -1577556052145832500L;

        @JsonProperty("foreign_key")
        private String foreignKey;

        @JsonProperty("primary_key")
        private String primaryKey;

        @JsonProperty("op")
        private SqlKind op;

        public SimplifiedNonEquiJoinCondition(String foreignKey, String primaryKey, SqlKind op) {
            this.foreignKey = foreignKey;
            this.primaryKey = primaryKey;
            this.op = op;
        }

        public SimplifiedNonEquiJoinCondition(String foreignKey, TblColRef fk, String primaryKey, TblColRef pk,
                SqlKind op) {
            this.foreignKey = foreignKey;
            this.primaryKey = primaryKey;
            this.op = op;
            this.fk = fk;
            this.pk = pk;
        }

        @JsonIgnore
        private TblColRef fk;

        @JsonIgnore
        private TblColRef pk;
    }
}

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

package org.apache.kylin.metadata.model.graph;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.NonEquiJoinCondition;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;

import lombok.Getter;
import lombok.Setter;

public class Edge implements Serializable {
    private static final long serialVersionUID = 1L;

    public final JoinDesc join;
    public final ColumnDesc[] leftCols;
    public final ColumnDesc[] rightCols;
    public final NonEquiJoinCondition nonEquiJoinCondition;
    @Setter
    private IJoinEdgeMatcher joinEdgeMatcher = new DefaultJoinEdgeMatcher();
    @Setter
    @Getter
    private boolean swapJoin;

    public Edge(JoinDesc join) {
        this(join, false);
    }

    public Edge(JoinDesc join, boolean swapJoin) {
        this.join = join;

        int i = 0;
        leftCols = new ColumnDesc[join.getForeignKeyColumns().length];
        for (TblColRef colRef : join.getForeignKeyColumns()) {
            leftCols[i++] = colRef.getColumnDesc();
        }

        i = 0;
        rightCols = new ColumnDesc[join.getPrimaryKeyColumns().length];
        for (TblColRef colRef : join.getPrimaryKeyColumns()) {
            rightCols[i++] = colRef.getColumnDesc();
        }

        nonEquiJoinCondition = join.getNonEquiJoinCondition();
        this.swapJoin = swapJoin;
    }

    public boolean isJoinMatched(JoinDesc other) {
        return join.equals(other);
    }

    public boolean isNonEquiJoin() {
        return nonEquiJoinCondition != null;
    }

    public boolean isLeftJoin() {
        return !join.isLeftOrInnerJoin() && join.isLeftJoin();
    }

    public boolean isLeftOrInnerJoin() {
        return join.isLeftOrInnerJoin();
    }

    public TableRef pkSide() {
        return join.getPKSide();
    }

    public TableRef fkSide() {
        return join.getFKSide();
    }

    public boolean isFkSide(TableRef tableRef) {
        return fkSide().equals(tableRef);
    }

    public boolean isPkSide(TableRef tableRef) {
        return pkSide().equals(tableRef);
    }

    public TableRef otherSide(TableRef tableRef) {
        if (isFkSide(tableRef)) {
            return pkSide();
        } else if (isPkSide(tableRef)) {
            return fkSide();
        }
        throw new IllegalArgumentException("table " + tableRef + " is not on the edge: " + this);
    }

    @Override
    public boolean equals(Object other) {
        if (other == null) {
            return false;
        }

        if (this.getClass() != other.getClass()) {
            return false;
        }
        return joinEdgeMatcher.matches(this, (Edge) other);
    }

    @Override
    public int hashCode() {
        if (this.isLeftJoin()) {
            return Objects.hash(isLeftJoin(), leftCols, rightCols);
        }
        if (Arrays.hashCode(leftCols) < Arrays.hashCode(rightCols)) {
            return Objects.hash(isLeftJoin(), leftCols, rightCols);
        }
        return Objects.hash(isLeftJoin(), rightCols, leftCols);
    }

    @Override
    public String toString() {
        // Edge: TableRef[TEST_KYLIN_FACT] LEFT JOIN TableRef[TEST_ORDER] ON [ORDER_ID] = [ORDER_ID]
        return "Edge: " + join.getFKSide() + getJoinTypeStr() + join.getPKSide() + " ON "
                + Arrays.toString(Arrays.stream(leftCols).map(ColumnDesc::getName).toArray()) + " = "
                + Arrays.toString(Arrays.stream(rightCols).map(ColumnDesc::getName).toArray());
    }

    private String getJoinTypeStr() {
        if (isLeftJoin()) {
            return " LEFT JOIN ";
        }
        return isLeftOrInnerJoin() ? " LEFT OR INNER JOIN " : " INNER JOIN ";
    }
}

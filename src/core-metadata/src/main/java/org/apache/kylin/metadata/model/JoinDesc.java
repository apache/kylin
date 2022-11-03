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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class JoinDesc implements Serializable {

    // inner, left, right, outer...
    @JsonProperty("type")
    private String type;
    @JsonProperty("primary_key")
    private String[] primaryKey;
    @JsonProperty("foreign_key")
    private String[] foreignKey;
    @JsonProperty("non_equi_join_condition")
    private NonEquiJoinCondition nonEquiJoinCondition;
    @JsonProperty("primary_table")
    private String primaryTable;
    @JsonProperty("foreign_table")
    private String foreignTable;
    private boolean leftOrInner = false;

    private TblColRef[] primaryKeyColumns;
    private TblColRef[] foreignKeyColumns;
    private TableRef primaryTableRef;
    private TableRef foreignTableRef;

    public boolean isInnerJoin() {
        return "INNER".equalsIgnoreCase(type);
    }

    public boolean isLeftJoin() {
        return "LEFT".equalsIgnoreCase(type);
    }

    public boolean isLeftOrInnerJoin() {
        return leftOrInner;
    }

    public void setPrimaryKeyColumns(TblColRef[] primaryKeyColumns) {
        checkSameTable(primaryKeyColumns);
        this.primaryKeyColumns = primaryKeyColumns;
    }

    public void setForeignKeyColumns(TblColRef[] foreignKeyColumns) {
        checkSameTable(primaryKeyColumns);
        this.foreignKeyColumns = foreignKeyColumns;
    }

    private void checkSameTable(TblColRef[] cols) {
        if (cols == null || cols.length == 0)
            return;

        TableRef tableRef = cols[0].getTableRef();
        for (int i = 1; i < cols.length; i++)
            Preconditions.checkState(tableRef == cols[i].getTableRef());
    }

    public TableRef getPKSide() {
        return primaryTableRef != null ? primaryTableRef : primaryKeyColumns[0].getTableRef();
    }

    public TableRef getFKSide() {
        return foreignTableRef != null ? foreignTableRef : foreignKeyColumns[0].getTableRef();
    }

    public void sortByFK() {
        Preconditions.checkState(primaryKey.length == foreignKey.length && primaryKey.length == primaryKeyColumns.length
                && foreignKey.length == foreignKeyColumns.length);
        boolean cont = true;
        int n = foreignKey.length;
        for (int i = 0; i < n - 1 && cont; i++) {
            cont = false;
            for (int j = 0; j < n - 1 - i; j++) {
                int jj = j + 1;
                if (foreignKey[j].compareTo(foreignKey[jj]) > 0) {
                    swap(foreignKey, j, jj);
                    swap(primaryKey, j, jj);
                    swap(foreignKeyColumns, j, jj);
                    swap(primaryKeyColumns, j, jj);
                    cont = true;
                }
            }
        }
    }

    private void swap(String[] arr, int j, int jj) {
        String tmp = arr[j];
        arr[j] = arr[jj];
        arr[jj] = tmp;
    }

    private void swap(TblColRef[] arr, int j, int jj) {
        TblColRef tmp = arr[j];
        arr[j] = arr[jj];
        arr[jj] = tmp;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(primaryKeyColumns);
        result = prime * result + Arrays.hashCode(foreignKeyColumns);
        result = prime * result + this.type.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        JoinDesc other = (JoinDesc) obj;

        // note pk/fk are sorted, sortByFK()
        if (!Arrays.equals(foreignKey, other.foreignKey))
            return false;
        if (!Arrays.equals(primaryKey, other.primaryKey))
            return false;
        if (!Arrays.equals(foreignKeyColumns, other.foreignKeyColumns))
            return false;
        if (!Arrays.equals(primaryKeyColumns, other.primaryKeyColumns))
            return false;

        return this.type.equalsIgnoreCase(other.getType());
    }

    // equals() without alias
    public boolean matches(JoinDesc other) {
        if (other == null)
            return false;

        if (!this.type.equalsIgnoreCase(other.getType()))
            return false;

        // note pk/fk are sorted, sortByFK()
        if (!this.columnDescEquals(foreignKeyColumns, other.foreignKeyColumns))
            return false;
        return this.columnDescEquals(primaryKeyColumns, other.primaryKeyColumns);
    }

    private boolean columnDescEquals(TblColRef[] a, TblColRef[] b) {
        if (a.length != b.length)
            return false;

        for (int i = 0; i < a.length; i++) {
            if (!a[i].getColumnDesc().equals(b[i].getColumnDesc()))
                return false;
        }
        return true;
    }

    public boolean isNonEquiJoin() {
        return Objects.nonNull(nonEquiJoinCondition);
    }

    public boolean isJoinWithFactTable(String factTable) {
        TblColRef[] foreignKeyColumns = getForeignKeyColumns();
        if (foreignKeyColumns.length == 0) {
            return false;
        }
        TableRef tableRef = foreignKeyColumns[0].getTableRef();
        if (tableRef == null) {
            return false;
        }
        return factTable.equalsIgnoreCase(tableRef.getTableIdentity());
    }

    public void changeFKTableAlias(String oldAlias, String newAlias) {
        List<String> fks = Lists.newArrayList();
        for (String fk : foreignKey) {
            String table = fk.split("\\.")[0];
            String column = fk.split("\\.")[1];
            if (table.equalsIgnoreCase(oldAlias)) {
                fks.add(newAlias + "." + column);
                continue;
            }
            fks.add(fk);
        }
        foreignKey = fks.toArray(new String[] {});
    }

    @Override
    public String toString() {
        return "JoinDesc [type=" + type + ", primary_key=" + Arrays.toString(primaryKey) + ", foreign_key="
                + Arrays.toString(foreignKey) + "]";
    }

    public static class JoinDescBuilder {

        private final List<String> pks = new ArrayList<>();
        private final List<TblColRef> pkCols = new ArrayList<>();
        private final List<String> fks = new ArrayList<>();
        private final List<TblColRef> fkCols = new ArrayList<>();
        private TableRef primaryTableRef;
        private TableRef foreignTableRef;
        private NonEquiJoinCondition nonEquiJoinCondition;
        private String type;
        private boolean leftOrInner = false;

        public JoinDescBuilder addPrimaryKeys(String[] pkColNames, TblColRef[] colRefs) {
            pks.addAll(Arrays.asList(pkColNames));
            pkCols.addAll(Arrays.asList(colRefs));
            return this;
        }

        public JoinDescBuilder addForeignKeys(String[] fkColNames, TblColRef[] colRefs) {
            fks.addAll(Arrays.asList(fkColNames));
            fkCols.addAll(Arrays.asList(colRefs));
            return this;
        }

        public JoinDescBuilder addPrimaryKeys(Collection<TblColRef> colRefs) {
            pks.addAll(colRefs.stream().map(TblColRef::getName).collect(Collectors.toList()));
            pkCols.addAll(colRefs);
            return this;
        }

        public JoinDescBuilder addForeignKeys(Collection<TblColRef> colRefs) {
            fks.addAll(colRefs.stream().map(TblColRef::getName).collect(Collectors.toList()));
            fkCols.addAll(colRefs);
            return this;
        }

        public JoinDescBuilder setPrimaryTableRef(TableRef primaryTableRef) {
            this.primaryTableRef = primaryTableRef;
            return this;
        }

        public JoinDescBuilder setForeignTableRef(TableRef foreignTableRef) {
            this.foreignTableRef = foreignTableRef;
            return this;
        }

        public void setNonEquiJoinCondition(NonEquiJoinCondition nonEquiJoinCondition) {
            this.nonEquiJoinCondition = nonEquiJoinCondition;
        }

        public void setType(String type) {
            this.type = type;
        }

        public void setLeftOrInner(Boolean leftOrInner) {
            this.leftOrInner = leftOrInner;
        }

        public JoinDesc build() {
            JoinDesc join = new JoinDesc();
            join.setForeignKey(fks.toArray(new String[0]));
            join.setForeignKeyColumns(fkCols.toArray(new TblColRef[0]));
            join.setPrimaryKey(pks.toArray(new String[0]));
            join.setPrimaryKeyColumns(pkCols.toArray(new TblColRef[0]));
            join.primaryTable = primaryTableRef.getAlias();
            join.foreignTable = foreignTableRef.getAlias();
            join.primaryTableRef = primaryTableRef;
            join.foreignTableRef = foreignTableRef;
            join.nonEquiJoinCondition = nonEquiJoinCondition;
            join.type = type;
            join.leftOrInner = leftOrInner;
            return join;
        }
    }

    public List<String> getJoinKey() {
        List<String> joinKey = Lists.newArrayList();
        if (primaryKey != null) {
            joinKey.addAll(Arrays.stream(primaryKey).filter(Objects::nonNull).collect(Collectors.toList()));
        }
        if (foreignKey != null) {
            joinKey.addAll(Arrays.stream(foreignKey).filter(Objects::nonNull).collect(Collectors.toList()));
        }
        return joinKey;
    }
}

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

package org.apache.kylin.cube.model;

import java.util.Arrays;
import java.util.Locale;

import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.shaded.com.google.common.base.MoreObjects;

/**
 */
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class DimensionDesc implements java.io.Serializable {

    @JsonProperty("name")
    private String name;
    @JsonProperty("table")
    private String table;
    @JsonProperty("column")
    private String column;
    @JsonProperty("derived")
    private String[] derived;

    private TableRef tableRef;
    private JoinDesc join;

    // computed
    private TblColRef[] columnRefs;

    public void init(CubeDesc cubeDesc) {
        DataModelDesc model = cubeDesc.getModel();

        if (name != null)
            name = name.toUpperCase(Locale.ROOT);

        tableRef = model.findTable(table);
        table = tableRef.getAlias();

        join = null;
        for (JoinTableDesc joinTable : model.getJoinTables()) {
            if (joinTable.getTableRef().equals(this.tableRef)) {
                join = joinTable.getJoin();
                break;
            }
        }

        if (column != null && !"{FK}".equals(column)) {
            column = model.findColumn(table, column).getName();
        }
        if (derived != null && derived.length == 0) {
            derived = null;
        }
        if (derived != null) {
            for (int i = 0; i < derived.length; i++) {
                derived[i] = model.findColumn(table, derived[i]).getName();
            }
        }
        if (derived != null && join == null) {
            throw new IllegalStateException(
                    "Derived can only be defined on lookup table, cube " + cubeDesc + ", " + this);
        }

    }

    public boolean isDerived() {
        return derived != null;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public JoinDesc getJoin() {
        return join;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public TblColRef[] getColumnRefs() {
        return this.columnRefs;
    }

    public void setColumnRefs(TblColRef[] colRefs) {
        this.columnRefs = colRefs;
    }

    public String getColumn() {
        return this.column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public String[] getDerived() {
        return derived;
    }

    public void setDerived(String[] derived) {
        this.derived = derived;
    }

    public TableRef getTableRef() {
        return this.tableRef;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("name", name).add("table", table).add("column", column)
                .add("derived", Arrays.toString(derived)).add("join", join).toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DimensionDesc that = (DimensionDesc) o;

        if (column != null ? !column.equals(that.column) : that.column != null) {
            return false;
        }

        if (name != null ? !name.equals(that.name) : that.name != null) {
            return false;
        }

        if (table != null ? !table.equals(that.table) : that.table != null) {
            return false;
        }

        if (derived != null ? !Arrays.equals(derived, that.derived) : that.derived != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((column == null) ? 0 : column.hashCode());
        result = prime * result + ((table == null) ? 0 : table.hashCode());
        result = prime * result + ((derived == null) ? 0 : Arrays.hashCode(derived));
        return result;
    }
}

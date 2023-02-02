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
import java.util.Locale;

import org.apache.calcite.avatica.util.Quoting;
import org.apache.kylin.metadata.datatype.DataType;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.google.common.base.Preconditions;

/**
 * Column Metadata from Source. All name should be uppercase.
 * <p/>
 */
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class ColumnDesc implements Serializable {

    private static final String BACK_TICK = Quoting.BACK_TICK.string;

    @JsonProperty("id")
    private String id;

    @JsonProperty("name")
    private String name;

    @JsonProperty("datatype")
    private String datatype;

    @JsonProperty("comment")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String comment;

    @JsonProperty("data_gen")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String dataGen;

    @JsonProperty("index")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String index;

    @JsonProperty("cc_expr")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String computedColumnExpr = null;//if null, it's not a computed column

    @JsonProperty("case_sensitive_name")
    public String caseSensitiveName;

    @JsonProperty("is_partitioned")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public boolean isPartitioned = false;

    // parsed from data type
    private DataType type;
    private DataType upgradedType;

    private TableDesc table;
    private int zeroBasedIndex = -1;
    private boolean isNullable = true;

    public ColumnDesc() { // default constructor for Jackson
    }

    public ColumnDesc(ColumnDesc other) {
        this.id = other.id;
        this.name = other.name;
        this.datatype = other.datatype;
        this.comment = other.comment;
        this.dataGen = other.dataGen;
        this.index = other.index;
        this.computedColumnExpr = other.computedColumnExpr;
        this.caseSensitiveName = other.caseSensitiveName;
        this.isPartitioned = other.isPartitioned;
        this.table = other.table;
    }

    public ColumnDesc(String id, String name, String datatype, String comment, String dataGen, String index,
            String computedColumnExpr) {
        this.id = id;
        this.name = name;
        this.datatype = datatype;
        this.comment = comment;
        this.dataGen = dataGen;
        this.index = index;
        this.computedColumnExpr = computedColumnExpr;
    }

    public void setComputedColumn(String exp) {
        computedColumnExpr = exp;
    }

    public int getZeroBasedIndex() {
        return zeroBasedIndex;
    }

    public String getDatatype() {
        return datatype;
    }

    public void setDatatype(String datatype) {
        this.datatype = datatype;
        type = DataType.getType(datatype);
    }

    public void setUpgradedType(DataType upgradedType) {
        this.upgradedType = upgradedType;
    }

    public DataType getUpgradedType() {
        if (this.upgradedType == null) {
            return this.type;
        } else {
            return this.upgradedType;
        }
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
        if (id != null)
            zeroBasedIndex = Integer.parseInt(id) - 1;
    }

    //avoid calling getName(toUpperCase) frequently
    public String getOriginalName() {
        return name;
    }

    public String getName() {
        return (name == null) ? null : name.toUpperCase(Locale.ROOT);
    }

    public String getIdentity() {
        return table.getName() + "." + getName();
    }

    public String getBackTickIdentity() {
        return BACK_TICK + table.getName() + BACK_TICK + "." + BACK_TICK + getName() + BACK_TICK;
    }

    @JsonSetter("name")
    public void setName(String name) {
        this.name = name;
    }

    public String getCaseSensitiveName() {
        return caseSensitiveName == null ? name : caseSensitiveName;
    }

    @JsonSetter("case_sensitive_name")
    public void setCaseSensitiveName(String caseSensitiveName) {
        this.caseSensitiveName = caseSensitiveName;
    }

    public TableDesc getTable() {
        return table;
    }

    public void setTable(TableDesc table) {
        this.table = table;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public DataType getType() {
        return type;
    }

    public String getTypeName() {
        return type.getName();
    }

    public int getTypePrecision() {
        return type.getPrecision();
    }

    public int getTypeScale() {
        return type.getScale();
    }

    public boolean isNullable() {
        return this.isNullable;
    }

    public void setNullable(boolean nullable) {
        this.isNullable = nullable;
    }

    public String getDataGen() {
        return dataGen;
    }

    public String getIndex() {
        return index;
    }

    public String getComputedColumnExpr() {
        Preconditions.checkState(computedColumnExpr != null);

        return computedColumnExpr;
    }

    public boolean isComputedColumn() {
        return computedColumnExpr != null;
    }

    public boolean isPartitioned() {
        return isPartitioned;
    }

    public void setPartitioned(boolean partitioned) {
        isPartitioned = partitioned;
    }

    public void init(TableDesc table) {
        this.table = table;

        if (id != null)
            zeroBasedIndex = Integer.parseInt(id) - 1;

        DataType normalized = DataType.getType(datatype);
        if (normalized == null) {
            this.setDatatype(null);
        } else {
            this.setDatatype(normalized.toString());
        }
    }

    // for test mainly
    public static ColumnDesc mockup(TableDesc table, int oneBasedColumnIndex, String name, String datatype) {
        ColumnDesc desc = new ColumnDesc();
        String id = "" + oneBasedColumnIndex;
        desc.setId(id);
        desc.setName(name);
        desc.setDatatype(datatype);
        desc.init(table);
        return desc;
    }

    public String getCanonicalName() {
        String tableName = null;
        if (table != null) {
            tableName = table.getIdentity();
        }
        return tableName + "." + getName();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((table == null) ? 0 : table.hashCode());
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
        ColumnDesc other = (ColumnDesc) obj;

        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;

        return table == null ? other.table == null : table.getIdentity().equals(other.table.getIdentity());
    }

    @Override
    public String toString() {
        return "ColumnDesc{" + "id='" + id + '\'' + ", name='" + name + '\'' + ", datatype='" + datatype + '\''
                + ", comment='" + comment + '\'' + '}';
    }

    public ColumnDesc copy() {
        return new ColumnDesc(this);
    }
}

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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang.StringUtils;

/**
 * Column Metadata from Source. All name should be uppercase.
 * <p/>
 * User: lukhan Date: 10/15/13 Time: 9:07 AM To change this template use File |
 * Settings | File Templates.
 */
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class ColumnDesc {
    @JsonProperty("id")
    private String id;
    @JsonProperty("name")
    private String name;
    @JsonProperty("datatype")
    private String datatype;

    // parsed from data type
    private DataType type;

    private TableDesc table;
    private int zeroBasedIndex = -1;
    private boolean isNullable = true;

    public ColumnDesc() { // default constructor for Jackson
    }

    public int getZeroBasedIndex() {
        return zeroBasedIndex;
    }

    public String getDatatype() {
        return datatype;
    }

    public void setDatatype(String datatype) {
        this.datatype = datatype;
        type = DataType.getInstance(datatype);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public TableDesc getTable() {
        return table;
    }

    public void setTable(TableDesc table) {
        this.table = table;
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

    public void setNullable(boolean nullable) {
        this.isNullable = nullable;
    }

    public boolean isNullable() {
        return this.isNullable;
    }

    public void init(TableDesc table) {
        this.table = table;

        if (name != null)
            name = name.toUpperCase();

        if (id != null)
            zeroBasedIndex = Integer.parseInt(id) - 1;

        type = DataType.getInstance(datatype);
    }

    public boolean isSameAs(String tableName, String columnName) {
        return StringUtils.equalsIgnoreCase(table.getIdentity(), tableName) && //
                StringUtils.equalsIgnoreCase(name, columnName);
    }

    @Override
    public String toString() {
        return "ColumnDesc [name=" + name + ",table=" + table.getIdentity() + "]";
    }

    public static ColumnDesc mockup(TableDesc table, int oneBasedColumnIndex, String name, String datatype) {
        ColumnDesc desc = new ColumnDesc();
        String id = "" + oneBasedColumnIndex;
        desc.setId(id);
        desc.setName(name);
        desc.setDatatype(datatype);
        desc.init(table);
        return desc;
    }
}

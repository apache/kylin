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

import java.util.Arrays;
import java.util.Comparator;

import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.StringSplitter;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Table Metadata from Source. All name should be uppercase.
 */
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class TableDesc extends RootPersistentEntity implements ISourceAware {

    private static final String TABLE_TYPE_VIRTUAL_VIEW = "VIRTUAL_VIEW";
    private static final String materializedTableNamePrefix = "kylin_intermediate_";

    @JsonProperty("name")
    private String name;
    @JsonProperty("columns")
    private ColumnDesc[] columns;
    @JsonProperty("source_type")
    private int sourceType = ISourceAware.ID_HIVE;
    @JsonProperty("table_type")
    private String tableType;

    @JsonProperty("data_gen")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String dataGen;

    private DatabaseDesc database = new DatabaseDesc();

    private String identity = null;

    public TableDesc() {
    }

    public TableDesc(TableDesc other) {
        this.name = other.getName();
        this.columns = other.getColumns();
        this.database.setName(other.getDatabase());
        this.tableType = other.getTableType();
    }

    public ColumnDesc findColumnByName(String name) {
        //ignore the db name and table name if exists
        int lastIndexOfDot = name.lastIndexOf(".");
        if (lastIndexOfDot >= 0) {
            name = name.substring(lastIndexOfDot + 1);
        }

        for (ColumnDesc c : columns) {
            // return first matched column
            if (name.equalsIgnoreCase(c.getName())) {
                return c;
            }
        }
        return null;
    }

    public String getResourcePath() {
        return concatResourcePath(getIdentity());
    }

    /**
     * @deprecated this is for compatible with data model v1;
     * @return
     */
    public String getResourcePathV1() {
        return concatResourcePath(name);
    }

    public String getIdentity() {
        if (identity == null) {
            identity = String.format("%s.%s", this.getDatabase().toUpperCase(), this.getName()).toUpperCase();
        }
        return identity;
    }

    public boolean isView() {
        return TABLE_TYPE_VIRTUAL_VIEW.equals(tableType);
    }

    public static String concatResourcePath(String tableIdentity) {
        return ResourceStore.TABLE_RESOURCE_ROOT + "/" + tableIdentity + ".json";
    }

    public static String concatExdResourcePath(String tableIdentity) {
        return ResourceStore.TABLE_EXD_RESOURCE_ROOT + "/" + tableIdentity + ".json";
    }

    // ============================================================================

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        if (name != null) {
            String[] splits = StringSplitter.split(name, ".");
            if (splits.length == 2) {
                this.setDatabase(splits[0]);
                this.name = splits[1];
            } else if (splits.length == 1) {
                this.name = splits[0];
            }
        } else {
            this.name = null;
        }
    }

    @JsonProperty("database")
    public String getDatabase() {
        return database.getName();
    }

    @JsonProperty("database")
    public void setDatabase(String database) {
        this.database.setName(database);
    }

    public ColumnDesc[] getColumns() {
        return columns;
    }

    public void setColumns(ColumnDesc[] columns) {
        this.columns = columns;
    }

    public int getMaxColumnIndex() {
        int max = -1;
        for (ColumnDesc col : columns) {
            int idx = col.getZeroBasedIndex();
            max = Math.max(max, idx);
        }
        return max;
    }

    public int getColumnCount() {
        return getMaxColumnIndex() + 1;
    }

    public String getDataGen() {
        return dataGen;
    }

    public void init() {
        if (name != null)
            name = name.toUpperCase();

        if (getDatabase() != null)
            setDatabase(getDatabase().toUpperCase());

        if (columns != null) {
            Arrays.sort(columns, new Comparator<ColumnDesc>() {
                @Override
                public int compare(ColumnDesc col1, ColumnDesc col2) {
                    Integer id1 = Integer.parseInt(col1.getId());
                    Integer id2 = Integer.parseInt(col2.getId());
                    return id1.compareTo(id2);
                }
            });

            for (ColumnDesc col : columns) {
                col.init(this);
            }
        }
    }

    @Override
    public int hashCode() {
        return getIdentity().hashCode();
    }

    //    @Override
    //    public boolean equals(Object obj) {
    //        if (this == obj)
    //            return true;
    //        if (!super.equals(obj))
    //            return false;
    //        if (getClass() != obj.getClass())
    //            return false;
    //        TableDesc other = (TableDesc) obj;
    //        return getIdentity().equals(other.getIdentity());
    //    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        TableDesc tableDesc = (TableDesc) o;

        if (sourceType != tableDesc.sourceType)
            return false;
        if (name != null ? !name.equals(tableDesc.name) : tableDesc.name != null)
            return false;
        if (!Arrays.equals(columns, tableDesc.columns))
            return false;

        return getIdentity().equals(tableDesc.getIdentity());

    }

    public String getMaterializedName() {
        return materializedTableNamePrefix + database.getName() + "_" + name;
    }

    @Override
    public String toString() {
        return "TableDesc{" + "name='" + name + '\'' + ", columns=" + Arrays.toString(columns) + ", sourceType=" + sourceType + ", tableType='" + tableType + '\'' + ", database=" + database + ", identity='" + getIdentity() + '\'' + '}';
    }

    /** create a mockup table for unit test */
    public static TableDesc mockup(String tableName) {
        TableDesc mockup = new TableDesc();
        mockup.setName(tableName);
        return mockup;
    }

    @Override
    public int getSourceType() {
        return sourceType;
    }

    public void setSourceType(int sourceType) {
        this.sourceType = sourceType;
    }

    public String getTableType() {
        return tableType;
    }

    public void setTableType(String tableType) {
        this.tableType = tableType;
    }

}

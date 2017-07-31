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
import org.apache.kylin.common.util.Pair;
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

    private String project;
    private DatabaseDesc database = new DatabaseDesc();
    private String identity = null;

    public TableDesc() {
    }

    public TableDesc(TableDesc other) {
        this.uuid = other.uuid;
        this.lastModified = other.lastModified;

        this.name = other.name;
        this.sourceType = other.sourceType;
        this.tableType = other.tableType;
        this.dataGen = other.dataGen;

        this.columns = new ColumnDesc[other.columns.length];
        for (int i = 0; i < other.columns.length; i++) {
            this.columns[i] = new ColumnDesc(other.columns[i]);
            this.columns[i].init(this);
        }

        this.project = other.project;
        this.database.setName(other.getDatabase());
        this.identity = other.identity;
    }

    public TableDesc appendColumns(ColumnDesc[] computedColumns) {
        if (computedColumns == null || computedColumns.length == 0) {
            return this;
        }

        TableDesc ret = new TableDesc(this);//deep copy of the table desc
        ColumnDesc[] origin = ret.columns;
        ret.columns = new ColumnDesc[computedColumns.length + origin.length];
        for (int i = 0; i < origin.length; i++) {
            ret.columns[i] = origin[i];

            //check name conflict
            for (int j = 0; j < computedColumns.length; j++) {
                if (origin[i].getName().equalsIgnoreCase(computedColumns[j].getName())) {
                    throw new IllegalArgumentException(String.format(
                            "There is already a column named %s on table %s, please change your computed column name",
                            new Object[] { computedColumns[j].getName(), this.getIdentity() }));
                }
            }
        }
        for (int i = 0; i < computedColumns.length; i++) {
            computedColumns[i].init(ret);
            ret.columns[i + this.columns.length] = computedColumns[i];
        }
        return ret;
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
        return concatResourcePath(getIdentity(), project);
    }

    /**
     * @deprecated this is for compatible with data model v1;
     * @return
     */
    public String getResourcePathV1() {
        return concatResourcePath(name, null);
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

    public static String concatRawResourcePath(String nameOnPath) {
        return ResourceStore.TABLE_RESOURCE_ROOT + "/" + nameOnPath + ".json";
    }

    public static String concatResourcePath(String tableIdentity, String prj) {
        if (prj == null || prj.isEmpty())
            return ResourceStore.TABLE_RESOURCE_ROOT + "/" + tableIdentity + ".json";
        else
            return ResourceStore.TABLE_RESOURCE_ROOT + "/" + tableIdentity + "--" + prj + ".json";
    }

    // returns <table, project>
    public static Pair<String, String> parseResourcePath(String path) {
        if (path.endsWith(".json"))
            path = path.substring(0, path.length() - ".json".length());

        int cut = path.lastIndexOf("/");
        if (cut >= 0)
            path = path.substring(cut + 1);

        String table, prj;
        int dash = path.indexOf("--");
        if (dash >= 0) {
            table = path.substring(0, dash);
            prj = path.substring(dash + 2);
        } else {
            table = path;
            prj = null;
        }
        return Pair.newPair(table, prj);
    }

    // ============================================================================

    public String getProject() {
        return project;
    }

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
        if (columns == null) {
            return -1;
        }

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

    public void init(String project) {
        this.project = project;

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
        return "TableDesc{" + "name='" + name + '\'' + ", columns=" + Arrays.toString(columns) + ", sourceType="
                + sourceType + ", tableType='" + tableType + '\'' + ", database=" + database + ", identity='"
                + getIdentity() + '\'' + '}';
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

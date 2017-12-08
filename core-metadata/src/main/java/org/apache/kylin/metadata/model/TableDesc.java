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
import java.util.List;

import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.StringSplitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;

/**
 * Table Metadata from Source. All name should be uppercase.
 */
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class TableDesc extends RootPersistentEntity implements ISourceAware {
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(TableDesc.class);

    private static final String TABLE_TYPE_VIRTUAL_VIEW = "VIRTUAL_VIEW";
    private static final String materializedTableNamePrefix = "kylin_intermediate_";

    public static String concatRawResourcePath(String nameOnPath) {
        return ResourceStore.TABLE_RESOURCE_ROOT + "/" + nameOnPath + ".json";
    }
    
    public static String makeResourceName(String tableIdentity, String prj) {
        return prj == null ? tableIdentity : tableIdentity + "--" + prj;
    }

    // this method should only used for getting dest path when copying from src to dest.
    // if you want to get table's src path, use getResourcePath() instead.
    private static String concatResourcePath(String tableIdentity, String prj) {
        return concatRawResourcePath(makeResourceName(tableIdentity, prj));
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
    private boolean isBorrowedFromGlobal = false;

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

    @Override
    public String resourceName() {
        return makeResourceName(getIdentity(), getProject());
    }
    
    public TableDesc appendColumns(ColumnDesc[] computedColumns, boolean makeCopy) {
        if (computedColumns == null || computedColumns.length == 0) {
            return this;
        }

        TableDesc ret = makeCopy ? new TableDesc(this) : this;
        ColumnDesc[] existingColumns = ret.columns;
        List<ColumnDesc> newColumns = Lists.newArrayList();

        for (int j = 0; j < computedColumns.length; j++) {

            //check name conflict
            boolean isFreshCC = true;
            for (int i = 0; i < existingColumns.length; i++) {
                if (existingColumns[i].getName().equalsIgnoreCase(computedColumns[j].getName())) {
                    // if we're adding a computed column twice, it should be allowed without producing duplicates
                    if (!existingColumns[i].isComputedColumn()) {
                        throw new IllegalArgumentException(String.format(
                                "There is already a column named %s on table %s, please change your computed column name",
                                new Object[] { computedColumns[j].getName(), this.getIdentity() }));
                    } else {
                        isFreshCC = false;
                    }
                }
            }

            if (isFreshCC) {
                newColumns.add(computedColumns[j]);
            }
        }

        List<ColumnDesc> expandedColumns = Lists.newArrayList(existingColumns);
        for (ColumnDesc newColumnDesc : newColumns) {
            newColumnDesc.init(ret);
            expandedColumns.add(newColumnDesc);
        }
        ret.columns = expandedColumns.toArray(new ColumnDesc[0]);
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
        if (isBorrowedFromGlobal()) {
            return concatResourcePath(getIdentity(), null);
        }
        
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

    public boolean isBorrowedFromGlobal() {
        return isBorrowedFromGlobal;
    }

    public void setBorrowedFromGlobal(boolean borrowedFromGlobal) {
        isBorrowedFromGlobal = borrowedFromGlobal;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
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

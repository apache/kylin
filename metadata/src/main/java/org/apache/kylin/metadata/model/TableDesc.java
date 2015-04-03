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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.StringSplitter;

/**
 * Table Metadata from Source. All name should be uppercase.
 * <p/>
 * User: lukhan Date: 10/15/13 Time: 9:06 AM To change this template use File |
 * Settings | File Templates.
 */
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class TableDesc extends RootPersistentEntity {
    @JsonProperty("name")
    private String name;
    @JsonProperty("columns")
    private ColumnDesc[] columns;

    private DatabaseDesc database = new DatabaseDesc();

    private String identity = null;

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
            this.name = name;
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
    public String toString() {
        return "TableDesc [database=" + getDatabase() + " name=" + name + "]";
    }

    /** create a mockup table for unit test */
    public static TableDesc mockup(String tableName) {
        TableDesc mockup = new TableDesc();
        mockup.setName(tableName);
        return mockup;
    }
}

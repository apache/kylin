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
package org.apache.kylin.tool.bisync.model;

public class ColumnDef {

    private String role;

    private String tableAlias;

    private String columnAlias;

    private String columnName;

    private String columnType;

    private boolean isHidden;

    private boolean isComputedColumn;

    public ColumnDef(String role, String tableAlias, String columnAlias, String columnName, String columnType,
            boolean isHidden, boolean isComputedColumn) {
        this.role = role;
        this.tableAlias = tableAlias;
        this.columnAlias = columnAlias;
        this.columnName = columnName;
        this.columnType = columnType;
        this.isHidden = isHidden;
        this.isComputedColumn = isComputedColumn;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public String getTableAlias() {
        return tableAlias;
    }

    public void setTableAlias(String tableAlias) {
        this.tableAlias = tableAlias;
    }

    public boolean isHidden() {
        return isHidden;
    }

    public void setHidden(boolean hidden) {
        isHidden = hidden;
    }

    public String getColumnAlias() {
        return columnAlias;
    }

    public void setColumnAlias(String columnAlias) {
        this.columnAlias = columnAlias;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getColumnType() {
        return columnType;
    }

    public void setColumnType(String columnType) {
        this.columnType = columnType;
    }

    public boolean isComputedColumn() {
        return isComputedColumn;
    }

    public void setComputedColumn(boolean computedColumn) {
        isComputedColumn = computedColumn;
    }
}

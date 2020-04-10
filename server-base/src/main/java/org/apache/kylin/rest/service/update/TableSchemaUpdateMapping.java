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

package org.apache.kylin.rest.service.update;

import java.util.Locale;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

public class TableSchemaUpdateMapping {

    private String database;

    private String tableName;

    public boolean isDatabaseChanged() {
        return !Strings.isNullOrEmpty(database);
    }

    public String getDatabase(String dbName) {
        String ret = isDatabaseChanged() ? database : dbName;
        return ret.toUpperCase(Locale.ROOT);
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public boolean isTableNameChanged() {
        return !Strings.isNullOrEmpty(tableName);
    }

    public String getTableName(String tblName) {
        String ret = isTableNameChanged() ? tableName : tblName;
        return ret.toUpperCase(Locale.ROOT);
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public boolean isTableIdentityChanged() {
        return isDatabaseChanged() || isTableNameChanged();
    }

    public String getTableIdentity(String tableIdentity) {
        String[] tableNameEs = tableIdentity.split("\\.");
        Preconditions.checkArgument(tableNameEs.length == 2);
        return getTableIdentity(tableNameEs[0], tableNameEs[1]);
    }

    public String getTableIdentity(String database, String tableName) {
        return getDatabase(database) + "." + getTableName(tableName);
    }
}
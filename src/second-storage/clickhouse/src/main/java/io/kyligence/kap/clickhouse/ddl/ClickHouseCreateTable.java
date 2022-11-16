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
package io.kyligence.kap.clickhouse.ddl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.kyligence.kap.guava20.shaded.common.collect.Maps;
import io.kyligence.kap.secondstorage.ddl.CreateTable;
import io.kyligence.kap.secondstorage.ddl.exp.TableIdentifier;


public class ClickHouseCreateTable extends CreateTable<ClickHouseCreateTable> {

    // clickhouse special
    private String engine;
    private TableIdentifier likeTable;
    private String partitionBy;
    private final List<String> orderBy;
    private final Map<TableSetting, String> tableSettings = Maps.newHashMap();

    public ClickHouseCreateTable(TableIdentifier table, boolean ifNotExists) {
        super(table, ifNotExists);
        this.likeTable = null;
        this.engine = null;
        this.orderBy = new ArrayList<>();
    }

    public ClickHouseCreateTable engine(String engine) {
        this.engine = engine;
        return this;
    }
    public String engine() {
        return engine;
    }

    public ClickHouseCreateTable tableSettings(TableSetting tableSetting, String value) {
        tableSettings.put(tableSetting, value);
        return this;
    }

    public Map<TableSetting, String> getTableSettings() {
        return this.tableSettings;
    }

    public ClickHouseCreateTable partitionBy(String column) {
        this.partitionBy = column;
        return this;
    }

    public String partitionBy() {
        return this.partitionBy;
    }

    public ClickHouseCreateTable likeTable(String database, String table) {
        this.likeTable = TableIdentifier.table(database, table);
        return this;
    }
    public TableIdentifier likeTable() {
        return likeTable;
    }
    public boolean createTableWithColumns() {
        return likeTable == null;
    }

    public final ClickHouseCreateTable orderBy(List<String> fields) {
        orderBy.addAll(fields);
        return this;
    }

    public final List<String> orderBy() {
        return orderBy;
    }

    public static ClickHouseCreateTable createCKTable(String database, String table) {
        return new ClickHouseCreateTable(TableIdentifier.table(database, table), false);
    }

    public static ClickHouseCreateTable createCKTableIgnoreExist(String database, String table) {
        return new ClickHouseCreateTable(TableIdentifier.table(database, table), true);
    }
}

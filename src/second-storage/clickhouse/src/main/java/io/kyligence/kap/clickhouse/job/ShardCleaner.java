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

package io.kyligence.kap.clickhouse.job;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.kyligence.kap.clickhouse.database.ClickHouseOperator;
import io.kyligence.kap.clickhouse.ddl.ClickHouseRender;
import io.kyligence.kap.secondstorage.SecondStorageNodeHelper;
import io.kyligence.kap.secondstorage.ddl.AlterTable;
import io.kyligence.kap.secondstorage.ddl.DropDatabase;
import io.kyligence.kap.secondstorage.ddl.DropTable;
import io.kyligence.kap.secondstorage.ddl.exp.TableIdentifier;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.msgpack.core.Preconditions;

import java.sql.Date;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

@Getter
@Slf4j
public class ShardCleaner {
    @JsonProperty("node")
    private String node;
    @JsonProperty("database")
    private String database;
    @JsonProperty("table")
    private String table;
    @JsonProperty("partitions")
    private List<Date> partitions;
    @JsonProperty("isFull")
    private boolean isFull = false;
    @JsonProperty("dateFormat")
    private String dateFormat;

    @JsonIgnore
    private ClickHouse clickHouse;

    public ShardCleaner() {
    }

    public ShardCleaner(String node, String database) {
        this(node, database, null, null, null);
    }

    public ShardCleaner(String node, String database, String table) {
        this(node, database, table, null, null);
    }

    public ShardCleaner(String node, String database, String table, List<Date> partitions, String dateFormat) {
        this(node, database, table, partitions, false, dateFormat);
    }

    public ShardCleaner(String node, String database, String table, List<Date> partitions, boolean isFull, String dateFormat) {
        this.node = Preconditions.checkNotNull(node);
        this.database = Preconditions.checkNotNull(database);
        this.table = table;
        this.partitions = partitions;
        this.isFull = isFull;
        this.dateFormat = dateFormat;
        Preconditions.checkState(!(isFull && CollectionUtils.isNotEmpty(partitions)));
        if (!isFull && CollectionUtils.isNotEmpty(partitions)) {
            Preconditions.checkState(!StringUtils.isEmpty(dateFormat),
                    "incremental build should have partition dateformat");
        }
    }

    public ClickHouse getClickHouse() {
        if (Objects.isNull(clickHouse)) {
            clickHouse = new ClickHouse(SecondStorageNodeHelper.resolve(node));
        }
        return clickHouse;
    }

    public void cleanDatabase() throws SQLException {
        val dropDatabase = DropDatabase.dropDatabase(database);
        log.debug("drop database {}", database);
        Preconditions.checkNotNull(getClickHouse()).apply(dropDatabase.toSql(getRender()));
    }

    private ClickHouseRender getRender() {
        return new ClickHouseRender();
    }

    public void cleanTable() throws SQLException {
        Preconditions.checkNotNull(table);
        val dropTable = DropTable.dropTable(database, table);
        log.debug("drop table {}.{}", database, table);
        Preconditions.checkNotNull(getClickHouse()).apply(dropTable.toSql(getRender()));
    }

    public void cleanPartitions() throws SQLException {
        val operator = new ClickHouseOperator(Preconditions.checkNotNull(getClickHouse()));
        val databases = operator.listDatabases();
        if (!databases.contains(database)) {
            log.info("database {} doesn't exist, skip clean partitions {}", database, partitions);
            return;
        }
        val tables = operator.listTables(database);
        if (!tables.contains(table)) {
            log.info("table {}.{} doesn't exist, skip clean partitions {}", database, table, partitions);
            return;
        }
        Preconditions.checkNotNull(table);
        AlterTable alterTable;
        if (isFull) {
            cleanTable();
        } else {
            log.debug("drop partitions in table {}.{}: {}", database, table, partitions);
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat, Locale.ROOT);
            for (val partition : partitions) {
                alterTable = new AlterTable(TableIdentifier.table(database, table), new AlterTable.ManipulatePartition(
                        simpleDateFormat.format(partition), AlterTable.PartitionOperation.DROP));
                Preconditions.checkNotNull(getClickHouse()).apply(alterTable.toSql(getRender()));
            }
        }
    }
}

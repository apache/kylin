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

package io.kyligence.kap.clickhouse.database;

import io.kyligence.kap.clickhouse.ddl.ClickHouseRender;
import io.kyligence.kap.clickhouse.job.ClickHouse;
import io.kyligence.kap.secondstorage.database.DatabaseOperator;
import io.kyligence.kap.secondstorage.ddl.DropTable;
import io.kyligence.kap.secondstorage.ddl.ShowDatabases;
import io.kyligence.kap.secondstorage.ddl.ShowTables;
import lombok.SneakyThrows;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

public class ClickHouseOperator implements DatabaseOperator {
    private ClickHouse clickHouse;
    private final ClickHouseRender render = new ClickHouseRender();

    public ClickHouseOperator(final String jdbcUrl) {
        try {
            this.clickHouse = new ClickHouse(jdbcUrl);
        } catch (SQLException e) {
            ExceptionUtils.rethrow(e);
        }
    }

    public ClickHouseOperator(ClickHouse clickHouse) {
        this.clickHouse = clickHouse;
    }

    @SneakyThrows
    @Override
    public List<String> listDatabases() {
        ShowDatabases showDatabases = new ShowDatabases();
        return clickHouse.query(showDatabases.toSql(render), resultSet -> {
            try {
                return resultSet.getString(1);
            } catch (SQLException e) {
                return ExceptionUtils.rethrow(e);
            }
        });
    }

    @SneakyThrows
    @Override
    public List<String> listTables(final String database) {
        ShowTables showTables = ShowTables.createShowTables(database);
        return clickHouse.query(showTables.toSql(render), resultSet -> {
            try {
                return resultSet.getString(1);
            } catch (SQLException e) {
                return ExceptionUtils.rethrow(e);
            }
        });
    }

    @SneakyThrows
    @Override
    public void dropTable(final String database, final String table) {
        DropTable dropTable = DropTable.dropTable(database, table);
        clickHouse.apply(dropTable.toSql(render));
    }

    @Override
    public void close() throws IOException {
        this.clickHouse.close();
    }
}

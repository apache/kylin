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

import io.kyligence.kap.clickhouse.ddl.ClickHouseRender;
import io.kyligence.kap.secondstorage.ddl.Select;
import io.kyligence.kap.secondstorage.ddl.exp.ColumnWithAlias;
import io.kyligence.kap.secondstorage.ddl.exp.GroupBy;
import io.kyligence.kap.secondstorage.ddl.exp.TableIdentifier;
import lombok.Builder;
import lombok.Data;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;
import java.util.function.Function;

public class ClickHouseSystemQuery {
    public static final Function<ResultSet, PartitionSize> TABLE_STORAGE_MAPPER= rs -> {
        try {
            return PartitionSize.builder()
                    .database(rs.getString(1))
                    .table(rs.getString(2))
                    .partition(rs.getString(3))
                    .bytes(Long.parseLong(rs.getString(4)))
                    .build();
        } catch (SQLException sqlException) {
            return ExceptionUtils.rethrow(sqlException);
        }
    };

    public static final Function<ResultSet, QueryMetric> QUERY_METRIC_MAPPER= rs -> {
        try {
            return QueryMetric.builder()
                    .readRows(rs.getLong(1))
                    .readBytes(rs.getLong(2))
                    .clientName(rs.getString(3))
                    .resultRows(rs.getLong(4))
                    .build();
        } catch (SQLException sqlException) {
            return ExceptionUtils.rethrow(sqlException);
        }
    };

    private static final Select tableStorageSize = new Select(TableIdentifier.table("system", "parts"))
            .column(ColumnWithAlias.builder().name("database").build())
            .column(ColumnWithAlias.builder().name("table").build())
            .column(ColumnWithAlias.builder().name("partition").build())
            .column(ColumnWithAlias.builder().expr("sum(bytes)").build())
            .groupby(new GroupBy()
                    .column(ColumnWithAlias.builder().name("database").build())
                    .column(ColumnWithAlias.builder().name("table").build())
                    .column(ColumnWithAlias.builder().name("partition").build()))
            .where("active=1");

    private static final Select queryMetric = new Select(TableIdentifier.table("system", "query_log"))
            .column(ColumnWithAlias.builder().expr("sum(read_rows)").alias("readRows").build())
            .column(ColumnWithAlias.builder().expr("sum(read_bytes)").alias("readBytes").build())
            .column(ColumnWithAlias.builder().name("client_name").alias("clientName").build())
            .column(ColumnWithAlias.builder().expr("sum(result_rows)").alias("resultRows").build())
            .groupby(new GroupBy().column(ColumnWithAlias.builder().name("client_name").build()))
            .where("type = 'QueryFinish' AND event_time >= addHours(now(), -1) AND event_date >= addDays(now(), -1) AND position(client_name, '%s') = 1");

    private ClickHouseSystemQuery() {}

    public static String queryTableStorageSize() {
        return tableStorageSize.toSql(new ClickHouseRender());
    }

    public static String queryQueryMetric(String queryId) {
        return String.format(Locale.ROOT, queryMetric.toSql(new ClickHouseRender()), queryId);
    }

    @Data
    @Builder
    public static class PartitionSize {
        private String database;
        private String table;
        private String partition;
        private long bytes;
    }

    @Data
    @Builder
    public static class QueryMetric {
        private String clientName;
        private long readRows;
        private long readBytes;
        private long resultRows;
    }

    @Data
    @Builder
    public static class DescTable {
        private String column;
        private String datatype;
    }
}

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

package org.apache.kylin.metadata.query;

import java.nio.charset.Charset;
import java.sql.CallableStatement;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.TimeZone;

import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.mybatis.dynamic.sql.SqlColumn;
import org.mybatis.dynamic.sql.SqlTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;

public class QueryHistoryTable extends SqlTable {

    public final SqlColumn<String> sql = column("sql_text", JDBCType.VARCHAR);
    public final SqlColumn<String> sqlPattern = column("sql_pattern", JDBCType.VARCHAR);
    public final SqlColumn<Long> queryTime = column("query_time", JDBCType.BIGINT);
    public final SqlColumn<String> month = column("month", JDBCType.VARCHAR);
    public final SqlColumn<Long> queryFirstDayOfMonth = column("query_first_day_of_month", JDBCType.BIGINT);
    public final SqlColumn<Long> queryFirstDayOfWeek = column("query_first_day_of_week", JDBCType.BIGINT);
    public final SqlColumn<Long> queryDay = column("query_day", JDBCType.BIGINT);
    public final SqlColumn<Long> duration = column("duration", JDBCType.BIGINT);
    public final SqlColumn<String> queryRealizations = column("realizations", JDBCType.VARCHAR);
    public final SqlColumn<String> hostName = column("server", JDBCType.VARCHAR);
    public final SqlColumn<String> querySubmitter = column("submitter", JDBCType.VARCHAR);
    public final SqlColumn<String> queryStatus = column("query_status", JDBCType.VARCHAR);
    public final SqlColumn<String> queryId = column("query_id", JDBCType.VARCHAR);
    public final SqlColumn<Long> id = column("id", JDBCType.BIGINT);
    public final SqlColumn<Long> totalScanCount = column("total_scan_count", JDBCType.BIGINT);
    public final SqlColumn<Long> totalScanBytes = column("total_scan_bytes", JDBCType.BIGINT);
    public final SqlColumn<Long> resultRowCount = column("result_row_count", JDBCType.BIGINT);
    public final SqlColumn<Boolean> cacheHit = column("cache_hit", JDBCType.BOOLEAN);
    public final SqlColumn<Boolean> indexHit = column("index_hit", JDBCType.BOOLEAN);
    public final SqlColumn<String> engineType = column("engine_type", JDBCType.VARCHAR);
    public final SqlColumn<String> projectName = column("project_name", JDBCType.BIGINT);
    public final SqlColumn<String> errorType = column("error_type", JDBCType.VARCHAR);
    public final SqlColumn<QueryHistoryInfo> queryHistoryInfo = column("reserved_field_3", JDBCType.BLOB,
            QueryHistoryInfoHandler.class.getName());

    public QueryHistoryTable(String tableName) {
        super(tableName);
    }

    public static class QueryHistoryInfoHandler implements TypeHandler<QueryHistoryInfo> {

        private static final Logger logger = LoggerFactory.getLogger(QueryHistoryInfoHandler.class);

        @Override
        public void setParameter(PreparedStatement ps, int i, QueryHistoryInfo parameter, JdbcType jdbcType)
                throws SQLException {
            byte[] bytes = "".getBytes(Charset.defaultCharset());
            try {
                bytes = JsonUtil.writeValueAsBytes(parameter);
            } catch (JsonProcessingException e) {
                logger.error("Fail transform object to json", e);
            }
            ps.setBytes(i, bytes);
        }

        @Override
        public QueryHistoryInfo getResult(ResultSet rs, String columnName) throws SQLException {
            if (rs.getBytes(columnName) == null) {
                return null;
            }

            return JsonUtil.readValueQuietly(rs.getBytes(columnName), QueryHistoryInfo.class);
        }

        @Override
        public QueryHistoryInfo getResult(ResultSet rs, int columnIndex) throws SQLException {
            if (rs.getBytes(columnIndex) == null) {
                return null;
            }

            return JsonUtil.readValueQuietly(rs.getBytes(columnIndex), QueryHistoryInfo.class);
        }

        @Override
        public QueryHistoryInfo getResult(CallableStatement cs, int columnIndex) throws SQLException {
            if (cs.getBytes(columnIndex) == null) {
                return null;
            }

            return JsonUtil.readValueQuietly(cs.getBytes(columnIndex), QueryHistoryInfo.class);
        }
    }

    public static class InstantHandler implements TypeHandler<Instant> {

        @Override
        public void setParameter(PreparedStatement ps, int i, Instant parameter, JdbcType jdbcType)
                throws SQLException {
            ps.setLong(i, parameter.toEpochMilli());
        }

        @Override
        public Instant getResult(ResultSet rs, String columnName) throws SQLException {
            int offset = TimeZone.getTimeZone(KylinConfig.getInstanceFromEnv().getTimeZone()).getRawOffset();
            long offetTime = Instant.ofEpochMilli(rs.getLong(columnName)).plusMillis(offset).toEpochMilli();
            return Instant.ofEpochMilli(offetTime);
        }

        @Override
        public Instant getResult(ResultSet rs, int columnIndex) throws SQLException {
            int offset = TimeZone.getTimeZone(KylinConfig.getInstanceFromEnv().getTimeZone()).getRawOffset();
            long offetTime = Instant.ofEpochMilli(rs.getLong(columnIndex)).plusMillis(offset).toEpochMilli();
            return Instant.ofEpochMilli(offetTime);
        }

        @Override
        public Instant getResult(CallableStatement cs, int columnIndex) throws SQLException {
            int offset = TimeZone.getTimeZone(KylinConfig.getInstanceFromEnv().getTimeZone()).getRawOffset();
            long offetTime = Instant.ofEpochMilli(cs.getLong(columnIndex)).plusMillis(offset).toEpochMilli();
            return Instant.ofEpochMilli(offetTime);
        }
    }

}

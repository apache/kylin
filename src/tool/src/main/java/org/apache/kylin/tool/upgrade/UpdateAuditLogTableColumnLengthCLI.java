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

package org.apache.kylin.tool.upgrade;

import static org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil.datasourceParameters;

import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.util.Locale;
import java.util.Objects;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.metadata.JdbcDataSource;
import org.apache.kylin.common.util.Unsafe;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpdateAuditLogTableColumnLengthCLI {
    private static final String SHOW_TABLE = "SHOW TABLES LIKE '%s'";
    private static final String UPDATE_COL_TO_TABLE_SQL = "alter table %s modify column %s %s";
    private static final String TABLE_SUFFIX = "_audit_log";
    private static final String AUDIT_LOG_TABLE_OPERATOR = "operator";
    private static final int COLUMN_LENGTH = 200;

    public static void main(String[] args) throws Exception {
        log.info("Start to modify column length log...");
        try {
            KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            val url = kylinConfig.getMetadataUrl();
            val props = datasourceParameters(url);
            val dataSource = JdbcDataSource.getDataSource(props);
            JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
            jdbcTemplate.setQueryTimeout(-1);
            String tableName = url.getIdentifier() + TABLE_SUFFIX;
            if (tableIsExist(jdbcTemplate, tableName)) {
                int length = COLUMN_LENGTH;
                String sql = String.format(Locale.ROOT, "SELECT %s FROM %s LIMIT 1", AUDIT_LOG_TABLE_OPERATOR,
                        tableName);
                try (PreparedStatement preparedStatement = dataSource.getConnection().prepareStatement(sql)) {
                    ResultSetMetaData metaData = preparedStatement.getMetaData();
                    length = metaData.getPrecision(1);
                }
                if (length < COLUMN_LENGTH) {
                    modifyColumnLength(jdbcTemplate, tableName, AUDIT_LOG_TABLE_OPERATOR,
                            String.format(Locale.ROOT, "varchar(%s)", COLUMN_LENGTH));
                }
            } else {
                log.info("table {} not exist.", tableName);
            }
        } catch (Exception e) {
            log.error("modify column length error", e);
        }
        Unsafe.systemExit(0);
    }

    public static boolean tableIsExist(JdbcTemplate jdbcTemplate, String tableName) {
        try {
            String object = jdbcTemplate.queryForObject(String.format(Locale.ROOT, SHOW_TABLE, tableName),
                    (resultSet, i) -> resultSet.getString(1));
            return Objects.equals(tableName, object);
        } catch (EmptyResultDataAccessException emptyResultDataAccessException) {
            log.error("not found table", emptyResultDataAccessException);
            return false;
        }
    }

    public static void modifyColumnLength(JdbcTemplate jdbcTemplate, String tableName, String column, String type) {
        String sql = String.format(Locale.ROOT, UPDATE_COL_TO_TABLE_SQL, tableName, column, type);
        try {
            jdbcTemplate.execute(sql);
            log.info("update column length finished!");
        } catch (Exception e) {
            log.error("Failed to execute upgradeSql: {}", sql, e);
        }
    }
}

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
package org.apache.kylin.source.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Locale;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.DBUtils;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.sdk.datasource.framework.JdbcConnector;
import org.apache.kylin.source.IReadableTable;

import org.apache.kylin.guava30.shaded.common.collect.Lists;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcTableReader implements IReadableTable.TableReader {
    private static final String DATABASE_AND_TABLE = "%s.%s";
    private String dbName;
    private String tableName;

    private Connection conn;
    private Statement statement;
    private ResultSet rs;
    private int colCount;

    /**
     * Constructor for reading whole jdbc table
     *
     * @param dataSource
     * @param tableDesc
     * @throws IOException
     */
    public JdbcTableReader(JdbcConnector dataSource, TableDesc tableDesc) throws IOException {
        this.dbName = tableDesc.getDatabase();
        this.tableName = tableDesc.getName();

        String sql = dataSource.convertSql(generateSelectDataStatement(tableDesc));
        try {
            conn = dataSource.getConnection();
            statement = conn.createStatement();
            rs = statement.executeQuery(sql);
            colCount = rs.getMetaData().getColumnCount();
        } catch (SQLException e) {
            log.error("error when get jdbc tableReader.", e);
            throw new IOException(String.format(Locale.ROOT, "error while exec %s", sql));
        }
    }

    @Override
    public boolean next() throws IOException {
        try {
            return rs.next();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public String[] getRow() {
        String[] ret = new String[colCount];
        for (int i = 1; i <= colCount; i++) {
            try {
                Object o = rs.getObject(i);
                String result;
                if (null == o || o instanceof byte[]) {
                    result = null;
                } else {
                    result = o.toString();
                }
                ret[i - 1] = result;
            } catch (Exception e) {
                log.error("Failed to get row", e);
            }
        }
        return ret;
    }

    @Override
    public void close() throws IOException {
        DBUtils.closeQuietly(rs);
        DBUtils.closeQuietly(statement);
        DBUtils.closeQuietly(conn);
    }

    public String toString() {
        return "jdbc table reader for: " + dbName + "." + tableName;
    }

    private String generateSelectDataStatement(TableDesc tableDesc) {
        ColumnDesc columnDesc;
        List<String> columns = Lists.newArrayList();
        for (int i = 0; i < tableDesc.getColumns().length; i++) {
            columnDesc = tableDesc.getColumns()[i];
            columns.add(String.format(Locale.ROOT, DATABASE_AND_TABLE, tableDesc.getName(), columnDesc.getName()));
        }
        final String sep = " ";
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT").append(sep).append(StringUtils.join(columns, ",")).append(sep).append("FROM").append(sep)
                .append(String.format(Locale.ROOT, DATABASE_AND_TABLE, tableDesc.getDatabase(), tableDesc.getName()));
        return sql.toString();
    }
}

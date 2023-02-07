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

package org.apache.kylin.query.pushdown;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.util.DBUtils;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.source.adhocquery.IPushDownRunner;

public class PushDownRunnerJdbcImpl implements IPushDownRunner {

    private JdbcPushDownConnectionManager manager = null;

    @Override
    public void init(KylinConfig config, String project) {
        try {
            manager = JdbcPushDownConnectionManager.getConnectionManager(config, project);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public void init(KylinConfig config) {
        init(config, "");
    }

    @Override
    public void executeQuery(String query, List<List<String>> results, List<SelectedColumnMeta> columnMetas,
            String project) throws SQLException {
        Statement statement = null;
        Connection connection = manager.getConnection();
        ResultSet resultSet = null;

        //extract column metadata
        ResultSetMetaData metaData = null;
        int columnCount = 0;
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(query);
            extractResults(resultSet, results);
            metaData = resultSet.getMetaData();
            columnCount = metaData.getColumnCount();

            // fill in selected column meta
            for (int i = 1; i <= columnCount; ++i) {
                columnMetas.add(new SelectedColumnMeta(metaData.isAutoIncrement(i), metaData.isCaseSensitive(i), false,
                        metaData.isCurrency(i), metaData.isNullable(i), false, metaData.getColumnDisplaySize(i),
                        metaData.getColumnLabel(i), metaData.getColumnName(i), null, null, null,
                        metaData.getPrecision(i), metaData.getScale(i), metaData.getColumnType(i),
                        metaData.getColumnTypeName(i), metaData.isReadOnly(i), false, false));
            }
        } finally {
            DBUtils.closeQuietly(resultSet);
            DBUtils.closeQuietly(statement);
            manager.close(connection);
        }
    }

    @Override
    public void executeUpdate(String sql, String project) throws SQLException {
        Statement statement = null;
        Connection connection = manager.getConnection();

        try {
            statement = connection.createStatement();
            statement.execute(sql);
        } finally {
            DBUtils.closeQuietly(statement);
            manager.close(connection);
        }
    }

    private void extractResults(ResultSet resultSet, List<List<String>> results) throws SQLException {
        List<String> oneRow = new LinkedList<String>();

        while (resultSet.next()) {
            //logger.debug("resultSet value: " + resultSet.getString(1));
            for (int i = 0; i < resultSet.getMetaData().getColumnCount(); i++) {
                oneRow.add((resultSet.getString(i + 1)));
            }

            results.add(new LinkedList<String>(oneRow));
            oneRow.clear();
        }
    }

    @Override
    public String getName() {
        return QueryContext.PUSHDOWN_RDBMS;
    }
}

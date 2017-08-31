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

package org.apache.kylin.query.adhoc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DBUtils;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.source.adhocquery.IPushDownRunner;

public class PushDownRunnerJdbcImpl implements IPushDownRunner {

    private static org.apache.kylin.query.adhoc.JdbcConnectionPool pool = null;

    @Override
    public void init(KylinConfig config) {
        if (pool == null) {
            pool = new JdbcConnectionPool();
            JdbcConnectionFactory factory = new JdbcConnectionFactory(config.getJdbcUrl(), config.getJdbcDriverClass(),
                    config.getJdbcUsername(), config.getJdbcPassword());
            GenericObjectPool.Config poolConfig = new GenericObjectPool.Config();
            poolConfig.maxActive = config.getPoolMaxTotal();
            poolConfig.maxIdle = config.getPoolMaxIdle();
            poolConfig.minIdle = config.getPoolMinIdle();

            try {
                pool.createPool(factory, poolConfig);
            } catch (IOException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
    }

    @Override
    public void executeQuery(String query, List<List<String>> results, List<SelectedColumnMeta> columnMetas)
            throws Exception {
        Statement statement = null;
        Connection connection = this.getConnection();
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
        } catch (SQLException sqlException) {
            throw sqlException;
        } finally {
            DBUtils.closeQuietly(resultSet);
            DBUtils.closeQuietly(statement);
            closeConnection(connection);
        }
    }

    @Override
    public void executeUpdate(String sql) throws Exception {
        Statement statement = null;
        Connection connection = this.getConnection();

        try {
            statement = connection.createStatement();
            statement.execute(sql);
        } catch (SQLException sqlException) {
            throw sqlException;
        } finally {
            DBUtils.closeQuietly(statement);
            closeConnection(connection);
        }
    }

    private Connection getConnection() {
        return pool.getConnection();
    }

    private void closeConnection(Connection connection) {
        pool.returnConnection(connection);
    }

    static void extractResults(ResultSet resultSet, List<List<String>> results) throws SQLException {
        List<String> oneRow = new LinkedList<String>();

        try {
            while (resultSet.next()) {
                //logger.debug("resultSet value: " + resultSet.getString(1));
                for (int i = 0; i < resultSet.getMetaData().getColumnCount(); i++) {
                    oneRow.add((resultSet.getString(i + 1)));
                }

                results.add(new LinkedList<String>(oneRow));
                oneRow.clear();
            }
        } catch (SQLException sqlException) {
            throw sqlException;
        }
    }

}

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

package org.apache.kylin.rest.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.calcite.sql.parser.impl.ParseException;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.model.SelectedColumnMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveReroute {

    private static final Logger logger = LoggerFactory.getLogger(HiveReroute.class);

    public static final String HIVE_DRIVER_CLASS = "org.apache.hive.jdbc.HiveDriver";

    public boolean shouldReroute(SQLException sqlException) {
        Throwable rootCause = ExceptionUtils.getRootCause(sqlException);
        boolean isExpectedCause = (rootCause instanceof SqlValidatorException || rootCause instanceof ParseException);
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        return isExpectedCause && kylinConfig.isHiveReroutingEnabled();
    }

    public void query(String sql, List<List<String>> results, List<SelectedColumnMeta> columnMetas) throws Exception {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        logger.debug("query rerouting option enabled for Kylin");

        // running query to hive
        Connection conn = null;
        ResultSet resultSet = null;
        try {
            conn = createConnection(kylinConfig.getHiveRerouteUrl(), kylinConfig.getHiveRerouteUsername(), kylinConfig.getHiveReroutePassword());
            resultSet = executQuery(conn, sql);
            extractColumnMetadata(resultSet, columnMetas);
            extractResults(resultSet, results);
        } finally {
            closeConnection(conn);
        }
    }

    /**
     * Create a connection to the Hive server by passing the required connection parameters.
     * @param connectionURL: JDBC URL to connect to the Hive server
     * @param username: Username to connect with (optional)
     * @param password: Password to connect with (optional)
     * @return: Connection object to the Hive server
     * @throws Exception
     */
    private Connection createConnection(String connectionURL, String username, String password) throws Exception {
        logger.info("rerouting to : " + connectionURL + " for executing the query");

        Class.forName(HIVE_DRIVER_CLASS);
        Connection connection = DriverManager.getConnection(connectionURL, username, password);
        return connection;
    }

    /**
     * Close the connection to the Hive server.
     * @param connection: Connection object to be closed
     */
    private void closeConnection(Connection connection) {
        if (null != connection) {
            try {
                connection.close();
            } catch (SQLException sqlException) {
                logger.error("failed to close connection", sqlException);
            }
        }
    }

    /**
     * Execute a query in Hive.
     * @param connection: Connection object to the Hive server
     * @param query: Query to be executed
     * @return: ResultSet object of the query executed
     * @throws Exception
     */
    private ResultSet executQuery(Connection connection, String query) throws Exception {
        Statement statement = null;
        ResultSet resultSet = null;

        statement = connection.createStatement();
        resultSet = statement.executeQuery(query);
        return resultSet;
    }

    private void extractColumnMetadata(ResultSet resultSet, List<SelectedColumnMeta> columnMetas) throws SQLException {
        ResultSetMetaData metaData = null;
        int columnCount = 0;

        metaData = resultSet.getMetaData();
        columnCount = metaData.getColumnCount();

        // fill in selected column meta
        for (int i = 1; i <= columnCount; ++i) {
            columnMetas.add(new SelectedColumnMeta(metaData.isAutoIncrement(i), metaData.isCaseSensitive(i), false, metaData.isCurrency(i), metaData.isNullable(i), false, metaData.getColumnDisplaySize(i), metaData.getColumnLabel(i), metaData.getColumnName(i), null, null, null, metaData.getPrecision(i), metaData.getScale(i), metaData.getColumnType(i), metaData.getColumnTypeName(i), metaData.isReadOnly(i), false, false));
        }
    }

    private void extractResults(ResultSet resultSet, List<List<String>> results) throws SQLException {
        List<String> oneRow = new LinkedList<String>();

        while (resultSet.next()) {
            //logger.debug("resultSet value: " + resultSet.getString(1));
            for (int i = 0; i < resultSet.getMetaData().getColumnCount(); i++) {
                oneRow.add((resultSet.getString(i + 1)));
            }

            results.add(new ArrayList<String>(oneRow));
            oneRow.clear();
        }
    }

}
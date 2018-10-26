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

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DBUtils;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.source.adhocquery.AbstractPushdownRunner;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.LinkedList;
import java.util.List;

public class PushDownRunnerJdbcImpl extends AbstractPushdownRunner {

    private JdbcPushDownConnectionManager manager = null;

    @Override
    public void init(KylinConfig config) {
        try {
            manager = JdbcPushDownConnectionManager.getConnectionManager();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public void executeQuery(String query, List<List<String>> results, List<SelectedColumnMeta> columnMetas)
            throws Exception {
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
                SelectedColumnMeta columnMeta = extractColumnMeta(metaData, i);
                columnMetas.add(columnMeta);
            }
        } finally {
            DBUtils.closeQuietly(resultSet);
            DBUtils.closeQuietly(statement);
            manager.close(connection);
        }
    }

    private SelectedColumnMeta extractColumnMeta(ResultSetMetaData resultSetMetaData, int columnIndex) throws SQLException {
        boolean isAutoIncrement = false;
        try {
            isAutoIncrement = resultSetMetaData.isAutoIncrement(columnIndex);
        } catch (SQLException e) {
            // Fall back to default value
        }

        boolean isCaseSensitive = false;
        try {
            isCaseSensitive = resultSetMetaData.isCaseSensitive(columnIndex);
        } catch (SQLException e) {
            // Fall back to default value
        }

        boolean isSearchable = false;
        try {
            isSearchable = resultSetMetaData.isSearchable(columnIndex);
        } catch (SQLException e) {
            // Fall back to default value
        }

        boolean isCurrency = false;
        try {
            isCurrency = resultSetMetaData.isCurrency(columnIndex);
        } catch (SQLException e) {
            // Fall back to default value
        }

        int isNullable = ResultSetMetaData.columnNullableUnknown;
        try {
            isNullable = resultSetMetaData.isNullable(columnIndex);
        } catch (SQLException e) {
            // Fall back to default value
        }

        boolean isSigned = false;
        try {
            isSigned = resultSetMetaData.isSigned(columnIndex);
        } catch (SQLException e) {
            // Fall back to default value
        }

        int columnDisplaySize = 0;
        try {
            columnDisplaySize = resultSetMetaData.getColumnDisplaySize(columnIndex);
        } catch (SQLException e) {
            // Fall back to default value
        }

        String columnLabel = null;
        try {
            columnLabel = resultSetMetaData.getColumnLabel(columnIndex);
            // Suppose column label has format [table name].[column name]
            if (columnLabel.contains(".")) {
                columnLabel = StringUtils.substringAfterLast(columnLabel, ".");
            }
        } catch (SQLException e) {
            // Fall back to default value
        }

        String columnName = null;
        try {
            columnName = resultSetMetaData.getColumnName(columnIndex);
        } catch (SQLException e) {
            // Fall back to default value
        }

        String schemaName = null;
        try {
            schemaName = resultSetMetaData.getSchemaName(columnIndex);
        } catch (SQLException e) {
            // Fall back to default value
        }

        String catelogName = null;
        try {
            catelogName = resultSetMetaData.getCatalogName(columnIndex);
        } catch (SQLException e) {
            // Fall back to default value
        }

        String tableName = null;
        try {
            tableName = resultSetMetaData.getTableName(columnIndex);
        } catch (SQLException e) {
            // Fall back to default value
            // Suppose column label contains table name
            if (columnLabel.contains(".")) {
                tableName = StringUtils.substringBeforeLast(columnLabel, ".");
            }
        }

        int precision = 0;
        try {
            precision = resultSetMetaData.getPrecision(columnIndex);
        } catch (SQLException e) {
            // Fall back to default value
        }

        int scale = 0;
        try {
            scale = resultSetMetaData.getScale(columnIndex);
        } catch (SQLException e) {
            // Fall back to default value
        }

        String columnTypeName = null;
        try {
            columnTypeName = resultSetMetaData.getColumnTypeName(columnIndex);
        } catch (SQLException e) {
            // Fall back to default value
        }

        int columnType = toSqlType(columnTypeName);

        boolean isReadOnly = false;
        try {
            isReadOnly = resultSetMetaData.isReadOnly(columnIndex);
        } catch (SQLException e) {
            // Fall back to default value
        }

        boolean isWritable = false;
        try {
            isWritable = resultSetMetaData.isWritable(columnIndex);
        } catch (SQLException e) {
            // Fall back to default value
        }

        boolean isDefinitelyWritable = false;
        try {
            isDefinitelyWritable = resultSetMetaData.isDefinitelyWritable(columnIndex);
        } catch (SQLException e) {
            // Fall back to default value
        }

        SelectedColumnMeta columnMeta = new SelectedColumnMeta(isAutoIncrement, isCaseSensitive, isSearchable,
                isCurrency, isNullable, isSigned, columnDisplaySize, columnLabel, columnName, schemaName, catelogName,
                tableName, precision, scale, columnType, columnTypeName, isReadOnly, isWritable, isDefinitelyWritable);
        return columnMeta;
    }

    // calcite does not understand all java SqlTypes, for example LONGNVARCHAR -16, thus need this mapping (KYLIN-2966)
    public static int toSqlType(String type) throws SQLException {
        if ("string".equalsIgnoreCase(type)) {
            return Types.VARCHAR;
        } else if ("varchar".equalsIgnoreCase(type)) {
            return Types.VARCHAR;
        } else if ("char".equalsIgnoreCase(type)) {
            return Types.CHAR;
        } else if ("float".equalsIgnoreCase(type)) {
            return Types.FLOAT;
        } else if ("real".equalsIgnoreCase(type)) {
            return Types.REAL;
        } else if ("double".equalsIgnoreCase(type)) {
            return Types.DOUBLE;
        } else if ("boolean".equalsIgnoreCase(type)) {
            return Types.BOOLEAN;
        } else if ("tinyint".equalsIgnoreCase(type)) {
            return Types.TINYINT;
        } else if ("smallint".equalsIgnoreCase(type)) {
            return Types.SMALLINT;
        } else if ("int".equalsIgnoreCase(type)) {
            return Types.INTEGER;
        } else if ("bigint".equalsIgnoreCase(type)) {
            return Types.BIGINT;
        } else if ("date".equalsIgnoreCase(type)) {
            return Types.DATE;
        } else if ("timestamp".equalsIgnoreCase(type)) {
            return Types.TIMESTAMP;
        } else if ("decimal".equalsIgnoreCase(type)) {
            return Types.DECIMAL;
        } else if ("binary".equalsIgnoreCase(type)) {
            return Types.BINARY;
        } else if ("map".equalsIgnoreCase(type)) {
            return Types.JAVA_OBJECT;
        } else if ("array".equalsIgnoreCase(type)) {
            return Types.ARRAY;
        } else if ("struct".equalsIgnoreCase(type)) {
            return Types.STRUCT;
        }
        throw new SQLException("Unrecognized column type: " + type);
    }

    @Override
    public void executeUpdate(String sql) throws Exception {
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
}

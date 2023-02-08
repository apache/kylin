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
package org.apache.kylin.sdk.datasource.framework;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.regex.Pattern;

import javax.sql.rowset.CachedRowSet;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.sdk.datasource.adaptor.AbstractJdbcAdaptor;
import org.apache.kylin.sdk.datasource.framework.conv.ConvMaster;
import org.apache.kylin.sdk.datasource.framework.conv.DefaultConfigurer;
import org.apache.kylin.sdk.datasource.framework.conv.SqlConverter;
import org.apache.kylin.sdk.datasource.framework.def.DataSourceDef;
import org.apache.kylin.sdk.datasource.framework.def.DataSourceDefProvider;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

public class JdbcConnector implements Closeable {

    private final AbstractJdbcAdaptor adaptor;
    private SqlConverter sqlConverter;
    private DataSourceDef jdbcDs;
    private ConvMaster convMaster;

    public JdbcConnector(AbstractJdbcAdaptor adaptor) throws SQLException {
        this.adaptor = adaptor;

        DataSourceDefProvider provider = DataSourceDefProvider.getInstance();
        jdbcDs = provider.getById(this.adaptor.getDataSourceId());
        DataSourceDef kylinDs = provider.getDefault();
        convMaster = new ConvMaster(kylinDs, jdbcDs);
        SqlConverter.IConfigurer configurer = new DefaultConfigurer(this.adaptor, jdbcDs);
        this.sqlConverter = new SqlConverter(configurer, convMaster);
    }

    public String getJdbcUrl() {
        return adaptor.getJdbcUrl();
    }

    public String getJdbcDriver() {
        return adaptor.getJdbcDriver();
    }

    public String getJdbcUser() {
        return adaptor.getJdbcUser();
    }

    public String getJdbcPassword() {
        return adaptor.getJdbcPassword();
    }

    @VisibleForTesting
    public AbstractJdbcAdaptor getAdaptor() {
        return this.adaptor;
    }

    /**
     * Used for sql pushdown.
     *
     * @param orig
     * @return the converted sql statement according to data source dialect
     */
    public String convertSql(String orig) {
        return sqlConverter.convertSql(orig);
    }

    public String convertColumn(String column, String originQuote) {
        return sqlConverter.convertColumn(column, originQuote);
    }

    /**
     * Format date type column
     *
     * @param column
     * @param colType
     * @param format
     * @return
     */
    public String formatDateColumn(String column, DataType colType, String format) {
        return sqlConverter.formatDateColumn(column, colType, format);
    }

    /**
     * convert date type column in where clause
     *
     * @param orig
     * @param partColumn
     * @param partColType
     * @param partColFormat
     * @return the converted sql statement according to data source dialect
     */
    public String convertSqoopSql(String orig, String partColumn, DataType partColType, String partColFormat) {
        String converted = sqlConverter.convertDateCondition(orig, partColumn, partColType, partColFormat);
        return sqlConverter.convertSql(converted);
    }

    /**
     * To check if a sql statement is valid, used for CC evaluation.
     *
     * @param sql
     * @throws Exception
     */
    public void testSql(String sql) throws Exception {
        adaptor.executeUpdate(sql);
    }

    public int toKylinTypeId(String sourceTypeName, int sourceTypeId) {
        Integer kylinTypeId = jdbcDs.getDataTypeValue(sourceTypeName);
        return kylinTypeId != null ? kylinTypeId : adaptor.toKylinTypeId(sourceTypeName, sourceTypeId);
    }

    public String toKylinTypeName(int sourceTypeId) {
        return adaptor.toKylinTypeName(sourceTypeId);
    }

    public void executeUpdate(String sql) throws SQLException {
        adaptor.executeUpdate(sql);
    }

    public void executeUpdate(String[] sqls) throws SQLException {
        adaptor.executeUpdate(sqls);
    }

    public List<String> listDatabases() throws SQLException {
        List<String> dbNames = adaptor.listDatabasesWithCache(true);
        String blackList = jdbcDs.getPropertyValue("schema.database.black-list-pattern");
        if (!StringUtils.isEmpty(blackList)) {
            String[] patterns = blackList.split(",");
            List<String> removed = Lists.newLinkedList();
            for (String p : patterns) {
                Pattern ptn = Pattern.compile(p.trim(), Pattern.CASE_INSENSITIVE);
                for (String name : dbNames) {
                    if (ptn.matcher(name).matches()) {
                        removed.add(name);
                    }
                }
            }
            dbNames.removeAll(removed);
        }
        return dbNames;
    }

    public List<String> listTables(String schema) throws SQLException {
        return adaptor.listTablesWithCache(schema, true);
    }

    public CachedRowSet getTable(String database, String table) throws SQLException {
        return adaptor.getTable(database, table);
    }

    public CachedRowSet listColumns(String database, String table) throws SQLException {
        return adaptor.getTableColumns(database, table);
    }

    public String[] buildSqlToCreateSchema(String schemaName) {
        return adaptor.buildSqlToCreateSchema(schemaName);
    }

    public String[] buildSqlToLoadDataFromLocal(String tableName, String tableFileDir) {
        return adaptor.buildSqlToLoadDataFromLocal(tableName, tableFileDir);
    }

    public String[] buildSqlToCreateTable(String identity, LinkedHashMap<String, String> columnInfo) {
        return adaptor.buildSqlToCreateTable(identity, columnInfo);
    }

    public String[] buildSqlToCreateView(String viewName, String s) {
        return adaptor.buildSqlToCreateView(viewName, s);
    }

    public Connection getConnection() throws SQLException {
        return adaptor.getConnection();
    }

    public Connection getConnectionWithDefaultDb(String dbName) throws SQLException {
        return adaptor.getConnectionWithDefaultDB(dbName);
    }

    public String getPropertyValue(String key, String defaultValue) {
        return jdbcDs.getPropertyValue(key, defaultValue);
    }

    public String getPropertyValue(String key) {
        return jdbcDs.getPropertyValue(key);
    }

    public SqlConverter getSqlConverter() {
        return sqlConverter;
    }

    @Override
    public void close() throws IOException {
        if (adaptor != null)
            adaptor.close();
    }
}

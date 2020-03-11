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
package org.apache.kylin.sdk.datasource.adaptor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.sql.rowset.CachedRowSet;

import org.apache.kylin.shaded.com.google.common.base.Joiner;
import org.apache.commons.lang.StringUtils;

/**
 * A default implementation for <C>AbstractJdbcAdaptor</C>. By default, this adaptor supposed to support most cases.
 * Developers can just extends this class and modify some methods if found somewhere unsupported.
 */
public class DefaultAdaptor extends AbstractJdbcAdaptor {

    private static Joiner joiner = Joiner.on('_');

    public DefaultAdaptor(AdaptorConfig config) throws Exception {
        super(config);
    }

    /**
     * By default, the typeId from JDBC source will be returned.
     * @param type The column type name from JDBC source.
     * @param typeId The column type id from JDBC source.
     * @return
     */
    @Override
    public int toKylinTypeId(String type, int typeId) {
        return typeId;
    }

    /**
     * By default, we accord to Hive's type system for this converting.
     * @param sourceTypeId Column type id from Source
     * @return The column type name supported by Kylin.
     */
    @Override
    public String toKylinTypeName(int sourceTypeId) {
        String result = "any";

        switch (sourceTypeId) {
            case Types.CHAR:
                result = "char";
                break;
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
                result = "varchar";
                break;
            case Types.NUMERIC:
            case Types.DECIMAL:
                result = "decimal";
                break;
            case Types.BIT:
            case Types.BOOLEAN:
                result = "boolean";
                break;
            case Types.TINYINT:
                result = "tinyint";
                break;
            case Types.SMALLINT:
                result = "smallint";
                break;
            case Types.INTEGER:
                result = "integer";
                break;
            case Types.BIGINT:
                result = "bigint";
                break;
            case Types.REAL:
            case Types.FLOAT:
            case Types.DOUBLE:
                result = "double";
                break;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                result = "byte";
                break;
            case Types.DATE:
                result = "date";
                break;
            case Types.TIME:
                result = "time";
                break;
            case Types.TIMESTAMP:
                result = "timestamp";
                break;
            default:
                //do nothing
                break;
        }

        return result;
    }

    /**
     * By default, the column type name from kylin will be returned.
     * @param kylinTypeName A column type name which is defined in Kylin.
     * @return
     */
    @Override
    public String toSourceTypeName(String kylinTypeName) {
        return kylinTypeName;
    }

    /**
     * Be default, nothing happens when fix a sql.
     * @param sql The SQL statement to be fixed.
     * @return The fixed SQL statement.
     */
    @Override
    public String fixSql(String sql) {
        return sql;
    }

    /**
     * By default, use schema as database of kylin.
     * @return
     * @throws SQLException
     */
    @Override
    public List<String> listDatabases() throws SQLException {
        List<String> ret = new LinkedList<>();
        try (Connection con = getConnection(); ResultSet rs = con.getMetaData().getSchemas()) {
            while (rs.next()) {
                String schema = rs.getString("TABLE_SCHEM");
                if (StringUtils.isNotBlank(schema)) {
                    ret.add(schema);
                }
            }
        }
        return ret;
    }

    /**
     * By default, use schema to list tables.
     * @param schema
     * @return
     * @throws SQLException
     */
    @Override
    public List<String> listTables(String schema) throws SQLException {
        List<String> ret = new ArrayList<>();
        try (Connection conn = getConnection(); ResultSet rs = conn.getMetaData().getTables(null, schema, null, null)) {
            while (rs.next()) {
                String name = rs.getString("TABLE_NAME");
                if (StringUtils.isNotBlank(schema)) {
                    ret.add(name);
                }
            }
        }
        return ret;
    }

    @Override
    public List<String> listColumns(String database, String tableName) throws SQLException {
        List<String> ret = new ArrayList<>();
        CachedRowSet columnsRs = getTableColumns(database, tableName);
        while (columnsRs.next()) {
            String name = columnsRs.getString("COLUMN_NAME");
            if (StringUtils.isNotBlank(name)) {
                ret.add(name);
            }
        }
        return ret;
    }

    @Override
    public CachedRowSet getTable(String schema, String table) throws SQLException {
        try (Connection conn = getConnection();
             ResultSet rs = conn.getMetaData().getTables(null, schema, table, null)) {
            return cacheResultSet(rs);
        }
    }

    @Override
    public CachedRowSet getTableColumns(String schema, String table) throws SQLException {
        try (Connection conn = getConnection();
             ResultSet rs = conn.getMetaData().getColumns(null, schema, table, null)) {
            return cacheResultSet(rs);
        }
    }

    @Override
    public String[] buildSqlToCreateSchema(String schemaName) {
        return new String[] { String.format(Locale.ROOT, "CREATE schema IF NOT EXISTS %s", schemaName) };
    }

    @Override
    public String[] buildSqlToLoadDataFromLocal(String tableName, String tableFileDir) {
        return new String[] { String.format(Locale.ROOT, "LOAD DATA INFILE '%s/%s.csv' INTO %s FIELDS TERMINATED BY ',';",
                tableFileDir, tableName, tableName) };
    }

    @Override
    public String[] buildSqlToCreateTable(String tableIdentity, Map<String, String> columnInfo) {
        String dropsql = "DROP TABLE IF EXISTS " + tableIdentity;
        String dropsql2 = "DROP VIEW IF EXISTS " + tableIdentity;

        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE " + tableIdentity + "\n");
        ddl.append("(" + "\n");

        for (Map.Entry<String, String> col : columnInfo.entrySet()) {
            ddl.append(col.getKey() + " " + toSourceTypeName(col.getValue()) + ",\n");
        }

        ddl.deleteCharAt(ddl.length() - 2);
        ddl.append(")");

        return new String[] { dropsql, dropsql2, ddl.toString() };
    }

    @Override
    public String[] buildSqlToCreateView(String viewName, String sql) {
        String dropView = "DROP VIEW IF EXISTS " + viewName;
        String dropTable = "DROP TABLE IF EXISTS " + viewName;
        String createSql = ("CREATE VIEW " + viewName + " AS " + sql);

        return new String[] { dropView, dropTable, createSql };
    }

    /**
     * defects:
     * identifier can not tell column or table or database, here follow the order database->table->column, once matched and returns
     * so once having a database name Test and table name TEst, will always find Test.
     * @param identifier
     * @return identifier with case sensitive
     */
    public String fixIdentifierCaseSensitve(String identifier) {
        try {
            List<String> databases = listDatabasesWithCache();
            for (String database : databases) {
                if (identifier.equalsIgnoreCase(database)) {
                    return database;
                }
            }
            List<String> tables = listTables();
            for (String table : tables) {
                if (identifier.equalsIgnoreCase(table)) {
                    return table;
                }
            }
            List<String> columns = listColumns();
            for (String column : columns) {
                if (identifier.equalsIgnoreCase(column)) {
                    return column;
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return identifier;
    }

    /**
     * Get All tables for sql case sensitive
     * @return
     * @throws SQLException
     */
    public List<String> listTables() throws SQLException {
        List<String> ret = new ArrayList<>();
        if (tablesCache == null || tablesCache.size() == 0) {
            try (Connection conn = getConnection();
                    ResultSet rs = conn.getMetaData().getTables(null, null, null, null)) {
                while (rs.next()) {
                    String name = rs.getString("TABLE_NAME");
                    String database = rs.getString("TABLE_SCHEM") != null ? rs.getString("TABLE_SCHEM")
                            : rs.getString("TABLE_CAT");
                    String cacheKey = joiner.join(config.datasourceId, config.url, database, "tables");
                    List<String> cachedTables = tablesCache.getIfPresent(cacheKey);
                    if (cachedTables == null) {
                        cachedTables = new ArrayList<>();
                        tablesCache.put(cacheKey, cachedTables);
                        logger.debug("Add table cache for database {}", database);
                    }
                    if (!cachedTables.contains(name)) {
                        cachedTables.add(name);
                    }
                    ret.add(name);
                }
            }
        } else {
            for (Map.Entry<String, List<String>> entry : tablesCache.asMap().entrySet()) {
                ret.addAll(entry.getValue());
            }
        }

        return ret;
    }

    /**
     * Get All columns for sql case sensitive
     * @return
     * @throws SQLException
     */
    public List<String> listColumns() throws SQLException {
        List<String> ret = new ArrayList<>();
        if (columnsCache == null || columnsCache.size() == 0) {
            CachedRowSet columnsRs = null;
            try (Connection conn = getConnection();
                    ResultSet rs = conn.getMetaData().getColumns(null, null, null, null)) {
                columnsRs = cacheResultSet(rs);
            }
            while (columnsRs.next()) {
                String database = columnsRs.getString("TABLE_SCHEM") != null ? columnsRs.getString("TABLE_SCHEM")
                        : columnsRs.getString("TABLE_CAT");
                String table = columnsRs.getString("TABLE_NAME");
                String column = columnsRs.getString("COLUMN_NAME");
                String cacheKey = joiner.join(config.datasourceId, config.url, database, table, "columns");
                List<String> cachedColumns = columnsCache.getIfPresent(cacheKey);
                if (cachedColumns == null) {
                    cachedColumns = new ArrayList<>();
                    columnsCache.put(cacheKey, cachedColumns);
                    logger.debug("Add column cache for table {}.{}", database, table);
                }
                if (!cachedColumns.contains(column)) {
                    cachedColumns.add(column);
                }
                ret.add(column);
            }
        } else {
            for (Map.Entry<String, List<String>> entry : columnsCache.asMap().entrySet()) {
                ret.addAll(entry.getValue());
            }
        }
        return ret;
    }
}
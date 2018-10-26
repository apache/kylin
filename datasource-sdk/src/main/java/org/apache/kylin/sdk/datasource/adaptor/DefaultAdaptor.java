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
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import javax.sql.rowset.CachedRowSet;

/**
 * A default implementation for <C>AbstractJdbcAdaptor</C>. By default, this adaptor supposed to support most cases.
 * Developers can just extends this class and modify some methods if found somewhere unsupported.
 */
public class DefaultAdaptor extends AbstractJdbcAdaptor {

    private final static String [] POSSIBLE_TALBE_END= {",", " ", ")", "\r", "\n", "."};

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
     * All known defects:
     * Can not support one database has two toUppercase-same tables (e.g. ACCOUNT and account table can't coexist in one database)
     * @param sql The SQL statement to be fixed.
     * @return The changed sql
     */
    @Override
    public String fixCaseSensitiveSql(String sql) {
        try {
            String orig = sql.toUpperCase(Locale.ROOT);
            List<String> databases = listDatabasesWithCache();
            String category = "";
            for (String c : databases) {
                if (orig.contains(c.toUpperCase(Locale.ROOT)+".")||orig.contains(c.toUpperCase(Locale.ROOT)+'"')) {
                    sql = sql.replaceAll(c.toUpperCase(Locale.ROOT), c);
                    category = c;
                }
            }
            List<String> tables = listTables(category);
            for (String table : tables) {
                if(checkSqlContainstable(orig, table)) {
                    sql = sql.replaceAll("(?i)" + table, table);// use (?i) to matchIgnoreCase
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return sql;
    }

    private boolean checkSqlContainstable(String orig, String table) {
        // ensure table is single match(e.g match account but not match accountant)
        if (orig.endsWith(table.toUpperCase(Locale.ROOT))) {
            return true;
        }
        for (String end:POSSIBLE_TALBE_END) {
            if (orig.contains(table.toUpperCase(Locale.ROOT) + end)){
                return true;
            }
        }
        return false;
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
                if (schema != null && !schema.isEmpty())
                    ret.add(schema);
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
                if (name != null && !name.isEmpty())
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
    public String[] buildSqlToCreateTable(String tableIdentity, LinkedHashMap<String, String> columnInfo) {
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
}
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

package org.apache.kylin.job.util;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.calcite.sql.SqlDialect;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.SourceDialect;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

import static org.apache.calcite.sql.SqlDialect.EMPTY_CONTEXT;

public class FlatTableSqlQuoteUtils {

    private static KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
    private static final Map<String, SqlDialect> sqlDialectMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    private static SqlDialect defaultDialect = null;
    public static final SqlDialect NON_QUOTE_DIALECT = new SqlDialect(EMPTY_CONTEXT);
    static final SqlDialect HIVE_DIALECT = new SqlDialect(EMPTY_CONTEXT.withIdentifierQuoteString("`"));

    static {
        sqlDialectMap.put("default", SqlDialect.CALCITE);
        sqlDialectMap.put("calcite", SqlDialect.CALCITE);
        sqlDialectMap.put("greenplum", SqlDialect.DatabaseProduct.POSTGRESQL.getDialect());
        sqlDialectMap.put("postgresql", SqlDialect.DatabaseProduct.POSTGRESQL.getDialect());
        sqlDialectMap.put("mysql", SqlDialect.DatabaseProduct.MYSQL.getDialect());
        sqlDialectMap.put("mssql", SqlDialect.DatabaseProduct.MSSQL.getDialect());
        sqlDialectMap.put("oracle", SqlDialect.DatabaseProduct.ORACLE.getDialect());
        sqlDialectMap.put("vertica", SqlDialect.DatabaseProduct.VERTICA.getDialect());
        sqlDialectMap.put("redshift", SqlDialect.DatabaseProduct.REDSHIFT.getDialect());
        sqlDialectMap.put("hive", HIVE_DIALECT);
        sqlDialectMap.put("h2", SqlDialect.DatabaseProduct.H2.getDialect());
    }

    private FlatTableSqlQuoteUtils() {
    }

    private static synchronized void setQuote() {
        if (defaultDialect != null)
            return;
        defaultDialect = sqlDialectMap.getOrDefault(kylinConfig.getFactTableDialect(), NON_QUOTE_DIALECT);
    }

    public static String quoteIdentifier(SourceDialect sourceDialect, String identifier) {
        if (!kylinConfig.enableHiveDdlQuote()) {
            return identifier;
        }
        SqlDialect specificSqlDialect = sqlDialectMap.get(sourceDialect.name());
        if (specificSqlDialect != null) {
            return specificSqlDialect.quoteIdentifier(identifier);
        }
        setQuote();
        return defaultDialect.quoteIdentifier(identifier);
    }

    /**
     * If KylinConfig#enableHiveDdlQuote return false, disable quote.
     * If SqlDialect is specific, use it; else use the KylinConfig#getFactTableDialect to quote identifier.
     */
    public static String quoteIdentifier(String identifier, SqlDialect specificSqlDialect) {
        if (!kylinConfig.enableHiveDdlQuote()) {
            return identifier;
        }
        if (specificSqlDialect != null) {
            return specificSqlDialect.quoteIdentifier(identifier);
        }
        setQuote();
        return defaultDialect.quoteIdentifier(identifier);
    }

    public static String quoteTableIdentity(TableRef tableRef, SqlDialect specificSqlDialect) {
        return quoteTableIdentity(tableRef.getTableDesc().getDatabase(), tableRef.getTableName(), specificSqlDialect);
    }

    public static String quoteTableIdentity(String database, String table, SqlDialect specificSqlDialect) {
        String dbName = quoteIdentifier(database, specificSqlDialect);
        String tableName = quoteIdentifier(table, specificSqlDialect);
        return String.format(Locale.ROOT, "%s.%s", dbName, tableName).toUpperCase(Locale.ROOT);
    }

    /**
     * Used for quote identifiers in Sql Filter Expression & Computed Column Expression for flat table
     */
    public static String quoteIdentifierInSqlExpr(IJoinedFlatTableDesc flatDesc, String sqlExpr, SqlDialect sqlDialect) {
        setQuote();
        Map<String, String> tabToAliasMap = buildTableToTableAliasMap(flatDesc);
        Map<String, Map<String, String>> tabToColsMap = buildTableToColumnsMap(flatDesc);

        boolean tableMatched = false;
        for (String table : tabToAliasMap.keySet()) {
            List<String> tabPatterns = getTableNameOrAliasPatterns(table);
            if (isIdentifierNeedToQuote(sqlExpr, table, tabPatterns)) {
                sqlExpr = quoteIdentifier(sqlExpr, table, tabPatterns, sqlDialect);
                tableMatched = true;
            }

            String tabAlias = tabToAliasMap.get(table);
            List<String> tabAliasPatterns = getTableNameOrAliasPatterns(tabAlias);
            if (isIdentifierNeedToQuote(sqlExpr, tabAlias, tabAliasPatterns)) {
                sqlExpr = quoteIdentifier(sqlExpr, tabAlias, tabAliasPatterns, sqlDialect);
                tableMatched = true;
            }

            if (tableMatched) {
                Set<String> columns = listColumnsInTable(table, tabToColsMap);
                for (String column : columns) {
                    List<String> colPatterns = getColumnNameOrAliasPatterns(column);
                    if (isIdentifierNeedToQuote(sqlExpr, column, colPatterns)) {
                        sqlExpr = quoteIdentifier(sqlExpr, column, colPatterns, sqlDialect);
                    }
                    if (columnHasAlias(table, column, tabToColsMap)) {
                        String colAlias = getColumnAlias(table, column, tabToColsMap);
                        List<String> colAliasPattern = getColumnNameOrAliasPatterns(colAlias);
                        if (isIdentifierNeedToQuote(sqlExpr, colAlias, colAliasPattern)) {
                            sqlExpr = quoteIdentifier(sqlExpr, colAlias, colPatterns, sqlDialect);
                        }
                    }
                }
            }

            tableMatched = false; //reset
        }
        return sqlExpr;
    }

    /**
     * Used to quote identifiers for JDBC ext job when quoting cc expr
     * @param tableDesc
     * @param sqlExpr
     * @return
     */
    public static String quoteIdentifierInSqlExpr(TableDesc tableDesc, String sqlExpr, SqlDialect sqlDialect) {
        String table = tableDesc.getName();
        boolean tableMatched = false;
        List<String> tabPatterns = getTableNameOrAliasPatterns(table);
        if (isIdentifierNeedToQuote(sqlExpr, table, tabPatterns)) {
            sqlExpr = quoteIdentifier(sqlExpr, table, tabPatterns, sqlDialect);
            tableMatched = true;
        }

        if (tableMatched) {
            for (ColumnDesc columnDesc : tableDesc.getColumns()) {
                String column = columnDesc.getName();
                List<String> colPatterns = getColumnNameOrAliasPatterns(column);
                if (isIdentifierNeedToQuote(sqlExpr, column, colPatterns)) {
                    sqlExpr = quoteIdentifier(sqlExpr, column, colPatterns, sqlDialect);
                }
            }
        }

        return sqlExpr;
    }

    static List<String> getTableNameOrAliasPatterns(String tableName) {
        // Pattern must contain three regex groups, and place identifier in sec group ($2)
        List<String> patterns = Lists.newArrayList();
        patterns.add("([+\\-*/%&|^=><\\s,(])(" + tableName.trim() + ")(\\.)");
        patterns.add("([\\.\\s])(" + tableName.trim() + ")([,\\s)])");
        patterns.add("(^)(" + tableName.trim() + ")([\\.])");
        return patterns;
    }

    static List<String> getColumnNameOrAliasPatterns(String colName) {
        // Pattern must contain three regex groups, and place identifier in sec group ($2)
        List<String> patterns = Lists.newArrayList();
        patterns.add("([\\.\\s(])(" + colName.trim() + ")([+\\-*/%&|^=><\\s,)])");
        patterns.add("(^)(" + colName.trim() + ")([+\\-*/%&|^=><\\s,)])");
        return patterns;
    }

    // visible for test
    static String quoteIdentifier(String sqlExpr, String identifier, List<String> identifierPatterns) {
        return quoteIdentifier(sqlExpr, identifier, identifierPatterns, null);
    }

    static String quoteIdentifier(String sqlExpr, String identifier, List<String> identifierPatterns, SqlDialect sqlDialect) {
        String quotedIdentifier = quoteIdentifier(identifier.trim(), sqlDialect);
        for (String pattern : identifierPatterns) {
            Matcher matcher = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE | Pattern.DOTALL).matcher(sqlExpr);
            if (matcher.find()) {
                sqlExpr = matcher.replaceAll("$1" + quotedIdentifier + "$3");
            }
        }
        return sqlExpr;
    }

     static boolean isIdentifierNeedToQuote(String sqlExpr, String identifier, List<String> identifierPatterns) {
        if (StringUtils.isBlank(sqlExpr) || StringUtils.isBlank(identifier)) {
            return false;
        }

        for (String pattern : identifierPatterns) {
            if (Pattern.compile(pattern, Pattern.CASE_INSENSITIVE | Pattern.DOTALL).matcher(sqlExpr).find()) {
                return true;
            }
        }
        return false;
    }

    private static Map<String, String> buildTableToTableAliasMap(IJoinedFlatTableDesc flatDesc) {
        Map<String, String> map = Maps.newHashMap();
        List<TblColRef> colRefs = flatDesc.getAllColumns();
        for (TblColRef colRef : colRefs) {
            String tableName = colRef.getTableRef().getTableName();
            String alias = colRef.getTableAlias();
            map.put(tableName, alias);
        }
        return map;
    }

    private static Map<String, Map<String, String>> buildTableToColumnsMap(IJoinedFlatTableDesc flatDesc) {
        Map<String, Map<String, String>> map = Maps.newHashMap();
        List<TblColRef> colRefs = flatDesc.getAllColumns();
        for (TblColRef colRef : colRefs) {
            String colName = colRef.getName();
            String tableName = colRef.getTableRef().getTableName();
            String colAlias = colRef.getTableAlias() + "_" + colRef.getName();
            if (map.containsKey(tableName)) {
                map.get(tableName).put(colName, colAlias);
            } else {
                Map<String, String> colToAliasMap = Maps.newHashMap();
                colToAliasMap.put(colName, colAlias);
                map.put(tableName, colToAliasMap);
            }
        }
        return map;
    }

    private static Map<String, String> getColToColAliasMapInTable(String tableName,
            Map<String, Map<String, String>> tableToColumnsMap) {
        if (tableToColumnsMap.containsKey(tableName)) {
            return tableToColumnsMap.get(tableName);
        }
        return Maps.newHashMap();
    }

    private static Set<String> listColumnsInTable(String tableName,
            Map<String, Map<String, String>> tableToColumnsMap) {
        Map<String, String> colToAliasMap = getColToColAliasMapInTable(tableName, tableToColumnsMap);
        return colToAliasMap.keySet();
    }

    private static boolean columnHasAlias(String tableName, String columnName,
            Map<String, Map<String, String>> tableToColumnsMap) {
        Map<String, String> colToAliasMap = getColToColAliasMapInTable(tableName, tableToColumnsMap);
        if (colToAliasMap.containsKey(columnName)) {
            return true;
        }
        return false;
    }

    private static String getColumnAlias(String tableName, String columnName,
            Map<String, Map<String, String>> tableToColumnsMap) {
        Map<String, String> colToAliasMap = getColToColAliasMapInTable(tableName, tableToColumnsMap);
        if (colToAliasMap.containsKey(columnName)) {
            return colToAliasMap.get(columnName);
        }
        return null;
    }
}
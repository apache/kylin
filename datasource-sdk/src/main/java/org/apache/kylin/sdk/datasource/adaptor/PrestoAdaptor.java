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
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.rowset.CachedRowSet;

public class PrestoAdaptor extends DefaultAdaptor {

    private Pattern patternASYM = Pattern.compile("BETWEEN(\\s*)ASYMMETRIC");
    private Pattern patternSYM = Pattern.compile("BETWEEN(\\s*)SYMMETRIC");
    private Pattern patternTrim = Pattern.compile("TRIM\\(.*BOTH.*FROM\\s+(.+)\\)");
    private Pattern patternOffset = Pattern.compile("(?i)OFFSET\\s\\d+");

    public PrestoAdaptor(AdaptorConfig config) throws Exception {
        super(config);
    }

    @Override
    public String fixSql(String sql) {
        sql = resolveBetweenAsymmetricSymmetric(sql);
        sql = convertTrim(sql);
        sql = convertOffset(sql);
        return sql;
    }

    @Override
    public int toKylinTypeId(String type, int typeId) {
        if (2000 == typeId) {
            return Types.DECIMAL;
        } else if (-16 == typeId) {
            return Types.VARCHAR;
        } else if (-1 == typeId) {
            return Types.VARCHAR;
        }
        return typeId;
    }

    @Override
    public String toKylinTypeName(int sourceTypeId) {
        String result = "any";
        logger.info("table schema info :" + sourceTypeId);
        switch (sourceTypeId) {
        case Types.CHAR:
            result = "char";
            break;
        case Types.VARCHAR:
            result = "varchar";
            break;
        case Types.NVARCHAR:
            result = "varchar";
            break;
        case Types.LONGVARCHAR:
            result = "varchar";
            break;
        case Types.LONGNVARCHAR:
            result = "varchar";
            break;
        case Types.NUMERIC:
            result = "decimal";
            break;
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
            result = "real";
            break;
        case Types.FLOAT:
            result = "real";
            break;
        case Types.DOUBLE:
            result = "double";
            break;
        case Types.BINARY:
            result = "VARBINARY";
            break;
        case Types.VARBINARY:
            result = "VARBINARY";
            break;
        case Types.LONGVARBINARY:
            result = "char";
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

    private String resolveBetweenAsymmetricSymmetric(String sql) {
        String sqlReturn = sql;

        Matcher matcher = patternASYM.matcher(sql);
        if (matcher.find()) {
            sqlReturn = sql.replace(matcher.group(), "BETWEEN");
        }

        matcher = patternSYM.matcher(sql);
        if (matcher.find()) {
            sqlReturn = sqlReturn.replace(matcher.group(), "BETWEEN");
        }

        return sqlReturn;
    }

    private String convertTrim(String sql) {
        String sqlReturn = sql;
        Matcher matcher = patternTrim.matcher(sql);
        boolean isFind = matcher.find();
        if (isFind) {
            String originStr = matcher.group(0);
            String fixStr = "TRIM(" + matcher.group(1) + ")";
            sqlReturn = sqlReturn.replace(originStr, fixStr);
        }
        return sqlReturn;
    }

    /**
     * Presto does not support paging
     * @param sql
     * @return
     */
    private String convertOffset(String sql) {
        String sqlReturn = sql;
        Matcher matcher = patternOffset.matcher(sqlReturn);
        while (matcher.find()) {
            String originStr = matcher.group(0);
            sqlReturn = sqlReturn.replaceFirst(originStr, " ");
        }
        return sqlReturn;
    }

    @Override
    public List<String> listTables(String schema) throws SQLException {
        List<String> ret = new ArrayList<>();
        try (Connection conn = getConnection(); ResultSet rs = conn.getMetaData().getTables(null, schema, null, null)) {
            while (rs.next()) {
                String name = rs.getString("TABLE_NAME");
                if (org.apache.commons.lang.StringUtils.isNotBlank(name)) {
                    ret.add(name);
                }
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
}

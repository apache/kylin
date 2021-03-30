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

package org.apache.kylin.common.persistence;

import java.text.FieldPosition;
import java.text.MessageFormat;
import java.util.Locale;

public class JDBCResourceSQL {

    final private JDBCSqlQueryFormat format;
    private String tableName;
    final private String metaTableKey;
    final private String metaTableTs;
    final private String metaTableContent;

    public JDBCResourceSQL(String dialect, String tableName, String metaTableKey, String metaTableTs,
            String metaTableContent) {
        this.format = JDBCSqlQueryFormatProvider.createJDBCSqlQueriesFormat(dialect);
        this.tableName = tableName;
        this.metaTableKey = metaTableKey;
        this.metaTableTs = metaTableTs;
        this.metaTableContent = metaTableContent;
    }

    public String getCheckTableExistsSql(final String tableName) {
        final String sql = new MessageFormat(format.getCheckTableExistsSql(), Locale.ROOT)
                .format(new Object[] { tableName }, new StringBuffer(), new FieldPosition(0)).toString();
        return sql;
    }

    public String getCreateIfNeededSql(String tableName) {
        final String sql = new MessageFormat(format.getCreateIfNeedSql(), Locale.ROOT)
                .format(new Object[] { tableName, metaTableKey, metaTableTs, metaTableContent }, new StringBuffer(),
                        new FieldPosition(0))
                .toString();
        return sql;
    }

    public String getCreateIndexSql(String indexName, String tableName, String indexCol) {
        final String sql = new MessageFormat(format.getCreateIndexSql(), Locale.ROOT)
                .format(new Object[] { indexName, tableName, indexCol }, new StringBuffer(), new FieldPosition(0))
                .toString();
        return sql;
    }

    public String getKeyEqualSqlString(boolean fetchContent, boolean fetchTimestamp) {
        final String sql = new MessageFormat(format.getKeyEqualsSql(), Locale.ROOT)
                .format(new Object[] { getSelectList(fetchContent, fetchTimestamp), tableName, metaTableKey },
                        new StringBuffer(), new FieldPosition(0))
                .toString();
        return sql;
    }

    public String getDeletePstatSql() {
        final String sql = new MessageFormat(format.getDeletePstatSql(), Locale.ROOT)
                .format(new Object[] { tableName, metaTableKey }, new StringBuffer(), new FieldPosition(0)).toString();
        return sql;
    }

    public String getAllResourceSqlString(boolean loadContent) {
        final String sql = new MessageFormat(format.getAllResourceSql(), Locale.ROOT).format(
                new Object[] { getSelectList(loadContent, true), tableName, metaTableKey, metaTableTs, metaTableTs },
                new StringBuffer(), new FieldPosition(0)).toString();
        return sql;
    }

    public String getReplaceSql() {
        final String sql = new MessageFormat(format.getReplaceSql(), Locale.ROOT)
                .format(new Object[] { tableName, metaTableTs, metaTableContent, metaTableKey }, new StringBuffer(),
                        new FieldPosition(0))
                .toString();
        return sql;
    }

    public String getInsertSql() {
        final String sql = new MessageFormat(format.getInsertSql(), Locale.ROOT)
                .format(new Object[] { tableName, metaTableKey, metaTableTs, metaTableContent }, new StringBuffer(),
                        new FieldPosition(0))
                .toString();
        return sql;
    }

    public String getReplaceSqlWithoutContent() {
        final String sql = new MessageFormat(format.getReplaceSqlWithoutContent(), Locale.ROOT)
                .format(new Object[] { tableName, metaTableTs, metaTableKey }, new StringBuffer(), new FieldPosition(0))
                .toString();
        return sql;
    }

    public String getInsertSqlWithoutContent() {
        final String sql = new MessageFormat(format.getInsertSqlWithoutContent(), Locale.ROOT)
                .format(new Object[] { tableName, metaTableKey, metaTableTs }, new StringBuffer(), new FieldPosition(0))
                .toString();
        return sql;
    }

    public String getUpdateContentAndTsSql() {
        final String sql = new MessageFormat(format.getUpdateContentAndTsSql(), Locale.ROOT)
                .format(new Object[] { tableName, metaTableTs, metaTableContent, metaTableKey, metaTableTs }, new StringBuffer(),
                        new FieldPosition(0))
                .toString();
        return sql;
    }

    private String getSelectList(boolean fetchContent, boolean fetchTimestamp) {
        StringBuilder sb = new StringBuilder();
        sb.append(metaTableKey);
        if (fetchTimestamp)
            sb.append("," + metaTableTs);
        if (fetchContent)
            sb.append("," + metaTableContent);
        return sb.toString();
    }

}
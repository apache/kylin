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

import java.util.Locale;
import java.util.Properties;

public class JDBCSqlQueryFormat {
    private Properties sqlQueries;

    public JDBCSqlQueryFormat(Properties props) {
        this.sqlQueries = props;
    }

    private String getSqlFromProperties(String key) {
        String sql = sqlQueries.getProperty(key);
        if (sql == null)
            throw new RuntimeException(String.format(Locale.ROOT, "Property '%s' not found", key));
        return sql;
    }

    public String getCreateIfNeedSql() {
        return getSqlFromProperties("format.sql.create-if-need");
    }

    public String getKeyEqualsSql() {
        return getSqlFromProperties("format.sql.key-equals");
    }

    public String getDeletePstatSql() {
        return getSqlFromProperties("format.sql.delete-pstat");
    }

    public String getListResourceSql() {
        return getSqlFromProperties("format.sql.list-resource");
    }

    public String getAllResourceSql() {
        return getSqlFromProperties("format.sql.all-resource");
    }

    public String getReplaceSql() {
        return getSqlFromProperties("format.sql.replace");
    }

    public String getInsertSql() {
        return getSqlFromProperties("format.sql.insert");
    }

    public String getReplaceSqlWithoutContent() {
        return getSqlFromProperties("format.sql.replace-without-content");
    }

    public String getInsertSqlWithoutContent() {
        return getSqlFromProperties("format.sql.insert-without-content");
    }

    public String getUpdateContentAndTsSql() {
        return getSqlFromProperties("format.sql.update-content-ts");
    }

    public String getTestCreateSql() {
        return getSqlFromProperties("format.sql.test.create");
    }

    public String getTestDropSql() {
        return getSqlFromProperties("format.sql.test.drop");
    }

    public String getCreateIndexSql() {
        return getSqlFromProperties("format.sql.create-index");
    }

    public String getCheckTableExistsSql() {
        return getSqlFromProperties("format.sql.check-table-exists");
    }
}

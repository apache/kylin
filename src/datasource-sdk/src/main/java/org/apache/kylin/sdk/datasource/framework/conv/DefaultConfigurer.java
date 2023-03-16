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
package org.apache.kylin.sdk.datasource.framework.conv;

import java.util.Locale;
import java.util.Map;

import org.apache.calcite.sql.SqlDialect;
import org.apache.kylin.sdk.datasource.adaptor.AbstractJdbcAdaptor;
import org.apache.kylin.sdk.datasource.framework.def.DataSourceDef;

import org.apache.kylin.guava30.shaded.common.collect.Maps;

public class DefaultConfigurer implements SqlConverter.IConfigurer {

    private static final Map<String, SqlDialect> sqlDialectMap = Maps.newHashMap();

    static {
        sqlDialectMap.put("default", SqlDialect.DatabaseProduct.CALCITE.getDialect());
        sqlDialectMap.put("calcite", SqlDialect.DatabaseProduct.CALCITE.getDialect());
        sqlDialectMap.put("greenplum", SqlDialect.DatabaseProduct.POSTGRESQL.getDialect());
        sqlDialectMap.put("postgresql", SqlDialect.DatabaseProduct.POSTGRESQL.getDialect());
        sqlDialectMap.put("mysql", SqlDialect.DatabaseProduct.MYSQL.getDialect());
        sqlDialectMap.put("mssql", SqlDialect.DatabaseProduct.MSSQL.getDialect());
        sqlDialectMap.put("oracle", SqlDialect.DatabaseProduct.ORACLE.getDialect());
        sqlDialectMap.put("vertica", SqlDialect.DatabaseProduct.VERTICA.getDialect());
        sqlDialectMap.put("redshift", SqlDialect.DatabaseProduct.REDSHIFT.getDialect());
        sqlDialectMap.put("hive", SqlDialect.DatabaseProduct.HIVE.getDialect());
        sqlDialectMap.put("h2", SqlDialect.DatabaseProduct.H2.getDialect());
        sqlDialectMap.put("snowflake", SqlDialect.DatabaseProduct.CALCITE.getDialect());
        sqlDialectMap.put("unknown", SqlDialect.DatabaseProduct.UNKNOWN.getDialect());
    }

    private AbstractJdbcAdaptor adaptor;

    private DataSourceDef dsDef;

    public DefaultConfigurer(AbstractJdbcAdaptor adaptor, DataSourceDef dsDef) {
        this.adaptor = adaptor;
        this.dsDef = dsDef;
    }

    public DefaultConfigurer(DataSourceDef dsDef) {
        this(null, dsDef);
    }

    @Override
    public boolean skipDefaultConvert() {
        return !"true".equalsIgnoreCase(dsDef.getPropertyValue("sql.default-converted-enabled", "true"));
    }

    @Override
    public boolean skipHandleDefault() {
        return !"true".equalsIgnoreCase(dsDef.getPropertyValue("sql.keyword-default-escape", "true"));
    }

    @Override
    public boolean useUppercaseDefault() {
        return "true".equalsIgnoreCase(dsDef.getPropertyValue("sql.keyword-default-uppercase", "true"));
    }

    @Override
    public String fixAfterDefaultConvert(String orig) {
        if (this.adaptor == null) {
            return orig;
        }
        return adaptor.fixSql(orig);
    }

    @Override
    public SqlDialect getSqlDialect() {
        String dialectName = dsDef.getDialectName() == null ? dsDef.getId() : dsDef.getDialectName();
        SqlDialect sqlDialect = sqlDialectMap.get(dialectName.toLowerCase(Locale.ROOT));
        return sqlDialect == null ? sqlDialectMap.get("unknown") : sqlDialect;
    }

    @Override
    public boolean allowNoOffset() {
        return "true".equalsIgnoreCase(dsDef.getPropertyValue("sql.allow-no-offset", "true"));
    }

    @Override
    public boolean allowFetchNoRows() {
        return "true".equalsIgnoreCase(dsDef.getPropertyValue("sql.allow-fetch-no-rows", "true"));
    }

    @Override
    public boolean allowNoOrderByWithFetch() {
        return "true".equalsIgnoreCase(dsDef.getPropertyValue("sql.allow-no-orderby-with-fetch", "true"));
    }

    @Override
    public String getPagingType() {
        return dsDef.getPropertyValue("sql.paging-type", "AUTO");
    }

    @Override
    public boolean isCaseSensitive() {
        return "true".equalsIgnoreCase(dsDef.getPropertyValue("sql.case-sensitive", "false"));
    }

    @Override
    public boolean enableCache() {
        return "true".equalsIgnoreCase(dsDef.getPropertyValue("metadata.enable-cache", "false"));
    }

    @Override
    public boolean enableQuote() {
        return "true".equalsIgnoreCase(dsDef.getPropertyValue("sql.enable-quote-all-identifiers", "false"));
    }

    @Override
    public String fixIdentifierCaseSensitive(String orig) {
        if (this.adaptor == null || !isCaseSensitive()) {
            return orig;
        }
        return adaptor.fixIdentifierCaseSensitive(orig);
    }

    @Override
    public boolean enableTransformDateToString() {
        return "true".equalsIgnoreCase(dsDef.getPropertyValue("sql.sqoop.enable-transform-date-to-string", "false"));
    }

    @Override
    public String getTransformDateToStringExpression() {
        return dsDef.getPropertyValue("sql.sqoop.transform-date-to-string-expression", "");
    }

    @Override
    public String getTransformDatePattern() {
        return dsDef.getPropertyValue("sql.sqoop.transform-date-pattern", "");
    }
}

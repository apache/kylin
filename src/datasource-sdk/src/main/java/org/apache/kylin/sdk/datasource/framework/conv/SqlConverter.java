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

import java.sql.SQLException;
import java.util.Locale;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.metadata.datatype.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlConverter {
    private static final Logger logger = LoggerFactory.getLogger(SqlConverter.class);

    private final IConfigurer configurer;
    private final SqlNodeConverter sqlNodeConverter;
    private final SingleSqlNodeReplacer singleSqlNodeReplacer;

    public SqlConverter(IConfigurer configurer, ConvMaster convMaster) throws SQLException {
        this.sqlNodeConverter = new SqlNodeConverter(convMaster);
        this.configurer = configurer;
        this.singleSqlNodeReplacer = new SingleSqlNodeReplacer(convMaster);
    }

    public String convertSql(String orig) {
        String converted = orig;

        if (!configurer.skipHandleDefault()) {
            String escapedDefault = SqlDialect.DatabaseProduct.CALCITE.getDialect()
                    .quoteIdentifier(configurer.useUppercaseDefault() ? "DEFAULT" : "default");
            converted = converted.replaceAll("(?i)default\\.", escapedDefault + "."); // use Calcite dialect to cater to SqlParser
            converted = converted.replaceAll("\"(?i)default\"\\.", escapedDefault + ".");
        }

        if (!configurer.skipDefaultConvert()) {
            ConvSqlWriter sqlWriter = null;
            String beforeConvert = converted;
            try {
                // calcite cannot recognize `, convert ` to " before parse
                converted = converted.replace("`", "\"");
                sqlWriter = getConvSqlWriter();
                SqlNode sqlNode = SqlParser.create(converted).parseQuery();
                sqlNode = sqlNode.accept(sqlNodeConverter);
                converted = sqlWriter.format(sqlNode);
            } catch (Exception e) {
                logger.error("Failed to default convert sql, will use the origin input: {}", beforeConvert, e);
                // revert to beforeConvert when occur Exception
                converted = beforeConvert;
            } finally {
                if (sqlWriter != null) {
                    sqlWriter.reset();
                }
            }
        }

        converted = configurer.fixAfterDefaultConvert(converted);
        return converted;
    }

    public String convertColumn(String column, String originQuote) {
        String converted = column;
        if (StringUtils.isNotEmpty(originQuote)) {
            converted = column.replace(originQuote, "\"");
        }
        ConvSqlWriter sqlWriter = null;
        try {
            sqlWriter = getConvSqlWriter();
            SqlNode sqlNode = SqlParser.create(converted).parseExpression();
            sqlNode = sqlNode.accept(sqlNodeConverter);
            converted = sqlWriter.format(sqlNode);
            converted = configurer.fixAfterDefaultConvert(converted);
        } catch (Throwable e) {
            logger.error("Failed to default convert Column, will use the input: {}", column, e);
        } finally {
            if (sqlWriter != null) {
                sqlWriter.reset();
            }
        }
        return converted;
    }

    /**
    * for oracle,  oracle does not support convert date to varchar implicitly
    * @param column
    * @param colType
    * @param format
    * @return 
    */
    public String formatDateColumn(String column, DataType colType, String format) {
        String formated = column;
        if (configurer.enableTransformDateToString() && colType != null && colType.isDateTimeFamily()) {
            String datePattern = StringUtils.isNotEmpty(format) ? format : configurer.getTransformDatePattern();
            String template = configurer.getTransformDateToStringExpression();
            formated = String.format(Locale.ROOT, template, column, datePattern);
        }
        return convertColumn(formated, "");
    }

    /**
    * for oracle,  oracle does not support convert date to varchar implicitly
     * ORA-01861: literal does not match format string
     *
     * format date column in where clause
     *
    * @param orig
    * @param partColumn
    * @param partColType
    * @param partColFormat
    * @return 
    */
    public String convertDateCondition(String orig, String partColumn, DataType partColType, String partColFormat) {
        // for jdbc source, convert quote from backtick to double quote
        String converted = orig.replaceAll("`", "\"");

        if (configurer.enableTransformDateToString() && partColType != null && partColType.isDateTimeFamily()) {
            SqlNode sqlNode;
            ConvSqlWriter sqlWhereWriter = null;
            try {
                sqlNode = SqlParser.create(converted).parseQuery();
                if (sqlNode instanceof SqlSelect) {
                    SqlNode sqlNodeWhere = ((SqlSelect) sqlNode).getWhere();
                    if (sqlNodeWhere != null) {
                        ((SqlSelect) sqlNode).setWhere(null);
                        sqlWhereWriter = getConvSqlWriter();
                        String strSelect = sqlWhereWriter.format(sqlNode);
                        StringBuilder sb = new StringBuilder(strSelect);
                        sqlWhereWriter.reset();

                        String datePattern = StringUtils.isNotEmpty(partColFormat) ? partColFormat
                                : configurer.getTransformDatePattern();
                        String template = configurer.getTransformDateToStringExpression();
                        if (StringUtils.isNotEmpty(template)) {
                            SqlNode sqlNodeTryToFind = SqlParser.create(partColumn).parseExpression();
                            SqlNode sqlNodeToReplace = SqlParser
                                    .create(String.format(Locale.ROOT, template, partColumn, datePattern))
                                    .parseExpression();
                            singleSqlNodeReplacer.setSqlNodeTryToFind(sqlNodeTryToFind);
                            singleSqlNodeReplacer.setSqlNodeToReplace(sqlNodeToReplace);
                            sqlNodeWhere = sqlNodeWhere.accept(singleSqlNodeReplacer);
                        }
                        String sqlWhere = sqlWhereWriter.format(sqlNodeWhere);
                        sb.append(" WHERE ").append(sqlWhere);

                        return sb.toString();
                    }
                }
            } catch (Throwable e) {
                logger.error("Failed to default convert date condition for sqoop, will use the input: {}", orig, e);
            } finally {
                if (sqlWhereWriter != null) {
                    sqlWhereWriter.reset();
                }
            }
        }
        return converted;
    }

    public IConfigurer getConfigurer() {
        return configurer;
    }

    public interface IConfigurer {
        default boolean skipDefaultConvert() {
            return false;
        }

        default boolean skipHandleDefault() {
            return false;
        }

        default boolean useUppercaseDefault() {
            return false;
        }

        default String fixAfterDefaultConvert(String orig) {
            return orig;
        }

        default SqlDialect getSqlDialect() {
            return CalciteSqlDialect.DEFAULT;
        }

        default boolean allowNoOffset() {
            return false;
        }

        default boolean allowFetchNoRows() {
            return false;
        }

        default boolean allowNoOrderByWithFetch() {
            return false;
        }

        default String getPagingType() {
            return "AUTO";
        }

        default boolean isCaseSensitive() {
            return false;
        }

        default boolean enableCache() {
            return false;
        }

        default boolean enableQuote() {
            return false;
        }

        default String fixIdentifierCaseSensitive(String orig) {
            return orig;
        }

        default boolean enableTransformDateToString() {
            return false;
        }

        default String getTransformDateToStringExpression() {
            return "";
        }

        default String getTransformDatePattern() {
            return "";
        }
    }

    private ConvSqlWriter getConvSqlWriter() throws SQLException {
        ConvSqlWriter sqlWriter;
        if ("ROWNUM".equalsIgnoreCase(configurer.getPagingType())) {
            sqlWriter = new ConvRownumSqlWriter(configurer);
        } else {
            sqlWriter = new ConvSqlWriter(configurer);
        }
        sqlWriter.setQuoteAllIdentifiers(false);
        return sqlWriter;
    }
}

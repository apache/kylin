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

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlConverter {
    private static final Logger logger = LoggerFactory.getLogger(SqlConverter.class);

    private final ConvSqlWriter sqlWriter;
    private final IConfigurer configurer;
    private final SqlNodeConverter sqlNodeConverter;

    public SqlConverter(IConfigurer configurer, ConvMaster convMaster) throws SQLException {
        this.sqlNodeConverter = new SqlNodeConverter(convMaster);
        this.configurer = configurer;

        sqlWriter = new ConvSqlWriter(configurer);
        sqlWriter.setQuoteAllIdentifiers(false);
    }

    public String convertSql(String orig) {
        // for jdbc source, convert quote from backtick to double quote
        String converted = orig.replaceAll("`", "\"");

        if (!configurer.skipHandleDefault()) {
            String escapedDefault = SqlDialect.CALCITE
                    .quoteIdentifier(configurer.useUppercaseDefault() ? "DEFAULT" : "default");
            converted = converted.replaceAll("(?i)default\\.", escapedDefault + "."); // use Calcite dialect to cater to SqlParser
            converted = converted.replaceAll("\"(?i)default\"\\.", escapedDefault + ".");
        }

        if (!configurer.skipDefaultConvert()) {
            try {
                SqlNode sqlNode = SqlParser.create(converted).parseQuery();
                sqlNode = sqlNode.accept(sqlNodeConverter);
                converted = sqlWriter.format(sqlNode);
            } catch (Throwable e) {
                logger.error("Failed to default convert sql, will use the input: {}", orig, e);
            } finally {
                sqlWriter.reset();
            }
        }
        converted = configurer.fixAfterDefaultConvert(converted);
        return converted;
    }

    public String convertColumn(String column, String originQuote) {
        String converted = column.replace(originQuote, "");
        try {
            SqlNode sqlNode = SqlParser.create(converted).parseExpression();
            sqlNode = sqlNode.accept(sqlNodeConverter);
            converted = sqlWriter.format(sqlNode);
        } catch (Throwable e) {
            logger.error("Failed to default convert Column, will use the input: {}", column, e);
        } finally {
            sqlWriter.reset();
        }
        return converted;
    }

    public IConfigurer getConfigurer() {
        return configurer;
    }

    public interface IConfigurer {
        boolean skipDefaultConvert();

        boolean skipHandleDefault();

        boolean useUppercaseDefault();

        String fixAfterDefaultConvert(String orig);

        SqlDialect getSqlDialect();

        boolean allowNoOffset();

        boolean allowFetchNoRows();

        boolean allowNoOrderByWithFetch();

        String getPagingType();

        boolean isCaseSensitive();

        boolean enableCache();

        boolean enableQuote();

        String fixIdentifierCaseSensitve(String orig);

        /**
         * Only support following 3 types
         * TRANSACTION_READ_COMMITTED，TRANSACTION_READ_UNCOMMITTED，TRANSACTION_READ_COMMITTED
         */
        String getTransactionIsolationLevel();
    }
}

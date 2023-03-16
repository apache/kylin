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
package org.apache.kylin.source.adhocquery;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.tool.CalciteParser;

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;

import lombok.extern.slf4j.Slf4j;

/**
 * <pre>
 * for example:
 *
 *     select ACCOUNT_ID as id, ACCOUNT_COUNTRY as "country" from "DEFAULT".TEST_ACCOUNT
 *
 * will be converted to:
 *
 *      select "ACCOUNT_ID" as "ID", "ACCOUNT_COUNTRY" as "country" from "DEFAULT"."TEST_ACCOUNT"
 * </pre>
 *
 * <P>if unquoted,quote all SqlIdentifier with {@link org.apache.calcite.avatica.util.Quoting#DOUBLE_QUOTE}
 * <P>if already quoted, unchanged
 * </P>and visit SqlIdentifier with
 * {@link DoubleQuoteSqlIdentifierConvert}
 */
@Slf4j
public class DoubleQuotePushDownConverter implements IPushDownConverter {

    //inner class for convert SqlIdentifier with DoubleQuote
    private static class DoubleQuoteSqlIdentifierConvert {

        private final String sql;

        private final String project;

        public DoubleQuoteSqlIdentifierConvert(String sql, String project) {
            this.sql = sql;
            this.project = project;
        }

        private SqlNode parse() throws SqlParseException {
            return CalciteParser.parse(this.sql, this.project);
        }

        private Collection<SqlIdentifier> getAllSqlIdentifiers() throws SqlParseException {
            Set<SqlIdentifier> allSqlIdentifier = Sets.newHashSet();
            SqlVisitor<Void> sqlVisitor = new SqlBasicVisitor<Void>() {
                @Override
                public Void visit(SqlIdentifier id) {
                    if (!isFunctionWithoutParentheses(id)) {
                        allSqlIdentifier.add(id);
                    }
                    return null;
                }
            };
            parse().accept(sqlVisitor);

            return allSqlIdentifier;
        }

        public String convert() throws SqlParseException {
            final StringBuilder sqlConvertedStringBuilder = new StringBuilder(sql);
            List<SqlIdentifier> sqlIdentifierList = Lists.newArrayList(getAllSqlIdentifiers());
            CalciteParser.descSortByPosition(sqlIdentifierList);
            sqlIdentifierList.forEach(sqlIdentifier -> {
                Pair<Integer, Integer> replacePos = CalciteParser.getReplacePos(sqlIdentifier, sql);
                //* 'name is empty
                List<String> toStarNames = SqlIdentifier.toStar(sqlIdentifier.names);
                String newIdentifierStr = toStarNames.stream().map(this::convertIdentifier)
                        .collect(Collectors.joining("."));
                sqlConvertedStringBuilder.replace(replacePos.getFirst(), replacePos.getSecond(), newIdentifierStr);
            });
            return sqlConvertedStringBuilder.toString();
        }

        private String convertIdentifier(String identifierStr) {
            if (identifierStr.equals("*")) {
                return identifierStr;
            } else {
                return Quoting.DOUBLE_QUOTE.string + identifierStr + Quoting.DOUBLE_QUOTE.string;
            }

        }

        /**
         * filter the function without parentheses
         * {@link org.apache.calcite.sql.SqlUtil#makeCall(SqlOperatorTable, SqlIdentifier)}
         * @param id
         * @return
         */
        private boolean isFunctionWithoutParentheses(SqlIdentifier id) {
            SqlOperatorTable opTab = SqlStdOperatorTable.instance();
            return Objects.nonNull(SqlUtil.makeCall(opTab, id));
        }

    }

    //End DoubleQuoteSqlIdentifierConvert.class
    @Override
    public String convert(String originSql, String project, String defaultSchema) {

        return convertDoubleQuote(originSql, project);
    }

    public static String convertDoubleQuote(String originSql) {
        return convertDoubleQuote(originSql, null);
    }

    public static String convertDoubleQuote(String originSql, String project) {
        String sqlParsed = originSql;

        try {
            DoubleQuoteSqlIdentifierConvert sqlIdentifierConvert = new DoubleQuoteSqlIdentifierConvert(originSql,
                    project);
            sqlParsed = sqlIdentifierConvert.convert();
        } catch (Exception e) {
            log.warn("convert sql:{} with double quoted with exception", originSql, e);
        }
        return sqlParsed;
    }
}

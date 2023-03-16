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

package org.apache.kylin.metadata.model.tool;

import static org.apache.calcite.sql.SqlDialect.EMPTY_CONTEXT;

import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.dialect.HiveSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ExpModifier;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.ParseException;
import org.apache.kylin.metadata.project.NProjectManager;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableSet;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;

import org.apache.kylin.guava30.shaded.common.cache.Cache;
import org.apache.kylin.guava30.shaded.common.cache.CacheBuilder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CalciteParser {

    /**
     * Overwrite {@link HiveSqlDialect#DEFAULT} with backtick quote. 
     */
    public static final HiveSqlDialect HIVE_SQL_DIALECT = new HiveSqlDialect(
            EMPTY_CONTEXT.withDatabaseProduct(SqlDialect.DatabaseProduct.HIVE) //
                    .withNullCollation(NullCollation.LOW) //
                    .withIdentifierQuoteString(Quoting.BACK_TICK.string));

    private CalciteParser() {
    }

    private static final String SQL_PREFIX = "select ";
    private static final String SQL_SUFFIX = " from t";
    private static final String QUOTE = Quoting.DOUBLE_QUOTE.string;
    private static final ImmutableSet<String> LATENT_NILADIC_SET = ImmutableSet.of("current_", "pi");

    private static final Cache<String, SqlNode> expCache = CacheBuilder.newBuilder().maximumSize(10000)
            .expireAfterWrite(10, TimeUnit.MINUTES).build();

    public static SqlNode parse(String sql) throws SqlParseException {
        return parse(sql, null);
    }

    public static SqlNode parse(String sql, String project) throws SqlParseException {
        KylinConfig kylinConfig = StringUtils.isNotEmpty(project) //
                ? NProjectManager.getProjectConfig(project) //
                : KylinConfig.getInstanceFromEnv();
        SqlParser.ConfigBuilder parserBuilder = SqlParser.configBuilder()
                .setIdentifierMaxLength(kylinConfig.getMaxModelDimensionMeasureNameLength());
        if (kylinConfig.getSourceNameCaseSensitiveEnabled()) {
            parserBuilder.setCaseSensitive(false).setUnquotedCasing(Casing.UNCHANGED);
        }

        // todo: best way to solve this problem is to use BACK_TICK all the time, in CalciteParser.
        try {
            SqlParser sqlParser = SqlParser.create(sql, parserBuilder.setQuoting(Quoting.DOUBLE_QUOTE).build());
            return sqlParser.parseQuery();
        } catch (Exception e) {
            log.info("The SqlIdentifier is not DOUBLE_QUOTE, try with BACK_TICK");
            SqlParser sqlParser = SqlParser.create(sql, parserBuilder.setQuoting(Quoting.BACK_TICK).build());
            return sqlParser.parseQuery();
        }

    }

    public static SqlNode getSelectNode(String sql) {
        try {
            String normalizedSql = normalize(sql);
            return ((SqlSelect) CalciteParser.parse(normalizedSql)).getSelectList();
        } catch (SqlParseException e) {
            throw new IllegalStateException(
                    "Failed to parse expression '" + sql + "', please make sure the expression is valid", e);
        }
    }

    public static SqlNode getOnlySelectNode(String sql) {
        SqlNodeList selectList = (SqlNodeList) getSelectNode(sql);
        Preconditions.checkArgument(selectList.size() == 1,
                "Expression is invalid because size of select list exceeds one");
        return selectList.get(0);
    }

    public static SqlNode getReadonlyExpNode(String expr) {
        SqlNode sqlNode = expCache.getIfPresent(expr);
        if (sqlNode == null) {
            String preHandledExp = normalize(expr);
            sqlNode = getExpNode(preHandledExp);
            expCache.put(expr, sqlNode);
        }
        return sqlNode;
    }

    private static boolean needNormalize(String expr) {
        for (String fun : LATENT_NILADIC_SET) {
            if (StringUtils.containsIgnoreCase(expr, fun)) {
                return true;
            }
        }
        return false;
    }

    /**
     * At present, normalize these functions:
     * current_date  => current_date()
     * current_time  => current_time()
     * current_timestamp  => current_timestamp()
     */
    public static String normalize(String expression) {
        if (!needNormalize(expression)) {
            return expression;
        }
        String transformedExp = expression;
        try {
            transformedExp = new ExpModifier(expression).transform();
        } catch (ParseException e) {
            log.warn("Origin expression returned for handling exception: {}", expression, e);
        }
        return transformedExp;
    }

    public static SqlNode getExpNode(String expr) {
        return getOnlySelectNode(SQL_PREFIX + expr + SQL_SUFFIX);
    }

    public static String getLastNthName(SqlIdentifier id, int n) {
        //n = 1 is getting column
        //n = 2 is getting table's alias, if it has.
        //n = 3 is getting database name, if it has.
        return id.names.get(id.names.size() - n).replace("\"", "").toUpperCase(Locale.ROOT);
    }

    /**
     * Insert alias, the correctness is controlled by the user.
     * For example: given expression a + b, alias `x`, the result is x.a + x.b
     */
    public static String insertAliasInExpr(String expr, String alias) {
        String sql = SQL_PREFIX + expr + SQL_SUFFIX;
        SqlNode sqlNode = getOnlySelectNode(sql);

        final Set<SqlIdentifier> identifiers = Sets.newHashSet();
        SqlVisitor<Object> sqlVisitor = new SqlBasicVisitor<Object>() {
            @Override
            public Object visit(SqlIdentifier id) {
                Preconditions.checkArgument(id.names.size() == 1, "SqlIdentifier %s contains DB/Table name", id);
                identifiers.add(id);
                return null;
            }
        };

        sqlNode.accept(sqlVisitor);
        List<SqlIdentifier> sqlIdentifiers = Lists.newArrayList(identifiers);

        descSortByPosition(sqlIdentifiers);

        for (SqlIdentifier sqlIdentifier : sqlIdentifiers) {
            Pair<Integer, Integer> replacePos = getReplacePos(sqlIdentifier, sql);
            int start = replacePos.getFirst();
            sql = sql.substring(0, start) + alias + "." + sql.substring(start);
        }

        return sql.substring(SQL_PREFIX.length(), sql.length() - SQL_SUFFIX.length());
    }

    public static void descSortByPosition(List<? extends SqlNode> sqlNodes) {
        sqlNodes.sort((Comparator<SqlNode>) (o1, o2) -> {
            int linegap = o2.getParserPosition().getLineNum() - o1.getParserPosition().getLineNum();
            if (linegap != 0)
                return linegap;

            return o2.getParserPosition().getColumnNum() - o1.getParserPosition().getColumnNum();
        });
    }

    public static Pair<Integer, Integer> getReplacePos(SqlNode node, String inputSql) {
        if (inputSql == null) {
            return Pair.newPair(0, 0);
        }
        String[] lines = inputSql.split("\n");
        SqlParserPos pos = node.getParserPosition();
        int lineStart = pos.getLineNum();
        int lineEnd = pos.getEndLineNum();
        int columnStart = pos.getColumnNum() - 1;
        int columnEnd = pos.getEndColumnNum();
        //for the case that sql is multi lines
        for (int i = 0; i < lineStart - 1; i++) {
            columnStart += lines[i].length() + 1;
        }
        for (int i = 0; i < lineEnd - 1; i++) {
            columnEnd += lines[i].length() + 1;
        }
        //for calcite's bug CALCITE-1875
        return getPosWithBracketsCompletion(inputSql, columnStart, columnEnd);
    }

    private static Pair<Integer, Integer> getPosWithBracketsCompletion(String inputSql, int left, int right) {
        int leftBracketNum = 0;
        int rightBracketNum = 0;
        boolean constantFlag = false;
        boolean inQuotes = false;
        String substring = inputSql.substring(left, right);
        for (int i = 0; i < substring.length(); i++) {
            char temp = substring.charAt(i);
            if (temp == '\"' && !constantFlag) {
                inQuotes = !inQuotes;
            }
            if (temp == '\'') {
                constantFlag = !constantFlag;
            }
            if (inQuotes || constantFlag) {
                continue;
            }
            if (temp == '(') {
                leftBracketNum++;
            }
            if (temp == ')') {
                rightBracketNum++;
                if (leftBracketNum < rightBracketNum) {
                    left = moveLeft(inputSql, left);
                    leftBracketNum++;
                }
            }
        }
        while (rightBracketNum < leftBracketNum) {
            right = moveRight(inputSql, right);
            rightBracketNum++;
        }
        return Pair.newPair(left, right);
    }

    private static int moveRight(String inputSql, int right) {
        while (')' != inputSql.charAt(right)) {
            right++;
        }
        right++;
        return right;
    }

    private static int moveLeft(String inputSql, int left) {
        while ('(' != inputSql.charAt(left - 1)) {
            left--;
        }
        left--;
        return left;
    }

    public static String replaceAliasInExpr(String expr, Map<String, String> renaming) {
        String sql = SQL_PREFIX + expr + SQL_SUFFIX;
        SqlNode sqlNode = CalciteParser.getOnlySelectNode(sql);

        final Set<SqlIdentifier> s = Sets.newHashSet();
        SqlVisitor<Object> sqlVisitor = new SqlBasicVisitor<Object>() {
            @Override
            public Object visit(SqlIdentifier id) {
                Preconditions.checkState(id.names.size() == 2);
                s.add(id);
                return null;
            }
        };

        sqlNode.accept(sqlVisitor);
        List<SqlIdentifier> sqlIdentifiers = Lists.newArrayList(s);

        CalciteParser.descSortByPosition(sqlIdentifiers);

        for (SqlIdentifier sqlIdentifier : sqlIdentifiers) {
            Pair<Integer, Integer> replacePos = CalciteParser.getReplacePos(sqlIdentifier, sql);
            int start = replacePos.getFirst();
            int end = replacePos.getSecond();
            String aliasInExpr = sqlIdentifier.names.get(0);
            String col = sqlIdentifier.names.get(1);
            String renamedAlias = renaming.get(aliasInExpr);
            Preconditions.checkNotNull(renamedAlias, "rename for alias {} in expr ({}) is not found", aliasInExpr,
                    expr);
            sql = sql.substring(0, start) + normedIdentifier(new Pair<>(renamedAlias, col)) + sql.substring(end);
        }

        return sql.substring(SQL_PREFIX.length(), sql.length() - SQL_SUFFIX.length());
    }

    private static String normedIdentifier(Pair<String, String> pair) {
        return QUOTE + pair.getKey() + QUOTE + "." + QUOTE + pair.getValue() + QUOTE;
    }

    public static String transformDoubleQuote(String expr) throws SqlParseException {
        SqlParser.ConfigBuilder parserBuilder = SqlParser.configBuilder().setQuoting(Quoting.BACK_TICK);
        SqlParser sqlParser = SqlParser.create(SQL_PREFIX + expr, parserBuilder.build());
        SqlSelect select = (SqlSelect) sqlParser.parseQuery();
        return select.getSelectList().getList().get(0)
                .toSqlString(new SqlDialect(EMPTY_CONTEXT.withIdentifierQuoteString(QUOTE))).toString();
    }

    public static Set<String> getUsedAliasSet(String expr) {
        if (expr == null) {
            return Sets.newHashSet();
        }
        SqlNode sqlNode = CalciteParser.getReadonlyExpNode(expr);

        final Set<String> s = Sets.newHashSet();
        SqlVisitor<Object> sqlVisitor = new SqlBasicVisitor<Object>() {
            @Override
            public Object visit(SqlIdentifier id) {
                Preconditions.checkState(id.names.size() == 2);
                s.add(id.names.get(0));
                return null;
            }
        };

        sqlNode.accept(sqlVisitor);
        return s;
    }
}

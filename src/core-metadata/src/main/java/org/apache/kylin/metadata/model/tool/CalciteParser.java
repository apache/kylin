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
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.project.NProjectManager;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.guava20.shaded.common.cache.Cache;
import io.kyligence.kap.guava20.shaded.common.cache.CacheBuilder;

public class CalciteParser {

    private CalciteParser() {
    }

    private static final String SQL_PREFIX = "select ";
    private static final String SQL_SUFFIX = " from t";
    private static final String QUOTE = Quoting.DOUBLE_QUOTE.string;

    private static final Cache<String, SqlNode> expCache = CacheBuilder.newBuilder().maximumSize(10000)
            .expireAfterWrite(10, TimeUnit.MINUTES).build();

    public static SqlNode parse(String sql) throws SqlParseException {
        return parse(sql, null);
    }

    public static SqlNode parse(String sql, String project) throws SqlParseException {
        KylinConfig kylinConfig = StringUtils.isNotEmpty(project)
                ? NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(project).getConfig()
                : KylinConfig.getInstanceFromEnv();
        SqlParser.ConfigBuilder parserBuilder = SqlParser.configBuilder()
                .setIdentifierMaxLength(kylinConfig.getMaxModelDimensionMeasureNameLength());
        if (kylinConfig.getSourceNameCaseSensitiveEnabled()) {
            parserBuilder.setCaseSensitive(false).setUnquotedCasing(Casing.UNCHANGED);
        }
        SqlParser sqlParser = SqlParser.create(sql, parserBuilder.build());
        return sqlParser.parseQuery();
    }

    public static SqlNode getSelectNode(String sql) {
        SqlNodeList selectList = null;
        try {
            selectList = ((SqlSelect) CalciteParser.parse(sql)).getSelectList();
        } catch (SqlParseException e) {
            throw new IllegalStateException(
                    "Failed to parse expression \'" + sql + "\', please make sure the expression is valid", e);
        }
        return selectList;
    }

    public static SqlNode getOnlySelectNode(String sql) {
        SqlNodeList selectList = null;
        selectList = (SqlNodeList) getSelectNode(sql);
        Preconditions.checkArgument(selectList.size() == 1,
                "Expression is invalid because size of select list exceeds one");
        return selectList.get(0);
    }

    public static SqlNode getReadonlyExpNode(String expr) {
        SqlNode sqlNode = expCache.getIfPresent(expr);
        if (sqlNode == null) {
            sqlNode = getExpNode(expr);
            expCache.put(expr, sqlNode);
        }
        return sqlNode;
    }

    public static SqlNode getExpNode(String expr) {
        return getOnlySelectNode(SQL_PREFIX + expr + SQL_SUFFIX);
    }

    public static String getLastNthName(SqlIdentifier id, int n) {
        //n = 1 is getting column
        //n = 2 is getting table's alias, if has.
        //n = 3 is getting database name, if has.
        return id.names.get(id.names.size() - n).replace("\"", "").toUpperCase(Locale.ROOT);
    }

    public static void ensureNoAliasInExpr(String expr) {
        SqlNode sqlNode = getExpNode(expr);

        SqlVisitor sqlVisitor = new SqlBasicVisitor() {
            @Override
            public Object visit(SqlIdentifier id) {
                if (id.names.size() > 1) {
                    throw new IllegalArgumentException(
                            "Column Identifier in the computed column expression should only contain COLUMN");
                }
                return null;
            }
        };

        sqlNode.accept(sqlVisitor);
    }

    public static boolean hasAliasInExpr(String expr) {
        try {
            ensureNoAliasInExpr(expr);
            return false;
        } catch (Exception e) {
            return true;
        }
    }

    public static String insertAliasInExpr(String expr, String alias) {
        String sql = SQL_PREFIX + expr + SQL_SUFFIX;
        SqlNode sqlNode = getOnlySelectNode(sql);

        final Set<SqlIdentifier> s = Sets.newHashSet();
        SqlVisitor sqlVisitor = new SqlBasicVisitor() {
            @Override
            public Object visit(SqlIdentifier id) {
                if (id.names.size() > 1) {
                    throw new IllegalArgumentException("SqlIdentifier " + id + " contains DB/Table name");
                }
                s.add(id);
                return null;
            }
        };

        sqlNode.accept(sqlVisitor);
        List<SqlIdentifier> sqlIdentifiers = Lists.newArrayList(s);

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
        SqlVisitor sqlVisitor = new SqlBasicVisitor() {
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
            sql = sql.substring(0, start) //
                    + quote(renamedAlias) + "." + quote(col) //
                    + sql.substring(end);
        }

        return sql.substring(SQL_PREFIX.length(), sql.length() - SQL_SUFFIX.length());
    }

    private static String quote(String key) {
        return QUOTE + key + QUOTE;
    }

    public static String transformDoubleQuote(String expr) throws SqlParseException {
        SqlParser.ConfigBuilder parserBuilder = SqlParser.configBuilder().setQuoting(Quoting.BACK_TICK);
        SqlParser sqlParser = SqlParser.create(SQL_PREFIX + expr, parserBuilder.build());
        SqlSelect select = (SqlSelect) sqlParser.parseQuery();
        return select.getSelectList().getList().get(0)
                .toSqlString(new SqlDialect(EMPTY_CONTEXT.withIdentifierQuoteString("\""))).toString();

    }

}

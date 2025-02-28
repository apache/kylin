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

package org.apache.kylin.query.util;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.StringHelper;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableSet;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.query.BigQueryThresholdUpdater;
import org.apache.kylin.query.IQueryTransformer;
import org.apache.kylin.query.security.AccessDeniedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryUtil {

    private static final Logger log = LoggerFactory.getLogger("query");

    public static final ImmutableSet<String> REMOVED_TRANSFORMERS = ImmutableSet.of("ReplaceStringWithVarchar");
    private static final Pattern SELECT_PATTERN = Pattern.compile("^select", Pattern.CASE_INSENSITIVE);
    private static final Pattern SELECT_STAR_PTN = Pattern.compile("^select\\s+\\*\\p{all}*", Pattern.CASE_INSENSITIVE);
    private static final Pattern LIMIT_PATTERN = Pattern.compile("(limit\\s+\\d+)$", Pattern.CASE_INSENSITIVE);
    private static final String SELECT = "select";
    private static final String COLON = ":";
    private static final String SEMI_COLON = ";";
    public static final String JDBC = "jdbc";
    private static final Map<String, IQueryTransformer> QUERY_TRANSFORMER_MAP = Maps.newConcurrentMap();
    private static final EscapeTransformer ESCAPE_TRANSFORMER = new EscapeTransformer();

    static {
        String[] classNames = KylinConfig.getInstanceFromEnv().getQueryTransformers();
        for (String clz : classNames) {
            try {
                IQueryTransformer transformer = (IQueryTransformer) ClassUtil.newInstance(clz);
                QUERY_TRANSFORMER_MAP.put(clz, transformer);
            } catch (Exception e) {
                log.error("Failed to init query transformer of the sys-config: {}", clz);
            }
        }
    }

    private QueryUtil() {
    }

    /**
     * Convert special functions in the input string into the counterpart
     * which can be parsed by Apache Calcite.
     */
    public static String adaptCalciteSyntax(String str) {
        if (StringUtils.isBlank(str)) {
            return str;
        }
        String transformed = StringHelper.backtickToDoubleQuote(str);
        transformed = ESCAPE_TRANSFORMER.transform(transformed);
        return transformed;
    }

    public static boolean isSelectStatement(String sql) {
        String sql1 = sql.toLowerCase(Locale.ROOT);
        sql1 = removeCommentInSql(sql1);
        sql1 = sql1.trim();
        while (sql1.startsWith("(")) {
            sql1 = sql1.substring(1).trim();
        }

        return sql1.startsWith(SELECT) || (sql1.startsWith("with") && sql1.contains(SELECT))
                || (sql1.startsWith("explain") && sql1.contains(SELECT));
    }

    public static boolean isSelectStarStatement(String sql) {
        return SELECT_STAR_PTN.matcher(sql).find();
    }

    /**
     * Remove comment from query statement. There are two kind of comment patterns:<br/>
     * 1. single line comment begins with "--",
     * with <strong>-- CubePriority(m1,m2)</strong> excluded.<br/>
     * 2. block comment like "/* comment content *&frasl;,
     * with <strong>/*+ MODEL_PRIORITY(m1,m2) *&frasl;</strong> excluded.<br/>
     *
     * @param sql the sql to handle
     * @return sql without comment
     */
    public static String removeCommentInSql(String sql) {
        try {
            return new RawSqlParser(sql).parse().getStatementString();
        } catch (Exception ex) {
            log.error("Something unexpected while removing comments in the query, return original query", ex);
            return sql;
        }
    }

    public static String makeErrorMsgUserFriendly(Throwable e) {
        String msg = e.getMessage();

        // pick ParseException error message if possible
        Throwable cause = e;
        boolean needBreak = false;
        while (cause != null) {
            String className = cause.getClass().getName();
            if (className.contains("ParseException") || className.contains("NoSuchTableException")
                    || className.contains("NoSuchDatabaseException") || cause instanceof AccessDeniedException) {
                msg = cause.getMessage();
                needBreak = true;
            } else if (className.contains("ArithmeticException")) {
                msg = "ArithmeticException: " + cause.getMessage();
                needBreak = true;
            } else if (className.contains("NoStreamingRealizationFoundException")) {
                msg = "NoStreamingRealizationFoundException: " + cause.getMessage();
                needBreak = true;
            }
            if (needBreak) {
                break;
            }
            cause = cause.getCause();
        }

        return makeErrorMsgUserFriendly(msg);
    }

    public static String makeErrorMsgUserFriendly(String errorMsg) {
        if (StringUtils.isBlank(errorMsg)) {
            return errorMsg;
        }
        errorMsg = errorMsg.trim();
        String[] split = errorMsg.split(COLON);
        if (split.length == 3) {
            String prefix = "Error";
            if (StringUtils.startsWithIgnoreCase(split[0], prefix)) {
                split[0] = split[0].substring(prefix.length()).trim();
            }
            prefix = "while executing SQL";
            if (StringUtils.startsWith(split[0], prefix)) {
                split[0] = split[0].substring(0, prefix.length()) + COLON + split[0].substring(prefix.length());
            }
            return split[1].trim() + COLON + StringUtils.SPACE + split[2].trim() + "\n" + split[0];
        } else {
            return errorMsg;
        }
    }

    public static String addLimit(String originString) {
        if (StringUtils.isBlank(originString)) {
            return originString;
        }
        String replacedString = originString.trim();
        Matcher selectMatcher = SELECT_PATTERN.matcher(replacedString);
        if (!selectMatcher.find()) {
            return originString;
        }

        while (replacedString.endsWith(SEMI_COLON)) {
            replacedString = replacedString.substring(0, replacedString.length() - 1).trim();
        }

        Matcher limitMatcher = LIMIT_PATTERN.matcher(replacedString);
        return limitMatcher.find() ? originString : replacedString.concat(" limit 1");
    }

    public static String massageSql(QueryParams queryParams) {
        String massagedSql = appendLimitOffset(queryParams.getProject(), queryParams.getSql(), queryParams.getLimit(),
                queryParams.getOffset());
        queryParams.setSql(massagedSql);
        massagedSql = transformSql(queryParams);
        QueryContext.current().record("end_massage_sql");
        return massagedSql;
    }

    public static String massageSqlAndExpandCC(QueryParams queryParams) {
        String massaged = massageSql(queryParams);
        return new RestoreFromComputedColumn().convert(massaged, queryParams.getProject(),
                queryParams.getDefaultSchema());
    }

    private static String transformSql(QueryParams queryParams) {
        String sql = queryParams.getSql();
        String[] classes = queryParams.getKylinConfig().getQueryTransformers();
        List<IQueryTransformer> transformers = fetchTransformers(queryParams.isCCNeeded(), classes);
        if (log.isDebugEnabled()) {
            log.debug("All used query transformers are: {}", transformers.stream()
                    .map(clz -> clz.getClass().getCanonicalName()).collect(Collectors.joining(",")));
        }

        for (IQueryTransformer t : transformers) {
            QueryInterruptChecker.checkThreadInterrupted(
                    "Interrupted sql transformation at the stage of " + t.getClass(),
                    "Current step: SQL transformation.");
            sql = t.transform(sql, queryParams.getProject(), queryParams.getDefaultSchema());
        }
        return sql;
    }

    public static String trimRightSemiColon(String sql) {
        while (sql.endsWith(SEMI_COLON)) {
            sql = sql.substring(0, sql.length() - 1).trim();
        }
        return sql;
    }

    public static String appendLimitOffset(String project, String sql, int limit, int offset) {
        sql = sql.replace("\r", StringUtils.SPACE).replace("\n", System.lineSeparator());
        sql = trimRightSemiColon(sql);

        //Split keywords and variables from sql by punctuation and whitespace character
        List<String> sqlElements = Lists.newArrayList(sql.toLowerCase(Locale.ROOT).split("(?![._'\"`])\\p{P}|\\s+"));

        KylinConfig projectConfig = NProjectManager.getProjectConfig(project);
        Integer maxRows = projectConfig.getMaxResultRows();
        if (maxRows != null && maxRows > 0 && (maxRows < limit || limit <= 0)) {
            limit = maxRows;
        }

        // https://issues.apache.org/jira/browse/KYLIN-2649
        if (projectConfig.getForceLimit() > 0 && limit <= 0 && !sql.toLowerCase(Locale.ROOT).contains("limit")
                && isSelectStarStatement(sql)) {
            limit = projectConfig.getForceLimit();
        }

        if (isBigQueryPushDownCapable(projectConfig)) {
            long bigQueryThreshold = BigQueryThresholdUpdater.getBigQueryThreshold();
            if (limit <= 0 && bigQueryThreshold > 0) {
                log.info("Big query route to pushdown, Add limit {} to sql.", bigQueryThreshold);
                limit = (int) bigQueryThreshold;
            }
        }

        if (limit > 0 && !sqlElements.contains("limit")) {
            sql += ("\nLIMIT " + limit);
        }

        if (offset > 0 && !sqlElements.contains("offset")) {
            sql += ("\nOFFSET " + offset);
        }

        return sql.trim();
    }

    public static boolean isBigQueryPushDownCapable(KylinConfig kylinConfig) {
        return kylinConfig.isBigQueryPushDown()
                && JDBC.equals(KapConfig.getInstanceFromEnv().getShareStateSwitchImplement());
    }

    public static List<IQueryTransformer> fetchTransformers(boolean isCCNeeded, String[] configTransformers) {
        List<IQueryTransformer> transformers = Lists.newArrayList();
        for (String clz : configTransformers) {
            String name = clz.substring(clz.lastIndexOf('.') + 1);
            if (REMOVED_TRANSFORMERS.contains(name)) {
                continue;
            }
            IQueryTransformer transformer;
            if (QUERY_TRANSFORMER_MAP.containsKey(clz)) {
                transformer = QUERY_TRANSFORMER_MAP.get(clz);
            } else {
                try {
                    transformer = (IQueryTransformer) ClassUtil.newInstance(clz);
                    QUERY_TRANSFORMER_MAP.putIfAbsent(clz, transformer);
                } catch (Exception e) {
                    throw new IllegalStateException("Failed to init query transformer", e);
                }
            }
            if (transformer instanceof ConvertToComputedColumn) {
                if (isCCNeeded) {
                    transformers.add(transformer);
                }
            } else {
                transformers.add(transformer);
            }
        }
        return transformers;
    }

    public static SqlSelect extractSqlSelect(SqlCall selectOrOrderby) {
        SqlSelect sqlSelect = null;
        if (selectOrOrderby instanceof SqlSelect) {
            sqlSelect = (SqlSelect) selectOrOrderby;
        } else if (selectOrOrderby instanceof SqlOrderBy) {
            SqlOrderBy sqlOrderBy = ((SqlOrderBy) selectOrOrderby);
            if (sqlOrderBy.query instanceof SqlSelect) {
                sqlSelect = (SqlSelect) sqlOrderBy.query;
            }
        }
        return sqlSelect;
    }
}

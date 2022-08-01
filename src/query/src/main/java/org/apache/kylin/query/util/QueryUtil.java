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

package org.apache.kylin.query.util;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.query.engine.QueryExec;
import org.apache.kylin.query.security.AccessDeniedException;
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

import com.google.common.collect.Lists;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryUtil {

    private static final Pattern SELECT_PATTERN = Pattern.compile("^select", Pattern.CASE_INSENSITIVE);
    private static final Pattern LIMIT_PATTERN = Pattern.compile("(limit\\s+[0-9;]+)$", Pattern.CASE_INSENSITIVE);
    static List<KapQueryUtil.IQueryTransformer> tableDetectTransformers = Collections.emptyList();

    private QueryUtil() {
        throw new IllegalStateException("Utility class");
    }

    public static String normalizeForTableDetecting(String project, String sql) {
        KylinConfig kylinConfig = KapQueryUtil.getKylinConfig(project);
        String convertedSql = KapQueryUtil.normalMassageSql(kylinConfig, sql, 0, 0);
        String defaultSchema = "DEFAULT";
        try {
            QueryExec queryExec = new QueryExec(project, kylinConfig);
            defaultSchema = queryExec.getDefaultSchemaName();
        } catch (Exception e) {
            log.error("Get project default schema failed.", e);
        }

        String[] detectorTransformers = kylinConfig.getTableDetectorTransformers();
        List<KapQueryUtil.IQueryTransformer> transformerList = KapQueryUtil.initTransformers(false, detectorTransformers);
        tableDetectTransformers = Collections.unmodifiableList(transformerList);
        for (KapQueryUtil.IQueryTransformer t : tableDetectTransformers) {
            convertedSql = t.transform(convertedSql, project, defaultSchema);
        }
        return convertedSql;
    }

    public static String makeErrorMsgUserFriendly(Throwable e) {
        String msg = e.getMessage();

        // pick ParseException error message if possible
        Throwable cause = e;
        while (cause != null) {
            if (cause.getClass().getName().contains("ParseException") || cause instanceof NoSuchTableException
                    || cause instanceof NoSuchDatabaseException || cause instanceof AccessDeniedException) {
                msg = cause.getMessage();
                break;
            }

            if (cause.getClass().getName().contains("ArithmeticException")) {
                msg = "ArithmeticException: " + cause.getMessage();
                break;
            }

            if (cause.getClass().getName().contains("NoStreamingRealizationFoundException")) {
                msg = "NoStreamingRealizationFoundException: " + cause.getMessage();
                break;
            }
            cause = cause.getCause();
        }

        return makeErrorMsgUserFriendly(msg);
    }

    public static String makeErrorMsgUserFriendly(String errorMsg) {
        try {
            errorMsg = errorMsg.trim();

            // move cause to be ahead of sql, calcite creates the message pattern below
            Pattern pattern = Pattern.compile("Error while executing SQL ([\\s\\S]*):(.*):(.*)");
            Matcher matcher = pattern.matcher(errorMsg);
            if (matcher.find()) {
                return matcher.group(2).trim() + ": " + matcher.group(3).trim() + "\nwhile executing SQL: "
                        + matcher.group(1).trim();
            } else
                return errorMsg;
        } catch (Exception e) {
            return errorMsg;
        }
    }

    public static boolean isSelectStatement(String sql) {
        String sql1 = sql.toLowerCase(Locale.ROOT);
        sql1 = removeCommentInSql(sql1);
        sql1 = sql1.trim();
        while (sql1.startsWith("(")) {
            sql1 = sql1.substring(1).trim();
        }
        return sql1.startsWith("select") || (sql1.startsWith("with") && sql1.contains("select"))
                || (sql1.startsWith("explain") && sql1.contains("select"));
    }

    public static String removeCommentInSql(String sql) {
        // match two patterns, one is "-- comment", the other is "/* comment */"
        try {
            return new RawSqlParser(sql).parse().getStatementString();
        } catch (Exception ex) {
            log.error("Something unexpected while removing comments in the query, return original query", ex);
            return sql;
        }
    }

    public static List<String> splitBySemicolon(String s) {
        List<String> r = Lists.newArrayList();
        StringBuilder sb = new StringBuilder();
        boolean inQuota = false;
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) == '\'') {
                inQuota = !inQuota;
            }
            if (s.charAt(i) == ';' && !inQuota) {
                if (sb.length() != 0) {
                    r.add(sb.toString());
                    sb = new StringBuilder();
                }
                continue;
            }
            sb.append(s.charAt(i));
        }
        if (sb.length() != 0) {
            r.add(sb.toString());
        }
        return r;
    }

    public static String addLimit(String originString) {
        Matcher selectMatcher = SELECT_PATTERN.matcher(originString);
        Matcher limitMatcher = LIMIT_PATTERN.matcher(originString);
        String replacedString = originString;

        if (selectMatcher.find() && !limitMatcher.find()) {
            if (originString.endsWith(";")) {
                replacedString = originString.replaceAll(";+$", "");
            }

            replacedString = replacedString.concat(" limit 1");
        }

        return replacedString;
    }
}

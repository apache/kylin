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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 */
public class QueryUtil {

    protected static final Logger logger = LoggerFactory.getLogger(QueryUtil.class);

    private static List<IQueryTransformer> queryTransformers;

    public interface IQueryTransformer {
        String transform(String sql, String project, String defaultSchema);
    }

    public static String massageSql(String sql, String project, int limit, int offset, String defaultSchema) {
        sql = sql.trim();
        sql = sql.replace("\r", " ").replace("\n", System.getProperty("line.separator"));
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        while (sql.endsWith(";"))
            sql = sql.substring(0, sql.length() - 1);

        if (limit > 0 && !sql.toLowerCase().contains("limit")) {
            sql += ("\nLIMIT " + limit);
        }

        if (offset > 0 && !sql.toLowerCase().contains("offset")) {
            sql += ("\nOFFSET " + offset);
        }

        // https://issues.apache.org/jira/browse/KYLIN-2649
        if (kylinConfig.getForceLimit() > 0 && !sql.toLowerCase().contains("limit")
                && sql.toLowerCase().contains("*")) {
            sql += ("\nLIMIT " + kylinConfig.getForceLimit());
        }

        // customizable SQL transformation
        if (queryTransformers == null) {
            initQueryTransformers();
        }
        for (IQueryTransformer t : queryTransformers) {
            sql = t.transform(sql, project, defaultSchema);
        }
        return sql;
    }

    private static void initQueryTransformers() {
        List<IQueryTransformer> transformers = Lists.newArrayList();
        transformers.add(new DefaultQueryTransformer());

        String[] classes = KylinConfig.getInstanceFromEnv().getQueryTransformers();
        for (String clz : classes) {
            try {
                IQueryTransformer t = (IQueryTransformer) ClassUtil.newInstance(clz);
                transformers.add(t);
            } catch (Exception e) {
                throw new RuntimeException("Failed to init query transformer", e);
            }
        }
        queryTransformers = transformers;
    }

    // correct sick / invalid SQL
    private static class DefaultQueryTransformer implements IQueryTransformer {

        private static final String S0 = "\\s*";
        private static final String S1 = "\\s";
        private static final String SM = "\\s+";
        private static final Pattern PTN_GROUP_BY = Pattern.compile(S1 + "GROUP" + SM + "BY" + S1,
                Pattern.CASE_INSENSITIVE);
        private static final Pattern PTN_HAVING_COUNT_GREATER_THAN_ZERO = Pattern.compile(S1 + "HAVING" + SM + "[(]?"
                + S0 + "COUNT" + S0 + "[(]" + S0 + "1" + S0 + "[)]" + S0 + ">" + S0 + "0" + S0 + "[)]?",
                Pattern.CASE_INSENSITIVE);
        private static final Pattern PTN_SUM_1 = Pattern.compile(S0 + "SUM" + S0 + "[(]" + S0 + "[1]" + S0 + "[)]" + S0,
                Pattern.CASE_INSENSITIVE);
        private static final Pattern PTN_NOT_EQ = Pattern.compile(S0 + "!=" + S0, Pattern.CASE_INSENSITIVE);
        private static final Pattern PTN_INTERVAL = Pattern.compile(
                "interval" + SM + "(floor\\()([\\d\\.]+)(\\))" + SM + "(second|minute|hour|day|month|year)",
                Pattern.CASE_INSENSITIVE);
        private static final Pattern PTN_HAVING_ESCAPE_FUNCTION = Pattern.compile("\\{fn" + "(.*?)" + "\\}",
                Pattern.CASE_INSENSITIVE);

        @Override
        public String transform(String sql, String project, String defaultSchema) {
            Matcher m;

            // Case fn{ EXTRACT(...) }
            // Use non-greedy regrex matching to remove escape functions
            while (true) {
                m = PTN_HAVING_ESCAPE_FUNCTION.matcher(sql);
                if (!m.find())
                    break;
                sql = sql.substring(0, m.start()) + m.group(1) + sql.substring(m.end());
            }

            // Case: HAVING COUNT(1)>0 without Group By
            // Tableau generates: SELECT SUM(1) AS "COL" FROM "VAC_SW" HAVING
            // COUNT(1)>0
            m = PTN_HAVING_COUNT_GREATER_THAN_ZERO.matcher(sql);
            if (m.find() && PTN_GROUP_BY.matcher(sql).find() == false) {
                sql = sql.substring(0, m.start()) + " " + sql.substring(m.end());
            }

            // Case: SUM(1)
            // Replace it with COUNT(1)
            while (true) {
                m = PTN_SUM_1.matcher(sql);
                if (!m.find())
                    break;
                sql = sql.substring(0, m.start()) + " COUNT(1) " + sql.substring(m.end());
            }

            // Case: !=
            // Replace it with <>
            while (true) {
                m = PTN_NOT_EQ.matcher(sql);
                if (!m.find())
                    break;
                sql = sql.substring(0, m.start()) + " <> " + sql.substring(m.end());
            }

            // ( date '2001-09-28' + interval floor(1) day ) generated by cognos
            // calcite only recognizes date '2001-09-28' + interval '1' day
            while (true) {
                m = PTN_INTERVAL.matcher(sql);
                if (!m.find())
                    break;

                int value = (int) Math.floor(Double.valueOf(m.group(2)));
                sql = sql.substring(0, m.start(1)) + "'" + value + "'" + sql.substring(m.end(3));
            }

            return sql;
        }

    }

    public static String makeErrorMsgUserFriendly(Throwable e) {
        String msg = e.getMessage();

        // pick ParseException error message if possible
        Throwable cause = e;
        while (cause != null) {
            if (cause.getClass().getName().contains("ParseException")) {
                msg = cause.getMessage();
                break;
            }
            cause = cause.getCause();
        }

        return makeErrorMsgUserFriendly(msg);
    }

    public static String makeErrorMsgUserFriendly(String errorMsg) {
        try {
            // make one line
            errorMsg = errorMsg.replaceAll("\\s", " ");

            // move cause to be ahead of sql, calcite creates the message pattern below
            Pattern pattern = Pattern.compile("Error while executing SQL \"(.*)\":(.*)");
            Matcher matcher = pattern.matcher(errorMsg);
            if (matcher.find()) {
                return matcher.group(2).trim() + "\nwhile executing SQL: \"" + matcher.group(1).trim() + "\"";
            } else
                return errorMsg;
        } catch (Exception e) {
            return errorMsg;
        }
    }

    public static boolean isSelectStatement(String sql) {
        String sql1 = sql.toLowerCase();
        sql1 = removeCommentInSql(sql1);
        sql1 = sql1.trim();
        return sql1.startsWith("select") || (sql1.startsWith("with") && sql1.contains("select"))
                || (sql1.startsWith("explain") && sql1.contains("select"));
    }

    public static String removeCommentInSql(String sql1) {
        // match two patterns, one is "-- comment", the other is "/* comment */"
        final String[] commentPatterns = new String[] { "--[^\r\n]*", "/\\*[^\\*/]*" };
        final int[] endOffset = new int[] { 0, 2 };

        for (int i = 0; i < commentPatterns.length; i++) {
            String commentPattern = commentPatterns[i];
            Pattern pattern = Pattern.compile(commentPattern);
            Matcher matcher = pattern.matcher(sql1);

            while (matcher.find()) {
                if (matcher.start() == 0) {
                    sql1 = sql1.substring(matcher.end() + endOffset[i]).trim();
                } else if ((matcher.start() > 0 && sql1.charAt(matcher.start() - 1) != '\'')) {
                    sql1 = (sql1.substring(0, matcher.start()) + sql1.substring(matcher.end() + endOffset[i])).trim();
                }
                matcher = pattern.matcher(sql1);
            }
        }

        return sql1;
    }
}

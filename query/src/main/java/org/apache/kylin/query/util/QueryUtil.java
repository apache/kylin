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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Lists;

/**
 */
public class QueryUtil {

    protected static final Logger logger = LoggerFactory.getLogger(QueryUtil.class);
    private static final String KEYWORD_SELECT = "select";
    private static final String KEYWORD_WITH = "with";
    private static final String KEYWORD_EXPLAIN = "explain";
    private static List<IQueryTransformer> queryTransformers;
    private QueryUtil() {
        throw new IllegalStateException("Class QueryUtil is an utility class !");
    }

    public static String appendLimitOffsetToSql(String sql, int limit, int offset) {
        String retSql = sql;
        String prefixSql = "select * from (";
        String suffixSql = ")";
        if (StringUtils.startsWithIgnoreCase(sql, KEYWORD_EXPLAIN)
                || StringUtils.startsWithIgnoreCase(sql, KEYWORD_WITH)) {
            prefixSql = "";
            suffixSql = "";
        }
        if (0 != limit && 0 != offset) {
            retSql = prefixSql + sql + suffixSql + " limit " + String.valueOf(limit) + " offset "
                    + String.valueOf(offset);
        } else if (0 == limit && 0 != offset) {
            retSql = prefixSql + sql + suffixSql + " offset " + String.valueOf(offset);
        } else if (0 != limit && 0 == offset) {
            retSql = prefixSql + sql + suffixSql + " limit " + String.valueOf(limit);
        } else {
            // do nothing
        }
        return retSql;
    }

    /**
     * @deprecated Deprecated because of KYLIN-3594
     */
    @Deprecated
    public static String massageSql(String sql, String project, int limit, int offset, String defaultSchema) {
        sql = sql.trim();
        sql = sql.replace("\r", " ").replace("\n", System.getProperty("line.separator"));

        ProjectManager projectManager = ProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ProjectInstance projectInstance = projectManager.getProject(project);
        KylinConfig kylinConfig = projectInstance.getConfig();
        sql = removeCommentInSql(sql);
        while (sql.endsWith(";"))
            sql = sql.substring(0, sql.length() - 1);

        String sql1 = sql;
        final String suffixPattern = "^.+?\\s(limit\\s\\d+)?\\s(offset\\s\\d+)?\\s*$";
        sql = sql.replaceAll("\\s+", " ");
        Pattern pattern = Pattern.compile(suffixPattern);
        Matcher matcher = pattern.matcher(sql.toLowerCase(Locale.ROOT) + "  ");

        int toAppendLimit = 0;
        int toAppendOffset = 0;
        if (matcher.find()) {
            if (limit > 0 && matcher.group(1) == null) {
                toAppendLimit = limit;
            }
            if (offset > 0 && matcher.group(2) == null) {
                toAppendOffset = offset;
            }
        }

        // https://issues.apache.org/jira/browse/KYLIN-2649
        if (kylinConfig.getForceLimit() > 0 && limit <= 0 && matcher.group(1) == null
                && sql1.toLowerCase(Locale.ROOT).matches("^select\\s+\\*\\p{all}*")) {
            toAppendLimit = kylinConfig.getForceLimit();
        }

        sql1 = appendLimitOffsetToSql(sql1, toAppendLimit, toAppendOffset);

        // customizable SQL transformation
        if (queryTransformers == null) {
            initQueryTransformers();
        }
        for (IQueryTransformer t : queryTransformers) {
            sql1 = t.transform(sql1, project, defaultSchema);
        }
        return sql1;
    }

    /**
     * add remove catalog step at final
     */
    public static String massageSql(String sql, String project, int limit, int offset, String defaultSchema,
            String catalog) {
        String correctedSql = massageSql(sql, project, limit, offset, defaultSchema);
        correctedSql = removeCatalog(correctedSql, catalog);
        return correctedSql;
    }

    /**
     * Although SQL standard define CATALOG concept, ISV has right not to implement it.
     * We remove it in before send it to SQL parser.
     *
     * @param sql query which maybe has such pattern: [[catalogName.]schemaName.]tableName
     * @return replace [[catalogName.]schemaName.]tableName with [schemaName.]tableName
     */
    static String removeCatalog(String sql, String catalog) {
        if (catalog == null)
            return sql;
        else
            return sql.replace(catalog + ".", "");
    }

    private static void initQueryTransformers() {
        List<IQueryTransformer> transformers = Lists.newArrayList();

        String[] classes = KylinConfig.getInstanceFromEnv().getQueryTransformers();
        for (String clz : classes) {
            try {
                IQueryTransformer t = (IQueryTransformer) ClassUtil.newInstance(clz);
                transformers.add(t);
            } catch (Exception e) {
                throw new IllegalStateException("Failed to init query transformer", e);
            }
        }

        queryTransformers = transformers;
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

            if (cause.getClass().getName().contains("ArithmeticException")) {
                msg = "ArithmeticException: " + cause.getMessage();
                break;
            }

            if (cause.getClass().getName().contains("NumberFormatException")) {
                msg = "NumberFormatException: " + cause.getMessage();
                break;
            }

            //where in(...) condition has too many elements
            if (cause.getClass().equals(StackOverflowError.class)) {
                msg = "StackOverflowError maybe caused by that filters have too many elements";
                break;
            }
            cause = cause.getCause();
        }

        return makeErrorMsgUserFriendly(msg);
    }

    public static String makeErrorMsgUserFriendly(String errorMsg) {
        if (errorMsg == null) {
            return "Unknown error.";
        }
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
        String sql1 = sql.toLowerCase(Locale.ROOT);
        sql1 = removeCommentInSql(sql1);
        sql1 = sql1.trim();

        return sql1.startsWith(KEYWORD_SELECT) || (sql1.startsWith(KEYWORD_WITH) && sql1.contains(KEYWORD_SELECT))
                || (sql1.startsWith(KEYWORD_EXPLAIN) && sql1.contains(KEYWORD_SELECT));
    }


    public static String removeCommentInSql(String sql) {
        // match two patterns, one is "-- comment", the other is "/* comment */"
        try {
            return new CommentParser(sql).Input().trim();
        } catch (ParseException e) {
            logger.error("Failed to parse sql: {}", sql, e);
            return sql;
        }
    }

    public interface IQueryTransformer {
        String transform(String sql, String project, String defaultSchema);
    }
}

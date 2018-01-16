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
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
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

        ProjectManager projectManager = ProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ProjectInstance projectInstance = projectManager.getProject(project);
        KylinConfig kylinConfig = projectInstance.getConfig();

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
                && sql.toLowerCase().matches("^select\\s+\\*\\p{all}*")) {
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
        final String[] commentPatterns = new String[] { "--.*?[\r\n]", "/\\*.*?\\*/" };

        for (int i = 0; i < commentPatterns.length; i++) {
            sql1 = sql1.replaceAll(commentPatterns[i], "");
        }

        sql1 = sql1.trim();

        return sql1;
    }
}

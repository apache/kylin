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

import java.io.IOException;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.TempStatementManager;

public class TempStatementUtil {
    private static final String WITH = "WITH";
    private static final String DROP = "DROP";
    private static final String CREATE = "CREATE";

    public static Pair<Boolean, String> handleTempStatement(String sql, KylinConfig config) {
        if (!config.isConvertCreateTableToWith()) {
            return new Pair<>(false, sql);
        }

        if (isDropTable(sql)) {
            return new Pair<>(true, sql);
        }

        if (isCreateTable(sql)) {
            try {
                translateCreateToWith(sql, config);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
            return new Pair<>(true, sql);
        }

        sql = TempStatementUtil.appendWith(sql, config);

        return new Pair<>(false, sql);
    }

    private static void translateCreateToWith(String sql, KylinConfig config) throws IOException {
        Pair<String, String> translated = translateCreateToWithInternal(sql);
        String identifier = translated.getFirst();
        String sql1 = translated.getSecond();

        TempStatementManager manager = TempStatementManager.getInstance(config);
        if (manager.getTempStatement(identifier) == null || !manager.getTempStatement(identifier).equals(sql1)) {
            manager.updateTempStatement(identifier, sql1);
        }
    }

    private enum ParserState {
        WAITING_WITH, WAITING_IDENTIFIER, WAITING_AS, WAITING_LEFT_PAREN, IGNORE
    }

    static Pair<String, String> translateCreateToWithInternal(String sql) {
        String sql1 = sql.trim();
        if (sql1.endsWith(";")) {
            sql1 = sql1.substring(0, sql1.length() - 1);
        }

        if (sql1.matches("(?i)^CREATE\\s+TABLE\\p{all}*")) {
            sql1 = sql1.replaceAll("(?i)CREATE\\s+TABLE", WITH);
        } else if (sql1.matches("(?i)^CREATE\\s+TEMPORARY\\s+TABLE\\p{all}*")) {
            sql1 = sql1.replaceAll("(?i)CREATE\\s+TEMPORARY\\s+TABLE", WITH);
        } else {
            throw new RuntimeException("Sql " + sql + " is not create table sql");
        }

        String identifier = null;
        String[] splits = sql1.split("((?<=\\s)|(?=\\s))");
        Pattern spacePattern = Pattern.compile("\\s");
        ParserState state = ParserState.WAITING_WITH;

        for (int i = 0; i < splits.length; i++) {
            if (state == ParserState.IGNORE) {
                break;
            }
            if (spacePattern.matcher(splits[i]).matches()) {
                continue;
            }
            switch (state) {
            case WAITING_WITH:
                if (!splits[i].equals(WITH)) {
                    throw new RuntimeException("the translated sql should start with \"WITH\", sql is " + sql1);
                }
                state = ParserState.WAITING_IDENTIFIER;
                break;
            case WAITING_IDENTIFIER:
                identifier = splits[i];
                state = ParserState.WAITING_AS;
                break;
            case WAITING_AS:
                if (!splits[i].equalsIgnoreCase("AS")) {
                    throw new RuntimeException("the translated sql should contains \"AS\", sql is " + sql1);
                }
                state = ParserState.WAITING_LEFT_PAREN;
                break;
            // Calcite require "(" after AS
            case WAITING_LEFT_PAREN:
                if (!splits[i].startsWith("(")) {
                    splits[i] = "(" + splits[i];
                    splits[splits.length - 1] = splits[splits.length - 1] + ")";
                }
                state = ParserState.IGNORE;
                break;
            default:
                break;
            }
        }

        // drop word WITH
        sql1 = StringUtils.join(splits, "", 1, splits.length);
        return new Pair<>(identifier, sql1);
    }

    private static boolean isCreateTable(String sql) {
        return sql.trim().toUpperCase(Locale.ROOT).startsWith(CREATE);
    }

    private static boolean isDropTable(String sql) {
        return sql.trim().toUpperCase(Locale.ROOT).startsWith(DROP);
    }

    private static boolean isWith(String sql) {
        return sql.trim().toUpperCase(Locale.ROOT).startsWith(WITH);
    }

    private static String appendWith(String sql, KylinConfig config) {
        if (!config.isConvertCreateTableToWith() || isWith(sql)) {
            return sql;
        }

        String[] splits = sql.split("\\W+");
        StringBuilder builder = new StringBuilder();
        Set<String> appended = new HashSet<>();

        TempStatementManager manager = TempStatementManager.getInstance(config);
        for (String s : splits) {
            if (manager.getTempStatement(s) != null && !appended.contains(s)) {
                appended.add(s);
                if (appended.size() == 1) {
                    builder.append(WITH);
                } else {
                    builder.append(",");
                }
                builder.append(manager.getTempStatement(s)).append(" ");
            }
        }

        return builder.append(sql).toString();
    }
}

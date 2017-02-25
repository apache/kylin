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

package org.apache.kylin.rest.util;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.rest.util.QueryUtil.IQueryTransformer;

/**
 * from (a join b on a.x = b.y) join c
 * 
 * similar in https://issues.apache.org/jira/browse/CALCITE-35
 * 
 * we'll find such pattern and remove the parentheses
 */
public class CognosParenthesesEscape implements IQueryTransformer {

    private static final String S0 = "\\s*";
    private static final String S1 = "\\s";
    private static final String SM = "\\s+";
    private static final String TABLE_OR_COLUMN_NAME = "[\\w\\\"\\'\\.]+";
    private static final String TABLE_NAME_WITH_OPTIONAL_ALIAS = TABLE_OR_COLUMN_NAME + "((\\s+as)?\\s+" + TABLE_OR_COLUMN_NAME + ")?";
    private static final String JOIN = "(\\s+inner|\\s+((left|right|full)(\\s+outer)?))?\\s+join";// as per http://stackoverflow.com/questions/406294/left-join-vs-left-outer-join-in-sql-server
    private static final String EQUAL_CONDITION = SM + TABLE_OR_COLUMN_NAME + S0 + "=" + S0 + TABLE_OR_COLUMN_NAME;
    private static final String PARENTHESE_PATTERN_STR = "\\(" + S0 + // (
            TABLE_NAME_WITH_OPTIONAL_ALIAS + // a
            JOIN + SM + // join
            TABLE_NAME_WITH_OPTIONAL_ALIAS + //b
            SM + "on" + EQUAL_CONDITION + "(\\s+and" + EQUAL_CONDITION + ")*" + // on a.x = b.y [and a.x2 = b.y2]
            S0 + "\\)";// )
    private static final Pattern PARENTTHESES_PATTERN = Pattern.compile(PARENTHESE_PATTERN_STR, Pattern.CASE_INSENSITIVE);

    private static int identifierNum = 0;

    @Override
    public String transform(String sql) {
        Matcher m;
        List<Pair<String, String>> matches = new LinkedList<>();
        while (true) {
            m = PARENTTHESES_PATTERN.matcher(sql);
            if (!m.find())
                break;

            String oneParentheses = m.group(0);
            String identifier = generateRandomName();
            matches.add(new Pair<String, String>(identifier, oneParentheses.substring(1, oneParentheses.length() - 1)));
            sql = sql.substring(0, m.start()) + identifier + sql.substring(m.end());
        }

        for (int i = matches.size() - 1; i >= 0; i--) {
            sql = sql.replaceAll(matches.get(i).getKey(), matches.get(i).getValue());
        }

        return sql;
    }

    private String generateRandomName() {
        UUID uuid = UUID.randomUUID();
        return uuid.toString().replace("-", "_") + "_" + (identifierNum++);
    }

}

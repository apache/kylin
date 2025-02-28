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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.query.IQueryTransformer;
import org.apache.kylin.source.adhocquery.IPushDownConverter;

public class CognosParenthesesEscapeTransformer implements IQueryTransformer, IPushDownConverter {
    private static final Pattern FROM_PATTERN = Pattern.compile("\\bfrom(\\s*\\()+(?!\\s*select\\s)",
            Pattern.CASE_INSENSITIVE);

    @Override
    public String transform(String sql, String project, String defaultSchema) {
        return StringUtils.isEmpty(sql) ? sql : completion(sql);
    }

    String completion(String sql) {
        Map<Integer, Integer> parenthesesPairs = findParenthesesPairs(sql);
        if (parenthesesPairs.isEmpty()) {
            // parentheses not found
            return sql;
        }

        List<Integer> parentheses = Lists.newArrayList();
        String originSql = sql;
        Matcher m;
        int offset = 0; // use this to locate the index of matched parentheses in the pattern in original sql
        boolean done = false;
        while (!done) {
            m = FROM_PATTERN.matcher(sql);
            if (m.find()) {
                int i = m.end() - 1;
                while (i > m.start()) {
                    if (sql.charAt(i) == '(') {
                        parentheses.add(i + offset);
                    }
                    i--;
                }
                if (m.end() < sql.length()) {
                    offset += m.end();
                    sql = sql.substring(m.end());
                    continue;
                }
            }
            done = true;
        }

        List<Integer> indices = Lists.newArrayList();
        parentheses.forEach(index -> {
            indices.add(index);
            indices.add(parenthesesPairs.get(index));
        });
        indices.sort(Integer::compareTo);

        StringBuilder builder = new StringBuilder();
        int lastIndex = 0;
        for (Integer i : indices) {
            builder.append(originSql, lastIndex, i);
            lastIndex = i + 1;
        }
        builder.append(originSql, lastIndex, originSql.length());
        return builder.toString();
    }

    private Map<Integer, Integer> findParenthesesPairs(String sql) {
        Map<Integer, Integer> result = new HashMap<>();
        if (sql.length() > 1) {
            Deque<Integer> lStack = new ArrayDeque<>();
            boolean inStrVal = false;
            for (int i = 0; i < sql.length(); i++) {
                switch (sql.charAt(i)) {
                case '(':
                    if (!inStrVal) {
                        lStack.push(i);
                    }
                    break;
                case ')':
                    if (!inStrVal && !lStack.isEmpty()) {
                        result.put(lStack.pop(), i);
                    }
                    break;
                case '\'':
                    inStrVal = !inStrVal;
                    break;
                default:
                    break;
                }
            }
        }
        return result;
    }

    @Override
    public String convert(String originSql, String project, String defaultSchema) {
        return transform(originSql, project, defaultSchema);
    }
}

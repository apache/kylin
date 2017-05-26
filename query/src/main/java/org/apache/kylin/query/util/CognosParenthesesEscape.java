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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.Lists;

public class CognosParenthesesEscape implements QueryUtil.IQueryTransformer {
    private static final Pattern FROM_PATTERN = Pattern.compile("\\s+from\\s+(\\s*\\(\\s*)+(?!\\s*select\\s)",
            Pattern.CASE_INSENSITIVE);

    @Override
    public String transform(String sql) {
        if (sql == null || sql.isEmpty()) {
            return sql;
        }

        Map<Integer, Integer> parenthesesPairs = findParenthesesPairs(sql);
        if (parenthesesPairs.isEmpty()) {
            // parentheses not found
            return sql;
        }

        List<Integer> parentheses = Lists.newArrayList();
        StringBuilder result = new StringBuilder(sql);

        Matcher m;
        while (true) {
            m = FROM_PATTERN.matcher(sql);
            if (!m.find()) {
                break;
            }

            int i = m.end() - 1;
            while (i > m.start()) {
                if (sql.charAt(i) == '(') {
                    parentheses.add(i);
                }
                i--;
            }

            if (m.end() < sql.length()) {
                sql = sql.substring(m.end());
            } else {
                break;
            }
        }

        Collections.sort(parentheses);
        for (int i = 0; i < parentheses.size(); i++) {
            result.deleteCharAt(parentheses.get(i) - i);
            result.deleteCharAt(parenthesesPairs.get(parentheses.get(i)) - i - 1);
        }
        return result.toString();
    }

    private Map<Integer, Integer> findParenthesesPairs(String sql) {
        Map<Integer, Integer> result = new HashMap<>();
        if (sql.length() > 1) {
            Stack<Integer> lStack = new Stack<>();
            boolean inStrVal = false;
            for (int i = 0; i < sql.length(); i++) {
                switch (sql.charAt(i)) {
                case '(':
                    if (!inStrVal) {
                        lStack.push(i);
                    }
                    break;
                case ')':
                    if (!inStrVal && !lStack.empty()) {
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
}

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
package org.apache.kylin.storage.adhoc;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//TODO: Some workaround ways to make sql readable by hive parser, should replaced it with a more well-designed way
public class HiveAdhocConverter implements IAdhocConverter {

    private static final Logger logger = LoggerFactory.getLogger(HiveAdhocConverter.class);

    private static final Pattern EXTRACT_PATTERN = Pattern.compile("\\s+extract\\s*(\\()\\s*(.*?)\\s*from(\\s+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern FROM_PATTERN = Pattern.compile("\\s+from\\s+(\\()\\s*select\\s", Pattern.CASE_INSENSITIVE);
    private static final Pattern CAST_PATTERN = Pattern.compile("CAST\\((.*?) (?i)AS\\s*(.*?)\\s*\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern CONCAT_PATTERN = Pattern.compile("(['_a-z0-9A-Z]+)\\|\\|(['_a-z0-9A-Z]+)", Pattern.CASE_INSENSITIVE);

    public static String replaceString(String originString, String fromString, String toString) {
        return originString.replace(fromString, toString);
    }

    public static String extractReplace(String originString) {
        Matcher extractMatcher = EXTRACT_PATTERN.matcher(originString);
        String replacedString = originString;
        Map<Integer, Integer> parenthesesPairs = null;

        while (extractMatcher.find()) {
            if (parenthesesPairs == null) {
                parenthesesPairs = findParenthesesPairs(originString);
            }

            String functionStr = extractMatcher.group(2);
            int startIdx = extractMatcher.end(3);
            int endIdx = parenthesesPairs.get(extractMatcher.start(1));
            String extractInner = originString.substring(startIdx, endIdx);
            int originStart = extractMatcher.start(0) + 1;
            int originEnd = endIdx + 1;

            replacedString = replaceString(replacedString, originString.substring(originStart, originEnd), functionStr + "(" + extractInner + ")");
        }

        return replacedString;
    }

    public static String castRepalce(String originString) {
        Matcher castMatcher = CAST_PATTERN.matcher(originString);
        String replacedString = originString;

        while (castMatcher.find()) {
            String castStr = castMatcher.group();
            String type = castMatcher.group(2);
            String supportedType = "";
            switch (type.toUpperCase()) {
            case "INTEGER":
                supportedType = "int";
                break;
            case "SHORT":
                supportedType = "smallint";
                break;
            case "LONG":
                supportedType = "bigint";
                break;
            default:
                supportedType = type;
            }

            if (!supportedType.equals(type)) {
                String replacedCastStr = castStr.replace(type, supportedType);
                replacedString = replaceString(replacedString, castStr, replacedCastStr);
            }
        }

        return replacedString;
    }

    public static String subqueryRepalce(String originString) {
        Matcher subqueryMatcher = FROM_PATTERN.matcher(originString);
        String replacedString = originString;
        Map<Integer, Integer> parenthesesPairs = null;

        while (subqueryMatcher.find()) {
            if (parenthesesPairs == null) {
                parenthesesPairs = findParenthesesPairs(originString);
            }

            int startIdx = subqueryMatcher.start(1);
            int endIdx = parenthesesPairs.get(startIdx) + 1;

            replacedString = replaceString(replacedString, originString.substring(startIdx, endIdx), originString.substring(startIdx, endIdx) + " as alias");
        }

        return replacedString;
    }

    public static String concatReplace(String originString) {
        Matcher concatMatcher = CONCAT_PATTERN.matcher(originString);
        String replacedString = originString;

        while (concatMatcher.find()) {
            String leftString = concatMatcher.group(1);
            String rightString = concatMatcher.group(2);
            replacedString = replaceString(replacedString, leftString + "||" + rightString, "concat(" + leftString + "," + rightString + ")");
        }

        return replacedString;
    }

    public static String doConvert(String originStr) {
        // Step1.Replace " with `
        String convertedSql = replaceString(originStr, "\"", "`");

        // Step2.Replace extract functions
        convertedSql = extractReplace(convertedSql);

        // Step3.Replace cast type string
        convertedSql = castRepalce(convertedSql);

        // Step4.Replace sub query
        convertedSql = subqueryRepalce(convertedSql);

        // Step5.Replace char_length with length
        convertedSql = replaceString(convertedSql, "char_length", "length");

        // Step6.Replace "||" with concat
        convertedSql = concatReplace(convertedSql);

        return convertedSql;
    }

    private static Map<Integer, Integer> findParenthesesPairs(String sql) {
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
                default:
                    break;
                }
            }
        }
        return result;
    }

    @Override
    public String convert(String originSql) {
        return doConvert(originSql);
    }
}

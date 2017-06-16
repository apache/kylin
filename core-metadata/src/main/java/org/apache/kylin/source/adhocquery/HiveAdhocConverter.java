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
package org.apache.kylin.source.adhocquery;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;

//TODO: Some workaround ways to make sql readable by hive parser, should replaced it with a more well-designed way
public class HiveAdhocConverter implements IAdHocConverter {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(HiveAdhocConverter.class);

    private static final Pattern EXTRACT_PATTERN = Pattern.compile("extract\\s*(\\()\\s*(.*?)\\s*from(\\s+)",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern FROM_PATTERN = Pattern.compile("\\s+from\\s+(\\()\\s*select\\s",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern ALIAS_PATTERN = Pattern.compile("\\s+([`'_a-z0-9A-Z]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern CAST_PATTERN = Pattern.compile("CAST\\((.*?) (?i)AS\\s*(.*?)\\s*\\)",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern CONCAT_PATTERN = Pattern.compile("(['_a-z0-9A-Z]+)\\|\\|(['_a-z0-9A-Z]+)",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern TIMESTAMPADD_PATTERN = Pattern.compile("timestampadd\\s*\\(\\s*(.*?)\\s*,",
            Pattern.CASE_INSENSITIVE);
    private static final ImmutableSet<String> sqlKeyWordsExceptAS = ImmutableSet.of("ABSOLUTE", "ACTION", "ADD", "ALL",
            "ALLOCATE", "ALTER", "AND", "ANY", "ARE", "ASC", "ASSERTION", "AT", "AUTHORIZATION", "AVG", "BEGIN",
            "BETWEEN", "BIT", "BIT_LENGTH", "BOTH", "BY", "CASCADE", "CASCADED", "CASE", "CAST", "CATALOG", "CHAR",
            "CHARACTER", "CHARACTER_LENGTH", "CHAR_LENGTH", "CHECK", "CLOSE", "COALESCE", "COLLATE", "COLLATION",
            "COLUMN", "COMMIT", "CONNECT", "CONNECTION", "CONSTRAINT", "CONSTRAINTS", "CONTINUE", "CONVERT",
            "CORRESPONDING", "COUNT", "CREATE", "CROSS", "CURRENT", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
            "CURRENT_USER", "CURSOR", "DATE", "DAY", "DEALLOCATE", "DEC", "DECIMAL", "DECLARE", "DEFAULT", "DEFERRABLE",
            "DEFERRED", "DELETE", "DESC", "DESCRIBE", "DESCRIPTOR", "DIAGNOSTICS", "DISCONNECT", "DISTINCT", "DOMAIN",
            "DOUBLE", "DROP", "ELSE", "END", "END-EXEC", "ESCAPE", "EXCEPT", "EXCEPTION", "EXEC", "EXECUTE", "EXISTS",
            "EXTERNAL", "EXTRACT", "FALSE", "FETCH", "FIRST", "FLOAT", "FOR", "FOREIGN", "FOUND", "FROM", "FULL", "GET",
            "GLOBAL", "GO", "GOTO", "GRANT", "GROUP", "HAVING", "HOUR", "IDENTITY", "IMMEDIATE", "IN", "INDICATOR",
            "INITIALLY", "INNER", "INADD", "INSENSITIVE", "INSERT", "INT", "INTEGER", "INTERSECT", "INTERVAL", "INTO",
            "IS", "ISOLATION", "JOIN", "KEY", "LANGUAGE", "LAST", "LEADING", "LEFT", "LEVEL", "LIKE", "LOCAL", "LOWER",
            "MATCH", "MAX", "MIN", "MINUTE", "MODULE", "MONTH", "NAMES", "NATIONAL", "NATURAL", "NCHAR", "NEXT", "NO",
            "NOT", "NULL", "NULLIF", "NUMERIC", "OCTET_LENGTH", "OF", "ON", "ONLY", "OPEN", "OPTION", "OR", "ORDER",
            "OUTER", "OUTADD", "OVERLAPS", "PAD", "PARTIAL", "POSITION", "PRECISION", "PREPARE", "PRESERVE", "PRIMARY",
            "PRIOR", "PRIVILEGES", "PROCEDURE", "PUBLIC", "READ", "REAL", "REFERENCES", "RELATIVE", "RESTRICT",
            "REVOKE", "RIGHT", "ROLLBACK", "ROWS", "SCHEMA", "SCROLL", "SECOND", "SECTION", "SELECT", "SESSION",
            "SESSION_USER", "SET", "SIZE", "SMALLINT", "SOME", "SPACE", "SQL", "SQLCODE", "SQLERROR", "SQLSTATE",
            "SUBSTRING", "SUM", "SYSTEM_USER", "TABLE", "TEMPORARY", "THEN", "TIME", "TIMESTAMP", "TIMEZONE_HOUR",
            "TIMEZONE_MINUTE", "TO", "TRAILING", "TRANSACTION", "TRANSLATE", "TRANSLATION", "TRIM", "TRUE", "UNION",
            "UNIQUE", "UNKNOWN", "UPDATE", "UPPER", "USAGE", "USER", "USING", "VALUE", "VALUES", "VARCHAR", "VARYING",
            "VIEW", "WHEN", "WHENEVER", "WHERE", "WITH", "WORK", "WRITE", "YEAR", "ZONE");

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
            int originStart = extractMatcher.start(0);
            int originEnd = endIdx + 1;

            replacedString = replaceString(replacedString, originString.substring(originStart, originEnd),
                    functionStr + "(" + extractInner + ")");
        }

        return replacedString;
    }

    public static String castReplace(String originString) {
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

    public static String subqueryReplace(String originString) {
        Matcher subqueryMatcher = FROM_PATTERN.matcher(originString);
        String replacedString = originString;
        Map<Integer, Integer> parenthesesPairs = null;

        while (subqueryMatcher.find()) {
            if (parenthesesPairs == null) {
                parenthesesPairs = findParenthesesPairs(originString);
            }

            int startIdx = subqueryMatcher.start(1);
            int endIdx = parenthesesPairs.get(startIdx) + 1;

            Matcher aliasMatcher = ALIAS_PATTERN.matcher(originString.substring(endIdx));
            if (aliasMatcher.find()) {
                String aliasCandidate = aliasMatcher.group(1);

                if (aliasCandidate != null && !sqlKeyWordsExceptAS.contains(aliasCandidate.toUpperCase())) {
                    continue;
                }

                replacedString = replaceString(replacedString, originString.substring(startIdx, endIdx),
                        originString.substring(startIdx, endIdx) + " as alias");
            }
        }

        return replacedString;
    }

    public static String timestampaddReplace(String originString) {
        Matcher timestampaddMatcher = TIMESTAMPADD_PATTERN.matcher(originString);
        String replacedString = originString;

        while (timestampaddMatcher.find()) {
            String interval = timestampaddMatcher.group(1);
            String timestampaddStr = replaceString(timestampaddMatcher.group(), interval, "'" + interval + "'");
            replacedString = replaceString(replacedString, timestampaddMatcher.group(), timestampaddStr);
        }

        return replacedString;
    }

    public static String concatReplace(String originString) {
        Matcher concatMatcher = CONCAT_PATTERN.matcher(originString);
        String replacedString = originString;

        while (concatMatcher.find()) {
            String leftString = concatMatcher.group(1);
            String rightString = concatMatcher.group(2);
            replacedString = replaceString(replacedString, leftString + "||" + rightString,
                    "concat(" + leftString + "," + rightString + ")");
        }

        return replacedString;
    }

    public static String doConvert(String originStr) {
        // Step1.Replace " with `
        String convertedSql = replaceString(originStr, "\"", "`");

        // Step2.Replace extract functions
        convertedSql = extractReplace(convertedSql);

        // Step3.Replace cast type string
        convertedSql = castReplace(convertedSql);

        // Step4.Replace sub query
        convertedSql = subqueryReplace(convertedSql);

        // Step5.Replace char_length with length
        convertedSql = replaceString(convertedSql, "char_length", "length");

        // Step6.Replace "||" with concat
        convertedSql = concatReplace(convertedSql);

        // Step7.Add quote for interval in timestampadd
        convertedSql = timestampaddReplace(convertedSql);

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

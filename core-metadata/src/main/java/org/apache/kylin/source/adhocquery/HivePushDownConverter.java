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
public class HivePushDownConverter implements IPushDownConverter {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(HivePushDownConverter.class);

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
    private static final ImmutableSet<String> sqlKeyWordsExceptAS = ImmutableSet.of("A", "ABS", "ABSOLUTE", "ACTION",
            "ADA", "ADD", "ADMIN", "AFTER", "ALL", "ALLOCATE", "ALLOW", "ALTER", "ALWAYS", "AND", "ANY", "APPLY", "ARE",
            "ARRAY", "ARRAY_MAX_CARDINALITY", "ASC", "ASENSITIVE", "ASSERTION", "ASSIGNMENT", "ASYMMETRIC", "AT",
            "ATOMIC", "ATTRIBUTE", "ATTRIBUTES", "AUTHORIZATION", "AVG", "BEFORE", "BEGIN", "BEGIN_FRAME",
            "BEGIN_PARTITION", "BERNOULLI", "BETWEEN", "BIGINT", "BINARY", "BIT", "BLOB", "BOOLEAN", "BOTH", "BREADTH",
            "BY", "C", "CALL", "CALLED", "CARDINALITY", "CASCADE", "CASCADED", "CASE", "CAST", "CATALOG",
            "CATALOG_NAME", "CEIL", "CEILING", "CENTURY", "CHAIN", "CHAR", "CHARACTER", "CHARACTERISTICS", "CHARACTERS",
            "CHARACTER_LENGTH", "CHARACTER_SET_CATALOG", "CHARACTER_SET_NAME", "CHARACTER_SET_SCHEMA", "CHAR_LENGTH",
            "CHECK", "CLASSIFIER", "CLASS_ORIGIN", "CLOB", "CLOSE", "COALESCE", "COBOL", "COLLATE", "COLLATION",
            "COLLATION_CATALOG", "COLLATION_NAME", "COLLATION_SCHEMA", "COLLECT", "COLUMN", "COLUMN_NAME",
            "COMMAND_FUNCTION", "COMMAND_FUNCTION_CODE", "COMMIT", "COMMITTED", "CONDITION", "CONDITION_NUMBER",
            "CONNECT", "CONNECTION", "CONNECTION_NAME", "CONSTRAINT", "CONSTRAINTS", "CONSTRAINT_CATALOG",
            "CONSTRAINT_NAME", "CONSTRAINT_SCHEMA", "CONSTRUCTOR", "CONTAINS", "CONTINUE", "CONVERT", "CORR",
            "CORRESPONDING", "COUNT", "COVAR_POP", "COVAR_SAMP", "CREATE", "CROSS", "CUBE", "CUME_DIST", "CURRENT",
            "CURRENT_CATALOG", "CURRENT_DATE", "CURRENT_DEFAULT_TRANSFORM_GROUP", "CURRENT_PATH", "CURRENT_ROLE",
            "CURRENT_ROW", "CURRENT_SCHEMA", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_TRANSFORM_GROUP_FOR_TYPE",
            "CURRENT_USER", "CURSOR", "CURSOR_NAME", "CYCLE", "DATA", "DATABASE", "DATE", "DATETIME_INTERVAL_CODE",
            "DATETIME_INTERVAL_PRECISION", "DAY", "DEALLOCATE", "DEC", "DECADE", "DECIMAL", "DECLARE", "DEFAULT",
            "DEFAULTS", "DEFERRABLE", "DEFERRED", "DEFINE", "DEFINED", "DEFINER", "DEGREE", "DELETE", "DENSE_RANK",
            "DEPTH", "DEREF", "DERIVED", "DESC", "DESCRIBE", "DESCRIPTION", "DESCRIPTOR", "DETERMINISTIC",
            "DIAGNOSTICS", "DISALLOW", "DISCONNECT", "DISPATCH", "DISTINCT", "DOMAIN", "DOUBLE", "DOW", "DOY", "DROP",
            "DYNAMIC", "DYNAMIC_FUNCTION", "DYNAMIC_FUNCTION_CODE", "EACH", "ELEMENT", "ELSE", "EMPTY", "END",
            "END-EXEC", "END_FRAME", "END_PARTITION", "EPOCH", "EQUALS", "ESCAPE", "EVERY", "EXCEPT", "EXCEPTION",
            "EXCLUDE", "EXCLUDING", "EXEC", "EXECUTE", "EXISTS", "EXP", "EXPLAIN", "EXTEND", "EXTERNAL", "EXTRACT",
            "FALSE", "FETCH", "FILTER", "FINAL", "FIRST", "FIRST_VALUE", "FLOAT", "FLOOR", "FOLLOWING", "FOR",
            "FOREIGN", "FORTRAN", "FOUND", "FRAC_SECOND", "FRAME_ROW", "FREE", "FROM", "FULL", "FUNCTION", "FUSION",
            "G", "GENERAL", "GENERATED", "GET", "GLOBAL", "GO", "GOTO", "GRANT", "GRANTED", "GROUP", "GROUPING",
            "GROUPS", "HAVING", "HIERARCHY", "HOLD", "HOUR", "IDENTITY", "IMMEDIATE", "IMMEDIATELY", "IMPLEMENTATION",
            "IMPORT", "IN", "INCLUDING", "INCREMENT", "INDICATOR", "INITIAL", "INITIALLY", "INNER", "INOUT", "INPUT",
            "INSENSITIVE", "INSERT", "INSTANCE", "INSTANTIABLE", "INT", "INTEGER", "INTERSECT", "INTERSECTION",
            "INTERVAL", "INTO", "INVOKER", "IS", "ISOLATION", "JAVA", "JOIN", "JSON", "K", "KEY", "KEY_MEMBER",
            "KEY_TYPE", "LABEL", "LAG", "LANGUAGE", "LARGE", "LAST", "LAST_VALUE", "LATERAL", "LEAD", "LEADING", "LEFT",
            "LENGTH", "LEVEL", "LIBRARY", "LIKE", "LIKE_REGEX", "LIMIT", "LN", "LOCAL", "LOCALTIME", "LOCALTIMESTAMP",
            "LOCATOR", "LOWER", "M", "MAP", "MATCH", "MATCHED", "MATCHES", "MATCH_NUMBER", "MATCH_RECOGNIZE", "MAX",
            "MAXVALUE", "MEASURES", "MEMBER", "MERGE", "MESSAGE_LENGTH", "MESSAGE_OCTET_LENGTH", "MESSAGE_TEXT",
            "METHOD", "MICROSECOND", "MILLENNIUM", "MIN", "MINUS", "MINUTE", "MINVALUE", "MOD", "MODIFIES", "MODULE",
            "MONTH", "MORE", "MULTISET", "MUMPS", "NAME", "NAMES", "NATIONAL", "NATURAL", "NCHAR", "NCLOB", "NESTING",
            "NEW", "NEXT", "NO", "NONE", "NORMALIZE", "NORMALIZED", "NOT", "NTH_VALUE", "NTILE", "NULL", "NULLABLE",
            "NULLIF", "NULLS", "NUMBER", "NUMERIC", "OBJECT", "OCCURRENCES_REGEX", "OCTETS", "OCTET_LENGTH", "OF",
            "OFFSET", "OLD", "OMIT", "ON", "ONE", "ONLY", "OPEN", "OPTION", "OPTIONS", "OR", "ORDER", "ORDERING",
            "ORDINALITY", "OTHERS", "OUT", "OUTER", "OUTPUT", "OVER", "OVERLAPS", "OVERLAY", "OVERRIDING", "PAD",
            "PARAMETER", "PARAMETER_MODE", "PARAMETER_NAME", "PARAMETER_ORDINAL_POSITION", "PARAMETER_SPECIFIC_CATALOG",
            "PARAMETER_SPECIFIC_NAME", "PARAMETER_SPECIFIC_SCHEMA", "PARTIAL", "PARTITION", "PASCAL", "PASSTHROUGH",
            "PAST", "PATH", "PATTERN", "PER", "PERCENT", "PERCENTILE_CONT", "PERCENTILE_DISC", "PERCENT_RANK", "PERIOD",
            "PERMUTE", "PLACING", "PLAN", "PLI", "PORTION", "POSITION", "POSITION_REGEX", "POWER", "PRECEDES",
            "PRECEDING", "PRECISION", "PREPARE", "PRESERVE", "PREV", "PRIMARY", "PRIOR", "PRIVILEGES", "PROCEDURE",
            "PUBLIC", "QUARTER", "RANGE", "RANK", "READ", "READS", "REAL", "RECURSIVE", "REF", "REFERENCES",
            "REFERENCING", "REGR_AVGX", "REGR_AVGY", "REGR_COUNT", "REGR_INTERCEPT", "REGR_R2", "REGR_SLOPE",
            "REGR_SXX", "REGR_SXY", "REGR_SYY", "RELATIVE", "RELEASE", "REPEATABLE", "REPLACE", "RESET", "RESTART",
            "RESTRICT", "RESULT", "RETURN", "RETURNED_CARDINALITY", "RETURNED_LENGTH", "RETURNED_OCTET_LENGTH",
            "RETURNED_SQLSTATE", "RETURNS", "REVOKE", "RIGHT", "ROLE", "ROLLBACK", "ROLLUP", "ROUTINE",
            "ROUTINE_CATALOG", "ROUTINE_NAME", "ROUTINE_SCHEMA", "ROW", "ROWS", "ROW_COUNT", "ROW_NUMBER", "RUNNING",
            "SAVEPOINT", "SCALE", "SCHEMA", "SCHEMA_NAME", "SCOPE", "SCOPE_CATALOGS", "SCOPE_NAME", "SCOPE_SCHEMA",
            "SCROLL", "SEARCH", "SECOND", "SECTION", "SECURITY", "SEEK", "SELECT", "SELF", "SENSITIVE", "SEQUENCE",
            "SERIALIZABLE", "SERVER", "SERVER_NAME", "SESSION", "SESSION_USER", "SET", "SETS", "SHOW", "SIMILAR",
            "SIMPLE", "SIZE", "SKIP", "SMALLINT", "SOME", "SOURCE", "SPACE", "SPECIFIC", "SPECIFICTYPE",
            "SPECIFIC_NAME", "SQL", "SQLEXCEPTION", "SQLSTATE", "SQLWARNING", "SQL_BIGINT", "SQL_BINARY", "SQL_BIT",
            "SQL_BLOB", "SQL_BOOLEAN", "SQL_CHAR", "SQL_CLOB", "SQL_DATE", "SQL_DECIMAL", "SQL_DOUBLE", "SQL_FLOAT",
            "SQL_INTEGER", "SQL_INTERVAL_DAY", "SQL_INTERVAL_DAY_TO_HOUR", "SQL_INTERVAL_DAY_TO_MINUTE",
            "SQL_INTERVAL_DAY_TO_SECOND", "SQL_INTERVAL_HOUR", "SQL_INTERVAL_HOUR_TO_MINUTE",
            "SQL_INTERVAL_HOUR_TO_SECOND", "SQL_INTERVAL_MINUTE", "SQL_INTERVAL_MINUTE_TO_SECOND", "SQL_INTERVAL_MONTH",
            "SQL_INTERVAL_SECOND", "SQL_INTERVAL_YEAR", "SQL_INTERVAL_YEAR_TO_MONTH", "SQL_LONGVARBINARY",
            "SQL_LONGVARCHAR", "SQL_LONGVARNCHAR", "SQL_NCHAR", "SQL_NCLOB", "SQL_NUMERIC", "SQL_NVARCHAR", "SQL_REAL",
            "SQL_SMALLINT", "SQL_TIME", "SQL_TIMESTAMP", "SQL_TINYINT", "SQL_TSI_DAY", "SQL_TSI_FRAC_SECOND",
            "SQL_TSI_HOUR", "SQL_TSI_MICROSECOND", "SQL_TSI_MINUTE", "SQL_TSI_MONTH", "SQL_TSI_QUARTER",
            "SQL_TSI_SECOND", "SQL_TSI_WEEK", "SQL_TSI_YEAR", "SQL_VARBINARY", "SQL_VARCHAR", "SQRT", "START", "STATE",
            "STATEMENT", "STATIC", "STDDEV_POP", "STDDEV_SAMP", "STREAM", "STRUCTURE", "STYLE", "SUBCLASS_ORIGIN",
            "SUBMULTISET", "SUBSET", "SUBSTITUTE", "SUBSTRING", "SUBSTRING_REGEX", "SUCCEEDS", "SUM", "SYMMETRIC",
            "SYSTEM", "SYSTEM_TIME", "SYSTEM_USER", "TABLE", "TABLESAMPLE", "TABLE_NAME", "TEMPORARY", "THEN", "TIES",
            "TIME", "TIMESTAMP", "TIMESTAMPADD", "TIMESTAMPDIFF", "TIMEZONE_HOUR", "TIMEZONE_MINUTE", "TINYINT", "TO",
            "TOP_LEVEL_COUNT", "TRAILING", "TRANSACTION", "TRANSACTIONS_ACTIVE", "TRANSACTIONS_COMMITTED",
            "TRANSACTIONS_ROLLED_BACK", "TRANSFORM", "TRANSFORMS", "TRANSLATE", "TRANSLATE_REGEX", "TRANSLATION",
            "TREAT", "TRIGGER", "TRIGGER_CATALOG", "TRIGGER_NAME", "TRIGGER_SCHEMA", "TRIM", "TRIM_ARRAY", "TRUE",
            "TRUNCATE", "TYPE", "UESCAPE", "UNBOUNDED", "UNCOMMITTED", "UNDER", "UNION", "UNIQUE", "UNKNOWN", "UNNAMED",
            "UNNEST", "UPDATE", "UPPER", "UPSERT", "USAGE", "USER", "USER_DEFINED_TYPE_CATALOG",
            "USER_DEFINED_TYPE_CODE", "USER_DEFINED_TYPE_NAME", "USER_DEFINED_TYPE_SCHEMA", "USING", "VALUE", "VALUES",
            "VALUE_OF", "VARBINARY", "VARCHAR", "VARYING", "VAR_POP", "VAR_SAMP", "VERSION", "VERSIONING", "VIEW",
            "WEEK", "WHEN", "WHENEVER", "WHERE", "WIDTH_BUCKET", "WINDOW", "WITH", "WITHIN", "WITHOUT", "WORK",
            "WRAPPER", "WRITE", "XML", "YEAR", "ZONE");

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

        // Step8.Replace integer with int
        convertedSql = replaceString(convertedSql, "INTEGER", "INT");
        convertedSql = replaceString(convertedSql, "integer", "int");

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
    public String convert(String originSql, String project, String defaultSchema) {
        return doConvert(originSql);
    }
}

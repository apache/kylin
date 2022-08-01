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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class EscapeFunction {

    private static final String SUBSTRING_FUNCTION = "SUBSTRING";

    private static final String CEIL_EXCEPTION_MSG = "ceil function only support ceil(numeric) or ceil(datetime to timeunit)";
    private static final String FLOOR_EXCEPTION_MSG = "ceil function only support ceil(numeric) or ceil(datetime to timeunit)";
    private static final String SUBSTRING_EXCEPTION_MSG = "substring/substr only support substring(col from start for len) or substring(col from start)";
    private static final String SUBSTR_EXCEPTION_MSG = "substring/substr only support substr(col from start for len) or substr(col from start)";
    private static final String OVERLAY_EXCEPTION_MSG = "overlay only support overlay(exp1 placing exp2 from start for len) or overlay(exp1 placing exp2 from start)";

    // Present as normal function: "func(arg1, arg2, ...)"
    public static String normalFN(String functionName, String[] args) {
        return String.format(Locale.ROOT, "%s(%s)", functionName, String.join(", ", args));
    }

    // Present as JDBC/ODBC scalar function: "{ fn func(arg1, arg2, ...) }"
    public static String scalarFN(String functionName, String[] args) {
        return String.format(Locale.ROOT, "{fn %s(%s)}", functionName, String.join(", ", args));
    }

    private static void checkArgs(String[] args, int expectedCount) {
        int actualCount = 0;
        if (args != null) {
            actualCount = args.length;
        }
        if (actualCount != expectedCount) {
            throw new IllegalArgumentException(
                    "Bad arguments count, expected count " + expectedCount + " but actual " + actualCount);
        }
    }

    /* Function conversion implementations */
    /*
     * Notice: Prefix "FN_" means converting to {fn ...} format
     */
    public enum FnConversion {
        LEFT(args -> {
            checkArgs(args, 2);
            String[] newArgs = new String[] { args[0], "1", args[1] };
            return normalFN(SUBSTRING_FUNCTION, newArgs);
        }),

        RIGHT(args -> {
            checkArgs(args, 2);
            String origStrRef = args[0];
            String rightOffset = args[1];
            String[] newArgs = new String[] { origStrRef, "CHAR_LENGTH(" + origStrRef + ") + 1 - " + rightOffset,
                    "" + rightOffset };
            return normalFN(SUBSTRING_FUNCTION, newArgs);
        }),

        TRIM(args -> {
            checkArgs(args, 1);
            String[] newArgs = new String[] { "both " + args[0] };
            return normalFN("TRIM", newArgs);
        }),

        LTRIM(args -> {
            checkArgs(args, 1);
            String[] newArgs = new String[] { "leading " + args[0] };
            return normalFN("TRIM", newArgs);
        }),

        RTRIM(args -> {
            checkArgs(args, 1);
            String[] newArgs = new String[] { "trailing " + args[0] };
            return normalFN("TRIM", newArgs);
        }),

        FN_LENGTH(args -> {
            checkArgs(args, 1);
            return scalarFN("LENGTH", args);
        }),

        LENGTH(args -> {
            checkArgs(args, 1);
            return normalFN("LENGTH", args);
        }),

        CONVERT(args -> {
            checkArgs(args, 2);
            String value = args[0];
            String sqlType = args[1].toUpperCase(Locale.ROOT);
            String sqlPrefix = "SQL_";
            if (sqlType.startsWith(sqlPrefix)) {
                sqlType = sqlType.substring(sqlPrefix.length());
            }
            switch (sqlType) {
            case "WVARCHAR":
            case "CHAR":
            case "WCHAR":
                sqlType = "VARCHAR";
                break;
            default:
                break;
            }
            String[] newArgs = new String[] { value + " AS " + sqlType };
            return normalFN("CAST", newArgs);
        }),

        LCASE(args -> {
            checkArgs(args, 1);
            return normalFN("LOWER", args);
        }),

        UCASE(args -> {
            checkArgs(args, 1);
            return normalFN("UPPER", args);
        }),

        LOG(args -> {
            checkArgs(args, 1);
            return normalFN("LN", args);
        }),

        EXTRACT(args -> {
            checkArgs(args, 3);
            String function = args[0];
            if (function.equalsIgnoreCase("DAY")) {
                function = "DAYOFMONTH";
            } else if (function.equalsIgnoreCase("DOW")) {
                function = "DAYOFWEEK";
            } else if (function.equalsIgnoreCase("DOY")) {
                function = "DAYOFYEAR";
            }
            return normalFN(function, new String[] { args[2] });
        }),

        CURRENT_DATE(args -> {
            checkArgs(args, 0);
            return "CURRENT_DATE";
        }),

        CURRENT_TIME(args -> {
            checkArgs(args, 0);
            return "CURRENT_TIME";
        }),

        CURRENT_TIMESTAMP(args -> {
            if (args == null || args.length > 1) {
                throw new IllegalArgumentException("Bad arguments");
            }
            return "CURRENT_TIMESTAMP";
        }),

        WEEK_CALCITE(args -> {
            checkArgs(args, 1);
            return normalFN("WEEK", args);
        }),

        WEEK_SPARK(args -> {
            checkArgs(args, 1);
            return normalFN("WEEKOFYEAR", args);
        }),

        YEAR(args -> {
            checkArgs(args, 1);
            return normalFN("YEAR", args);
        }),

        QUARTER(args -> {
            checkArgs(args, 1);
            return normalFN("QUARTER", args);
        }),

        MONTH(args -> {
            checkArgs(args, 1);
            return normalFN("MONTH", args);
        }),

        DAYOFMONTH(args -> {
            checkArgs(args, 1);
            return normalFN("DAYOFMONTH", args);
        }),

        DAYOFWEEK(args -> {
            checkArgs(args, 1);
            return normalFN("DAYOFWEEK", args);
        }),

        DAYOFYEAR(args -> {
            checkArgs(args, 1);
            return normalFN("DAYOFYEAR", args);
        }),

        HOUR(args -> {
            checkArgs(args, 1);
            return normalFN("HOUR", args);
        }),

        MINUTE(args -> {
            checkArgs(args, 1);
            return normalFN("MINUTE", args);
        }),

        SECOND(args -> {
            checkArgs(args, 1);
            return normalFN("SECOND", args);
        }),

        TRUNCATE_NUM(args -> {
            checkArgs(args, 2);
            return normalFN("TRUNCATE_NUM", args);
        }),

        TIMESTAMPADD(args -> {
            checkArgs(args, 3);
            return normalFN("TIMESTAMPADD", args);
        }),

        TIMESTAMPDIFF(args -> {
            checkArgs(args, 3);
            return normalFN("TIMESTAMPDIFF", args);
        }),

        SUSTRING(args -> {
            Preconditions.checkArgument(args.length == 2 || args.length == 3, EscapeFunction.SUBSTRING_EXCEPTION_MSG);
            return normalFN(SUBSTRING_FUNCTION, args);
        }),

        SUSTR(args -> {
            Preconditions.checkArgument(args.length == 2 || args.length == 3, EscapeFunction.SUBSTR_EXCEPTION_MSG);
            return normalFN(SUBSTRING_FUNCTION, args);
        }),

        OVERLAY_SPARK(args -> {
            Preconditions.checkArgument(args.length == 3 || args.length == 4, EscapeFunction.OVERLAY_EXCEPTION_MSG);
            List<String> newArgs = Lists.newArrayList();
            newArgs.add(String.format(Locale.ROOT, "SUBSTRING(%s, %s, %s - 1)", args[0], 0, args[2]));
            newArgs.add(args[1]);
            if (args.length == 3) {
                newArgs.add(
                        String.format(Locale.ROOT, "SUBSTRING(%s, %s + CHAR_LENGTH(%s))", args[0], args[2], args[1]));
            } else {
                newArgs.add(String.format(Locale.ROOT, "SUBSTRING(%s, %s + %s)", args[0], args[2], args[3]));
            }
            return normalFN("CONCAT", newArgs.toArray(new String[0]));
        }),

        OVERLAY(args -> {
            Preconditions.checkArgument(args.length == 3 || args.length == 4, EscapeFunction.OVERLAY_EXCEPTION_MSG);
            String newArg;
            if (args.length == 3) {
                newArg = String.format(Locale.ROOT, "%s PLACING %s FROM %s", args[0], args[1], args[2]);
            } else {
                newArg = String.format(Locale.ROOT, "%s PLACING %s FROM %s for %s", args[0], args[1], args[2], args[3]);
            }
            return normalFN("OVERLAY", new String[] { newArg });
        }),

        GROUPING(args -> {
            Preconditions.checkArgument(args.length > 0);
            return normalFN("GROUPING", args);
        }),

        SETS_SPARK(args -> {
            // group by grouping sets(...)
            Preconditions.checkArgument(args.length > 0);
            String groupByColumns = args[0].trim();
            if (groupByColumns.startsWith("(") && groupByColumns.endsWith(")")) {
                groupByColumns = groupByColumns.substring(1, groupByColumns.length() - 1);
            }
            return String.format(Locale.ROOT, "%s grouping sets(%s)", groupByColumns, String.join(",", args));
        }),

        SETS(args -> {
            // group by grouping sets(...)
            Preconditions.checkArgument(args.length > 0);
            return String.format(Locale.ROOT, "grouping sets(%s)", String.join(",", args));
        }),

        CEIL2(args -> {
            Preconditions.checkArgument(args.length == 1 || args.length == 2, EscapeFunction.CEIL_EXCEPTION_MSG);
            if (args.length == 1) {
                return normalFN("CEIL", args);
            } else {
                String[] newArgs = new String[] { args[0], "'" + args[1].toUpperCase(Locale.ROOT) + "'" };
                return normalFN("CEIL_DATETIME", newArgs);
            }
        }),

        CEIL(args -> {
            Preconditions.checkArgument(args.length == 1 || args.length == 2, EscapeFunction.CEIL_EXCEPTION_MSG);
            String[] newArgs = args.length == 1 ? args : new String[] { args[0] + " to " + args[1] };
            return normalFN("CEIL", newArgs);
        }),

        FLOOR2(args -> {
            Preconditions.checkArgument(args.length == 1 || args.length == 2, EscapeFunction.FLOOR_EXCEPTION_MSG);
            if (args.length == 1) {
                return normalFN("FLOOR", args);
            } else {
                String[] newArgs = new String[] { args[0].trim(), "'" + args[1].toUpperCase(Locale.ROOT) + "'" };
                return normalFN("FLOOR_DATETIME", newArgs);
            }
        }),

        FLOOR(args -> {
            Preconditions.checkArgument(args.length == 1 || args.length == 2, EscapeFunction.FLOOR_EXCEPTION_MSG);
            String[] newArgs = args.length == 1 ? args : new String[] { args[0] + " to " + args[1] };
            return normalFN("FLOOR", newArgs);
        }),

        // tableau func
        SPACE(args -> {
            checkArgs(args, 1);
            return normalFN("SPACE", args);
        }),

        CHR(args -> {
            checkArgs(args, 1);
            return normalFN("CHR", args);
        }),

        ASCII(args -> {
            checkArgs(args, 1);
            return normalFN("ASCII", args);
        });

        private final IConvert innerCvt;

        FnConversion(IConvert convert) {
            this.innerCvt = convert;
        }

        public String convert(String[] args) {
            return innerCvt.convert(args);
        }

        private interface IConvert {
            String convert(String[] args);
        }
    }
}

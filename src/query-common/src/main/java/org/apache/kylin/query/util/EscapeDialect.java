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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.calcite.avatica.util.Quoting;
import org.apache.kylin.query.util.EscapeFunction.FnConversion;

public abstract class EscapeDialect {

    private static final String FN_LENGTH_ALIAS = "CHAR_LENGTH";
    private static final String FN_WEEK = "WEEK";
    private static final String FN_CEIL = "CEIL";
    private static final String FN_FLOOR = "FLOOR";
    private static final String FN_SUBSTR = "SUBSTR";
    private static final String FN_SUBSTRING = "SUBSTRING";
    private static final String FN_ASCII = "ASCII";
    private static final String FN_CHR = "CHR";
    private static final String FN_SPACE = "SPACE";

    /* Define SQL dialect for different data source */

    /**
     * CALCITE (CUBE)
     */
    public static final EscapeDialect CALCITE = new EscapeDialect() {

        @Override
        public void init() {
            registerAll(FnConversion.LEFT, //
                    FnConversion.RIGHT, //
                    FnConversion.CURRENT_DATE, //
                    FnConversion.CURRENT_TIME, //
                    FnConversion.CURRENT_TIMESTAMP, //
                    FnConversion.CONVERT, //
                    FnConversion.OVERLAY, //
                    FnConversion.SETS, //
                    FnConversion.GROUPING, //
                    FnConversion.TRIM, //
                    FnConversion.TIMESTAMPADD, //
                    FnConversion.TIMESTAMPDIFF, //
                    FnConversion.YEAR, //
                    FnConversion.QUARTER, //
                    FnConversion.MONTH, //
                    FnConversion.DAYOFMONTH, //
                    FnConversion.DAYOFYEAR, //
                    FnConversion.DAYOFWEEK, //
                    FnConversion.HOUR, //
                    FnConversion.MINUTE, //
                    FnConversion.SECOND //
            );

            register(FN_LENGTH_ALIAS, FnConversion.FN_LENGTH);
            register(FN_WEEK, FnConversion.WEEK_CALCITE);
            register(FN_CEIL, FnConversion.CEIL);
            register(FN_FLOOR, FnConversion.FLOOR);
            register(FN_SUBSTR, FnConversion.SUSTR);
            register(FN_SUBSTRING, FnConversion.SUSTRING);

            register(FN_ASCII, FnConversion.ASCII);
            register(FN_CHR, FnConversion.CHR);
            register(FN_SPACE, FnConversion.SPACE);
        }

        @Override
        public String defaultConversion(String functionName, String[] args) {
            return EscapeFunction.scalarFN(functionName, args);
        }

        @Override
        public String transformDataType(String type) {
            return type.equalsIgnoreCase("STRING") ? "VARCHAR" : type;
        }
    };

    /**
     * SPARK SQL
     */
    public static final EscapeDialect SPARK_SQL = new EscapeDialect() {

        @Override
        public void init() {
            registerAll(FnConversion.LEFT, //
                    FnConversion.RIGHT, //
                    FnConversion.CONVERT, //
                    FnConversion.GROUPING, //
                    FnConversion.LOG, //
                    FnConversion.CURRENT_DATE, //
                    FnConversion.CURRENT_TIME, //
                    FnConversion.CURRENT_TIMESTAMP, //
                    FnConversion.TIMESTAMPADD, //
                    FnConversion.TIMESTAMPDIFF, //
                    FnConversion.YEAR, //
                    FnConversion.QUARTER, //
                    FnConversion.MONTH, //
                    FnConversion.DAYOFMONTH, //
                    FnConversion.DAYOFYEAR, //
                    FnConversion.DAYOFWEEK, //
                    FnConversion.HOUR, //
                    FnConversion.MINUTE, //
                    FnConversion.SECOND, //
                    FnConversion.TRIM //
            );

            register(FN_WEEK, FnConversion.WEEK_SPARK);
            register(FN_LENGTH_ALIAS, FnConversion.LENGTH);
            register(FN_CEIL, FnConversion.CEIL2);
            register(FN_FLOOR, FnConversion.FLOOR2);
            register(FN_SUBSTR, FnConversion.SUSTR);
            register(FN_SUBSTRING, FnConversion.SUSTRING);
            register("OVERLAY", FnConversion.OVERLAY_SPARK);
            register("SETS", FnConversion.SETS_SPARK);

            register(FN_ASCII, FnConversion.ASCII);
            register(FN_CHR, FnConversion.CHR);
            register(FN_SPACE, FnConversion.SPACE);
        }

        @Override
        public String defaultConversion(String functionName, String[] args) {
            return EscapeFunction.normalFN(functionName, args);
        }

        @Override
        public String transformDoubleQuoteString(String input) {
            if (!input.startsWith("\"")) {
                return input;
            }
            return Quoting.BACK_TICK.string + input.substring(1, input.length() - 1) + Quoting.BACK_TICK.string;
        }

        @Override
        public String transformTimeunitOfTimestampDiffOrAdd(String timeunit) {
            return "'" + timeunit + "'";
        }

        @Override
        public String transformDataType(String type) {
            if (type.equalsIgnoreCase("INTEGER")) {
                return type.substring(0, 3);
            } else if (type.equalsIgnoreCase("WVARCHAR")) {
                return type.substring(1);
            } else if (type.equalsIgnoreCase("SQL_WVARCHAR")) {
                return type.substring(5);
            }
            return type;
        }

        @Override
        public String transformNiladicFunction(String funcName) {
            return String.format(Locale.ROOT, "%s()", funcName);
        }
    };

    public static final EscapeDialect HIVE = new EscapeDialect() {

        @Override
        public void init() {
            registerAll(FnConversion.LEFT, //
                    FnConversion.RIGHT, //
                    FnConversion.CONVERT, //
                    FnConversion.GROUPING, //
                    FnConversion.LOG, //
                    FnConversion.CURRENT_DATE, //
                    FnConversion.CURRENT_TIME, //
                    FnConversion.CURRENT_TIMESTAMP, //
                    FnConversion.TIMESTAMPADD, //
                    FnConversion.TIMESTAMPDIFF, //
                    FnConversion.YEAR, //
                    FnConversion.QUARTER, //
                    FnConversion.MONTH, //
                    FnConversion.DAYOFMONTH, //
                    FnConversion.DAYOFYEAR, //
                    FnConversion.DAYOFWEEK, //
                    FnConversion.HOUR, //
                    FnConversion.MINUTE, //
                    FnConversion.SECOND //
            );

            register(FN_LENGTH_ALIAS, FnConversion.LENGTH);
            register(FN_WEEK, FnConversion.WEEK_SPARK);
            register("TRUNCATE", FnConversion.TRUNCATE_NUM);
            register(FN_CEIL, FnConversion.CEIL2);
            register(FN_FLOOR, FnConversion.FLOOR2);
            register(FN_SUBSTR, FnConversion.SUSTR);
            register(FN_SUBSTRING, FnConversion.SUSTRING);
            register("OVERLAY", FnConversion.OVERLAY_SPARK);
            register("SETS", FnConversion.SETS_SPARK);
        }

        @Override
        public String defaultConversion(String functionName, String[] args) {
            return EscapeFunction.normalFN(functionName, args);
        }

        @Override
        public String transformDoubleQuoteString(String input) {
            if (!input.startsWith("\"")) {
                return input;
            }
            return Quoting.BACK_TICK.string + input.substring(1, input.length() - 1) + Quoting.BACK_TICK.string;
        }

        @Override
        public String transformTimeunitOfTimestampDiffOrAdd(String timeunit) {
            return "'" + timeunit + "'";
        }
    };

    public static final EscapeDialect DEFAULT = CALCITE; // Default dialect is CALCITE

    /**
     * base of function dialects
     */

    private final Map<String, FnConversion> registeredFunction = new HashMap<>();

    public EscapeDialect() {
        init();
    }

    public abstract void init();

    public abstract String defaultConversion(String functionName, String[] args);

    public String transformDoubleQuoteString(String input) {
        return input;
    }

    public String transformTimeunitOfTimestampDiffOrAdd(String timeunit) {
        return timeunit;
    }

    public String transformDataType(String type) {
        return type;
    }

    public String transformFN(String functionName, String[] args) {
        FnConversion fnType = registeredFunction.get(functionName.toUpperCase(Locale.ROOT));
        if (fnType != null) {
            return fnType.convert(args);
        } else {
            return defaultConversion(functionName, args);
        }
    }

    public void registerAll(FnConversion... fnTypes) {
        Arrays.stream(fnTypes).forEach(this::register);
    }

    public void register(FnConversion fnType) {
        register(fnType.name(), fnType);
    }

    // Support register function with different names
    public void register(String fnAlias, FnConversion fnType) {
        if (fnType == null) {
            return;
        }
        registeredFunction.put(fnAlias, fnType);
    }

    public String transformNiladicFunction(String funcName) {
        return funcName;
    }
}

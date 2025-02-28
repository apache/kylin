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

package org.apache.kylin.query.udf;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

import org.apache.calcite.adapter.enumerable.CallImplementor;
import org.apache.calcite.adapter.enumerable.UdfMethodNameImplementor;
import org.apache.calcite.linq4j.function.Parameter;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.fun.udf.UdfDef;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.kylin.common.KylinVersion;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.TimeUtil;

public class KylinOtherUDF {

    public String TO_CHAR(@Parameter(name = "date") Timestamp date, @Parameter(name = "part") String part) {
        String partOfDate = null;
        switch (part.toUpperCase(Locale.ROOT)) {
        case "YEAR":
            partOfDate = date.toString().substring(0, 4);
            break;
        case "MONTH":
            partOfDate = date.toString().substring(5, 7);
            break;
        case "DAY":
            partOfDate = date.toString().substring(8, 10);
            break;
        case "HOUR":
            partOfDate = date.toString().substring(11, 13);
            break;
        case "MINUTE":
        case "MINUTES":
            partOfDate = date.toString().substring(14, 16);
            break;
        case "SECOND":
        case "SECONDS":
            partOfDate = date.toString().substring(17, 19);
            break;
        default:
            //throws
        }
        return partOfDate;
    }

    public String TO_CHAR(@Parameter(name = "date") Date date, @Parameter(name = "part") String part) {
        return TO_CHAR(new Timestamp(date.getTime()), part);
    }

    public String _ymdint_between(@Parameter(name = "date1") Object date1, @Parameter(name = "date2") Object date2) {
        try {
            long d1 = DateUtils.parseDate(date1.toString(), DateFormat.DEFAULT_DATE_PATTERN).getTime();
            long d2 = DateUtils.parseDate(date2.toString(), DateFormat.DEFAULT_DATE_PATTERN).getTime();
            return TimeUtil.ymdintBetween(d1, d2);
        } catch (ParseException e) {
            throw new IllegalArgumentException("Only dates in yyyy-MM-dd format are supported!", e);
        }
    }

    public Boolean MASSIN(@Parameter(name = "col") Object col, @Parameter(name = "filterTable") String filterTable) {
        return true;
    }

    public String VERSION() {
        return KylinVersion.getCurrentVersion().toString();
    }

    public String SPLIT_PART(@Parameter(name = "str") String str, @Parameter(name = "regex") String regex,
            @Parameter(name = "index") int index) {
        String[] parts = str.split(regex);
        if (index - 1 < parts.length && index > 0) {
            return parts[index - 1];
        } else if (index < 0 && Math.abs(index) <= parts.length) {
            return parts[parts.length + index];
        } else {
            return null;
        }
    }

    public Integer INSTR(@Parameter(name = "str") String s1, @Parameter(name = "subStr") String s2) {
        return s1.indexOf(s2) + 1;
    }

    public int INSTR(@Parameter(name = "str") String s1, @Parameter(name = "subStr") String s2,
            @Parameter(name = "position") int p) {
        return s1.indexOf(s2, p - 1) + 1;
    }

    public int STRPOS(@Parameter(name = "str1") String s1, @Parameter(name = "str2") String s2) {
        return s1.indexOf(s2) + 1;
    }

    public static class ConcatWithNull implements UdfDef {

        private static final String FUNC_NAME = "CONCAT";

        private ConcatWithNull() {
            throw new IllegalStateException("Utility class");
        }

        public static final SqlFunction OPERATOR = new SqlFunction(new SqlIdentifier(FUNC_NAME, SqlParserPos.ZERO),
                ReturnTypes.MULTIVALENT_STRING_SUM_PRECISION_NULLABLE, InferTypes.ANY_NULLABLE,
                OperandTypes.repeat(SqlOperandCountRanges.from(0), OperandTypes.ANY), null,
                SqlFunctionCategory.USER_DEFINED_FUNCTION);

        public static final CallImplementor IMPLEMENTOR = new UdfMethodNameImplementor(
                FUNC_NAME.toLowerCase(Locale.ROOT), ConcatWithNull.class);

        public static String concat(Object... args) {
            if (args == null) {
                return null;
            }

            return Arrays.stream(args).map(String::valueOf).collect(Collectors.joining());
        }
    }

}

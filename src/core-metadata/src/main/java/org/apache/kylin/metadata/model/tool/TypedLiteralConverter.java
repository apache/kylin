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

package org.apache.kylin.metadata.model.tool;

import java.math.BigDecimal;

import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.apache.kylin.metadata.model.DataType;

public class TypedLiteralConverter {
    private static final String NULL = "KY_LITERAL_NULL";

    public static Object stringValueToTypedValue(String value, DataType dataType) {
        if (value.equals(NULL)) {
            return null;
        }
        switch (dataType.getTypeName()) {
        case CHAR:
        case VARCHAR:
            return value;
        case DECIMAL:
            return new BigDecimal(value);
        case BIGINT:
            return Long.parseLong(value);
        case SMALLINT:
            return Short.parseShort(value);
        case TINYINT:
            return Byte.parseByte(value);
        case INTEGER:
            return Integer.parseInt(value);
        case DOUBLE:
            return Double.parseDouble(value);
        case FLOAT:
        case REAL:
            return Float.parseFloat(value);
        case DATE:
            return DateString.fromDaysSinceEpoch(Integer.parseInt(value));
        case TIME:
        case TIME_WITH_LOCAL_TIME_ZONE:
            return TimeString.fromMillisOfDay(Integer.parseInt(value));
        case TIMESTAMP:
        case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            return TimestampString.fromMillisSinceEpoch(Long.parseLong(value));
        case BOOLEAN:
            return Boolean.parseBoolean(value);
        default:
            return value;
        }
    }

    public static String typedLiteralToString(RexLiteral literal) {
        if (literal.getValue3() == null) {
            return NULL;
        }
        return String.valueOf(literal.getValue3());
    }
}

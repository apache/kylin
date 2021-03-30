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

package org.apache.kylin.common.util;

import java.math.BigDecimal;
import java.math.BigInteger;

public class DecimalUtil {

    // Copy from org.apache.calcite.runtime.SqlFunctions.toBigDecimal
    public static BigDecimal toBigDecimal(String s) {
        return new BigDecimal(s.trim());
    }

    public static BigDecimal toBigDecimal(Number number) {
        // There are some values of "long" that cannot be represented as "double".
        // Not so "int". If it isn't a long, go straight to double.
        return number instanceof BigDecimal ? (BigDecimal) number
                : number instanceof Integer ? new BigDecimal((Integer) number)
                        : number instanceof BigInteger ? new BigDecimal((BigInteger) number)
                                : number instanceof Long ? new BigDecimal(number.longValue())
                                        : BigDecimal.valueOf(number.doubleValue());
    }

    public static BigDecimal toBigDecimal(Object o) {
        if (o == null)
            return null;
        return o instanceof Number ? toBigDecimal((Number) o) : toBigDecimal(o.toString());
    }
}

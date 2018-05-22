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

package org.apache.kylin.metadata.datatype;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.Comparator;

import org.apache.kylin.common.util.DateFormat;

/**
 * Define order for string literals based on its underlying data type.
 * 
 * Null is the smallest.
 */
abstract public class DataTypeOrder implements Comparator<String> {

    public static final DataTypeOrder INTEGER_ORDER = new IntegerOrder();
    public static final DataTypeOrder DOUBLE_ORDER = new DoubleOrder();
    public static final DataTypeOrder DECIMAL_ORDER = new DecimalOrder();
    public static final DataTypeOrder DATETIME_ORDER = new DateTimeOrder();
    public static final DataTypeOrder STRING_ORDER = new StringOrder();

    // package private, access via DataType.getOrder()
    static DataTypeOrder getInstance(DataType type) throws IllegalArgumentException {
        if (type.isStringFamily())
            return STRING_ORDER;
        else if (type.isDateTimeFamily())
            return DATETIME_ORDER;
        else if (type.isIntegerFamily())
            return INTEGER_ORDER;
        else if (type.isFloat() || type.isDouble())
            return DOUBLE_ORDER;
        else if (type.isDecimal())
            return DECIMAL_ORDER;
        else
            throw new IllegalArgumentException("Unsupported data type " + type);
    }

    public String max(Collection<String> values) {
        String max = null;
        for (String v : values) {
            max = max(max, v);
        }
        return max;
    }

    public String min(Collection<String> values) {
        String min = null;
        for (String v : values) {
            min = min(min, v);
        }
        return min;
    }

    public String min(String v1, String v2) {
        if (v1 == null)
            return v2;
        else if (v2 == null)
            return v1;
        else
            return compare(v1, v2) <= 0 ? v1 : v2;
    }

    public String max(String v1, String v2) {
        if (v1 == null)
            return v2;
        else if (v2 == null)
            return v1;
        else
            return compare(v1, v2) >= 0 ? v1 : v2;
    }

    @Override
    public int compare(String s1, String s2) {
        Comparable o1 = toComparable(s1);
        Comparable o2 = toComparable(s2);
        
        // consider null
        if (o1 == o2)
            return 0;
        if (o1 == null)
            return -1;
        if (o2 == null)
            return 1;

        return o1.compareTo(o2);
    }

    abstract Comparable toComparable(String s);

    private static class StringOrder extends DataTypeOrder {
        @Override
        public String toComparable(String s) {
            return s;
        }
    }

    private static class IntegerOrder extends DataTypeOrder {
        @Override
        public Long toComparable(String s) {
            if (s == null || s.isEmpty())
                return null;
            else
                return Long.parseLong(s);
        }
    }

    private static class DoubleOrder extends DataTypeOrder {
        @Override
        public Double toComparable(String s) {
            if (s == null || s.isEmpty())
                return null;
            else
                return Double.parseDouble(s);
        }
    }
    
    private static class DecimalOrder extends DataTypeOrder {
        @Override
        public BigDecimal toComparable(String s) {
            if (s == null || s.isEmpty())
                return null;
            else
                return new BigDecimal(s);
        }
    }
    
    private static class DateTimeOrder extends DataTypeOrder {
        @Override
        public Long toComparable(String s) {
            if (s == null || s.isEmpty())
                return null;
            else
                return DateFormat.stringToMillis(s);
        }
    }
    
}

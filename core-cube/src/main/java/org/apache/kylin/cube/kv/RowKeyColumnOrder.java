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

package org.apache.kylin.cube.kv;

import java.util.Collection;
import java.util.Comparator;

import org.apache.kylin.metadata.datatype.DataType;

/**
 * @author yangli9
 */
abstract public class RowKeyColumnOrder implements Comparator<String> {

    public static final NumberOrder NUMBER_ORDER = new NumberOrder();
    public static final StringOrder STRING_ORDER = new StringOrder();

    public static RowKeyColumnOrder getInstance(DataType type) {
        if (type.isNumberFamily() || type.isDateTimeFamily())
            return NUMBER_ORDER;
        else
            return STRING_ORDER;
    }

    public String max(Collection<String> values) {
        String max = null;
        for (String v : values) {
            if (max == null || compare(max, v) < 0)
                max = v;
        }
        return max;
    }

    public String min(Collection<String> values) {
        String min = null;
        for (String v : values) {
            if (min == null || compare(min, v) > 0)
                min = v;
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
    public int compare(String o1, String o2) {
        // consider null
        if (o1 == o2)
            return 0;
        if (o1 == null)
            return -1;
        if (o2 == null)
            return 1;

        return compareNonNull(o1, o2);
    }

    abstract int compareNonNull(String o1, String o2);

    private static class StringOrder extends RowKeyColumnOrder {
        @Override
        public int compareNonNull(String o1, String o2) {
            return o1.compareTo(o2);
        }
    }

    private static class NumberOrder extends RowKeyColumnOrder {
        @Override
        public int compareNonNull(String o1, String o2) {
            double d1 = Double.parseDouble(o1);
            double d2 = Double.parseDouble(o2);
            return Double.compare(d1, d2);
        }
    }

}

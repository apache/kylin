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

package org.apache.kylin.measure.sumlc;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Map;
import java.util.function.BiFunction;

import com.google.common.collect.Maps;

public class SumLCCounter implements Serializable {
    private static final Map<String, BiFunction<Number, Number, Number>> MERGE_FUNC_MAP = Maps.newHashMap();

    static {
        MERGE_FUNC_MAP.put(Long.class.getSimpleName(), (s1, s2) -> Long.sum((Long) s1, (Long) s2));
        MERGE_FUNC_MAP.put(Double.class.getSimpleName(), (s1, s2) -> Double.sum((Double) s1, (Double) s2));
        MERGE_FUNC_MAP.put(BigDecimal.class.getSimpleName(), (s1, s2) -> ((BigDecimal) s1).add((BigDecimal) s2));
    }

    Number sumLC;
    Long timestamp;

    public SumLCCounter() {

    }

    public SumLCCounter(Number sumLC, Long timestamp) {
        this.sumLC = numericTypeConversion(sumLC);
        this.timestamp = timestamp;
    }

    public static SumLCCounter merge(SumLCCounter current, Number sumLC, Long timestamp) {
        SumLCCounter merged = new SumLCCounter(sumLC, timestamp);
        return merge(current, merged);
    }

    public static SumLCCounter merge(SumLCCounter value1, SumLCCounter value2) {
        if (value1 == null || value1.timestamp == null)
            return value2;
        if (value2 == null || value2.timestamp == null)
            return value1;
        if (value2.timestamp > value1.timestamp) {
            return value2;
        } else if (value1.timestamp > value2.timestamp) {
            return value1;
        } else {
            return mergeSum(value1, value2);
        }
    }

    private static SumLCCounter mergeSum(SumLCCounter cnt1, SumLCCounter cnt2) {
        if (cnt1.sumLC == null)
            return cnt2;
        if (cnt2.sumLC == null)
            return cnt1;
        String sumLCTypeName = cnt1.sumLC.getClass().getSimpleName();
        Number semiSum = MERGE_FUNC_MAP.get(sumLCTypeName).apply(cnt1.sumLC, cnt2.sumLC);
        return new SumLCCounter(semiSum, cnt1.timestamp);
    }

    private static Number numericTypeConversion(Number input) {
        if (input instanceof Byte || input instanceof Short || input instanceof Integer) {
            return input.longValue();
        } else if (input instanceof Float) {
            return input.doubleValue();
        } else {
            return input;
        }
    }

    public Number getSumLC() {
        return sumLC;
    }

    public Long getTimestamp() {
        return timestamp;
    }

}

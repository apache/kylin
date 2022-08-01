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
package io.kyligence.kap.secondstorage.util;

import io.kyligence.kap.secondstorage.config.SecondStorageProjectModelSegment;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Locale;

public class ConvertUtilsTest {

    @Test
    public void testConvertValue() {
        Assertions.assertEquals(Float.valueOf("1.22"), ConvertUtils.convertValue(1.22, Float.class));
        Assertions.assertEquals(Float.valueOf("1.30"), ConvertUtils.convertValue(1.30, Float.class));

        Assertions.assertEquals(Double.valueOf("1.22"), ConvertUtils.convertValue(1.22, Double.class));
        Assertions.assertEquals(Double.valueOf("1.30"), ConvertUtils.convertValue(1.30, Double.class));

        SecondStorageProjectModelSegment segment = new SecondStorageProjectModelSegment();
        ConvertUtils.convertValue(segment, SecondStorageProjectModelSegment.class);

        Assertions.assertThrows(IllegalArgumentException.class, () -> ConvertUtils.convertValue(segment, UnsupportedClass.class));

        ConvertUtils.convertValue(segment, SecondStorageProjectModelSegment.class);
    }

    @Test
    public void testConvertValueBoolean() {
        Assertions.assertEquals(true, ConvertUtils.convertValue(true, Boolean.class));
        Assertions.assertEquals(false, ConvertUtils.convertValue(false, Boolean.class));

        Assertions.assertEquals(true, ConvertUtils.convertValue("true", Boolean.class));
        Assertions.assertEquals(false, ConvertUtils.convertValue("false", Boolean.class));

        Exception exception = Assertions.assertThrows(IllegalArgumentException.class, () -> ConvertUtils.convertValue("10", Boolean.class));
        Assertions.assertEquals(exception.getMessage(), String.format(Locale.ROOT,
                "Unrecognized option for boolean: %s. Expected either true or false(case insensitive)",
                10));
    }

    @Test
    public void testConvertValueLong() {
        Assertions.assertEquals(Long.valueOf(10), ConvertUtils.convertValue(Long.valueOf(10), Long.class));
        Assertions.assertEquals(Long.valueOf(10), ConvertUtils.convertValue(Integer.valueOf(10), Long.class));
        Assertions.assertEquals(Long.valueOf(10), ConvertUtils.convertValue("10", Long.class));
    }

    @Test
    public void testConvertValueInteger() {
        Assertions.assertEquals(Integer.valueOf(10), ConvertUtils.convertValue(Integer.valueOf(10), Integer.class));
        Assertions.assertEquals(Integer.valueOf(10), ConvertUtils.convertValue(Long.valueOf(10), Integer.class));
        Assertions.assertEquals(Integer.valueOf(10), ConvertUtils.convertValue("10", Integer.class));
        Exception exception = Assertions.assertThrows(IllegalArgumentException.class, () -> ConvertUtils.convertValue(Long.MAX_VALUE, Integer.class));
        Assertions.assertEquals(exception.getMessage(), String.format(Locale.ROOT,
                "Configuration value %s overflow/underflow the integer type.",
                Long.MAX_VALUE));
    }

    public class UnsupportedClass {

    }
}

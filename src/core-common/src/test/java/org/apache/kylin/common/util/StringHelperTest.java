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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class StringHelperTest {

    @Test
    void testMin() {
        Assertions.assertEquals("", StringHelper.min(null, ""));
        Assertions.assertEquals("", StringHelper.min("", null));
        Assertions.assertEquals("1", StringHelper.min("1", "2"));
    }

    @Test
    void testMax() {
        Assertions.assertEquals("", StringHelper.max(null, ""));
        Assertions.assertEquals("", StringHelper.max("", null));
        Assertions.assertEquals("2", StringHelper.max("1", "2"));
    }

    @Test
    void testValidateBoolean() {
        Assertions.assertTrue(StringHelper.validateBoolean("true"));
        Assertions.assertTrue(StringHelper.validateBoolean("false"));
    }

    @Test
    void testBacktickToDoubleQuote() {
        Assertions.assertEquals("\"a\".\"b\" + 1", StringHelper.backtickToDoubleQuote("`a`.`b` + 1"));
    }

    @Test
    void testDoubleQuoteToBackTick() {
        Assertions.assertEquals("`a`.`b` + '''1'", StringHelper.doubleQuoteToBacktick("\"a\".\"b\" + '''1'"));
    }

    @Test
    void testBacktickQuote() {
        Assertions.assertEquals("`aa`", StringHelper.backtickQuote("aa"));
    }

    @Test
    void testDoubleQuote() {
        Assertions.assertEquals("\"aa\"", StringHelper.doubleQuote("aa"));
    }

    @Test
    void testSubArray() {
        String[] arr = { "a", "b", "c" };
        try {
            StringHelper.subArray(arr, -1, 1);
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof IllegalArgumentException);
        }

        try {
            StringHelper.subArray(arr, 2, 1);
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof IllegalArgumentException);
        }

        try {
            StringHelper.subArray(arr, 1, 5);
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof IllegalArgumentException);
        }

        String[] arrNew = StringHelper.subArray(arr, 0, 2);
        Assertions.assertEquals(2, arrNew.length);
        Assertions.assertEquals("a", arrNew[0]);
        Assertions.assertEquals("b", arrNew[1]);
    }

    @Test
    void testSplitAndTrim() {
        String[] arr = StringHelper.splitAndTrim("a, ,b, c", ",");
        Assertions.assertEquals(3, arr.length);
        Assertions.assertEquals("a", arr[0]);
        Assertions.assertEquals("b", arr[1]);
        Assertions.assertEquals("c", arr[2]);
    }
}

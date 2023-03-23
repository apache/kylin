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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;

import org.junit.jupiter.api.Test;

import lombok.val;

public class RandomUtilTest {

    @Test
    void testRandomUUID() {
        assertEquals(RandomUtil.randomUUID().toString().length(), UUID.randomUUID().toString().length());
        assertNotEquals(RandomUtil.randomUUID().toString(), RandomUtil.randomUUID().toString());
    }

    @Test
    void testRandomUUIDStr() {
        assertEquals(RandomUtil.randomUUIDStr().length(), UUID.randomUUID().toString().length());
        assertNotEquals(RandomUtil.randomUUIDStr(), RandomUtil.randomUUIDStr());
    }

    @Test
    void nextInt() {
        testNextInt(0, 999);
        testNextInt(0, 99);
        testNextInt(0, 9);
        testNextInt(0, 0);
        testNextInt(9, 9);
        testNextInt(99, 99);
        testNextInt(99, 999);
    }

    private void testNextInt(final int startInclusive, final int endExclusive) {
        for (int i = 0; i < 100; i++) {
            val randomInt = RandomUtil.nextInt(startInclusive, endExclusive);
            assertTrue(randomInt >= startInclusive);
            if (startInclusive == endExclusive) {
                assertEquals(startInclusive, randomInt);
            } else {
                assertTrue(randomInt < endExclusive);
            }
        }
    }
}

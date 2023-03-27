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

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.kylin.guava30.shaded.common.collect.Lists;
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

    @Test
    void nextIntMultithreading() throws Exception {
        concurrentTest(10, 30);
    }

    public static void concurrentTest(long concurrentThreads, int times) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool((int) concurrentThreads);
        List<Future<Integer>> results = Lists.newArrayList();

        for (int i = 1; i < times; i++) {
            int finalI = i;
            Callable<Integer> callable1 = () -> RandomUtil.nextInt(100 * finalI, 200 * finalI);
            Callable<Integer> callable2 = () -> RandomUtil.nextInt(200000 * finalI, 300000 * finalI);
            results.add(executor.submit(callable1));
            results.add(executor.submit(callable2));
        }
        executor.shutdown();

        for (int i = 0; i < times - 1; i++) {
            Integer result1 = results.get(2 * i).get();
            assertTrue(result1 < 200 * (i + 1));
            assertTrue(result1 >= 100 * (i + 1));

            Integer result2 = results.get(2 * i + 1).get();
            assertTrue(result2 < 300000 * (i + 1));
            assertTrue(result2 >= 200000 * (i + 1));
        }
    }
}

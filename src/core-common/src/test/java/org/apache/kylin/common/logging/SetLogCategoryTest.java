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

package org.apache.kylin.common.logging;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Deque;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfigMultithreadingTest;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.logging.log4j.ThreadContext;
import org.awaitility.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import lombok.val;

class SetLogCategoryTest {
    @AfterEach
    @BeforeEach
    void beforeAndAfter() {
        ThreadContext.remove("logCategory");
    }

    @Test
    void testMulti() throws Exception {
        val callables = Lists.<Callable<String>> newArrayList();
        for (int i = 0; i < 20; i++) {
            Callable<String> callable = () -> {
                checkSetLogCategory();
                return "ok";
            };
            callables.add(callable);
        }
        KylinConfigMultithreadingTest.concurrentTest(100, 10, callables);
    }

    private void checkSetLogCategory() {
        checkResult(null, Lists.newArrayList(), 0);
        val category0 = RandomUtil.randomUUIDStr();
        try (SetLogCategory logCategory = new SetLogCategory(category0)) {
            checkResult(category0, Lists.newArrayList(logCategory), 0);
        }
        checkResult(null, Lists.newArrayList(), 0);

        val category1 = RandomUtil.randomUUIDStr();
        val category2 = RandomUtil.randomUUIDStr();
        val category3 = RandomUtil.randomUUIDStr();
        val category4 = RandomUtil.randomUUIDStr();
        val category5 = RandomUtil.randomUUIDStr();
        try (SetLogCategory logCategory1 = new SetLogCategory(category1)) {
            checkResult(category1, Lists.newArrayList(logCategory1), 0);

            try (SetLogCategory logCategory2 = new SetLogCategory(category2)) {
                checkResult(category2, Lists.newArrayList(logCategory1, logCategory2), 1);
            }
            checkResult(category1, Lists.newArrayList(logCategory1), 0);

            try (SetLogCategory logCategory3 = new SetLogCategory(category3)) {
                checkResult(category3, Lists.newArrayList(logCategory1, logCategory3), 1);

                try (SetLogCategory logCategory4 = new SetLogCategory(category4)) {
                    checkResult(category4, Lists.newArrayList(logCategory1, logCategory3, logCategory4), 2);

                    try (SetLogCategory logCategory5 = new SetLogCategory(category5)) {
                        checkResult(category5,
                                Lists.newArrayList(logCategory1, logCategory3, logCategory4, logCategory5), 3);
                    }

                    checkResult(category4, Lists.newArrayList(logCategory1, logCategory3, logCategory4), 2);
                }

                checkResult(category3, Lists.newArrayList(logCategory1, logCategory3), 1);
            }

            checkResult(category1, Lists.newArrayList(logCategory1), 0);

        }

        checkResult(null, Lists.newArrayList(), 0);
    }

    private void checkResult(String category, List<SetLogCategory> logCategories, int size) {
        checkCategory(category);
        checkCategoryThreadLocal(logCategories, size);
    }

    private void checkCategory(String category) {
        await().pollDelay(new Duration(RandomUtil.nextInt(90, 100), TimeUnit.MILLISECONDS)).until(() -> true);
        assertEquals(category, ThreadContext.get("logCategory"));
    }

    private void checkCategoryThreadLocal(List<SetLogCategory> logCategories, int size) {
        for (SetLogCategory logCategory : logCategories) {
            val categoryThreadLocal = (ThreadLocal<Deque<String>>) ReflectionTestUtils.getField(logCategory,
                    "categoryThreadLocal");
            assertNotNull(categoryThreadLocal);
            assertEquals(size, categoryThreadLocal.get().size());

        }

        val categoryThreadLocalStatic = (ThreadLocal<Deque<String>>) ReflectionTestUtils.getField(SetLogCategory.class,
                "categoryThreadLocal");
        assertNotNull(categoryThreadLocalStatic);
        assertEquals(size, categoryThreadLocalStatic.get().size());
    }
}

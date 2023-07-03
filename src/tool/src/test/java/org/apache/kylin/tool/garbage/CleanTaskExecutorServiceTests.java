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

package org.apache.kylin.tool.garbage;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class CleanTaskExecutorServiceTests extends NLocalFileMetadataTestCase {

    CleanTaskExecutorService mockedHelper = getTempInstance();

    @BeforeEach
    public void setup() {
        createTestMetadata();
    }

    static class TestStorageCleaner extends StorageCleaner {

        private long timeout = 1000;

        private Runnable runnable;

        public TestStorageCleaner(Runnable runnable) throws Exception {
            this.runnable = runnable;
        }

        public TestStorageCleaner(long timeout) throws Exception {
            this.timeout = timeout;
        }

        @Override
        public void execute() throws Exception {
            if (runnable != null) {
                runnable.run();
            }
            try {
                await().atMost(timeout, TimeUnit.MILLISECONDS).untilTrue(new AtomicBoolean(false));
            } catch (ConditionTimeoutException cte) {
                // Ignore
            }
        }
    }

    static class TestComparableCleanTask extends AbstractComparableCleanTask {
        private final StorageCleaner.CleanerTag tag;
        public TestComparableCleanTask(StorageCleaner.CleanerTag tag) {
            this.tag = tag;
        }

        @Override
        public StorageCleaner.CleanerTag getCleanerTag() {
            return tag;
        }
    }

    @Test
    @Order(1)
    void testCreateCleanTask() {
        AbstractComparableCleanTask t1 = new TestComparableCleanTask(StorageCleaner.CleanerTag.ROUTINE);
        assertTrue(t1.getName().startsWith(CleanTaskExecutorServiceTests.class.getName()));
        assertEquals(String.format("Task-%s: {tag: %s, class: %s}", t1.getName(), StorageCleaner.CleanerTag.ROUTINE,
                t1.getClass().getName()), t1.getBrief());

        AbstractComparableCleanTask t2 = new TestComparableCleanTask(StorageCleaner.CleanerTag.SERVICE);
        assertEquals(TestComparableCleanTask.class.getName(), t2.getName());
        assertEquals(String.format("Task-%s: {tag: %s, class: %s}", t2.getName(), StorageCleaner.CleanerTag.SERVICE,
                t2.getClass().getName()), t2.getBrief());

        // t2 has a higher priority
        assertTrue(t1.compareTo(t2) > 0);

        AbstractComparableCleanTask t3 = new TestComparableCleanTask(StorageCleaner.CleanerTag.SERVICE);
        AbstractComparableCleanTask t4 = t3;

        assertNotEquals(null, t1);
        assertEquals(0, t2.compareTo(t3));
        assertEquals(0, t3.compareTo(t4));
        assertNotEquals(t2, t3);
        assertEquals(t3, t4);
    }

    @Test
    @Order(1)
    void testComparableFutureTask() {
        AbstractComparableCleanTask ct1 = new AbstractComparableCleanTask() {
            @Override
            public StorageCleaner.CleanerTag getCleanerTag() {
                return StorageCleaner.CleanerTag.ROUTINE;
            }
        };

        PriorityExecutor.ComparableFutureTask<AbstractComparableCleanTask, Boolean> t1 =
            new PriorityExecutor.ComparableFutureTask<>(ct1, true);
        PriorityExecutor.ComparableFutureTask<AbstractComparableCleanTask, Boolean> t11 =
            new PriorityExecutor.ComparableFutureTask<>(ct1, true);

        assertNotEquals(null, t1);

        PriorityExecutor.ComparableFutureTask<AbstractComparableCleanTask, Boolean> t1Ref = t1;
        assertEquals(t1, t1Ref);
        assertEquals(t1, t11);

        AbstractComparableCleanTask ct2 = new AbstractComparableCleanTask() {
            @Override
            public StorageCleaner.CleanerTag getCleanerTag() {
                return StorageCleaner.CleanerTag.ROUTINE;
            }
        };

        PriorityExecutor.ComparableFutureTask<AbstractComparableCleanTask, Boolean> t21 =
            new PriorityExecutor.ComparableFutureTask<>(ct2, true);

        assertNotEquals(t11, t21);

        assertEquals(0, t21.compareTo(t11));
        assertNotEquals(t11, t21);

        AbstractComparableCleanTask ct3 = new AbstractComparableCleanTask() {
            @Override
            public StorageCleaner.CleanerTag getCleanerTag() {
                return StorageCleaner.CleanerTag.SERVICE;
            }
        };

        PriorityExecutor.ComparableFutureTask<AbstractComparableCleanTask, Boolean> t31 =
            new PriorityExecutor.ComparableFutureTask<>(ct3, true);

        assertTrue(t31.compareTo(t21) < 0, "t31 should be the predecessor task");
    }

    @Test
    @Order(1)
    void testBindThreadPool() throws NoSuchFieldException, IllegalAccessException {
        resetInnerPool();
        assertTrue(mockedHelper.bindWorkingPool(createPriorityPool()));
        assertFalse(mockedHelper.bindWorkingPool(createPriorityPool()));
    }

    @Test
    @Order(1)
    void testCleanTaskTimeout() throws Exception {
        StorageCleaner routineSc = new TestStorageCleaner(5000).withTag(StorageCleaner.CleanerTag.ROUTINE)
                .withTraceId("1");

        overwriteSystemProp("kylin.storage.clean-tasks-concurrency", "1");

        mockedHelper.bindWorkingPool(createPriorityPool());
        CompletableFuture<?> f = mockedHelper.submit(routineSc, 1, TimeUnit.SECONDS);
        CompletionException ce = assertThrows(CompletionException.class, f::join);

        assertTrue(ce.getCause() instanceof TimeoutException);
    }

    @Test
    @Order(10)
    void testCleanTaskRejected() throws Exception {
        StorageCleaner routineSc = new TestStorageCleaner(10000).withTag(StorageCleaner.CleanerTag.ROUTINE)
                .withTraceId("1");

        overwriteSystemProp("kylin.storage.clean-tasks-concurrency", "1");

        mockedHelper.bindWorkingPool(createPriorityPool());
        mockedHelper.close();

        CompletableFuture<?> f = mockedHelper.submit(routineSc, 1, TimeUnit.SECONDS);
        CompletionException ce = assertThrows(CompletionException.class, f::join);

        assertTrue(ce.getCause() instanceof RejectedExecutionException);
        assertTrue(f.isDone());
        resetInnerPool();
    }

    @Test
    @Order(1)
    void testCleanTasksCompletedInOrderByTag() throws Exception {
        StorageCleaner routineSc = new TestStorageCleaner(1000).withTag(StorageCleaner.CleanerTag.ROUTINE)
                .withTraceId("1");
        StorageCleaner cliSc = new TestStorageCleaner(200).withTag(StorageCleaner.CleanerTag.CLI).withTraceId("2");
        StorageCleaner serviceSc = new TestStorageCleaner(200).withTag(StorageCleaner.CleanerTag.SERVICE)
                .withTraceId("3");

        overwriteSystemProp("kylin.storage.clean-tasks-concurrency", "1");

        resetInnerPool();
        mockedHelper.bindWorkingPool(createPriorityPool());

        List<String> completedTasks = new ArrayList<>();
        List<CompletableFuture<?>> tasks = new ArrayList<>();

        StorageCleaner initSc = new TestStorageCleaner(() -> {
            tasks.add(mockedHelper.submit(routineSc, 6, TimeUnit.SECONDS)
                    .whenComplete((v, t) -> completedTasks.add(routineSc.getTraceId())));
            tasks.add(mockedHelper.submit(cliSc, 6, TimeUnit.SECONDS)
                    .whenComplete((v, t) -> completedTasks.add(cliSc.getTraceId())));
            tasks.add(mockedHelper.submit(serviceSc, 6, TimeUnit.SECONDS)
                    .whenComplete((v, t) -> completedTasks.add(serviceSc.getTraceId())));
        });

        CompletableFuture<?> initFuture = mockedHelper.submit(initSc, 6, TimeUnit.SECONDS);
        CompletableFuture.allOf(initFuture).join();
        CompletableFuture.allOf(tasks.toArray(new CompletableFuture[0])).join();

        // initSc is the first routine task in to the working queue, and it should be the first to be executed.
        // During cf1 executing, cf1/cf2/cf3 will be added into the working queue and sorted by cleaner tag,
        // of which permutation should be [cf3, cf2, cf1], so after initSc completed, the other tasks will
        // finally complete in sequence.
        assertEquals(ImmutableList.of("3", "2", "1"), completedTasks);
    }

    private ExecutorService createPriorityPool() {
        return PriorityExecutor.newWorkingThreadPool("test-pool", getTestConfig().getStorageCleanTaskConcurrency());
    }

    @Test
    @Order(11)
    void testDoubleRetriesToShutdown() throws NoSuchFieldException, IllegalAccessException {
        overwriteSystemProp("kylin.storage.clean-tasks-concurrency", "1");
        mockedHelper.bindWorkingPool(createPriorityPool());

        AtomicBoolean shutdownTriggered = new AtomicBoolean(false);
        CompletableFuture<?> future = mockedHelper.submit(new AbstractComparableCleanTask() {
            @Override
            protected void doRun() {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        await().atMost(10, TimeUnit.SECONDS).untilTrue(new AtomicBoolean(false));
                    } catch (Exception e) {
                        shutdownTriggered.set(true);
                        Thread.currentThread().interrupt();
                    }
                }
            }

            @Override
            public StorageCleaner.CleanerTag getCleanerTag() {
                return StorageCleaner.CleanerTag.SERVICE;
            }
        }, 10, TimeUnit.SECONDS);

        new Thread(() -> {
            try {
                await().atMost(1, TimeUnit.SECONDS).untilTrue(new AtomicBoolean(false));
            } catch (Exception ignored) {
                try {
                    mockedHelper.close();
                } catch (IOException ignored1) {
                }
            }
        }).start();
        future.join();
        assertTrue(future.isDone() && shutdownTriggered.get());
    }

    private CleanTaskExecutorService getTempInstance() {
        try {
            Constructor<CleanTaskExecutorService> constructor = CleanTaskExecutorService.class.getDeclaredConstructor();
            constructor.setAccessible(true);
            return constructor.newInstance();
        } catch (Exception ignored) {
            return null;
        }
    }

    private void resetInnerPool() throws NoSuchFieldException, IllegalAccessException {
        Field poolField = CleanTaskExecutorService.class.getDeclaredField("pool");
        poolField.setAccessible(true);
        poolField.set(mockedHelper, null);

        Field boundField = CleanTaskExecutorService.class.getDeclaredField("bound");
        boundField.setAccessible(true);
        boundField.set(mockedHelper, new AtomicBoolean(false));
    }

}

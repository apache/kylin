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
package org.apache.kylin.query.util;

import static org.awaitility.Awaitility.await;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.query.exception.BusyQueryException;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class QueryLimiterTest extends NLocalFileMetadataTestCase {
    private volatile boolean queryRejected = false;

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
        overwriteSystemProp("kylin.guardian.downgrade-mode-parallel-query-threshold", "3");
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void testDowngrade() {
        concurrenceQuery(10);

        Assert.assertFalse(queryRejected);

        QueryLimiter.downgrade();

        concurrenceQuery(10);

        Assert.assertTrue(queryRejected);
        QueryLimiter.recover();
    }

    @Test
    public void testDetail() throws InterruptedException {
        int times = 10;
        ExecutorService executorService = Executors.newFixedThreadPool(times * 2);
        final CountDownLatch tasks = new CountDownLatch(times);
        for (int i = 0; i < times; i++) {
            int time = (i + 1) * 1000;
            executorService.submit(() -> {
                query(time);
                tasks.countDown();
            });
        }
        // at least finished 3 query
        await().atMost(5, TimeUnit.SECONDS).until(() -> tasks.getCount() <= 7);

        // have running query, downgrade state is false
        Assert.assertTrue(tasks.getCount() > 0);

        QueryLimiter.downgrade();

        final CountDownLatch tasks1 = new CountDownLatch(3);

        for (int i = 0; i < 3; i++) {
            executorService.submit(() -> {
                query(5000);
                tasks1.countDown();
            });
        }

        Thread.sleep(1000);

        // failed
        try {
            query(1000);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof BusyQueryException);
        }

        await().atMost(5, TimeUnit.SECONDS).until(() -> tasks1.getCount() == 0);

        // success
        query(1000);

        Semaphore semaphore = QueryLimiter.getSemaphore();

        Assert.assertEquals(3, semaphore.availablePermits());
        QueryLimiter.recover();
    }

    private void concurrenceQuery(int times) {
        ExecutorService executorService = Executors.newFixedThreadPool(times);
        final CountDownLatch tasks = new CountDownLatch(times);

        for (int i = 0; i < times; i++) {
            executorService.submit(() -> {
                try {
                    query(1000);
                } catch (BusyQueryException e) {
                    queryRejected = true;
                }

                tasks.countDown();
            });
        }

        await().atMost(15, TimeUnit.SECONDS).until(() -> tasks.getCount() == 0);
        executorService.shutdown();
    }

    private void query(long millis) {
        QueryLimiter.tryAcquire();

        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            //
        } finally {
            QueryLimiter.release();
        }
    }
}

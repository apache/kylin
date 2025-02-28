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

package org.apache.kylin.common.lock;

import static org.apache.kylin.common.util.TestUtils.getTestConfig;
import static org.awaitility.Awaitility.await;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.junit.Assert;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DistributedLockFactoryTest {

    public void testConcurrence(String key, int threadNum, int times) throws Exception {
        threadNum = Math.max(2, threadNum);
        ExecutorService executorService = Executors.newFixedThreadPool(threadNum);

        final CountDownLatch tasks = new CountDownLatch(threadNum);

        int[] count = new int[] { 0 };

        for (int i = 0; i < threadNum; i++) {
            executorService.submit(new DirtyReadTest(key, times, tasks, count));
        }

        await().atMost(20, TimeUnit.SECONDS).until(() -> tasks.getCount() == 0);
        Assert.assertEquals(threadNum * times, count[0]);
    }

    static class DirtyReadTest implements Runnable {
        private final int times;
        private final int[] count;
        private final CountDownLatch countDownLatch;
        private final String key;

        DirtyReadTest(String key, int times, CountDownLatch countDownLatch, int[] count) {
            this.times = times;
            this.count = count;
            this.key = key;
            this.countDownLatch = countDownLatch;
        }

        @Override
        public void run() {
            Lock lock = null;
            try {
                lock = getTestConfig().getDistributedLockFactory().getLockForCurrentThread(key);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
            for (int i = 0; i < times; i++) {
                try {
                    lock.lock();
                    int v = count[0];
                    count[0] = v + 1;
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                } finally {
                    lock.unlock();
                }
            }
            countDownLatch.countDown();
        }
    }
}

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

package org.apache.kylin.rest.util;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

public class QueryRequestLimitsTest {

    @Test
    public void testOpenAndCloseQueryRequest() {
        int nThread = 5;

        final Integer maxConcurrentQuery = 2;
        final String project = "test";

        final AtomicInteger nQueryFailed = new AtomicInteger(0);

        Thread[] threads = new Thread[nThread];
        final CountDownLatch lock = new CountDownLatch(nThread);
        for (int i = 0; i < nThread; i++) {
            final int j = i;
            threads[j] = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        boolean ifOpen = QueryRequestLimits.openQueryRequest(project, maxConcurrentQuery);
                        lock.countDown();
                        if (ifOpen) {
                            lock.await();
                            QueryRequestLimits.closeQueryRequest(project, maxConcurrentQuery);
                        } else {
                            nQueryFailed.incrementAndGet();
                        }
                    } catch (InterruptedException e) {
                    }
                }
            });
            threads[j].start();
        }
        for (int i = 0; i < nThread; i++) {
            try {
                threads[i].join();
            } catch (InterruptedException e) {
            }
        }
        Assert.assertEquals(new Integer(0), QueryRequestLimits.getCurrentRunningQuery(project));
        Assert.assertEquals(nThread - maxConcurrentQuery, nQueryFailed.get());
    }
}

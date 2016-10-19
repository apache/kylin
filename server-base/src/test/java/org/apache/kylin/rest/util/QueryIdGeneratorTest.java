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

import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class QueryIdGeneratorTest {

    @Test
    public void testIdFormat() {
        QueryIdGenerator generator = new QueryIdGenerator();
        for (int i = 0; i < 100; i++) {
            String queryId = generator.nextId("project");
            Assert.assertTrue(queryId.contains("project"));
        }
    }

    @Test
    public void testIdUniqueness() {
        QueryIdGenerator generator = new QueryIdGenerator();
        Set<String> idSet = new HashSet<>();

        for (int i = 0; i < 1000; i++) {
            idSet.add(generator.nextId("test"));
        }

        Assert.assertEquals(1000, idSet.size());
    }

    @Test
    public void testSingleThreadThroughput() {
        int N = 1_000_000;
        long millis = new GenIdTask(new QueryIdGenerator(), N).call();

        // ops / second
        double throughput = (N * 1000.0) / millis;
        System.out.format("QueryIdGenerator single thread throughput: %d ops/second\n", (int) throughput);
    }

    @Test
    public void testMultiThreadsThroughput() throws ExecutionException, InterruptedException {
        QueryIdGenerator generator = new QueryIdGenerator();
        int N = 1_000_000;

        final int numThreads = 4;
        ExecutorService pool = Executors.newFixedThreadPool(numThreads);
        Future[] futures = new Future[numThreads];

        for (int i = 0; i < numThreads; i++) {
            futures[i] = pool.submit(new GenIdTask(generator, N));
        }

        long sumMillis = 0;
        for (int i = 0; i < numThreads; i++) {
            sumMillis += (long) futures[i].get();
        }
        pool.shutdown();

        double avgThroughputPerThread = (N * 1000.0) / (sumMillis / (double) numThreads);
        System.out.format("QueryIdGenerator multi threads throughput: %d ops/second\n", (int) avgThroughputPerThread);
    }

    private static class GenIdTask implements Callable<Long> {
        private final QueryIdGenerator generator;
        private final int N;

        GenIdTask(QueryIdGenerator generator, int N) {
            this.generator = generator;
            this.N = N;
        }

        @Override
        public Long call() {
            long start = System.currentTimeMillis();
            for (int i = 0; i < N; i++) {
                generator.nextId("test");
            }
            return System.currentTimeMillis() - start;
        }
    }
}
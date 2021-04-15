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

package org.apache.kylin.common;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

public class SubThreadPoolExecutorTest {
    private final static Logger logger = LoggerFactory.getLogger(SubThreadPoolExecutorTest.class);

    private static final long SleepTime = 50L;

    @Test
    public void testOneLayer() throws InterruptedException, ExecutionException {
        int nTotalThread = 10;
        int nTask = 5;
        int nSubMaxThread = 2;
        long minNRound = (nTask - 1) / nSubMaxThread + 1;

        ExecutorService executor = Executors.newFixedThreadPool(nTotalThread);
        SubThreadPoolExecutor subExecutor = new SubThreadPoolExecutor(executor, "layOne", nSubMaxThread);
        testInner(subExecutor, nTask, minNRound);
    }

    @Test
    public void testTwoLayer() throws InterruptedException, ExecutionException {
        testTwoLayer(5);
        testTwoLayer(1);
    }

    private void testTwoLayer(int nSubMaxThread) throws InterruptedException, ExecutionException {
        int nTotalThread = 10;
        int nTask = 5;
        int nSSubMaxThread = 2;
        long minNRound = (nTask - 1) / Math.min(nSubMaxThread, nSSubMaxThread) + 1;

        ExecutorService executor = Executors.newFixedThreadPool(nTotalThread);
        SubThreadPoolExecutor subExecutor = new SubThreadPoolExecutor(executor, "layOne", nSubMaxThread);
        SubThreadPoolExecutor ssubExecutor = new SubThreadPoolExecutor(subExecutor, "layTwo", nSSubMaxThread);
        testInner(ssubExecutor, nTask, minNRound);
    }

    private void testInner(SubThreadPoolExecutor subExecutor, int nTask, long minNRound)
            throws InterruptedException, ExecutionException {
        Stopwatch sw = new Stopwatch();

        sw.start();
        List<Future<?>> futureList = Lists.newArrayListWithExpectedSize(nTask);
        for (int i = 0; i < nTask; i++) {
            futureList.add(subExecutor.submit(new TestTask()));
        }
        for (Future<?> future : futureList) {
            future.get();
        }
        long timeCost = sw.elapsed(TimeUnit.MILLISECONDS);
        Assert.assertTrue("Time cost should be more than " + timeCost + "ms", timeCost >= minNRound * SleepTime);
        logger.info("time cost: {}ms", timeCost);
    }

    private static class TestTask implements Runnable {
        @Override
        public void run() {
            try {
                Thread.sleep(SleepTime);
            } catch (InterruptedException e) {
            }
        }
    }
}

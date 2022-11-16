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

package org.apache.kylin.common.asyncprofiler;

import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.awaitility.Duration;
import org.junit.Assert;
import org.junit.Test;

public class AsyncProfilerUtilsTest {

    String buildAsyncFileName = "ke-build-async-profiler-result-";

    private AsyncProfilerUtils init() throws IOException {
        AsyncProfilerUtils asyncProfilerUtils = AsyncProfilerUtils.getInstance();
        asyncProfilerUtils.build(new CountDownLatch(3));
        asyncProfilerUtils.build(300000L, Files.createTempDirectory(buildAsyncFileName).toFile());
        return asyncProfilerUtils;
    }

    @Test
    public void testBuild() throws IOException {
        AsyncProfilerUtils asyncProfilerUtilsBuild = AsyncProfilerUtils.getInstance();
        asyncProfilerUtilsBuild.build(new CountDownLatch(2));
        Assert.assertEquals(2, asyncProfilerUtilsBuild.cachedResult.getCount());

        File testFile = Files.createTempDirectory("ke-build-async-test-profiler-").toFile();
        asyncProfilerUtilsBuild.build(2L, testFile);
        Assert.assertEquals(2L, asyncProfilerUtilsBuild.resultCollectionTimeout);
        Assert.assertEquals(testFile, asyncProfilerUtilsBuild.localCacheDir);
    }

    @Test
    public void testWaitForResultTimeout() throws IOException, InterruptedException {
        AsyncProfilerUtils asyncProfilerUtils = AsyncProfilerUtils.getInstance();
        asyncProfilerUtils.build(new CountDownLatch(3));
        asyncProfilerUtils.build(-1L, Files.createTempDirectory(buildAsyncFileName).toFile());

        ExecutorService cachedThreadPool = Executors.newCachedThreadPool();

        Thread t1 = new Thread(() -> {
            await().pollDelay(new Duration(1, TimeUnit.SECONDS)).until(() -> true);
            for (int i = 0; i < 3; i++) {
                cachedThreadPool.execute(() -> asyncProfilerUtils.cachedResult.countDown());
            }
        });

        t1.start();
        asyncProfilerUtils.waitForResult(mock(OutputStream.class));
        cachedThreadPool.shutdown();
        t1.interrupt();
    }

    @Test
    public void testWaitForResult() throws IOException, InterruptedException {
        AsyncProfilerUtils asyncProfilerUtils = init();
        ExecutorService cachedThreadPool = Executors.newCachedThreadPool();

        Thread t1 = new Thread(() -> {
            await().pollDelay(new Duration(1, TimeUnit.MILLISECONDS)).until(() -> true);
            for (int i = 0; i < 3; i++) {
                cachedThreadPool.execute(() -> asyncProfilerUtils.cachedResult.countDown());
            }
        });

        t1.start();
        asyncProfilerUtils.waitForResult(mock(OutputStream.class));
        cachedThreadPool.shutdown();
        t1.interrupt();
    }

    @Test
    public void testSuffix() {
        Assert.assertEquals(0, AsyncProfilerUtils.getInstance().suffix("noHtml").length());
        Assert.assertEquals(".html", AsyncProfilerUtils.getInstance().suffix("<!DOCTYPE html>"));
    }

    @Test
    public void testCacheExecutorResult() throws IOException {
        AsyncProfilerUtils asyncProfilerUtils = init();
        asyncProfilerUtils.cacheExecutorResult("content", "1");
        Files.deleteIfExists(new File(asyncProfilerUtils.localCacheDir.getAbsolutePath(), "executor-1").toPath());
    }

    @Test
    public void testCacheDriverResult() throws IOException {
        AsyncProfilerUtils asyncProfilerUtils = init();
        asyncProfilerUtils.cacheDriverResult("content");
        Files.deleteIfExists(new File(asyncProfilerUtils.localCacheDir.getAbsolutePath(), "driver").toPath());
    }

    @Test
    public void testCacheResultError() throws IOException {
        AsyncProfilerUtils asyncProfilerUtils = init();
        asyncProfilerUtils.cacheResult(null, "result");
        Files.deleteIfExists(new File(asyncProfilerUtils.localCacheDir.getAbsolutePath(), "result").toPath());
    }

    @Test
    public void testCacheResult() throws IOException {
        AsyncProfilerUtils asyncProfilerUtils = init();
        asyncProfilerUtils.cacheResult("content", "result");
        Files.deleteIfExists(new File(asyncProfilerUtils.localCacheDir.getAbsolutePath(), "result").toPath());
    }

    @Test
    public void testCleanLocalCacheError() {
        AsyncProfilerUtils.getInstance().cleanLocalCache();
    }

    @Test
    public void testCleanLocalCache() throws IOException {
        AsyncProfilerUtils asyncProfilerUtils = init();
        asyncProfilerUtils.cleanLocalCache();
    }
}
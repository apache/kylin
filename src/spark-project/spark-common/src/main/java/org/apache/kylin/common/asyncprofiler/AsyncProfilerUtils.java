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

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.util.ZipFileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncProfilerUtils {

    private final Logger logger = LoggerFactory.getLogger(AsyncProfilerUtils.class);

    private static AsyncProfilerUtils asyncProfilerUtils;

    public static synchronized AsyncProfilerUtils getInstance() {
        if (asyncProfilerUtils == null) {
            asyncProfilerUtils = new AsyncProfilerUtils();
        }
        return asyncProfilerUtils;
    }

    CountDownLatch cachedResult;
    long resultCollectionTimeout;
    File localCacheDir;

    public void build(CountDownLatch countDownLatch) {
        this.cachedResult = countDownLatch;
    }

    public void build(File localCacheDir) {
        this.localCacheDir = localCacheDir;
    }

    public void build(long resultCollectionTimeout, File localCacheDir) {
        this.resultCollectionTimeout = resultCollectionTimeout;
        this.localCacheDir = localCacheDir;
    }

    public void waitForResult(OutputStream outStream) throws InterruptedException, IOException {
        if (!cachedResult.await(resultCollectionTimeout, TimeUnit.MILLISECONDS)) {
            logger.warn("timeout while waiting for profile result");
        }
        logger.debug("profiler stopped and result dumped to $localCacheDir");
        ZipFileUtils.compressZipFile(localCacheDir.getAbsolutePath(), outStream);
    }

    public String suffix(String content) {
        if (content.startsWith("<!DOCTYPE html>")) {
            return ".html";
        }
        return "";
    }

    public void cacheExecutorResult(String content, String executorId) {
        cacheResult(content, "executor-" + executorId + suffix(content));
        logger.debug("cached result from executor-{}", executorId);
        cachedResult.countDown();
    }

    public void cacheDriverResult(String content) {
        cacheResult(content, "driver" + suffix(content));
        logger.debug("cached result from driver");
        cachedResult.countDown();
    }

    public void cacheResult(String content, String destPath) {
        Path path = new File(localCacheDir.getAbsolutePath(), destPath).toPath();
        try {
            Files.write(path, content.getBytes(Charset.defaultCharset()));
        } catch (Exception e) {
            logger.error("error writing dumped data to disk", e);
        }
    }

    public void cleanLocalCache() {
        try {
            FileUtils.cleanDirectory(localCacheDir);
        } catch (Exception e) {
            logger.error("error clean cache directory", e);
        }
    }
}
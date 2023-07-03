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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.util.DaemonThreadFactory;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class CleanTaskExecutorService implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(CleanTaskExecutorService.class);

    private static final CleanTaskExecutorService INSTANCE = new CleanTaskExecutorService();

    private static final int SHUTDOWN_TIMEOUT_SECONDS = 2;

    private final ScheduledExecutorService timeoutCheckerPool = Executors
            .newSingleThreadScheduledExecutor(new DaemonThreadFactory("storage-cleaner-timeout-checker"));

    private final AtomicBoolean bound = new AtomicBoolean(false);
    // Should move it outside to gain a more sensible composition code.
    private ExecutorService pool;

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                LOGGER.info("[{}] Shutdown when JVM is to shutdown.", CleanTaskExecutorService.class.getName());
                INSTANCE.close();
                LOGGER.info("[{}] Shutdown successfully.", CleanTaskExecutorService.class.getName());
            } catch (Exception e) {
                LOGGER.error("[{}] Occurring exceptions to shutdown!", CleanTaskExecutorService.class.getName());
            }
        }));
    }

    private CleanTaskExecutorService() {
    }

    public static CleanTaskExecutorService getInstance() {
        return INSTANCE;
    }

    /**
     * True if the instance binds nothing, else false.
     */
    public boolean bindWorkingPool(ExecutorService workingPool) {
        Preconditions.checkArgument(workingPool != null, "workingPool is null");
        if (bound.compareAndSet(false, true)) {
            pool = workingPool;
            return true;
        }
        return false;
    }

    public CompletableFuture<Void> cleanStorageForService(boolean storageCleanup, List<String> projects,
            double requestFSRate, int retryTimes) {
        return cleanStorageAsync(RandomUtil.randomUUIDStr(), StorageCleaner.CleanerTag.SERVICE, storageCleanup,
                projects, requestFSRate, retryTimes);
    }

    public void cleanStorageForRoutine(boolean storageCleanup, List<String> projects, double requestFSRate,
            int retryTimes) {
        cleanStorageSync(StorageCleaner.CleanerTag.ROUTINE, storageCleanup, projects, requestFSRate, retryTimes);
    }

    private void cleanStorageSync(StorageCleaner.CleanerTag tag, boolean storageCleanup, List<String> projects,
            double requestFSRate, int retryTimes) {
        cleanStorageAsync(RandomUtil.randomUUIDStr(), tag, storageCleanup, projects, requestFSRate, retryTimes).join();
    }

    private CompletableFuture<Void> cleanStorageAsync(String traceId, StorageCleaner.CleanerTag tag,
            boolean storageCleanup, List<String> projects, double requestFSRate, int retryTimes) {
        StorageCleaner cleaner = makeCleaner(traceId, tag, projects, requestFSRate, retryTimes, storageCleanup);
        if (cleaner != null) {
            return cleanStorageAsync(cleaner);
        }
        return CompletableFuture.completedFuture(null);
    }

    private StorageCleaner makeCleaner(String traceId, StorageCleaner.CleanerTag tag, List<String> projects,
            double requestFSRate, int retryTimes, boolean storageCleanup) {
        Preconditions.checkArgument(projects != null, "projects is null");
        StorageCleaner storageCleaner = null;
        try {
            storageCleaner = new StorageCleaner(storageCleanup, projects, requestFSRate, retryTimes);
            storageCleaner.withTag(tag);
            storageCleaner.withTraceId(traceId);
        } catch (Exception e) {
            LOGGER.error(
                    "Failed to create storage cleaner for projects: {}. TraceId: {}", projects, traceId,
                    e);
        }
        return storageCleaner;
    }

    private CompletableFuture<Void> cleanStorageAsync(StorageCleaner cleaner) {
        LOGGER.info("To submit cleaning hdfs files task. TraceId: {}.", cleaner.getTraceId());
        CompletableFuture<Void> f = submit(cleaner, KylinConfig.getInstanceFromEnv().getStorageCleanTaskTimeout(),
                TimeUnit.MILLISECONDS);
        f.whenComplete((v, t) -> {
            if (t == null) {
                LOGGER.info("HDFS files cleaning task has successfully completed. TraceId: {}", cleaner.getTraceId());
                return;
            }
            LOGGER.error(StorageCleaner.ANSI_RED
                    + "cleanup HDFS failed. Detailed Message is at ${KYLIN_HOME}/logs/shell.stderr"
                    + StorageCleaner.ANSI_RESET + ". TraceId: " + cleaner.getTraceId(), t);
        });
        return f;
    }

    public CompletableFuture<Void> submit(StorageCleaner cleaner, long timeout, TimeUnit timeUnit) {
        return submit(new AbstractComparableCleanTask() {
            @Override
            public String getName() {
                return cleaner.getTraceId();
            }

            @Override
            protected String details() {
                return String.format("traceId: %s, tag: %s, projects: %s", cleaner.getTraceId(), cleaner.getTag(),
                        cleaner.getProjectNames().toString());
            }

            @Override
            public StorageCleaner.CleanerTag getCleanerTag() {
                return cleaner.getTag();
            }

            @Override
            protected void doRun() {
                try {
                    cleaner.execute();
                } catch (Exception e) {
                    throw new KylinRuntimeException(e);
                }
            }
        }, timeout, timeUnit);
    }

    public CompletableFuture<Void> submit(AbstractComparableCleanTask task, long timeout, TimeUnit timeUnit) {
        if (pool == null) {
            return CompletableFuture.completedFuture(null);
        }

        LOGGER.debug("To submit storage cleaning task {}.", task.getBrief());
        CompletableFuture<Void> resultFuture = task.getWatcher();
        AtomicReference<Future<?>> cancelFuture = new AtomicReference<>(null);
        try {
            if (!resultFuture.isDone()) {
                Future<?> workingFuture = pool.submit(task);
                LOGGER.info("Submitted storage cleaning task {}.", task.getBrief());

                cancelFuture.set(timeoutCheckerPool.schedule(() -> {
                    if (!workingFuture.isDone() && !workingFuture.cancel(true)) {
                        LOGGER.warn("You may have leaked threads, failed to cancel task {}!", task.getBrief());
                    }
                    resultFuture.completeExceptionally(new TimeoutException("Timeout for cleaning!"));
                }, timeout, timeUnit));
            }
        } catch (RejectedExecutionException re) {
            resultFuture.completeExceptionally(re);
        }

        resultFuture.whenComplete((v, t) -> {
            if (cancelFuture.get() != null) {
                cancelFuture.get().cancel(true);
            }
            if (t == null) {
                LOGGER.debug("Cleaning task {} successfully completed!", task.getBrief());
            } else {
                throw new CompletionException(
                        "To cancel task watcher because of " + "encountering exceptions for task " + task.getBrief(),
                        t);
            }
        });

        return resultFuture;
    }

    @Override
    public void close() throws IOException {
        if (pool != null) {
            try {
                pool.shutdown();
                if (!pool.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    LOGGER.warn("The working thread pool couldn't shutdown before timeout {}s!",
                            SHUTDOWN_TIMEOUT_SECONDS);
                    pool.shutdownNow();
                }
            } catch (InterruptedException ie) {
                pool.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
}

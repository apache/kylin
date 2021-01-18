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

package org.apache.kylin.stream.core.query;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.stream.core.storage.Record;
import org.apache.kylin.stream.core.util.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

public class MultiThreadsResultCollector extends ResultCollector {
    private static Logger logger = LoggerFactory.getLogger(MultiThreadsResultCollector.class);
    private static ThreadPoolExecutor scannerThreadPool;
    private static int MAX_RUNNING_THREAD_COUNT;

    static {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        MAX_RUNNING_THREAD_COUNT = config.getStreamingReceiverQueryMaxThreads();
        int coreThreads = config.getStreamingReceiverQueryCoreThreads();
        scannerThreadPool = new ThreadPoolExecutor(coreThreads,
                MAX_RUNNING_THREAD_COUNT, 60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(), new NamedThreadFactory("query-worker"));
    }

    private long deadline;
    private String queryId;

    /**
     * if current query exceeded the deadline
     */
    private AtomicBoolean cancelFlag = new AtomicBoolean(false);
    private Semaphore workersSemaphore;
    private AtomicInteger notCompletedWorkers;

    final private BlockingQueue<Record> recordCachePool = new LinkedBlockingQueue<>(10000);

    public MultiThreadsResultCollector(int numOfWorkers, long deadline) {
        this.workersSemaphore = new Semaphore(numOfWorkers);
        this.deadline = deadline;
        this.queryId = StreamingQueryProfile.get().getQueryId();
    }

    @Override
    public Iterator<Record> iterator() {
        notCompletedWorkers = new AtomicInteger(searchResults.size());
        Thread masterThread = new Thread(new WorkSubmitter(), "MultiThreadsResultCollector_" + queryId);
        masterThread.start();

        final int batchSize = 100;
        final long startTime = System.currentTimeMillis();
        return new Iterator<Record>() {
            List<Record> recordList = Lists.newArrayListWithExpectedSize(batchSize);
            Iterator<Record> internalIT = recordList.iterator();

            @Override
            public boolean hasNext() {
                boolean exits = (internalIT.hasNext() || !recordCachePool.isEmpty());
                if (!exits) {
                    while (notCompletedWorkers.get() > 0) {
                        Thread.yield();
                        if (System.currentTimeMillis() > deadline) {
                            masterThread.interrupt(); // notify main thread
                            cancelFlag.set(true);
                            recordCachePool.clear();
                            logger.warn("Beyond the deadline for {}.", queryId);
                            throw new RuntimeException("Timeout when iterate search result");
                        }
                        if (internalIT.hasNext() || !recordCachePool.isEmpty()) {
                            return true;
                        }
                    }
                }
                return exits;
            }

            @Override
            public Record next() {
                try {
                    if (System.currentTimeMillis() > deadline) {
                        throw new RuntimeException("Timeout when iterate search result");
                    }
                    if (!internalIT.hasNext()) {
                        recordList.clear();
                        Record one = recordCachePool.poll(deadline - startTime, TimeUnit.MILLISECONDS);
                        if (one == null) {
                            masterThread.interrupt(); // notify main thread
                            cancelFlag.set(true);
                            recordCachePool.clear();
                            logger.debug("Exceeded the deadline for {}.", queryId);
                            throw new RuntimeException("Timeout when iterate search result");
                        }
                        recordList.add(one);
                        recordCachePool.drainTo(recordList, batchSize - 1);
                        internalIT = recordList.iterator();
                    }
                    return internalIT.next();
                } catch (InterruptedException e) {
                    throw new RuntimeException("Error when waiting queue", e);
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("not support");
            }
        };
    }

    private class ResultIterateWorker implements Runnable {
        IStreamingSearchResult result;

        public ResultIterateWorker(IStreamingSearchResult result) {
            this.result = result;
        }

        @Override
        public void run() {
            long offserTimeout = 0L;
            try {
                result.startRead();
                for (Record record : result) {
                    offserTimeout = deadline - System.currentTimeMillis();
                    if (!recordCachePool.offer(record.copy(), offserTimeout, TimeUnit.MILLISECONDS)) {
                        logger.warn("Timeout when offer to recordCachePool, deadline: {}, offser Timeout: {}", deadline, offserTimeout);
                        break;
                    }
                }
                result.endRead();
            } catch (InterruptedException inter) {
                logger.debug("Cancelled scan streaming segment", inter);
            } catch (Exception e) {
                logger.warn("Error when iterate search result", e);
            } finally {
                notCompletedWorkers.decrementAndGet();
                workersSemaphore.release();
            }
        }
    }

    private class WorkSubmitter implements Runnable {
        @Override
        public void run() {
            List<Future> futureList = Lists.newArrayListWithExpectedSize(searchResults.size());
            int cancelTimes = 0;
            try {
                for (final IStreamingSearchResult result : searchResults) {
                    Future f = scannerThreadPool.submit(new ResultIterateWorker(result));
                    futureList.add(f);
                    workersSemaphore.acquire(); // Throw InterruptedException when interrupted
                }
                while (notCompletedWorkers.get() > 0) {
                    Thread.sleep(100);
                    if (cancelFlag.get() || Thread.currentThread().isInterrupted()) {
                        break;
                    }
                }
            } catch (InterruptedException inter) {
                logger.warn("Interrupted", inter);
            } finally {
                for (Future f : futureList) {
                    if (!f.isCancelled() || !f.isDone()) {
                        if (f.cancel(true)) {
                            cancelTimes++;
                        }
                    }
                }
            }
            logger.debug("Finish MultiThreadsResultCollector for queryId {}, cancel {}. Current thread pool: {}.",
                    queryId, cancelTimes, scannerThreadPool);
        }
    }

    /**
     * block query if return true
     */
    public static boolean isFullUp() {
        boolean occupied = scannerThreadPool.getActiveCount() >= MAX_RUNNING_THREAD_COUNT;
        if (occupied) {
            logger.debug("ThreadPool {} is full .", scannerThreadPool);
        }
        return occupied;
    }
}

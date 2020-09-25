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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.stream.core.storage.Record;
import org.apache.kylin.stream.core.util.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

public class MultiThreadsResultCollector extends ResultCollector {
    private static Logger logger = LoggerFactory.getLogger(MultiThreadsResultCollector.class);
    private static ExecutorService executor;
    static {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        executor = new ThreadPoolExecutor(config.getStreamingReceiverQueryCoreThreads(),
                config.getStreamingReceiverQueryMaxThreads(), 60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(), new NamedThreadFactory("query-worker"));
    }

    private int timeout;
    private Semaphore workersSemaphore;
    final BlockingQueue<Record> queue = new LinkedBlockingQueue<>(10000);
    private AtomicInteger notCompletedWorkers;

    public MultiThreadsResultCollector(int numOfWorkers, int timeout) {
        this.workersSemaphore = new Semaphore(numOfWorkers);
        this.timeout = timeout;
    }

    @Override
    public Iterator<Record> iterator() {
        notCompletedWorkers = new AtomicInteger(searchResults.size());
        executor.submit(new WorkSubmitter());

        final int batchSize = 100;
        final long startTime = System.currentTimeMillis();
        return new Iterator<Record>() {
            List<Record> recordList = Lists.newArrayListWithExpectedSize(batchSize);
            Iterator<Record> internalIT = recordList.iterator();

            @Override
            public boolean hasNext() {
                boolean exits = (internalIT.hasNext() || queue.size() > 0);
                if (!exits) {
                    while (notCompletedWorkers.get() > 0) {
                        Thread.yield();
                        long takeTime = System.currentTimeMillis() - startTime;
                        if (takeTime > timeout) {
                            throw new RuntimeException("Timeout when iterate search result");
                        }
                        if (internalIT.hasNext() || queue.size() > 0) {
                            return true;
                        }
                    }
                }

                return exits;
            }

            @Override
            public Record next() {
                try {
                    long takeTime = System.currentTimeMillis() - startTime;
                    if (takeTime > timeout) {
                        throw new RuntimeException("Timeout when iterate search result");
                    }
                    if (!internalIT.hasNext()) {
                        recordList.clear();
                        Record one = queue.poll(timeout - takeTime, TimeUnit.MILLISECONDS);
                        if (one == null) {
                            throw new RuntimeException("Timeout when iterate search result");
                        }
                        recordList.add(one);
                        queue.drainTo(recordList, batchSize - 1);
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
            try {
                result.startRead();
                for (Record record : result) {
                    try {
                        queue.put(record.copy());
                    } catch (InterruptedException e) {
                        throw new RuntimeException("Timeout when visiting streaming segmenent", e);
                    }
                }
                result.endRead();
            } catch (Exception e) {
                logger.error("error when iterate search result", e);
            } finally {
                notCompletedWorkers.decrementAndGet();
                workersSemaphore.release();
            }
        }
    }

    private class WorkSubmitter implements Runnable {
        @Override
        public void run() {
            for (final IStreamingSearchResult result : searchResults) {
                executor.submit(new ResultIterateWorker(result));
                try {
                    workersSemaphore.acquire();
                } catch (InterruptedException e) {
                    logger.error("interrupted", e);
                }
            }
        }
    }

}

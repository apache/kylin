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
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.MoreExecutors;

public class SubThreadPoolExecutor extends AbstractExecutorService {

    private final static Logger logger = LoggerFactory.getLogger(SubThreadPoolExecutor.class);

    private final Semaphore semaphore;

    private final ExecutorService impl;

    private final String subject;

    public SubThreadPoolExecutor(ExecutorService impl, String subject, int maxThreads) {
        this.impl = impl;
        this.subject = subject;
        this.semaphore = new Semaphore(maxThreads);
    }

    // Obtain a thread resource. If no resources, block it
    private void obtainThread() {
        try {
            semaphore.acquire();
            logger.debug("Obtain thread for {}", subject);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    // Release a thread resource
    private void releaseThread() {
        semaphore.release();
        logger.debug("Release thread for {}", subject);
    }

    public void execute(Runnable command) {
        obtainThread();
        impl.execute(command);
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
        ListenableFutureTask<T> ret = impl instanceof SubThreadPoolExecutor
                ? (ListenableFutureTask) ((SubThreadPoolExecutor) impl).newTaskFor(runnable, value)
                : ListenableFutureTask.create(runnable, value);
        ret.addListener(new Runnable() {
            @Override
            public void run() {
                releaseThread();
            }
        }, MoreExecutors.sameThreadExecutor());
        return ret;
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
        ListenableFutureTask<T> ret = impl instanceof SubThreadPoolExecutor
                ? (ListenableFutureTask) ((SubThreadPoolExecutor) impl).newTaskFor(callable)
                : ListenableFutureTask.create(callable);
        ret.addListener(new Runnable() {
            @Override
            public void run() {
                releaseThread();
            }
        }, MoreExecutors.sameThreadExecutor());
        return ret;
    }

    @Override
    public void shutdown() {
        throw new IllegalStateException(
                "Manual shutdown not supported - SubThreadPoolExecutor is dependent on an external lifecycle");
    }

    @Override
    public List<Runnable> shutdownNow() {
        throw new IllegalStateException(
                "Manual shutdown not supported - SubThreadPoolExecutor is dependent on an external lifecycle");
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        throw new IllegalStateException(
                "Manual shutdown not supported - SubThreadPoolExecutor is dependent on an external lifecycle");
    }

    @Override
    public boolean isShutdown() {
        return false;
    }

    @Override
    public boolean isTerminated() {
        return false;
    }
}
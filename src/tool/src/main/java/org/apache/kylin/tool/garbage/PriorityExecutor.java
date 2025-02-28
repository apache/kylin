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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.FutureTask;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.util.DaemonThreadFactory;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;

import lombok.EqualsAndHashCode;

public class PriorityExecutor extends ThreadPoolExecutor {

    @EqualsAndHashCode
    public static class ComparableFutureTask<T extends Comparable<?>, R> extends FutureTask<R>
            implements Comparable<ComparableFutureTask<T, R>> {

        private final Comparable<T> task;

        public ComparableFutureTask(Runnable runnable, R result) {
            super(runnable, result);
            Preconditions.checkArgument(runnable instanceof Comparable, "runnable should also be comparable!");
            this.task = (Comparable<T>) runnable;
        }

        /**
         * Note: this class has a natural ordering that is inconsistent with equals.
         */
        @Override
        public int compareTo(ComparableFutureTask<T, R> o) {
            return task.compareTo((T) o.task);
        }
    }

    private PriorityExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
            BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
    }

    public static PriorityExecutor newWorkingThreadPool(String name, int maxPoolSize) {
        return new PriorityExecutor(maxPoolSize, maxPoolSize, 60L, TimeUnit.SECONDS, new PriorityBlockingQueue<>(),
                new DaemonThreadFactory(name));
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
        return new ComparableFutureTask<>(runnable, null);
    }

}

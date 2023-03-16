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
package org.apache.kylin.common.persistence.transaction;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.kylin.common.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.guava30.shaded.common.collect.Maps;

import lombok.experimental.Delegate;

public class TransactionLock {

    static Map<String, Pair<ReentrantReadWriteLock, AtomicLong>> projectLocks = Maps.newConcurrentMap();
    private static final Logger logger = LoggerFactory.getLogger(TransactionLock.class);

    public static TransactionLock getLock(String project, boolean readonly) {
        Pair<ReentrantReadWriteLock, AtomicLong> lockPair = projectLocks.get(project);
        if (lockPair == null) {
            synchronized (UnitOfWork.class) {
                lockPair = projectLocks.computeIfAbsent(project,
                        k -> Pair.newPair(new ReentrantReadWriteLock(true), new AtomicLong()));
            }
        }
        lockPair.getSecond().incrementAndGet();
        ReentrantReadWriteLock lock = lockPair.getFirst();
        return new TransactionLock(lock, readonly ? lock.readLock() : lock.writeLock());
    }

    public static void removeLock(String project) {
        Pair<ReentrantReadWriteLock, AtomicLong> lockPair = projectLocks.get(project);
        if (lockPair != null) {
            synchronized (UnitOfWork.class) {
                AtomicLong atomicLong = lockPair.getSecond();
                if (atomicLong.decrementAndGet() == 0) {
                    projectLocks.remove(project);
                }

            }
        }
        if (projectLocks.size() > 5000L) {
            projectLocks.forEach((k, v) -> logger.warn("lock leaks lock: {}，num: {}", k, v.getSecond()));
        }
    }

    public static boolean isWriteLocked(String project) {
        ReentrantReadWriteLock lock = projectLocks.get(project).getFirst();
        return lock != null && lock.isWriteLocked();
    }

    private ReentrantReadWriteLock rootLock;

    @Delegate
    private Lock lock;

    public TransactionLock(ReentrantReadWriteLock rootLock, Lock lock) {
        this.rootLock = rootLock;
        this.lock = lock;
    }

    boolean isHeldByCurrentThread() {
        if (lock instanceof ReentrantReadWriteLock.ReadLock) {
            return rootLock.getReadHoldCount() > 0;
        } else {
            return rootLock.getWriteHoldCount() > 0;
        }
    }

    public static Map<String, ReentrantReadWriteLock> getProjectLocksForRead() {
        Map<String, ReentrantReadWriteLock> map = Maps.newConcurrentMap();
        projectLocks.forEach((k, v) -> map.put(k, v.getFirst()));
        return Collections.unmodifiableMap(map);
    }
}

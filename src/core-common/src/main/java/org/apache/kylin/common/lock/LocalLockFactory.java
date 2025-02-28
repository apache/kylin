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

package org.apache.kylin.common.lock;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * A local version of the distributed lock factory.
 * Wraps around the ReentrantLock, a straightforward implementation.
 */
@Slf4j
public class LocalLockFactory extends DistributedLockFactory {

    public Lock getLockForClient(String client, String lockId) {
        return getLockInternal(client, lockId);
    }

    private static final HashMap<String, ReentrantLock> cache = new HashMap<>();

    private Lock getLockInternal(String client, String lockId) {
        synchronized (cache) {
            cache.putIfAbsent(lockId, new ReentrantLock());
            ReentrantLock lock = cache.get(lockId);
            return new LocalLock(lock, client, lockId);
        }
    }

    public void initialize() {
        // nothing to do
    }

    @AllArgsConstructor
    private static class LocalLock implements Lock {
        private ReentrantLock lock;
        private String client;
        private String lockId;

        @Override
        public void lock() {
            log.debug(client + " locking " + lockId);
            lock.lock();
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            lock.lockInterruptibly();
        }

        @Override
        public boolean tryLock() {
            return lock.tryLock();
        }

        @Override
        public boolean tryLock(long l, TimeUnit timeUnit) throws InterruptedException {
            return lock.tryLock(l, timeUnit);
        }

        @Override
        public void unlock() {
            synchronized (cache) {
                log.debug(client + " unlocking " + lockId);
                lock.unlock();
                cache.remove(lockId);
            }
        }

        @Override
        public Condition newCondition() {
            return lock.newCondition();
        }
    }
}

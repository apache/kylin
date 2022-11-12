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

package org.apache.kylin.common.lock.curator;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import org.apache.kylin.common.exception.DistributedLockException;
import org.apache.kylin.common.util.ThrowableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import io.kyligence.kap.shaded.curator.org.apache.curator.framework.CuratorFramework;
import io.kyligence.kap.shaded.curator.org.apache.curator.framework.recipes.locks.InterProcessMutex;

public class CuratorDistributedLock implements Lock {
    private static final Logger logger = LoggerFactory.getLogger(CuratorDistributedLock.class);
    private static final String ZK_ROOT = "/distribute_lock";

    static final Map<CuratorFramework, ConcurrentMap<LockEntry, Boolean>> lockedThreads = Maps.newConcurrentMap();
    private InterProcessMutex lock;
    private CuratorFramework client;
    private String path;
    private long clientSessionId = -1;

    CuratorDistributedLock(CuratorFramework client, String path) {
        this.path = ZK_ROOT + fixPath(path);
        this.lock = new InterProcessMutex(client, this.path);
        this.client = client;
        try {
            this.clientSessionId = client.getZookeeperClient().getZooKeeper().getSessionId();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to get zk Session Id of " + client, e);
        }

        if (!lockedThreads.containsKey(client)) {
            lockedThreads.put(client, Maps.newConcurrentMap());
        }
    }

    private String fixPath(String path) {
        return path.startsWith("/") ? path : "/" + path;
    }

    public void lock() {
        try {
            if (isAcquiredInThisThread()) {
                logger.info("Thread: {} already own the lock, for path: {}, zk Session Id: {}",
                        Thread.currentThread().getId(), path, clientSessionId);
                return;
            }

            LockEntry lockEntry = new LockEntry(Thread.currentThread(), this.path);

            lockedThreads.get(client).put(lockEntry, false);

            logger.info("Thread: {} try to get lock, for path: {}, zk Session Id: {}", Thread.currentThread().getId(),
                    path, clientSessionId);

            lock.acquire();

            lockedThreads.get(client).put(lockEntry, true);
            logger.info("Thread: {} get the lock, for path: {}, zk Session Id: {}", Thread.currentThread().getId(),
                    path, clientSessionId);
        } catch (Exception e) {
            try {
                unlock();
            } catch (Exception ee) {
                logger.error("Faild to release lock, zk Session Id: {}", clientSessionId, ee);
            }

            throw new DistributedLockException("Failed to get curator distributed lock for path: " + path, e);
        }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean tryLock() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    public boolean tryLock(long time, TimeUnit unit) {
        try {
            if (isAcquiredInThisThread()) {
                logger.info("Thread: {} already own the lock, for path: {}, zk Session Id: {}",
                        Thread.currentThread().getId(), path, clientSessionId);
                return true;
            }

            LockEntry lockEntry = new LockEntry(Thread.currentThread(), this.path);

            lockedThreads.get(client).put(lockEntry, false);
            logger.info("Thread: {} try to get lock, for path: {}, zk Session Id: {}", Thread.currentThread().getId(),
                    path, clientSessionId);

            boolean acquired = lock.acquire(time, unit);

            if (acquired) {
                lockedThreads.get(client).put(lockEntry, true);
                logger.info("Thread: {} get the lock, for path: {}, zk Session Id: {}", Thread.currentThread().getId(),
                        path, clientSessionId);
            } else {
                lockedThreads.get(client).remove(lockEntry);
                logger.info("Thread: {} get lock timeout, for path: {}, zk Session Id: {}",
                        Thread.currentThread().getId(), path, clientSessionId);
            }

            return acquired;
        } catch (Exception e) {
            try {
                unlock();
            } catch (Exception ee) {
                logger.error("Faild to release lock, zk Session Id: {}", clientSessionId, ee);
            }

            throw new DistributedLockException(
                    "Failed to get curator distributed lock, for path: " + path + ",zk Session Id: " + clientSessionId,
                    e);
        }
    }

    public void unlock() {
        try {
            unlockInternal();
        } catch (Exception e) {
            if (ThrowableUtils.isInterruptedException(e)) {
                logger.info("unlock failed due to interrupt, re-unlock it for path {}, zk Session Id: {}", path,
                        clientSessionId);
                try {
                    unlockInternal();
                } catch (Exception ee) {
                    logger.error("Failed to re-unlock for path {}, zk Session Id: {}", path, clientSessionId, ee);
                }
            }
            throw new DistributedLockException("Failed to release curator distributed lock for path: " + path
                    + ",zk Session Id: " + clientSessionId, e);
        }
    }

    private void unlockInternal() throws Exception {
        logger.info("Thread: {} try to release lock, for path: {}, zk Session Id: {}", Thread.currentThread().getId(),
                path, clientSessionId);

        if (isAcquiredInThisThread()) {
            lock.release();

            lockedThreads.get(client).remove(new LockEntry(Thread.currentThread(), this.path));
            logger.info("Thread: {} release lock, for path: {}, zk Session Id: {}", Thread.currentThread().getId(),
                    path, clientSessionId);
        } else {
            logger.warn("Thread: {} do not own the lock, for path: {}, zk Session Id: {}",
                    Thread.currentThread().getId(), path, clientSessionId);
        }
    }

    public boolean isAcquiredInThisThread() {
        return lock.isOwnedByCurrentThread();
    }

    static class LockEntry {
        Thread thread;
        String path;

        LockEntry(Thread thread, String path) {
            this.thread = thread;
            this.path = path;
        }

        public Thread getThread() {
            return thread;
        }

        public void setThread(Thread thread) {
            this.thread = thread;
        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            LockEntry lockEntry = (LockEntry) o;
            return Objects.equals(thread, lockEntry.thread) && Objects.equals(path, lockEntry.path);
        }

        @Override
        public int hashCode() {
            return Objects.hash(thread, path);
        }
    }
}

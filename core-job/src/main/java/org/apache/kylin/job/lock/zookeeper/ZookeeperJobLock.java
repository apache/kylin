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

package org.apache.kylin.job.lock.zookeeper;

import java.io.Closeable;
import java.util.concurrent.Executor;

import org.apache.kylin.common.lock.DistributedLock;
import org.apache.kylin.job.lock.JobLock;

/**
 * A simple delegator to ZookeeperDistributedLock with a default constructor.
 */
public class ZookeeperJobLock implements DistributedLock, JobLock {

    private ZookeeperDistributedLock lock = (ZookeeperDistributedLock) new ZookeeperDistributedLock.Factory().lockForCurrentProcess();

    @Override
    public String getClient() {
        return lock.getClient();
    }

    @Override
    public boolean lock(String lockPath) {
        return lock.lock(lockPath);
    }

    @Override
    public boolean globalPermanentLock(String lockPath) {
        return lock.globalPermanentLock(lockPath);
    }

    @Override
    public boolean lock(String lockPath, long timeout) {
        return lock.lock(lockPath, timeout);
    }

    @Override
    public String peekLock(String lockPath) {
        return lock.peekLock(lockPath);
    }

    @Override
    public boolean isLocked(String lockPath) {
        return lock.isLocked(lockPath);
    }

    @Override
    public boolean isLockedByMe(String lockPath) {
        return lock.isLockedByMe(lockPath);
    }

    @Override
    public void unlock(String lockPath) {
        lock.unlock(lockPath);
    }

    @Override
    public void purgeLocks(String lockPathRoot) {
        lock.purgeLocks(lockPathRoot);
    }

    @Override
    public Closeable watchLocks(String lockPathRoot, Executor executor, Watcher watcher) {
        return lock.watchLocks(lockPathRoot, executor, watcher);
    }

    @Override
    public boolean lockJobEngine() {
        return lock.lockJobEngine();
    }

    @Override
    public void unlockJobEngine() {
        lock.unlockJobEngine();
    }

}

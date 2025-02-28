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

package org.apache.kylin.job.core.lock;

import java.util.concurrent.TimeUnit;

import lombok.Getter;
import lombok.Setter;

public class JdbcJobLock implements JobLock {

    private final String lockId;
    private final String lockNode;

    private final long renewalSec;
    private final long renewalDelaySec;

    private final JdbcLockClient lockClient;
    private final LockAcquireListener acquireListener;
    // Now only used for streaming jobLock
    @Getter
    @Setter
    private boolean isLocked = false;

    public JdbcJobLock(String lockId, String lockNode, long renewalSec, double renewalRatio, JdbcLockClient lockClient,
            LockAcquireListener acquireListener) {
        this.lockId = lockId;
        this.lockNode = lockNode;
        this.renewalSec = renewalSec;
        this.renewalDelaySec = (int) (renewalRatio * renewalSec);
        this.lockClient = lockClient;
        this.acquireListener = acquireListener;
    }

    @Override
    public boolean tryAcquire() throws LockException {
        return lockClient.tryAcquire(this);
    }

    @Override
    public boolean tryAcquire(long time, TimeUnit unit) throws LockException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean tryRelease() throws LockException {
        return lockClient.tryRelease(this);
    }

    @Override
    public void stopRenew() {
        lockClient.stopRenew(lockId);
    }

    @Override
    public String toString() {
        return "JdbcJobLock{" + "lockId='" + lockId + '\'' + ", lockNode='" + lockNode + '\'' + '}';
    }

    public String getLockId() {
        return lockId;
    }

    public String getLockNode() {
        return lockNode;
    }

    public long getRenewalSec() {
        return renewalSec;
    }

    public long getRenewalDelaySec() {
        return renewalDelaySec;
    }

    public LockAcquireListener getAcquireListener() {
        return acquireListener;
    }
}

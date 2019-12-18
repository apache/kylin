/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

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

package io.kyligence.kap.engine.spark.utils;

import java.io.Closeable;
import java.nio.charset.Charset;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import org.apache.curator.framework.CuratorFramework;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.lock.DistributedLock;
import org.apache.kylin.common.lock.DistributedLockFactory;
import org.apache.kylin.common.util.AutoReadWriteLock;
import org.apache.kylin.job.lock.JobLock;

public class MockedDistributedLock implements DistributedLock, JobLock {

    public static class MockedFactory extends DistributedLockFactory {

        private static final ConcurrentMap<KylinConfig, CuratorFramework> CACHE = new ConcurrentHashMap<KylinConfig, CuratorFramework>();

        private static CuratorFramework getZKClient(KylinConfig config) {
            CuratorFramework zkClient = CACHE.get(config);
            return zkClient;
        }

        final String zkPathBase;
        final CuratorFramework curator;

        public MockedFactory() {
            this(KylinConfig.getInstanceFromEnv());
        }

        public MockedFactory(KylinConfig config) {
            this.curator = getZKClient(config);
            this.zkPathBase = "/";
        }

        @Override
        public DistributedLock lockForClient(String client) {
            return new MockedDistributedLock(curator, zkPathBase, client);
        }
    }

    final CuratorFramework curator;
    final String zkPathBase;
    final String client;
    final byte[] clientBytes;

    private static AutoReadWriteLock mockLock = new AutoReadWriteLock();

    private MockedDistributedLock(CuratorFramework curator, String zkPathBase, String client) {
        if (client == null)
            throw new NullPointerException("client must not be null");
        if (zkPathBase == null)
            throw new NullPointerException("zkPathBase must not be null");

        this.curator = curator;
        this.zkPathBase = zkPathBase;
        this.client = client;
        this.clientBytes = client.getBytes(Charset.forName("UTF-8"));
    }

    @Override
    public String getClient() {
        return null;
    }

    @Override
    public boolean lock(String lockPath) {
        return true;
    }

    @Override
    public boolean lock(String lockPath, long timeout) {
        mockLock.lockForWrite();
        return false;
    }

    @Override
    public boolean isLocked(String lockPath) {
        return false;
    }

    @Override
    public boolean isLockedByMe(String lockPath) {
        return false;
    }

    @Override
    public String peekLock(String lockPath) {
        return null;
    }

    @Override
    public void unlock(String lockPath) throws IllegalStateException {
        mockLock.innerLock().writeLock().unlock();
    }

    @Override
    public void purgeLocks(String lockPathRoot) {

    }

    @Override
    public Closeable watchLocks(String lockPathRoot, Executor executor, Watcher watcher) {
        return null;
    }

    @Override
    public boolean lockJobEngine() {
        return false;
    }

    @Override
    public void unlockJobEngine() {

    }
}

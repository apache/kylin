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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.junit.jupiter.api.Test;

class LocalLockFactoryTest {

    @Test
    void testLocking() throws InterruptedException {

        LocalLockFactory lockFactory = new LocalLockFactory();
        lockFactory.initialize();

        String client = "client1";
        String lockId = "lock1";

        Lock lock1 = lockFactory.getLockForClient(client, lockId);
        lock1.lock();
        lock1.unlock();

        assertTrue(lock1.tryLock());
        assertTrue(lock1.tryLock(30, TimeUnit.SECONDS));

        lock1.lockInterruptibly();
        lock1.newCondition();
    }
}

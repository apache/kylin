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

import static org.apache.kylin.common.util.TestUtils.getTestConfig;
import static org.awaitility.Awaitility.await;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.apache.curator.test.TestingServer;
import org.apache.kylin.common.lock.DistributedLockFactoryTest;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.RetryingTest;
import org.springframework.test.util.ReflectionTestUtils;

import io.kyligence.kap.shaded.curator.org.apache.curator.framework.CuratorFramework;
import io.kyligence.kap.shaded.curator.org.apache.curator.framework.state.ConnectionState;
import io.kyligence.kap.shaded.curator.org.apache.curator.framework.state.ConnectionStateListener;

@Disabled("TODO: re-run to check.")
@MetadataInfo(onlyProps = true)
class CuratorDistributedLockFactoryTest extends DistributedLockFactoryTest {

    private TestingServer zkTestServer;
    private volatile boolean locked = false;
    private volatile boolean isInterrupted = false;

    @BeforeEach
    public void setup() throws Exception {
        zkTestServer = new TestingServer(true);
    }

    @AfterEach
    public void after() throws Exception {
        zkTestServer.close();
    }

    @Test
    public void testBasic() throws Exception {
        String path = "/test/distributed_lock_factory_test/test_basic/" + RandomUtil.randomUUIDStr();

        getTestConfig().setProperty("kylin.env.zookeeper-connect-string", zkTestServer.getConnectString());
        CuratorDistributedLock lock = (CuratorDistributedLock) getTestConfig().getDistributedLockFactory().getLockForCurrentThread(path);

        Assert.assertFalse(lock.isAcquiredInThisThread());
        lock.lock();
        Assert.assertTrue(lock.isAcquiredInThisThread());
        lock.unlock();
        Assert.assertFalse(lock.isAcquiredInThisThread());
    }

    @RetryingTest(3)
    public void testInterruptWhenLost() throws Exception {
        String path = "/test/distributed_lock_factory_test/test_interrupt_lost/" + RandomUtil.randomUUIDStr();
        TestingServer zkTestServer2 = new TestingServer(true);

        ExecutorService executorService = Executors.newFixedThreadPool(1);

        getTestConfig().setProperty("kylin.env.zookeeper-connect-string", zkTestServer.getConnectString());
        getTestConfig().setProperty("kap.env.zookeeper-max-retries", "1");
        getTestConfig().setProperty("kap.env.zookeeper-base-sleep-time", "1000");

        executorService.submit(() -> {
            CuratorDistributedLock lock = null;
            try {
                lock = (CuratorDistributedLock)getTestConfig().getDistributedLockFactory().getLockForCurrentThread(path);
            } catch (Exception e) {
                e.printStackTrace();
            }

            lock.lock();
            locked = true;

            try {
                Thread.sleep(20000);
            } catch (InterruptedException e) {
                isInterrupted = true;
            }
        });

        await().atMost(5, TimeUnit.SECONDS).until(() -> locked);
        Assert.assertFalse(isInterrupted);

        locked = false;
        zkTestServer.stop();

        // zk for thread1 lost
        // thread1 will be interrupted
        await().atMost(20, TimeUnit.SECONDS).until(() -> isInterrupted);

        Assert.assertFalse(locked);

        getTestConfig().setProperty("kylin.env.zookeeper-connect-string", zkTestServer2.getConnectString());
        executorService.submit(() -> {
            Lock lock = null;
            try {
                lock = getTestConfig().getDistributedLockFactory().getLockForCurrentThread(path);
            } catch (Exception e) {
                e.printStackTrace();
            }
            lock.lock();
            locked = true;
            lock.unlock();
        });

        // thread1 released the lock
        // thread2 will get the lock
        await().atMost(5, TimeUnit.SECONDS).until(() -> locked);
    }

    @Test
    public void testInterruptWhenSuspended() throws Exception {
        String path = "/test/distributed_lock_factory_test/test_interrupt_suspended/" + RandomUtil.randomUUIDStr();
        TestingServer zkTestServer2 = new TestingServer(true);

        ExecutorService executorService = Executors.newFixedThreadPool(1);
        CuratorDistributedLockFactory lockFactory;
        CuratorDistributedLock lock1;
        getTestConfig().setProperty("kylin.env.zookeeper-connect-string", zkTestServer.getConnectString());
        getTestConfig().setProperty("kap.env.zookeeper-max-retries", "1");
        getTestConfig().setProperty("kap.env.zookeeper-base-sleep-time", "1000");
        lockFactory = (CuratorDistributedLockFactory) getTestConfig().getDistributedLockFactory();
        lock1 = lockFactory.getLockForCurrentThread(path);
        executorService.submit(() -> {

            lock1.lock();
            locked = true;

            try {
                Thread.sleep(20000);
            } catch (InterruptedException e) {
                isInterrupted = true;
            }
        });

        await().atMost(5, TimeUnit.SECONDS).until(() -> locked);

        Assert.assertFalse(isInterrupted);

        ConnectionStateListener listener = (ConnectionStateListener) ReflectionTestUtils.getField(lockFactory,
                "listener");
        CuratorFramework client = (CuratorFramework) ReflectionTestUtils.getField(lockFactory, "client");
        listener.stateChanged(client, ConnectionState.SUSPENDED);

        // zk for thread1 suspended
        // thread1 will be interrupted
        await().atMost(5, TimeUnit.SECONDS).until(() -> isInterrupted);
    }

    @Test
    void testConcurrence() throws Exception {
        getTestConfig().setProperty("kylin.env.zookeeper-connect-string", zkTestServer.getConnectString());
        String key = "/test/distributed_lock_factory_test/test_concurrence/" + RandomUtil.randomUUIDStr();
        super.testConcurrence(key, 10, 10);
    }
}

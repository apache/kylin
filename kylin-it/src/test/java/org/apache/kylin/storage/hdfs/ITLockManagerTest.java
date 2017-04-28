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
package org.apache.kylin.storage.hdfs;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.apache.kylin.storage.hbase.util.ZookeeperUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;

public class ITLockManagerTest extends HBaseMetadataTestCase {

    private KylinConfig kylinConfig;

    private CuratorFramework zkClient;

    private static final String lockRootPath = "/test_lock";

    private LockManager manager;

    private static final int QTY = 5;

    private static final int REPETITIONS = QTY * 10;

    private static final Logger logger = LoggerFactory.getLogger(ITLockManagerTest.class);

    @Before
    public void setup() throws Exception {
        this.createTestMetadata();
        kylinConfig = KylinConfig.getInstanceFromEnv();
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        zkClient = CuratorFrameworkFactory.newClient(ZookeeperUtil.getZKConnectString(), retryPolicy);
        zkClient.start();
        manager = new LockManager(kylinConfig, lockRootPath);
        logger.info("nodes in lock root : " + zkClient.getChildren().forPath(lockRootPath));

    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
        zkClient.delete().deletingChildrenIfNeeded().forPath(lockRootPath);
        List<String> nodes = zkClient.getChildren().forPath("/");
        logger.info("nodes in zk after delete : " + nodes);
        manager.close();
    }

    @Test
    public void testCreateLock() throws Exception {

        ResourceLock lock = manager.getLock("/dictionary/numberdict.json");
        lock.acquire();
        manager.releaseLock(lock);
        logger.info(zkClient.getChildren().forPath(lockRootPath + "/dictionary").toString());
        List<String> nodes = zkClient.getChildren().forPath(lockRootPath + "/dictionary");
        assertEquals(1, nodes.size());
        assertEquals("numberdict.json", nodes.get(0));
    }

    @Test
    public void testLockSafty() throws Exception {
        // all of the useful sample code is in ExampleClientThatLocks.java

        // FakeLimitedResource simulates some external resource that can only be access by one process at a time
        final FakeLimitedResource resource = new FakeLimitedResource();
        ExecutorService service = Executors.newFixedThreadPool(QTY);
        final TestingServer server = new TestingServer(ZookeeperUtil.getZKConnectString());
        final List<FutureTask<Void>> tasks = new ArrayList<>();
        try {
            for (int i = 0; i < QTY; ++i) {
                final int index = i;
                FutureTask<Void> task = new FutureTask<Void>(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        LockManager threadLocalLockManager = new LockManager(kylinConfig, lockRootPath);
                        try {
                            ExampleClientThatLocks example = new ExampleClientThatLocks(threadLocalLockManager, lockRootPath, resource, "Client " + index);
                            for (int j = 0; j < REPETITIONS; ++j) {
                                example.doWork(10, TimeUnit.SECONDS);
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } catch (Exception e) {
                            e.printStackTrace();
                            // log or do something
                        } finally {
                            threadLocalLockManager.close();
                        }
                        return null;
                    }
                });
                tasks.add(task);
                service.submit(task);
            }
            for (FutureTask<Void> task : tasks) {
                task.get();
            }
        } finally {
            CloseableUtils.closeQuietly(server);
        }
    }

    class FakeLimitedResource {
        private final AtomicBoolean inUse = new AtomicBoolean(false);

        public void use() throws InterruptedException {
            // in a real application this would be accessing/manipulating a shared resource

            if (!inUse.compareAndSet(false, true)) {
                throw new IllegalStateException("Needs to be used by one client at a time");
            }

            try {
                Thread.sleep((long) (3 * Math.random()));
            } finally {
                inUse.set(false);
            }
        }
    }

    class TestingServer implements Closeable {

        private String connectionString;

        public TestingServer(String connectionStr) {
            this.connectionString = connectionStr;
        }

        @Override
        public void close() throws IOException {

        }

        public String getConnectString() {
            return connectionString;
        }
    }

    class ExampleClientThatLocks {

        private final FakeLimitedResource resource;

        private final String clientName;

        private LockManager lockManager;

        private String lockPath;

        public ExampleClientThatLocks(LockManager lockManager, String lockPath, FakeLimitedResource resource, String clientName) {
            this.resource = resource;
            this.clientName = clientName;
            this.lockManager = lockManager;
            this.lockPath = lockPath;
        }

        public void doWork(long time, TimeUnit unit) throws Exception {
            ResourceLock lock = lockManager.getLock(lockPath);
            lock.acquire(time, unit);
            try {
                logger.info(clientName + " has the lock");
                resource.use();
            } finally {
                logger.info(clientName + " releasing the lock");
                lock.release(); // always release the lock in a finally block
            }
        }
    }

}

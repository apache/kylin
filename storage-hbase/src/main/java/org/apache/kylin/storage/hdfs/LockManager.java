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

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.storage.hbase.util.ZookeeperDistributedJobLock;
import org.apache.kylin.storage.hbase.util.ZookeeperUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class LockManager {

    private static Logger logger = LoggerFactory.getLogger(ZookeeperDistributedJobLock.class);

    final private KylinConfig config;

    final CuratorFramework zkClient;

    private String lockRootPath;

    public LockManager(String lockRootPath) throws Exception {

        this(KylinConfig.getInstanceFromEnv(), lockRootPath);
    }

    public LockManager(KylinConfig config, String lockRootPath) throws Exception {
        this.config = config;
        this.lockRootPath = lockRootPath;
        String zkConnectString = ZookeeperUtil.getZKConnectString();
        logger.info("zk connection string:" + zkConnectString);
        if (StringUtils.isEmpty(zkConnectString)) {
            throw new IllegalArgumentException("ZOOKEEPER_QUORUM is empty!");
        }
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        zkClient = CuratorFrameworkFactory.newClient(zkConnectString, retryPolicy);
        zkClient.start();
        if (zkClient.checkExists().forPath(lockRootPath) == null)
            zkClient.create().creatingParentsIfNeeded().forPath(lockRootPath);
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                close();
            }
        }));
    }

    public ResourceLock getLock(String name) throws Exception {
        String lockPath = getLockPath(name);
        InterProcessMutex lock = new InterProcessMutex(zkClient, lockPath);
        return new ResourceLock(lockPath, lock);
    }

    public void releaseLock(ResourceLock lock) throws IOException {
        try {
            if (lock != null)
                lock.release();
        } catch (Exception e) {
            throw new IOException("Fail to release lock", e);
        }

    }


    public String getLockPath(String resourceName) {
        if (!resourceName.startsWith("/"))
            resourceName = "/" + resourceName;
        if (resourceName.endsWith("/"))
            resourceName = resourceName.substring(0, resourceName.length() - 1);
        return lockRootPath + resourceName;
    }

    public void close() {
        try {
            zkClient.close();
        } catch (Exception e) {
            logger.error("error occurred to close PathChildrenCache", e);
        }
    }

}

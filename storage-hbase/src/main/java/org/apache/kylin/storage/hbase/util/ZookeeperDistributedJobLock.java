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

package org.apache.kylin.storage.hbase.util;

import java.nio.charset.Charset;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.lock.DistributedJobLock;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperDistributedJobLock implements DistributedJobLock {
    private static Logger logger = LoggerFactory.getLogger(ZookeeperDistributedJobLock.class);

    @SuppressWarnings("unused")
    private final KylinConfig config;

    private static final ConcurrentMap<KylinConfig, CuratorFramework> CACHE = new ConcurrentHashMap<KylinConfig, CuratorFramework>();
    private final CuratorFramework zkClient;

    public ZookeeperDistributedJobLock() {
        this(KylinConfig.getInstanceFromEnv());
    }

    public ZookeeperDistributedJobLock(KylinConfig config) {
        this.config = config;

        String zkConnectString = ZookeeperUtil.getZKConnectString();
        if (StringUtils.isEmpty(zkConnectString)) {
            throw new IllegalArgumentException("ZOOKEEPER_QUORUM is empty!");
        }

        zkClient = getZKClient(config, zkConnectString);

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                close();
            }
        }));
    }

    //make the zkClient to be singleton
    private static CuratorFramework getZKClient(KylinConfig config, String zkConnectString) {
        CuratorFramework zkClient = CACHE.get(config);
        if (zkClient == null) {
            synchronized (ZookeeperDistributedJobLock.class) {
                zkClient = CACHE.get(config);
                if (zkClient == null) {
                    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
                    zkClient = CuratorFrameworkFactory.newClient(zkConnectString, 120000, 15000, retryPolicy);
                    zkClient.start();
                    CACHE.put(config, zkClient);
                    if (CACHE.size() > 1) {
                        logger.warn("More than one singleton exist");
                    }
                }
            }
        }
        return zkClient;
    }

    /**
     * Try locking the path with the lockPath and lockClient, if lock successfully,
     * the lockClient will write into the data of lockPath.
     *
     * @param lockPath the path will create in zookeeper
     *
     * @param lockClient the mark of client
     *
     * @return <tt>true</tt> if lock successfully or the lockClient has kept the lock
     *
     * @since 2.0
     */

    @Override
    public boolean lockPath(String lockPath, String lockClient) {
        logger.info(lockClient + " start lock the path: " + lockPath);

        boolean hasLock = false;
        try {
            if (zkClient.checkExists().forPath(lockPath) != null) {
                if (isKeepLock(lockClient, lockPath)) {
                    hasLock = true;
                    logger.info(lockClient + " has kept the lock for the path: " + lockPath);
                }
            } else {
                zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(lockPath, lockClient.getBytes(Charset.forName("UTF-8")));
                if (isKeepLock(lockClient, lockPath)) {
                    hasLock = true;
                    logger.info(lockClient + " lock the path: " + lockPath + " successfully");
                }
            }
        } catch (Exception e) {
            logger.error(lockClient + " error acquire lock for the path: " + lockPath, e);
        }
        return hasLock;
    }

    /**
     *
     * Returns <tt>true</tt> if, the lockClient is keeping the lock for the lockPath
     *
     * @param lockClient the mark of client
     *
     * @param lockPath the zookeeper node path for the lock
     *
     * @return <tt>true</tt> if the lockClient is keeping the lock for the lockPath, otherwise
     * <tt>false</tt>
     *
     * @since 2.0
     */

    private boolean isKeepLock(String lockClient, String lockPath) {
        try {
            if (zkClient.checkExists().forPath(lockPath) != null) {
                byte[] data = zkClient.getData().forPath(lockPath);
                String lockServerName = new String(data, Charset.forName("UTF-8"));
                return lockServerName.equalsIgnoreCase(lockClient);
            }
        } catch (Exception e) {
            logger.error("fail to get the lockClient for the path: " + lockPath, e);
        }
        return false;
    }

    /**
     *
     * Returns <tt>true</tt> if, and only if, the path has been locked.
     *
     * @param lockPath the zookeeper node path for the lock
     *
     * @return <tt>true</tt> if the path has been locked, otherwise
     * <tt>false</tt>
     *
     * @since 2.0
     */

    @Override
    public boolean isPathLocked(String lockPath) {
        try {
            return zkClient.checkExists().forPath(lockPath) != null;
        } catch (Exception e) {
            logger.error("fail to get the path: " + lockPath, e);
        }
        return false;
    }

    /**
     * if lockClient keep the lock, it will release the lock with the specific path
     *
     * <p> the path related zookeeper node will be deleted.
     *
     * @param lockPath the zookeeper node path for the lock.
     *
     * @param lockClient the mark of client
     *
     * @since 2.0
     */

    @Override
    public void unlockPath(String lockPath, String lockClient) {
        try {
            if (isKeepLock(lockClient, lockPath)) {
                zkClient.delete().guaranteed().deletingChildrenIfNeeded().forPath(lockPath);
                logger.info("the lock for " + lockPath + " release successfully");
            }
        } catch (Exception e) {
            logger.error("error release lock :" + lockPath);
            throw new RuntimeException(e);
        }
    }

    /**
     * watch the path so that when zookeeper node change, the client could receive the notification.
     * Note: the client should close the PathChildrenCache in time.
     *
     * @param watchPath the path need to watch
     *
     * @param watchExecutor the executor watching the zookeeper node change
     *
     * @param watcherProcess do the concrete action with the node path and node data when zookeeper node changed
     *
     * @return PathChildrenCache  the client should close the PathChildrenCache in time
     *
     * @since 2.0
     */

    @Override
    public PathChildrenCache watchPath(String watchPath, Executor watchExecutor, final Watcher watcherProcess) {
        PathChildrenCache cache = new PathChildrenCache(zkClient, watchPath, true);
        try {
            cache.start();
            cache.getListenable().addListener(new PathChildrenCacheListener() {
                @Override
                public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                    switch (event.getType()) {
                    case CHILD_REMOVED:
                        watcherProcess.process(event.getData().getPath(), new String(event.getData().getData(), Charset.forName("UTF-8")));
                        break;
                    default:
                        break;
                    }
                }
            }, watchExecutor);
        } catch (Exception e) {
            logger.warn("watch the zookeeper node fail: " + e);
        }
        return cache;
    }

    @Override
    public boolean lockJobEngine() {
        return true;
    }

    @Override
    public void unlockJobEngine() {
    }

    @Override
    public void close() {
        try {
            zkClient.close();
        } catch (Exception e) {
            logger.error("error occurred to close PathChildrenCache", e);
        }
    }
}

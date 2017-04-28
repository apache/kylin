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
import java.util.concurrent.ExecutorService;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.lock.DistributedJobLock;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * the jobLock is specially used to support distributed scheduler.
 */

public class ZookeeperDistributedJobLock implements DistributedJobLock {
    private static Logger logger = LoggerFactory.getLogger(ZookeeperDistributedJobLock.class);

    public static final String ZOOKEEPER_LOCK_PATH = "/kylin/job_engine/lock";

    final private KylinConfig config;
    final CuratorFramework zkClient;
    final PathChildrenCache childrenCache;

    public ZookeeperDistributedJobLock() {
        this(KylinConfig.getInstanceFromEnv());
    }

    public ZookeeperDistributedJobLock(KylinConfig config) {
        this.config = config;

        String zkConnectString = ZookeeperUtil.getZKConnectString();
        logger.info("zk connection string:" + zkConnectString);
        if (StringUtils.isEmpty(zkConnectString)) {
            throw new IllegalArgumentException("ZOOKEEPER_QUORUM is empty!");
        }

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        zkClient = CuratorFrameworkFactory.newClient(zkConnectString, retryPolicy);
        zkClient.start();

        childrenCache = new PathChildrenCache(zkClient, getWatchPath(), true);
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                close();
            }
        }));
    }

    /**
     * Lock the segment with the segmentId and serverName.
     *
     * <p> if the segment related job want to be scheduled,
     * it must acquire the segment lock. segmentId is used to get the lock path,
     * serverName marked which job server keep the segment lock.
     *
     * @param segmentId the id of segment need to lock
     *
     * @param serverName the hostname of job server
     *
     * @return <tt>true</tt> if the segment locked successfully
     *
     * @since 2.0
     */

    @Override
    public boolean lockWithName(String segmentId, String serverName) {
        String lockPath = getLockPath(segmentId);
        logger.info(serverName + " start lock the segment: " + segmentId);

        boolean hasLock = false;
        try {
            if (!(zkClient.getState().equals(CuratorFrameworkState.STARTED))) {
                logger.error("zookeeper have not start");
                return false;
            }
            if (zkClient.checkExists().forPath(lockPath) != null) {
                if (isKeepLock(serverName, lockPath)) {
                    hasLock = true;
                    logger.info(serverName + " has kept the lock for segment: " + segmentId);
                }
            } else {
                zkClient.create().withMode(CreateMode.EPHEMERAL).forPath(lockPath, serverName.getBytes(Charset.forName("UTF-8")));
                if (isKeepLock(serverName, lockPath)) {
                    hasLock = true;
                    logger.info(serverName + " lock the segment: " + segmentId + " successfully");
                }
            }
        } catch (Exception e) {
            logger.error(serverName + " error acquire lock for the segment: " + segmentId, e);
        }
        if (!hasLock) {
            logger.info(serverName + " fail to acquire lock for the segment: " + segmentId);
            return false;
        }
        return true;
    }

    /**
     *
     * Returns <tt>true</tt> if, the job server is keeping the lock for the lockPath
     *
     * @param serverName the hostname of job server
     *
     * @param lockPath the zookeeper node path of segment
     *
     * @return <tt>true</tt> if the job server is keeping the lock for the lockPath, otherwise
     * <tt>false</tt>
     *
     * @since 2.0
     */

    private boolean isKeepLock(String serverName, String lockPath) {
        try {
            if (zkClient.checkExists().forPath(lockPath) != null) {
                byte[] data = zkClient.getData().forPath(lockPath);
                String lockServerName = new String(data, Charset.forName("UTF-8"));
                return lockServerName.equalsIgnoreCase(serverName);
            }
        } catch (Exception e) {
            logger.error("fail to get the serverName for the path: " + lockPath, e);
        }
        return false;
    }

    /**
     *
     * Returns <tt>true</tt> if, and only if, the segment has been locked.
     *
     * @param segmentId the id of segment need to release the lock.
     *
     * @return <tt>true</tt> if the segment has been locked, otherwise
     * <tt>false</tt>
     *
     * @since 2.0
     */

    @Override
    public boolean isHasLocked(String segmentId) {
        String lockPath = getLockPath(segmentId);
        try {
            return zkClient.checkExists().forPath(lockPath) != null;
        } catch (Exception e) {
            logger.error("fail to get the path: " + lockPath, e);
        }
        return false;
    }

    /**
     * release the segment lock with the segmentId.
     *
     * <p> the segment related zookeeper node will be deleted.
     *
     * @param segmentId the id of segment need to release the lock
     *
     * @since 2.0
     */

    @Override
    public void unlockWithName(String segmentId) {
        String lockPath = getLockPath(segmentId);
        try {
            if (zkClient.getState().equals(CuratorFrameworkState.STARTED)) {
                if (zkClient.checkExists().forPath(lockPath) != null) {
                    zkClient.delete().guaranteed().deletingChildrenIfNeeded().forPath(lockPath);
                    logger.info("the lock for " + segmentId + " release successfully");
                } else {
                    logger.info("the lock for " + segmentId + " has released");
                }
            }
        } catch (Exception e) {
            logger.error("error release lock :" + segmentId);
            throw new RuntimeException(e);
        }
    }

    /**
     * watching all the locked segments related zookeeper nodes change,
     * in order to when one job server is down, other job server can take over the running jobs.
     *
     * @param pool the threadPool watching the zookeeper node change
     *
     * @param doWatch do the concrete action with the zookeeper node path and zookeeper node data
     *
     * @since 2.0
     */

    @Override
    public void watchLock(ExecutorService pool, final DoWatchLock doWatch) {
        try {
            childrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
            childrenCache.getListenable().addListener(new PathChildrenCacheListener() {
                @Override
                public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                    switch (event.getType()) {
                    case CHILD_REMOVED:
                        doWatch.doWatch(event.getData().getPath(), new String(event.getData().getData(), Charset.forName("UTF-8")));
                        break;
                    default:
                        break;
                    }
                }
            }, pool);
        } catch (Exception e) {
            logger.warn("watch the zookeeper node fail: " + e);
        }
    }

    private String getLockPath(String pathName) {
        return ZOOKEEPER_LOCK_PATH + "/" + config.getMetadataUrlPrefix() + "/" + pathName;
    }

    private String getWatchPath() {
        return ZOOKEEPER_LOCK_PATH + "/" + config.getMetadataUrlPrefix();
    }

    @Override
    public boolean lock() {
        return true;
    }

    @Override
    public void unlock() {
    }

    public void close() {
        try {
            childrenCache.close();
            zkClient.close();
        } catch (Exception e) {
            logger.error("error occurred to close PathChildrenCache", e);
        }
    }
}

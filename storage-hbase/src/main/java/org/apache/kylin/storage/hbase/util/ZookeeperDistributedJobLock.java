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
import java.util.Arrays;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nullable;

import org.apache.commons.lang.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.lock.DistributedJobLock;
import org.apache.kylin.storage.hbase.HBaseConnection;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

/**
 * the jobLock is specially used to support distributed scheduler.
 */

public class ZookeeperDistributedJobLock implements DistributedJobLock {
    private static Logger logger = LoggerFactory.getLogger(ZookeeperDistributedJobLock.class);

    private static final String ZOOKEEPER_LOCK_PATH = "/kylin/job_engine/lock";
    private static CuratorFramework zkClient;
    private static PathChildrenCache childrenCache;

    static {
        String zkConnectString = getZKConnectString();
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
                try {
                    childrenCache.close();
                    zkClient.close();
                } catch (Exception e) {
                    logger.error("error occurred to close PathChildrenCache", e);
                }
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
                if (hasLock(serverName, lockPath)) {
                    hasLock = true;
                    logger.info(serverName + " has kept the lock for segment: " + segmentId);
                }
            } else {
                zkClient.create().withMode(CreateMode.EPHEMERAL).forPath(lockPath, serverName.getBytes(Charset.forName("UTF-8")));
                if (hasLock(serverName, lockPath)) {
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

    private boolean hasLock(String serverName, String lockPath) {
        String lockServerName = null;
        try {
            if (zkClient.checkExists().forPath(lockPath) != null) {
                byte[] data = zkClient.getData().forPath(lockPath);
                lockServerName = new String(data, Charset.forName("UTF-8"));
            }
        } catch (Exception e) {
            logger.error("fail to get the serverName for the path: " + lockPath, e);
        }
        return lockServerName.equalsIgnoreCase(serverName);
    }

    /**
     * release the segment lock with the segmentId.
     *
     * <p> the segment related zookeeper node will be deleted.
     *
     * @param segmentId the name of segment need to release the lock
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
     * @param doWatch do the concrete action with the zookeeper node path and zookeeper node data
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

    private static String getZKConnectString() {
        Configuration conf = HBaseConnection.getCurrentHBaseConfiguration();
        final String serverList = conf.get(HConstants.ZOOKEEPER_QUORUM);
        final String port = conf.get(HConstants.ZOOKEEPER_CLIENT_PORT);
        return org.apache.commons.lang3.StringUtils.join(Iterables.transform(Arrays.asList(serverList.split(",")), new Function<String, String>() {
            @Nullable
            @Override
            public String apply(String input) {
                return input + ":" + port;
            }
        }), ",");
    }

    private String getLockPath(String pathName) {
        return ZOOKEEPER_LOCK_PATH + "/" + KylinConfig.getInstanceFromEnv().getMetadataUrlPrefix() + "/" + pathName;
    }

    private static String getWatchPath() {
        return ZOOKEEPER_LOCK_PATH + "/" + KylinConfig.getInstanceFromEnv().getMetadataUrlPrefix();
    }

    @Override
    public boolean lock() {
        return true;
    }

    @Override
    public void unlock() {

    }
}

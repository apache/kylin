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

import org.apache.commons.lang.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 */
public class ZookeeperJobLock implements JobLock {
    private Logger logger = LoggerFactory.getLogger(ZookeeperJobLock.class);

    private static final String ZOOKEEPER_LOCK_PATH = "/kylin/job_engine/lock";

    private String scheduleID;
    private InterProcessMutex sharedLock;
    private CuratorFramework zkClient;

    @Override
    public boolean lock() {
        this.scheduleID = schedulerId();
        String ZKConnectString = getZKConnectString();
        if (StringUtils.isEmpty(ZKConnectString)) {
            throw new IllegalArgumentException("ZOOKEEPER_QUORUM is empty!");
        }

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        this.zkClient = CuratorFrameworkFactory.newClient(ZKConnectString, retryPolicy);
        this.zkClient.start();
        this.sharedLock = new InterProcessMutex(zkClient, this.scheduleID);
        boolean hasLock = false;
        try {
            hasLock = sharedLock.acquire(3, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.warn("error acquire lock", e);
        }
        if (!hasLock) {
            logger.warn("fail to acquire lock, scheduler has not been started");
            zkClient.close();
            return false;
        }
        return true;
    }

    @Override
    public void unlock() {
        releaseLock();
    }

    private String getZKConnectString() {
        Configuration conf = HadoopUtil.newHBaseConfiguration(KylinConfig.getInstanceFromEnv().getStorageUrl());
        return conf.get(HConstants.ZOOKEEPER_QUORUM) + ":" + conf.get(HConstants.ZOOKEEPER_CLIENT_PORT);
    }

    private void releaseLock() {
        try {
            if (zkClient.getState().equals(CuratorFrameworkState.STARTED)) {
                // client.setData().forPath(ZOOKEEPER_LOCK_PATH, null);
                if (zkClient.checkExists().forPath(scheduleID) != null) {
                    zkClient.delete().guaranteed().deletingChildrenIfNeeded().forPath(scheduleID);
                }
            }
        } catch (Exception e) {
            logger.error("error release lock:" + scheduleID);
            throw new RuntimeException(e);
        }
    }

    private String schedulerId() {
        return ZOOKEEPER_LOCK_PATH + "/" + KylinConfig.getInstanceFromEnv().getMetadataUrlPrefix();
    }
}

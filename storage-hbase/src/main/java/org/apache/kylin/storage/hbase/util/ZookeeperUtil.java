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

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperUtil {

    private static final Logger logger = LoggerFactory.getLogger(ZookeeperUtil.class);
    public static String ZOOKEEPER_UTIL_HBASE_CLASSNAME = "org.apache.kylin.storage.hbase.util.ZooKeeperUtilHbase";

    /**
     * Get zookeeper connection string from HBase Configuration or from kylin.properties
     */
    public static String getZKConnectString() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        return config.getZookeeperConnectString();
    }

    public static void cleanZkPath(String path) {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework zkClient = CuratorFrameworkFactory.newClient(getZKConnectString(), retryPolicy);
        zkClient.start();

        try {
            zkClient.delete().deletingChildrenIfNeeded().forPath(path);
        } catch (Exception e) {
            logger.warn("Failed to delete zookeeper path: " + path, e);
        } finally {
            zkClient.close();
        }
    }
}
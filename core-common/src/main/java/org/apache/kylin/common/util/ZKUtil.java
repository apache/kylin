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

package org.apache.kylin.common.util;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.ZKPaths;
import org.apache.hadoop.conf.Configuration;
import org.apache.kylin.common.KylinConfig;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Shell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.annotations.VisibleForTesting;
import org.apache.kylin.shaded.com.google.common.base.Function;
import org.apache.kylin.shaded.com.google.common.base.Strings;
import org.apache.kylin.shaded.com.google.common.cache.Cache;
import org.apache.kylin.shaded.com.google.common.cache.CacheBuilder;
import org.apache.kylin.shaded.com.google.common.cache.RemovalListener;
import org.apache.kylin.shaded.com.google.common.cache.RemovalNotification;
import org.apache.kylin.shaded.com.google.common.collect.Iterables;

public class ZKUtil {
    private static final Logger logger = LoggerFactory.getLogger(ZKUtil.class);

    private static final KylinConfig defaultKylinConfig = KylinConfig.getInstanceFromEnv();
    private static final String zkChRoot = fixPath(defaultKylinConfig.getZookeeperBasePath(),
            defaultKylinConfig.getClusterName());
    private static TestingServer server = null;
    private static Cache<String, CuratorFramework> CACHE = CacheBuilder.newBuilder()
            .removalListener(new RemovalListener<String, CuratorFramework>() {
                @Override
                public void onRemoval(RemovalNotification<String, CuratorFramework> notification) {
                    logger.info("CuratorFramework for zkString " + notification.getKey() + " is removed due to "
                            + notification.getCause());
                    CuratorFramework curator = notification.getValue();
                    try {
                        curator.close();
                    } catch (Exception ex) {
                        logger.error("Error at closing " + curator, ex);
                    }
                }
            }).expireAfterWrite(10000, TimeUnit.DAYS).build();//never expired

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                logger.info("Going to remove {} cached curator clients", CACHE.size());
                CACHE.invalidateAll();
            }
        }));
    }

    private static String fixPath(String parent, String child) {
        String path = ZKPaths.makePath(parent, child);

        try {
            if (Shell.WINDOWS) {
                return new File(path).toURI().getPath();
            } else {
                return new File(path).getCanonicalPath();
            }
        } catch (IOException e) {
            logger.error("get canonical path failed, use original path", e);
            return path;
        }
    }

    /**
     * Get zookeeper connection string from kylin.properties
     */
    public static String getZKConnectString(KylinConfig config) {
        String zkString = config.getZookeeperConnectString();
        if (zkString == null) {
            zkString = getZKConnectStringFromHBase();
            if (zkString == null) {
                throw new RuntimeException("Please set 'kylin.env.zookeeper-connect-string' in kylin.properties");
            }
        }

        return zkString;
    }

    public static String getZkRootBasedPath(String path) {
        return zkChRoot + "/" + path;
    }

    public static CuratorFramework getZookeeperClient(KylinConfig config) {
        RetryPolicy retryPolicy = getRetryPolicy(config);
        if (config.isZKLocal()) {
            startTestingServer();
        }
        return getZookeeperClient(getZKConnectString(config), retryPolicy);
    }

    private static void startTestingServer() {
        if (server == null) {
            try {
                server = new TestingServer(12181, true);
                server.start();
                logger.error("Started zk testing server.");
            } catch (Exception e) {
                logger.error("Failed to start zk testing server.", e);
            }
        }
    }

    private static CuratorFramework getZookeeperClient(final String zkString, final RetryPolicy retryPolicy) {
        if (StringUtils.isEmpty(zkString)) {
            throw new IllegalArgumentException("ZOOKEEPER_QUORUM is empty!");
        }
        try {
            CuratorFramework instance = CACHE.get(zkString, new Callable<CuratorFramework>() {
                @Override
                public CuratorFramework call() throws Exception {
                    return newZookeeperClient(zkString, retryPolicy);
                }
            });
            // during test, curator may be closed by others, remove it from CACHE and reinitialize a new one
            if (instance.getState() != CuratorFrameworkState.STARTED) {
                logger.warn("curator for {} is closed by others unexpectedly, reinitialize a new one", zkString);
                CACHE.invalidate(zkString);
                instance = getZookeeperClient(zkString, retryPolicy);
            }
            return instance;
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    //no cache
    public static CuratorFramework newZookeeperClient() {
        return newZookeeperClient(KylinConfig.getInstanceFromEnv());
    }

    @VisibleForTesting
    //no cache
    public static CuratorFramework newZookeeperClient(KylinConfig config) {
        RetryPolicy retryPolicy = getRetryPolicy(config);
        return newZookeeperClient(getZKConnectString(config), retryPolicy);
    }

    @VisibleForTesting
    //no cache
    public static CuratorFramework newZookeeperClient(String zkString, RetryPolicy retryPolicy) {
        if (zkChRoot == null)
            throw new NullPointerException("zkChRoot must not be null");

        logger.info("zookeeper connection string: {} with namespace {}", zkString, zkChRoot);

        CuratorFramework instance = getCuratorFramework(zkString, zkChRoot, retryPolicy);
        instance.start();
        logger.info("new zookeeper Client start: " + zkString);
        // create zkChRoot znode if necessary
        createZkChRootIfNecessary(instance, zkString);
        return instance;
    }

    private static RetryPolicy getRetryPolicy(KylinConfig config) {
        int baseSleepTimeMs = config.getZKBaseSleepTimeMs();
        int maxRetries = config.getZKMaxRetries();
        return new ExponentialBackoffRetry(baseSleepTimeMs, maxRetries);
    }

    private static synchronized void createZkChRootIfNecessary(CuratorFramework instance, String zkString) {
        try {
            RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
            if (instance.checkExists().forPath("/") == null) {
                CuratorFramework tmpCurator = getCuratorFramework(zkString, null, retryPolicy);
                tmpCurator.start();
                tmpCurator.create().creatingParentsIfNeeded().forPath(zkChRoot);
                tmpCurator.close();
            }
        } catch (KeeperException.NodeExistsException e) {
            logger.warn("The chRoot znode {} has been created by others", zkChRoot);
        } catch (Exception e) {
            throw new RuntimeException("Fail to check or create znode for chRoot " + zkChRoot + " due to ", e);
        }
    }

    private static CuratorFramework getCuratorFramework(String zkString, String zkChRoot, RetryPolicy retryPolicy) {
        if (!Strings.isNullOrEmpty(zkChRoot)) {
            zkString += zkChRoot;
        }
        return CuratorFrameworkFactory.newClient(zkString, 120000, 15000, retryPolicy);
    }

    private static String getZKConnectStringFromHBase() {
        Configuration hconf = null;
        try {
            Class<? extends Object> hbaseConnClz = ClassUtil.forName("org.apache.kylin.storage.hbase.HBaseConnection",
                    Object.class);
            hconf = (Configuration) hbaseConnClz.getMethod("getCurrentHBaseConfiguration").invoke(null);
        } catch (Throwable ex) {
            logger.warn("Failed to get zookeeper connect string from HBase configuration", ex);
            return null;
        }

        final String serverList = hconf.get("hbase.zookeeper.quorum");
        final String port = hconf.get("hbase.zookeeper.property.clientPort");
        return StringUtils
                .join(Iterables.transform(Arrays.asList(serverList.split(",")), new Function<String, String>() {
                    @Nullable
                    @Override
                    public String apply(String input) {
                        return input + ":" + port;
                    }
                }), ",");
    }

    public static void cleanZkPath(String path) {
        CuratorFramework zkClient = ZKUtil.newZookeeperClient();

        try {
            zkClient.delete().deletingChildrenIfNeeded().forPath(path);
        } catch (Exception e) {
            logger.warn("Failed to delete zookeeper path: {}", path, e);
        } finally {
            zkClient.close();
        }
    }
}

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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.ZookeeperConfig;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.guava30.shaded.common.base.Strings;
import org.apache.kylin.guava30.shaded.common.cache.Cache;
import org.apache.kylin.guava30.shaded.common.cache.CacheBuilder;
import org.apache.kylin.guava30.shaded.common.cache.RemovalNotification;

import org.apache.kylin.shaded.curator.org.apache.curator.RetryPolicy;
import org.apache.kylin.shaded.curator.org.apache.curator.framework.CuratorFramework;
import org.apache.kylin.shaded.curator.org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.kylin.shaded.curator.org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.kylin.shaded.curator.org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.kylin.shaded.curator.org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.kylin.shaded.curator.org.apache.curator.utils.ZKPaths;

public class ZKUtil {
    private static final Logger logger = LoggerFactory.getLogger(ZKUtil.class);

    private static final KylinConfig defaultKylinConfig = KylinConfig.getInstanceFromEnv();
    public static final String ZK_ROOT = fixPath(defaultKylinConfig.getZookeeperBasePath(),
            defaultKylinConfig.getClusterName());

    private ZKUtil() {

    }

    private static String fixPath(String parent, String child) {
        String path = ZKPaths.makePath(parent, child);

        try {
            return new File(path).getCanonicalPath();
        } catch (IOException e) {
            logger.error("get canonical path failed, use original path", e);
            return path;
        }
    }

    private static Cache<KylinConfig, CuratorFramework> CACHE = CacheBuilder.newBuilder()
            .removalListener((RemovalNotification<KylinConfig, CuratorFramework> notification) -> {
                logger.info("CuratorFramework for zkString " + notification.getKey() + " is removed due to "
                        + notification.getCause());
                CuratorFramework curator = notification.getValue();
                try {
                    if (curator != null) {
                        curator.close();
                    }
                } catch (Exception ex) {
                    logger.error("Error at closing " + curator, ex);
                }
            }).expireAfterWrite(1, TimeUnit.DAYS).build();

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Going to remove {} cached curator clients", CACHE.size());
            CACHE.invalidateAll();
        }));
    }

    public static CuratorFramework getZookeeperClient(KylinConfig config, ConnectionStateListener listener)
            throws Exception {
        if (config == null) {
            throw new IllegalArgumentException("KylinConfig can not be null");
        }

        RetryPolicy retryPolicy = getRetryPolicy(config);
        return getZookeeperClient(config, retryPolicy, listener);
    }

    public static CuratorFramework getZookeeperClient(KylinConfig config) throws Exception {
        return getZookeeperClient(config, null);
    }

    private static CuratorFramework getZookeeperClient(final KylinConfig config, final RetryPolicy retryPolicy,
            ConnectionStateListener listener) throws Exception {
        String zkString = getZKConnectString(config);

        try {
            CuratorFramework instance = CACHE.get(config, new Callable<CuratorFramework>() {
                @Override
                public CuratorFramework call() throws Exception {
                    return newZookeeperClient(config, zkString, retryPolicy, listener);
                }
            });

            // curator may be closed by others, remove it from CACHE and reinitialize a new one
            if (instance.getState() != CuratorFrameworkState.STARTED || !instance.getZookeeperClient().isConnected()) {
                logger.warn("curator for {} is not available, reinitialize a new one", zkString);
                CACHE.invalidate(config);
                instance = getZookeeperClient(config, retryPolicy, listener);
            }
            return instance;
        } catch (ExecutionException e) {
            return newZookeeperClient(config, zkString, retryPolicy, listener);
        }
    }

    private static String getZKConnectString(KylinConfig config) {
        return config.getZookeeperConnectString();
    }

    private static CuratorFramework newZookeeperClient(final KylinConfig config, String zkString,
            RetryPolicy retryPolicy, ConnectionStateListener listener) throws Exception {
        logger.info("zookeeper connection string: {} with namespace {}", zkString, ZK_ROOT);

        CuratorFramework instance = getCuratorFramework(config, zkString, ZK_ROOT, retryPolicy);
        instance.start();
        logger.info("new zookeeper Client start: {}", zkString);

        // register listener
        if (listener != null) {
            instance.getConnectionStateListenable().addListener(listener);
        }

        // create zkRoot znode if necessary
        createzkRootIfNecessary(config, instance, zkString);
        return instance;
    }

    private static RetryPolicy getRetryPolicy(KylinConfig config) {
        int baseSleepTimeMs = config.getZKBaseSleepTimeMs();
        int maxRetries = config.getZKMaxRetries();
        return new ExponentialBackoffRetry(baseSleepTimeMs, maxRetries);
    }

    private static synchronized void createzkRootIfNecessary(KylinConfig config, CuratorFramework instance,
            String zkString) throws Exception {
        try {
            RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
            if (instance.checkExists().forPath("/") == null) {
                CuratorFramework tmpCurator = getCuratorFramework(config, zkString, null, retryPolicy);
                tmpCurator.start();
                tmpCurator.create().creatingParentsIfNeeded().forPath(ZK_ROOT);
                tmpCurator.close();
            }
        } catch (KeeperException.NodeExistsException e) {
            logger.warn("The zkRoot znode {} has been created by others", ZK_ROOT);
        } catch (Exception e) {
            logger.error("Fail to check or create znode for zkRoot {}", ZK_ROOT);
            throw e;
        }
    }

    private static CuratorFramework getCuratorFramework(final KylinConfig config, String zkString, String zkRoot,
            RetryPolicy retryPolicy) {
        if (!Strings.isNullOrEmpty(zkRoot)) {
            zkString += zkRoot;
        }
        int sessionTimeout = ZookeeperConfig.geZKClientSessionTimeoutMs();
        int connectionTimeout = ZookeeperConfig.geZKClientConnectionTimeoutMs();
        ZookeeperAclBuilder aclBuilder = new ZookeeperAclBuilder().invoke();
        return aclBuilder.setZKAclBuilder(CuratorFrameworkFactory.builder()).connectString(zkString)
                .sessionTimeoutMs(sessionTimeout).connectionTimeoutMs(connectionTimeout).retryPolicy(retryPolicy)
                .build();
    }

    public static boolean pathExisted(String path, KylinConfig kylinConfig) throws Exception {
        try {
            CuratorFramework zkClient = getZookeeperClient(kylinConfig);
            return zkClient.checkExists().forPath(path) != null;
        } catch (Exception e) {
            logger.error("Failed to create path: {}", path);
            throw e;
        }
    }
}

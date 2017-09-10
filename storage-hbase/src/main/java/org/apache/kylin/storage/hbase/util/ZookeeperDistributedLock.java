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

import java.io.Closeable;
import java.nio.charset.Charset;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.lock.DistributedLock;
import org.apache.kylin.common.lock.DistributedLockFactory;
import org.apache.kylin.job.lock.JobLock;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A distributed lock based on zookeeper. Every instance is owned by a client, on whose behalf locks are acquired and/or released.
 * 
 * All <code>lockPath</code> will be prefix-ed with "/kylin/metadata-prefix" automatically.
 */
public class ZookeeperDistributedLock implements DistributedLock, JobLock {
    private static Logger logger = LoggerFactory.getLogger(ZookeeperDistributedLock.class);

    public static class Factory extends DistributedLockFactory {

        private static final ConcurrentMap<KylinConfig, CuratorFramework> CACHE = new ConcurrentHashMap<KylinConfig, CuratorFramework>();

        static {
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                @Override
                public void run() {
                    for (CuratorFramework curator : CACHE.values()) {
                        try {
                            curator.close();
                        } catch (Exception ex) {
                            logger.error("Error at closing " + curator, ex);
                        }
                    }
                }
            }));
        }

        private static CuratorFramework getZKClient(KylinConfig config) {
            CuratorFramework zkClient = CACHE.get(config);
            if (zkClient == null) {
                synchronized (ZookeeperDistributedLock.class) {
                    zkClient = CACHE.get(config);
                    if (zkClient == null) {
                        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
                        String zkConnectString = getZKConnectString(config);
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

        private static String getZKConnectString(KylinConfig config) {
            // the ZKConnectString should come from KylinConfig, however it is taken from HBase configuration at the moment
            return ZookeeperUtil.getZKConnectString();
        }

        final String zkPathBase;
        final CuratorFramework curator;

        public Factory() {
            this(KylinConfig.getInstanceFromEnv());
        }

        public Factory(KylinConfig config) {
            this.curator = getZKClient(config);
            this.zkPathBase = fixSlash(config.getZookeeperBasePath() + "/" + config.getMetadataUrlPrefix());
        }

        @Override
        public DistributedLock lockForClient(String client) {
            return new ZookeeperDistributedLock(curator, zkPathBase, client);
        }
    }

    // ============================================================================

    final CuratorFramework curator;
    final String zkPathBase;
    final String client;
    final byte[] clientBytes;

    private ZookeeperDistributedLock(CuratorFramework curator, String zkPathBase, String client) {
        if (client == null)
            throw new NullPointerException("client must not be null");
        if (zkPathBase == null)
            throw new NullPointerException("zkPathBase must not be null");

        this.curator = curator;
        this.zkPathBase = zkPathBase;
        this.client = client;
        this.clientBytes = client.getBytes(Charset.forName("UTF-8"));
    }

    @Override
    public String getClient() {
        return client;
    }

    @Override
    public boolean lock(String lockPath) {
        lockPath = norm(lockPath);

        logger.debug(client + " trying to lock " + lockPath);

        try {
            curator.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(lockPath, clientBytes);
        } catch (KeeperException.NodeExistsException ex) {
            logger.debug(client + " see " + lockPath + " is already locked");
        } catch (Exception ex) {
            throw new RuntimeException("Error while " + client + " trying to lock " + lockPath, ex);
        }

        String lockOwner = peekLock(lockPath);
        if (client.equals(lockOwner)) {
            logger.info(client + " acquired lock at " + lockPath);
            return true;
        } else {
            logger.debug(client + " failed to acquire lock at " + lockPath + ", which is held by " + lockOwner);
            return false;
        }
    }

    @Override
    public boolean lock(String lockPath, long timeout) {
        lockPath = norm(lockPath);

        if (lock(lockPath))
            return true;

        if (timeout <= 0)
            timeout = Long.MAX_VALUE;

        logger.debug(client + " will wait for lock path " + lockPath);
        long waitStart = System.currentTimeMillis();
        Random random = new Random();
        long sleep = 10 * 1000; // 10 seconds

        while (System.currentTimeMillis() - waitStart <= timeout) {
            try {
                Thread.sleep((long) (1000 + sleep * random.nextDouble()));
            } catch (InterruptedException e) {
                return false;
            }

            if (lock(lockPath)) {
                logger.debug(client + " waited " + (System.currentTimeMillis() - waitStart) + " ms for lock path " + lockPath);
                return true;
            }
        }

        // timeout
        return false;
    }

    @Override
    public String peekLock(String lockPath) {
        lockPath = norm(lockPath);

        try {
            byte[] bytes = curator.getData().forPath(lockPath);
            return new String(bytes, Charset.forName("UTF-8"));
        } catch (KeeperException.NoNodeException ex) {
            return null;
        } catch (Exception ex) {
            throw new RuntimeException("Error while peeking at " + lockPath, ex);
        }
    }

    @Override
    public boolean isLocked(String lockPath) {
        return peekLock(lockPath) != null;
    }

    @Override
    public boolean isLockedByMe(String lockPath) {
        return client.equals(peekLock(lockPath));
    }

    @Override
    public void unlock(String lockPath) {
        lockPath = norm(lockPath);

        logger.debug(client + " trying to unlock " + lockPath);

        String owner = peekLock(lockPath);
        if (owner == null)
            throw new IllegalStateException(client + " cannot unlock path " + lockPath + " which is not locked currently");
        if (client.equals(owner) == false)
            throw new IllegalStateException(client + " cannot unlock path " + lockPath + " which is locked by " + owner);

        try {
            curator.delete().guaranteed().deletingChildrenIfNeeded().forPath(lockPath);

            logger.info(client + " released lock at " + lockPath);

        } catch (Exception ex) {
            throw new RuntimeException("Error while " + client + " trying to unlock " + lockPath, ex);
        }
    }

    @Override
    public void purgeLocks(String lockPathRoot) {
        lockPathRoot = norm(lockPathRoot);

        try {
            curator.delete().guaranteed().deletingChildrenIfNeeded().forPath(lockPathRoot);

            logger.info(client + " purged all locks under " + lockPathRoot);

        } catch (Exception ex) {
            throw new RuntimeException("Error while " + client + " trying to purge " + lockPathRoot, ex);
        }
    }

    @Override
    public Closeable watchLocks(String lockPathRoot, Executor executor, final Watcher watcher) {
        lockPathRoot = norm(lockPathRoot);

        PathChildrenCache cache = new PathChildrenCache(curator, lockPathRoot, true);
        try {
            cache.start();
            cache.getListenable().addListener(new PathChildrenCacheListener() {
                @Override
                public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                    switch (event.getType()) {
                    case CHILD_ADDED:
                        watcher.onLock(event.getData().getPath(), new String(event.getData().getData(), Charset.forName("UTF-8")));
                        break;
                    case CHILD_REMOVED:
                        watcher.onUnlock(event.getData().getPath(), new String(event.getData().getData(), Charset.forName("UTF-8")));
                        break;
                    default:
                        break;
                    }
                }
            }, executor);
        } catch (Exception ex) {
            logger.error("Error to watch lock path " + lockPathRoot, ex);
        }
        return cache;
    }

    // normalize lock path
    private String norm(String lockPath) {
        if (!lockPath.startsWith(zkPathBase))
            lockPath = zkPathBase + (lockPath.startsWith("/") ? "" : "/") + lockPath;
        return fixSlash(lockPath);
    }

    private static String fixSlash(String path) {
        if (!path.startsWith("/"))
            path = "/" + path;
        if (path.endsWith("/"))
            path = path.substring(0, path.length() - 1);
        for (int n = Integer.MAX_VALUE; n > path.length();) {
            n = path.length();
            path = path.replace("//", "/");
        }
        return path;
    }

    // ============================================================================

    @Override
    public boolean lockJobEngine() {
        String path = jobEngineLockPath();
        return lock(path, 3000);
    }

    @Override
    public void unlockJobEngine() {
        unlock(jobEngineLockPath());
    }

    private String jobEngineLockPath() {
        return "/job_engine/global_job_engine_lock";
    }

}

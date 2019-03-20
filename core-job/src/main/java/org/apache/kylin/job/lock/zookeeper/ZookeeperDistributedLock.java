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

package org.apache.kylin.job.lock.zookeeper;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.Executor;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.lock.DistributedLock;
import org.apache.kylin.common.lock.DistributedLockFactory;
import org.apache.kylin.common.util.ZKUtil;
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
    private static final Random random = new Random();

    public static class Factory extends DistributedLockFactory {
        final CuratorFramework curator;

        public Factory() {
            this(KylinConfig.getInstanceFromEnv());
        }

        public Factory(KylinConfig config) {
            this(ZKUtil.getZookeeperClient(config));
        }

        public Factory(CuratorFramework curator) {
            this.curator = curator;
        }

        @Override
        public DistributedLock lockForClient(String client) {
            return new ZookeeperDistributedLock(curator, client);
        }
    }

    // ============================================================================

    final CuratorFramework curator;
    final String client;
    final byte[] clientBytes;

    private ZookeeperDistributedLock(CuratorFramework curator, String client) {
        if (client == null)
            throw new NullPointerException("client must not be null");

        this.curator = curator;
        this.client = client;
        this.clientBytes = client.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public String getClient() {
        return client;
    }

    @Override
    public boolean lock(String lockPath) {
        logger.debug("{} trying to lock {}", client, lockPath);

        try {
            curator.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(lockPath, clientBytes);
        } catch (KeeperException.NodeExistsException ex) {
            logger.debug("{} see {} is already locked", client, lockPath);
        } catch (Exception ex) {
            throw new IllegalStateException("Error while " + client + " trying to lock " + lockPath, ex);
        }

        String lockOwner = peekLock(lockPath);
        if (client.equals(lockOwner)) {
            logger.info("{} acquired lock at {}", client, lockPath);
            return true;
        } else {
            logger.debug("{} failed to acquire lock at {}, which is held by {}", client, lockPath, lockOwner);
            return false;
        }
    }

    @Override
    public boolean lock(String lockPath, long timeout) {
        if (lock(lockPath))
            return true;

        if (timeout <= 0)
            timeout = Long.MAX_VALUE;

        logger.debug("{} will wait for lock path {}", client, lockPath);
        long waitStart = System.currentTimeMillis();
        long sleep = 10 * 1000L; // 10 seconds

        while (System.currentTimeMillis() - waitStart <= timeout) {
            try {
                Thread.sleep((long) (1000 + sleep * random.nextDouble()));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }

            if (lock(lockPath)) {
                logger.debug("{} waited {} ms for lock path {}", client, (System.currentTimeMillis() - waitStart), lockPath);
                return true;
            }
        }

        // timeout
        return false;
    }

    @Override
    public String peekLock(String lockPath) {
        try {
            byte[] bytes = curator.getData().forPath(lockPath);
            return new String(bytes, StandardCharsets.UTF_8);
        } catch (KeeperException.NoNodeException ex) {
            return null;
        } catch (Exception ex) {
            throw new IllegalStateException("Error while peeking at " + lockPath, ex);
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
        logger.debug("{} trying to unlock {}", client, lockPath);

        String owner = peekLock(lockPath);
        if (owner == null)
            throw new IllegalStateException(client + " cannot unlock path " + lockPath + " which is not locked currently");
        if (client.equals(owner) == false)
            throw new IllegalStateException(client + " cannot unlock path " + lockPath + " which is locked by " + owner);

        try {
            curator.delete().guaranteed().deletingChildrenIfNeeded().forPath(lockPath);

            logger.info("{} released lock at {}", client, lockPath);

        } catch (Exception ex) {
            throw new IllegalStateException("Error while " + client + " trying to unlock " + lockPath, ex);
        }
    }

    @Override
    public void purgeLocks(String lockPathRoot) {
        try {
            curator.delete().guaranteed().deletingChildrenIfNeeded().forPath(lockPathRoot);

            logger.info("{} purged all locks under {}", client, lockPathRoot);

        } catch (Exception ex) {
            throw new IllegalStateException("Error while " + client + " trying to purge " + lockPathRoot, ex);
        }
    }

    @Override
    public Closeable watchLocks(String lockPathRoot, Executor executor, final Watcher watcher) {
        PathChildrenCache cache = new PathChildrenCache(curator, lockPathRoot, true);
        try {
            cache.start();
            cache.getListenable().addListener(new PathChildrenCacheListener() {
                @Override
                public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                    switch (event.getType()) {
                    case CHILD_ADDED:
                        watcher.onLock(event.getData().getPath(), new String(event.getData().getData(), StandardCharsets.UTF_8));
                        break;
                    case CHILD_REMOVED:
                        watcher.onUnlock(event.getData().getPath(), new String(event.getData().getData(), StandardCharsets.UTF_8));
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

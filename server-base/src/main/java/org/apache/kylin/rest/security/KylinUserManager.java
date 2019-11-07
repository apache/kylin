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

package org.apache.kylin.rest.security;

import static org.apache.kylin.common.persistence.ResourceStore.USER_ROOT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.AutoReadWriteLock;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.cachesync.CaseInsensitiveStringCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KylinUserManager {

    private static final Logger logger = LoggerFactory.getLogger(KylinUserManager.class);

    public static KylinUserManager getInstance(KylinConfig config) {
        return config.getManager(KylinUserManager.class);
    }

    // called by reflection
    static KylinUserManager newInstance(KylinConfig config) throws IOException {
        return new KylinUserManager(config);
    }

    // ============================================================================

    private KylinConfig config;
    // user ==> ManagedUser
    private CaseInsensitiveStringCache<ManagedUser> userMap;
    private CachedCrudAssist<ManagedUser> crud;
    private AutoReadWriteLock lock = new AutoReadWriteLock();

    public KylinUserManager(KylinConfig config) throws IOException {
        logger.info("Initializing KylinUserManager with config " + config);
        this.config = config;
        this.userMap = new CaseInsensitiveStringCache<>(config, "user");
        this.crud = new CachedCrudAssist<ManagedUser>(getStore(), USER_ROOT, "", ManagedUser.class, userMap, false) {
            @Override
            protected ManagedUser initEntityAfterReload(ManagedUser user, String resourceName) {
                return user;
            }
        };

        crud.reloadAll();
        Broadcaster.getInstance(config).registerListener(new ManagedUserSyncListener(), "user");
    }

    private class ManagedUserSyncListener extends Broadcaster.Listener {

        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Broadcaster.Event event, String cacheKey)
                throws IOException {
            try (AutoReadWriteLock.AutoLock l = lock.lockForWrite()) {
                if (event == Broadcaster.Event.DROP)
                    userMap.removeLocal(cacheKey);
                else
                    crud.reloadQuietly(cacheKey);
            }
        }
    }

    public KylinConfig getConfig() {
        return config;
    }

    public ResourceStore getStore() {
        return ResourceStore.getStore(this.config);
    }

    public ManagedUser get(String name) {
        try (AutoReadWriteLock.AutoLock l = lock.lockForRead()) {
            return userMap.get(name);
        }
    }

    public List<ManagedUser> list() {
        try (AutoReadWriteLock.AutoLock l = lock.lockForRead()) {
            List<ManagedUser> users = new ArrayList<>();
            users.addAll(userMap.values());
            Collections.sort(users, new Comparator<ManagedUser>() {
                @Override
                public int compare(ManagedUser o1, ManagedUser o2) {
                    return o1.getUsername().compareToIgnoreCase(o2.getUsername());
                }
            });
            return users;
        }
    }

    public void update(ManagedUser user) {
        try (AutoReadWriteLock.AutoLock l = lock.lockForWrite()) {
            ManagedUser exist = userMap.get(user.getUsername());
            if (exist != null) {
                user.setLastModified(exist.getLastModified());
            }
            user.setUsername(user.getUsername());
            crud.save(user);
        } catch (IOException e) {
            throw new RuntimeException("Can not update user.", e);
        }
    }

    public void delete(String username) {
        try (AutoReadWriteLock.AutoLock l = lock.lockForWrite()) {
            crud.delete(username);
        } catch (IOException e) {
            throw new RuntimeException("Can not delete user.", e);
        }
    }

    public boolean exists(String username) {
        try (AutoReadWriteLock.AutoLock l = lock.lockForRead()) {
            return userMap.containsKey(username);
        }
    }
}

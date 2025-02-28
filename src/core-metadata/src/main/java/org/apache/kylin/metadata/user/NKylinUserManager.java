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

package org.apache.kylin.metadata.user;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

public class NKylinUserManager {

    private static final Logger logger = LoggerFactory.getLogger(NKylinUserManager.class);

    public static NKylinUserManager getInstance(KylinConfig config) {
        return config.getManager(NKylinUserManager.class);
    }

    // called by reflection
    static NKylinUserManager newInstance(KylinConfig config) throws IOException {
        return new NKylinUserManager(config);
    }

    // ============================================================================

    private KylinConfig config;
    // user ==> ManagedUser
    private CachedCrudAssist<ManagedUser> crud;

    public NKylinUserManager(KylinConfig config) {
        if (!UnitOfWork.isAlreadyInTransaction())
            logger.info("Initializing NKylinUserManager with KylinConfig Id: {}", System.identityHashCode(config));
        this.config = config;
        this.crud = new CachedCrudAssist<ManagedUser>(getStore(), MetadataType.USER_INFO, null, ManagedUser.class) {
            @Override
            protected ManagedUser initEntityAfterReload(ManagedUser user, String resourceName) {
                return user;
            }
        };

    }

    public ManagedUser copyForWrite(ManagedUser user) {
        return crud.copyForWrite(user);
    }

    public ManagedUser copy(ManagedUser user) {
        return crud.copyBySerialization(user);
    }

    public KylinConfig getConfig() {
        return config;
    }

    public ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.config);
    }

    public ManagedUser get(String name) {
        if (StringUtils.isEmpty(name)) {
            return null;
        }
        return crud.get(name);
    }

    public List<ManagedUser> list() {
        return list(true);
    }

    public List<ManagedUser> list(boolean needSort) {
        List<ManagedUser> users = new ArrayList<>(crud.listAll());
        if (needSort) {
            users.sort((o1, o2) -> o1.getUsername().compareToIgnoreCase(o2.getUsername()));
        }
        return users;
    }

    public void createUser(ManagedUser user) {
        ManagedUser copy = copyForWrite(user);
        crud.save(copy);
    }

    public void updateUser(String userName, UserUpdater updater) {
        ManagedUser cached = get(userName);
        ManagedUser copy = copyForWrite(cached);
        updater.modify(copy);
        crud.save(copy);
    }

    /**
     * @deprecated Use updateUser(String userName, UserUpdater updater)
     */
    @Deprecated
    public void update(ManagedUser user) {
        if (exists(user.getUsername())) {
            updateUser(user.getUsername(), user::copyPropertiesTo);
        } else {
            createUser(user);
        }
    }

    public void delete(String username) {
        crud.delete(username);
    }

    public boolean exists(String username) {
        return Objects.nonNull(get(username));
    }

    public Set<String> getUserGroups(String userName) {
        ManagedUser user = get(userName);
        if (user == null)
            return Sets.newHashSet();

        return user.getAuthorities().stream().map(SimpleGrantedAuthority::getAuthority).collect(Collectors.toSet());
    }

    public interface UserUpdater {
        void modify(ManagedUser copy);
    }
}

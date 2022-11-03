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

import static org.apache.kylin.common.persistence.ResourceStore.ACL_GLOBAL_ROOT;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.acls.model.Permission;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import lombok.val;

public class UserAclManager {
    private static final Logger logger = LoggerFactory.getLogger(UserAclManager.class);

    public static UserAclManager getInstance(KylinConfig config) {
        return config.getManager(UserAclManager.class);
    }

    // called by reflection
    static UserAclManager newInstance(KylinConfig config) {
        return new UserAclManager(config);
    }

    private KylinConfig config;
    private CachedCrudAssist<UserAcl> crud;

    public UserAclManager(KylinConfig config) {
        if (!UnitOfWork.isAlreadyInTransaction())
            logger.info("Initializing UserAclManager with KylinConfig Id: {}", System.identityHashCode(config));
        this.config = config;
        this.crud = new CachedCrudAssist<UserAcl>(getStore(), ACL_GLOBAL_ROOT, "", UserAcl.class) {
            @Override
            protected UserAcl initEntityAfterReload(UserAcl userAcl, String resourceName) {
                return userAcl;
            }
        };
        this.crud.reloadAll();
    }

    public ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.config);
    }

    public List<String> listAclUsernames() {
        return ImmutableList.copyOf(crud.listAll().stream().map(UserAcl::getUsername).collect(Collectors.toList()));
    }

    public List<UserAcl> listUserAcl() {
        return ImmutableList.copyOf(crud.listAll());
    }

    public boolean exists(String name) {
        return filterUsername(name).isPresent();
    }

    public Optional<String> filterUsername(String username) {
        return listAclUsernames().stream().filter(name -> StringUtils.equalsIgnoreCase(name, username)).findAny();
    }

    public UserAcl get(String name) {
        val username = filterUsername(name);
        return username.isPresent() ? crud.get(username.get()) : null;
    }

    public void add(String name) {
        addPermission(name, AclPermission.DATA_QUERY);
    }

    public void delete(String name) {
        val username = filterUsername(name);
        if (username.isPresent()) {
            crud.delete(username.get());
        }
    }

    public void addPermission(String name, Permission permission) {
        addPermission(name, Sets.newHashSet(permission));
    }

    public void addPermission(String name, Set<Permission> permissions) {
        val username = filterUsername(name);
        if (!username.isPresent()) {
            UserAcl userAcl = new UserAcl(name, permissions);
            crud.save(userAcl);
        } else {
            UserAcl userAcl = get(username.get());
            updateAcl(userAcl, (UserAcl acl) -> permissions.forEach(acl::addPermission));
        }
    }

    public void addDataQueryProject(String name, String project) {
        val username = filterUsername(name);
        if (!username.isPresent()) {
            UserAcl userAcl = new UserAcl(name, Collections.emptySet());
            userAcl.addDataQueryProject(project);
            crud.save(userAcl);
        } else {
            UserAcl userAcl = get(username.get());
            if (!userAcl.hasPermission(AclPermission.DATA_QUERY)) {
                updateAcl(userAcl, (UserAcl acl) -> acl.addDataQueryProject(project));
            }
        }
    }

    public void deletePermission(String name, Permission permission) {
        val username = filterUsername(name);
        if (!username.isPresent()) {
            return;
        }
        UserAcl userAcl = get(username.get());
        updateAcl(userAcl, (UserAcl acl) -> {
            acl.deletePermission(permission);
            acl.setDataQueryProjects(Collections.emptyList());
        });
    }

    public void deleteDataQueryProject(String name, String project) {
        val username = filterUsername(name);
        if (!username.isPresent()) {
            return;
        }
        UserAcl userAcl = get(username.get());
        if (!userAcl.hasPermission(AclPermission.DATA_QUERY)) {
            updateAcl(userAcl, (UserAcl acl) -> acl.deleteDataQueryProject(project));
        }
    }

    public interface UserAclUpdater {
        void update(UserAcl copyForWrite);
    }

    private UserAcl updateAcl(UserAcl acl, UserAclUpdater updater) {
        val copyForWrite = crud.copyForWrite(acl);
        updater.update(copyForWrite);
        crud.save(copyForWrite);
        return get(acl.getUsername());
    }

}

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

package org.apache.kylin.rest.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.AclEntity;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.security.ManagedUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.AccessControlEntry;
import org.springframework.security.acls.model.Acl;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.provisioning.UserDetailsManager;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;

/**
 */
@Component("userService")
public class UserService implements UserDetailsManager {

    private Logger logger = LoggerFactory.getLogger(UserService.class);

    public static final String DIR_PREFIX = "/user/";

    public static final String SUPER_ADMIN = "ADMIN";

    public static final Serializer<ManagedUser> SERIALIZER = new JsonSerializer<>(ManagedUser.class);

    protected ResourceStore aclStore;

    private boolean evictCacheFlag = false;

    @Autowired
    @Qualifier("accessService")
    private AccessService accessService;

    @Autowired
    @Qualifier("projectService")
    private ProjectService projectService;

    public boolean isEvictCacheFlag() {
        return evictCacheFlag;
    }

    public void setEvictCacheFlag(boolean evictCacheFlag) {
        this.evictCacheFlag = evictCacheFlag;
    }

    @PostConstruct
    public void init() throws IOException {
        aclStore = ResourceStore.getStore(KylinConfig.getInstanceFromEnv());
    }

    @Override
    //@PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN) --- DON'T DO THIS, CAUSES CIRCULAR DEPENDENCY BETWEEN UserService & AclService
    public void createUser(UserDetails user) {
        updateUser(user);
    }

    @Override
    //@PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN) --- DON'T DO THIS, CAUSES CIRCULAR DEPENDENCY BETWEEN UserService & AclService
    public void updateUser(UserDetails user) {
        Preconditions.checkState(user instanceof ManagedUser, "User {} is not ManagedUser", user);
        ManagedUser managedUser = (ManagedUser) user;
        try {
            String id = getId(user.getUsername());
            aclStore.putResourceWithoutCheck(id, managedUser, System.currentTimeMillis(), SERIALIZER);
            logger.trace("update user : {}", user.getUsername());
        } catch (IOException e) {
            throw new InternalErrorException(e);
        }
    }

    @Override
    public void deleteUser(String userName) {
        if (userName.equals(SUPER_ADMIN))
            throw new InternalErrorException("User " + userName + " is not allowed to be deleted.");

        try {
            //revoke user's project permission
            List<ProjectInstance> projectInstances = projectService.listProjects(null, null);
            for (ProjectInstance pi : projectInstances) {
                AclEntity ae = accessService.getAclEntity("ProjectInstance", pi.getUuid());
                Acl acl = accessService.getAcl(ae);

                if (acl != null) {
                    for (AccessControlEntry ace : acl.getEntries()) {
                        if (((PrincipalSid) ace.getSid()).getPrincipal().equals(userName)) {
                            accessService.revoke(ae, userName);
                        }
                    }
                }
            }

            String id = getId(userName);
            aclStore.deleteResource(id);
            logger.trace("delete user : {}", userName);
        } catch (IOException e) {
            throw new InternalErrorException(e);
        }
    }

    @Override
    public void changePassword(String oldPassword, String newPassword) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean userExists(String userName) {
        try {
            logger.trace("judge user exist: {}", userName);
            return aclStore.exists(getId(userName));
        } catch (IOException e) {
            throw new InternalErrorException(e);
        }
    }

    /**
     * 
     * @return a ManagedUser
     */
    @Override
    public UserDetails loadUserByUsername(String userName) throws UsernameNotFoundException {
        Message msg = MsgPicker.getMsg();
        try {
            ManagedUser managedUser = aclStore.getResource(getId(userName), ManagedUser.class, SERIALIZER);
            if (managedUser == null) {
                throw new UsernameNotFoundException(String.format(msg.getUSER_NOT_FOUND(), userName));
            }
            logger.trace("load user : {}", userName);
            return managedUser;
        } catch (IOException e) {
            throw new InternalErrorException(e);
        }
    }

    public List<String> listUserAuthorities() throws IOException {
        List<String> all = new ArrayList<String>();
        for (UserDetails user : listUsers()) {
            for (GrantedAuthority auth : user.getAuthorities()) {
                if (!all.contains(auth.getAuthority())) {
                    all.add(auth.getAuthority());
                }
            }
        }
        return all;
    }

    public List<ManagedUser> listUsers() throws IOException {
        return aclStore.getAllResources(DIR_PREFIX, ManagedUser.class, SERIALIZER);
    }

    public static String getId(String userName) {
        return DIR_PREFIX + userName;
    }

}

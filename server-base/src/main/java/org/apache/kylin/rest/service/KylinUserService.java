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

import javax.annotation.Nullable;
import javax.annotation.PostConstruct;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.security.ManagedUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UsernameNotFoundException;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class KylinUserService implements UserService {

    private Logger logger = LoggerFactory.getLogger(KylinUserService.class);

    public static final String DIR_PREFIX = "/user/";

    public static final String SUPER_ADMIN = "ADMIN";

    public static final Serializer<ManagedUser> SERIALIZER = new JsonSerializer<>(ManagedUser.class);

    protected ResourceStore aclStore;

    private boolean evictCacheFlag = false;

    @Override
    public boolean isEvictCacheFlag() {
        return evictCacheFlag;
    }

    @Override
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
            setEvictCacheFlag(true);
        } catch (IOException e) {
            throw new InternalErrorException(e);
        }
    }

    @Override
    public void deleteUser(String userName) {
        if (userName.equals(SUPER_ADMIN))
            throw new InternalErrorException("User " + userName + " is not allowed to be deleted.");

        try {
            String id = getId(userName);
            aclStore.deleteResource(id);
            logger.trace("delete user : {}", userName);
            setEvictCacheFlag(true);
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

    @Override
    public List<ManagedUser> listUsers() throws IOException {
        return aclStore.getAllResources(DIR_PREFIX, ManagedUser.class, SERIALIZER);
    }

    @Override
    public List<String> listUsernames() throws IOException {
        List<String> paths = new ArrayList<>();
        paths.addAll(aclStore.listResources(ResourceStore.USER_ROOT));
        List<String> users = Lists.transform(paths, new Function<String, String>() {
            @Nullable
            @Override
            public String apply(@Nullable String input) {
                String[] path = input.split("/");
                Preconditions.checkArgument(path.length == 3);
                return path[2];
            }
        });
        return users;
    }

    @Override
    public List<String> listAdminUsers() throws IOException{
        List<String> adminUsers = new ArrayList<>();
        for (ManagedUser managedUser : listUsers()) {
            if (managedUser.getAuthorities().contains(new SimpleGrantedAuthority(Constant.ROLE_ADMIN))) {
                adminUsers.add(managedUser.getUsername());
            }
        }
        return adminUsers;
    }

    @Override
    public void completeUserInfo(ManagedUser user){
    }

    public static String getId(String userName) {
        return DIR_PREFIX + userName;
    }

}

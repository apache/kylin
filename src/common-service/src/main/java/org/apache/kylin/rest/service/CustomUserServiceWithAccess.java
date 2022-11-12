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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.user.ManagedUser;
import org.apache.kylin.metadata.user.NKylinUserManager;
import org.apache.kylin.query.security.AccessDeniedException;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.security.AccessProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.util.Assert;

import java.io.IOException;
import java.util.List;

public class CustomUserServiceWithAccess extends OpenUserService{

    private Logger logger = LoggerFactory.getLogger(CustomUserServiceWithAccess.class);

    public static final String SUPER_ADMIN = "ADMIN";

    private final AccessProvider accessProvider;

    private final UserService userService;

    public CustomUserServiceWithAccess(AccessProvider accessProvider, UserService userService) {
        Assert.notNull(accessProvider, "The accessProvider should not be null.");
        Assert.notNull(userService, "The userService should not be null.");
        this.accessProvider = accessProvider;
        this.userService = userService;
    }

    /**
     * check whether the new user can access Kylin System
     */
    @Override
    public void createUser(UserDetails user) {
        accessProvider.checkAccessible(user.getUsername());//visit bdp hive or other corp inner service
        userService.createUser(user);
    }

    @Override
    public void updateUser(UserDetails user) {
        userService.updateUser(user);
    }

    @Override
    public boolean userExists(String userName){
        boolean ifRegistered = userService.userExists(userName);
        if(!ifRegistered){
            try{
                accessProvider.checkAccessible(userName);// default true
            } catch (AccessDeniedException e){
                logger.info("user {} is not able to access Kylin due to ", userName, e);
                return false;
            }
            ManagedUser newUser = new ManagedUser(userName);
            userService.createUser(newUser);
        }
        return true;
    }



    @Override
    public List<ManagedUser> listUsers() throws IOException {
        return userService.listUsers();
    }

    @Override
    public List<String> listAdminUsers() throws IOException {
        return userService.listAdminUsers();
    }

    @Override
    public void completeUserInfo(ManagedUser user) {
        userService.completeUserInfo(user);
    }


    @Override
    public UserDetails loadUserByUsername(String userName) {
        if(!userExists(userName)){//if not in registered list, then need to check BDP access ability
            throw new AccessDeniedException(userName + " is unable to access Kylin System");
        }
        return userService.loadUserByUsername(userName);
    }

    @Override
    public void deleteUser(String userName) {
        if (userName.equals(SUPER_ADMIN)) {
            throw new InternalErrorException("User " + userName + " is not allowed to be deleted.");
        }

        getKylinUserManager().delete(userName);
        logger.trace("delete user : {}", userName);
    }

    protected NKylinUserManager getKylinUserManager() {
        return NKylinUserManager.getInstance(KylinConfig.getInstanceFromEnv());
    }

}

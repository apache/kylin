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

import static org.apache.kylin.common.exception.ServerErrorCode.DUPLICATE_USER_NAME;
import static org.apache.kylin.common.exception.ServerErrorCode.PERMISSION_DENIED;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.USER_LOGIN_FAILED;
import static org.apache.kylin.rest.constant.Constant.ROLE_ADMIN;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.rest.aspect.Transaction;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.security.AclPermission;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.util.CollectionUtils;

import com.google.common.base.Preconditions;

import org.apache.kylin.metadata.user.ManagedUser;
import org.apache.kylin.metadata.user.NKylinUserManager;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KylinUserService implements UserService {

    public static final String DIR_PREFIX = "/user/";

    public static final Serializer<ManagedUser> SERIALIZER = new JsonSerializer<>(ManagedUser.class);

    @Autowired
    @Qualifier("userAclService")
    protected UserAclService userAclService;

    @Override
    @Transaction
    //@PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN) --- DON'T DO THIS, CAUSES CIRCULAR DEPENDENCY BETWEEN UserService & AclService
    public void createUser(UserDetails user) {
        if (getKylinUserManager().exists(user.getUsername())) {
            throw new KylinException(DUPLICATE_USER_NAME,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getUserExists(), user.getUsername()));
        }
        updateUser(user);
    }

    @Override
    @Transaction
    //@PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN) --- DON'T DO THIS, CAUSES CIRCULAR DEPENDENCY BETWEEN UserService & AclService
    public void updateUser(UserDetails user) {
        Preconditions.checkState(user instanceof ManagedUser, "User {} is not ManagedUser", user);
        ManagedUser managedUser = (ManagedUser) user;

        if (!managedUser.getAuthorities().contains(new SimpleGrantedAuthority(Constant.GROUP_ALL_USERS))) {
            throw new KylinException(PERMISSION_DENIED, MsgPicker.getMsg().getInvalidRemoveUserFromAllUser());
        }
        getKylinUserManager().update(managedUser);
        userAclService.updateUserAclPermission(user, AclPermission.DATA_QUERY);
        log.trace("update user : {}", user.getUsername());
    }

    @SneakyThrows
    @Override
    @Transaction
    public void deleteUser(String userName) {
        val superAdminUsers = listSuperAdminUsers();
        if (!CollectionUtils.isEmpty(superAdminUsers) && !superAdminUsers.stream()
                .filter(u -> u.equalsIgnoreCase(userName)).collect(Collectors.toList()).isEmpty()) {
            throw new InternalErrorException("User " + userName + " is not allowed to be deleted.");
        }

        userAclService.deleteUserAcl(userName);
        getKylinUserManager().delete(userName);
        log.trace("delete user : {}", userName);
    }

    @Override
    public void changePassword(String oldPassword, String newPassword) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean userExists(String userName) {
        log.trace("judge user exist: {}", userName);
        return getKylinUserManager().exists(userName);
    }

    /**
     *
     * @return a ManagedUser
     */
    @Override
    public UserDetails loadUserByUsername(String userName) throws UsernameNotFoundException {
        Message msg = MsgPicker.getMsg();
        ManagedUser managedUser;
        try {
            managedUser = getKylinUserManager().get(userName);
        } catch (IllegalArgumentException e) {
            log.error("exception: ", e);
            throw new UsernameNotFoundException(USER_LOGIN_FAILED.getMsg());
        }
        if (managedUser == null) {
            throw new UsernameNotFoundException(String.format(Locale.ROOT, msg.getUserNotFound(), userName));
        }
        log.trace("load user : {}", userName);
        return managedUser;
    }

    @Override
    public List<ManagedUser> listUsers() {
        return getKylinUserManager().list();
    }

    @Override
    public List<ManagedUser> listUsers(boolean needSort) {
        return getKylinUserManager().list(needSort);
    }

    @Override
    public List<String> listAdminUsers() {
        SimpleGrantedAuthority adminAuthority = new SimpleGrantedAuthority(Constant.ROLE_ADMIN);
        return listUsers().stream().filter(user -> user.getAuthorities().contains(adminAuthority))
                .map(ManagedUser::getUsername).collect(Collectors.toList());
    }

    @Override
    public boolean isGlobalAdmin(String username) {
        try {
            UserDetails userDetails = loadUserByUsername(username);
            return isGlobalAdmin(userDetails);
        } catch (Exception e) {
            logger.debug("Cat not load user by username {}", username, e);
        }
        return false;
    }

    @Override
    public boolean isGlobalAdmin(UserDetails userDetails) {
        if (Objects.isNull(userDetails)) {
            return false;
        }
        return userDetails.getAuthorities().stream()
                .anyMatch(grantedAuthority -> grantedAuthority.getAuthority().equals(ROLE_ADMIN));
    }

    @Override
    public boolean containsGlobalAdmin(Set<String> usernames) {
        return usernames.stream().anyMatch(this::isGlobalAdmin);
    }

    @Override
    public Set<String> retainsNormalUser(Set<String> usernames) {
        return usernames.stream().filter(username -> !isGlobalAdmin(username)).collect(Collectors.toSet());
    }

    @Override
    public List<String> listNormalUsers() {
        SimpleGrantedAuthority adminAuthority = new SimpleGrantedAuthority(Constant.ROLE_ADMIN);
        return listUsers().stream().filter(user -> !user.getAuthorities().contains(adminAuthority))
                .map(ManagedUser::getUsername).collect(Collectors.toList());
    }

    @Override
    public List<String> listSuperAdminUsers() {
        return Collections.singletonList("ADMIN");
    }

    @Override
    public void completeUserInfo(ManagedUser user) {
    }

    public static String getId(String userName) {
        return DIR_PREFIX + userName;
    }

    protected NKylinUserManager getKylinUserManager() {
        return NKylinUserManager.getInstance(KylinConfig.getInstanceFromEnv());
    }
}

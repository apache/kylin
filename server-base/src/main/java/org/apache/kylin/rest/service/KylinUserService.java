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
import java.util.Locale;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinVersion;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.security.KylinUserManager;
import org.apache.kylin.rest.security.ManagedUser;
import org.apache.kylin.rest.util.AclEvaluate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

import org.apache.kylin.shaded.com.google.common.base.Preconditions;

public class KylinUserService implements UserService {

    private Logger logger = LoggerFactory.getLogger(KylinUserService.class);
    @Autowired
    private AclEvaluate aclEvaluate;

    public static final String DIR_PREFIX = "/user/";

    public static final String SUPER_ADMIN = "ADMIN";

    public static final Serializer<ManagedUser> SERIALIZER = new JsonSerializer<>(ManagedUser.class);

    private static final String ADMIN = "ADMIN";
    private static final String MODELER = "MODELER";
    private static final String ANALYST = "ANALYST";
    private static final String ADMIN_DEFAULT = "KYLIN";
    private BCryptPasswordEncoder pwdEncoder;
    public List<User> configUsers;

    public KylinUserService() {
    }

    public KylinUserService(List<User> users) throws IOException {
        pwdEncoder = new BCryptPasswordEncoder();
        synchronized (KylinUserService.class) {
            KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            if (!StringUtils.equals("testing", kylinConfig.getSecurityProfile())) {
                return;
            }
            List<ManagedUser> all = listUsers();
            configUsers = users;
            // old security.xml config user pwd sync to user metadata
            if (!configUsers.isEmpty()) {
                for (User cuser : configUsers) {
                    try {
                        String username = cuser.getUsername();
                        ManagedUser userDetail = (ManagedUser) loadUserByUsername(username);
                        if (userDetail != null && new KylinVersion(userDetail.getVersion()).major < KylinVersion
                                .getCurrentVersion().major) {
                            updateUser(new ManagedUser(cuser.getUsername(), cuser.getPassword(), false,
                                    cuser.getAuthorities()));
                        }
                    } catch (UsernameNotFoundException e) {
                        // add new security user in security.xml if it is not in metadata
                        createUser(new ManagedUser(cuser.getUsername(), cuser.getPassword(), false,
                                cuser.getAuthorities()));
                    }
                }
            }
            // add default user info in metadata
            if (all.isEmpty() && configUsers.isEmpty()) {
                createUser(new ManagedUser(ADMIN, pwdEncoder.encode(ADMIN_DEFAULT), true, Constant.ROLE_ADMIN,
                        Constant.GROUP_ALL_USERS));
                createUser(new ManagedUser(ANALYST, pwdEncoder.encode(ANALYST), true, Constant.GROUP_ALL_USERS));
                createUser(new ManagedUser(MODELER, pwdEncoder.encode(MODELER), true, Constant.GROUP_ALL_USERS));
            }
        }

    }

    protected ResourceStore aclStore;

    @PostConstruct
    public void init() throws IOException {
        aclStore = ResourceStore.getStore(KylinConfig.getInstanceFromEnv());

        // check members
        if (pwdEncoder == null) {
            pwdEncoder = new BCryptPasswordEncoder();
        }
        // add default admin user if there is none
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        if (kylinConfig.createAdminWhenAbsent() && listAdminUsers().isEmpty()) {
            logger.info("default admin user created: username=ADMIN, password=*****");
            createUser(new ManagedUser(ADMIN, pwdEncoder.encode(ADMIN_DEFAULT), true, Constant.ROLE_ADMIN,
                    Constant.GROUP_ALL_USERS));
        }
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
        if (!managedUser.getAuthorities().contains(new SimpleGrantedAuthority(Constant.GROUP_ALL_USERS))) {
            managedUser.addAuthorities(Constant.GROUP_ALL_USERS);
        }
        getKylinUserManager().update(managedUser);
        logger.trace("update user : {}", user.getUsername());
    }

    @Override
    public void deleteUser(String userName) {
        if (userName.equalsIgnoreCase(SUPER_ADMIN)) {
            throw new InternalErrorException("User " + userName + " is not allowed to be deleted.");
        }
        getKylinUserManager().delete(userName);
        logger.trace("delete user : {}", userName);
    }

    @Override
    public void changePassword(String oldPassword, String newPassword) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean userExists(String userName) {
        logger.trace("judge user exist: {}", userName);
        return getKylinUserManager().exists(userName);
    }

    /**
     * 
     * @return a ManagedUser
     */
    @Override
    public UserDetails loadUserByUsername(String userName) throws UsernameNotFoundException {
        Message msg = MsgPicker.getMsg();
        ManagedUser managedUser = getKylinUserManager().get(userName);
        if (managedUser == null) {
            throw new UsernameNotFoundException(String.format(Locale.ROOT, msg.getUSER_NOT_FOUND(), userName));
        }
        logger.trace("load user : {}", userName);
        return managedUser;
    }

    @Override
    public List<ManagedUser> listUsers() throws IOException {
        return getKylinUserManager().list();
    }

    @Override
    public List<ManagedUser> listUsers(String userName, Boolean isFuzzMatch) throws IOException {
        List<ManagedUser> userList = getKylinUserManager().list();
        return getManagedUsersByFuzzMatching(userName, isFuzzMatch, userList, null);
    }

    @Override
    public List<ManagedUser> listUsers(String userName, String groupName, Boolean isFuzzMatch) throws IOException {
        List<ManagedUser> userList = getKylinUserManager().list();
        return getManagedUsersByFuzzMatching(userName, isFuzzMatch, userList, groupName);
    }

    @Override
    public List<String> listAdminUsers() throws IOException {
        List<String> adminUsers = new ArrayList<>();
        for (ManagedUser managedUser : listUsers()) {
            if (managedUser.getAuthorities().contains(new SimpleGrantedAuthority(Constant.ROLE_ADMIN))) {
                adminUsers.add(managedUser.getUsername());
            }
        }
        return adminUsers;
    }

    @Override
    public void completeUserInfo(ManagedUser user) {
    }

    public static String getId(String userName) {
        return DIR_PREFIX + userName;
    }

    private KylinUserManager getKylinUserManager() {
        return KylinUserManager.getInstance(KylinConfig.getInstanceFromEnv());
    }

    private List<ManagedUser> getManagedUsersByFuzzMatching(String nameSeg, boolean isFuzzMatch,
            List<ManagedUser> userList, String groupName) {
        aclEvaluate.checkIsGlobalAdmin();
        //for name fuzzy matching
        if (StringUtils.isBlank(nameSeg) && StringUtils.isBlank(groupName)) {
            return userList;
        }

        List<ManagedUser> usersByFuzzyMatching = new ArrayList<>();
        for (ManagedUser u : userList) {
            if (!isFuzzMatch && StringUtils.equals(u.getUsername(), nameSeg) && isUserInGroup(u, groupName)) {
                usersByFuzzyMatching.add(u);
            }
            if (isFuzzMatch && StringUtils.containsIgnoreCase(u.getUsername(), nameSeg)
                    && isUserInGroup(u, groupName)) {
                usersByFuzzyMatching.add(u);
            }

        }
        return usersByFuzzyMatching;
    }

    private boolean isUserInGroup(ManagedUser user, String groupName) {
        return StringUtils.isBlank(groupName) || user.getAuthorities().contains(new SimpleGrantedAuthority(groupName));
    }
}

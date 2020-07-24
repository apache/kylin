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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.persistence.WriteConflictException;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.acl.UserGroup;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.security.ManagedUser;
import org.apache.kylin.rest.util.AclEvaluate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

public class KylinUserGroupService extends UserGroupService {
    public static final Logger logger = LoggerFactory.getLogger(KylinUserGroupService.class);

    private ResourceStore store = ResourceStore.getStore(KylinConfig.getInstanceFromEnv());
    private static final String PATH = "/user_group/";
    private static final Serializer<UserGroup> USER_GROUP_SERIALIZER = new JsonSerializer<>(UserGroup.class);
    @Autowired
    @Qualifier("userService")
    private UserService userService;

    @Autowired
    private AclEvaluate aclEvaluate;

    @Override
    public boolean exists(String name) throws IOException {
        return getUserGroup().getAllGroups().contains(name);
    }

    @Autowired
    @Qualifier("accessService")
    private AccessService accessService;

    @PostConstruct
    public void init() throws IOException, InterruptedException {
        int retry = 100;
        while (retry > 0) {
            UserGroup userGroup = getUserGroup();
            if (!userGroup.exists(Constant.GROUP_ALL_USERS)) {
                userGroup.add(Constant.GROUP_ALL_USERS);
            }
            if (!userGroup.exists(Constant.ROLE_ADMIN)) {
                userGroup.add(Constant.ROLE_ADMIN);
            }
            if (!userGroup.exists(Constant.ROLE_MODELER)) {
                userGroup.add(Constant.ROLE_MODELER);
            }
            if (!userGroup.exists(Constant.ROLE_ANALYST)) {
                userGroup.add(Constant.ROLE_ANALYST);
            }

            try {
                store.checkAndPutResource(PATH, userGroup, USER_GROUP_SERIALIZER);
                return;
            } catch (WriteConflictException e) {
                logger.info("Find WriteConflictException, sleep 100 ms.", e);
                Thread.sleep(100L);
                retry--;
            }
        }
        logger.error("Failed to update user group's metadata.");
    }

    private UserGroup getUserGroup() throws IOException {
        UserGroup userGroup = store.getResource(PATH, USER_GROUP_SERIALIZER);
        if (userGroup == null) {
            userGroup = new UserGroup();
        }
        return userGroup;
    }

    @Override
    protected List<String> getAllUserGroups() throws IOException {
        return getUserGroup().getAllGroups();
    }

    @Override
    public Map<String, List<String>> getGroupMembersMap() throws IOException {
        Map<String, List<String>> result = Maps.newHashMap();
        List<ManagedUser> users = userService.listUsers();
        for (ManagedUser user : users) {
            for (SimpleGrantedAuthority authority : user.getAuthorities()) {
                String role = authority.getAuthority();
                List<String> usersInGroup = result.get(role);
                if (usersInGroup == null) {
                    result.put(role, Lists.newArrayList(user.getUsername()));
                } else {
                    usersInGroup.add(user.getUsername());
                }
            }
        }
        return result;
    }

    @Override
    public List<ManagedUser> getGroupMembersByName(String name) throws IOException {
        List<ManagedUser> users = userService.listUsers();
        for (Iterator<ManagedUser> it = users.iterator(); it.hasNext();) {
            ManagedUser user = it.next();
            if (!user.getAuthorities().contains(new SimpleGrantedAuthority(name))) {
                it.remove();
            }
        }
        return users;
    }

    @Override
    public void addGroup(String name) throws IOException {
        aclEvaluate.checkIsGlobalAdmin();
        UserGroup userGroup = getUserGroup();
        store.checkAndPutResource(PATH, userGroup.add(name), USER_GROUP_SERIALIZER);
    }

    @Override
    public void deleteGroup(String name) throws IOException {
        aclEvaluate.checkIsGlobalAdmin();
        // remove retained user group in all users
        List<ManagedUser> managedUsers = userService.listUsers();
        for (ManagedUser managedUser : managedUsers) {
            if (managedUser.getAuthorities().contains(new SimpleGrantedAuthority(name))) {
                managedUser.removeAuthorities(name);
                userService.updateUser(managedUser);
            }
        }
        //delete group's project ACL
        accessService.revokeProjectPermission(name, MetadataConstants.TYPE_GROUP);
        //delete group's table/row/column ACL
        //        ACLOperationUtil.delLowLevelACL(name, MetadataConstants.TYPE_GROUP);

        store.checkAndPutResource(PATH, getUserGroup().delete(name), USER_GROUP_SERIALIZER);
    }

    //user's group information is stored by user its own.Object user group does not hold user's ref.
    @Override
    public void modifyGroupUsers(String groupName, List<String> users) throws IOException {
        aclEvaluate.checkIsGlobalAdmin();
        List<String> groupUsers = new ArrayList<>();
        for (ManagedUser user : getGroupMembersByName(groupName)) {
            groupUsers.add(user.getUsername());
        }
        List<String> moveInUsers = Lists.newArrayList(users);
        List<String> moveOutUsers = Lists.newArrayList(groupUsers);
        moveInUsers.removeAll(groupUsers);
        moveOutUsers.removeAll(users);

        for (String in : moveInUsers) {
            ManagedUser managedUser = (ManagedUser) userService.loadUserByUsername(in);
            managedUser.addAuthorities(groupName);
            userService.updateUser(managedUser);
        }

        for (String out : moveOutUsers) {
            ManagedUser managedUser = (ManagedUser) userService.loadUserByUsername(out);
            managedUser.removeAuthorities(groupName);
            userService.updateUser(managedUser);
        }
    }
}

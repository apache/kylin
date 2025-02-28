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

import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_UPDATE_USER;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_USERGROUP_NAME;
import static org.apache.kylin.common.exception.ServerErrorCode.PERMISSION_DENIED;
import static org.apache.kylin.common.exception.ServerErrorCode.USERGROUP_NOT_EXIST;
import static org.apache.kylin.rest.constant.Constant.GROUP_ALL_USERS;
import static org.apache.kylin.rest.constant.Constant.ROLE_ADMIN;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.user.ManagedUser;
import org.apache.kylin.metadata.user.NKylinUserManager;
import org.apache.kylin.metadata.usergroup.NUserGroupManager;
import org.apache.kylin.metadata.usergroup.UserGroup;
import org.apache.kylin.rest.aspect.Transaction;
import org.apache.kylin.rest.response.UserGroupResponseKI;
import org.apache.kylin.rest.util.AclEvaluate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.stereotype.Component;

import lombok.val;

@Component("nUserGroupService")
public class NUserGroupService implements IUserGroupService {
    public static final Logger logger = LoggerFactory.getLogger(NUserGroupService.class);

    @Autowired
    @Qualifier("accessService")
    private AccessService accessService;

    @Autowired
    AclEvaluate aclEvaluate;

    @Autowired
    @Qualifier("userService")
    UserService userService;

    @Override
    public List<String> getAllUserGroups() {
        return getUserGroupManager().getAllGroupNames();
    }

    @Override
    public List<ManagedUser> getGroupMembersByName(String name) throws IOException {
        List<ManagedUser> users = userService.listUsers();
        users.removeIf(user -> !user.getAuthorities().contains(new SimpleGrantedAuthority(name)));
        return users;
    }

    @Override
    @Transaction
    public void addGroup(String name) {
        aclEvaluate.checkIsGlobalAdmin();
        getUserGroupManager().add(name);
    }

    @Override
    @Transaction
    public void deleteGroup(String name) throws IOException {
        aclEvaluate.checkIsGlobalAdmin();
        checkGroupCanBeDeleted(name);
        // remove retained user group in all users
        SimpleGrantedAuthority simpleAuthority = new SimpleGrantedAuthority(name);
        userService.listUsers(false).stream().filter(
                user -> user.getAuthorities().parallelStream().anyMatch(authority -> authority.equals(simpleAuthority)))
                .forEach(user -> {
                    user.removeAuthorities(name);
                    userService.updateUser(user);
                });
        //delete group's project ACL
        accessService.revokeProjectPermission(name, MetadataConstants.TYPE_GROUP);
        getUserGroupManager().delete(name);
    }

    //user's group information is stored by user its own.Object user group does not hold user's ref.
    @Override
    @Transaction
    public void modifyGroupUsers(String groupName, List<String> users) throws IOException {
        aclEvaluate.checkIsGlobalAdmin();
        checkGroupNameExist(groupName);

        List<String> groupUsers = getGroupMembersByName(groupName).stream().map(ManagedUser::getUsername)
                .collect(Collectors.toList());
        List<String> moveInUsers = Lists.newArrayList(users);
        List<String> moveOutUsers = Lists.newArrayList(groupUsers);
        moveInUsers.removeAll(groupUsers);
        moveOutUsers.removeAll(users);

        val msg = MsgPicker.getMsg();

        String currentUser = aclEvaluate.getCurrentUserName();

        List<String> moveList = Lists.newArrayList();
        moveList.addAll(moveInUsers);
        moveList.addAll(moveOutUsers);
        val superAdminList = userService.listSuperAdminUsers();
        for (String user : moveList) {
            if (!CollectionUtils.isEmpty(superAdminList) && superAdminList.stream()
                    .filter(superAdmin -> superAdmin.equalsIgnoreCase(user)).collect(Collectors.toList()).size() > 0) {
                throw new KylinException(PERMISSION_DENIED, MsgPicker.getMsg().getChangeGlobaladmin());

            }
            if (StringUtils.equalsIgnoreCase(currentUser, user)) {
                throw new KylinException(FAILED_UPDATE_USER, msg.getSelfEditForbidden());
            }
        }

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

    @Override
    public List<String> listAllAuthorities() {
        aclEvaluate.checkIsGlobalAdmin();
        return getAllUserGroups();
    }

    @Override
    public List<String> getAuthoritiesFilterByGroupName(String userGroupName) {
        aclEvaluate.checkIsGlobalAdmin();
        return StringUtils.isEmpty(userGroupName) ? getAllUserGroups()
                : getAllUserGroups().stream()
                        .filter(userGroup -> StringUtils.containsIgnoreCase(userGroup, userGroupName))
                        .collect(Collectors.toList());
    }

    @Override
    public List<UserGroup> listUserGroups() {
        return getUserGroupManager().getAllGroups();
    }

    @Override
    public List<UserGroup> getUserGroupsFilterByGroupName(String userGroupName) {
        aclEvaluate.checkIsGlobalAdmin();
        if (StringUtils.isEmpty(userGroupName)) {
            return listUserGroups();
        }
        return getUserGroupManager().getGroupsByName(userGroupName, true);
    }

    @Override
    public String getGroupNameByUuid(String uuid) {
        val groups = getUserGroupManager().getAllGroups();
        for (val group : groups) {
            if (StringUtils.equalsIgnoreCase(uuid, group.getUuid())) {
                return group.getGroupName();
            }
        }
        throw new KylinException(USERGROUP_NOT_EXIST,
                String.format(Locale.ROOT, MsgPicker.getMsg().getGroupUuidNotExist(), uuid));
    }

    @Override
    public String getUuidByGroupName(String groupName) {
        if (StringUtils.isEmpty(groupName)) {
            throw new KylinException(USERGROUP_NOT_EXIST,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getUserGroupNotExist(), groupName));
        }
        val optional = getUserGroupManager().getGroupsByName(groupName, false).stream().findFirst();
        if (!optional.isPresent()) {
            throw new KylinException(USERGROUP_NOT_EXIST,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getUserGroupNotExist(), groupName));
        }
        return optional.get().getUuid();
    }

    public boolean exists(String name) {
        return getUserGroupManager().exists(name);
    }

    public ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
    }

    private void checkGroupNameExist(String groupName) {
        val groups = getAllUserGroups();
        if (!groups.contains(groupName)) {
            throw new KylinException(INVALID_PARAMETER,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getUserGroupNotExist(), groupName));
        }
    }

    public Map<String, List<String>> getUserAndUserGroup() throws IOException {
        Map<String, List<String>> result = Maps.newHashMap();

        List<String> userNames = userService.getManagedUsersByFuzzMatching(null, false).stream()
                .map(ManagedUser::getUsername).collect(Collectors.toList());
        List<String> groupNames = getAllUserGroups();

        result.put("user", userNames);
        result.put("group", groupNames);
        return result;
    }

    public Set<String> listUserGroups(String username) {
        ManagedUser user = NKylinUserManager.getInstance(KylinConfig.getInstanceFromEnv()).get(username);
        if (Objects.isNull(user)) {
            return Collections.emptySet();
        }
        return user.getAuthorities().stream().map(GrantedAuthority::getAuthority).filter(this::exists)
                .collect(Collectors.toSet());
    }

    private NUserGroupManager getUserGroupManager() {
        return NUserGroupManager.getInstance(KylinConfig.getInstanceFromEnv());
    }

    public List<UserGroupResponseKI> getUserGroupResponse(List<UserGroup> userGroups) throws IOException {
        List<UserGroupResponseKI> result = new ArrayList<>();

        List<String> groupNames = userGroups.stream().map(UserGroup::getGroupName).collect(Collectors.toList());

        Map<String, Set<String>> groupMembersMap = userService.listUsers(false).parallelStream()
                .filter(user -> user.getAuthorities().stream()
                        .anyMatch(authority -> groupNames.contains(authority.getAuthority())))
                .map(user -> user.getAuthorities().stream().map(SimpleGrantedAuthority::getAuthority)
                        .filter(groupNames::contains)
                        .collect(Collectors.toMap(Function.identity(), authority -> user.getUsername())))
                .map(Map::entrySet).flatMap(Collection::stream).collect(Collectors.groupingBy(Map.Entry::getKey,
                        Collectors.mapping(Map.Entry::getValue, Collectors.toSet())));

        for (UserGroup group : userGroups) {
            result.add(new UserGroupResponseKI(group.getUuid(), group.getGroupName(),
                    groupMembersMap.getOrDefault(group.getGroupName(), new HashSet<>())));
        }
        return result;
    }

    @Override
    @Transaction
    public void addGroups(List<String> groups) {
        aclEvaluate.checkIsGlobalAdmin();
        getUserGroupManager().batchAdd(groups);
    }

    protected List<UserGroup> getUserGroupSpecialUuid() {
        List<String> groups = getAllUserGroups();
        List<UserGroup> result = new ArrayList<>();
        for (String group : groups) {
            UserGroup userGroup = new UserGroup();
            userGroup.setUuid(group);
            userGroup.setGroupName(group);
            result.add(userGroup);
        }
        return result;
    }

    private void checkGroupCanBeDeleted(String groupName) {
        if (groupName.equals(GROUP_ALL_USERS) || groupName.equals(ROLE_ADMIN)) {
            throw new KylinException(INVALID_USERGROUP_NAME,
                    "Failed to delete user group, user groups of ALL_USERS and ROLE_ADMIN cannot be deleted.");
        }
    }

}

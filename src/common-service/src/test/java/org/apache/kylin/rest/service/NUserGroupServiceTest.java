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

import static org.apache.kylin.rest.constant.Constant.GROUP_ALL_USERS;
import static org.apache.kylin.rest.constant.Constant.ROLE_ADMIN;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.transaction.TransactionException;
import io.kyligence.kap.metadata.user.ManagedUser;
import org.apache.kylin.metadata.usergroup.UserGroup;
import org.apache.kylin.rest.response.UserGroupResponseKI;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import lombok.val;
import lombok.var;

public class NUserGroupServiceTest extends ServiceTestBase {

    @Autowired
    @Qualifier("nUserGroupService")
    private IUserGroupService userGroupService;

    @Autowired
    @Qualifier("userService")
    private UserService userService;

    @Test
    public void testBasic() throws IOException {
        for (String group : userGroupService.getAllUserGroups()) {
            userGroupService.deleteGroup(group);
        }
        //test group add and get
        //        userGroupService.addGroup(GROUP_ALL_USERS);
        userGroupService.addGroup("g1");
        userGroupService.addGroup("g2");
        userGroupService.addGroup("g3");
        Assert.assertEquals(Lists.newArrayList("g1", "g2", "g3"), userGroupService.getAllUserGroups());
        Assert.assertEquals(Lists.newArrayList("g1", "g2", "g3"),
                userGroupService.getAuthoritiesFilterByGroupName("G"));
        Assert.assertEquals(Lists.newArrayList("g1"), userGroupService.getAuthoritiesFilterByGroupName("g1"));
        val groups = userGroupService.getUserGroupsFilterByGroupName("G");
        Assert.assertEquals(3, groups.size());
        for (val group : groups) {
            Assert.assertNotNull(group.getUuid());
            Assert.assertTrue(group.getGroupName().contains("g"));
            Assert.assertEquals(group.getUuid(), userGroupService.getUuidByGroupName(group.getGroupName()));
            Assert.assertThrows(KylinException.class, () -> userGroupService.getUuidByGroupName(""));
            Assert.assertEquals(group.getGroupName(), userGroupService.getGroupNameByUuid(group.getUuid()));
            Assert.assertThrows(KylinException.class, () -> userGroupService.getGroupNameByUuid(""));
        }

        // test add a existing user group
        try {
            userGroupService.addGroup("g1");
        } catch (Exception e) {
            Assert.assertTrue(
                    StringUtils.contains(e.getCause().getCause().getMessage(), "user group \"g1\" already exists"));
        }

        //test modify users in user group
        for (int i = 1; i <= 6; i++) {
            userService.updateUser(new ManagedUser("u" + i, "kylin", false));
        }
        userGroupService.modifyGroupUsers("g1", Lists.newArrayList("u1", "u3", "u5"));
        userGroupService.modifyGroupUsers("g2", Lists.newArrayList("u2", "u4", "u6"));

        checkResult();

        userGroupService.modifyGroupUsers("g1", Lists.newArrayList("u3", "u5"));
        Assert.assertEquals(Lists.newArrayList(new SimpleGrantedAuthority(GROUP_ALL_USERS)),
                userService.loadUserByUsername("u1").getAuthorities());

        //test delete
        Assert.assertEquals(
                Lists.newArrayList(new SimpleGrantedAuthority(GROUP_ALL_USERS), new SimpleGrantedAuthority("g1")),
                userService.loadUserByUsername("u3").getAuthorities());
        Assert.assertEquals(
                Lists.newArrayList(new SimpleGrantedAuthority(GROUP_ALL_USERS), new SimpleGrantedAuthority("g1")),
                userService.loadUserByUsername("u5").getAuthorities());
        userGroupService.deleteGroup("g1");
        Assert.assertEquals(Lists.newArrayList(new SimpleGrantedAuthority(GROUP_ALL_USERS)),
                userService.loadUserByUsername("u3").getAuthorities());
        Assert.assertEquals(Lists.newArrayList(new SimpleGrantedAuthority(GROUP_ALL_USERS)),
                userService.loadUserByUsername("u5").getAuthorities());

        Map<String, List<String>> result = userGroupService.getUserAndUserGroup();
        Assert.assertEquals(2, result.size());
        Assert.assertEquals(9, result.get("user").size());
        Assert.assertEquals(2, result.get("group").size());
        Assert.assertEquals("g2", result.get("group").get(0));
        Assert.assertEquals("g3", result.get("group").get(1));
    }

    private void checkResult() throws IOException {
        Assert.assertEquals(Lists.newArrayList("u1", "u3", "u5"), getUsers("g1"));
        Assert.assertEquals(Lists.newArrayList("u2", "u4", "u6"), getUsers("g2"));
        Assert.assertEquals(0, userGroupService.getGroupMembersByName("g3").size());
        Assert.assertEquals(
                Lists.newArrayList(new SimpleGrantedAuthority(GROUP_ALL_USERS), new SimpleGrantedAuthority("g1")),
                userService.loadUserByUsername("u1").getAuthorities());
        Assert.assertEquals(
                Lists.newArrayList(new SimpleGrantedAuthority(GROUP_ALL_USERS), new SimpleGrantedAuthority("g2")),
                userService.loadUserByUsername("u2").getAuthorities());
    }

    private List<String> getUsers(String groupName) throws IOException {
        List<String> users = new ArrayList<>();
        for (ManagedUser u : userGroupService.getGroupMembersByName(groupName)) {
            users.add(u.getUsername());
        }
        return users;
    }

    @Test
    public void testAddUserToNotExistGroup() throws Exception {
        try {
            userGroupService.modifyGroupUsers("UNKNOWN", Arrays.asList("ADMIN"));
        } catch (TransactionException e) {
            Assert.assertTrue(e.getCause().getCause() instanceof KylinException);
            Assert.assertTrue(StringUtils.equals(e.getCause().getCause().getMessage(),
                    "Invalid values in parameter “group_name“. The value UNKNOWN doesn’t exist."));
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testListUserGroups() throws IOException {
        userGroupService.addGroup("t1");
        userGroupService.addGroup("t2");
        userGroupService.modifyGroupUsers("t1", Arrays.asList("MODELER"));
        userGroupService.modifyGroupUsers("t2", Arrays.asList("MODELER"));

        var groups = userGroupService.listUserGroups("MODELER");
        Assert.assertEquals(2, groups.size());
        Assert.assertTrue(groups.contains("t1"));
        Assert.assertTrue(groups.contains("t2"));
        userGroupService.addGroup("t3");
        userGroupService.modifyGroupUsers("t3", Arrays.asList("MODELER"));
        groups = userGroupService.listUserGroups("MODELER");
        Assert.assertEquals(3, groups.size());
        Assert.assertTrue(groups.contains("t3"));
        List<String> userList = Arrays.asList("ADMIN");
        Assert.assertThrows(RuntimeException.class, () -> userGroupService.modifyGroupUsers("t1", userList));
    }

    @Test
    public void testGetUserGroupResponse() throws IOException {
        List<String> users = new ArrayList<>();
        users.add("MODELER");
        userGroupService.addGroup("t1");
        userGroupService.addGroup("t2");
        userGroupService.addGroup("t3");
        userGroupService.modifyGroupUsers("t1", users);
        userGroupService.modifyGroupUsers("t2", users);
        List<UserGroup> groups = userGroupService.getUserGroupsFilterByGroupName(null);
        Assert.assertEquals(3, groups.size());
        List<UserGroupResponseKI> result = userGroupService.getUserGroupResponse(groups);
        Assert.assertEquals(3, result.size());
        for (val response : result) {
            val groupAndUser = response.getUserGroupAndUsers();
            Assert.assertEquals(response.getGroupName(), groupAndUser.getFirst());
            Assert.assertTrue(Sets.difference(response.getUsers(), groupAndUser.getSecond()).isEmpty());
            if (response.getGroupName().equals("t3")) {
                Assert.assertEquals(0, response.getUsers().size());
            } else {
                Assert.assertEquals(1, response.getUsers().size());
                Assert.assertTrue(response.getUsers().contains("MODELER"));
            }
        }
    }

    @Test
    public void testDelAdminAndAllUsers() {
        checkDelUserGroupWithException(ROLE_ADMIN);
        checkDelUserGroupWithException(GROUP_ALL_USERS);
    }

    @Test
    public void testAddGroups() throws IOException {
        userGroupService.addGroups(Arrays.asList("g1", "g2", "g3"));
        Assert.assertEquals(Lists.newArrayList("g1", "g2", "g3"), userGroupService.getAllUserGroups());
    }

    private void checkDelUserGroupWithException(String groupName) {
        try {
            userGroupService.deleteGroup(groupName);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(ExceptionUtils.getRootCause(e) instanceof KylinException);
            Assert.assertTrue(ExceptionUtils.getRootCause(e).getMessage().contains(
                    "Failed to delete user group, user groups of ALL_USERS and ROLE_ADMIN cannot be deleted."));
        }
    }

}

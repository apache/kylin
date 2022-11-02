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

import java.io.FileInputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.user.ManagedUser;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.io.ClassPathResource;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import com.google.common.collect.Lists;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = ServiceTestBase.SpringConfig.class)
@WebAppConfiguration(value = "src/main/resources")
@ActiveProfiles({ "custom", "test" })
public class OpenUserServiceTest extends NLocalFileMetadataTestCase {

    @Autowired
    @Qualifier("userService")
    private OpenUserService userService;

    @Autowired
    @Qualifier("userGroupService")
    private OpenUserGroupService userGroupService;

    @Autowired
    @Qualifier("customAuthProvider")
    private AuthenticationProvider authenticationProvider;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void setupResource() throws Exception {
        staticCreateTestMetadata();
        Properties ldapConfig = new Properties();
        ldapConfig.load(new FileInputStream(new ClassPathResource("ut_custom/custom-config.properties").getFile()));
        final KylinConfig kylinConfig = getTestConfig();
        ldapConfig.forEach((k, v) -> kylinConfig.setProperty(k.toString(), v.toString()));

        Authentication authentication = new TestingAuthenticationToken("ADMIN", "123456", Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    @AfterClass
    public static void cleanupResource() {
        staticCleanupTestMetadata();
    }

    @Test
    public void testBasic() {

        Assert.assertNotNull(userService);

        // test list users
        List<ManagedUser> managedUsers = userService.listUsers();
        List<String> userName = Lists.newArrayList();
        List<ManagedUser> adminUsers = Lists.newArrayList();
        for (ManagedUser user : managedUsers) {
            userName.add(user.getUsername());
            if (user.getAuthorities().contains(new SimpleGrantedAuthority(Constant.ROLE_ADMIN))) {
                adminUsers.add(user);
            }
        }
        Assert.assertEquals(2, userName.size());
        Assert.assertTrue(userName.contains("admin"));
        Assert.assertTrue(userName.contains("test"));

        //test list admin
        List<String> admins = userService.listAdminUsers();
        Assert.assertEquals(admins.size(), adminUsers.size());
        for (ManagedUser user : adminUsers) {
            if (!admins.contains(user.getUsername())) {
                throw new RuntimeException("test get admin fail");
            }
        }

        //test list groups
        Assert.assertTrue(userService.userExists("test"));
        Assert.assertFalse(userService.userExists("test2"));

        Assert.assertNotNull(userGroupService);
        List<String> allUserGroups = userGroupService.getAllUserGroups();
        Assert.assertEquals(2, allUserGroups.size());
        Assert.assertTrue(allUserGroups.contains(Constant.ROLE_ADMIN));
        Assert.assertTrue(allUserGroups.contains(Constant.ROLE_ANALYST));

        //test get user by group
        List<ManagedUser> groupMembersByName = userGroupService.getGroupMembersByName(Constant.ROLE_ADMIN);
        Assert.assertEquals(1, groupMembersByName.size());
        Assert.assertEquals("admin", groupMembersByName.get(0).getUsername());
    }

    @Test
    public void testCreateUser() {
        thrown.expect(UnsupportedOperationException.class);
        userService.createUser(null);
    }

    @Test
    public void testUpdateUser() {
        thrown.expect(UnsupportedOperationException.class);
        userService.updateUser(null);
    }

    @Test
    public void testDeleteUser() {
        thrown.expect(UnsupportedOperationException.class);
        userService.deleteUser("ben");
    }

    @Test
    public void testChangePassword() {
        thrown.expect(UnsupportedOperationException.class);
        userService.changePassword("old", "new");
    }

    @Test
    public void testUserExists() {
        Assert.assertTrue(userService.userExists("test"));
    }

    @Test
    public void testUserNotExists() {
        Assert.assertFalse(userService.userExists("ben"));
    }

    @Test
    public void testAddGroup() {
        thrown.expect(UnsupportedOperationException.class);
        userGroupService.addGroup("gg");
    }

    @Test
    public void testUpdateGroup() {
        thrown.expect(UnsupportedOperationException.class);
        userGroupService.modifyGroupUsers("gg", Lists.newArrayList());
    }

    @Test
    public void testDeleteGroup() {
        thrown.expect(UnsupportedOperationException.class);
        userGroupService.deleteGroup("gg");
    }

    @Test
    public void testGetUserAndUserGroup() throws Exception {
        Map<String, List<String>> groupUsers = userGroupService.getUserAndUserGroup();
        Assert.assertTrue(groupUsers.containsKey(Constant.ROLE_ADMIN));
        Assert.assertTrue(groupUsers.containsKey(Constant.ROLE_ANALYST));
        Assert.assertTrue(groupUsers.get(Constant.ROLE_ADMIN).contains("admin"));
        Assert.assertTrue(groupUsers.get(Constant.ROLE_ANALYST).contains("test"));
    }

    @Test
    public void testBeanInit() {
        Assert.assertTrue(
                userService.getClass().getName().startsWith("org.apache.kylin.rest.service.StaticUserService"));
        Assert.assertTrue(userGroupService.getClass().getName()
                .startsWith("org.apache.kylin.rest.service.StaticUserGroupService"));
        Assert.assertTrue(authenticationProvider.getClass().getName()
                .startsWith("org.apache.kylin.rest.security.StaticAuthenticationProvider"));
    }
}

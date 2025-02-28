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

import static org.apache.kylin.rest.constant.Constant.ROLE_ADMIN;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.metadata.user.ManagedUser;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.GlobalAccessRequest;
import org.apache.kylin.rest.request.GlobalBatchAccessRequest;
import org.apache.kylin.rest.security.AclPermission;
import org.apache.kylin.rest.security.UserAclManager;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.test.util.ReflectionTestUtils;

import lombok.SneakyThrows;

public class UserAclServiceTest extends ServiceTestBase {

    @Mock
    protected UserAclService userAclService = Mockito.spy(new UserAclService());

    @Autowired
    @Qualifier("userService")
    UserService userService;

    @Mock
    AclEvaluate aclEvaluate = Mockito.spy(new AclEvaluate());

    @Spy
    AclUtil aclUtil = Mockito.spy(new AclUtil());

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() {
        super.setUp();
        getTestConfig().setProperty("kylin.security.acl.data-permission-default-enabled", "true");
        ReflectionTestUtils.setField(userAclService, "userService", userService);
        ReflectionTestUtils.setField(aclEvaluate, "userAclService", userAclService);
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
    }

    @Test
    public void testCreateUser() {
        getTestConfig().setProperty("kylin.security.acl.data-permission-default-enabled", "false");
        if (!userService.userExists("ADMIN1")) {
            userService.createUser(new ManagedUser("ADMIN1", "ADMIN1", false, Arrays.asList(//
                    new SimpleGrantedAuthority(Constant.ROLE_ADMIN))));
        }
        userAclService.deleteUserAcl("ADMIN1");
        userAclService.revokeUserAclPermission("ADMIN1", "DATA_QUERY");
        Assert.assertFalse(userAclService.hasUserAclPermission("ADMIN1", AclPermission.DATA_QUERY));
        userAclService.grantUserAclPermission("ADMIN1", "DATA_QUERY");
        Assert.assertTrue(userAclService.hasUserAclPermission("ADMIN1", AclPermission.DATA_QUERY));
        Assert.assertEquals(2, userAclService.listUserAcl().size());
        userAclService.revokeUserAclPermission("ADMIN1", "DATA_QUERY");
        Assert.assertFalse(userAclService.hasUserAclPermission("ADMIN1", AclPermission.DATA_QUERY));

        GlobalAccessRequest accessRequest = new GlobalAccessRequest();
        accessRequest.setUsername("ADMIN1");
        accessRequest.setProject("default");
        userAclService.addProjectToUserAcl(accessRequest, "DATA_QUERY");
        aclEvaluate.checkProjectQueryPermission("default");
        Assert.assertTrue(
                userAclService.hasUserAclPermissionInProject(accessRequest.getUsername(), accessRequest.getProject()));
        userAclService.deleteProjectFromUserAcl(accessRequest, "DATA_QUERY");
        Assert.assertFalse(
                userAclService.hasUserAclPermissionInProject(accessRequest.getUsername(), accessRequest.getProject()));

        ReflectionTestUtils.setField(userAclService, "userService", new KylinUserService() {
            public List<String> listSuperAdminUsers() {
                return Arrays.asList("ADMIN", "ADMIN1", "ADMIN2");
            }
        });
        Assert.assertEquals(3, userAclService.listUserAcl().size());
        Assert.assertTrue(userAclService.listUserAcl().get(0).getExtPermissions().contains("DATA_QUERY"));
        Assert.assertTrue(userAclService.listUserAcl().get(1).getExtPermissions().contains("DATA_QUERY"));
        Assert.assertTrue(userAclService.listUserAcl().get(2).getExtPermissions().contains("DATA_QUERY"));

        thrown.expect(KylinException.class);
        thrown.expectMessage(MsgPicker.getMsg().getModifyPermissionOfSuperAdminFailed());
        userAclService.grantUserAclPermission("admin", "DATA_QUERY");
    }

    @Test
    public void testGetAllUsersHasGlobalPermission() {
        KylinUserService kylinUserService = new KylinUserService() {
            @SneakyThrows
            @Override
            public List<String> listAdminUsers() {
                throw new IOException("test");
            }
        };
        ReflectionTestUtils.setField(userAclService, "userService", kylinUserService);
        Assert.assertTrue(userAclService.listUserAcl().isEmpty());
    }

    @Test
    public void testGrantUserAclExceptions() {
        Assert.assertThrows(KylinException.class, () -> userAclService.grantUserAclPermission("ADMIN", "DATA_QUERY"));
    }

    @Test
    public void testRevokeUserAclExceptions() {
        Assert.assertThrows(KylinException.class, () -> userAclService.revokeUserAclPermission("ADMIN", "DATA_QUERY"));
    }

    @Test
    public void testCheckAclPermission() {
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ReflectionTestUtils.invokeMethod(userAclService, "checkAclPermission", "", ""));
        Assert.assertThrows(MsgPicker.getMsg().getModifyPermissionOfSuperAdminFailed(), KylinException.class,
                () -> ReflectionTestUtils.invokeMethod(userAclService, "checkAclPermission", "admin", "DATA_QUERY"));

        ReflectionTestUtils.setField(userAclService, "userService", new KylinUserService() {
            public List<String> listSuperAdminUsers() {
                return Collections.emptyList();
            }
        });
        Assert.assertThrows(MsgPicker.getMsg().getModifyOwnPermissionFailed(), KylinException.class,
                () -> ReflectionTestUtils.invokeMethod(userAclService, "checkAclPermission", "admin", "DATA_QUERY"));
        UserAclManager manager = userAclService.getManager(UserAclManager.class);
        manager.addPermission("admin", AclPermission.DATA_QUERY);
        Assert.assertThrows(MsgPicker.getMsg().getGrantPermissionFailedByNonSystemAdmin(), KylinException.class,
                () -> ReflectionTestUtils.invokeMethod(userAclService, "checkAclPermission", "test", "DATA_QUERY"));
        ReflectionTestUtils.setField(userAclService, "userService", userService);
    }

    @Test
    public void testCheckAclPermissionException() {
        ReflectionTestUtils.setField(userAclService, "userService", new KylinUserService() {
            public List<String> listSuperAdminUsers() {
                return Collections.emptyList();
            }
        });
        UserAclManager manager = userAclService.getManager(UserAclManager.class);
        manager.deletePermission("admin", AclPermission.DATA_QUERY);
        Assert.assertTrue(userAclService.listUserAcl().stream()
                .allMatch(userAcl -> CollectionUtils.isEmpty(userAcl.getExtPermissions())));
        Assert.assertThrows(MsgPicker.getMsg().getGrantPermissionFailedByIllegalAuthorizingUser(), KylinException.class,
                () -> ReflectionTestUtils.invokeMethod(userAclService, "checkLoginUserPermission"));
        Assert.assertThrows(MsgPicker.getMsg().getGrantPermissionFailedByIllegalAuthorizingUser(), KylinException.class,
                () -> ReflectionTestUtils.invokeMethod(userAclService, "checkLoginUserPermissionInPrj", "default"));
        Assert.assertFalse(userAclService.hasUserAclPermissionInProject("default"));
        Assert.assertThrows(AccessDeniedException.class,
                () -> ReflectionTestUtils.invokeMethod(aclEvaluate, "checkProjectQueryPermission", "default"));
    }

    @Test
    public void testCheckAdminUser() {
        thrown.expect(KylinException.class);
        thrown.expectMessage(MsgPicker.getMsg().getEmptySid());
        ReflectionTestUtils.invokeMethod(userAclService, "checkAdminUser", "");
        thrown.expect(KylinException.class);
        thrown.expectMessage(
                String.format(Locale.ROOT, MsgPicker.getMsg().getOperationFailedByUserNotExist(), "test_not"));
        ReflectionTestUtils.invokeMethod(userAclService, "checkAdminUser", "test_not");
        thrown.expect(KylinException.class);
        thrown.expectMessage(MsgPicker.getMsg().getGrantPermissionFailedByNonSystemAdmin());
        ReflectionTestUtils.invokeMethod(userAclService, "checkAdminUser", "test");
    }

    @Test
    public void testUpdateGlobalPermission() {
        if (!userService.userExists("ADMIN1")) {
            userService.createUser(new ManagedUser("ADMIN1", "ADMIN1", false, Arrays.asList(//
                    new SimpleGrantedAuthority(Constant.ROLE_ADMIN))));
        }
        userAclService.grantUserAclPermission("ADMIN1", "DATA_QUERY");
        Assert.assertTrue(userAclService.hasUserAclPermission("ADMIN1", AclPermission.DATA_QUERY));
        UserDetails userDetails = userService.loadUserByUsername("ADMIN1");
        userDetails.getAuthorities().remove(new SimpleGrantedAuthority(ROLE_ADMIN));
        userService.updateUser(userDetails);
        Assert.assertFalse(userAclService.hasUserAclPermission("ADMIN1", AclPermission.DATA_QUERY));
        Assert.assertFalse(userAclService.getManager(UserAclManager.class).exists("admin1"));

        if (!userService.userExists("ADMIN2")) {
            userService.createUser(new ManagedUser("ADMIN2", "ADMIN2", false,
                    Arrays.asList(new SimpleGrantedAuthority(Constant.ROLE_ADMIN))));
        }
        userAclService.deleteUserAcl("ADMIN2");
        userAclService.updateUserAclPermission(userService.loadUserByUsername("ADMIN2"), AclPermission.DATA_QUERY);
        Assert.assertTrue(userAclService.hasUserAclPermission("ADMIN2", AclPermission.DATA_QUERY));

        getTestConfig().setProperty("kylin.security.acl.data-permission-default-enabled", "false");
        if (!userService.userExists("ADMIN3")) {
            userService.createUser(new ManagedUser("ADMIN3", "ADMIN3", false,
                    Arrays.asList(new SimpleGrantedAuthority(Constant.ROLE_ADMIN))));
        }
        userAclService.updateUserAclPermission(userService.loadUserByUsername("ADMIN3"), AclPermission.DATA_QUERY);
        Assert.assertFalse(userAclService.hasUserAclPermission("ADMIN3", AclPermission.DATA_QUERY));
    }

    @Test
    public void testDeleteUser() {
        getTestConfig().setProperty("kylin.security.acl.data-permission-default-enabled", "false");
        if (!userService.userExists("ADMIN4")) {
            userService.createUser(new ManagedUser("ADMIN4", "ADMIN4", false, Arrays.asList(//
                    new SimpleGrantedAuthority(Constant.ROLE_ADMIN))));
        }
        Assert.assertFalse(userAclService.hasUserAclPermission("ADMIN4", AclPermission.DATA_QUERY));
        userService.deleteUser("ADMIN4");
        Assert.assertFalse(userAclService.getManager(UserAclManager.class).exists("ADMIN4"));

        getTestConfig().setProperty("kylin.security.acl.data-permission-default-enabled", "true");
        if (!userService.userExists("ADMIN4")) {
            userService.createUser(new ManagedUser("ADMIN4", "ADMIN4", false, Arrays.asList(//
                    new SimpleGrantedAuthority(Constant.ROLE_ADMIN))));
        }
        Assert.assertTrue(userAclService.hasUserAclPermission("ADMIN4", AclPermission.DATA_QUERY));
        userService.deleteUser("ADMIN4");
        Assert.assertFalse(userAclService.getManager(UserAclManager.class).exists("ADMIN4"));
    }

    @Test
    public void testSyncAdminUserAcl() {
        userAclService.syncAdminUserAcl();
        Assert.assertTrue(userAclService.hasUserAclPermission("admin", AclPermission.DATA_QUERY));
    }

    @Test
    public void testSuperAdmin() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        Assert.assertTrue(userAclService.isSuperAdmin(authentication.getName()));
        Assert.assertTrue(userAclService.canAdminUserQuery());
        Mockito.when(userAclService.isSuperAdmin(Mockito.anyString())).thenReturn(false);
        Assert.assertTrue(userAclService.canAdminUserQuery());
    }

    @Test
    public void testBatchGrantUserAclPermission() {
        getTestConfig().setProperty("kylin.security.acl.data-permission-default-enabled", "false");
        if (!userService.userExists("ADMIN1")) {
            userService.createUser(new ManagedUser("ADMIN1", "ADMIN1", false, Arrays.asList(//
                    new SimpleGrantedAuthority(Constant.ROLE_ADMIN))));
        }
        if (!userService.userExists("ADMIN2")) {
            userService.createUser(new ManagedUser("ADMIN2", "ADMIN2", false, Arrays.asList(//
                    new SimpleGrantedAuthority(Constant.ROLE_ADMIN))));
        }
        GlobalBatchAccessRequest globalBatchAccessRequest = new GlobalBatchAccessRequest();
        globalBatchAccessRequest.setUsernameList(Arrays.asList("ADMIN1", "ADMIN2"));
        userAclService.grantUserAclPermission(globalBatchAccessRequest, "DATA_QUERY");
        Assert.assertTrue(userAclService.hasUserAclPermission("ADMIN1", AclPermission.DATA_QUERY));
        Assert.assertTrue(userAclService.hasUserAclPermission("ADMIN2", AclPermission.DATA_QUERY));

    }
}

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

package org.apache.kylin.rest.controller;

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.REPEATED_PARAMETER;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.USER_GROUP_NOT_EXIST;
import static org.apache.kylin.rest.constant.Constant.GROUP_ALL_USERS;
import static org.apache.kylin.rest.constant.Constant.ROLE_ADMIN;
import static org.apache.kylin.rest.constant.Constant.ROLE_ANALYST;
import static org.apache.kylin.rest.constant.Constant.ROLE_MODELER;
import static org.hamcrest.CoreMatchers.containsString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.junit.rule.ClearKEPropertiesRule;
import org.apache.kylin.metadata.user.ManagedUser;
import org.apache.kylin.rest.request.PasswordChangeRequest;
import org.apache.kylin.rest.request.UserRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.AclTCRService;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.service.UserAclService;
import org.apache.kylin.rest.service.UserService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.springframework.core.env.Environment;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.session.SessionRegistry;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.session.FindByIndexNameSessionRepository;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.accept.ContentNegotiationManager;

import lombok.val;

public class NUserControllerTest extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;
    private static final BCryptPasswordEncoder pwdEncoder = new BCryptPasswordEncoder();

    @Rule
    public ClearKEPropertiesRule clearKEProperties = new ClearKEPropertiesRule();

    @InjectMocks
    private NUserController nUserController;

    @Mock
    private UserService userService;

    @Mock
    UserAclService userAclService;

    @Mock
    private AclEvaluate aclEvaluate;

    @Mock
    private AclTCRService aclTCRService;

    @Mock
    private IUserGroupService userGroupService;

    @Mock
    private SessionRegistry sessionRegistry;

    @Spy
    private FindByIndexNameSessionRepository sessionRepository;

    @Mock
    Environment env;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    private AccessService accessService;

    @Before
    public void setupResource() {
        super.createTestMetadata();
        getTestConfig().setProperty("kylin.env", "UT");
        nUserController = Mockito.spy(new NUserController());
    }

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        Mockito.doReturn(true).when(env).acceptsProfiles("testing", "custom");
        ContentNegotiationManager contentNegotiationManager = new ContentNegotiationManager();
        mockMvc = MockMvcBuilders.standaloneSetup(nUserController)
                .setContentNegotiationManager(contentNegotiationManager).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();
        List<GrantedAuthority> authorities = new ArrayList<GrantedAuthority>();
        ManagedUser user = new ManagedUser("ADMIN", "ADMIN", false, authorities);
        Authentication authentication = new TestingAuthenticationToken(user, "ADMIN", ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(authentication);
        ReflectionTestUtils.setField(nUserController, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(nUserController, "userAclService", userAclService);
        ReflectionTestUtils.setField(nUserController, "sessionRepository", sessionRepository);
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testBasics() {
        EnvelopeResponse<UserDetails> userDetailsEnvelopeResponse = nUserController.authenticatedUser();
        Assert.assertNotNull(userDetailsEnvelopeResponse);
        Assert.assertEquals(KylinException.CODE_SUCCESS, userDetailsEnvelopeResponse.getCode());
    }

    @Test
    public void testCreateUser() throws Exception {
        val user = new UserRequest();
        user.setUsername("azAZ_#");
        user.setPassword("p14532522?");
        user.setDisabled(false);
        userGroupService.addGroup(GROUP_ALL_USERS);
        user.setAuthorities(Lists.newArrayList(GROUP_ALL_USERS));
        Mockito.doReturn(true).when(userGroupService).exists(GROUP_ALL_USERS);
        Mockito.doNothing().when(userService).createUser(Mockito.any(UserDetails.class));
        Mockito.doReturn(Lists.newArrayList(GROUP_ALL_USERS)).when(userGroupService).getAllUserGroups();
        mockMvc.perform(MockMvcRequestBuilders.post("/api/user").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(user))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nUserController).createUser(Mockito.any(UserRequest.class));

        Map<String, Object> requestMap = Maps.newHashMap();
        requestMap.put("disabled", "123");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/user").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(requestMap))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().is5xxServerError());
        Mockito.verify(nUserController).createUser(Mockito.any(UserRequest.class));

    }

    @Test
    public void testCreateUserWithEmptyUsername() {
        val user = new UserRequest();
        user.setUsername("");
        user.setPassword("p1234sgw$");
        user.setDisabled(false);
        assertKylinExeption(() -> {
            nUserController.createUser(user);
        }, "Username should not be empty.");

        user.setUsername(".abc");
        assertKylinExeption(() -> {
            nUserController.createUser(user);
        }, "The user / group names cannot start with a period");

        user.setUsername(" abc ");
        assertKylinExeption(() -> {
            nUserController.createUser(user);
        }, "User / group names cannot start or end with a space");

        user.setUsername(" abc ");
        assertKylinExeption(() -> {
            nUserController.createUser(user);
        }, "User / group names cannot start or end with a space");

        user.setUsername("useraaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab");
        assertKylinExeption(() -> {
            nUserController.createUser(user);
        }, Message.getInstance().getInvalidNameLength());

        user.setUsername("<1qaz>");
        assertKylinExeption(() -> {
            nUserController.createUser(user);
        }, "The user / group names cannot contain the following symbols:");
    }

    @Test
    public void testCreateUserWithEmptyGroup() throws IOException {
        val user = new UserRequest();
        user.setUsername("test");
        user.setPassword("p1234sgw$");
        user.setDisabled(false);
        thrown.expect(KylinException.class);
        thrown.expectMessage(REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY.getMsg("authorities"));
        nUserController.createUser(user);
    }

    @Test
    public void testCreateUserWithNotExistGroup() throws IOException {
        val user = new UserRequest();
        user.setUsername("test");
        user.setPassword("p1234sgw$");
        user.setDisabled(false);
        userGroupService.addGroup(GROUP_ALL_USERS);
        user.setAuthorities(Lists.newArrayList(GROUP_ALL_USERS));
        Mockito.doReturn(false).when(userGroupService).exists(GROUP_ALL_USERS);
        thrown.expect(KylinException.class);
        thrown.expectMessage(USER_GROUP_NOT_EXIST.getMsg("ALL_USERS"));
        nUserController.createUser(user);
    }

    @Test
    public void testCreateUserWithDuplicatedGroup() throws IOException {
        val user = new UserRequest();
        user.setUsername("test");
        user.setPassword("p1234sgw$");
        user.setDisabled(false);
        userGroupService.addGroup(GROUP_ALL_USERS);
        List<String> groups = Lists.newArrayList(GROUP_ALL_USERS, GROUP_ALL_USERS);
        Mockito.doReturn(true).when(userGroupService).exists(GROUP_ALL_USERS);
        Mockito.doReturn(Lists.newArrayList(GROUP_ALL_USERS)).when(userGroupService).getAllUserGroups();
        user.setAuthorities(groups);
        thrown.expect(KylinException.class);
        thrown.expectMessage(REPEATED_PARAMETER.getMsg("authorities"));
        nUserController.createUser(user);
    }

    @Test
    public void testCreateUserWithNotEnglishUsername() throws IOException {
        val user = new UserRequest();
        user.setUsername("中文");
        user.setPassword("p1234sgw$");
        user.setDisabled(false);
        thrown.expect(KylinException.class);
        thrown.expectMessage(Message.getInstance().getInvalidNameContainsOtherCharacter());
        nUserController.createUser(user);
    }

    @Test(expected = KylinException.class)
    public void testCreateUserWithIlegalCharacter() throws IOException {
        val user = new UserRequest();
        user.setUsername("gggg?k");
        user.setPassword("p1234sgw$");
        nUserController.createUser(user);
    }

    @Test
    public void testCreateUserWithNullPassword() throws IOException {
        UserRequest userRequest = new UserRequest();
        userRequest.setDisabled(false);
        userRequest.setUsername("123");
        userRequest.setPassword(null);
        thrown.expect(KylinException.class);
        thrown.expectMessage(REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY.getMsg("password"));
        nUserController.createUser(userRequest);
    }

    @Test
    public void testCreateUser_PasswordLength_Exception() throws Exception {
        val user = new UserRequest();
        user.setUsername("u1");
        user.setPassword("p1");
        user.setDisabled(false);
        Mockito.doNothing().when(userService).createUser(Mockito.any(UserDetails.class));
        mockMvc.perform(MockMvcRequestBuilders.post("/api/user").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(user))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.content()
                        .string(containsString("The password should contain more than 8 characters!")));

        Mockito.verify(nUserController).createUser(Mockito.any(UserRequest.class));
    }

    @Test
    public void testCreateUser_InvalidPasswordPattern() throws Exception {
        val user = new UserRequest();
        user.setUsername("u1");
        user.setPassword("kylin123456");
        user.setDisabled(false);
        Mockito.doNothing().when(userService).createUser(Mockito.any(UserDetails.class));
        mockMvc.perform(MockMvcRequestBuilders.post("/api/user").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(user))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.content().string(containsString(
                        "The password should contain at least one number, letter and special character (~!@#$%^&*(){}|:\\\"<>?[];\\\\'\\\\,./`).")));

        Mockito.verify(nUserController).createUser(Mockito.any(UserRequest.class));
    }

    @Test
    public void testDelUserByUUID() throws Exception {
        ManagedUser user = new ManagedUser();
        user.setUuid("u1");
        user.setUsername("username1");

        Mockito.doReturn(Lists.newArrayList(user)).when(userService).listUsers();
        Mockito.doNothing().when(userService).deleteUser(Mockito.anyString());
        Mockito.doNothing().when(accessService).revokeProjectPermission(Mockito.anyString(), Mockito.anyString());
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/user/{uuid:.+}", "u1")
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nUserController).deleteByUUID("u1");
    }

    @Test
    public void testDelUserByUsername() throws Exception {
        String username = "username1";
        ManagedUser user = new ManagedUser();
        user.setUuid("u1");
        user.setUsername(username);

        Mockito.doReturn(user).when(userService).loadUserByUsername(username);
        Mockito.doReturn(Lists.newArrayList(user)).when(userService).listUsers();
        Mockito.doNothing().when(userService).deleteUser(Mockito.anyString());
        Mockito.doNothing().when(accessService).revokeProjectPermission(Mockito.anyString(), Mockito.anyString());

        nUserController.delete(username);
    }

    @Test
    public void testDelUserByUsernameException() throws Exception {
        String username = "username1";
        ManagedUser user = new ManagedUser();
        user.setUuid("u1");
        user.setUsername(username);

        Mockito.doReturn(Lists.newArrayList(user)).when(userService).listUsers();
        Mockito.doNothing().when(userService).deleteUser(Mockito.anyString());
        Mockito.doNothing().when(accessService).revokeProjectPermission(Mockito.anyString(), Mockito.anyString());

        thrown.expect(KylinException.class);
        thrown.expectMessage(String.format(Locale.ROOT, "User '%s' not found.", username));
        nUserController.delete(username);
    }

    @Test
    public void testUpdatePassword_UserNotFound() throws Exception {
        val request = new PasswordChangeRequest();
        request.setUsername("ADMIN");
        request.setPassword("KYLIN");
        request.setNewPassword("KYLIN1234@");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/user/password").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.content().string(containsString("User 'ADMIN' not found.")));

        Mockito.verify(nUserController).updateUserPassword(Mockito.any(PasswordChangeRequest.class));
    }

    @Test
    public void testUpdatePassword_Success() throws Exception {
        val user = new ManagedUser("ADMIN", pwdEncoder.encode("KYLIN"), false);
        val request = new PasswordChangeRequest();
        request.setUsername("ADMIN");
        request.setPassword("KYLIN");
        request.setNewPassword("KYLIN1234@");

        Mockito.doReturn(user).when(nUserController).getManagedUser("ADMIN");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/user/password").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nUserController).updateUserPassword(Mockito.any(PasswordChangeRequest.class));
    }

    @Test
    public void testUpdatePassword_OldSameAsNew() throws Exception {
        val user = new ManagedUser("ADMIN", pwdEncoder.encode("KYLIN1234@"), false);
        val request = new PasswordChangeRequest();
        request.setUsername("ADMIN");
        request.setPassword("KYLIN1234@");
        request.setNewPassword("KYLIN1234@");

        Mockito.doReturn(user).when(nUserController).getManagedUser("ADMIN");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/user/password").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.content()
                        .string(containsString("New password should not be same as old one!")));

        Mockito.verify(nUserController).updateUserPassword(Mockito.any(PasswordChangeRequest.class));
    }

    @Test
    public void testUpdatePassword_InvalidPasswordPattern() throws Exception {
        val user = new ManagedUser("ADMIN", pwdEncoder.encode("KYLIN"), false);
        val request = new PasswordChangeRequest();

        request.setUsername("ADMIN");
        request.setPassword("KYLIN");
        request.setNewPassword("kylin123456");
        Mockito.doReturn(user).when(nUserController).getManagedUser("ADMIN");

        mockMvc.perform(MockMvcRequestBuilders.put("/api/user/password").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.content().string(containsString(
                        "The password should contain at least one number, letter and special character")));

        Mockito.verify(nUserController).updateUserPassword(Mockito.any(PasswordChangeRequest.class));
    }

    @Test
    public void testUpdatePassword_InvalidPasswordLength() throws Exception {
        val user = new ManagedUser("ADMIN", pwdEncoder.encode("KYLIN"), false);
        val request = new PasswordChangeRequest();

        request.setUsername("ADMIN");
        request.setPassword("KYLIN");
        request.setNewPassword("123456");
        Mockito.doReturn(user).when(nUserController).getManagedUser("ADMIN");

        mockMvc.perform(MockMvcRequestBuilders.put("/api/user/password").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.content()
                        .string(containsString("The password should contain more than 8 characters!")));

        Mockito.verify(nUserController).updateUserPassword(Mockito.any(PasswordChangeRequest.class));
    }

    @Test
    public void testUpdatePasswordForNormalUser_Success() throws Exception {
        val user = new ManagedUser("ADMIN", pwdEncoder.encode("KYLIN"), false);
        Authentication authentication = new TestingAuthenticationToken(user, "MODELER", ROLE_MODELER);
        SecurityContextHolder.getContext().setAuthentication(authentication);
        val request = new PasswordChangeRequest();
        request.setUsername("ADMIN");
        request.setPassword("KYLIN");
        request.setNewPassword("KYLIN1234@");

        Mockito.doReturn(user).when(nUserController).getManagedUser("ADMIN");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/user/password").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nUserController).updateUserPassword(Mockito.any(PasswordChangeRequest.class));
    }

    @Test
    public void testUpdatePasswordForNormalUser_RequireOldPwdSuccess() throws Exception {
        val user = new ManagedUser("ADMIN", pwdEncoder.encode("KYLIN"), false);
        Authentication authentication = new TestingAuthenticationToken(user, "MODELER", ROLE_MODELER);
        SecurityContextHolder.getContext().setAuthentication(authentication);
        val request = new PasswordChangeRequest();
        request.setUsername("ADMIN");
        request.setPassword(StringUtils.EMPTY);
        request.setNewPassword("KYLIN1234@");

        Mockito.doReturn(user).when(nUserController).getManagedUser("ADMIN");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/user/password").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().is5xxServerError());

        Mockito.verify(nUserController).updateUserPassword(Mockito.any(PasswordChangeRequest.class));
    }

    @Test
    public void testUpdatePasswordForNormalUser_RequireOldPwdSuccess2() throws Exception {
        val user = new ManagedUser("ADMIN", pwdEncoder.encode("KYLIN"), false);
        Authentication authentication = new TestingAuthenticationToken(user, "MODELER", ROLE_MODELER);
        SecurityContextHolder.getContext().setAuthentication(authentication);
        val request = new PasswordChangeRequest();
        request.setUsername("ADMIN");
        request.setPassword("KYLIN");
        request.setNewPassword("KYLIN1234@");

        Mockito.doReturn(user).when(nUserController).getManagedUser("ADMIN");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/user/password").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nUserController).updateUserPassword(Mockito.any(PasswordChangeRequest.class));
    }

    @Test
    public void testUpdatePasswordForNormalUser_RequireNewPwdSuccess() throws Exception {
        val user = new ManagedUser("ADMIN", pwdEncoder.encode("KYLIN"), false);
        Authentication authentication = new TestingAuthenticationToken(user, "MODELER", ROLE_MODELER);
        SecurityContextHolder.getContext().setAuthentication(authentication);
        val request = new PasswordChangeRequest();
        request.setUsername("ADMIN");
        request.setPassword("KYLIN");
        request.setNewPassword(StringUtils.EMPTY);

        Mockito.doReturn(user).when(nUserController).getManagedUser("ADMIN");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/user/password").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().is5xxServerError());

        Mockito.verify(nUserController).updateUserPassword(Mockito.any(PasswordChangeRequest.class));
    }

    @Test
    public void testUpdatePasswordForNormalUser_WrongPassword() throws Exception {
        val user = new ManagedUser("ADMIN", pwdEncoder.encode("KYLIN"), false);
        Authentication authentication = new TestingAuthenticationToken(user, "ANALYST", ROLE_ANALYST);
        SecurityContextHolder.getContext().setAuthentication(authentication);
        val request = new PasswordChangeRequest();
        request.setUsername("ADMIN");
        request.setPassword("KYLIN1");
        request.setNewPassword("KYLIN1234@");

        Mockito.doReturn(user).when(nUserController).getManagedUser("ADMIN");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/user/password").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.content().string(containsString("Old password is not correct!")));

        Mockito.verify(nUserController).updateUserPassword(Mockito.any(PasswordChangeRequest.class));
    }

    @Test
    public void testListAll() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/user").contentType(MediaType.APPLICATION_JSON)
                .param("name", "KYLIN").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nUserController).listAllUsers(Mockito.anyString(), Mockito.anyBoolean(), Mockito.anyInt(),
                Mockito.anyInt());
    }

    @Test
    public void testUpdateUser() throws Exception {
        val user = new ManagedUser("ADMIN", pwdEncoder.encode("KYLIN"), false);
        val userRequest = new UserRequest();
        userRequest.setUsername("ADMIN");
        userRequest.setDisabled(false);
        Mockito.doReturn(true).when(userGroupService).exists("ALL_USERS");
        Mockito.doReturn(new HashSet<String>() {
            {
                add(GROUP_ALL_USERS);
            }
        }).when(userGroupService).listUserGroups(user.getUsername());
        Mockito.doReturn(new ArrayList<String>() {
            {
                add(GROUP_ALL_USERS);
            }
        }).when(userGroupService).getAllUserGroups();
        Mockito.doReturn(user).when(nUserController).getManagedUser("ADMIN");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/user").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(userRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nUserController).updateUser(Mockito.any(UserRequest.class));
    }

    @Test
    public void testUpdateUserWithDuplicatedGroup() throws Exception {
        val user = new ManagedUser();
        user.setUsername("ADMIN");
        user.setPassword("KYLIN1234@");
        val userRequest = new UserRequest();
        userRequest.setUsername(user.getUsername());
        userRequest.setPassword(user.getPassword());
        userGroupService.addGroup(GROUP_ALL_USERS);
        List<String> groups = Lists.newArrayList(GROUP_ALL_USERS, GROUP_ALL_USERS);
        Mockito.doReturn(user).when(nUserController).getManagedUser("ADMIN");
        Mockito.doReturn(new HashSet<String>() {
            {
                add(GROUP_ALL_USERS);
            }
        }).when(userGroupService).listUserGroups(user.getUsername());
        Mockito.doReturn(new ArrayList<String>() {
            {
                add(GROUP_ALL_USERS);
            }
        }).when(userGroupService).getAllUserGroups();
        Mockito.doReturn(true).when(userGroupService).exists(GROUP_ALL_USERS);
        userRequest.setAuthorities(groups);
        thrown.expect(KylinException.class);
        thrown.expectMessage(REPEATED_PARAMETER.getMsg("authorities"));
        nUserController.updateUser(userRequest);
    }

    @Test
    public void testUpdateOwnUserType() throws Exception {
        val user = new ManagedUser("ADMIN", pwdEncoder.encode("KYLIN"), false);
        val userRequest = new UserRequest();
        userRequest.setUsername(user.getUsername());
        userRequest.setPassword(user.getPassword());
        userRequest.setDefaultPassword(user.isDefaultPassword());
        HashSet<String> groups = new HashSet<String>() {
            {
                add(GROUP_ALL_USERS);
                add(ROLE_ADMIN);
            }
        };
        Mockito.doReturn(groups).when(userGroupService).listUserGroups(user.getUsername());
        Mockito.doReturn(user).when(nUserController).getManagedUser("ADMIN");
        Mockito.doReturn(true).when(userGroupService).exists(GROUP_ALL_USERS);
        Mockito.doReturn(true).when(userGroupService).exists(ROLE_ADMIN);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/user").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(userRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());
        Mockito.verify(nUserController).updateUser(Mockito.any(UserRequest.class));
    }

    @Test
    public void testBatchAddUsers() throws Exception {
        List<UserRequest> users = new ArrayList<>();
        {
            ManagedUser user = new ManagedUser();
            user.setPassword("KYLIN");
            user.setUsername("u1");
        }
        {
            ManagedUser user = new ManagedUser();
            user.setPassword("KYLIN");
            user.setUsername("u2");
        }
        {
            ManagedUser user = new ManagedUser();
            user.setPassword("KYLIN");
            user.setUsername("u3");
        }
        mockMvc.perform(MockMvcRequestBuilders.post("/api/user/batch").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(users))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nUserController).batchCreate(users);
    }

    @Test
    public void testBatchDelUsersNotFound() throws Exception {
        List<String> users = Arrays.asList("u1", "u2", "u3");
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/user/batch").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(users))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)));
        Mockito.verify(nUserController).batchDelete(users);
    }

    @Test
    public void testBatchDelUsers() throws Exception {
        List<String> users = Arrays.asList("u1", "u2", "u3");
        List<ManagedUser> managedUsers = new ArrayList<>();
        {
            ManagedUser user = new ManagedUser();
            user.setUsername("u1");
            managedUsers.add(user);
        }
        {
            ManagedUser user = new ManagedUser();
            user.setUsername("u2");
            managedUsers.add(user);
        }
        {
            ManagedUser user = new ManagedUser();
            user.setUsername("u3");
            managedUsers.add(user);
        }
        Mockito.doReturn(managedUsers).when(userService).listUsers();
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/user/batch").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(users))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)));
        Mockito.verify(nUserController).batchDelete(users);
    }

    @Test
    public void testCreateUserWithNullDisabled() throws IOException {
        val user = new UserRequest();
        user.setUsername("u1");
        user.setPassword("p1234sgw$");
        userGroupService.addGroup(GROUP_ALL_USERS);
        user.setAuthorities(Lists.newArrayList(GROUP_ALL_USERS));
        Mockito.doReturn(false).when(userGroupService).exists(GROUP_ALL_USERS);
        Mockito.doNothing().when(userService).createUser(Mockito.any(UserDetails.class));
        Mockito.doReturn(Lists.newArrayList(GROUP_ALL_USERS)).when(userGroupService).getAllUserGroups();
        Assert.assertThrows(REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY.getMsg("disabled"), KylinException.class,
                () -> nUserController.createUser(user));
    }

    @Test
    public void testListUnassignedUser() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/user/unassigned_users").contentType(MediaType.APPLICATION_JSON)
                .param("name", "KYLIN").param("group_name", "AAA")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nUserController).listGroupUnassignedUsers(Mockito.anyString(), Mockito.anyString(),
                Mockito.anyBoolean(), Mockito.anyInt(), Mockito.anyInt());
    }
}

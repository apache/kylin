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

package org.apache.kylin.rest.controller.open;

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.junit.rule.ClearKEPropertiesRule;
import org.apache.kylin.metadata.user.ManagedUser;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.controller.NUserController;
import org.apache.kylin.rest.request.CachedUserUpdateRequest;
import org.apache.kylin.rest.request.PasswordChangeRequest;
import org.apache.kylin.rest.request.UserRequest;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.UserService;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.core.env.Environment;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.accept.ContentNegotiationManager;

import lombok.val;

public class OpenUserControllerTest extends NLocalFileMetadataTestCase {
    private MockMvc mockMvc;
    private static final BCryptPasswordEncoder pwdEncoder = new BCryptPasswordEncoder();

    @Rule
    public ClearKEPropertiesRule clearKEProperties = new ClearKEPropertiesRule();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @InjectMocks
    private OpenUserController openUserController;

    @Mock
    private NUserController userController;

    @Mock
    private UserService userService;

    @Mock
    private AccessService accessService;

    @Mock
    Environment env;

    @Before
    public void setupResource() {
        createTestMetadata();
        getTestConfig().setProperty("kylin.env", "UT");
        openUserController = Mockito.spy(new OpenUserController());
    }

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        Mockito.doReturn(true).when(env).acceptsProfiles("testing", "custom");
        ContentNegotiationManager contentNegotiationManager = new ContentNegotiationManager();
        mockMvc = MockMvcBuilders.standaloneSetup(openUserController)
                .setContentNegotiationManager(contentNegotiationManager).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();
        List<GrantedAuthority> authorities = new ArrayList<GrantedAuthority>();
        ManagedUser user = new ManagedUser("ADMIN", "KYLIN", false, authorities);
        Authentication authentication = new TestingAuthenticationToken(user, "ADMIN", Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(authentication);
        ReflectionTestUtils.setField(openUserController, "userController", userController);
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testListAll() throws Exception {
        Mockito.doReturn(
                new EnvelopeResponse<>(KylinException.CODE_SUCCESS, DataResult.get(Lists.newArrayList(), 0, 10), ""))
                .when(userController).listAllUsers("ADMIN", false, 0, 10);

        mockMvc.perform(MockMvcRequestBuilders.get("/api/user").contentType(MediaType.APPLICATION_JSON)
                .param("name", "ADMIN").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(openUserController).listAllUsers("ADMIN", false, 0, 10);
    }

    @Test
    public void testCreateUser() throws Exception {
        val user = new UserRequest();
        user.setUsername("azAZ_#");
        user.setPassword("p14532522?");
        user.setDisabled(false);
        Mockito.doNothing().when(userService).createUser(Mockito.any(UserDetails.class));
        mockMvc.perform(MockMvcRequestBuilders.post("/api/user").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(user))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(openUserController).createUser(Mockito.any(UserRequest.class));
    }

    @Test
    public void testUpdateUser() throws Exception {
        val user = new ManagedUser("ADMIN", pwdEncoder.encode("KYLIN"), false);
        val userRequest = new UserRequest();
        userRequest.setUsername("ADMIN");
        userRequest.setDisabled(false);
        Mockito.doReturn(user).when(userController).getManagedUser("ADMIN");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/user").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(userRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(openUserController).updateUser(Mockito.any(UserRequest.class));
    }

    @Test
    public void testDelUser() throws Exception {
        Mockito.doNothing().when(userService).deleteUser(Mockito.anyString());
        Mockito.doNothing().when(accessService).revokeProjectPermission(Mockito.anyString(), Mockito.anyString());
        mockMvc.perform(
                MockMvcRequestBuilders.delete("/api/user/{username:.+}", "u1").contentType(MediaType.APPLICATION_JSON)
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(openUserController).delete("u1");
    }

    @Test
    public void testDelUserWithBody() throws Exception {
        Mockito.doNothing().when(userService).deleteUser(Mockito.anyString());
        Mockito.doNothing().when(accessService).revokeProjectPermission(Mockito.anyString(), Mockito.anyString());
        ManagedUser request = new ManagedUser();
        request.setUsername("u1");
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/user", "u1").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(openUserController).deleteUser(request);
    }

    @Test
    public void testUpdatePassword_Success() throws Exception {
        val user = new ManagedUser("ADMIN", pwdEncoder.encode("KYLIN"), false);
        val request = new PasswordChangeRequest();
        request.setUsername("ADMIN");
        request.setPassword("KYLIN");
        request.setNewPassword("KYLIN1234@");

        Mockito.doReturn(user).when(userController).getManagedUser("ADMIN");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/user/password").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(openUserController).updateUserPassword(Mockito.any(PasswordChangeRequest.class));
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
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openUserController).batchCreate(users);
    }

    @Test
    public void testBatchDelUsers() throws Exception {
        List<String> users = Arrays.asList("u1", "u2", "u3");
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/user/batch").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(users))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openUserController).batchDelete(users);
    }

    @Test
    public void testRefreshUsers() throws Exception {
        CachedUserUpdateRequest request = new CachedUserUpdateRequest();
        request.setOperationType("USER_DELETE");
        request.setUsernameList(Arrays.asList("oliver"));
        mockMvc.perform(MockMvcRequestBuilders.put("/api/user/refresh").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openUserController).refreshUser(Mockito.any(CachedUserUpdateRequest.class));
    }
}

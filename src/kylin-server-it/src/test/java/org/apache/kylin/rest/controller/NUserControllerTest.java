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
import static org.apache.kylin.rest.constant.Constant.GROUP_ALL_USERS;
import static org.apache.kylin.rest.constant.Constant.ROLE_ADMIN;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;

import org.apache.commons.codec.binary.Base64;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.user.ManagedUser;
import org.apache.kylin.metadata.user.NKylinUserManager;
import org.apache.kylin.metadata.usergroup.NUserGroupManager;
import org.apache.kylin.rest.request.PasswordChangeRequest;
import org.apache.kylin.rest.request.UserRequest;
import org.apache.kylin.server.AbstractMVCIntegrationTestCase;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.http.MediaType;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.session.SessionRegistry;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

public class NUserControllerTest extends AbstractMVCIntegrationTestCase {

    static BCryptPasswordEncoder pwdEncoder = new BCryptPasswordEncoder();

    @SpyBean
    SessionRegistry sessionRegistry;

    UserRequest request;
    String username = "test_user";
    String password = "1234567890Q!";

    @Override
    public void setUp() throws Exception {
        super.setUp();

        try {
            NUserGroupManager userGroupManager = NUserGroupManager.getInstance(getTestConfig());
            userGroupManager.add(GROUP_ALL_USERS);
            userGroupManager.add(ROLE_ADMIN);
            request = new UserRequest();
            request.setUsername(username);
            request.setPassword(Base64.encodeBase64String(password.getBytes(StandardCharsets.UTF_8)));
            request.setDisabled(false);
            request.setAuthorities(Collections.singletonList(GROUP_ALL_USERS));
            mockMvc.perform(MockMvcRequestBuilders.post("/api/user").contentType(MediaType.APPLICATION_JSON)
                    .content(JsonUtil.writeValueAsString(request))
                    .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                    .andExpect(MockMvcResultMatchers.status().isOk());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSaveUser() throws Exception {
        request.setUsername(username.toUpperCase(Locale.ROOT));
        MvcResult result = mockMvc
                .perform(MockMvcRequestBuilders.post("/api/user").contentType(MediaType.APPLICATION_JSON)
                        .content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError())
                .andExpect(jsonPath("$.code").value("999")).andReturn();
        Assert.assertTrue(result.getResponse().getContentAsString().contains("Username:[test_user] already exists"));
    }

    @Test
    public void testCreateUserWithBase64EncodePwd() {
        ManagedUser user = NKylinUserManager.getInstance(KylinConfig.getInstanceFromEnv()).get(username);

        Assert.assertNotEquals(null, user);
        Assert.assertTrue(pwdEncoder.matches(password, user.getPassword()));
    }

    @Test
    public void testUpdateUserPasswordWithBase64EncodePwd() throws Exception {
        Mockito.doReturn(Lists.newArrayList()).when(sessionRegistry).getAllSessions(any(), anyBoolean());

        String newPassword = "kylin@2020";

        PasswordChangeRequest passwordChangeRequest = new PasswordChangeRequest();
        passwordChangeRequest.setUsername(username);
        passwordChangeRequest.setPassword(Base64.encodeBase64String(password.getBytes("utf-8")));
        passwordChangeRequest.setNewPassword(Base64.encodeBase64String(newPassword.getBytes("utf-8")));
        mockMvc.perform(MockMvcRequestBuilders.put("/api/user/password").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(passwordChangeRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        ManagedUser user = NKylinUserManager.getInstance(KylinConfig.getInstanceFromEnv()).get(username);

        Assert.assertNotEquals(null, user);
        Assert.assertTrue(pwdEncoder.matches(newPassword, user.getPassword()));
    }

    @Test
    public void testUpdateUserWithBase64EncodePwd() throws Exception {
        String newPassword = "kylin@2022";
        UserRequest userRequest = new UserRequest();
        userRequest.setUsername(username);
        userRequest.setPassword(Base64.encodeBase64String(newPassword.getBytes("utf-8")));
        userRequest.setDisabled(false);
        userRequest.setAuthorities(Arrays.asList(GROUP_ALL_USERS, ROLE_ADMIN));
        mockMvc.perform(MockMvcRequestBuilders.put("/api/user").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(userRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        ManagedUser user = NKylinUserManager.getInstance(KylinConfig.getInstanceFromEnv()).get(username);

        Assert.assertNotEquals(null, user);
        Assert.assertTrue(pwdEncoder.matches(newPassword, user.getPassword()));
        Assert.assertEquals(2, user.getAuthorities().size());
        Assert.assertTrue(user.getAuthorities().contains(new SimpleGrantedAuthority(GROUP_ALL_USERS)));
        Assert.assertTrue(user.getAuthorities().contains(new SimpleGrantedAuthority(ROLE_ADMIN)));
    }

    @Test
    public void testSaveUserReturnValueHasNotSensitiveElements() throws Exception {
        UserRequest request = new UserRequest();
        request.setUsername("test_user_2");
        request.setPassword("1234567890Q!");
        request.setDisabled(false);
        request.setAuthorities(Collections.singletonList(GROUP_ALL_USERS));

        mockMvc.perform(MockMvcRequestBuilders.post("/api/user").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andExpect(jsonPath("$.password").doesNotExist())
                .andExpect(jsonPath("$.default_password").doesNotExist());

    }
}

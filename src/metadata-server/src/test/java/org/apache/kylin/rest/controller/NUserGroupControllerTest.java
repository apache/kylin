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

import static org.apache.kylin.rest.constant.Constant.ROLE_ADMIN;
import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;

import java.util.Arrays;
import java.util.List;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.service.UserService;
import org.apache.kylin.metadata.user.ManagedUser;
import org.apache.kylin.metadata.usergroup.UserGroup;
import org.apache.kylin.rest.request.UpdateGroupRequest;
import org.apache.kylin.rest.request.UserGroupRequest;
import org.apache.kylin.rest.response.UserGroupResponseKI;
import org.apache.kylin.rest.service.AclTCRService;
import org.apache.kylin.rest.service.NUserGroupService;
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
import org.springframework.http.MediaType;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.accept.ContentNegotiationManager;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.FilterProvider;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import com.google.common.collect.Lists;

import lombok.val;

public class NUserGroupControllerTest {

    private MockMvc mockMvc;

    @Mock
    private NUserGroupService userGroupService;

    @Mock
    private AclTCRService aclTCRService;

    @Mock
    private UserService userService;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @InjectMocks
    private NUserGroupController nUserGroupController = Mockito.spy(new NUserGroupController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", ROLE_ADMIN);

    @Before
    public void setup() {
        FilterProvider filterProvider = new SimpleFilterProvider().addFilter("passwordFilter",
                SimpleBeanPropertyFilter.serializeAllExcept("password", "defaultPassword"));
        ObjectMapper objectMapper = new ObjectMapper().setFilterProvider(filterProvider);
        MappingJackson2HttpMessageConverter converter = new MappingJackson2HttpMessageConverter();
        converter.setObjectMapper(objectMapper);
        MockitoAnnotations.initMocks(this);
        ContentNegotiationManager contentNegotiationManager = new ContentNegotiationManager();
        mockMvc = MockMvcBuilders.standaloneSetup(nUserGroupController).setMessageConverters(converter)
                .setContentNegotiationManager(contentNegotiationManager).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();
        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testGetUsersByGroup() throws Exception {
        Mockito.doReturn(mockManagedUser()).when(userGroupService).getGroupMembersByName(Mockito.anyString());
        Mockito.doNothing().when(aclTCRService).revokeAclTCR(Mockito.anyString(), Mockito.anyBoolean());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/user_group/group_members/{group_name:.+}", "g1@.h")
                .contentType(MediaType.APPLICATION_JSON).param("name", "").param("page_offset", "0")
                .param("page_size", "10").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nUserGroupController).getUsersByGroupName("g1@.h", "", 0, 10);
    }

    @Test
    public void testGetGroups() throws Exception {
        Mockito.doReturn(null).when(userGroupService).listAllAuthorities();
        mockMvc.perform(MockMvcRequestBuilders.get("/api/user_group/groups").contentType(MediaType.APPLICATION_JSON)
                .param("project", "").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nUserGroupController).listUserAuthorities();
    }

    @Test
    public void testGetUserWithGroup() throws Exception {
        Integer allDataSize = 20;
        Integer pageSize = 10;
        Mockito.doReturn(Lists.newArrayList(new UserGroup[allDataSize])).when(userGroupService)
                .getUserGroupsFilterByGroupName(Mockito.anyString());
        Mockito.doReturn(Lists.newArrayList(new UserGroupResponseKI[pageSize])).when(userGroupService)
                .getUserGroupResponse(Mockito.any());
        MvcResult mvcResult = mockMvc.perform(MockMvcRequestBuilders.get("/api/user_group/users_with_group")
                .contentType(MediaType.APPLICATION_JSON).param("page_offset", "0")
                .param("page_size", pageSize.toString()).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Assert.assertTrue(mvcResult.getResponse().getContentAsString().contains(allDataSize.toString()));
    }

    @Test
    public void testAddGroup() throws Exception {
        UserGroupRequest request = new UserGroupRequest();
        request.setGroupName("g1");
        Mockito.doNothing().when(userGroupService).addGroup("g1");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/user_group").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nUserGroupController).addUserGroup(request);
    }

    @Test
    public void testAddEmptyGroup() throws Exception {
        thrown.expect(KylinException.class);
        thrown.expectMessage("User group name should not be empty.");
        UserGroupRequest request = new UserGroupRequest();
        request.setGroupName("");
        nUserGroupController.addUserGroup(request);
    }

    @Test(expected = KylinException.class)
    public void testAddIllegalGroupName() throws Exception {
        UserGroupRequest request = new UserGroupRequest();
        request.setGroupName(".hhhh");
        nUserGroupController.addUserGroup(request);
    }

    @Test
    public void testDelGroup() throws Exception {
        Mockito.doNothing().when(userGroupService).deleteGroup("g1@.h");
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/user_group/{groupName:.+}", "g1@.h")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)));
        Mockito.verify(nUserGroupController).delUserGroup("g1@.h");
    }

    @Test
    public void testAddOrDelUser() throws Exception {
        val request = new UpdateGroupRequest();
        request.setGroup("g1");
        request.setUsers(Lists.newArrayList("u1", "u2"));
        Mockito.doNothing().when(userGroupService).modifyGroupUsers("g1", request.getUsers());
        mockMvc.perform(MockMvcRequestBuilders.put("/api/user_group/users").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nUserGroupController).addOrDelUsers(Mockito.any(UpdateGroupRequest.class));
    }

    @Test
    public void testBatchAddGroups() throws Exception {
        List<String> groupList = Arrays.asList("g1", "g2", "g3");
        Mockito.doNothing().when(userGroupService).addGroups(groupList);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/user_group/batch").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(groupList))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nUserGroupController).batchAddUserGroups(groupList);
    }

    @Test
    public void testBatchDelGroups() throws Exception {
        List<String> groupList = Arrays.asList("g1", "g2");
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/user_group/batch").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(groupList))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)));
        Mockito.verify(nUserGroupController).batchDelUserGroup(groupList);
    }

    private List<ManagedUser> mockManagedUser() {
        val user1 = new ManagedUser();
        user1.setUsername("user1");
        val user2 = new ManagedUser();
        user1.setUsername("user2");
        return Lists.newArrayList(user1, user2);
    }
}

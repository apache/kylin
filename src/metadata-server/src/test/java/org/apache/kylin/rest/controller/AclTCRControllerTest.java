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
import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.service.UserService;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.rest.request.AclTCRRequest;
import org.apache.kylin.rest.response.AclTCRResponse;
import org.apache.kylin.rest.service.AclTCRService;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import org.apache.kylin.guava30.shaded.common.collect.Lists;

import lombok.val;

public class AclTCRControllerTest extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;

    @Mock
    private UserService userService;

    @Mock
    private IUserGroupService userGroupService;

    @Mock
    private AclTCRService aclTCRService;

    @Mock
    private AccessService accessService;

    @InjectMocks
    private AclTCRController aclTCRController = Mockito.spy(new AclTCRController());

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private static final String APPLICATION_JSON = HTTP_VND_APACHE_KYLIN_JSON;

    private static final String APPLICATION_JSON_PUBLIC = HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

    @Before
    public void setup() throws IOException {
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(aclTCRController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();

        final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(authentication);
        createTestMetadata();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testGetProjectSidTCR() throws Exception {

        Mockito.doReturn(true).when(userService).userExists(Mockito.anyString());
        Mockito.doReturn(true).when(userGroupService).exists(Mockito.anyString());

        AclTCRResponse response = new AclTCRResponse();
        response.setTables(Lists.newArrayList(new AclTCRResponse.Table()));
        response.getTables().get(0).setRowFilter(new AclTCRResponse.RowFilter());
        response.getTables().get(0).setRows(Lists.newArrayList(new AclTCRResponse.Row()));
        response.getTables().get(0).setLikeRows(Lists.newArrayList(new AclTCRResponse.Row()));

        Mockito.doReturn(Lists.newArrayList(response)).when(aclTCRService).getAclTCRResponse(Mockito.anyString(),
                Mockito.anyString(), Mockito.anyBoolean(), Mockito.anyBoolean());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/acl/sid/{sidType}/{sid}", "user", "u1") //
                .param("project", "default") //
                .param("authorizedOnly", "false") //
                .contentType(MediaType.APPLICATION_JSON) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(aclTCRController).getProjectSidTCR("user", "u1", "default", false);

        mockMvc.perform(MockMvcRequestBuilders.get("/api/acl/sid/{sidType}/{sid}", "group", "g1") //
                .param("project", "default") //
                .param("authorizedOnly", "false") //
                .contentType(MediaType.APPLICATION_JSON) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(aclTCRController).getProjectSidTCR("group", "g1", "default", false);

        mockMvc.perform(MockMvcRequestBuilders.get("/api/acl/{sidType}/{sid}", "user", "u1") //
                .param("project", "default") //
                .param("authorizedOnly", "false") //
                .contentType(MediaType.APPLICATION_JSON) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON_PUBLIC))) //
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(aclTCRController).getProjectSidTCRV2("user", "u1", "default", false);

        mockMvc.perform(MockMvcRequestBuilders.get("/api/acl/{sidType}/{sid}", "group", "g1") //
                .param("project", "default") //
                .param("authorizedOnly", "false") //
                .contentType(MediaType.APPLICATION_JSON) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON_PUBLIC))) //
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(aclTCRController).getProjectSidTCRV2("group", "g1", "default", false);
    }

    @Test
    public void testUpdateProject() throws Exception {

        Mockito.doReturn(true).when(userService).userExists(Mockito.anyString());
        Mockito.doReturn(true).when(userGroupService).exists(Mockito.anyString());
        Mockito.doReturn(true).when(accessService).hasProjectPermission(Mockito.anyString(), Mockito.anyString(),
                Mockito.anyBoolean());

        Mockito.doNothing().when(aclTCRService).updateAclTCR(Mockito.anyString(), Mockito.anyString(),
                Mockito.anyBoolean(), Mockito.anyList());

        AclTCRRequest request = new AclTCRRequest();
        request.setDatabaseName("DEFAULT");
        AclTCRRequest.Table u1t1 = new AclTCRRequest.Table();
        u1t1.setTableName("TEST_ORDER");
        u1t1.setAuthorized(true);
        val rf1 = new AclTCRRequest.RowFilter();
        val filterGroups = new ArrayList<AclTCRRequest.FilterGroup>();
        val fg1 = new AclTCRRequest.FilterGroup();
        fg1.setGroup(false);
        val filters = new ArrayList<AclTCRRequest.Filter>();
        val filter = new AclTCRRequest.Filter();
        filter.setColumnName("TEST_EXTENDED_COLUMN");
        filter.setInItems(Lists.newArrayList("a", "b"));
        filter.setLikeItems(Lists.newArrayList("1", "2"));
        filters.add(filter);
        fg1.setFilters(filters);
        filterGroups.add(fg1);
        rf1.setFilterGroups(filterGroups);
        u1t1.setRowFilter(rf1);
        request.setTables(Lists.newArrayList(u1t1));

        mockMvc.perform(MockMvcRequestBuilders.put("/api/acl/sid/{sidType}/{sid}", "user", "u1") //
                .param("project", "default") //
                .content(JsonUtil.writeValueAsBytes(Lists.<AclTCRRequest> newArrayList(request))) //
                .contentType(MediaType.APPLICATION_JSON) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        request.getTables().get(0).setRowFilter(null);
        Mockito.verify(aclTCRController).updateProject("user", "u1", "default", Lists.newArrayList(request));

        mockMvc.perform(MockMvcRequestBuilders.put("/api/acl/sid/{sidType}/{sid}", "group", "g1") //
                .param("project", "default") //
                .content(JsonUtil.writeValueAsBytes(Lists.<AclTCRRequest> newArrayList())) //
                .contentType(MediaType.APPLICATION_JSON) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(aclTCRController).updateProject("group", "g1", "default", Lists.newArrayList());
    }

    @Test
    public void testGetAllowModifyAcl() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/acl/updatable") //
                .param("project", "default") //
                .content(JsonUtil.writeValueAsBytes(Lists.<AclTCRRequest> newArrayList())) //
                .contentType(MediaType.APPLICATION_JSON) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(aclTCRController).getAllowAclUpdatable("default");

    }
}

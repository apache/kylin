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
import static org.hamcrest.CoreMatchers.containsString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.apache.kylin.common.persistence.AclEntity;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.AccessRequest;
import org.apache.kylin.rest.request.BatchAccessRequest;
import org.apache.kylin.rest.request.GlobalAccessRequest;
import org.apache.kylin.rest.request.GlobalBatchAccessRequest;
import org.apache.kylin.rest.security.AclEntityType;
import org.apache.kylin.rest.security.AclPermission;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.AclTCRService;
import org.apache.kylin.rest.service.ProjectService;
import org.apache.kylin.rest.service.UserAclService;
import org.apache.kylin.rest.service.UserService;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
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
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

public class NAccessControllerTest extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;

    @Mock
    private AccessService accessService;

    @Mock
    private UserAclService userAclService;

    @Mock
    private UserService userService;

    @Mock
    private AclTCRService aclTCRService;

    @Mock
    private ProjectService projectService;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @InjectMocks
    private NAccessController nAccessController = Mockito.spy(new NAccessController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    private String type = "ProjectInstance";

    private String uuid = "u126snk32242152";

    private String sid = "user_g1";

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(nAccessController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testGetUserPermissionInPrj() throws Exception {
        List<JobStatusEnum> status = new ArrayList<>();
        status.add(JobStatusEnum.NEW);
        ArrayList<AbstractExecutable> jobs = new ArrayList<>();
        Integer[] statusInt = { 4 };
        String[] subjects = {};
        Mockito.when(accessService.getCurrentNormalUserPermissionInProject("default")).thenReturn("ADMIN");
        MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.get("/api/access/permission/project_permission")
                        .contentType(MediaType.APPLICATION_JSON).param("project", "default")
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nAccessController).getUserPermissionInPrj("default");
    }

    @Test
    public void testGrantPermissionForValidUser() throws Exception {
        AccessRequest accessRequest = new AccessRequest();
        accessRequest.setSid(sid);
        accessRequest.setPrincipal(true);
        AclEntity ae = accessService.getAclEntity(type, uuid);
        Mockito.doReturn(true).when(userService).userExists(sid);
        Mockito.doNothing().when(aclTCRService).updateAclTCR(uuid, null);
        Mockito.doNothing().when(accessService).grant(ae, "1", true, "ADMIN");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/access/{type}/{uuid}", type, uuid)
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(accessRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nAccessController).grant(type, uuid, accessRequest);
    }

    @Test
    public void testGrantPermissionForValidUserWithProject() throws Exception {
        AccessRequest accessRequest = new AccessRequest();
        accessRequest.setSid(sid);
        accessRequest.setPrincipal(true);
        accessRequest.setProject(uuid);
        AclEntity ae = accessService.getAclEntity(type, uuid);
        Mockito.doReturn(true).when(userService).userExists(sid);
        Mockito.doNothing().when(aclTCRService).updateAclTCR(uuid, null);
        Mockito.doNothing().when(accessService).grant(uuid, ae, "1", true, "ADMIN");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/access/{type}/{uuid}", type, uuid)
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(accessRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nAccessController).grant(type, uuid, accessRequest);
    }

    @Test
    @Ignore
    public void testGrantPermissionForInvalidUser() throws Exception {
        String sid = "1/";
        String expectedErrorMsg = "User/Group name should only contain alphanumerics and underscores.";
        testGrantPermissionForUser(sid, expectedErrorMsg);
    }

    @Test
    public void testUpdateAcl() throws Exception {
        AccessRequest accessRequest = new AccessRequest();
        accessRequest.setSid(sid);
        accessRequest.setPrincipal(true);
        accessRequest.setPermission("OPERATION");
        accessRequest.setAccessEntryId(0);

        Mockito.doReturn(true).when(userService).userExists(sid);
        Mockito.doNothing().when(aclTCRService).updateAclTCR(uuid, null);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/access/{type}/{uuid}", type, uuid)
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(accessRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nAccessController).updateAcl(type, uuid, accessRequest);
    }

    @Test
    public void testUpdateAclWithProject() throws Exception {
        AccessRequest accessRequest = new AccessRequest();
        accessRequest.setSid(sid);
        accessRequest.setPrincipal(true);
        accessRequest.setPermission("OPERATION");
        accessRequest.setAccessEntryId(0);
        accessRequest.setProject(uuid);

        Mockito.doReturn(true).when(userService).userExists(sid);
        Mockito.doNothing().when(aclTCRService).updateAclTCR(uuid, null);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/access/{type}/{uuid}", type, uuid)
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(accessRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nAccessController).updateAcl(type, uuid, accessRequest);
    }

    @Test
    public void testRevokeAcl() throws Exception {
        Mockito.doReturn(true).when(userService).userExists(sid);
        Mockito.doNothing().when(aclTCRService).revokeAclTCR(uuid, true);
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/access/{type}/{uuid}", type, uuid)
                .contentType(MediaType.APPLICATION_JSON).param("access_entry_id", "1").param("sid", sid)
                .param("principal", "true").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nAccessController).revokeAcl(type, uuid, 1, sid, true, null);
    }

    @Test
    public void testRevokeAclWithProject() throws Exception {
        Mockito.doReturn(true).when(userService).userExists(sid);
        Mockito.doNothing().when(aclTCRService).revokeAclTCR(uuid, true);
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/access/{type}/{uuid}", type, uuid)
                .contentType(MediaType.APPLICATION_JSON).param("project", uuid).param("access_entry_id", "1")
                .param("sid", sid).param("principal", "true")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nAccessController).revokeAcl(type, uuid, 1, sid, true, uuid);
    }

    @Test
    public void testRevokeAclWithNotExistSid() throws Exception {
        Mockito.doNothing().when(aclTCRService).revokeAclTCR(uuid, false);
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/access/{type}/{uuid}", type, uuid)
                .contentType(MediaType.APPLICATION_JSON).param("access_entry_id", "1").param("sid", "NotExist")
                .param("principal", "false").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nAccessController).revokeAcl(type, uuid, 1, "NotExist", false, null);
    }

    @Test
    public void testRevokeAclWithNotExistSidWithProject() throws Exception {
        Mockito.doNothing().when(aclTCRService).revokeAclTCR(uuid, false);
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/access/{type}/{uuid}", type, uuid)
                .contentType(MediaType.APPLICATION_JSON).param("project", uuid).param("access_entry_id", "1")
                .param("sid", "NotExist").param("principal", "false")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nAccessController).revokeAcl(type, uuid, 1, "NotExist", false, uuid);
    }

    @Test
    public void testBatchRevokeAcl() throws Exception {
        AccessRequest accessRequest = new AccessRequest();
        accessRequest.setSid(sid);
        List<AccessRequest> requests = Lists.newArrayList(accessRequest);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/access/{type}/{uuid}/deletion", type, uuid)
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(requests))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nAccessController).deleteAces(type, uuid, requests);
    }

    @Test
    public void testGetAvailableUsersForProject() throws Exception {
        List<ProjectInstance> list = Lists.newArrayList();
        list.add(Mockito.mock(ProjectInstance.class));
        Mockito.doReturn(list).when(projectService).getReadableProjects("default", true);
        Mockito.doReturn(new HashSet<>()).when(accessService).getProjectAdminUsers("default");
        mockMvc.perform(MockMvcRequestBuilders.get("/api/access/available/{entity_type:.+}", type)
                .contentType(MediaType.APPLICATION_JSON).param("project", "default").param("model", uuid)
                .param("name", "").param("is_case_sensitive", "false").param("page_offset", "0")
                .param("page_size", "10").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAccessController).getAvailableUsers(type, "default", uuid, "", false, 0, 10);
    }

    @Test
    public void testGetGlobalUserAccessEntities() throws Exception {
        Mockito.doReturn(true).when(userAclService).hasUserAclPermission(sid, AclPermission.DATA_QUERY);
        mockMvc.perform(
                MockMvcRequestBuilders.get("/api/access/global/permission/{permissionType:.+}/{sid:.+}", type, sid)
                        .contentType(MediaType.APPLICATION_JSON).param("permissionType", type).param("sid", sid)
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAccessController).getGlobalUserAccessEntities(type, sid);
    }

    @Test
    public void testGetAllGlobalUsersAccessEntities() throws Exception {
        Mockito.doReturn(Collections.emptyList()).when(userAclService).listUserAcl();
        mockMvc.perform(MockMvcRequestBuilders.get("/api/access/global/permission/user_acls", type, sid)
                .contentType(MediaType.APPLICATION_JSON).param("permissionType", type).param("sid", sid)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAccessController).getAllGlobalUsersAccessEntities();
    }

    @Test
    public void testAddGlobalUserAccessEntities() throws Exception {
        GlobalAccessRequest accessRequest = new GlobalAccessRequest();
        accessRequest.setEnabled(true);
        accessRequest.setQueryPermission(true);
        accessRequest.setUsername(sid);
        {
            Mockito.doNothing().when(userAclService).grantUserAclPermission(accessRequest, "DATA_QUERY");
            mockMvc.perform(MockMvcRequestBuilders.put("/api/access/global/permission/{permissionType:.+}", type)
                    .contentType(MediaType.APPLICATION_JSON).param("permissionType", type)
                    .content(JsonUtil.writeValueAsString(accessRequest))
                    .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                    .andExpect(MockMvcResultMatchers.status().isOk());
            Mockito.verify(nAccessController).addGlobalUserAccessEntities(type, accessRequest);
        }
        {
            accessRequest.setEnabled(false);
            Mockito.doNothing().when(userAclService).revokeUserAclPermission(accessRequest, "DATA_QUERY");
            mockMvc.perform(MockMvcRequestBuilders.put("/api/access/global/permission/{permissionType:.+}", type)
                    .contentType(MediaType.APPLICATION_JSON).param("permissionType", type)
                    .content(JsonUtil.writeValueAsString(accessRequest))
                    .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                    .andExpect(MockMvcResultMatchers.status().isOk());
            Mockito.verify(nAccessController).addGlobalUserAccessEntities(type, accessRequest);
        }
    }

    @Test
    public void testAddProjectToUserAcl() throws Exception {
        GlobalAccessRequest accessRequest = new GlobalAccessRequest();
        accessRequest.setUsername(sid);
        accessRequest.setProject("default");
        Mockito.doNothing().when(userAclService).addProjectToUserAcl(accessRequest, "data_query");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/access/global/permission/project/{permissionType:.+}", type)
                .contentType(MediaType.APPLICATION_JSON).param("permissionType", type)
                .content(JsonUtil.writeValueAsString(accessRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAccessController).addProjectToUserAcl(type, accessRequest);
    }

    @Test
    public void testDeleteProjectToUserAcl() throws Exception {
        GlobalAccessRequest accessRequest = new GlobalAccessRequest();
        accessRequest.setUsername(sid);
        accessRequest.setProject("default");
        Mockito.doNothing().when(userAclService).deleteProjectFromUserAcl(accessRequest, "data_query");
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/access/global/permission/project/{permissionType:.+}", type)
                .contentType(MediaType.APPLICATION_JSON).param("permissionType", type)
                .content(JsonUtil.writeValueAsString(accessRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAccessController).deleteProjectFromUserAcl(type, accessRequest);
    }

    @Test
    public void testBatchAddGlobalUserAccessEntities() throws Exception {
        GlobalBatchAccessRequest batchAccessRequest = new GlobalBatchAccessRequest();
        batchAccessRequest.setEnabled(true);
        batchAccessRequest.setQueryPermission(true);
        batchAccessRequest.setUsernameList(Arrays.asList(sid, "user_g2"));
        {
            Mockito.doNothing().when(userAclService).grantUserAclPermission(batchAccessRequest, "DATA_QUERY");
            mockMvc.perform(MockMvcRequestBuilders.put("/api/access/global/batch/permission/{permissionType:.+}", type)
                    .contentType(MediaType.APPLICATION_JSON).param("permissionType", type)
                    .content(JsonUtil.writeValueAsString(batchAccessRequest))
                    .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                    .andExpect(MockMvcResultMatchers.status().isOk());
            Mockito.verify(nAccessController).batchAddGlobalUserAccessEntities(type, batchAccessRequest);
        }
        {
            batchAccessRequest.setEnabled(false);
            Mockito.doNothing().when(userAclService).revokeUserAclPermission(batchAccessRequest, "DATA_QUERY");
            mockMvc.perform(MockMvcRequestBuilders.put("/api/access/global/batch/permission/{permissionType:.+}", type)
                    .contentType(MediaType.APPLICATION_JSON).param("permissionType", type)
                    .content(JsonUtil.writeValueAsString(batchAccessRequest))
                    .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                    .andExpect(MockMvcResultMatchers.status().isOk());
            Mockito.verify(nAccessController).batchAddGlobalUserAccessEntities(type, batchAccessRequest);
        }
    }

    @Test
    public void testGetProjectUsersAndGroups() throws Exception {
        mockMvc.perform(
                MockMvcRequestBuilders.get("/api/access/{uuid:.+}/all", uuid).contentType(MediaType.APPLICATION_JSON)
                        .param("project", "default").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nAccessController).getProjectUsersAndGroups(uuid);
    }

    @Test
    public void testGetAvailableUsersForModel() throws Exception {
        String type = AclEntityType.N_DATA_MODEL;

        NDataModelManager nDataModelManager = Mockito.mock(NDataModelManager.class);
        NDataModel dataModel = Mockito.mock(NDataModel.class);
        Mockito.doReturn(dataModel).when(nDataModelManager).getDataModelDesc(uuid);
        Mockito.doReturn(nDataModelManager).when(projectService).getManager(NDataModelManager.class, "default");
        Mockito.doReturn(new HashSet<>()).when(accessService).getProjectManagementUsers("default");

        mockMvc.perform(MockMvcRequestBuilders.get("/api/access/available/{entity_type:.+}", type)
                .contentType(MediaType.APPLICATION_JSON).param("project", "default").param("model", uuid)
                .param("name", "").param("is_case_sensitive", "false").param("page_offset", "0")
                .param("page_size", "10").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAccessController).getAvailableUsers(type, "default", uuid, "", false, 0, 10);

        mockMvc.perform(MockMvcRequestBuilders.get("/api/access/available/{entity_type:.+}", type)
                .contentType(MediaType.APPLICATION_JSON).param("project", "default")
                .param("model", RandomUtil.randomUUIDStr()).param("name", "").param("is_case_sensitive", "false")
                .param("page_offset", "0").param("page_size", "10")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());
        Mockito.verify(nAccessController).getAvailableUsers(type, "default", uuid, "", false, 0, 10);
    }

    private void testGrantPermissionForUser(String sid, String expectedMsg) throws Exception {
        AccessRequest accessRequest = new AccessRequest();
        accessRequest.setSid(sid);
        AclEntity ae = accessService.getAclEntity(type, uuid);
        Mockito.doNothing().when(accessService).grant(ae, "1", true, "ADMIN");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/access/{type}/{uuid}", type, uuid)
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(accessRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.content().string(containsString(expectedMsg)));
        Mockito.verify(nAccessController).grant(type, uuid, accessRequest);
    }

    @Test
    public void testBatchGrant() throws Exception {
        BatchAccessRequest accessRequest = new BatchAccessRequest();
        List<String> sids = Lists.newArrayList(sid);
        accessRequest.setSids(sids);
        accessRequest.setPrincipal(true);
        List<BatchAccessRequest> requests = Lists.newArrayList(accessRequest);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/access/batch/{type}/{uuid}", type, uuid)
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(requests))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nAccessController).batchGrant(type, uuid, null, true, requests);
    }

    @Test
    public void testBatchGrantWithProject() throws Exception {
        BatchAccessRequest accessRequest = new BatchAccessRequest();
        List<String> sids = Lists.newArrayList(sid);
        accessRequest.setSids(sids);
        accessRequest.setPrincipal(true);
        List<BatchAccessRequest> requests = Lists.newArrayList(accessRequest);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/access/batch/{type}/{uuid}", type, uuid)
                .contentType(MediaType.APPLICATION_JSON).param("project", uuid)
                .content(JsonUtil.writeValueAsString(requests))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nAccessController).batchGrant(type, uuid, uuid, true, requests);
    }

    @Test
    public void testBatchGrantDuplicateName() throws Exception {
        BatchAccessRequest accessRequest = new BatchAccessRequest();
        List<String> sids = Lists.newArrayList(sid, sid);
        accessRequest.setSids(sids);
        accessRequest.setPrincipal(true);
        List<BatchAccessRequest> requests = Lists.newArrayList(accessRequest);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/access/batch/{type}/{uuid}", type, uuid)
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(requests))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());
        Mockito.verify(nAccessController).batchGrant(type, uuid, null, true, requests);
    }

    @Test
    public void testBatchGrantDuplicateNameWithProject() throws Exception {
        BatchAccessRequest accessRequest = new BatchAccessRequest();
        List<String> sids = Lists.newArrayList(sid, sid);
        accessRequest.setSids(sids);
        accessRequest.setPrincipal(true);
        List<BatchAccessRequest> requests = Lists.newArrayList(accessRequest);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/access/batch/{type}/{uuid}", type, uuid)
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(requests))
                .param("project", uuid).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());
        Mockito.verify(nAccessController).batchGrant(type, uuid, uuid, true, requests);
    }

    @Test
    public void testUpdateExtensionAcl() throws Exception {
        AccessRequest accessRequest = new AccessRequest();
        accessRequest.setSid(sid);
        accessRequest.setPrincipal(true);
        accessRequest.setExtPermissions(Collections.singletonList("DATA_QUERY"));

        Mockito.doReturn(true).when(userService).userExists(sid);
        Mockito.doNothing().when(aclTCRService).updateAclTCR(uuid, null);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/access/extension/{type}/{uuid}", type, uuid)
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(accessRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nAccessController).updateExtensionAcl(type, uuid, accessRequest);
    }

    @Test
    public void testUpdateExtensionAclWithProject() throws Exception {
        AccessRequest accessRequest = new AccessRequest();
        accessRequest.setSid(sid);
        accessRequest.setPrincipal(true);
        accessRequest.setExtPermissions(Collections.singletonList("DATA_QUERY"));
        accessRequest.setProject(uuid);

        Mockito.doReturn(true).when(userService).userExists(sid);
        Mockito.doNothing().when(aclTCRService).updateAclTCR(uuid, null);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/access/extension/{type}/{uuid}", type, uuid)
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(accessRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nAccessController).updateExtensionAcl(type, uuid, accessRequest);
    }
}

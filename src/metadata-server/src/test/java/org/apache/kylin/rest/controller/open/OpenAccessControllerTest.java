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

import static org.apache.kylin.rest.security.AclEntityType.PROJECT_INSTANCE;
import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.AclEntity;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.response.AccessEntryResponse;
import org.apache.kylin.rest.response.ProjectPermissionResponse;
import org.apache.kylin.rest.security.AclEntityFactory;
import org.apache.kylin.rest.security.AclEntityType;
import org.apache.kylin.rest.security.AclPermission;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.UserService;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.rest.request.AccessRequest;
import org.apache.kylin.rest.request.BatchProjectPermissionRequest;
import org.apache.kylin.rest.request.ProjectPermissionRequest;
import org.apache.kylin.rest.service.AclTCRService;
import org.apache.kylin.rest.service.ProjectService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.security.acls.domain.BasePermission;
import org.springframework.security.acls.domain.GrantedAuthoritySid;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.Permission;
import org.springframework.security.acls.model.Sid;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import org.apache.kylin.guava30.shaded.common.collect.Lists;

public class OpenAccessControllerTest extends NLocalFileMetadataTestCase {
    private MockMvc mockMvc;

    @InjectMocks
    private OpenAccessController openAccessController = Mockito.spy(new OpenAccessController());

    @Mock
    private AccessService accessService;

    @Mock
    private UserService userService;

    @Mock
    private AclTCRService aclTCRService;

    @Mock
    private ProjectService projectService;

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(openAccessController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testGetProjectAccessPermissions() throws Exception {
        AclEntity ae = accessService.getAclEntity(PROJECT_INSTANCE, "1eaca32a-a33e-4b69-83dd-0bb8b1f8c91b");
        Mockito.doNothing().when(accessService).grant(ae, "1", true, "ADMIN");
        Sid sid = new PrincipalSid("user1");
        Permission permission = BasePermission.ADMINISTRATION;
        AccessEntryResponse accessEntryResponse = new AccessEntryResponse("1L", sid, permission, false);

        List<AccessEntryResponse> accessEntryResponses = new ArrayList<>();
        accessEntryResponses.add(accessEntryResponse);
        sid = new GrantedAuthoritySid("group1");
        accessEntryResponse = new AccessEntryResponse("1L", sid, permission, false);
        accessEntryResponses.add(accessEntryResponse);
        Mockito.when(accessService.generateAceResponsesByFuzzMatching(null, "test", false))
                .thenReturn(accessEntryResponses);

        mockMvc.perform(MockMvcRequestBuilders.get("/api/access/project").contentType(MediaType.APPLICATION_JSON)
                .param("project", "default").param("name", "test")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(openAccessController).getProjectAccessPermissions("default", "test", false, 0, 10);
    }

    @Test
    public void testGrantProjectPermission() throws Exception {
        BatchProjectPermissionRequest batchProjectPermissionRequest = new BatchProjectPermissionRequest();
        batchProjectPermissionRequest.setProject("default");
        batchProjectPermissionRequest.setType("user");
        batchProjectPermissionRequest.setPermission("QUERY");
        batchProjectPermissionRequest.setNames(Lists.newArrayList("test"));

        mockMvc.perform(MockMvcRequestBuilders.post("/api/access/project").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(batchProjectPermissionRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(openAccessController).grantProjectPermission(batchProjectPermissionRequest);
    }

    @Test
    public void testUpdateProjectPermission() throws Exception {
        ProjectPermissionRequest projectPermissionRequest = new ProjectPermissionRequest();
        projectPermissionRequest.setProject("default");
        projectPermissionRequest.setType("user");
        projectPermissionRequest.setPermission("QUERY");
        projectPermissionRequest.setName("test");

        mockMvc.perform(MockMvcRequestBuilders.put("/api/access/project").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(projectPermissionRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(openAccessController).updateProjectPermission(projectPermissionRequest);
    }

    @Test
    public void testRevokeProjectPermission() throws Exception {
        List<AccessEntryResponse> accessEntryResponses = new ArrayList<>();
        accessEntryResponses.add(new AccessEntryResponse());
        Mockito.when(accessService.generateAceResponsesByFuzzMatching(null, "test", false))
                .thenReturn(accessEntryResponses);

        AclEntity ae = accessService.getAclEntity(PROJECT_INSTANCE, "1eaca32a-a33e-4b69-83dd-0bb8b1f8c91b");
        Mockito.doNothing().when(accessService).grant(ae, "1", true, "ADMIN");

        mockMvc.perform(MockMvcRequestBuilders.delete("/api/access/project").contentType(MediaType.APPLICATION_JSON)
                .param("project", "default").param("type", "user").param("name", "test")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(openAccessController).revokeProjectPermission("default", "user", "test");
    }

    @Test
    public void testRevokeProjectPermissionWithException() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/access/project").contentType(MediaType.APPLICATION_JSON)
                .param("project", "default").param("type", "user").param("name", "test")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();

        Mockito.verify(openAccessController).revokeProjectPermission("default", "user", "test");
    }

    @Test
    public void testGetUserOrGroupAclPermissions() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/access/acls").contentType(MediaType.APPLICATION_JSON)
                .param("type", "user").param("name", "test").param("project", "default")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(openAccessController).getUserOrGroupAclPermissions("user", "test", "default");
    }

    @Test
    public void testGetUserOrGroupAclPermissionsWithProjectBlank() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/access/acls").contentType(MediaType.APPLICATION_JSON)
                .param("type", "user").param("name", "test").param("project", "")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(openAccessController).getUserOrGroupAclPermissions("user", "test", "");
    }

    @Test
    public void testGetUserOrGroupAclPermissionsWithTypeError() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/access/acls").contentType(MediaType.APPLICATION_JSON)
                .param("type", "error").param("name", "test").param("project", "")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();

        Mockito.verify(openAccessController).getUserOrGroupAclPermissions("error", "test", "");
    }

    @Test
    public void testConvertAccessRequests() {
        ProjectInstance projectInstance = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getProject("default");
        AclEntity ae = AclEntityFactory.createAclEntity(AclEntityType.PROJECT_INSTANCE, projectInstance.getUuid());
        BatchProjectPermissionRequest request = new BatchProjectPermissionRequest();
        request.setNames(Lists.newArrayList("U5", "newUser"));
        request.setPermission("ADMIN");
        request.setProject("default");
        request.setType(MetadataConstants.TYPE_USER);
        List<AccessRequest> accessRequests = openAccessController.convertBatchPermissionRequestToAccessRequests(ae,
                request);
        Assert.assertEquals("U5", accessRequests.get(0).getSid());
        Assert.assertEquals("newUser", accessRequests.get(1).getSid());

        request.setType(MetadataConstants.TYPE_GROUP);
        request.setNames(Lists.newArrayList("newGroup"));
        accessRequests = openAccessController.convertBatchPermissionRequestToAccessRequests(ae, request);
        Assert.assertEquals("newGroup", accessRequests.get(0).getSid());
    }

    @Test
    public void testConvertAceResponseToProjectPermissionResponse() throws Exception {
        {
            ProjectPermissionRequest projectPermissionRequest = new ProjectPermissionRequest();
            projectPermissionRequest.setProject("default");
            projectPermissionRequest.setType(MetadataConstants.TYPE_USER);
            projectPermissionRequest.setPermission("OPERATION");
            projectPermissionRequest.setName("test");
            openAccessController.updateProjectPermission(projectPermissionRequest);
        }
        {
            ProjectPermissionRequest projectPermissionRequest = new ProjectPermissionRequest();
            projectPermissionRequest.setProject("default");
            projectPermissionRequest.setType(MetadataConstants.TYPE_GROUP);
            projectPermissionRequest.setPermission("OPERATION");
            projectPermissionRequest.setName("ALL_USERS");
            openAccessController.updateProjectPermission(projectPermissionRequest);
        }
        List<AccessEntryResponse> aclResponseList = new ArrayList<>();
        {
            Sid sid = new PrincipalSid("test");
            Permission permission = AclPermission.OPERATION;
            AccessEntryResponse accessEntryResponse = new AccessEntryResponse("1L", sid, permission, false);
            aclResponseList.add(accessEntryResponse);
        }
        {
            Sid sid = new GrantedAuthoritySid("ALL_USERS");
            Permission permission = AclPermission.MANAGEMENT;
            AccessEntryResponse accessEntryResponse = new AccessEntryResponse("2L", sid, permission, false);
            aclResponseList.add(accessEntryResponse);
        }

        ProjectInstance projectInstance = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getProject("default");
        List<ProjectPermissionResponse> responseList = ReflectionTestUtils.invokeMethod(openAccessController,
                "convertAceResponseToProjectPermissionResponse", aclResponseList);
        Assert.assertEquals(2, responseList.size());
    }
}

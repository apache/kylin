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

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V2_JSON;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.kylin.common.persistence.AclEntity;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.user.ManagedUser;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.controller.v2.NAccessControllerV2;
import org.apache.kylin.rest.response.AccessEntryResponse;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.OpenAccessGroupResponse;
import org.apache.kylin.rest.response.OpenAccessUserResponse;
import org.apache.kylin.rest.security.AclEntityType;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.AclTCRService;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.service.ProjectService;
import org.apache.kylin.rest.service.UserService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.security.acls.domain.GrantedAuthoritySid;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

public class NAccessControllerV2Test extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;

    @Mock
    private AccessService accessService;

    @Mock
    private UserService userService;

    @Mock
    private AclTCRService aclTCRService;

    @Mock
    private AclEvaluate aclEvaluate;

    @Mock
    private ProjectService projectService;

    @Mock
    private IUserGroupService userGroupService;

    @InjectMocks
    private NAccessControllerV2 nAccessControllerV2 = Mockito.spy(new NAccessControllerV2());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(nAccessControllerV2) //
                .defaultRequest(MockMvcRequestBuilders.get("/")).build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testGetAllAccessEntitiesOfUser() throws Exception {
        String userName = "ADMIN";
        Mockito.when(accessService.getGrantedProjectsOfUser(userName)).thenReturn(Lists.newArrayList("default"));
        Mockito.when(userService.userExists(userName)).thenReturn(Boolean.TRUE);
        Mockito.when(aclTCRService.getAuthorizedTables("default", userName)).thenReturn(Lists.newArrayList());

        mockMvc.perform(MockMvcRequestBuilders.get("/api/access/{userName:.+}", userName)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nAccessControllerV2).getAllAccessEntitiesOfUser(userName);
    }

    @Test
    public void testGetAllAccessUsers() throws Exception {
        String project = "default";
        String userName = "user01";
        mockMvc.perform(MockMvcRequestBuilders.get("/api/access/all/users").contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nAccessControllerV2).getAllAccessUsers(null, null, 0, 10);

        Mockito.doNothing().when(aclEvaluate).checkProjectAdminPermission(project);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/access/all/users").param("project", project)
                .param("userName", userName).contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().is5xxServerError()).andReturn();

        Mockito.verify(nAccessControllerV2).getAllAccessUsers(project, userName, 0, 10);

        List<GrantedAuthority> authorities = new ArrayList<>();
        ManagedUser user = new ManagedUser(userName, "123", false, authorities);
        Authentication authentication = new TestingAuthenticationToken(user, userName, Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(authentication);
        Mockito.doReturn(user).when(userService).loadUserByUsername(userName);
        try {
            nAccessControllerV2.getAllAccessUsers(project, userName, 0, 10);
        } catch (Exception e) {
            Assert.assertEquals(String.format(Locale.ROOT, "User '%s' does not exists.", userName), e.getMessage());
        }

        ProjectInstance projectInstance = NProjectManager.getInstance(getTestConfig()).getProject(project);
        Mockito.doReturn(Lists.newArrayList(projectInstance)).when(projectService).getReadableProjects(project, true);
        AccessEntryResponse accessEntryResponse = Mockito.mock(AccessEntryResponse.class);
        AclEntity aclEntity = Mockito.mock(RootPersistentEntity.class);
        PrincipalSid principalSid = Mockito.mock(PrincipalSid.class);
        Mockito.doReturn(principalSid).when(accessEntryResponse).getSid();
        Mockito.doReturn(userName).when(principalSid).getPrincipal();
        Mockito.doReturn(aclEntity).when(accessService).getAclEntity(AclEntityType.PROJECT_INSTANCE,
                projectInstance.getUuid());
        Mockito.doReturn(Lists.newArrayList(accessEntryResponse)).when(accessService)
                .generateAceResponsesByFuzzMatching(aclEntity, null, false);
        EnvelopeResponse<OpenAccessUserResponse> envelopeResponse1 = nAccessControllerV2.getAllAccessUsers(project,
                null, 0, 10);
        Assert.assertNotNull(envelopeResponse1.getData());
    }

    @Test
    public void testGetAllAccessGroups() throws Exception {
        String project = "default";
        String groupName = "group01";
        mockMvc.perform(MockMvcRequestBuilders.get("/api/access/all/groups").contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nAccessControllerV2).getAllAccessGroups(null, null, 0, 10);

        mockMvc.perform(MockMvcRequestBuilders.get("/api/access/all/groups").param("project", project)
                .param("groupName", groupName).contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nAccessControllerV2).getAllAccessGroups(project, groupName, 0, 10);

        List<GrantedAuthority> authorities = new ArrayList<>();
        ManagedUser user = new ManagedUser("user", "123", false, authorities);
        Authentication authentication = new TestingAuthenticationToken(user, "user", Constant.ROLE_ADMIN);
        Mockito.doReturn(Lists.newArrayList(user)).when(userGroupService).getGroupMembersByName(groupName);
        EnvelopeResponse<OpenAccessGroupResponse> envelopeResponse = nAccessControllerV2.getAllAccessGroups(project,
                groupName, 0, 10);
        Assert.assertNotNull(envelopeResponse.getData());

        ProjectInstance projectInstance = NProjectManager.getInstance(getTestConfig()).getProject(project);
        Mockito.doReturn(Lists.newArrayList(projectInstance)).when(projectService).getReadableProjects(project, true);
        AccessEntryResponse accessEntryResponse = Mockito.mock(AccessEntryResponse.class);
        AclEntity aclEntity = Mockito.mock(RootPersistentEntity.class);
        GrantedAuthoritySid grantedAuthoritySid = Mockito.mock(GrantedAuthoritySid.class);
        Mockito.doReturn(grantedAuthoritySid).when(accessEntryResponse).getSid();
        Mockito.doReturn(groupName).when(grantedAuthoritySid).getGrantedAuthority();
        Mockito.doReturn(aclEntity).when(accessService).getAclEntity(AclEntityType.PROJECT_INSTANCE,
                projectInstance.getUuid());
        Mockito.doReturn(Lists.newArrayList(accessEntryResponse)).when(accessService)
                .generateAceResponsesByFuzzMatching(aclEntity, null, false);
        EnvelopeResponse<OpenAccessGroupResponse> envelopeResponse1 = nAccessControllerV2.getAllAccessGroups(project,
                null, 0, 10);
        Assert.assertNotNull(envelopeResponse1.getData());
    }

}

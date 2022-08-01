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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.model.AutoMergeTimeEnum;
import org.apache.kylin.metadata.model.RetentionRange;
import org.apache.kylin.metadata.model.VolatileRange;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.DefaultDatabaseRequest;
import org.apache.kylin.rest.request.FavoriteQueryThresholdRequest;
import org.apache.kylin.rest.request.GarbageCleanUpConfigRequest;
import org.apache.kylin.rest.request.JdbcSourceInfoRequest;
import org.apache.kylin.rest.request.JobNotificationConfigRequest;
import org.apache.kylin.rest.request.OwnerChangeRequest;
import org.apache.kylin.rest.request.ProjectConfigRequest;
import org.apache.kylin.rest.request.ProjectGeneralInfoRequest;
import org.apache.kylin.rest.request.ProjectRequest;
import org.apache.kylin.rest.request.PushDownConfigRequest;
import org.apache.kylin.rest.request.PushDownProjectConfigRequest;
import org.apache.kylin.rest.request.SegmentConfigRequest;
import org.apache.kylin.rest.request.ShardNumConfigRequest;
import org.apache.kylin.rest.request.SnapshotConfigRequest;
import org.apache.kylin.rest.request.StorageQuotaRequest;
import org.apache.kylin.rest.request.YarnQueueRequest;
import org.apache.kylin.rest.response.FavoriteQueryThresholdResponse;
import org.apache.kylin.rest.response.ProjectConfigResponse;
import org.apache.kylin.rest.response.StorageVolumeInfoResponse;
import org.apache.kylin.rest.security.AclPermissionEnum;
import org.apache.kylin.rest.service.ProjectService;
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
import org.springframework.http.MediaType;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import lombok.val;

public class NProjectControllerTest extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    private AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    private ProjectService projectService;

    @InjectMocks
    private NProjectController nProjectController = Mockito.spy(new NProjectController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        createTestMetadata();
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(nProjectController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    private ProjectRequest mockProjectRequest() {
        ProjectRequest projectRequest = new ProjectRequest();
        projectRequest.setName("test");
        projectRequest.setDescription("test");
        projectRequest.setOverrideKylinProps(new LinkedHashMap<>());
        return projectRequest;
    }

    @Test
    public void testGetProjects() throws Exception {
        List<ProjectInstance> projects = new ArrayList<>();
        ProjectInstance projectInstance = new ProjectInstance();
        projectInstance.setName("project1");
        projects.add(projectInstance);
        Mockito.when(projectService.getReadableProjects("default", false)).thenReturn(projects);
        MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.get("/api/projects").contentType(MediaType.APPLICATION_JSON)
                        .param("project", "default").param("page_offset", "0").param("page_size", "10")
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nProjectController).getProjects("default", 0, 10, false, AclPermissionEnum.READ.name());

    }

    @Test
    public void testInvalidProjectName() {
        ProjectRequest projectRequest = mockProjectRequest();
        projectRequest.setName("^project");
        thrown.expect(KylinException.class);
        thrown.expectMessage(Message.getInstance().getInvalidProjectName());
        nProjectController.saveProject(projectRequest);
    }

    @Test
    public void testSaveProjects() throws Exception {
        ProjectInstance projectInstance = new ProjectInstance();
        projectInstance.setName("test");
        ProjectRequest projectRequest = mockProjectRequest();
        projectRequest.setName("test");
        Mockito.when(projectService.createProject(projectInstance.getName(), projectInstance))
                .thenReturn(projectInstance);
        MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.post("/api/projects").contentType(MediaType.APPLICATION_JSON)
                        .content(JsonUtil.writeValueAsString(projectRequest))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nProjectController).saveProject(Mockito.any(ProjectRequest.class));
    }

    @Test
    public void testUpdateQueryAccelerateThreshold() throws Exception {
        FavoriteQueryThresholdRequest favoriteQueryThresholdRequest = new FavoriteQueryThresholdRequest();
        favoriteQueryThresholdRequest.setThreshold(20);
        favoriteQueryThresholdRequest.setTipsEnabled(true);
        Mockito.doNothing().when(projectService).updateQueryAccelerateThresholdConfig("default", 20, true);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/{project}/query_accelerate_threshold", "default")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(favoriteQueryThresholdRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nProjectController).updateQueryAccelerateThresholdConfig(eq("default"),
                Mockito.any(FavoriteQueryThresholdRequest.class));
    }

    @Test
    public void testGetQueryAccelerateThreshold() throws Exception {
        FavoriteQueryThresholdResponse favoriteQueryThresholdResponse = new FavoriteQueryThresholdResponse();
        favoriteQueryThresholdResponse.setTipsEnabled(true);
        favoriteQueryThresholdResponse.setThreshold(20);
        Mockito.doReturn(favoriteQueryThresholdResponse).when(projectService)
                .getQueryAccelerateThresholdConfig("default");
        mockMvc.perform(MockMvcRequestBuilders.get("/api/projects/{project}/query_accelerate_threshold", "default")
                .contentType(MediaType.APPLICATION_JSON).param("project", "default")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nProjectController).getQueryAccelerateThresholdConfig("default");
    }

    @Test
    public void testGetStorageVolumeInfoResponse() throws Exception {
        StorageVolumeInfoResponse storageVolumeInfoResponse = new StorageVolumeInfoResponse();
        Mockito.doReturn(storageVolumeInfoResponse).when(projectService).getStorageVolumeInfoResponse("default");
        mockMvc.perform(MockMvcRequestBuilders.get("/api/projects/{project}/storage_volume_info", "default")
                .contentType(MediaType.APPLICATION_JSON).param("project", "default")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nProjectController).getStorageVolumeInfo("default");
    }

    @Test
    public void testUpdateStorageQuotaConfig() throws Exception {
        StorageQuotaRequest storageQuotaRequest = new StorageQuotaRequest();
        storageQuotaRequest.setStorageQuotaSize(2147483648L);
        Mockito.doNothing().when(projectService).updateStorageQuotaConfig("default", 2147483648L);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/{project}/storage_quota", "default")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(storageQuotaRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nProjectController).updateStorageQuotaConfig(eq("default"),
                Mockito.any(StorageQuotaRequest.class));
    }

    @Test
    public void testStorageCleanup() throws Exception {
        ProjectInstance projectInstance = new ProjectInstance();
        NProjectManager projectManager = Mockito.mock(NProjectManager.class);
        Mockito.doReturn(projectInstance).when(projectManager).getProject("default");
        Mockito.doReturn(projectManager).when(projectService).getManager(NProjectManager.class);
        Mockito.doNothing().when(projectService).cleanupGarbage("default");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/{project}/storage", "default")
                .contentType(MediaType.APPLICATION_JSON).param("project", "default")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nProjectController).cleanupProjectStorage("default");
    }

    @Test
    public void testUpdateJobNotificationConfig() throws Exception {
        val request = new JobNotificationConfigRequest();

        request.setJobErrorNotificationEnabled(true);
        request.setDataLoadEmptyNotificationEnabled(true);
        request.setJobNotificationEmails(Arrays.asList("fff@g.com"));

        Mockito.doNothing().when(projectService).updateJobNotificationConfig("default", request);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/{project}/job_notification_config", "default")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nProjectController).updateJobNotificationConfig("default", request);
    }

    @Test
    public void testUpdatePushDownConfig() throws Exception {
        val request = new PushDownConfigRequest();
        request.setPushDownEnabled(true);

        Mockito.doNothing().when(projectService).updatePushDownConfig("default", request);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/{project}/push_down_config", "default")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nProjectController).updatePushDownConfig("default", request);
    }

    @Test
    public void testUpdatePushDownProjectConfig() throws Exception {
        val request = new PushDownProjectConfigRequest();
        request.setConverterClassNames("org.apache.kylin.query.util.SparkSQLFunctionConverter");
        request.setRunnerClassName("org.apache.kylin.query.pushdown.PushDownRunnerSparkImpl");

        Mockito.doNothing().when(projectService).updatePushDownProjectConfig("default", request);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/{project}/push_down_project_config", "default")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nProjectController).updatePushDownProjectConfig("default", request);
    }

    @Test
    public void testUpdateSnapshotConfig() throws Exception {
        val request = new SnapshotConfigRequest();
        request.setSnapshotManualManagementEnabled(true);

        Mockito.doNothing().when(projectService).updateSnapshotConfig("default", request);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/{project}/snapshot_config", "default")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nProjectController).updateSnapshotConfig("default", request);
    }

    @Test
    public void testUpdateSnapshotConfigWithDefaultSnapshotManualManagementEnabled() throws Exception {
        val request = new SnapshotConfigRequest();

        Mockito.doNothing().when(projectService).updateSnapshotConfig("default", request);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/{project}/snapshot_config", "default")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nProjectController).updateSnapshotConfig("default", request);
    }

    @Test
    public void testUpdateShardNumConfig() throws Exception {
        val request = new ShardNumConfigRequest();
        Mockito.doNothing().when(projectService).updateShardNumConfig("default", request);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/{project}/shard_num_config", "default")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nProjectController).updateShardNumConfig("default", request);
    }

    @Test
    public void testUpdateSegmentConfig() throws Exception {
        val request = new SegmentConfigRequest();
        request.setVolatileRange(new VolatileRange());
        request.setRetentionRange(new RetentionRange());
        request.setAutoMergeEnabled(true);
        request.setAutoMergeTimeRanges(Arrays.asList(AutoMergeTimeEnum.DAY));
        request.setCreateEmptySegmentEnabled(true);

        Mockito.doNothing().when(projectService).updateSegmentConfig("default", request);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/{project}/segment_config", "default")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nProjectController).updateSegmentConfig(eq("default"), Mockito.any(request.getClass()));
    }

    @Test
    public void testUpdateSegmentConfigWithIllegalRetentionRange() throws Exception {
        val request = new SegmentConfigRequest();
        request.setVolatileRange(new VolatileRange());
        request.setRetentionRange(new RetentionRange(-1, true, AutoMergeTimeEnum.DAY));
        Mockito.doNothing().when(projectService).updateSegmentConfig("default", request);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/{project}/segment_config", "default")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();
        Mockito.verify(nProjectController, Mockito.never()).updateSegmentConfig("default", request);
    }

    @Test
    public void testUpdateSegmentConfigWithIllegalVolatileRange() throws Exception {
        val request = new SegmentConfigRequest();
        request.setRetentionRange(new RetentionRange());
        request.setVolatileRange(new VolatileRange(-1, true, AutoMergeTimeEnum.DAY));
        Mockito.doNothing().when(projectService).updateSegmentConfig("default", request);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/{project}/segment_config", "default")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();
        Mockito.verify(nProjectController, Mockito.never()).updateSegmentConfig("default", request);
    }

    @Test
    public void testUpdateProjectGeneralInfo() throws Exception {
        val request = new ProjectGeneralInfoRequest();
        request.setSemiAutoMode(true);
        Mockito.doNothing().when(projectService).updateProjectGeneralInfo("default", request);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/{project}/project_general_info", "default")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nProjectController).updateProjectGeneralInfo("default", request);
    }

    @Test
    public void testGetProjectConfig() throws Exception {
        val response = new ProjectConfigResponse();
        Mockito.doReturn(response).when(projectService).getProjectConfig("default");
        final MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.get("/api/projects/{project}/project_config", "default")
                        .contentType(MediaType.APPLICATION_JSON).param("project", "default")
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Assert.assertTrue(mvcResult.getResponse().getContentAsString().contains("\"semi_automatic_mode\":false"));
        Mockito.verify(nProjectController).getProjectConfig("default");
    }

    @Test
    public void testDeleteProjectConfig() throws Exception {
        ProjectConfigRequest request = new ProjectConfigRequest();
        request.setProject("default");
        request.setConfigName("a");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/projects/config/deletion")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nProjectController).deleteProjectConfig(Mockito.any(ProjectConfigRequest.class));
    }

    @Test
    public void testUpdateProjectConfig() throws Exception {
        {
            Map<String, String> map = new HashMap<>();
            map.put("a", "b");
            mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/{project}/config", "default")
                    .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(map))
                    .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                    .andExpect(MockMvcResultMatchers.status().isOk());

            Mockito.verify(nProjectController).updateProjectConfig("default", map);

            map.put("kylin.source.default", "1");

            mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/{project}/config", "default")
                    .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(map))
                    .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                    .andExpect(MockMvcResultMatchers.status().isInternalServerError());
            Mockito.doThrow(KylinException.class).when(nProjectController).updateProjectConfig("default", map);
        }
        {
            Map<String, String> map = new HashMap<>();
            getTestConfig().setProperty("kylin.server.non-custom-project-configs", "kylin.job.retry");
            map.put("kylin.job.retry", "1");
            mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/{project}/config", "default")
                    .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(map))
                    .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                    .andExpect(MockMvcResultMatchers.status().isInternalServerError());
            Mockito.doThrow(KylinException.class).when(nProjectController).updateProjectConfig("default", map);
        }
    }

    @Test
    public void testDeleteProjectConfigException() throws Exception {
        {
            ProjectConfigRequest request = new ProjectConfigRequest();
            request.setProject("default");
            request.setConfigName("kylin.source.default");
            mockMvc.perform(MockMvcRequestBuilders.post("/api/projects/config/deletion")
                    .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                    .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                    .andExpect(MockMvcResultMatchers.status().isInternalServerError());

            Mockito.doThrow(KylinException.class).when(nProjectController).deleteProjectConfig(request);
        }
        {
            ProjectConfigRequest request = new ProjectConfigRequest();
            request.setProject("default");
            getTestConfig().setProperty("kylin.server.non-custom-project-configs", "kylin.job.retry");
            request.setConfigName("kylin.job.retry");
            mockMvc.perform(MockMvcRequestBuilders.post("/api/projects/config/deletion")
                    .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                    .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                    .andExpect(MockMvcResultMatchers.status().isInternalServerError());

            Mockito.doThrow(KylinException.class).when(nProjectController).deleteProjectConfig(request);
        }

    }

    @Test
    public void testGetNonCustomProjectConfigs() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/projects/default_configs")
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nProjectController).getNonCustomProjectConfigs();
        Assert.assertEquals(17, getTestConfig().getNonCustomProjectConfigs().size());
    }

    @Test
    public void testUpdateDefaultDatabase() throws Exception {
        val request = new DefaultDatabaseRequest();
        request.setDefaultDatabase("EDW");
        Mockito.doNothing().when(projectService).updateDefaultDatabase("default", request.getDefaultDatabase());
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/{project}/default_database", "default")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nProjectController).updateDefaultDatabase("default", request);
    }

    @Test
    public void testUpdateYarnQueue() throws Exception {
        val request = new YarnQueueRequest();
        request.setQueueName("q.queue");
        Mockito.doNothing().when(projectService).updateYarnQueue("project", request.getQueueName());
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/{project}/yarn_queue", "project")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nProjectController).updateYarnQueue("project", request);
    }

    @Test
    public void testUpdateProjectOwner() {
        String project = "default";
        String owner = "test";

        OwnerChangeRequest ownerChangeRequest = new OwnerChangeRequest();
        ownerChangeRequest.setOwner(owner);

        Mockito.doNothing().when(projectService).updateProjectOwner(project, ownerChangeRequest);
        nProjectController.updateProjectOwner(project, ownerChangeRequest);
        Mockito.verify(nProjectController).updateProjectOwner(project, ownerChangeRequest);
    }

    @Test
    public void testUpdateGarbageCleanupConfig() throws Exception {
        GarbageCleanUpConfigRequest request = new GarbageCleanUpConfigRequest();
        request.setLowFrequencyThreshold(1L);
        request.setFrequencyTimeWindow(GarbageCleanUpConfigRequest.FrequencyTimeWindowEnum.DAY);
        Mockito.doAnswer(x -> null).when(projectService).updateGarbageCleanupConfig(Mockito.any(), Mockito.any());
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/{project}/garbage_cleanup_config", "default")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nProjectController).updateGarbageCleanupConfig(Mockito.any(), Mockito.any());
    }

    @Test
    public void testUpdateJdbcSourceConfig() throws Exception {
        val request = new JdbcSourceInfoRequest();
        request.setJdbcSourceEnable(true);
        Mockito.doNothing().when(projectService).updateJdbcInfo("project", request);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/{project}/jdbc_source_info_config", "project")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(projectService).updateJdbcInfo(any(), Mockito.any());
    }
}

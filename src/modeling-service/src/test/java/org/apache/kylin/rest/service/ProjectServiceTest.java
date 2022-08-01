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

import static org.apache.kylin.common.constant.Constants.HIDDEN_VALUE;
import static org.apache.kylin.common.constant.Constants.KYLIN_SOURCE_JDBC_DRIVER_KEY;
import static org.apache.kylin.common.constant.Constants.KYLIN_SOURCE_JDBC_PASS_KEY;
import static org.apache.kylin.metadata.model.MaintainModelType.MANUAL_MAINTAIN;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.EncryptUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.job.common.ShellExecutable;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.optimization.FrequencyMap;
import org.apache.kylin.metadata.model.AutoMergeTimeEnum;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.metadata.recommendation.candidate.JdbcRawRecStore;
import org.apache.kylin.query.pushdown.PushDownRunnerSparkImpl;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.GarbageCleanUpConfigRequest;
import org.apache.kylin.rest.request.JdbcRequest;
import org.apache.kylin.rest.request.JdbcSourceInfoRequest;
import org.apache.kylin.rest.request.JobNotificationConfigRequest;
import org.apache.kylin.rest.request.MultiPartitionConfigRequest;
import org.apache.kylin.rest.request.OwnerChangeRequest;
import org.apache.kylin.rest.request.ProjectGeneralInfoRequest;
import org.apache.kylin.rest.request.PushDownConfigRequest;
import org.apache.kylin.rest.request.PushDownProjectConfigRequest;
import org.apache.kylin.rest.request.SegmentConfigRequest;
import org.apache.kylin.rest.request.ShardNumConfigRequest;
import org.apache.kylin.rest.response.StorageVolumeInfoResponse;
import org.apache.kylin.rest.response.UserProjectPermissionResponse;
import org.apache.kylin.rest.security.AclPermissionEnum;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.apache.kylin.streaming.manager.StreamingJobManager;
import org.apache.kylin.streaming.metadata.StreamingJobMeta;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.multipart.MultipartFile;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.clickhouse.MockSecondStorage;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProjectServiceTest extends NLocalFileMetadataTestCase {
    private static final String PROJECT = "default";
    private static final String PROJECT_JDBC = "jdbc";
    private static final String PROJECT_ID = "a8f4da94-a8a4-464b-ab6f-b3012aba04d5";
    private static final String MODEL_ID = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";

    @InjectMocks
    private final ProjectService projectService = Mockito.spy(ProjectService.class);

    @InjectMocks
    private final ProjectSmartServiceSupporter projectSmartService = Mockito.spy(ProjectSmartServiceSupporter.class);

    @InjectMocks
    private final ModelService modelService = Mockito.spy(ModelService.class);

    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    private final AsyncTaskServiceSupporter asyncTaskService = Mockito.spy(AsyncTaskServiceSupporter.class);

    @Mock
    private final AccessService accessService = Mockito.spy(AccessService.class);

    @Mock
    private final UserService userService = Mockito.spy(UserService.class);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private NProjectManager projectManager;

    private JdbcRawRecStore jdbcRawRecStore;

    @Before
    public void setup() {
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        overwriteSystemProp("kylin.cube.low-frequency-threshold", "5");
        createTestMetadata();
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", Mockito.spy(AclUtil.class));
        ReflectionTestUtils.setField(projectService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(projectService, "accessService", accessService);
        ReflectionTestUtils.setField(projectService, "projectModelSupporter", modelService);
        ReflectionTestUtils.setField(projectService, "userService", userService);
        ReflectionTestUtils.setField(projectService, "projectSmartService", projectSmartService);

        ReflectionTestUtils.setField(modelService, "aclEvaluate", aclEvaluate);
        projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        try {
            jdbcRawRecStore = new JdbcRawRecStore(getTestConfig());
        } catch (Exception e) {
            log.error("initialize rec store failed.");
        }
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testCreateProjectSemiMode() {
        ProjectInstance projectInstance = new ProjectInstance();
        KylinConfig.getInstanceFromEnv().setProperty("kylin.metadata.semi-automatic-mode", "true");
        projectInstance.setName("project11");
        UnitOfWork.doInTransactionWithRetry(() -> {
            projectService.createProject(projectInstance.getName(), projectInstance);
            return null;
        }, projectInstance.getName());
        ProjectInstance projectInstance2 = projectManager.getProject("project11");
        Assert.assertNotNull(projectInstance2);
        Assert.assertEquals("true", projectInstance2.getOverrideKylinProps().get("kylin.metadata.semi-automatic-mode"));
        projectManager.dropProject("project11");
    }

    @Test
    public void testCreateProjectManualMaintainPass() {

        ProjectInstance projectInstance = new ProjectInstance();
        projectInstance.setName("project11");
        UnitOfWork.doInTransactionWithRetry(() -> {
            projectService.createProject(projectInstance.getName(), projectInstance);
            return null;
        }, projectInstance.getName());
        ProjectInstance projectInstance2 = projectManager.getProject("project11");
        Assert.assertNotNull(projectInstance2);
        Assert.assertFalse(projectInstance2.isSemiAutoMode());
        Assert.assertEquals(MANUAL_MAINTAIN, projectInstance2.getMaintainModelType());
        projectManager.dropProject("project11");
    }

    @Test
    public void testCreateProjectException() throws Exception {

        ProjectInstance projectInstance = new ProjectInstance();
        projectInstance.setName(PROJECT);
        thrown.expect(KylinException.class);
        thrown.expectMessage("The project name \"default\" already exists. Please rename it.");
        projectService.createProject(projectInstance.getName(), projectInstance);

    }

    @Test
    public void testGetReadableProjectsByName() {
        Mockito.doReturn(true).when(aclEvaluate).hasProjectAdminPermission(Mockito.any(ProjectInstance.class));
        List<ProjectInstance> projectInstances = projectService.getReadableProjects(PROJECT, true);
        Assert.assertTrue(projectInstances.size() == 1 && projectInstances.get(0).getName().equals(PROJECT));

    }

    @Test
    public void testGetReadableProjectsByFuzzyName() {
        Mockito.doReturn(true).when(aclEvaluate).hasProjectAdminPermission(Mockito.any(ProjectInstance.class));
        List<ProjectInstance> projectInstances = projectService.getReadableProjects("TOP", false);
        Assert.assertTrue(projectInstances.size() == 1 && projectInstances.get(0).getName().equals("top_n"));

        projectInstances = projectService.getReadableProjects("not_exist_project", false);
        Assert.assertEquals(0, projectInstances.size());
    }

    @Test
    public void testGetReadableProjects() {
        Mockito.doReturn(true).when(aclEvaluate).hasProjectAdminPermission(Mockito.any(ProjectInstance.class));
        List<ProjectInstance> projectInstances = projectService.getReadableProjects("", false);
        Assert.assertEquals(26, projectInstances.size());
    }

    @Test
    public void testGetAdminProjects() throws Exception {
        Mockito.doReturn(true).when(aclEvaluate).hasProjectAdminPermission(Mockito.any(ProjectInstance.class));
        List<ProjectInstance> projectInstances = projectService.getAdminProjects();
        Assert.assertEquals(26, projectInstances.size());
    }

    @Test
    public void testGetReadableProjectsNoPermission() {
        Mockito.doReturn(false).when(aclEvaluate).hasProjectReadPermission(Mockito.any(ProjectInstance.class));
        List<ProjectInstance> projectInstances = projectService.getReadableProjects();
        Assert.assertEquals(0, projectInstances.size());
    }

    @Test
    public void testGetReadableProjectsHasNoPermissionProject() {
        Mockito.doReturn(true).when(aclEvaluate).hasProjectAdminPermission(Mockito.any(ProjectInstance.class));
        List<ProjectInstance> projectInstances = projectService.getReadableProjects("", false);
        Assert.assertEquals(26, projectInstances.size());

    }

    @Test
    public void testGetProjectsWrapWIthUserPermission() throws Exception {
        Mockito.doReturn(Mockito.mock(UserDetails.class)).when(userService).loadUserByUsername(Mockito.anyString());
        Mockito.doReturn(true).when(userService).isGlobalAdmin(Mockito.any(UserDetails.class));
        List<UserProjectPermissionResponse> projectInstances = projectService
                .getProjectsFilterByExactMatchAndPermissionWrapperUserPermission("default", true,
                        AclPermissionEnum.READ);
        Assert.assertEquals(1, projectInstances.size());
        Assert.assertEquals("ADMINISTRATION", projectInstances.get(0).getPermission());
    }

    @Test
    public void testUpdateThreshold() throws Exception {
        Mockito.doReturn(true).when(aclEvaluate).hasProjectAdminPermission(Mockito.any(ProjectInstance.class));
        projectService.updateQueryAccelerateThresholdConfig(PROJECT, 30, false);
        List<ProjectInstance> projectInstances = projectService.getReadableProjects(PROJECT, false);
        Assert.assertEquals("30",
                projectInstances.get(0).getOverrideKylinProps().get("kylin.favorite.query-accelerate-threshold"));
        Assert.assertEquals("false",
                projectInstances.get(0).getOverrideKylinProps().get("kylin.favorite.query-accelerate-tips-enable"));
    }

    @Test
    public void testGetThreshold() {
        val response = projectService.getQueryAccelerateThresholdConfig(PROJECT);
        Assert.assertEquals(20, response.getThreshold());
        Assert.assertTrue(response.isTipsEnabled());
    }

    @Test
    public void testUpdateStorageQuotaConfig() {
        Assert.assertThrows(
                "No valid storage quota size, Please set an integer greater than or equal to 1TB "
                        + "to 'storage_quota_size', unit byte.",
                KylinException.class, () -> projectService.updateStorageQuotaConfig(PROJECT, 2147483648L));

        projectService.updateStorageQuotaConfig(PROJECT, 1024L * 1024 * 1024 * 1024);
        Assert.assertEquals(1024L * 1024 * 1024 * 1024,
                NProjectManager.getInstance(getTestConfig()).getProject(PROJECT).getConfig().getStorageQuotaSize());

    }

    @Test
    public void testGetStorageVolumeInfoResponse() {
        getTestConfig().setProperty("kylin.metadata.semi-automatic-mode", "true");
        prepareLayoutHitCount();
        String error = "do not use aclEvalute in getStorageVolumeInfoResponse, because backend thread would invoke this method in (BootstrapCommand.class)";
        Mockito.doThrow(new RuntimeException(error)).when(aclEvaluate).checkProjectReadPermission(Mockito.any());
        Mockito.doThrow(new RuntimeException(error)).when(aclEvaluate).checkProjectOperationPermission(Mockito.any());
        Mockito.doThrow(new RuntimeException(error)).when(aclEvaluate).checkProjectWritePermission(Mockito.any());
        Mockito.doThrow(new RuntimeException(error)).when(aclEvaluate).checkProjectAdminPermission(Mockito.any());
        Mockito.doThrow(new RuntimeException(error)).when(aclEvaluate).checkIsGlobalAdmin();
        StorageVolumeInfoResponse storageVolumeInfoResponse = projectService.getStorageVolumeInfoResponse(PROJECT);

        Assert.assertEquals(10240L * 1024 * 1024 * 1024, storageVolumeInfoResponse.getStorageQuotaSize());

        // for MODEL(MODEL_ID) layout-1000001 is manual and auto, it will be considered as manual layout
        Assert.assertEquals(2988131, storageVolumeInfoResponse.getGarbageStorageSize());
    }

    private void prepareLayoutHitCount() {
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), PROJECT);

        long currentTime = System.currentTimeMillis();
        long currentDate = TimeUtil.getDayStart(currentTime);
        long dayInMillis = 24 * 60 * 60 * 1000L;

        dataflowManager.updateDataflow(MODEL_ID, copyForWrite -> {
            copyForWrite.setLayoutHitCount(new HashMap<Long, FrequencyMap>() {
                {
                    put(1L, new FrequencyMap(new TreeMap<Long, Integer>() {
                        {
                            put(currentDate - 7 * dayInMillis, 1);
                            put(currentDate - 30 * dayInMillis, 12);
                        }
                    }));
                    put(10001L, new FrequencyMap(new TreeMap<Long, Integer>() {
                        {
                            put(currentDate - 7 * dayInMillis, 1);
                            put(currentDate - 30 * dayInMillis, 2);
                        }
                    }));
                    put(1000001L, new FrequencyMap(new TreeMap<Long, Integer>() {
                        {
                            put(currentDate - 30 * dayInMillis, 10);
                        }
                    }));
                }
            });
        });
    }

    @Test
    public void testGetProjectConfig() {
        val response = projectService.getProjectConfig(PROJECT);
        Assert.assertEquals(20, response.getFavoriteQueryThreshold());
        Assert.assertFalse(response.isAutoMergeEnabled());
        Assert.assertFalse(response.getRetentionRange().isRetentionRangeEnabled());
        Assert.assertFalse(response.isExposeComputedColumn());
    }

    @Test
    public void testJobNotificationConfig() {
        val project = PROJECT;
        var response = projectService.getProjectConfig(project);
        val jobNotificationConfigRequest = new JobNotificationConfigRequest();
        jobNotificationConfigRequest.setDataLoadEmptyNotificationEnabled(false);
        jobNotificationConfigRequest.setJobErrorNotificationEnabled(false);
        jobNotificationConfigRequest.setJobNotificationEmails(
                Lists.newArrayList("user1@kyligence.io", "user2@kyligence.io", "user2@kyligence.io"));
        projectService.updateJobNotificationConfig(project, jobNotificationConfigRequest);
        response = projectService.getProjectConfig(project);
        Assert.assertEquals(2, response.getJobNotificationEmails().size());
        Assert.assertFalse(response.isJobErrorNotificationEnabled());
        Assert.assertFalse(response.isDataLoadEmptyNotificationEnabled());

        jobNotificationConfigRequest
                .setJobNotificationEmails(Lists.newArrayList("@kyligence.io", "user2@.io", "user2@kyligence.io"));
        thrown.expect(KylinException.class);
        projectService.updateJobNotificationConfig(project, jobNotificationConfigRequest);
        thrown = ExpectedException.none();
    }

    @Test
    public void testUpdateSegmentConfigWhenError() {
        val project = PROJECT;

        val segmentConfigRequest = new SegmentConfigRequest();
        segmentConfigRequest.setAutoMergeEnabled(false);
        segmentConfigRequest.setAutoMergeTimeRanges(Collections.singletonList(AutoMergeTimeEnum.DAY));

        segmentConfigRequest.getRetentionRange().setRetentionRangeType(null);
        try {
            projectService.updateSegmentConfig(project, segmentConfigRequest);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertTrue(e.getMessage().contains(
                    "No valid value for 'retention_range_type', Please set {'DAY', 'MONTH', 'YEAR'} to specify the period of retention."));
        }
        segmentConfigRequest.getVolatileRange().setVolatileRangeNumber(-1);
        try {
            projectService.updateSegmentConfig(project, segmentConfigRequest);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertTrue(e.getMessage()
                    .contains("No valid value. Please set an integer 'x' to "
                            + "'volatile_range_number'. The 'Auto-Merge' will not merge latest 'x' "
                            + "period(day/week/month/etc..) segments."));
        }
        segmentConfigRequest.getVolatileRange().setVolatileRangeNumber(1);
        segmentConfigRequest.getRetentionRange().setRetentionRangeNumber(-1);
        try {
            projectService.updateSegmentConfig(project, segmentConfigRequest);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertTrue(e.getMessage().contains("No valid value for 'retention_range_number'."
                    + " Please set an integer 'x' to specify the retention threshold. The system will "
                    + "only retain the segments in the retention threshold (x years before the last data time). "));
        }
        segmentConfigRequest.getRetentionRange().setRetentionRangeNumber(1);
        segmentConfigRequest.setAutoMergeTimeRanges(new ArrayList<>());
        try {
            projectService.updateSegmentConfig(project, segmentConfigRequest);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertTrue(e.getMessage().contains("No valid value for 'auto_merge_time_ranges'. Please set "
                    + "{'DAY', 'WEEK', 'MONTH', 'QUARTER', 'YEAR'} to specify the period of auto-merge. "));
        }
    }

    @Test
    public void testUpdateProjectConfig() throws IOException {
        val project = PROJECT;

        val description = "test description";
        val request = new ProjectGeneralInfoRequest();
        request.setDescription(description);
        projectService.updateProjectGeneralInfo(project, request);
        var response = projectService.getProjectConfig(project);
        Assert.assertEquals(description, response.getDescription());

        request.setSemiAutoMode(true);
        projectService.updateProjectGeneralInfo(project, request);
        response = projectService.getProjectConfig(project);
        Assert.assertTrue(response.isSemiAutomaticMode());

        Assert.assertNull(response.getDefaultDatabase());
        projectService.updateDefaultDatabase(project, "EDW");
        Assert.assertEquals("EDW", projectService.getProjectConfig(project).getDefaultDatabase());

        val segmentConfigRequest = new SegmentConfigRequest();
        segmentConfigRequest.setAutoMergeEnabled(false);
        segmentConfigRequest.setAutoMergeTimeRanges(Collections.singletonList(AutoMergeTimeEnum.DAY));
        projectService.updateSegmentConfig(project, segmentConfigRequest);
        response = projectService.getProjectConfig(project);
        Assert.assertFalse(response.isAutoMergeEnabled());

        val pushDownConfigRequest = new PushDownConfigRequest();
        pushDownConfigRequest.setPushDownEnabled(false);
        projectService.updatePushDownConfig(project, pushDownConfigRequest);
        response = projectService.getProjectConfig(project);
        Assert.assertFalse(response.isPushDownEnabled());

        getTestConfig().setProperty("kylin.query.pushdown.runner-class-name",
                "org.apache.kylin.smart.query.mockup.MockupPushDownRunner");
        pushDownConfigRequest.setPushDownEnabled(true);
        projectService.updatePushDownConfig(project, pushDownConfigRequest);
        response = projectService.getProjectConfig(project);
        Assert.assertTrue(response.isPushDownEnabled());

        // this config should not expose to end users.
        val shardNumConfigRequest = new ShardNumConfigRequest();
        Map<String, String> map = new HashMap<>();
        map.put("DEFAULT.TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "100");
        map.put("DEFAULT.TEST_KYLIN_FACT.SELLER_ID", "50");
        shardNumConfigRequest.setColToNum(map);
        projectService.updateShardNumConfig(project, shardNumConfigRequest);
        val pi = NProjectManager.getInstance(getTestConfig()).getProject(project);
        Assert.assertEquals(
                JsonUtil.readValueAsMap(pi.getConfig().getExtendedOverrides().get("kylin.engine.shard-num-json")), map);

        getTestConfig().setProperty("kylin.query.pushdown.runner-class-name", "");
        pushDownConfigRequest.setPushDownEnabled(true);
        projectService.updatePushDownConfig(project, pushDownConfigRequest);
        val projectConfig = projectManager.getProject(project).getConfig();
        Assert.assertTrue(projectConfig.isPushDownEnabled());
        Assert.assertEquals(PushDownRunnerSparkImpl.class.getName(), projectConfig.getPushDownRunnerClassName());

        val pushDownProjectConfigRequest = new PushDownProjectConfigRequest();
        getTestConfig().setProperty("kylin.query.pushdown.runner-class-name", "");
        getTestConfig().setProperty("kylin.query.pushdown.converter-class-name", "");
        pushDownProjectConfigRequest.setRunnerClassName("org.apache.kylin.query.pushdown.PushDownRunnerSparkImpl");
        pushDownProjectConfigRequest.setConverterClassNames("org.apache.kylin.query.util.PowerBIConverter");
        projectService.updatePushDownProjectConfig(project, pushDownProjectConfigRequest);
        String[] converterClassNames = new String[] { "org.apache.kylin.query.util.PowerBIConverter" };
        // response
        response = projectService.getProjectConfig(project);
        Assert.assertEquals("org.apache.kylin.query.pushdown.PushDownRunnerSparkImpl", response.getRunnerClassName());
        Assert.assertEquals(String.join(",", converterClassNames), response.getConverterClassNames());
        // project config
        val projectConfig2 = projectManager.getProject(project).getConfig();
        Assert.assertEquals("org.apache.kylin.query.pushdown.PushDownRunnerSparkImpl",
                projectConfig2.getPushDownRunnerClassName());
        Assert.assertArrayEquals(converterClassNames, projectConfig2.getPushDownConverterClassNames());
        Assert.assertThrows(KylinException.class, () -> projectService.updateDefaultDatabase(project, "not_exits"));
    }

    @Test
    public void testUpdateProjectConfigTrim() {

        Map<String, String> testOverrideP = Maps.newLinkedHashMap();
        testOverrideP.put(" testk1 ", " testv1 ");
        testOverrideP.put("tes   tk2", "test    v2");
        testOverrideP.put("      tes    tk3 ", "    t     estv3    ");

        projectService.updateProjectConfig(PROJECT, testOverrideP);

        val kylinConfigExt = projectManager.getProject(PROJECT).getConfig().getExtendedOverrides();

        Assert.assertEquals("testv1", kylinConfigExt.get("testk1"));
        Assert.assertEquals("test    v2", kylinConfigExt.get("tes   tk2"));
        Assert.assertEquals("t     estv3", kylinConfigExt.get("tes    tk3"));
    }

    @Test
    public void testDeleteProjectConfig() {
        Map<String, String> testOverrideP = Maps.newLinkedHashMap();
        testOverrideP.put("testk1", "testv1");

        projectService.updateProjectConfig(PROJECT, testOverrideP);

        var kylinConfigExt = projectManager.getProject(PROJECT).getConfig().getExtendedOverrides();
        Assert.assertEquals("testv1", kylinConfigExt.get("testk1"));

        projectService.deleteProjectConfig(PROJECT, "testk1");

        kylinConfigExt = projectManager.getProject(PROJECT).getConfig().getExtendedOverrides();
        Assert.assertNull(kylinConfigExt.get("testk1"));
    }

    @Test
    public void testMultiPartitionConfig() {
        val project = PROJECT;
        val modelId = "b780e4e4-69af-449e-b09f-05c90dfa04b6";
        NDataflowManager dfm = NDataflowManager.getInstance(getTestConfig(), project);

        MultiPartitionConfigRequest request1 = new MultiPartitionConfigRequest(true);
        projectService.updateMultiPartitionConfig(project, request1, modelService);
        Assert.assertEquals(RealizationStatusEnum.ONLINE, dfm.getDataflow(modelId).getStatus());

        MultiPartitionConfigRequest request2 = new MultiPartitionConfigRequest(false);
        projectService.updateMultiPartitionConfig(project, request2, modelService);
        Assert.assertEquals(RealizationStatusEnum.OFFLINE, dfm.getDataflow(modelId).getStatus());
    }

    @Test
    public void testDropProject() {
        KylinConfig.getInstanceFromEnv().setMetadataUrl(
                "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,DATABASE_TO_UPPER=FALSE,username=sa,password=");
        val project = "project12";
        ProjectInstance projectInstance = new ProjectInstance();
        projectInstance.setName(project);
        UnitOfWork.doInTransactionWithRetry(() -> {
            projectService.createProject(project, projectInstance);
            return null;
        }, project);
        UnitOfWork.doInTransactionWithRetry(() -> {
            projectService.dropProject(project);
            return null;
        }, project);
        val prjManager = NProjectManager.getInstance(getTestConfig());
        Assert.assertNull(prjManager.getProject(project));
        Assert.assertNull(NDefaultScheduler.getInstanceByProject(project));
    }

    @Test
    public void testDropStreamingProject() {
        KylinConfig.getInstanceFromEnv().setMetadataUrl(
                "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,DATABASE_TO_UPPER=FALSE,username=sa,password=");
        val project = "project13";
        ProjectInstance projectInstance = new ProjectInstance();
        projectInstance.setName(project);
        UnitOfWork.doInTransactionWithRetry(() -> {
            projectService.createProject(project, projectInstance);
            return null;
        }, project);
        UnitOfWork.doInTransactionWithRetry(() -> {
            try {
                val mgr = Mockito.spy(StreamingJobManager.getInstance(getTestConfig(), PROJECT));
                val meta1 = new StreamingJobMeta();
                meta1.setCurrentStatus(JobStatusEnum.RUNNING);
                Mockito.when(projectService.getManager(StreamingJobManager.class, project)).thenReturn(mgr);
                Mockito.when(mgr.listAllStreamingJobMeta()).thenReturn(Collections.singletonList(meta1));
                projectService.dropProject(project);
            } catch (Exception e) {
                Assert.assertTrue(e instanceof KylinException);
                Assert.assertEquals("KE-010037009", ((KylinException) e).getErrorCode().getCodeString());
            }
            return null;
        }, project);
        UnitOfWork.doInTransactionWithRetry(() -> {
            try {
                val mgr = Mockito.spy(StreamingJobManager.getInstance(getTestConfig(), PROJECT));
                val meta1 = new StreamingJobMeta();
                meta1.setCurrentStatus(JobStatusEnum.STARTING);
                Mockito.when(projectService.getManager(StreamingJobManager.class, project)).thenReturn(mgr);
                Mockito.when(mgr.listAllStreamingJobMeta()).thenReturn(Collections.singletonList(meta1));
                projectService.dropProject(project);
            } catch (Exception e) {
                Assert.assertTrue(e instanceof KylinException);
                Assert.assertEquals("KE-010037009", ((KylinException) e).getErrorCode().getCodeString());
            }
            return null;
        }, project);
        UnitOfWork.doInTransactionWithRetry(() -> {
            try {
                val mgr = Mockito.spy(StreamingJobManager.getInstance(getTestConfig(), PROJECT));
                val meta1 = new StreamingJobMeta();
                meta1.setCurrentStatus(JobStatusEnum.STARTING);
                Mockito.when(projectService.getManager(StreamingJobManager.class, project)).thenReturn(mgr);
                Mockito.when(mgr.listAllStreamingJobMeta()).thenReturn(Collections.singletonList(meta1));
                projectService.dropProject(project);
            } catch (Exception e) {
                Assert.assertTrue(e instanceof KylinException);
                Assert.assertEquals("KE-010037009", ((KylinException) e).getErrorCode().getCodeString());
            }
            return null;
        }, project);
        UnitOfWork.doInTransactionWithRetry(() -> {
            val mgr = Mockito.spy(StreamingJobManager.getInstance(getTestConfig(), PROJECT));
            val meta1 = new StreamingJobMeta();
            meta1.setCurrentStatus(JobStatusEnum.STOPPED);
            Mockito.when(projectService.getManager(StreamingJobManager.class, project)).thenReturn(mgr);
            Mockito.when(mgr.listAllStreamingJobMeta()).thenReturn(Collections.singletonList(meta1));
            projectService.dropProject(project);
            return null;
        }, project);
    }

    @Test
    public void testDropProjectWithAllJobsBeenKilled() {
        KylinConfig.getInstanceFromEnv().setMetadataUrl(
                "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,DATABASE_TO_UPPER=FALSE,username=sa,password=");
        val project = "project13";
        ProjectInstance projectInstance = new ProjectInstance();
        projectInstance.setName(project);
        UnitOfWork.doInTransactionWithRetry(() -> {
            projectService.createProject(project, projectInstance);
            return null;
        }, project);

        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(project);
        scheduler.init(new JobEngineConfig(getTestConfig()));
        Assert.assertTrue(scheduler.hasStarted());
        NExecutableManager jobMgr = NExecutableManager.getInstance(getTestConfig(), project);

        val job1 = new DefaultChainedExecutable();
        job1.setProject(project);
        val task1 = new ShellExecutable();
        job1.addTask(task1);
        jobMgr.addJob(job1);

        jobMgr.updateJobOutput(job1.getId(), ExecutableState.DISCARDED, null, null, null);

        UnitOfWork.doInTransactionWithRetry(() -> {
            projectService.dropProject(project);
            return null;
        }, project);
        val prjManager = NProjectManager.getInstance(getTestConfig());
        Assert.assertNull(prjManager.getProject(project));
        Assert.assertNull(NDefaultScheduler.getInstanceByProject(project));
    }

    @Test
    public void testDropProjectWithoutAllJobsBeenKilled() {
        KylinConfig.getInstanceFromEnv().setMetadataUrl(
                "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,DATABASE_TO_UPPER=FALSE,username=sa,password=");
        val project = "project13";
        ProjectInstance projectInstance = new ProjectInstance();
        projectInstance.setName(project);
        UnitOfWork.doInTransactionWithRetry(() -> {
            projectService.createProject(project, projectInstance);
            return null;
        }, project);

        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(project);
        scheduler.init(new JobEngineConfig(getTestConfig()));
        Assert.assertTrue(scheduler.hasStarted());
        NExecutableManager jobMgr = NExecutableManager.getInstance(getTestConfig(), project);

        val job1 = new DefaultChainedExecutable();
        job1.setProject(project);
        val task1 = new ShellExecutable();
        job1.addTask(task1);
        jobMgr.addJob(job1);

        val job2 = new DefaultChainedExecutable();
        job2.setProject(project);
        val task2 = new ShellExecutable();
        job2.addTask(task2);
        jobMgr.addJob(job2);

        val job3 = new DefaultChainedExecutable();
        job3.setProject(project);
        val task3 = new ShellExecutable();
        job3.addTask(task3);
        jobMgr.addJob(job3);

        jobMgr.updateJobOutput(job2.getId(), ExecutableState.RUNNING, null, null, null);
        jobMgr.updateJobOutput(job3.getId(), ExecutableState.PAUSED, null, null, null);

        Assert.assertThrows(KylinException.class, () -> projectService.dropProject(project));
        val prjManager = NProjectManager.getInstance(getTestConfig());
        Assert.assertNotNull(prjManager.getProject(project));
        Assert.assertNotNull(NDefaultScheduler.getInstanceByProject(project));
    }

    @Test
    public void testClearManagerCache() throws Exception {
        val config = getTestConfig();
        val modelManager = NDataModelManager.getInstance(config, "default");
        ConcurrentHashMap<Class, Object> managersCache = getInstances();
        ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>> managersByPrjCache = getInstanceByProject();

        Assert.assertTrue(managersByPrjCache.containsKey(NDataModelManager.class));
        Assert.assertTrue(managersByPrjCache.get(NDataModelManager.class).containsKey("default"));

        Assert.assertTrue(managersCache.containsKey(NProjectManager.class));
        projectService.clearManagerCache("default");

        managersCache = getInstances();
        managersByPrjCache = getInstanceByProject();
        //cleared
        Assert.assertTrue(!managersCache.containsKey(NProjectManager.class));
        Assert.assertTrue(!managersByPrjCache.get(NDataModelManager.class).containsKey("default"));

    }

    @Test
    public void testSetDataSourceType() {
        projectService.setDataSourceType("default", "11");
        val prjMgr = NProjectManager.getInstance(getTestConfig());
        val prj = prjMgr.getProject("default");
        Assert.assertEquals(11, prj.getSourceType());
    }

    @Test
    public void testUpdateGarbageCleanupConfig() {
        val request = new GarbageCleanUpConfigRequest();
        request.setFrequencyTimeWindow(GarbageCleanUpConfigRequest.FrequencyTimeWindowEnum.WEEK);
        request.setLowFrequencyThreshold(12L);
        projectService.updateGarbageCleanupConfig("default", request);
        val prjMgr = NProjectManager.getInstance(getTestConfig());
        val prj = prjMgr.getProject("default");
        Assert.assertEquals(7, prj.getConfig().getFrequencyTimeWindowInDays());
        Assert.assertEquals(12, prj.getConfig().getLowFrequencyThreshold());
    }

    private void updateProject() {
        val segmentConfigRequest = new SegmentConfigRequest();
        segmentConfigRequest.setAutoMergeEnabled(false);
        segmentConfigRequest.setAutoMergeTimeRanges(Arrays.asList(AutoMergeTimeEnum.YEAR));
        projectService.updateSegmentConfig(PROJECT, segmentConfigRequest);

        val jobNotificationConfigRequest = new JobNotificationConfigRequest();
        jobNotificationConfigRequest.setDataLoadEmptyNotificationEnabled(true);
        jobNotificationConfigRequest.setJobErrorNotificationEnabled(true);
        jobNotificationConfigRequest.setJobNotificationEmails(
                Lists.newArrayList("user1@kyligence.io", "user2@kyligence.io", "user2@kyligence.io"));
        projectService.updateJobNotificationConfig(PROJECT, jobNotificationConfigRequest);

        projectService.updateQueryAccelerateThresholdConfig(PROJECT, 30, false);

        val request = new GarbageCleanUpConfigRequest();
        request.setFrequencyTimeWindow(GarbageCleanUpConfigRequest.FrequencyTimeWindowEnum.WEEK);
        request.setLowFrequencyThreshold(12L);
        projectService.updateGarbageCleanupConfig("default", request);
    }

    @Test
    public void testResetProjectConfig() {
        updateProject();
        var response = projectService.getProjectConfig(PROJECT);
        Assert.assertEquals(2, response.getJobNotificationEmails().size());
        Assert.assertTrue(response.isJobErrorNotificationEnabled());
        Assert.assertTrue(response.isDataLoadEmptyNotificationEnabled());

        response = projectService.resetProjectConfig(PROJECT, "job_notification_config");
        Assert.assertEquals(0, response.getJobNotificationEmails().size());
        Assert.assertFalse(response.isJobErrorNotificationEnabled());
        Assert.assertFalse(response.isDataLoadEmptyNotificationEnabled());

        Assert.assertFalse(response.isFavoriteQueryTipsEnabled());
        Assert.assertEquals(30, response.getFavoriteQueryThreshold());
        Assert.assertEquals(GarbageCleanUpConfigRequest.FrequencyTimeWindowEnum.WEEK.name(),
                response.getFrequencyTimeWindow());
        Assert.assertEquals(12, response.getLowFrequencyThreshold());
        Assert.assertFalse(response.isAutoMergeEnabled());

        response = projectService.resetProjectConfig(PROJECT, "query_accelerate_threshold");
        Assert.assertTrue(response.isFavoriteQueryTipsEnabled());
        Assert.assertEquals(20, response.getFavoriteQueryThreshold());

        response = projectService.resetProjectConfig(PROJECT, "garbage_cleanup_config");
        Assert.assertEquals(GarbageCleanUpConfigRequest.FrequencyTimeWindowEnum.MONTH.name(),
                response.getFrequencyTimeWindow());
        Assert.assertEquals(5, response.getLowFrequencyThreshold());

        response = projectService.resetProjectConfig(PROJECT, "segment_config");
        Assert.assertFalse(response.isAutoMergeEnabled());
        Assert.assertEquals(4, response.getAutoMergeTimeRanges().size());

        response = projectService.resetProjectConfig(PROJECT, "storage_quota_config");
        Assert.assertEquals(10995116277760L, response.getStorageQuotaSize());
    }

    @Test
    public void testUpdateYarnQueue() throws Exception {
        final String updateTo = "q.queue";
        Assert.assertEquals("default", projectService.getProjectConfig(PROJECT).getYarnQueue());
        projectService.updateYarnQueue(PROJECT, updateTo);
        Assert.assertEquals(updateTo, projectService.getProjectConfig(PROJECT).getYarnQueue());
        Assert.assertEquals(updateTo, NProjectManager.getInstance(getTestConfig()).getProject(PROJECT).getConfig()
                .getOptional("kylin.engine.spark-conf.spark.yarn.queue", ""));
    }

    @Test
    public void testCreateProjectComputedColumnConfig() throws Exception {
        // auto
        {
            getTestConfig().setProperty("kylin.metadata.semi-automatic-mode", "true");
            ProjectInstance projectInstance = new ProjectInstance();
            projectInstance.setName("project11");
            UnitOfWork.doInTransactionWithRetry(() -> {
                projectService.createProject(projectInstance.getName(), projectInstance);
                return null;
            }, projectInstance.getName());
            ProjectInstance projectInstance2 = projectManager.getProject("project11");
            Assert.assertNotNull(projectInstance2);
            Assert.assertTrue(projectInstance2.getConfig().exposeComputedColumn());
            Assert.assertTrue(projectInstance2.isSemiAutoMode());
            projectManager.dropProject("project11");
        }

        // manual
        {
            getTestConfig().setProperty("kylin.metadata.semi-automatic-mode", "false");
            ProjectInstance projectInstance = new ProjectInstance();
            projectInstance.setName("project11");
            UnitOfWork.doInTransactionWithRetry(() -> {
                projectService.createProject(projectInstance.getName(), projectInstance);
                return null;
            }, projectInstance.getName());
            ProjectInstance projectInstance2 = projectManager.getProject("project11");
            Assert.assertNotNull(projectInstance2);
            Assert.assertTrue(projectInstance2.getConfig().exposeComputedColumn());
            Assert.assertTrue(projectInstance2.isExpertMode());
            projectManager.dropProject("project11");
        }
    }

    @Test
    public void testUpdateProjectOwner() throws IOException {
        String project = "default";
        String owner = "test";

        // normal case
        Set<String> projectAdminUsers1 = Sets.newHashSet();
        projectAdminUsers1.add("test");
        Mockito.doReturn(projectAdminUsers1).when(accessService).getProjectAdminUsers(project);

        OwnerChangeRequest ownerChangeRequest1 = new OwnerChangeRequest();
        ownerChangeRequest1.setOwner(owner);

        projectService.updateProjectOwner(project, ownerChangeRequest1);
        ProjectInstance projectInstance = projectManager.getProject(project);
        Assert.assertEquals(owner, projectInstance.getOwner());

        // user not exists
        ownerChangeRequest1.setOwner("nonUser");
        thrown.expectMessage(
                "This user can’t be set as the project’s owner. Please select system admin, or the admin of this project.");
        projectService.updateProjectOwner(project, ownerChangeRequest1);

        // empty admin users, throw exception
        Set<String> projectAdminUsers = Sets.newHashSet();
        Mockito.doReturn(projectAdminUsers).when(accessService).getProjectAdminUsers(project);

        OwnerChangeRequest ownerChangeRequest2 = new OwnerChangeRequest();
        ownerChangeRequest2.setOwner(owner);

        thrown.expectMessage(
                "This user can’t be set as the project’s owner. Please select system admin, or the admin of this project.");
        projectService.updateProjectOwner(project, ownerChangeRequest2);
    }

    @Test
    public void testUpdateJdbcConfig() throws Exception {
        ProjectInstance projectInstance = new ProjectInstance();
        projectInstance.setName(PROJECT_JDBC);
        UnitOfWork.doInTransactionWithRetry(() -> {
            projectService.createProject(projectInstance.getName(), projectInstance);
            return null;
        }, projectInstance.getName());

        JdbcRequest jdbcRequest = new JdbcRequest();
        jdbcRequest.setAdaptor("org.apache.kylin.sdk.datasource.adaptor.H2Adaptor");
        jdbcRequest.setDialect("h2");
        jdbcRequest.setDriver("org.h2.Driver");
        jdbcRequest.setPushdownClass("org.apache.kylin.sdk.datasource.PushDownRunnerSDKImpl");
        jdbcRequest.setSourceConnector("org.apache.kylin.source.jdbc.DefaultSourceConnector");
        jdbcRequest.setUrl("jdbc:h2:mem:db");
        jdbcRequest.setPass("kylin");
        projectService.updateJdbcConfig(PROJECT_JDBC, jdbcRequest);

        ProjectInstance project = NProjectManager.getInstance(getTestConfig()).getProject(PROJECT_JDBC);
        Assert.assertEquals(ISourceAware.ID_JDBC, project.getSourceType());
        Assert.assertEquals("org.apache.kylin.sdk.datasource.PushDownRunnerSDKImpl",
                project.getOverrideKylinProps().get("kylin.query.pushdown.runner-class-name"));
        Assert.assertEquals("org.apache.kylin.sdk.datasource.PushDownRunnerSDKImpl",
                project.getOverrideKylinProps().get("kylin.query.pushdown.partition-check.runner-class-name"));
        Assert.assertEquals("org.apache.kylin.source.jdbc.DefaultSourceConnector",
                project.getOverrideKylinProps().get("kylin.source.jdbc.connector-class-name"));
        Assert.assertEquals("ENC('YeqVr9MakSFbgxEec9sBwg==')",
                project.getOverrideKylinProps().get("kylin.source.jdbc.pass"));

        Mockito.doReturn(Mockito.mock(UserDetails.class)).when(userService).loadUserByUsername(Mockito.anyString());
        Mockito.doReturn(true).when(userService).isGlobalAdmin(Mockito.any(UserDetails.class));
        List<UserProjectPermissionResponse> projectInstances = projectService
                .getProjectsFilterByExactMatchAndPermissionWrapperUserPermission(PROJECT_JDBC, true,
                        AclPermissionEnum.READ);
        Assert.assertEquals(1, projectInstances.size());
        Assert.assertEquals(HIDDEN_VALUE,
                projectInstances.get(0).getProject().getOverrideKylinProps().get("kylin.source.jdbc.pass"));
    }

    @Test
    public void testUpdateJdbcInfo() {
        ProjectInstance projectInstance = new ProjectInstance();
        projectInstance.setName(PROJECT_JDBC);
        UnitOfWork.doInTransactionWithRetry(() -> {
            projectService.createProject(projectInstance.getName(), projectInstance);
            return null;
        }, projectInstance.getName());

        JdbcSourceInfoRequest jdbcSourceInfoRequest = new JdbcSourceInfoRequest();
        jdbcSourceInfoRequest.setJdbcSourceEnable(true);
        jdbcSourceInfoRequest.setJdbcSourceName("h2");
        jdbcSourceInfoRequest.setJdbcSourceDriver("org.h2.Driver");
        jdbcSourceInfoRequest.setJdbcSourceConnectionUrl("jdbc:h2:mem:db");
        jdbcSourceInfoRequest.setJdbcSourceUser("kylin");
        jdbcSourceInfoRequest.setJdbcSourcePass("kylin");
        projectService.updateJdbcInfo(PROJECT_JDBC, jdbcSourceInfoRequest);

        ProjectInstance project = NProjectManager.getInstance(getTestConfig()).getProject(PROJECT_JDBC);
        Assert.assertEquals("ENC('YeqVr9MakSFbgxEec9sBwg==')",
                project.getOverrideKylinProps().get(KYLIN_SOURCE_JDBC_PASS_KEY));
        jdbcSourceInfoRequest = new JdbcSourceInfoRequest();
        jdbcSourceInfoRequest.setJdbcSourceEnable(true);
        jdbcSourceInfoRequest.setJdbcSourceName("h2");
        jdbcSourceInfoRequest.setJdbcSourceDriver("com.mysql.jdbc.driver");
        JdbcSourceInfoRequest finalJdbcSourceInfoRequest = jdbcSourceInfoRequest;
        Assert.assertThrows(KylinException.class,
                () -> projectService.updateJdbcInfo(PROJECT_JDBC, finalJdbcSourceInfoRequest));

        project = NProjectManager.getInstance(getTestConfig()).getProject(PROJECT_JDBC);
        Assert.assertEquals("org.h2.Driver", project.getOverrideKylinProps().get(KYLIN_SOURCE_JDBC_DRIVER_KEY));
        jdbcSourceInfoRequest = new JdbcSourceInfoRequest();
        jdbcSourceInfoRequest.setJdbcSourceEnable(false);
        jdbcSourceInfoRequest.setJdbcSourcePass("test");
        projectService.updateJdbcInfo(PROJECT_JDBC, jdbcSourceInfoRequest);

        project = NProjectManager.getInstance(getTestConfig()).getProject(PROJECT_JDBC);
        Assert.assertEquals(EncryptUtil.encryptWithPrefix("test"),
                project.getOverrideKylinProps().get(KYLIN_SOURCE_JDBC_PASS_KEY));
    }

    @Test(expected = KylinException.class)
    public void testDropProjectFailed() throws IOException {
        val project = "default";
        MockSecondStorage.mock(project, new ArrayList<>(), this);
        projectService.dropProject(project);
    }

    @Test
    public void testGenerateTempKeytab() {
        Assert.assertThrows(KylinException.class, () -> projectService.generateTempKeytab(null, null));
        MultipartFile multipartFile = new MockMultipartFile("234", new byte[] {});
        Assert.assertThrows(KylinException.class, () -> projectService.generateTempKeytab("test", multipartFile));
    }

}

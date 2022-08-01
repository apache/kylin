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

import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_ACTION_ILLEGAL;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_STATUS_ILLEGAL;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_UPDATE_STATUS_FAILED;
import static org.apache.kylin.job.constant.JobStatusEnum.SKIP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.dao.ExecutableOutputPO;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.dao.NExecutableDao;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.BaseTestExecutable;
import org.apache.kylin.job.execution.ChainedExecutable;
import org.apache.kylin.job.execution.ChainedStageExecutable;
import org.apache.kylin.job.execution.DefaultOutput;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.FiveSecondSucceedTestExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.execution.StageBase;
import org.apache.kylin.job.execution.SucceedChainedTestExecutable;
import org.apache.kylin.job.execution.SucceedTestExecutable;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.engine.spark.job.NSparkCubingJob;
import org.apache.kylin.engine.spark.job.NSparkExecutable;
import org.apache.kylin.engine.spark.job.NSparkSnapshotJob;
import org.apache.kylin.engine.spark.job.NTableSamplingJob;
import org.apache.kylin.engine.spark.job.step.NStageForBuild;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.FusionModel;
import org.apache.kylin.metadata.model.FusionModelManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.rest.request.JobFilter;
import org.apache.kylin.rest.request.JobUpdateRequest;
import org.apache.kylin.rest.response.ExecutableResponse;
import org.apache.kylin.rest.response.ExecutableStepResponse;
import org.apache.kylin.rest.response.JobStatisticsResponse;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AssignableTypeFilter;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import lombok.val;
import lombok.var;

public class JobServiceTest extends NLocalFileMetadataTestCase {

    @InjectMocks
    private final JobService jobService = Mockito.spy(new JobService());

    @Mock
    private final ModelService modelService = Mockito.spy(ModelService.class);

    @Mock
    private final NExecutableDao executableDao = Mockito.mock(NExecutableDao.class);

    @Mock
    private final TableExtService tableExtService = Mockito.spy(TableExtService.class);

    @Mock
    private final AclUtil aclUtil = Mockito.spy(AclUtil.class);

    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    private ProjectService projectService = Mockito.spy(ProjectService.class);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void setup() {
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        createTestMetadata();
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(jobService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(jobService, "projectService", projectService);
        ReflectionTestUtils.setField(jobService, "modelService", modelService);
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    private String getProject() {
        return "default";
    }

    @Test
    public void testCreateInstanceFromJobByReflection() throws Exception {
        ClassPathScanningCandidateComponentProvider provider = new ClassPathScanningCandidateComponentProvider(false);
        provider.addIncludeFilter(new AssignableTypeFilter(AbstractExecutable.class));

        Set<BeanDefinition> components_kylin = provider.findCandidateComponents("org.apache.kylin");
        Set<BeanDefinition> components_kap = provider.findCandidateComponents("io.kyligence.kap");
        Set<BeanDefinition> components = Sets.newHashSet(components_kylin);
        components.addAll(components_kap);
        for (BeanDefinition component : components) {
            final String beanClassName = component.getBeanClassName();
            Class<? extends AbstractExecutable> clazz = ClassUtil.forName(beanClassName, AbstractExecutable.class);
            // no construction method to create a random number ID
            Constructor<? extends AbstractExecutable> constructor = clazz.getConstructor(Object.class);
            AbstractExecutable result = constructor.newInstance(new Object());
            if (org.apache.commons.lang3.StringUtils.equals(result.getId(), null)) {
                Assert.assertNull(result.getId());
            } else {
                Assert.assertTrue(org.apache.commons.lang3.StringUtils.endsWith(result.getId(), "null"));
            }
        }
    }

    @Test
    public void testListJobs() throws Exception {

        val modelManager = Mockito.mock(NDataModelManager.class);

        Mockito.when(modelService.getManager(NDataModelManager.class, "default")).thenReturn(modelManager);
        NDataModel nDataModel = Mockito.mock(NDataModel.class);
        Mockito.when(modelManager.getDataModelDesc(Mockito.anyString())).thenReturn(nDataModel);

        NExecutableManager executableManager = Mockito.spy(NExecutableManager.getInstance(getTestConfig(), "default"));
        Mockito.when(jobService.getManager(NExecutableManager.class, "default")).thenReturn(executableManager);
        val mockJobs = mockDetailJobs(false);
        Mockito.when(executableManager.getAllJobs(Mockito.anyLong(), Mockito.anyLong())).thenReturn(mockJobs);
        for (ExecutablePO po : mockJobs) {
            AbstractExecutable exe = executableManager.fromPO(po);
            Mockito.when(executableManager.getJob(po.getId())).thenReturn(exe);
        }
        getTestConfig().setProperty("kylin.streaming.enabled", "false");
        // test size
        List<String> jobNames = Lists.newArrayList();
        JobFilter jobFilter = new JobFilter(Lists.newArrayList(), jobNames, 4, "", "", "default", "", true);
        List<ExecutableResponse> jobs = jobService.listJobs(jobFilter);
        Assert.assertEquals(3, jobs.size());
        jobService.addOldParams(jobs);

        jobFilter.setTimeFilter(0);
        jobNames.add("sparkjob1");
        jobFilter.setJobNames(jobNames);
        List<ExecutableResponse> jobs2 = jobService.listJobs(jobFilter);
        Assert.assertEquals(1, jobs2.size());

        jobFilter.setSubject("model1");
        jobNames.remove(0);
        jobFilter.setJobNames(jobNames);
        jobFilter.setTimeFilter(2);
        List<ExecutableResponse> jobs3 = jobService.listJobs(jobFilter);
        Assert.assertEquals(1, jobs3.size());

        jobFilter.setSubject("");
        jobFilter.setStatuses(Lists.newArrayList("NEW"));
        jobFilter.setTimeFilter(1);
        List<ExecutableResponse> jobs4 = jobService.listJobs(jobFilter);
        Assert.assertEquals(2, jobs4.size());

        jobFilter.setSubject("");
        jobFilter.setStatuses(Lists.newArrayList("NEW", "FINISHED"));
        jobFilter.setTimeFilter(1);
        jobs4 = jobService.listJobs(jobFilter);
        Assert.assertEquals(3, jobs4.size());

        jobFilter.setStatuses(Lists.newArrayList());
        jobFilter.setTimeFilter(3);

        jobFilter.setSortBy("job_name");
        List<ExecutableResponse> jobs5 = jobService.listJobs(jobFilter);
        Assert.assertTrue(jobs5.size() == 3 && jobs5.get(0).getJobName().equals("sparkjob3"));

        jobFilter.setTimeFilter(4);
        jobFilter.setReverse(false);
        List<ExecutableResponse> jobs6 = jobService.listJobs(jobFilter);
        Assert.assertTrue(jobs6.size() == 3 && jobs6.get(0).getJobName().equals("sparkjob1"));

        //        jobFilter.setSortBy("duration");
        //        jobFilter.setReverse(true);
        //        List<ExecutableResponse> jobs7 = jobService.listJobs(jobFilter);
        //        Assert.assertTrue(jobs7.size() == 3 && jobs7.get(0).getJobName().equals("sparkjob3"));

        jobFilter.setSortBy("create_time");
        jobFilter.setReverse(true);
        List<ExecutableResponse> jobs8 = jobService.listJobs(jobFilter);
        Assert.assertTrue(jobs8.size() == 3 && jobs8.get(0).getJobName().equals("sparkjob3"));

        jobFilter.setReverse(false);
        jobFilter.setStatuses(Lists.newArrayList());
        jobFilter.setSortBy("");
        List<ExecutableResponse> jobs10 = jobService.listJobs(jobFilter);
        Assert.assertEquals(3, jobs10.size());

        jobFilter.setSortBy("job_status");
        List<ExecutableResponse> jobs11 = jobService.listJobs(jobFilter);
        Assert.assertTrue(jobs11.size() == 3 && jobs11.get(0).getJobName().equals("sparkjob1"));

        jobFilter.setSortBy("create_time");
        List<ExecutableResponse> jobs12 = jobService.listJobs(jobFilter);
        Assert.assertTrue(jobs12.size() == 3 && jobs12.get(0).getJobName().equals("sparkjob1"));

        DataResult<List<ExecutableResponse>> jobs13 = jobService.listJobs(jobFilter, 0, 10);
        Assert.assertEquals(3, jobs13.getValue().size());

        String jobId = jobs13.getValue().get(0).getId();
        for (ExecutablePO job : mockJobs) {
            job.setJobType(JobTypeEnum.TABLE_SAMPLING);
        }
        jobFilter.setKey(jobId);
        DataResult<List<ExecutableResponse>> jobs14 = jobService.listJobs(jobFilter, 0, 10);
        Assert.assertTrue(jobs14.getValue().size() == 1 && jobs14.getValue().get(0).getId().equals(jobId));

        jobFilter.setStatuses(Lists.newArrayList());
        DataResult<List<ExecutableResponse>> jobs15 = jobService.listJobs(jobFilter, 0, 10);
        assertEquals(1, jobs15.getValue().size());

        jobFilter.setStatuses(Lists.newArrayList("NEW"));
        DataResult<List<ExecutableResponse>> jobs16 = jobService.listJobs(jobFilter, 0, 10);
        assertEquals(0, jobs16.getValue().size());

    }

    @Test
    public void testFilterJob() throws Exception {
        NExecutableManager executableManager = NExecutableManager.getInstance(getTestConfig(), "default");
        Mockito.when(jobService.getManager(NExecutableManager.class, "default")).thenReturn(executableManager);
        ReflectionTestUtils.setField(executableManager, "executableDao", executableDao);
        val mockJobs = mockDetailJobs(true);
        Mockito.when(executableDao.getJobs(Mockito.anyLong(), Mockito.anyLong())).thenReturn(mockJobs);
        {
            List<String> jobNames = Lists.newArrayList();
            JobFilter jobFilter = new JobFilter(Lists.newArrayList(), jobNames, 0, "", "", "default", "total_duration",
                    true);
            List<ExecutableResponse> jobs = jobService.listJobs(jobFilter);

            val totalDurationArrays = jobs.stream().map(ExecutableResponse::getTotalDuration)
                    .collect(Collectors.toList());
            List<Long> copyDurationList = new ArrayList<>(totalDurationArrays);
            copyDurationList.sort(Collections.reverseOrder());
            Assert.assertEquals(3, copyDurationList.size());
            Assert.assertEquals(totalDurationArrays, copyDurationList);
        }

        for (int i = 0; i < 3; i++) {
            if (i < 2) {
                mockJobs.get(i).setJobType(JobTypeEnum.SECOND_STORAGE_NODE_CLEAN);
            } else {
                mockJobs.get(i).setJobType(JobTypeEnum.TABLE_SAMPLING);
            }
        }
        List<String> jobNames = Lists.newArrayList();
        JobFilter jobFilter = new JobFilter(Lists.newArrayList(), jobNames, 0, "", "default", "default", "", false);
        List<ExecutableResponse> jobs = jobService.listJobs(jobFilter);
        Assert.assertEquals(2, jobs.size());
    }

    private List<ProjectInstance> mockProjects() {
        ProjectInstance defaultProject = new ProjectInstance();
        defaultProject.setName("default");
        defaultProject.setMvcc(0);

        ProjectInstance defaultProject1 = new ProjectInstance();
        defaultProject1.setName("default1");
        defaultProject1.setMvcc(0);

        return Lists.newArrayList(defaultProject, defaultProject1);
    }

    private List<AbstractExecutable> mockJobs1(NExecutableManager executableManager) throws Exception {
        NExecutableManager manager = Mockito.spy(NExecutableManager.getInstance(getTestConfig(), "default1"));
        ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>> managersByPrjCache = getInstanceByProject();
        managersByPrjCache.get(NExecutableManager.class).put(getProject(), manager);
        List<AbstractExecutable> jobs = new ArrayList<>();
        SucceedChainedTestExecutable job1 = new SucceedChainedTestExecutable();
        job1.setProject("default1");
        job1.setName("sparkjob22");
        job1.setTargetSubject("model22");
        jobs.add(job1);
        mockExecutablePOJobs(jobs, executableManager);
        Mockito.when(manager.getCreateTime(job1.getId())).thenReturn(1560324102100L);

        return jobs;
    }

    @Test
    public void testListAllJobs() throws Exception {
        Mockito.doReturn(mockProjects()).when(jobService).getReadableProjects();

        NExecutableManager executableManager = Mockito.mock(NExecutableManager.class);
        Mockito.when(jobService.getManager(NExecutableManager.class, "default")).thenReturn(executableManager);
        val mockJobs = mockJobs(executableManager);
        Mockito.when(executableManager.getAllExecutables(Mockito.anyLong(), Mockito.anyLong())).thenReturn(mockJobs);

        NExecutableManager executableManager1 = Mockito.mock(NExecutableManager.class);
        Mockito.when(jobService.getManager(NExecutableManager.class, "default1")).thenReturn(executableManager1);
        val mockJobs1 = mockJobs1(executableManager1);
        Mockito.when(executableManager1.getAllExecutables(Mockito.anyLong(), Mockito.anyLong())).thenReturn(mockJobs1);

        List<String> jobNames = Lists.newArrayList();
        JobFilter jobFilter = new JobFilter(Lists.newArrayList(), jobNames, 4, "", "", "default", "", true);
        List<ExecutableResponse> jobs = jobService.listGlobalJobs(jobFilter, 0, 10).getValue();
        Assert.assertEquals(4, jobs.size());
        Assert.assertEquals("default1", jobs.get(3).getProject());
    }

    private void addSegment(AbstractExecutable job) {
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
    }

    @Test
    public void testJobStepRatio() {
        val project = "default";
        NExecutableManager manager = NExecutableManager.getInstance(jobService.getConfig(), project);
        SucceedChainedTestExecutable executable = new SucceedChainedTestExecutable();
        executable.setProject(project);
        addSegment(executable);
        FiveSecondSucceedTestExecutable task = new FiveSecondSucceedTestExecutable();
        task.setProject(project);
        addSegment(task);
        executable.addTask(task);
        manager.addJob(executable);
        manager.updateJobOutput(executable.getId(), ExecutableState.PAUSED, null, null, null);
        manager.updateJobOutput(task.getId(), ExecutableState.RUNNING, null, null, null);
        manager.updateJobOutput(task.getId(), ExecutableState.SUCCEED, null, null, null);

        ExecutableResponse response = ExecutableResponse.create(executable);
        Assert.assertEquals(0.99F, response.getStepRatio(), 0.001);
    }

    @Test
    public void testSnapshotDataRange() {
        NSparkSnapshotJob snapshotJob = new NSparkSnapshotJob();
        snapshotJob.setProject("default");
        Map params = new HashMap<String, String>();
        params.put(NBatchConstants.P_INCREMENTAL_BUILD, "true");
        params.put(NBatchConstants.P_SELECTED_PARTITION_VALUE, "[\"1\",\"2\",\"3\"]");
        params.put(NBatchConstants.P_SELECTED_PARTITION_COL, "testCol");
        snapshotJob.setParams(params);
        ExecutableResponse response = ExecutableResponse.create(snapshotJob);

        params.put(NBatchConstants.P_INCREMENTAL_BUILD, "false");
        params.put(NBatchConstants.P_SELECTED_PARTITION_COL, "testCol");
        params.put(NBatchConstants.P_SELECTED_PARTITION_VALUE, "[\"1\",\"2\",\"3\"]");
        snapshotJob.setParams(params);
        response = ExecutableResponse.create(snapshotJob);
        assertEquals("[\"1\",\"2\",\"3\"]", response.getSnapshotDataRange());

        params.put(NBatchConstants.P_INCREMENTAL_BUILD, "false");
        params.put(NBatchConstants.P_SELECTED_PARTITION_COL, "testCol");
        params.put(NBatchConstants.P_SELECTED_PARTITION_VALUE, "[\"3\",\"2\",\"1\"]");
        snapshotJob.setParams(params);
        response = ExecutableResponse.create(snapshotJob);
        assertEquals("[\"1\",\"2\",\"3\"]", response.getSnapshotDataRange());

        params.put(NBatchConstants.P_INCREMENTAL_BUILD, "false");
        params.put(NBatchConstants.P_SELECTED_PARTITION_COL, "testCol");
        params.put(NBatchConstants.P_SELECTED_PARTITION_VALUE, null);
        snapshotJob.setParams(params);
        response = ExecutableResponse.create(snapshotJob);
        assertEquals("FULL", response.getSnapshotDataRange());

        params.put(NBatchConstants.P_INCREMENTAL_BUILD, "true");
        params.put(NBatchConstants.P_SELECTED_PARTITION_COL, "testCol");
        params.put(NBatchConstants.P_SELECTED_PARTITION_VALUE, null);
        snapshotJob.setParams(params);
        response = ExecutableResponse.create(snapshotJob);
        assertEquals("INC", response.getSnapshotDataRange());

        params.put(NBatchConstants.P_INCREMENTAL_BUILD, "true");
        params.put(NBatchConstants.P_SELECTED_PARTITION_COL, null);
        params.put(NBatchConstants.P_SELECTED_PARTITION_VALUE, "[\"1\",\"2\",\"3\"]");
        snapshotJob.setParams(params);
        response = ExecutableResponse.create(snapshotJob);
        assertEquals("FULL", response.getSnapshotDataRange());
    }

    @Test
    public void testCalculateStepRatio() {
        val project = "default";
        NExecutableManager manager = NExecutableManager.getInstance(jobService.getConfig(), project);
        SucceedChainedTestExecutable executable = new SucceedChainedTestExecutable();
        executable.setProject(project);
        addSegment(executable);
        FiveSecondSucceedTestExecutable task = new FiveSecondSucceedTestExecutable();
        task.setProject(project);
        addSegment(task);
        executable.addTask(task);
        manager.addJob(executable);
        manager.updateJobOutput(executable.getId(), ExecutableState.PAUSED, null, null, null);
        manager.updateJobOutput(task.getId(), ExecutableState.RUNNING, null, null, null);
        manager.updateJobOutput(task.getId(), ExecutableState.SUCCEED, null, null, null);

        var ratio = ExecutableResponse.calculateStepRatio(executable);
        assertTrue(0.99F == ratio);
    }

    @Test
    public void testcalculateSuccessStageInTaskMapSingle() {
        String segmentId = RandomUtil.randomUUIDStr();

        val project = "default";
        NExecutableManager manager = NExecutableManager.getInstance(jobService.getConfig(), project);
        SucceedChainedTestExecutable executable = new SucceedChainedTestExecutable();
        executable.setId(RandomUtil.randomUUIDStr());
        executable.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        NSparkExecutable sparkExecutable = new NSparkExecutable();
        sparkExecutable.setParam(NBatchConstants.P_SEGMENT_IDS, segmentId);
        sparkExecutable.setParam(NBatchConstants.P_INDEX_COUNT, "10");
        sparkExecutable.setId(RandomUtil.randomUUIDStr());
        executable.addTask(sparkExecutable);

        NStageForBuild build1 = new NStageForBuild();
        NStageForBuild build2 = new NStageForBuild();
        NStageForBuild build3 = new NStageForBuild();
        final StageBase logicStep1 = (StageBase) sparkExecutable.addStage(build1);
        final StageBase logicStep2 = (StageBase) sparkExecutable.addStage(build2);
        final StageBase logicStep3 = (StageBase) sparkExecutable.addStage(build3);
        sparkExecutable.setStageMap();

        manager.addJob(executable);

        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.SUCCEED, null, "test output");
        manager.updateStageStatus(logicStep2.getId(), segmentId, ExecutableState.SKIP, null, "test output");
        manager.updateStageStatus(logicStep3.getId(), segmentId, ExecutableState.SUCCEED, null, "test output");

        var buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap();
        var successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps);
        assertTrue(3 == successLogicStep);

        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.ERROR, null, "test output", true);
        manager.updateStageStatus(logicStep2.getId(), segmentId, ExecutableState.ERROR, null, "test output", true);
        manager.updateStageStatus(logicStep3.getId(), segmentId, ExecutableState.ERROR, null, "test output", true);

        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap();
        successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps);
        assertTrue(0 == successLogicStep);

        Map<String, String> info = Maps.newHashMap();
        info.put(NBatchConstants.P_INDEX_SUCCESS_COUNT, "1");
        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.RUNNING, info, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap();
        successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps);
        assertTrue(0.1 == successLogicStep);

        info.put(NBatchConstants.P_INDEX_SUCCESS_COUNT, "8");
        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.RUNNING, info, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap();
        successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps);
        assertTrue(0.8 == successLogicStep);

        info.put(NBatchConstants.P_INDEX_SUCCESS_COUNT, "10");
        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.RUNNING, info, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap();
        successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps);
        assertTrue(1 == successLogicStep);

        info.put(NBatchConstants.P_INDEX_SUCCESS_COUNT, "12");
        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.SUCCEED, info, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap();
        successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps);
        assertTrue(1 == successLogicStep);

        manager.updateStageStatus(logicStep2.getId(), segmentId, ExecutableState.RUNNING, null, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap();
        successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps);
        assertTrue(1 == successLogicStep);

        manager.updateStageStatus(logicStep2.getId(), segmentId, ExecutableState.SUCCEED, null, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap();
        successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps);
        assertTrue(2 == successLogicStep);
    }

    @Test
    public void testcalculateSuccessStageInTaskMap() {
        String segmentId = RandomUtil.randomUUIDStr();
        String segmentIds = segmentId + "," + UUID.randomUUID();

        val project = "default";
        NExecutableManager manager = NExecutableManager.getInstance(jobService.getConfig(), project);
        SucceedChainedTestExecutable executable = new SucceedChainedTestExecutable();
        executable.setId(RandomUtil.randomUUIDStr());
        executable.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        NSparkExecutable sparkExecutable = new NSparkExecutable();
        sparkExecutable.setParam(NBatchConstants.P_SEGMENT_IDS, segmentIds);
        sparkExecutable.setParam(NBatchConstants.P_INDEX_COUNT, "10");
        sparkExecutable.setId(RandomUtil.randomUUIDStr());
        executable.addTask(sparkExecutable);

        NStageForBuild build1 = new NStageForBuild();
        NStageForBuild build2 = new NStageForBuild();
        NStageForBuild build3 = new NStageForBuild();
        final StageBase logicStep1 = (StageBase) sparkExecutable.addStage(build1);
        final StageBase logicStep2 = (StageBase) sparkExecutable.addStage(build2);
        final StageBase logicStep3 = (StageBase) sparkExecutable.addStage(build3);
        sparkExecutable.setStageMap();

        manager.addJob(executable);

        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.SUCCEED, null, "test output");
        manager.updateStageStatus(logicStep2.getId(), segmentId, ExecutableState.SKIP, null, "test output");
        manager.updateStageStatus(logicStep3.getId(), segmentId, ExecutableState.SUCCEED, null, "test output");

        var buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap();
        var successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps);
        assertTrue(1.5 == successLogicStep);

        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.ERROR, null, "test output", true);
        manager.updateStageStatus(logicStep2.getId(), segmentId, ExecutableState.ERROR, null, "test output", true);
        manager.updateStageStatus(logicStep3.getId(), segmentId, ExecutableState.ERROR, null, "test output", true);

        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap();
        successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps);
        assertTrue(0 == successLogicStep);

        Map<String, String> info = Maps.newHashMap();
        info.put(NBatchConstants.P_INDEX_SUCCESS_COUNT, "1");
        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.RUNNING, info, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap();
        successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps);
        assertTrue(0 == successLogicStep);

        info.put(NBatchConstants.P_INDEX_SUCCESS_COUNT, "10");
        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.RUNNING, info, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap();
        successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps);
        assertTrue(0 == successLogicStep);

        info.put(NBatchConstants.P_INDEX_SUCCESS_COUNT, "10");
        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.SUCCEED, info, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap();
        successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps);
        assertTrue(0.5 == successLogicStep);

        info.put(NBatchConstants.P_INDEX_SUCCESS_COUNT, "12");
        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.SUCCEED, info, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap();
        successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps);
        assertTrue(0.5 == successLogicStep);

        manager.updateStageStatus(logicStep2.getId(), segmentId, ExecutableState.RUNNING, null, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap();
        successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps);
        assertTrue(0.5 == successLogicStep);

        manager.updateStageStatus(logicStep2.getId(), segmentId, ExecutableState.SUCCEED, null, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap();
        successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps);
        assertTrue(1 == successLogicStep);
    }

    @Test
    public void testcalculateSuccessStage() {
        String segmentId = RandomUtil.randomUUIDStr();

        val project = "default";
        NExecutableManager manager = NExecutableManager.getInstance(jobService.getConfig(), project);
        SucceedChainedTestExecutable executable = new SucceedChainedTestExecutable();
        executable.setId(RandomUtil.randomUUIDStr());
        executable.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        NSparkExecutable sparkExecutable = new NSparkExecutable();
        sparkExecutable.setParam(NBatchConstants.P_SEGMENT_IDS, segmentId);
        sparkExecutable.setParam(NBatchConstants.P_INDEX_COUNT, "10");
        sparkExecutable.setId(RandomUtil.randomUUIDStr());
        executable.addTask(sparkExecutable);

        NStageForBuild build1 = new NStageForBuild();
        NStageForBuild build2 = new NStageForBuild();
        NStageForBuild build3 = new NStageForBuild();
        final StageBase logicStep1 = (StageBase) sparkExecutable.addStage(build1);
        final StageBase logicStep2 = (StageBase) sparkExecutable.addStage(build2);
        final StageBase logicStep3 = (StageBase) sparkExecutable.addStage(build3);
        sparkExecutable.setStageMap();

        manager.addJob(executable);

        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.SUCCEED, null, "test output");
        manager.updateStageStatus(logicStep2.getId(), segmentId, ExecutableState.SKIP, null, "test output");
        manager.updateStageStatus(logicStep3.getId(), segmentId, ExecutableState.SUCCEED, null, "test output");

        var buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap().get(segmentId);
        var successLogicStep = ExecutableResponse.calculateSuccessStage(sparkExecutable, segmentId, buildSteps, false);
        assertTrue(3 == successLogicStep);

        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.ERROR, null, "test output", true);
        manager.updateStageStatus(logicStep2.getId(), segmentId, ExecutableState.ERROR, null, "test output", true);
        manager.updateStageStatus(logicStep3.getId(), segmentId, ExecutableState.ERROR, null, "test output", true);

        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap().get(segmentId);
        successLogicStep = ExecutableResponse.calculateSuccessStage(sparkExecutable, segmentId, buildSteps, true);
        assertTrue(0 == successLogicStep);

        Map<String, String> info = Maps.newHashMap();
        info.put(NBatchConstants.P_INDEX_SUCCESS_COUNT, "1");
        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.RUNNING, info, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap().get(segmentId);
        successLogicStep = ExecutableResponse.calculateSuccessStage(sparkExecutable, segmentId, buildSteps, true);
        assertTrue(0.1 == successLogicStep);
        successLogicStep = ExecutableResponse.calculateSuccessStage(sparkExecutable, segmentId, buildSteps, false);
        assertTrue(0 == successLogicStep);

        info.put(NBatchConstants.P_INDEX_SUCCESS_COUNT, "8");
        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.RUNNING, info, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap().get(segmentId);
        successLogicStep = ExecutableResponse.calculateSuccessStage(sparkExecutable, segmentId, buildSteps, true);
        assertTrue(0.8 == successLogicStep);
        successLogicStep = ExecutableResponse.calculateSuccessStage(sparkExecutable, segmentId, buildSteps, false);
        assertTrue(0 == successLogicStep);

        info.put(NBatchConstants.P_INDEX_SUCCESS_COUNT, "10");
        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.RUNNING, info, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap().get(segmentId);
        successLogicStep = ExecutableResponse.calculateSuccessStage(sparkExecutable, segmentId, buildSteps, true);
        assertTrue(1 == successLogicStep);
        successLogicStep = ExecutableResponse.calculateSuccessStage(sparkExecutable, segmentId, buildSteps, false);
        assertTrue(0 == successLogicStep);

        info.put(NBatchConstants.P_INDEX_SUCCESS_COUNT, "12");
        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.SUCCEED, info, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap().get(segmentId);
        successLogicStep = ExecutableResponse.calculateSuccessStage(sparkExecutable, segmentId, buildSteps, true);
        assertTrue(1 == successLogicStep);
        successLogicStep = ExecutableResponse.calculateSuccessStage(sparkExecutable, segmentId, buildSteps, false);
        assertTrue(1 == successLogicStep);

        manager.updateStageStatus(logicStep2.getId(), segmentId, ExecutableState.RUNNING, null, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap().get(segmentId);
        successLogicStep = ExecutableResponse.calculateSuccessStage(sparkExecutable, segmentId, buildSteps, true);
        assertTrue(1 == successLogicStep);
        successLogicStep = ExecutableResponse.calculateSuccessStage(sparkExecutable, segmentId, buildSteps, false);
        assertTrue(1 == successLogicStep);

        manager.updateStageStatus(logicStep2.getId(), segmentId, ExecutableState.SUCCEED, null, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap().get(segmentId);
        successLogicStep = ExecutableResponse.calculateSuccessStage(sparkExecutable, segmentId, buildSteps, true);
        assertTrue(2 == successLogicStep);
        successLogicStep = ExecutableResponse.calculateSuccessStage(sparkExecutable, segmentId, buildSteps, false);
        assertTrue(2 == successLogicStep);
    }

    @Test
    public void testUpdateStageOutput() {
        String segmentId = RandomUtil.randomUUIDStr();
        String segmentId2 = RandomUtil.randomUUIDStr();
        String segmentIds = segmentId + "," + segmentId2;

        val project = "default";
        NExecutableManager manager = NExecutableManager.getInstance(jobService.getConfig(), project);
        SucceedChainedTestExecutable executable = new SucceedChainedTestExecutable();
        executable.setId(RandomUtil.randomUUIDStr());
        executable.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        NSparkExecutable sparkExecutable = new NSparkExecutable();
        sparkExecutable.setParam(NBatchConstants.P_SEGMENT_IDS, segmentIds);
        sparkExecutable.setId(RandomUtil.randomUUIDStr());
        executable.addTask(sparkExecutable);

        NStageForBuild build = new NStageForBuild();
        final StageBase logicStep = (StageBase) sparkExecutable.addStage(build);
        sparkExecutable.setStageMap();

        manager.addJob(executable);

        SucceedChainedTestExecutable job = ((SucceedChainedTestExecutable) manager.getJob(executable.getId()));
        long createTime = job.getCreateTime();
        assertNotEquals(0L, createTime);
        List<AbstractExecutable> result = manager.getAllExecutables();
        assertEquals(1, result.size());

        List<AbstractExecutable> tasks = job.getTasks();
        assertEquals(1, tasks.size());
        NSparkExecutable sparkExecutable1 = (NSparkExecutable) tasks.get(0);
        assertEquals(sparkExecutable.getId(), sparkExecutable1.getId());
        assertEquals(segmentIds, sparkExecutable1.getParam(NBatchConstants.P_SEGMENT_IDS));

        Map<String, List<StageBase>> tasksMap = sparkExecutable1.getStagesMap();
        assertEquals(2, tasksMap.size());
        List<StageBase> logicStepBases = tasksMap.get(segmentId);
        assertEquals(1, logicStepBases.size());
        StageBase logicStepBase = logicStepBases.get(0);
        assertEquals(logicStep.getId(), logicStepBase.getId());

        manager.updateStageStatus(logicStep.getId(), segmentId, ExecutableState.RUNNING, null, "test output");

        List<ExecutableStepResponse> jobDetail = jobService.getJobDetail(project, executable.getId());
        assertEquals(1, jobDetail.size());
        ExecutableStepResponse executableStepResponse = jobDetail.get(0);
        checkResponse(executableStepResponse, sparkExecutable.getId(), null);
        Map<String, ExecutableStepResponse.SubStages> subStages = executableStepResponse.getSegmentSubStages();
        assertEquals(2, subStages.size());
        List<ExecutableStepResponse> stages = subStages.get(segmentId).getStage();
        assertEquals(1, stages.size());
        ExecutableStepResponse logicStepResponse = stages.get(0);
        checkResponse(logicStepResponse, logicStep.getId(), JobStatusEnum.RUNNING);

        List<ExecutableStepResponse> stages2 = subStages.get(segmentId2).getStage();
        assertEquals(1, stages2.size());
        ExecutableStepResponse logicStepResponse2 = stages2.get(0);
        checkResponse(logicStepResponse2, logicStep.getId(), JobStatusEnum.PENDING);
        assertEquals(0, logicStepResponse2.getExecStartTime());
        assertTrue(logicStepResponse2.getExecStartTime() < System.currentTimeMillis());

        manager.updateStageStatus(logicStep.getId(), segmentId2, ExecutableState.RUNNING, null, "test output");

        manager.updateStageStatus(logicStep.getId(), null, ExecutableState.SUCCEED, null, "test output");

        jobDetail = jobService.getJobDetail(project, executable.getId());
        assertEquals(1, jobDetail.size());
        executableStepResponse = jobDetail.get(0);
        checkResponse(executableStepResponse, sparkExecutable.getId(), null);
        subStages = executableStepResponse.getSegmentSubStages();
        assertEquals(2, subStages.size());
        stages = subStages.get(segmentId).getStage();
        assertEquals(1, stages.size());
        logicStepResponse = stages.get(0);
        checkResponse(logicStepResponse, logicStep.getId(), JobStatusEnum.FINISHED);

        stages2 = subStages.get(segmentId2).getStage();
        assertEquals(1, stages2.size());
        logicStepResponse2 = stages2.get(0);
        checkResponse(logicStepResponse2, logicStep.getId(), JobStatusEnum.FINISHED);
    }

    private void checkResponse(ExecutableStepResponse response, String expectId, JobStatusEnum expectStatus) {
        if (expectId != null) {
            assertEquals(expectId, response.getId());
        }
        if (expectStatus != null) {
            assertEquals(expectStatus, response.getStatus());
        }
    }

    @Test
    public void updateStageOutputTaskMapEmpty() {
        String segmentId = RandomUtil.randomUUIDStr();
        String segmentId2 = RandomUtil.randomUUIDStr();
        String segmentIds = segmentId + "," + segmentId2;

        val project = "default";
        NExecutableManager manager = NExecutableManager.getInstance(jobService.getConfig(), project);
        SucceedChainedTestExecutable executable = new SucceedChainedTestExecutable();
        executable.setId(RandomUtil.randomUUIDStr());
        executable.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        NSparkExecutable sparkExecutable = new NSparkExecutable();
        sparkExecutable.setParam(NBatchConstants.P_SEGMENT_IDS, segmentIds);
        sparkExecutable.setId(RandomUtil.randomUUIDStr());
        executable.addTask(sparkExecutable);

        manager.addJob(executable);

        val outputOld = manager.getOutput(sparkExecutable.getId());
        manager.updateStageStatus(sparkExecutable.getId(), segmentId, ExecutableState.RUNNING, null, "test output");
        val outputNew = manager.getOutput(sparkExecutable.getId());
        assertEquals(outputOld.getState(), outputNew.getState());
        assertEquals(outputOld.getCreateTime(), outputNew.getCreateTime());
        assertEquals(outputOld.getEndTime(), outputNew.getEndTime());
        assertEquals(outputOld.getStartTime(), outputNew.getStartTime());
    }

    @Test
    public void testBasic() throws IOException {
        NExecutableManager manager = NExecutableManager.getInstance(jobService.getConfig(), "default");
        NDataflowManager dsMgr = NDataflowManager.getInstance(jobService.getConfig(), "default");
        SucceedChainedTestExecutable executable = new SucceedChainedTestExecutable();
        manager.addJob(executable);
        jobService.batchUpdateJobStatus(Lists.newArrayList(executable.getId()), "default", "PAUSE",
                Lists.newArrayList());
        Assert.assertEquals(ExecutableState.PAUSED, manager.getJob(executable.getId()).getStatus());
        UnitOfWork.doInTransactionWithRetry(() -> {
            jobService.batchUpdateJobStatus(Lists.newArrayList(executable.getId()), "default", "RESUME",
                    Lists.newArrayList());
            return null;
        }, "default");
        jobService.batchUpdateJobStatus(Lists.newArrayList(executable.getId()), "default", "PAUSE",
                Lists.newArrayList());
        Assert.assertEquals(ExecutableState.PAUSED, manager.getJob(executable.getId()).getStatus());
        UnitOfWork.doInTransactionWithRetry(() -> {
            jobService.batchUpdateJobStatus(Lists.newArrayList(executable.getId()), "default", "RESUME",
                    Lists.newArrayList("STOPPED"));
            return null;
        }, "default");
        Assert.assertEquals(ExecutableState.READY, manager.getJob(executable.getId()).getStatus());
        UnitOfWork.doInTransactionWithRetry(() -> {
            jobService.batchUpdateJobStatus(Lists.newArrayList(executable.getId()), "default", "DISCARD",
                    Lists.newArrayList());
            return null;
        }, "default");
        Assert.assertEquals(ExecutableState.DISCARDED, manager.getJob(executable.getId()).getStatus());
        Assert.assertNull(dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa").getSegments().getFirstSegment());
        Mockito.doNothing().when(tableExtService).removeJobIdFromTableExt(executable.getId(), "default");
        jobService.batchDropJob("default", Lists.newArrayList(executable.getId()), Lists.newArrayList());
        List<AbstractExecutable> executables = manager.getAllExecutables();
        Assert.assertFalse(executables.contains(executable));
    }

    @Test
    public void testGlobalBasic() throws IOException {
        ProjectInstance defaultProject = new ProjectInstance();
        defaultProject.setName("default");
        defaultProject.setMvcc(0);
        Mockito.doReturn(Lists.newArrayList(defaultProject)).when(jobService).getReadableProjects();

        NExecutableManager manager = NExecutableManager.getInstance(jobService.getConfig(), "default");
        NDataflowManager dsMgr = NDataflowManager.getInstance(jobService.getConfig(), "default");
        SucceedChainedTestExecutable executable = new SucceedChainedTestExecutable();
        manager.addJob(executable);
        Mockito.when(projectService.getOwnedProjects()).thenReturn(Lists.newArrayList("default"));
        jobService.batchUpdateGlobalJobStatus(Lists.newArrayList(executable.getId()), "PAUSE", Lists.newArrayList());
        Assert.assertEquals(ExecutableState.PAUSED, manager.getJob(executable.getId()).getStatus());

        jobService.batchUpdateGlobalJobStatus(Lists.newArrayList(executable.getId()), "RESUME", Lists.newArrayList());
        jobService.batchUpdateGlobalJobStatus(Lists.newArrayList(executable.getId()), "PAUSE", Lists.newArrayList());
        Assert.assertEquals(ExecutableState.PAUSED, manager.getJob(executable.getId()).getStatus());

        jobService.batchUpdateGlobalJobStatus(Lists.newArrayList(executable.getId()), "RESUME",
                Lists.newArrayList("STOPPED"));
        Assert.assertEquals(ExecutableState.READY, manager.getJob(executable.getId()).getStatus());

        jobService.batchUpdateGlobalJobStatus(Lists.newArrayList(executable.getId()), "DISCARD", Lists.newArrayList());
        Assert.assertEquals(ExecutableState.DISCARDED, manager.getJob(executable.getId()).getStatus());

        Assert.assertNull(dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa").getSegments().getFirstSegment());

        Mockito.doNothing().when(tableExtService).removeJobIdFromTableExt(executable.getId(), "default");
        jobService.batchDropGlobalJob(Lists.newArrayList(executable.getId()), Lists.newArrayList());
        Assert.assertFalse(manager.getAllExecutables().contains(executable));
    }

    @Test
    public void testDiscardJobException() throws IOException {
        NExecutableManager manager = NExecutableManager.getInstance(jobService.getConfig(), "default");
        SucceedChainedTestExecutable executable = new SucceedChainedTestExecutable();
        executable.setProject("default");
        manager.addJob(executable);
        manager.updateJobOutput(executable.getId(), ExecutableState.RUNNING, null, null, null);
        manager.updateJobOutput(executable.getId(), ExecutableState.SUCCEED, null, null, null);
        Assert.assertEquals(ExecutableState.SUCCEED, executable.getStatus());
        thrown.expect(KylinException.class);
        thrown.expectMessage(JOB_UPDATE_STATUS_FAILED.getMsg("DISCARD", executable.getId(), ExecutableState.SUCCEED));
        jobService.batchUpdateJobStatus(Lists.newArrayList(executable.getId()), "default", "DISCARD",
                Lists.newArrayList());
    }

    @Test
    public void testUpdateException() throws IOException {
        NExecutableManager manager = NExecutableManager.getInstance(jobService.getConfig(), "default");
        SucceedChainedTestExecutable executable = new SucceedChainedTestExecutable();
        executable.setParam("test1", "test1");
        executable.setParam("test2", "test2");
        executable.setParam("test3", "test3");
        executable.setProject("default");
        executable.setName("test");
        manager.addJob(executable);
        thrown.expect(KylinException.class);
        jobService.batchUpdateJobStatus(Lists.newArrayList(executable.getId()), "default", "ROLLBACK",
                Lists.newArrayList());
    }

    @Test
    public void testGetJobDetail() {
        NExecutableManager manager = NExecutableManager.getInstance(jobService.getConfig(), "default");
        SucceedChainedTestExecutable executable = new SucceedChainedTestExecutable();
        executable.setParam("test1", "test1");
        executable.setParam("test2", "test2");
        executable.setParam("test3", "test3");
        executable.setProject("default");
        executable.setName("test");
        executable.addTask(new FiveSecondSucceedTestExecutable());
        manager.addJob(executable);
        List<ExecutableStepResponse> result = jobService.getJobDetail("default", executable.getId());
        Assert.assertEquals(1, result.size());
    }

    @Test
    public void testGetJobCreateTime() {
        NExecutableManager manager = NExecutableManager.getInstance(jobService.getConfig(), "default");
        SucceedChainedTestExecutable executable = new SucceedChainedTestExecutable();
        addSegment(executable);
        executable.setParam("test1", "test1");
        executable.setParam("test2", "test2");
        executable.setParam("test3", "test3");
        executable.setProject("default");
        executable.setName("test_create_time");
        manager.addJob(executable);
        List<String> jobNames = Lists.newArrayList();
        JobFilter jobFilter = new JobFilter(Lists.newArrayList(), jobNames, 4, "", "", "default", "", true);
        List<ExecutableResponse> jobs = jobService.listJobs(jobFilter);
        Assert.assertTrue(jobs.get(0).getCreateTime() > 0);
    }

    @Test
    public void testGetTargetSubjectAndJobType() {
        NExecutableManager manager = NExecutableManager.getInstance(jobService.getConfig(), "default");
        SucceedChainedTestExecutable job1 = new SucceedChainedTestExecutable();
        job1.setProject(getProject());
        job1.setName("mocked job");
        job1.setTargetSubject("12345678");
        final TableDesc tableDesc = NTableMetadataManager.getInstance(getTestConfig(), getProject())
                .getTableDesc("DEFAULT.TEST_KYLIN_FACT");
        NTableSamplingJob samplingJob = NTableSamplingJob.create(tableDesc, getProject(), "ADMIN", 20000);
        manager.addJob(job1);
        manager.addJob(samplingJob);
        List<String> jobNames = Lists.newArrayList();
        JobFilter jobFilter = new JobFilter(Lists.newArrayList(), jobNames, 4, "", "", "default", "", true);
        jobFilter.setSortBy("job_name");
        List<ExecutableResponse> jobs = jobService.listJobs(jobFilter);

        Assert.assertEquals("The model is deleted", jobs.get(0).getTargetSubject()); // no target model so it's null
        Assert.assertEquals("mocked job", jobs.get(0).getJobName());
        Assert.assertEquals(tableDesc.getIdentity(), jobs.get(1).getTargetSubject());
        Assert.assertEquals("TABLE_SAMPLING", jobs.get(1).getJobName());

    }

    private List<AbstractExecutable> mockJobs(NExecutableManager executableManager) throws Exception {
        NExecutableManager manager = Mockito.spy(NExecutableManager.getInstance(getTestConfig(), getProject()));
        ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>> managersByPrjCache = NLocalFileMetadataTestCase
                .getInstanceByProject();
        managersByPrjCache.get(NExecutableManager.class).put(getProject(), manager);
        List<AbstractExecutable> jobs = new ArrayList<>();
        SucceedChainedTestExecutable job1 = new SucceedChainedTestExecutable();
        job1.setProject(getProject());
        job1.setName("sparkjob1");
        job1.setTargetSubject("model1");

        SucceedChainedTestExecutable job2 = new SucceedChainedTestExecutable();
        job2.setProject(getProject());
        job2.setName("sparkjob2");
        job2.setTargetSubject("model2");

        SucceedChainedTestExecutable job3 = new SucceedChainedTestExecutable();
        job3.setProject(getProject());
        job3.setName("sparkjob3");
        job3.setTargetSubject("model3");

        jobs.add(job1);
        jobs.add(job2);
        jobs.add(job3);

        val job1Output = new DefaultOutput();
        job1Output.setState(ExecutableState.SUCCEED);
        Mockito.when(manager.getCreateTime(job1.getId())).thenReturn(1560324101000L);
        Mockito.when(manager.getCreateTime(job2.getId())).thenReturn(1560324102000L);
        Mockito.when(manager.getCreateTime(job3.getId())).thenReturn(1560324103000L);

        Mockito.when(manager.getOutput(job1.getId())).thenReturn(job1Output);

        mockExecutablePOJobs(jobs, executableManager);//id update
        Mockito.when(manager.getCreateTime(job1.getId())).thenReturn(1560324101000L);
        Mockito.when(manager.getCreateTime(job2.getId())).thenReturn(1560324102000L);
        Mockito.when(manager.getCreateTime(job3.getId())).thenReturn(1560324103000L);
        Mockito.when(manager.getOutput(job1.getId())).thenReturn(job1Output);

        return jobs;
    }

    private void mockExecutablePOJobs(List<AbstractExecutable> mockJobs, NExecutableManager executableManager) {
        NExecutableManager manager = Mockito.spy(NExecutableManager.getInstance(getTestConfig(), getProject()));
        List<ExecutablePO> jobs = new ArrayList<>();
        for (int i = 0; i < mockJobs.size(); i++) {
            AbstractExecutable executable = mockJobs.get(i);
            ExecutablePO job1 = new ExecutablePO();
            if (executable.getOutput() != null) {
                job1.getOutput().setStatus(executable.getOutput().getState().name());
            }
            job1.setCreateTime(executable.getCreateTime());
            job1.getOutput().setCreateTime(executable.getCreateTime());
            job1.getOutput().getInfo().put("applicationid", "app000");

            job1.setType("org.apache.kylin.job.execution.SucceedChainedTestExecutable");
            job1.setProject(executable.getProject());
            job1.setName(executable.getName());
            job1.setTargetModel(executable.getTargetSubject());

            jobs.add(job1);
            executable.setId(jobs.get(i).getId());
            Mockito.doReturn(executable).when(executableManager).fromPO(job1);

        }

        Mockito.when(executableManager.getAllJobs(Mockito.anyLong(), Mockito.anyLong())).thenReturn(jobs);
    }

    private List<ExecutablePO> mockDetailJobs(boolean random) throws Exception {
        List<ExecutablePO> jobs = new ArrayList<>();
        for (int i = 1; i < 4; i++) {
            jobs.add(mockExecutablePO(random, i + ""));
        }
        return jobs;
    }

    private ExecutablePO mockExecutablePO(boolean random, String name) {
        ExecutablePO mockJob = new ExecutablePO();
        mockJob.setType("org.apache.kylin.job.execution.SucceedChainedTestExecutable");
        mockJob.setProject(getProject());
        mockJob.setName("sparkjob" + name);
        mockJob.setTargetModel("model" + name);
        val jobOutput = mockJob.getOutput();
        if ("1".equals(name))
            jobOutput.setStatus(ExecutableState.SUCCEED.name());

        val startTime = getCreateTime(name);
        mockJob.setCreateTime(startTime);
        jobOutput.setCreateTime(startTime);
        jobOutput.setStartTime(startTime);
        var lastEndTime = startTime;
        List<ExecutablePO> tasks = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            val childExecutable = new ExecutablePO();
            childExecutable.setUuid(mockJob.getId() + "_0" + i);
            childExecutable.setType("org.apache.kylin.job.execution.SucceedSubTaskTestExecutable");
            childExecutable.setProject(getProject());
            val jobChildOutput = childExecutable.getOutput();
            mockOutputTime(random, lastEndTime, jobChildOutput, i);
            lastEndTime = jobChildOutput.getEndTime();
            tasks.add(childExecutable);
        }
        mockJob.setTasks(tasks);

        jobOutput.setEndTime(lastEndTime);
        Mockito.when(executableDao.getJobByUuid(eq(mockJob.getId()))).thenReturn(mockJob);
        return mockJob;
    }

    private long getCreateTime(String name) {
        switch (name) {
        case "1":
            return 1560324101000L;
        case "2":
            return 1560324102000L;
        case "3":
            return 1560324103000L;
        default:
            return 0L;
        }
    }

    private void mockOutputTime(boolean random, long baseTime, ExecutableOutputPO output, int index) {
        long createTime = baseTime + (index + 1) * 2000L;
        long startTime = createTime + (index + 1) * 2000L;
        long endTime = startTime + (index + 1) * 2000L;
        if (random) {
            val randomObj = new Random();
            Supplier<Long> randomSupplier = () -> (long) randomObj.nextInt(100);
            endTime += randomSupplier.get();
        }

        output.setStartTime(startTime);
        output.setCreateTime(createTime);
        output.setEndTime(endTime);

    }

    @Test
    public void testJobnameResponse() throws Exception {
        NExecutableManager manager = Mockito.spy(NExecutableManager.getInstance(getTestConfig(), getProject()));
        ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>> managersByPrjCache = NLocalFileMetadataTestCase
                .getInstanceByProject();
        managersByPrjCache.get(NExecutableManager.class).put(getProject(), manager);
        ExecutablePO job1 = Mockito.spy(ExecutablePO.class);
        job1.setProject(getProject());
        job1.setName("sparkjob1");
        job1.setTargetModel("model1");

        job1.setType("org.apache.kylin.job.execution.SucceedChainedTestExecutable");
        ExecutablePO subJob = new ExecutablePO();
        subJob.setType("org.apache.kylin.job.execution.SucceedChainedTestExecutable");

        subJob.getOutput().setStatus("SUCCEED");

        subJob.setUuid(job1.getId() + "_00");
        job1.setTasks(Lists.newArrayList(subJob));
        manager.addJob(job1);
        manager.addJob(subJob);

        Mockito.when(manager.getAllJobs(Mockito.anyLong(), Mockito.anyLong()))
                .thenReturn(Collections.singletonList(job1));

        JobFilter jobFilter = new JobFilter(Lists.newArrayList(), Lists.newArrayList(), 4, "", "", "default", "", true);
        List<ExecutableResponse> jobs = jobService.listJobs(jobFilter);

        Assert.assertEquals(1, jobs.size());

        ExecutableResponse executableResponse = jobs.get(0);

        Assert.assertEquals("sparkjob1", executableResponse.getJobName());

    }

    @Test
    public void testGetJobStats() throws ParseException {
        JobStatisticsResponse jobStats = jobService.getJobStats("default", Long.MIN_VALUE, Long.MAX_VALUE);
        Assert.assertEquals(0, jobStats.getCount());
        Assert.assertEquals(0, jobStats.getTotalByteSize(), 0);
        Assert.assertEquals(0, jobStats.getTotalDuration(), 0);

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd", Locale.getDefault(Locale.Category.FORMAT));
        String date = "2018-01-01";
        long startTime = format.parse(date).getTime();
        date = "2018-02-01";
        long endTime = format.parse(date).getTime();
        Map<String, Integer> jobCount = jobService.getJobCount("default", startTime, endTime, "day");
        Assert.assertEquals(32, jobCount.size());
        Assert.assertEquals(0, (int) jobCount.get("2018-01-01"));
        Assert.assertEquals(0, (int) jobCount.get("2018-02-01"));

        jobCount = jobService.getJobCount("default", startTime, endTime, "model");
        Assert.assertEquals(0, jobCount.size());

        Map<String, Double> jobDurationPerMb = jobService.getJobDurationPerByte("default", startTime, endTime, "day");
        Assert.assertEquals(32, jobDurationPerMb.size());
        Assert.assertEquals(0, jobDurationPerMb.get("2018-01-01"), 0.1);

        jobDurationPerMb = jobService.getJobDurationPerByte("default", startTime, endTime, "model");
        Assert.assertEquals(0, jobDurationPerMb.size());
    }

    @Test
    public void testGetJobOutput() {
        NExecutableManager manager = NExecutableManager.getInstance(jobService.getConfig(), "default");
        ExecutableOutputPO executableOutputPO = new ExecutableOutputPO();
        executableOutputPO.setStatus("SUCCEED");
        executableOutputPO.setContent("succeed");
        manager.updateJobOutputToHDFS(KylinConfig.getInstanceFromEnv().getJobTmpOutputStorePath("default",
                "e1ad7bb0-522e-456a-859d-2eab1df448de"), executableOutputPO);

        Assertions.assertThat(jobService.getJobOutput("default", "e1ad7bb0-522e-456a-859d-2eab1df448de"))
                .isEqualTo("succeed");
    }

    @Test
    public void testGetAllJobOutput() throws IOException {
        File file = temporaryFolder.newFile("execute_output.json." + System.currentTimeMillis() + ".log");
        for (int i = 0; i < 200; i++) {
            Files.write(file.toPath(), String.format(Locale.ROOT, "lines: %s\n", i).getBytes(Charset.defaultCharset()),
                    StandardOpenOption.APPEND);
        }

        String[] exceptLines = Files.readAllLines(file.toPath()).toArray(new String[0]);

        NExecutableManager manager = NExecutableManager.getInstance(jobService.getConfig(), "default");
        ExecutableOutputPO executableOutputPO = new ExecutableOutputPO();
        executableOutputPO.setStatus("SUCCEED");
        executableOutputPO.setContent("succeed");
        executableOutputPO.setLogPath(file.getAbsolutePath());
        manager.updateJobOutputToHDFS(KylinConfig.getInstanceFromEnv().getJobTmpOutputStorePath("default",
                "e1ad7bb0-522e-456a-859d-2eab1df448de"), executableOutputPO);

        String sampleLog = "";
        try (InputStream allJobOutput = jobService.getAllJobOutput("default", "e1ad7bb0-522e-456a-859d-2eab1df448de",
                "e1ad7bb0-522e-456a-859d-2eab1df448de");
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(allJobOutput, Charset.defaultCharset()))) {

            String line;
            StringBuilder sampleData = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                if (sampleData.length() > 0) {
                    sampleData.append('\n');
                }
                sampleData.append(line);
            }

            sampleLog = sampleData.toString();
        }
        String[] actualLines = StringUtils.splitByWholeSeparatorPreserveAllTokens(sampleLog, "\n");
        Assert.assertTrue(Arrays.deepEquals(exceptLines, actualLines));
    }

    @Test
    public void testGetJobInstance_ManageJob() throws IOException {
        String jobId = "job1";
        ExecutableResponse executableResponse = new ExecutableResponse();
        executableResponse.setId(jobId);

        AbstractExecutable job = new NSparkCubingJob();

        Mockito.doReturn(mockProjects()).when(jobService).getReadableProjects();
        NExecutableManager manager = Mockito.mock(NExecutableManager.class);
        Mockito.when(manager.getJob(jobId)).thenReturn(job);
        Mockito.doReturn(manager).when(jobService).getManager(NExecutableManager.class, "default");
        Assert.assertEquals("default", jobService.getProjectByJobId(jobId));

        Mockito.doReturn("default").when(jobService).getProjectByJobId(jobId);
        Mockito.doReturn(executableResponse).when(jobService).convert(job);
        Assert.assertEquals(jobId, jobService.getJobInstance(jobId).getId());

        Mockito.doNothing().when(jobService).updateJobStatus(jobId, "default", "RESUME");

        Assert.assertEquals(executableResponse, jobService.manageJob("default", executableResponse, "RESUME"));
    }

    @Test
    public void testCheckJobStatus() {
        jobService.checkJobStatus(Lists.newArrayList("RUNNING"));
        thrown.expect(KylinException.class);
        thrown.expectMessage(JOB_STATUS_ILLEGAL.getMsg());
        jobService.checkJobStatus("UNKNOWN");
    }

    @Test
    public void testCheckJobStatusAndAction() {
        JobUpdateRequest request = new JobUpdateRequest();
        request.setStatuses(Lists.newArrayList("RUNNING", "PENDING"));
        request.setAction("PAUSE");
        jobService.checkJobStatusAndAction(request);
        thrown.expect(KylinException.class);
        thrown.expectMessage(JOB_ACTION_ILLEGAL.getMsg("RUNNING", "DISCARD, PAUSE, RESTART"));
        jobService.checkJobStatusAndAction("RUNNING", "RESUME");
    }

    @Test
    public void testFusionModelStopBatchJob() {

        String project = "streaming_test";
        FusionModelManager mgr = FusionModelManager.getInstance(getTestConfig(), project);
        NExecutableManager manager = NExecutableManager.getInstance(jobService.getConfig(), project);

        FusionModel fusionModel = mgr.getFusionModel("b05034a8-c037-416b-aa26-9e6b4a41ee40");

        BaseTestExecutable executable = new SucceedTestExecutable();
        executable.setProject(project);
        executable.setTargetSubject(fusionModel.getBatchModel().getUuid());
        manager.addJob(executable);
        manager.updateJobOutput(executable.getId(), ExecutableState.RUNNING, null, null, null);

        // test fusion model stop batch job
        String table = "SSB.P_LINEORDER_STREAMING";
        NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(getTestConfig(), project);
        val tableDesc = tableMetadataManager.getTableDesc(table);
        UnitOfWork.doInTransactionWithRetry(() -> {
            jobService.stopBatchJob(project, tableDesc);
            return null;
        }, project);
        AbstractExecutable job = manager.getJob(executable.getId());
        Assert.assertEquals(ExecutableState.DISCARDED, job.getStatus());

        // test no fusion model
        String table2 = "SSB.DATES";
        val tableDesc2 = tableMetadataManager.getTableDesc(table2);
        UnitOfWork.doInTransactionWithRetry(() -> {
            jobService.stopBatchJob(project, tableDesc2);
            return null;
        }, project);
    }

    @Test
    public void testKillExistApplication() {
        NExecutableManager manager = NExecutableManager.getInstance(jobService.getConfig(), getProject());
        SucceedChainedTestExecutable executable = new SucceedChainedTestExecutable();
        executable.setProject(getProject());
        addSegment(executable);
        val task = new NSparkExecutable();
        task.setProject(getProject());
        addSegment(task);
        executable.addTask(task);
        manager.addJob(executable);
        jobService.killExistApplication(executable);

        jobService.killExistApplication(getProject(), executable.getId());
    }

    @Test
    public void testHistoryTrackerUrl() {
        getTestConfig().setProperty("kylin.history-server.enable", "true");
        AbstractExecutable task = new FiveSecondSucceedTestExecutable();
        task.setProject("default");
        DefaultOutput stepOutput = new DefaultOutput();
        stepOutput.setState(ExecutableState.RUNNING);
        stepOutput.setExtra(new HashMap<>());
        Map<String, String> waiteTimeMap = new HashMap<>();
        ExecutableState jobState = ExecutableState.RUNNING;
        ExecutableStepResponse result = jobService.parseToExecutableStep(task, stepOutput, waiteTimeMap, jobState);
        assert !result.getInfo().containsKey(ExecutableConstants.SPARK_HISTORY_APP_URL);
        stepOutput.getExtra().put(ExecutableConstants.YARN_APP_ID, "app-id");
        result = jobService.parseToExecutableStep(task, stepOutput, waiteTimeMap, jobState);
        assert result.getInfo().containsKey(ExecutableConstants.SPARK_HISTORY_APP_URL);
        getTestConfig().setProperty("kylin.history-server.enable", "false");
        result = jobService.parseToExecutableStep(task, stepOutput, waiteTimeMap, jobState);
        assert !result.getInfo().containsKey(ExecutableConstants.SPARK_HISTORY_APP_URL);
    }

    @Test
    public void testParseToExecutableState() {
        Assert.assertThrows(KylinException.class,
                () -> ReflectionTestUtils.invokeMethod(new JobService(), "parseToExecutableState", SKIP));
    }
}

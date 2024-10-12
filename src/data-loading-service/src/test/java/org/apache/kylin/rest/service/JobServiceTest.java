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

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.engine.spark.job.InternalTableLoadingJob;
import org.apache.kylin.engine.spark.job.NSparkSnapshotJob;
import org.apache.kylin.engine.spark.job.step.NStageForBuild;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.job.dao.ExecutableOutputPO;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ChainedExecutable;
import org.apache.kylin.job.execution.ChainedStageExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.FiveSecondSucceedTestExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NSparkExecutable;
import org.apache.kylin.job.execution.StageBase;
import org.apache.kylin.job.execution.SucceedChainedTestExecutable;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.table.InternalTableDesc;
import org.apache.kylin.metadata.table.InternalTableManager;
import org.apache.kylin.plugin.asyncprofiler.ProfilerStatus;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.response.ExecutableResponse;
import org.apache.kylin.rest.response.JobStatisticsResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.awaitility.Duration;
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
import org.springframework.context.ApplicationEvent;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.core.type.filter.AssignableTypeFilter;
import org.springframework.http.HttpHeaders;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import lombok.val;
import lombok.var;

public class JobServiceTest extends NLocalFileMetadataTestCase {

    String project = "default";
    String yarnAppId = "application_1554187389076_9296";
    String jobId = "273cf5ea-9a9b-dccb-004e-0e3b04bfb50c-c11baf56-a593-4c5f-d546-1fa86c2d54ad";
    String jobStepId = jobId + "_01";
    String startParams = "start,event=cpu";
    String dumpParams = "flamegraph";

    String sparkClusterManagerName = "org.apache.kylin.rest.service.MockClusterManager";

    @InjectMocks
    private final JobService jobService = Mockito.spy(new JobService());

    @Mock
    private final ModelService modelService = Mockito.spy(ModelService.class);

    @Mock
    private final TableExtService tableExtService = Mockito.spy(TableExtService.class);

    @Mock
    private final AclUtil aclUtil = Mockito.spy(AclUtil.class);

    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    private ProjectService projectService = Mockito.spy(ProjectService.class);

    @Mock
    private ApplicationEvent applicationEvent = Mockito.mock(ContextClosedEvent.class);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void setUp() {
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        overwriteSystemProp("kylin.engine.async-profiler-enabled", "true");
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
        Set<BeanDefinition> components = Sets.newHashSet(components_kylin);
        for (BeanDefinition component : components) {
            final String beanClassName = component.getBeanClassName();
            Class<? extends AbstractExecutable> clazz = ClassUtil.forName(beanClassName, AbstractExecutable.class);
            // no construction method to create a random number ID
            Constructor<? extends AbstractExecutable> constructor = clazz.getConstructor(Object.class);
            AbstractExecutable result = constructor.newInstance(new Object());
            if (StringUtils.equals(result.getId(), null)) {
                Assert.assertNull(result.getId());
            } else {
                Assert.assertTrue(StringUtils.endsWith(result.getId(), "null"));
            }
        }
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

    private List<AbstractExecutable> mockJobs1(ExecutableManager executableManager) throws Exception {
        ExecutableManager manager = Mockito.spy(ExecutableManager.getInstance(getTestConfig(), "default1"));
        ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>> managersByPrjCache = getInstanceByProject();
        managersByPrjCache.get(ExecutableManager.class).put(getProject(), manager);
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

    private void addSegment(AbstractExecutable job) {
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
    }

    @Test
    public void testJobStepRatio() {
        val project = "default";
        ExecutableManager manager = ExecutableManager.getInstance(jobService.getConfig(), project);
        SucceedChainedTestExecutable executable = new SucceedChainedTestExecutable();
        executable.setJobType(JobTypeEnum.INC_BUILD);
        executable.setProject(project);
        addSegment(executable);
        FiveSecondSucceedTestExecutable task = new FiveSecondSucceedTestExecutable();
        task.setProject(project);
        addSegment(task);
        executable.addTask(task);
        manager.addJob(executable);
        manager.updateJobOutput(executable.getId(), ExecutableState.PAUSED, null, null, null);
        manager.updateJobOutput(task.getId(), ExecutableState.READY, null, null, null);
        manager.updateJobOutput(task.getId(), ExecutableState.PENDING, null, null, null);
        manager.updateJobOutput(task.getId(), ExecutableState.RUNNING, null, null, null);
        manager.updateJobOutput(task.getId(), ExecutableState.SUCCEED, null, null, null);

        ExecutableResponse response = ExecutableResponse.create(manager.getJob(executable.getJobId()),
                manager.getExecutablePO(executable.getJobId()));
        Assert.assertEquals(0.99F, response.getStepRatio(), 0.001);
    }

    @Test
    public void testSnapshotDataRange() {
        val project = "default";
        NSparkSnapshotJob snapshotJob = new NSparkSnapshotJob();
        snapshotJob.setProject(project);
        Map params = new HashMap<String, String>();
        params.put(NBatchConstants.P_INCREMENTAL_BUILD, "true");
        params.put(NBatchConstants.P_SELECTED_PARTITION_VALUE, "[\"1\",\"2\",\"3\"]");
        params.put(NBatchConstants.P_SELECTED_PARTITION_COL, "testCol");
        snapshotJob.setParams(params);
        ExecutableResponse response = ExecutableResponse.create(snapshotJob,
                ExecutableManager.toPO(snapshotJob, project));

        params.put(NBatchConstants.P_INCREMENTAL_BUILD, "false");
        params.put(NBatchConstants.P_SELECTED_PARTITION_COL, "testCol");
        params.put(NBatchConstants.P_SELECTED_PARTITION_VALUE, "[\"1\",\"2\",\"3\"]");
        snapshotJob.setParams(params);
        response = ExecutableResponse.create(snapshotJob, ExecutableManager.toPO(snapshotJob, project));
        assertEquals("[\"1\",\"2\",\"3\"]", response.getSnapshotDataRange());

        params.put(NBatchConstants.P_INCREMENTAL_BUILD, "false");
        params.put(NBatchConstants.P_SELECTED_PARTITION_COL, "testCol");
        params.put(NBatchConstants.P_SELECTED_PARTITION_VALUE, "[\"3\",\"2\",\"1\"]");
        snapshotJob.setParams(params);
        response = ExecutableResponse.create(snapshotJob, ExecutableManager.toPO(snapshotJob, project));
        assertEquals("[\"1\",\"2\",\"3\"]", response.getSnapshotDataRange());

        params.put(NBatchConstants.P_INCREMENTAL_BUILD, "false");
        params.put(NBatchConstants.P_SELECTED_PARTITION_COL, "testCol");
        params.put(NBatchConstants.P_SELECTED_PARTITION_VALUE, null);
        snapshotJob.setParams(params);
        response = ExecutableResponse.create(snapshotJob, ExecutableManager.toPO(snapshotJob, project));
        assertEquals("FULL", response.getSnapshotDataRange());

        params.put(NBatchConstants.P_INCREMENTAL_BUILD, "true");
        params.put(NBatchConstants.P_SELECTED_PARTITION_COL, "testCol");
        params.put(NBatchConstants.P_SELECTED_PARTITION_VALUE, null);
        snapshotJob.setParams(params);
        response = ExecutableResponse.create(snapshotJob, ExecutableManager.toPO(snapshotJob, project));
        assertEquals("INC", response.getSnapshotDataRange());

        params.put(NBatchConstants.P_INCREMENTAL_BUILD, "true");
        params.put(NBatchConstants.P_SELECTED_PARTITION_COL, null);
        params.put(NBatchConstants.P_SELECTED_PARTITION_VALUE, "[\"1\",\"2\",\"3\"]");
        snapshotJob.setParams(params);
        response = ExecutableResponse.create(snapshotJob, ExecutableManager.toPO(snapshotJob, project));
        assertEquals("FULL", response.getSnapshotDataRange());
    }

    @Test
    public void testCalculateStepRatio() {
        val project = "default";
        ExecutableManager manager = ExecutableManager.getInstance(jobService.getConfig(), project);
        SucceedChainedTestExecutable executable = new SucceedChainedTestExecutable();
        executable.setJobType(JobTypeEnum.INC_BUILD);
        executable.setProject(project);
        addSegment(executable);
        FiveSecondSucceedTestExecutable task = new FiveSecondSucceedTestExecutable();
        task.setProject(project);
        addSegment(task);
        executable.addTask(task);
        manager.addJob(executable);
        manager.updateJobOutput(executable.getId(), ExecutableState.PAUSED, null, null, null);
        manager.updateJobOutput(task.getId(), ExecutableState.READY, null, null, null);
        manager.updateJobOutput(task.getId(), ExecutableState.PENDING, null, null, null);
        manager.updateJobOutput(task.getId(), ExecutableState.RUNNING, null, null, null);
        manager.updateJobOutput(task.getId(), ExecutableState.SUCCEED, null, null, null);

        var ratio = ExecutableResponse.calculateStepRatio(manager.getJob(executable.getJobId()),
                manager.getExecutablePO(executable.getJobId()));
        assertTrue(0.99F == ratio);
    }

    @Test
    public void testcalculateSuccessStageInTaskMapSingle() {
        String segmentId = RandomUtil.randomUUIDStr();

        val project = "default";
        ExecutableManager manager = ExecutableManager.getInstance(jobService.getConfig(), project);
        SucceedChainedTestExecutable executable = new SucceedChainedTestExecutable();
        executable.setJobType(JobTypeEnum.INC_BUILD);
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
        var successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps,
                ExecutableManager.toPO(executable, project));
        assertTrue(3 == successLogicStep);

        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.ERROR, null, "test output", true);
        manager.updateStageStatus(logicStep2.getId(), segmentId, ExecutableState.ERROR, null, "test output", true);
        manager.updateStageStatus(logicStep3.getId(), segmentId, ExecutableState.ERROR, null, "test output", true);

        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap();
        successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps,
                ExecutableManager.toPO(executable, project));
        assertTrue(0 == successLogicStep);

        Map<String, String> info = Maps.newHashMap();
        info.put(NBatchConstants.P_INDEX_SUCCESS_COUNT, "1");
        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.RUNNING, info, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap();
        successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps,
                manager.getExecutablePO(executable.getId()));
        assertTrue(0.1 == successLogicStep);

        info.put(NBatchConstants.P_INDEX_SUCCESS_COUNT, "8");
        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.RUNNING, info, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap();
        successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps,
                manager.getExecutablePO(executable.getId()));
        assertTrue(0.8 == successLogicStep);

        info.put(NBatchConstants.P_INDEX_SUCCESS_COUNT, "10");
        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.RUNNING, info, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap();
        successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps,
                manager.getExecutablePO(executable.getId()));
        assertTrue(1 == successLogicStep);

        info.put(NBatchConstants.P_INDEX_SUCCESS_COUNT, "12");
        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.SUCCEED, info, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap();
        successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps,
                manager.getExecutablePO(executable.getId()));
        assertTrue(1 == successLogicStep);

        manager.updateStageStatus(logicStep2.getId(), segmentId, ExecutableState.RUNNING, null, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap();
        successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps,
                manager.getExecutablePO(executable.getId()));
        assertTrue(1 == successLogicStep);

        manager.updateStageStatus(logicStep2.getId(), segmentId, ExecutableState.SUCCEED, null, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap();
        successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps,
                manager.getExecutablePO(executable.getId()));
        assertTrue(2 == successLogicStep);
    }

    @Test
    public void testcalculateSuccessStageInTaskMap() {
        String segmentId = RandomUtil.randomUUIDStr();
        String segmentIds = segmentId + "," + UUID.randomUUID();

        val project = "default";
        ExecutableManager manager = ExecutableManager.getInstance(jobService.getConfig(), project);
        SucceedChainedTestExecutable executable = new SucceedChainedTestExecutable();
        executable.setJobType(JobTypeEnum.INC_BUILD);
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
        var successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps,
                manager.getExecutablePO(executable.getJobId()));
        assertTrue(1.5 == successLogicStep);

        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.ERROR, null, "test output", true);
        manager.updateStageStatus(logicStep2.getId(), segmentId, ExecutableState.ERROR, null, "test output", true);
        manager.updateStageStatus(logicStep3.getId(), segmentId, ExecutableState.ERROR, null, "test output", true);

        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap();
        successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps,
                manager.getExecutablePO(executable.getJobId()));
        assertTrue(0 == successLogicStep);

        Map<String, String> info = Maps.newHashMap();
        info.put(NBatchConstants.P_INDEX_SUCCESS_COUNT, "1");
        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.RUNNING, info, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap();
        successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps,
                manager.getExecutablePO(executable.getJobId()));
        assertTrue(0 == successLogicStep);

        info.put(NBatchConstants.P_INDEX_SUCCESS_COUNT, "10");
        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.RUNNING, info, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap();
        successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps,
                manager.getExecutablePO(executable.getJobId()));
        assertTrue(0 == successLogicStep);

        info.put(NBatchConstants.P_INDEX_SUCCESS_COUNT, "10");
        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.SUCCEED, info, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap();
        successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps,
                manager.getExecutablePO(executable.getJobId()));
        assertTrue(0.5 == successLogicStep);

        info.put(NBatchConstants.P_INDEX_SUCCESS_COUNT, "12");
        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.SUCCEED, info, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap();
        successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps,
                manager.getExecutablePO(executable.getJobId()));
        assertTrue(0.5 == successLogicStep);

        manager.updateStageStatus(logicStep2.getId(), segmentId, ExecutableState.RUNNING, null, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap();
        successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps,
                manager.getExecutablePO(executable.getJobId()));
        assertTrue(0.5 == successLogicStep);

        manager.updateStageStatus(logicStep2.getId(), segmentId, ExecutableState.SUCCEED, null, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap();
        successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps,
                manager.getExecutablePO(executable.getJobId()));
        assertTrue(1 == successLogicStep);
    }

    @Test
    public void testcalculateSuccessStage() {
        String segmentId = RandomUtil.randomUUIDStr();

        val project = "default";
        ExecutableManager manager = ExecutableManager.getInstance(jobService.getConfig(), project);
        SucceedChainedTestExecutable executable = new SucceedChainedTestExecutable();
        executable.setJobType(JobTypeEnum.INC_BUILD);
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
        var successLogicStep = ExecutableResponse.calculateSuccessStage(sparkExecutable, segmentId, buildSteps, false,
                manager.getExecutablePO(executable.getJobId()));
        assertTrue(3 == successLogicStep);

        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.ERROR, null, "test output", true);
        manager.updateStageStatus(logicStep2.getId(), segmentId, ExecutableState.ERROR, null, "test output", true);
        manager.updateStageStatus(logicStep3.getId(), segmentId, ExecutableState.ERROR, null, "test output", true);

        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap().get(segmentId);
        successLogicStep = ExecutableResponse.calculateSuccessStage(sparkExecutable, segmentId, buildSteps, true,
                manager.getExecutablePO(executable.getJobId()));
        assertTrue(0 == successLogicStep);

        Map<String, String> info = Maps.newHashMap();
        info.put(NBatchConstants.P_INDEX_SUCCESS_COUNT, "1");
        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.RUNNING, info, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap().get(segmentId);
        successLogicStep = ExecutableResponse.calculateSuccessStage(sparkExecutable, segmentId, buildSteps, true,
                manager.getExecutablePO(executable.getJobId()));
        assertTrue(0.1 == successLogicStep);
        successLogicStep = ExecutableResponse.calculateSuccessStage(sparkExecutable, segmentId, buildSteps, false,
                manager.getExecutablePO(executable.getJobId()));
        assertTrue(0 == successLogicStep);

        info.put(NBatchConstants.P_INDEX_SUCCESS_COUNT, "8");
        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.RUNNING, info, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap().get(segmentId);
        successLogicStep = ExecutableResponse.calculateSuccessStage(sparkExecutable, segmentId, buildSteps, true,
                manager.getExecutablePO(executable.getJobId()));
        assertTrue(0.8 == successLogicStep);
        successLogicStep = ExecutableResponse.calculateSuccessStage(sparkExecutable, segmentId, buildSteps, false,
                manager.getExecutablePO(executable.getJobId()));
        assertTrue(0 == successLogicStep);

        info.put(NBatchConstants.P_INDEX_SUCCESS_COUNT, "10");
        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.RUNNING, info, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap().get(segmentId);
        successLogicStep = ExecutableResponse.calculateSuccessStage(sparkExecutable, segmentId, buildSteps, true,
                manager.getExecutablePO(executable.getJobId()));
        assertTrue(1 == successLogicStep);
        successLogicStep = ExecutableResponse.calculateSuccessStage(sparkExecutable, segmentId, buildSteps, false,
                manager.getExecutablePO(executable.getJobId()));
        assertTrue(0 == successLogicStep);

        info.put(NBatchConstants.P_INDEX_SUCCESS_COUNT, "12");
        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.SUCCEED, info, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap().get(segmentId);
        successLogicStep = ExecutableResponse.calculateSuccessStage(sparkExecutable, segmentId, buildSteps, true,
                manager.getExecutablePO(executable.getJobId()));
        assertTrue(1 == successLogicStep);
        successLogicStep = ExecutableResponse.calculateSuccessStage(sparkExecutable, segmentId, buildSteps, false,
                manager.getExecutablePO(executable.getJobId()));
        assertTrue(1 == successLogicStep);

        manager.updateStageStatus(logicStep2.getId(), segmentId, ExecutableState.RUNNING, null, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap().get(segmentId);
        successLogicStep = ExecutableResponse.calculateSuccessStage(sparkExecutable, segmentId, buildSteps, true,
                manager.getExecutablePO(executable.getJobId()));
        assertTrue(1 == successLogicStep);
        successLogicStep = ExecutableResponse.calculateSuccessStage(sparkExecutable, segmentId, buildSteps, false,
                manager.getExecutablePO(executable.getJobId()));
        assertTrue(1 == successLogicStep);

        manager.updateStageStatus(logicStep2.getId(), segmentId, ExecutableState.SUCCEED, null, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap().get(segmentId);
        successLogicStep = ExecutableResponse.calculateSuccessStage(sparkExecutable, segmentId, buildSteps, true,
                manager.getExecutablePO(executable.getJobId()));
        assertTrue(2 == successLogicStep);
        successLogicStep = ExecutableResponse.calculateSuccessStage(sparkExecutable, segmentId, buildSteps, false,
                manager.getExecutablePO(executable.getJobId()));
        assertTrue(2 == successLogicStep);
    }

    public void updateStageOutputTaskMapEmpty() {
        String segmentId = RandomUtil.randomUUIDStr();
        String segmentId2 = RandomUtil.randomUUIDStr();
        String segmentIds = segmentId + "," + segmentId2;

        val project = "default";
        ExecutableManager manager = ExecutableManager.getInstance(jobService.getConfig(), project);
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

    private void mockExecutablePOJobs(List<AbstractExecutable> mockJobs, ExecutableManager executableManager) {
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
    public void testGetProjectNameAndJobStepId() {
        // yarnAppName - "job_step_273cf5ea-9a9b-dccb-004e-0e3b04bfb50c-c11baf56-a593-4c5f-d546-1fa86c2d54ad_01"
        overwriteSystemProp("kylin.engine.spark.cluster-manager-class-name", sparkClusterManagerName);
        Mockito.doReturn(project).when(jobService).getProjectByJobId(jobId);
        Pair<String, String> projectNameAndJobStepId = jobService.getProjectNameAndJobStepId(yarnAppId);
        Assert.assertEquals(jobStepId, projectNameAndJobStepId.getSecond());
    }

    @Test
    public void testGetProjectNameAndJobStepIdError() {
        // testGetProjectNameAndJobStepId_NotContains
        String yarnAppId1 = "application";
        overwriteSystemProp("kylin.engine.spark.cluster-manager-class-name", sparkClusterManagerName);
        Assert.assertThrows("Async profiler status error, yarnAppId entered incorrectly, please try again.",
                KylinException.class, () -> jobService.getProjectNameAndJobStepId(yarnAppId1));

        // testGetProjectNameAndJobStepId_LengthError
        String yarnAppId2 = "application_";
        overwriteSystemProp("kylin.engine.spark.cluster-manager-class-name", sparkClusterManagerName);
        Assert.assertThrows("Async profiler status error, yarnAppId entered incorrectly, please try again.",
                KylinException.class, () -> jobService.getProjectNameAndJobStepId(yarnAppId2));

        // testGetProjectNameAndJobStepId_NotContainsJob
        String yarnAppId = "application_1554187389076_-1";
        overwriteSystemProp("kylin.engine.spark.cluster-manager-class-name", sparkClusterManagerName);
        Assert.assertThrows("Async profiler status error, job is finished already.", KylinException.class,
                () -> jobService.getProjectNameAndJobStepId(yarnAppId));
    }

    @Test
    public void testStartProfileByProjectError() {
        overwriteSystemProp("kylin.engine.async-profiler-enabled", "false");
        Assert.assertThrows("Async profiling is not enabled. check parameter 'kylin.engine.async-profiler-enabled'",
                KylinException.class, () -> jobService.startProfileByProject(project, jobStepId, startParams));
    }

    @Test
    public void testStartProfileByProject() throws IOException {
        overwriteSystemProp("kylin.engine.spark.cluster-manager-class-name", sparkClusterManagerName);
        Path actionPath = new Path(
                KylinConfig.getInstanceFromEnv().getJobTmpProfilerFlagsDir(project, jobStepId) + "/status");
        HadoopUtil.writeStringToHdfs("", actionPath);

        String errorMsg = "";
        try {
            jobService.startProfileByProject(project, jobStepId, startParams);
        } catch (Exception e) {
            errorMsg = e.getMessage();
        }
        Assert.assertEquals(0, errorMsg.length());
        HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration(), actionPath);
    }

    @Test
    public void testDumpProfileByProjectError() {
        overwriteSystemProp("kylin.engine.async-profiler-enabled", "false");
        Pair<InputStream, String> jobOutputAndDownloadFile = new Pair<>();
        Assert.assertThrows("Async profiling is not enabled. check parameter 'kylin.engine.async-profiler-enabled'",
                KylinException.class,
                () -> jobService.dumpProfileByProject(project, jobStepId, dumpParams, jobOutputAndDownloadFile));
    }

    @Test
    public void testDumpProfileByProject() throws IOException {
        overwriteSystemProp("kylin.engine.async-profiler-result-timeout", "3s");
        Path actionPath = new Path(
                KylinConfig.getInstanceFromEnv().getJobTmpProfilerFlagsDir(project, jobStepId) + "/status");
        Path dumpFilePath = new Path(
                KylinConfig.getInstanceFromEnv().getJobTmpProfilerFlagsDir(project, jobStepId) + "/dump.tar.gz");
        HadoopUtil.writeStringToHdfs(ProfilerStatus.RUNNING(), actionPath);

        Thread t1 = new Thread(() -> {
            try {
                await().pollDelay(new Duration(1, TimeUnit.MILLISECONDS)).until(() -> true);
                HadoopUtil.writeStringToHdfs("", dumpFilePath);
            } catch (IOException ignored) {
            }
        });
        t1.start();

        String errorMsg = "";
        try {
            jobService.dumpProfileByProject(project, jobStepId, dumpParams, new Pair<>());
        } catch (Exception e) {
            errorMsg = e.getMessage();
        }
        Assert.assertEquals(0, errorMsg.length());
        HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration(), actionPath);
        HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration(), dumpFilePath);
        t1.interrupt();
    }

    @Test
    public void testStartProfileByYarnAppIdError() {
        overwriteSystemProp("kylin.engine.async-profiler-enabled", "false");
        Assert.assertThrows("Async profiling is not enabled. check parameter 'kylin.engine.async-profiler-enabled'",
                KylinException.class, () -> jobService.startProfileByYarnAppId(yarnAppId, startParams));
    }

    @Test
    public void testStartProfileByYarnAppIdAlready() throws IOException {
        overwriteSystemProp("kylin.engine.async-profiler-result-timeout", "1s");
        overwriteSystemProp("kylin.engine.spark.cluster-manager-class-name", sparkClusterManagerName);

        Mockito.doReturn(project).when(jobService).getProjectByJobId(jobId);
        Path actionPath = new Path(
                KylinConfig.getInstanceFromEnv().getJobTmpProfilerFlagsDir(project, jobStepId) + "/status");
        HadoopUtil.writeStringToHdfs(ProfilerStatus.RUNNING(), actionPath);
        Assert.assertThrows("Async profiler status error, profiler is started already.", KylinException.class,
                () -> jobService.startProfileByYarnAppId(yarnAppId, startParams));
        HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration(), actionPath);
    }

    @Test
    public void testStartProfileByYarnAppId() throws IOException {
        overwriteSystemProp("kylin.engine.async-profiler-result-timeout", "1s");
        overwriteSystemProp("kylin.engine.spark.cluster-manager-class-name", sparkClusterManagerName);

        Mockito.doReturn(project).when(jobService).getProjectByJobId(jobId);
        Path actionPath = new Path(
                KylinConfig.getInstanceFromEnv().getJobTmpProfilerFlagsDir(project, jobStepId) + "/status");
        HadoopUtil.writeStringToHdfs(ProfilerStatus.IDLE(), actionPath);
        jobService.startProfileByYarnAppId(yarnAppId, startParams);
        HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration(), actionPath);
    }

    @Test
    public void testDumpProfileByYarnAppIdError() {
        overwriteSystemProp("kylin.engine.async-profiler-enabled", "false");
        Pair<InputStream, String> jobOutputAndDownloadFile = new Pair<>();
        Assert.assertThrows("Async profiling is not enabled. check parameter 'kylin.engine.async-profiler-enabled'",
                KylinException.class,
                () -> jobService.dumpProfileByYarnAppId(yarnAppId, dumpParams, jobOutputAndDownloadFile));
    }

    @Test
    public void testDumpProfileByYarnAppId() throws IOException {
        overwriteSystemProp("kylin.engine.async-profiler-result-timeout", "3s");
        overwriteSystemProp("kylin.engine.spark.cluster-manager-class-name", sparkClusterManagerName);

        Mockito.doReturn(project).when(jobService).getProjectByJobId(jobId);

        Path actionPath = new Path(
                KylinConfig.getInstanceFromEnv().getJobTmpProfilerFlagsDir(project, jobStepId) + "/status");
        Path dumpFilePath = new Path(
                KylinConfig.getInstanceFromEnv().getJobTmpProfilerFlagsDir(project, jobStepId) + "/dump.tar.gz");
        HadoopUtil.writeStringToHdfs(ProfilerStatus.RUNNING(), actionPath);

        Thread t1 = new Thread(() -> {
            try {
                await().pollDelay(new Duration(1, TimeUnit.MILLISECONDS)).until(() -> true);
                HadoopUtil.writeStringToHdfs("", dumpFilePath);
            } catch (IOException ignored) {
            }
        });
        t1.start();

        String errorMsg = "";
        try {
            jobService.dumpProfileByYarnAppId(yarnAppId, dumpParams, new Pair<>());
        } catch (Exception e) {
            errorMsg = e.getMessage();
        }
        Assert.assertEquals(0, errorMsg.length());
        HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration(), actionPath);
        HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration(), dumpFilePath);
        t1.interrupt();
    }

    @Test
    public void testSetResponseLanguageNull() {
        HttpServletRequest mockRequest = mock(HttpServletRequest.class);
        String errorMsg = "";
        try {
            jobService.setResponseLanguage(mockRequest);
        } catch (Exception e) {
            errorMsg = e.getMessage();
        }
        Assert.assertEquals("", errorMsg);
    }

    @Test
    public void testSetResponseLanguageWithZh() {
        HttpServletRequest mockRequest = mock(HttpServletRequest.class);
        // zh
        Mockito.doReturn("zh,zh-CN;q=0.9,zh-HK;q=0.8,zh-TW;q=0.7").when(mockRequest)
                .getHeader(HttpHeaders.ACCEPT_LANGUAGE);
        String errorMsg = "";
        try {
            jobService.setResponseLanguage(mockRequest);
        } catch (Exception e) {
            errorMsg = e.getMessage();
        }
        Assert.assertEquals("", errorMsg);

        // zh-CN
        Mockito.doReturn("zh-CN").when(mockRequest).getHeader(HttpHeaders.ACCEPT_LANGUAGE);
        try {
            jobService.setResponseLanguage(mockRequest);
        } catch (Exception e) {
            errorMsg = e.getMessage();
        }
        Assert.assertEquals("", errorMsg);

        // zh-HK
        Mockito.doReturn("zh-HK,zh-TW").when(mockRequest).getHeader(HttpHeaders.ACCEPT_LANGUAGE);
        try {
            jobService.setResponseLanguage(mockRequest);
        } catch (Exception e) {
            errorMsg = e.getMessage();
        }
        Assert.assertEquals("", errorMsg);

        // zh-TW
        Mockito.doReturn("zh-TW;q=0.9,zh;q=0.8,zh-CN;q=0.7").when(mockRequest).getHeader(HttpHeaders.ACCEPT_LANGUAGE);
        try {
            jobService.setResponseLanguage(mockRequest);
        } catch (Exception e) {
            errorMsg = e.getMessage();
        }
        Assert.assertEquals("", errorMsg);
    }

    @Test
    public void testSetResponseLanguageWithNoZh() {
        HttpServletRequest mockRequest = mock(HttpServletRequest.class);
        Mockito.doReturn("en;q=0.9,zh;q=0.8,zh-TW;q=0.7").when(mockRequest).getHeader(HttpHeaders.ACCEPT_LANGUAGE);
        String errorMsg = "";
        try {
            jobService.setResponseLanguage(mockRequest);
        } catch (Exception e) {
            errorMsg = e.getMessage();
        }
        Assert.assertEquals("", errorMsg);
    }

    @Test
    public void testGetStepOutput() throws PersistentException {
        String jobId = "e1ad7bb0-522e-456a-859d-2eab1df448de";
        ExecutableManager manager = ExecutableManager.getInstance(jobService.getConfig(), "default");
        ExecutableOutputPO executableOutputPO = new ExecutableOutputPO();
        Map<String, String> info = Maps.newHashMap();
        info.put("nodes", "localhost:7070:all");
        executableOutputPO.setInfo(info);
        manager.updateJobOutputToHDFS(KylinConfig.getInstanceFromEnv().getJobTmpOutputStorePath("default", jobId),
                executableOutputPO);

        Map<String, Object> result = Maps.newHashMap();
        result.put("nodes", Lists.newArrayList("localhost:7070"));
        result.put("cmd_output", null);
        Assert.assertEquals(result, jobService.getStepOutput("default", jobId, jobId));

        executableOutputPO.setInfo(null);
        manager.updateJobOutputToHDFS(KylinConfig.getInstanceFromEnv().getJobTmpOutputStorePath("default", jobId),
                executableOutputPO);

        result = Maps.newHashMap();
        result.put("nodes", Lists.newArrayList());
        result.put("cmd_output", null);
        Assert.assertEquals(result, jobService.getStepOutput("default", jobId, jobId));

        info = Maps.newHashMap();
        executableOutputPO.setInfo(info);
        manager.updateJobOutputToHDFS(KylinConfig.getInstanceFromEnv().getJobTmpOutputStorePath("default", jobId),
                executableOutputPO);

        result = Maps.newHashMap();
        result.put("nodes", Lists.newArrayList());
        result.put("cmd_output", null);
        Assert.assertEquals(result, jobService.getStepOutput("default", jobId, jobId));
    }

    @Test
    public void testJobSubdirectoryPermission() throws IOException, PersistentException {
        String jobId = "e1ad7bb0-522e-456a-859d-2eab1df448de";
        ExecutableManager manager = ExecutableManager.getInstance(jobService.getConfig(), "default");
        ExecutableOutputPO executableOutputPO = new ExecutableOutputPO();
        Map<String, String> info = Maps.newHashMap();
        info.put("nodes", "localhost:7070:all");
        executableOutputPO.setInfo(info);
        overwriteSystemProp("kylin.engine.job-tmp-dir-all-permission-enabled", "true");
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        String file = KylinConfig.getInstanceFromEnv().getJobTmpOutputStorePath("default", jobId);
        Path path = new Path(file);
        manager.updateJobOutputToHDFS(file, executableOutputPO);
        Assert.assertSame(FsAction.ALL, fs.getFileStatus(path.getParent()).getPermission().getOtherAction());
    }

    @Test
    public void testInternalTableLoadingJobResponse() {
        KylinConfig conf = KylinConfig.getInstanceFromEnv();
        NTableMetadataManager manager = NTableMetadataManager.getInstance(conf, "default");
        InternalTableManager internalManager = InternalTableManager.getInstance(conf, "default");
        InternalTableLoadingJob job = new InternalTableLoadingJob();
        job.setProject("default");
        job.setParam("startTime", "10001");
        job.setParam("endTime", "10002");

        ExecutableResponse response = ExecutableResponse.create(job, null);
        Assert.assertEquals(10001L, response.getDataRangeStart());
        Assert.assertEquals(10002L, response.getDataRangeEnd());

        job.setParam("incrementalBuild", "false");
        response = ExecutableResponse.create(job, null);
        Assert.assertEquals(Long.MAX_VALUE, response.getDataRangeEnd());

        job.setParam("incrementalBuild", "true");
        job.setParam("deletePartition", "true");
        response = ExecutableResponse.create(job, null);
        Assert.assertEquals(Long.MAX_VALUE, response.getDataRangeEnd());

        TableDesc originTable = manager.getTableDesc("DEFAULT.TEST_KYLIN_FACT");
        internalManager.createInternalTable(new InternalTableDesc(originTable));

        job.setParam(NBatchConstants.P_TABLE_NAME, originTable.getIdentity());
        response = ExecutableResponse.create(job, null);
        Assert.assertTrue(response.isTargetSubjectError());

        internalManager.updateInternalTable(originTable.getIdentity(),
                copyForWrite -> copyForWrite.setLocation(copyForWrite.generateInternalTableLocation()));
        response = ExecutableResponse.create(job, null);
        Assert.assertFalse(response.isTargetSubjectError());
        Assert.assertEquals(originTable.getIdentity(), response.getTargetSubject());
    }
}

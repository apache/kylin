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
import static org.apache.kylin.job.constant.JobStatusEnum.PENDING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.ErrorCode;
import org.apache.kylin.common.exception.ExceptionResolve;
import org.apache.kylin.common.exception.JobErrorCode;
import org.apache.kylin.common.exception.JobExceptionReason;
import org.apache.kylin.common.exception.JobExceptionResolve;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.LogOutputTestCase;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.engine.spark.job.NSparkExecutable;
import org.apache.kylin.engine.spark.job.NTableSamplingJob;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.dao.ExecutableOutputPO;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.dao.JobInfoDao;
import org.apache.kylin.job.domain.JobInfo;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.BaseTestExecutable;
import org.apache.kylin.job.execution.DefaultExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.FiveSecondSucceedTestExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.StageExecutable;
import org.apache.kylin.job.execution.SucceedChainedTestExecutable;
import org.apache.kylin.job.execution.SucceedTestExecutable;
import org.apache.kylin.job.rest.JobFilter;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.FusionModel;
import org.apache.kylin.metadata.model.FusionModelManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.request.JobUpdateRequest;
import org.apache.kylin.rest.response.ExecutableResponse;
import org.apache.kylin.rest.response.ExecutableStepResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.apache.spark.application.NoRetryException;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import lombok.val;
import lombok.var;

public class JobInfoServiceTest extends LogOutputTestCase {

    String project = "default";

    private JobInfoService jobInfoService = Mockito.spy(JobInfoService.class);

    @Mock
    private final ModelService modelService = Mockito.spy(ModelService.class);

    @Mock
    private JobInfoDao jobInfoDao;

    @Mock
    private final AclUtil aclUtil = Mockito.spy(AclUtil.class);

    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    private final TableExtService tableExtService = Mockito.spy(TableExtService.class);

    @Mock
    private ProjectService projectService = Mockito.spy(ProjectService.class);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void setUp() {
        createTestMetadata();
        KylinConfig config = getTestConfig();
        jobInfoDao = JobContextUtil.getJobInfoDao(config);
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(jobInfoService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(jobInfoService, "jobInfoDao", jobInfoDao);
        ReflectionTestUtils.setField(jobInfoService, "modelService", modelService);
        ReflectionTestUtils.setField(jobInfoService, "tableExtService", tableExtService);
        ReflectionTestUtils.setField(jobInfoService, "projectService", projectService);
        ReflectionTestUtils.setField(projectService, "aclEvaluate", aclEvaluate);
    }

    @After
    public void tearDown() {
        JobContextUtil.cleanUp();
        cleanupTestMetadata();
    }

    @Test
    public void testListJobs() throws Exception {
        val mockJobs = mockDetailJobs(true);
        getTestConfig().setProperty("kylin.streaming.enabled", "false");
        // test size
        List<String> jobNames = Lists.newArrayList();
        JobFilter jobFilter = new JobFilter(Lists.newArrayList(), jobNames, 4, "", "", false, "default", "", true);
        List<ExecutableResponse> jobs = jobInfoService.listJobs(jobFilter);
        Assert.assertEquals(3, jobs.size());
        jobInfoService.addOldParams(jobs);
        jobFilter.setSubject("");
        jobFilter.setStatuses(Lists.newArrayList(JobStatusEnum.NEW));
        jobFilter.setTimeFilter(1);
        List<ExecutableResponse> jobs4 = jobInfoService.listJobs(jobFilter);
        Assert.assertEquals(2, jobs4.size());

        jobFilter.setSubject("");
        jobFilter.setStatuses(Lists.newArrayList(JobStatusEnum.NEW, JobStatusEnum.FINISHED));
        jobFilter.setTimeFilter(1);
        jobs4 = jobInfoService.listJobs(jobFilter);
        Assert.assertEquals(3, jobs4.size());

        jobFilter.setStatuses(Lists.newArrayList());
        jobFilter.setTimeFilter(3);

        jobFilter.setSortBy("duration");
        jobFilter.setReverse(true);
        List<ExecutableResponse> jobs7 = jobInfoService.listJobs(jobFilter);
        long maxDuration = 0;
        for (ExecutableResponse response : jobs7) {
            long duration = response.getDuration();
            if (duration >= maxDuration) {
                maxDuration = duration;
            }
        }
        Assert.assertTrue(jobs7.size() == 3 && jobs7.get(0).getDuration() == maxDuration);

        jobFilter.setSortBy("create_time");
        jobFilter.setReverse(true);
        List<ExecutableResponse> jobs8 = jobInfoService.listJobs(jobFilter);
        Assert.assertTrue(jobs8.size() == 3 && jobs8.get(0).getId().equals("sparkjob3"));

        jobFilter.setReverse(false);
        jobFilter.setStatuses(Lists.newArrayList());
        jobFilter.setSortBy("");
        List<ExecutableResponse> jobs10 = jobInfoService.listJobs(jobFilter);
        Assert.assertEquals(3, jobs10.size());

        jobFilter.setSortBy("job_status");
        List<ExecutableResponse> jobs11 = jobInfoService.listJobs(jobFilter);
        Assert.assertTrue(jobs11.size() == 3 && jobs11.get(2).getId().equals("sparkjob1"));

        jobFilter.setSortBy("create_time");
        List<ExecutableResponse> jobs12 = jobInfoService.listJobs(jobFilter);
        Assert.assertTrue(jobs12.size() == 3 && jobs12.get(0).getId().equals("sparkjob1"));

        jobFilter.setSortBy("target_subject");
        for (ExecutablePO job : mockJobs) {
            jobInfoDao.updateJob(job.getUuid(), jobUpdater -> {
                jobUpdater.setJobType(JobTypeEnum.INDEX_BUILD);
                return true;
            });
        }
        List<ExecutableResponse> sortJobs2 = jobInfoService.listJobs(jobFilter);
        Assert.assertTrue(sortJobs2.size() == 3 && sortJobs2.get(0).getId().equals("sparkjob1"));
        for (ExecutablePO job : mockJobs) {
            jobInfoDao.updateJob(job.getUuid(), jobUpdater -> {
                jobUpdater.setJobType(JobTypeEnum.TABLE_SAMPLING);
                return true;
            });
        }
        List<ExecutableResponse> sortJobs3 = jobInfoService.listJobs(jobFilter);
        Assert.assertTrue(sortJobs3.size() == 3 && sortJobs3.get(0).getId().equals("sparkjob1"));
        for (ExecutablePO job : mockJobs) {
            jobInfoDao.updateJob(job.getUuid(), jobUpdater -> {
                jobUpdater.setJobType(JobTypeEnum.SNAPSHOT_BUILD);
                if (jobUpdater.getId().equals("sparkjob2")) {
                    jobUpdater.getOutput().setStatus("PAUSED");
                } else {
                    jobUpdater.getOutput().setStatus("DISCARDED");
                }
                return true;
            });
        }
        jobFilter.setSortBy("job_status");
        List<ExecutableResponse> sortJobs4 = jobInfoService.listJobs(jobFilter);
        Assert.assertTrue(sortJobs4.size() == 3 && sortJobs4.get(2).getId().equals("sparkjob2"));
        for (ExecutablePO job : mockJobs) {
            jobInfoDao.updateJob(job.getUuid(), jobUpdater -> {
                jobUpdater.setJobType(JobTypeEnum.SNAPSHOT_REFRESH);
                if (jobUpdater.getId().equals("sparkjob1")) {
                    jobUpdater.getOutput().setStatus("PAUSED");
                } else {
                    jobUpdater.getOutput().setStatus("SUCCEED");
                }
                return true;
            });
        }
        List<ExecutableResponse> sortJobs5 = jobInfoService.listJobs(jobFilter);
        Assert.assertTrue(sortJobs5.size() == 3 && sortJobs5.get(0).getId().equals("sparkjob1"));

        jobFilter.setSortBy("total_time");
        assertKylinExeption(() -> {
            jobInfoService.listJobs(jobFilter);
        }, "The selected sort filter \"total_time\" is invalid. Please select again.");

        jobFilter.setSortBy("create_time");
        List<ExecutableResponse> jobs13 = jobInfoService.listJobs(jobFilter, 0, 10);
        Assert.assertEquals(3, jobs13.size());
        String jobId = jobs13.get(0).getId();
        for (ExecutablePO job : mockJobs) {
            job.setJobType(JobTypeEnum.TABLE_SAMPLING);
        }
        jobFilter.setKey(jobId);
        List<ExecutableResponse> jobs14 = jobInfoService.listJobs(jobFilter, 0, 10);
        Assert.assertTrue(jobs14.size() == 1 && jobs14.get(0).getId().equals(jobId));
        jobFilter.setStatuses(Lists.newArrayList());
        List<ExecutableResponse> jobs15 = jobInfoService.listJobs(jobFilter, 0, 10);
        assertEquals(1, jobs15.size());
        jobFilter.setStatuses(Lists.newArrayList(JobStatusEnum.NEW));
        List<ExecutableResponse> jobs16 = jobInfoService.listJobs(jobFilter, 0, 10);
        assertEquals(0, jobs16.size());
    }

    private List<ExecutablePO> mockDetailJobs(boolean random) throws Exception {
        List<ExecutablePO> jobs = new ArrayList<>();
        for (int i = 1; i < 4; i++) {
            jobs.add(jobInfoDao.addJob(mockExecutablePO(random, i + "")));
        }
        return jobs;
    }

    private List<JobInfo> mockDetailJobInfoList(boolean random) throws Exception {
        List<JobInfo> jobs = new ArrayList<>();
        for (int i = 1; i < 4; i++) {
            ExecutablePO executablePO = mockExecutablePO(random, i + "");
            jobInfoDao.addJob(executablePO);
            jobs.add(jobInfoDao.constructJobInfo(executablePO, 1L));
        }
        return jobs;
    }

    private String getProject() {
        return "default";
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

    private ExecutablePO mockExecutablePO(boolean random, String name) {
        ExecutablePO mockJob = new ExecutablePO();
        mockJob.setType("org.apache.kylin.job.execution.SucceedChainedTestExecutable");
        mockJob.setJobType(JobTypeEnum.INDEX_BUILD);
        mockJob.setProject(getProject());
        mockJob.setUuid("sparkjob" + name);
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
        return mockJob;
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
    public void testFilterJob() throws Exception {
        val mockJobs = mockDetailJobInfoList(true);
        {
            List<String> jobNames = Lists.newArrayList();
            JobFilter jobFilter = new JobFilter(Lists.newArrayList(), jobNames, 0, "", "", false, "default",
                    "total_duration", true);
            List<ExecutableResponse> jobs = jobInfoService.listJobs(jobFilter);

            val totalDurationArrays = jobs.stream().map(ExecutableResponse::getTotalDuration)
                    .collect(Collectors.toList());
            List<Long> copyDurationList = new ArrayList<>(totalDurationArrays);
            copyDurationList.sort(Collections.reverseOrder());
            Assert.assertEquals(3, copyDurationList.size());
            Assert.assertEquals(totalDurationArrays, copyDurationList);
        }

        for (JobInfo jobInfo : mockJobs) {
            jobInfoDao.updateJob(jobInfo.getJobId(), executablePO -> {
                executablePO.setJobType(JobTypeEnum.TABLE_SAMPLING);
                return true;
            });
        }
        List<String> jobNames = Lists.newArrayList();
        JobFilter jobFilter = new JobFilter(Lists.newArrayList(), jobNames, 0, "", "default", false, "default", "",
                false);
        List<ExecutableResponse> jobs = jobInfoService.listJobs(jobFilter);
        Assert.assertEquals(0, jobs.size());
    }

    @Test
    public void testGetJobCreateTime() {
        ExecutableManager manager = ExecutableManager.getInstance(jobInfoService.getConfig(), "default");
        SucceedChainedTestExecutable executable = new SucceedChainedTestExecutable();
        addSegment(executable);
        executable.setJobType(JobTypeEnum.INDEX_BUILD);
        executable.setParam("test1", "test1");
        executable.setParam("test2", "test2");
        executable.setParam("test3", "test3");
        executable.setProject("default");
        executable.setName("test_create_time");
        manager.addJob(executable);
        List<String> jobNames = Lists.newArrayList();
        JobFilter jobFilter = new JobFilter(Lists.newArrayList(), jobNames, 4, "", "", false, "default", "", true);
        List<ExecutableResponse> jobs = jobInfoService.listJobs(jobFilter);
        Assert.assertTrue(jobs.get(0).getCreateTime() > 0);
    }

    private void addSegment(AbstractExecutable job) {
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
    }

    @Test
    public void testGetTargetSubjectAndJobType() {
        ExecutableManager manager = ExecutableManager.getInstance(jobInfoService.getConfig(), "default");
        SucceedChainedTestExecutable job1 = new SucceedChainedTestExecutable();
        job1.setProject(getProject());
        job1.setName("mocked job");
        job1.setTargetSubject("12345678");
        job1.setJobType(JobTypeEnum.INDEX_BUILD);
        final TableDesc tableDesc = NTableMetadataManager.getInstance(getTestConfig(), getProject())
                .getTableDesc("DEFAULT.TEST_KYLIN_FACT");
        NTableSamplingJob samplingJob = NTableSamplingJob.internalCreate(tableDesc, getProject(), "ADMIN", 20000);
        manager.addJob(job1);
        manager.addJob(samplingJob);
        List<String> jobNames = Lists.newArrayList();
        JobFilter jobFilter = new JobFilter(Lists.newArrayList(), jobNames, 4, "", "", false, "default", "", false);
        jobFilter.setSortBy("create_time");
        List<ExecutableResponse> jobs = jobInfoService.listJobs(jobFilter);

        Assert.assertEquals("The model is deleted", jobs.get(0).getTargetSubject()); // no target model so it's null
        Assert.assertEquals("mocked job", jobs.get(0).getJobName());
        Assert.assertEquals(tableDesc.getIdentity(), jobs.get(1).getTargetSubject());
        Assert.assertEquals("TABLE_SAMPLING", jobs.get(1).getJobName());
    }

    @Test
    public void testJobnameResponse() throws Exception {
        ExecutableManager manager = Mockito.spy(ExecutableManager.getInstance(getTestConfig(), getProject()));
        ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>> managersByPrjCache = NLocalFileMetadataTestCase
                .getInstanceByProject();
        managersByPrjCache.get(ExecutableManager.class).put(getProject(), manager);
        ExecutablePO job1 = Mockito.spy(ExecutablePO.class);
        job1.setProject(getProject());
        job1.setUuid("sparkjob1");
        job1.setTargetModel("model1");
        job1.setJobType(JobTypeEnum.INC_BUILD);
        job1.setType("org.apache.kylin.job.execution.SucceedChainedTestExecutable");
        ExecutablePO subJob = new ExecutablePO();
        subJob.setType("org.apache.kylin.job.execution.SucceedChainedTestExecutable");
        subJob.setJobType(JobTypeEnum.INC_BUILD);
        subJob.getOutput().setStatus("SUCCEED");
        subJob.setProject(getProject());
        subJob.setUuid(job1.getId() + "_00");
        job1.setTasks(Lists.newArrayList(subJob));
        manager.addJob(job1);
        manager.addJob(subJob);

        Mockito.when(manager.getAllJobs(Mockito.anyLong(), Mockito.anyLong()))
                .thenReturn(Collections.singletonList(job1));

        JobFilter jobFilter = new JobFilter(Lists.newArrayList(), Lists.newArrayList(), 4, "", "", false, "default", "",
                false);
        List<ExecutableResponse> jobs = jobInfoService.listJobs(jobFilter);

        Assert.assertEquals(2, jobs.size());

        ExecutableResponse executableResponse = jobs.get(0);

        Assert.assertEquals("sparkjob1", executableResponse.getId());

    }

    @Test
    public void testFilterJobExactMatch() throws Exception {
        val mockJobs = mockDetailJobs(false);

        for (int i = 0; i < 3; i++) {
            int j = i;
            jobInfoDao.updateJob(mockJobs.get(j).getUuid(), job -> {
                job.setJobType(JobTypeEnum.TABLE_SAMPLING);
                return true;
            });
        }
        List<String> jobNames = Lists.newArrayList();
        JobFilter jobFilter = new JobFilter(Lists.newArrayList(), jobNames, 0, "", "def", false, "default", "", false);
        List<ExecutableResponse> jobs = jobInfoService.listJobs(jobFilter);
        Assert.assertEquals(0, jobs.size());

        JobFilter jobFilter2 = new JobFilter(Lists.newArrayList(), jobNames, 0, "", "def", true, "default", "", false);
        List<ExecutableResponse> jobs2 = jobInfoService.listJobs(jobFilter2);
        Assert.assertEquals(0, jobs2.size());

        JobFilter jobFilter3 = new JobFilter(Lists.newArrayList(), jobNames, 0, "", null, true, "default", "", false);
        List<ExecutableResponse> jobs3 = jobInfoService.listJobs(jobFilter3);
        Assert.assertEquals(3, jobs3.size());
    }

    @Test
    public void testCheckJobStatusAndAction() {
        JobUpdateRequest request = new JobUpdateRequest();
        request.setStatuses(Lists.newArrayList("RUNNING", "PENDING"));
        request.setAction("PAUSE");
        jobInfoService.checkJobStatusAndAction(request);
        thrown.expect(KylinException.class);
        thrown.expectMessage(JOB_ACTION_ILLEGAL.getMsg("RUNNING", "DISCARD, PAUSE, RESTART"));
        jobInfoService.checkJobStatusAndAction("RUNNING", "RESUME");
    }

    @Test
    public void testUpdateStageOutput() {
        String segmentId = RandomUtil.randomUUIDStr();
        String segmentId2 = RandomUtil.randomUUIDStr();
        String segmentIds = segmentId + "," + segmentId2;

        ExecutableManager manager = ExecutableManager.getInstance(jobInfoService.getConfig(), project);
        SucceedChainedTestExecutable executable = new SucceedChainedTestExecutable();
        executable.setId(RandomUtil.randomUUIDStr());
        executable.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        NSparkExecutable sparkExecutable = new NSparkExecutable();
        sparkExecutable.setParam(NBatchConstants.P_SEGMENT_IDS, segmentIds);
        sparkExecutable.setId(RandomUtil.randomUUIDStr());
        executable.addTask(sparkExecutable);

        StageExecutable build = new StageExecutable();
        final StageExecutable logicStep = (StageExecutable) sparkExecutable.addStage(build);
        sparkExecutable.setStageMap();
        executable.setJobType(JobTypeEnum.INC_BUILD);

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

        Map<String, List<StageExecutable>> tasksMap = sparkExecutable1.getStagesMap();
        assertEquals(2, tasksMap.size());
        List<StageExecutable> logicStepBases = tasksMap.get(segmentId);
        assertEquals(1, logicStepBases.size());
        StageExecutable logicStepBase = logicStepBases.get(0);
        assertEquals(logicStep.getId(), logicStepBase.getId());

        manager.updateStageStatus(logicStep.getId(), segmentId, ExecutableState.RUNNING, null, "test output");

        List<ExecutableStepResponse> jobDetail = jobInfoService.getJobDetail(project, executable.getId());
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
        checkResponse(logicStepResponse2, logicStep.getId(), PENDING);
        assertEquals(0, logicStepResponse2.getExecStartTime());
        assertTrue(logicStepResponse2.getExecStartTime() < System.currentTimeMillis());

        manager.updateStageStatus(logicStep.getId(), segmentId2, ExecutableState.RUNNING, null, "test output");

        manager.updateStageStatus(logicStep.getId(), null, ExecutableState.SUCCEED, null, "test output");

        jobDetail = jobInfoService.getJobDetail(project, executable.getId());
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
    public void testGetJobDetail() {
        ExecutableManager manager = ExecutableManager.getInstance(jobInfoService.getConfig(), "default");
        SucceedChainedTestExecutable executable = new SucceedChainedTestExecutable();
        executable.setParam("test1", "test1");
        executable.setParam("test2", "test2");
        executable.setParam("test3", "test3");
        executable.setProject("default");
        executable.setName("test");
        executable.addTask(new FiveSecondSucceedTestExecutable());
        executable.setJobType(JobTypeEnum.INC_BUILD);
        manager.addJob(executable);
        List<ExecutableStepResponse> result = jobInfoService.getJobDetail("default", executable.getId());
        Assert.assertEquals(1, result.size());
    }

    @Test
    public void testBasic() throws IOException {
        ExecutableManager manager = ExecutableManager.getInstance(jobInfoService.getConfig(), "default");
        NDataflowManager dsMgr = NDataflowManager.getInstance(jobInfoService.getConfig(), "default");
        SucceedChainedTestExecutable executable = new SucceedChainedTestExecutable();
        executable.setJobType(JobTypeEnum.INC_BUILD);
        manager.addJob(executable);
        jobInfoService.batchUpdateJobStatus(Lists.newArrayList(executable.getId()), "default", "PAUSE",
                Lists.newArrayList());
        Assert.assertEquals(ExecutableState.PAUSED, manager.getJob(executable.getId()).getStatus());
        UnitOfWork.doInTransactionWithRetry(() -> {
            jobInfoService.batchUpdateJobStatus(Lists.newArrayList(executable.getId()), "default", "RESUME",
                    Lists.newArrayList());
            return null;
        }, "default");
        jobInfoService.batchUpdateJobStatus(Lists.newArrayList(executable.getId()), "default", "PAUSE",
                Lists.newArrayList());
        Assert.assertEquals(ExecutableState.PAUSED, manager.getJob(executable.getId()).getStatus());
        UnitOfWork.doInTransactionWithRetry(() -> {
            jobInfoService.batchUpdateJobStatus(Lists.newArrayList(executable.getId()), "default", "RESUME",
                    Lists.newArrayList("STOPPED"));
            return null;
        }, "default");
        Assert.assertEquals(ExecutableState.READY, manager.getJob(executable.getId()).getStatus());
        UnitOfWork.doInTransactionWithRetry(() -> {
            jobInfoService.batchUpdateJobStatus(Lists.newArrayList(executable.getId()), "default", "DISCARD",
                    Lists.newArrayList());
            return null;
        }, "default");
        Assert.assertEquals(ExecutableState.DISCARDED, manager.getJob(executable.getId()).getStatus());
        Assert.assertNull(dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa").getSegments().getFirstSegment());
        Mockito.doNothing().when(tableExtService).removeJobIdFromTableExt(executable.getId(), "default");
        jobInfoService.batchDropJob("default", Lists.newArrayList(executable.getId()), Lists.newArrayList());
        List<AbstractExecutable> executables = manager.getAllExecutables();
        Assert.assertFalse(executables.contains(executable));
    }

    @Test
    public void testDiscardJobException() throws IOException {
        ExecutableManager manager = ExecutableManager.getInstance(jobInfoService.getConfig(), "default");
        SucceedChainedTestExecutable executable = new SucceedChainedTestExecutable();
        executable.setProject("default");
        executable.setJobType(JobTypeEnum.INC_BUILD);
        manager.addJob(executable);
        manager.updateJobOutput(executable.getId(), ExecutableState.PENDING, null, null, null);
        manager.updateJobOutput(executable.getId(), ExecutableState.RUNNING, null, null, null);
        manager.updateJobOutput(executable.getId(), ExecutableState.SUCCEED, null, null, null);
        Assert.assertEquals(ExecutableState.SUCCEED, executable.getStatus());
        thrown.expect(KylinException.class);
        thrown.expectMessage(JOB_UPDATE_STATUS_FAILED.getMsg("DISCARD", executable.getId(), ExecutableState.SUCCEED));
        jobInfoService.batchUpdateJobStatus(Lists.newArrayList(executable.getId()), "default", "DISCARD",
                Lists.newArrayList());
    }

    @Test
    public void testGlobalBasic() throws IOException {
        ProjectInstance defaultProject = new ProjectInstance();
        defaultProject.setName("default");
        defaultProject.setMvcc(0);
        Mockito.doReturn(Lists.newArrayList(defaultProject)).when(jobInfoService).getReadableProjects();

        ExecutableManager manager = ExecutableManager.getInstance(jobInfoService.getConfig(), "default");
        NDataflowManager dsMgr = NDataflowManager.getInstance(jobInfoService.getConfig(), "default");
        SucceedChainedTestExecutable executable = new SucceedChainedTestExecutable();
        executable.setJobType(JobTypeEnum.INC_BUILD);
        manager.addJob(executable);
        Mockito.when(projectService.getOwnedProjects()).thenReturn(Lists.newArrayList("default"));
        jobInfoService.batchUpdateJobStatus(Lists.newArrayList(executable.getId()), "default", "PAUSE",
                Lists.newArrayList());
        Assert.assertEquals(ExecutableState.PAUSED, manager.getJob(executable.getId()).getStatus());

        jobInfoService.batchUpdateJobStatus(Lists.newArrayList(executable.getId()), "default", "RESUME",
                Lists.newArrayList());
        jobInfoService.batchUpdateJobStatus(Lists.newArrayList(executable.getId()), "default", "PAUSE",
                Lists.newArrayList());
        Assert.assertEquals(ExecutableState.PAUSED, manager.getJob(executable.getId()).getStatus());

        jobInfoService.batchUpdateJobStatus(Lists.newArrayList(executable.getId()), "default", "RESUME",
                Lists.newArrayList("STOPPED"));
        Assert.assertEquals(ExecutableState.READY, manager.getJob(executable.getId()).getStatus());

        jobInfoService.batchUpdateJobStatus(Lists.newArrayList(executable.getId()), "default", "DISCARD",
                Lists.newArrayList());
        Assert.assertEquals(ExecutableState.DISCARDED, manager.getJob(executable.getId()).getStatus());

        Assert.assertNull(dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa").getSegments().getFirstSegment());

        Mockito.doNothing().when(tableExtService).removeJobIdFromTableExt(executable.getId(), "default");
        jobInfoService.batchDropGlobalJob(Lists.newArrayList(executable.getId()), Lists.newArrayList());
        Assert.assertFalse(manager.getAllExecutables().contains(executable));
    }

    @Test
    public void testGetJobOutput() {
        ExecutableManager manager = ExecutableManager.getInstance(jobInfoService.getConfig(), "default");
        ExecutableOutputPO executableOutputPO = new ExecutableOutputPO();
        executableOutputPO.setStatus("SUCCEED");
        executableOutputPO.setContent("succeed");
        manager.updateJobOutputToHDFS(KylinConfig.getInstanceFromEnv().getJobTmpOutputStorePath("default",
                "e1ad7bb0-522e-456a-859d-2eab1df448de"), executableOutputPO);

        Assertions.assertThat(jobInfoService.getJobOutput("default", "e1ad7bb0-522e-456a-859d-2eab1df448de"))
                .isEqualTo("succeed");
    }

    @Test
    public void testGetAllJobOutput() throws IOException {
        File file = temporaryFolder.newFile("execute_output.json." + System.currentTimeMillis() + ".log");
        for (int i = 0; i < 200; i++) {
            Files.write(file.toPath(), String.format(Locale.ROOT, "lines: %s%n", i).getBytes(Charset.defaultCharset()),
                    StandardOpenOption.APPEND);
        }

        String[] exceptLines = Files.readAllLines(file.toPath()).toArray(new String[0]);

        ExecutableManager manager = ExecutableManager.getInstance(jobInfoService.getConfig(), "default");
        ExecutableOutputPO executableOutputPO = new ExecutableOutputPO();
        executableOutputPO.setStatus("SUCCEED");
        executableOutputPO.setContent("succeed");
        executableOutputPO.setLogPath(file.getAbsolutePath());
        manager.updateJobOutputToHDFS(KylinConfig.getInstanceFromEnv().getJobTmpOutputStorePath("default",
                "e1ad7bb0-522e-456a-859d-2eab1df448de"), executableOutputPO);

        String sampleLog = "";
        try (InputStream allJobOutput = jobInfoService.getAllJobOutput("default",
                "e1ad7bb0-522e-456a-859d-2eab1df448de", "e1ad7bb0-522e-456a-859d-2eab1df448de");
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

    public void testFusionModelStopBatchJob() {

        String project = "streaming_test";
        FusionModelManager mgr = FusionModelManager.getInstance(getTestConfig(), project);
        ExecutableManager manager = ExecutableManager.getInstance(jobInfoService.getConfig(), project);

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
            jobInfoService.stopBatchJob(project, tableDesc);
            return null;
        }, project);
        AbstractExecutable job = manager.getJob(executable.getId());
        Assert.assertEquals(ExecutableState.DISCARDED, job.getStatus());

        // test no fusion model
        String table2 = "SSB.DATES";
        val tableDesc2 = tableMetadataManager.getTableDesc(table2);
        UnitOfWork.doInTransactionWithRetry(() -> {
            jobInfoService.stopBatchJob(project, tableDesc2);
            return null;
        }, project);
    }

    @Test
    public void testKillExistApplication() {
        ExecutableManager manager = ExecutableManager.getInstance(jobInfoService.getConfig(), getProject());
        SucceedChainedTestExecutable executable = new SucceedChainedTestExecutable();
        executable.setProject(getProject());
        addSegment(executable);
        val task = new NSparkExecutable();
        task.setProject(getProject());
        addSegment(task);
        executable.addTask(task);
        executable.setJobType(JobTypeEnum.INC_BUILD);
        manager.addJob(executable);
        jobInfoService.killExistApplication(executable);

        jobInfoService.killExistApplication(getProject(), executable.getId());
    }

    @Test
    public void testSetExceptionResolveAndCode() {
        val manager = ExecutableManager.getInstance(jobInfoService.getConfig(), project);
        val executable = new SucceedChainedTestExecutable();
        executable.setProject(project);
        executable.setId(RandomUtil.randomUUIDStr());
        executable.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        executable.setJobType(JobTypeEnum.INC_BUILD);
        manager.addJob(executable);

        val jobId = executable.getId();
        var failedStepId = RandomUtil.randomUUIDStr();
        var failedSegmentId = RandomUtil.randomUUIDStr();
        var failedStack = ExceptionUtils.getStackTrace(new NoRetryException("date format not match"));
        var failedReason = "date format not match";
        jobInfoService.updateJobError(project, jobId, failedStepId, failedSegmentId, failedStack, failedReason);

        ExecutableStepResponse executableStepResponse = new ExecutableStepResponse();
        jobInfoService.setExceptionResolveAndCodeAndReason(executable.getOutput(), executableStepResponse);
        Assert.assertEquals(JobExceptionResolve.JOB_DATE_FORMAT_NOT_MATCH_ERROR.toExceptionResolve().getResolve(),
                executableStepResponse.getFailedResolve());
        Assert.assertEquals(JobErrorCode.JOB_DATE_FORMAT_NOT_MATCH_ERROR.toErrorCode().getLocalizedString(),
                executableStepResponse.getFailedCode());
        Assert.assertEquals(JobExceptionReason.JOB_DATE_FORMAT_NOT_MATCH_ERROR.toExceptionReason().getReason(),
                executableStepResponse.getFailedReason());

        ErrorCode.setMsg("en");
        ExceptionResolve.setLang("en");
        jobInfoService.setExceptionResolveAndCodeAndReason(executable.getOutput(), executableStepResponse);
        Assert.assertEquals(JobExceptionResolve.JOB_DATE_FORMAT_NOT_MATCH_ERROR.toExceptionResolve().getResolve(),
                executableStepResponse.getFailedResolve());
        Assert.assertEquals(JobErrorCode.JOB_DATE_FORMAT_NOT_MATCH_ERROR.toErrorCode().getLocalizedString(),
                executableStepResponse.getFailedCode());
        Assert.assertEquals(JobExceptionReason.JOB_DATE_FORMAT_NOT_MATCH_ERROR.toExceptionReason().getReason(),
                executableStepResponse.getFailedReason());

        // test default reason / code / resolve
        manager.updateJobError(jobId, null, null, null, null);
        jobInfoService.updateJobError(project, jobId, failedStepId, failedSegmentId, failedStack, "test");
        jobInfoService.setExceptionResolveAndCodeAndReason(executable.getOutput(), executableStepResponse);
        Assert.assertEquals(JobExceptionResolve.JOB_BUILDING_ERROR.toExceptionResolve().getResolve(),
                executableStepResponse.getFailedResolve());
        Assert.assertEquals(JobErrorCode.JOB_BUILDING_ERROR.toErrorCode().getLocalizedString(),
                executableStepResponse.getFailedCode());
        Assert.assertEquals(JobExceptionReason.JOB_BUILDING_ERROR.toExceptionReason().getReason() + ": test",
                executableStepResponse.getFailedReason());

        ErrorCode.setMsg("en");
        ExceptionResolve.setLang("en");
        jobInfoService.setExceptionResolveAndCodeAndReason(executable.getOutput(), executableStepResponse);
        Assert.assertEquals(JobExceptionResolve.JOB_BUILDING_ERROR.toExceptionResolve().getResolve(),
                executableStepResponse.getFailedResolve());
        Assert.assertEquals(JobErrorCode.JOB_BUILDING_ERROR.toErrorCode().getLocalizedString(),
                executableStepResponse.getFailedCode());
        Assert.assertEquals(JobExceptionReason.JOB_BUILDING_ERROR.toExceptionReason().getReason() + ": test",
                executableStepResponse.getFailedReason());
    }

    @Test
    public void testHistoryTrackerUrl() {
        getTestConfig().setProperty("kylin.history-server.enable", "true");
        AbstractExecutable task = new FiveSecondSucceedTestExecutable();
        task.setProject("default");
        ExecutablePO po = ExecutableManager.toPO(task, task.getProject());
        po.getOutput().setStatus(ExecutableState.RUNNING.name());
        Map<String, String> waiteTimeMap = new HashMap<>();
        ExecutableState jobState = ExecutableState.RUNNING;
        ExecutableStepResponse result = jobInfoService.parseToExecutableStep(task, po, waiteTimeMap, jobState);
        assert !result.getInfo().containsKey(ExecutableConstants.SPARK_HISTORY_APP_URL);
        po.getOutput().getInfo().put(ExecutableConstants.YARN_APP_ID, "app-id");
        result = jobInfoService.parseToExecutableStep(task, po, waiteTimeMap, jobState);
        assert result.getInfo().containsKey(ExecutableConstants.SPARK_HISTORY_APP_URL);
        getTestConfig().setProperty("kylin.history-server.enable", "false");
        result = jobInfoService.parseToExecutableStep(task, po, waiteTimeMap, jobState);
        assert !result.getInfo().containsKey(ExecutableConstants.SPARK_HISTORY_APP_URL);

    }

    @Test
    public void testDiscardJobAndNotify() {
        ExecutableManager manager = ExecutableManager.getInstance(getTestConfig(), project);
        val job = new DefaultExecutable();
        job.setProject(project);
        job.setJobType(JobTypeEnum.INC_BUILD);
        manager.addJob(job);

        overwriteSystemProp("kylin.job.notification-enabled", "true");

        UnitOfWork.doInTransactionWithRetry(() -> {
            jobInfoService.updateJobStatus(job.getId(), ExecutableManager.toPO(job, project), project, "DISCARD");
            return null;
        }, project);

        Assert.assertTrue(containsLog("[Job Discarded] is not specified by user, not need to notify users."));
    }

    @Test
    public void testCheckJobStatus() {
        jobInfoService.checkJobStatus(Lists.newArrayList("RUNNING"));
        thrown.expect(KylinException.class);
        thrown.expectMessage(JOB_STATUS_ILLEGAL.getMsg());
        jobInfoService.checkJobStatus("UNKNOWN");
    }

    @Test
    public void testExecutableResponse() throws Exception {
        val modelManager = mock(NDataModelManager.class);

        Mockito.when(modelService.getManager(NDataModelManager.class, "default")).thenReturn(modelManager);
        NDataModel nDataModel = mock(NDataModel.class);
        Mockito.when(modelManager.getDataModelDesc(Mockito.anyString())).thenReturn(nDataModel);

        ExecutableManager executableManager = Mockito.spy(ExecutableManager.getInstance(getTestConfig(), "default"));
        Mockito.when(jobInfoService.getManager(ExecutableManager.class, "default")).thenReturn(executableManager);
        val mockJobs = mockDetailJobs(false);
        Mockito.when(executableManager.getAllJobs(Mockito.anyLong(), Mockito.anyLong())).thenReturn(mockJobs);
        for (ExecutablePO po : mockJobs) {
            AbstractExecutable exe = executableManager.fromPO(po);
            Mockito.when(executableManager.getJob(po.getId())).thenReturn(exe);
        }
        getTestConfig().setProperty("kylin.streaming.enabled", "false");
        // test size
        List<String> jobNames = Lists.newArrayList();
        JobFilter jobFilter = new JobFilter(Lists.newArrayList(), jobNames, 4, "", "", false, "default", "", true);
        List<ExecutableResponse> jobs = jobInfoService.listJobs(jobFilter);
        List<ExecutableResponse> executableResponses = jobInfoService.addOldParams(jobs);
        ExecutableResponse executable = executableResponses.get(0);
        Assert.assertEquals("", executable.getRelatedSegment());
        Assert.assertEquals(0, executable.getProgress(), 0);
        executable.getSteps().get(0).setStatus(JobStatusEnum.FINISHED);
        Assert.assertEquals(33, executable.getProgress(), 1);
        executable.setSteps(null);
        String uuid = UUID.randomUUID().toString();
        executable.setTargetSegments(Lists.newArrayList(uuid));
        Assert.assertEquals(0.0, executable.getProgress(), 0);
        Assert.assertEquals(uuid, executable.getRelatedSegment());
        executable.setTargetSegments(Collections.emptyList());
        Assert.assertEquals(0.0, executable.getProgress(), 0);
        Assert.assertEquals("", executable.getRelatedSegment());
    }

    @Test
    public void testParseToExecutableStepWithStepOutputNull() {
        AbstractExecutable task = new FiveSecondSucceedTestExecutable();
        task.setProject("default");
        ExecutableState jobState = ExecutableState.RUNNING;
        ExecutableStepResponse result = jobInfoService.parseToExecutableStep(task, null, new HashMap<>(), jobState);
        Assert.assertSame(PENDING, result.getStatus());
    }

    @Test
    public void testJobDiscard() {
        ExecutableManager executableManager = Mockito.mock(ExecutableManager.class);
        Mockito.when(jobInfoService.getManager(ExecutableManager.class, project)).thenReturn(executableManager);
        Mockito.doAnswer(invocation -> {
            // ensure unit of work transaction
            Assert.assertNotNull(UnitOfWork.get());
            return null;
        }).when(executableManager).discardJob(Mockito.any());
        jobInfoService.discardJobs(project, Lists.newArrayList("job1", "job2"));
        Mockito.verify(executableManager, Mockito.times(1)).discardJob("job1");
        Mockito.verify(executableManager, Mockito.times(1)).discardJob("job2");
    }

    @Test
    public void testSuicideJobOfModel() {
        ExecutableManager executableManager = Mockito.mock(ExecutableManager.class);
        Mockito.when(jobInfoService.getManager(ExecutableManager.class, project)).thenReturn(executableManager);
        Mockito.doAnswer(invocation -> {
            // ensure unit of work transaction
            Assert.assertNotNull(UnitOfWork.get());
            return null;
        }).when(executableManager).checkSuicideJobOfModel(Mockito.any(), Mockito.anyString());
        jobInfoService.checkSuicideJobOfModel(project, "test");
        Mockito.verify(executableManager, Mockito.times(1)).checkSuicideJobOfModel(project, "test");
    }
}

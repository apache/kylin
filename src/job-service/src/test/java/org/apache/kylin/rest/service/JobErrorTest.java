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

import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_UPDATE_JOB_STATUS;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.exception.ErrorCode;
import org.apache.kylin.common.exception.ExceptionReason;
import org.apache.kylin.common.exception.ExceptionResolve;
import org.apache.kylin.common.exception.JobErrorCode;
import org.apache.kylin.common.exception.JobExceptionReason;
import org.apache.kylin.common.exception.JobExceptionResolve;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ChainedExecutable;
import org.apache.kylin.job.execution.ChainedStageExecutable;
import org.apache.kylin.job.execution.DefaultOutput;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.execution.StageBase;
import org.apache.kylin.job.execution.SucceedChainedTestExecutable;
import org.apache.kylin.job.execution.SucceedTestExecutable;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.engine.spark.job.NSparkExecutable;
import org.apache.kylin.engine.spark.job.step.NStageForBuild;
import org.apache.kylin.engine.spark.job.step.NStageForMerge;
import org.apache.kylin.engine.spark.job.step.NStageForSnapshot;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.rest.response.ExecutableStepResponse;
import org.apache.spark.application.NoRetryException;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Maps;

import lombok.val;
import lombok.var;

public class JobErrorTest extends NLocalFileMetadataTestCase {
    @InjectMocks
    private final JobService jobService = Mockito.spy(new JobService());

    @Mock
    private final ModelService modelService = Mockito.spy(ModelService.class);

    @Mock
    private final AclUtil aclUtil = Mockito.spy(AclUtil.class);

    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    private ProjectService projectService = Mockito.spy(ProjectService.class);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

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
    public void testWrapWithExecuteException() throws ExecuteException {
        val manager = NExecutableManager.getInstance(jobService.getConfig(), getProject());
        val executable = new SucceedChainedTestExecutable();
        executable.setProject(getProject());
        executable.setId(RandomUtil.randomUUIDStr());
        executable.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val sparkExecutable = new NSparkExecutable();
        sparkExecutable.setProject(getProject());
        sparkExecutable.setParam(NBatchConstants.P_SEGMENT_IDS, RandomUtil.randomUUIDStr());
        sparkExecutable.setParam(NBatchConstants.P_INDEX_COUNT, "10");
        sparkExecutable.setId(RandomUtil.randomUUIDStr());
        executable.addTask(sparkExecutable);
        manager.addJob(executable);

        val jobId = executable.getId();
        var failedStepId = sparkExecutable.getId();

        sparkExecutable.wrapWithExecuteException(() -> null);
        var output = manager.getJob(jobId).getOutput();
        Assert.assertNull(output.getFailedStepId());

        try {
            sparkExecutable.wrapWithExecuteException(() -> {
                throw new KylinException(FAILED_UPDATE_JOB_STATUS, "test");
            });
            Assert.fail();
        } catch (ExecuteException e) {
            output = manager.getJob(jobId).getOutput();
            Assert.assertEquals(failedStepId, output.getFailedStepId());
        }
    }

    @Test
    public void testGetExceptionCode() throws IOException {
        val manager = NExecutableManager.getInstance(jobService.getConfig(), getProject());
        val executable = new SucceedChainedTestExecutable();
        executable.setProject(getProject());
        executable.setId(RandomUtil.randomUUIDStr());
        executable.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        manager.addJob(executable);

        val exceptionCodeStream = getClass().getClassLoader().getResource(JobService.EXCEPTION_CODE_PATH).openStream();
        val map = JsonUtil.readValue(exceptionCodeStream, Map.class);
        var expectedCode = JobService.EXCEPTION_CODE_DEFAULT;

        var exceptionCode = jobService.getExceptionCode(executable.getOutput());
        Assert.assertEquals(expectedCode, exceptionCode);

        val project = getProject();
        val jobId = executable.getId();
        var failedStepId = RandomUtil.randomUUIDStr();
        var failedSegmentId = RandomUtil.randomUUIDStr();
        var failedStack = ExceptionUtils.getStackTrace(new NoRetryException("date format not match"));
        var failedReason = "date format not match";

        jobService.updateJobError(project, jobId, failedStepId, failedSegmentId, null, null);
        exceptionCode = jobService.getExceptionCode(executable.getOutput());
        Assert.assertEquals(expectedCode, exceptionCode);

        jobService.updateJobError(project, jobId, failedStepId, failedSegmentId, failedStack, null);
        exceptionCode = jobService.getExceptionCode(executable.getOutput());
        expectedCode = String.valueOf(map.get(failedReason));
        Assert.assertEquals(expectedCode, exceptionCode);

        jobService.updateJobError(project, jobId, failedStepId, failedSegmentId, "test", failedReason);
        exceptionCode = jobService.getExceptionCode(executable.getOutput());
        Assert.assertEquals(expectedCode, exceptionCode);

    }

    @Test
    public void testSetExceptionResolveAndCode() {
        val manager = NExecutableManager.getInstance(jobService.getConfig(), getProject());
        val executable = new SucceedChainedTestExecutable();
        executable.setProject(getProject());
        executable.setId(RandomUtil.randomUUIDStr());
        executable.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        manager.addJob(executable);

        val project = getProject();
        val jobId = executable.getId();
        var failedStepId = RandomUtil.randomUUIDStr();
        var failedSegmentId = RandomUtil.randomUUIDStr();
        var failedStack = ExceptionUtils.getStackTrace(new NoRetryException("date format not match"));
        var failedReason = "date format not match";
        jobService.updateJobError(project, jobId, failedStepId, failedSegmentId, failedStack, failedReason);

        ExecutableStepResponse executableStepResponse = new ExecutableStepResponse();
        jobService.setExceptionResolveAndCodeAndReason(executable.getOutput(), executableStepResponse);
        Assert.assertEquals(JobExceptionResolve.JOB_DATE_FORMAT_NOT_MATCH_ERROR.toExceptionResolve().getResolve(),
                executableStepResponse.getFailedResolve());
        Assert.assertEquals(JobErrorCode.JOB_DATE_FORMAT_NOT_MATCH_ERROR.toErrorCode().getLocalizedString(),
                executableStepResponse.getFailedCode());
        Assert.assertEquals(JobExceptionReason.JOB_DATE_FORMAT_NOT_MATCH_ERROR.toExceptionReason().getReason(),
                executableStepResponse.getFailedReason());

        ErrorCode.setMsg("en");
        ExceptionResolve.setLang("en");
        jobService.setExceptionResolveAndCodeAndReason(executable.getOutput(), executableStepResponse);
        Assert.assertEquals(JobExceptionResolve.JOB_DATE_FORMAT_NOT_MATCH_ERROR.toExceptionResolve().getResolve(),
                executableStepResponse.getFailedResolve());
        Assert.assertEquals(JobErrorCode.JOB_DATE_FORMAT_NOT_MATCH_ERROR.toErrorCode().getLocalizedString(),
                executableStepResponse.getFailedCode());
        Assert.assertEquals(JobExceptionReason.JOB_DATE_FORMAT_NOT_MATCH_ERROR.toExceptionReason().getReason(),
                executableStepResponse.getFailedReason());

        // test default reason / code / resolve
        manager.updateJobError(jobId, null, null, null, null);
        jobService.updateJobError(project, jobId, failedStepId, failedSegmentId, failedStack, "test");
        jobService.setExceptionResolveAndCodeAndReason(executable.getOutput(), executableStepResponse);
        Assert.assertEquals(JobExceptionResolve.JOB_BUILDING_ERROR.toExceptionResolve().getResolve(),
                executableStepResponse.getFailedResolve());
        Assert.assertEquals(JobErrorCode.JOB_BUILDING_ERROR.toErrorCode().getLocalizedString(),
                executableStepResponse.getFailedCode());
        Assert.assertEquals(JobExceptionReason.JOB_BUILDING_ERROR.toExceptionReason().getReason() + ": test",
                executableStepResponse.getFailedReason());

        ErrorCode.setMsg("en");
        ExceptionResolve.setLang("en");
        jobService.setExceptionResolveAndCodeAndReason(executable.getOutput(), executableStepResponse);
        Assert.assertEquals(JobExceptionResolve.JOB_BUILDING_ERROR.toExceptionResolve().getResolve(),
                executableStepResponse.getFailedResolve());
        Assert.assertEquals(JobErrorCode.JOB_BUILDING_ERROR.toErrorCode().getLocalizedString(),
                executableStepResponse.getFailedCode());
        Assert.assertEquals(JobExceptionReason.JOB_BUILDING_ERROR.toExceptionReason().getReason() + ": test",
                executableStepResponse.getFailedReason());
    }

    @Test
    public void testUpdateJobError() {
        val manager = NExecutableManager.getInstance(jobService.getConfig(), getProject());
        val executable = new SucceedChainedTestExecutable();
        executable.setProject(getProject());
        executable.setId(RandomUtil.randomUUIDStr());
        executable.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        manager.addJob(executable);

        val project = getProject();
        val jobId = executable.getId();
        var failedStepId = RandomUtil.randomUUIDStr();
        var failedSegmentId = RandomUtil.randomUUIDStr();
        var failedStack = ExceptionUtils.getStackTrace(new KylinException(FAILED_UPDATE_JOB_STATUS, "test"));
        var failedReason = new KylinException(FAILED_UPDATE_JOB_STATUS, "test").getMessage();

        jobService.updateJobError(project, jobId, failedStepId, failedSegmentId, failedStack, failedReason);
        var output = manager.getJob(jobId).getOutput();
        Assert.assertEquals(failedStepId, output.getFailedStepId());
        Assert.assertEquals(failedSegmentId, output.getFailedSegmentId());
        Assert.assertEquals(failedStack, output.getFailedStack());
        Assert.assertEquals(failedReason, output.getFailedReason());

        jobService.updateJobError(project, jobId, "", failedSegmentId, failedStack, failedReason);
        output = manager.getJob(jobId).getOutput();
        Assert.assertEquals(failedStepId, output.getFailedStepId());
        Assert.assertEquals(failedSegmentId, output.getFailedSegmentId());
        Assert.assertEquals(failedStack, output.getFailedStack());
        Assert.assertEquals(failedReason, output.getFailedReason());
    }

    @Test
    public void testUpdateJobErrorManager() throws InterruptedException {
        val manager = NExecutableManager.getInstance(jobService.getConfig(), getProject());
        val executable = new SucceedChainedTestExecutable();
        executable.setProject(getProject());
        executable.setId(RandomUtil.randomUUIDStr());
        executable.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        manager.addJob(executable);

        val jobId = executable.getId();
        var failedStepId = RandomUtil.randomUUIDStr();
        var failedSegmentId = RandomUtil.randomUUIDStr();
        var failedStack = ExceptionUtils.getStackTrace(new KylinException(FAILED_UPDATE_JOB_STATUS, "test"));
        var failedReason = new KylinException(FAILED_UPDATE_JOB_STATUS, "test").getMessage();

        manager.updateJobError(jobId, jobId, failedSegmentId, failedStack, failedReason);
        var output = manager.getJob(jobId).getOutput();
        Assert.assertNotNull(output.getFailedStepId());
        Assert.assertNotNull(output.getFailedSegmentId());
        Assert.assertNotNull(output.getFailedStack());
        Assert.assertNotNull(output.getFailedReason());

        manager.updateJobError(jobId, null, null, null, null);
        manager.updateJobError(jobId, failedStepId, failedSegmentId, failedStack, failedReason);
        output = manager.getJob(jobId).getOutput();
        Assert.assertEquals(failedStepId, output.getFailedStepId());
        Assert.assertEquals(failedSegmentId, output.getFailedSegmentId());
        Assert.assertEquals(failedStack, output.getFailedStack());
        Assert.assertEquals(failedReason, output.getFailedReason());

        manager.updateJobError(jobId, null, null, null, null);
        manager.updateJobError(jobId, "", failedSegmentId, failedStack, failedReason);
        output = manager.getJob(jobId).getOutput();
        Assert.assertEquals("", output.getFailedStepId());
        Assert.assertEquals(failedSegmentId, output.getFailedSegmentId());
        Assert.assertEquals(failedStack, output.getFailedStack());
        Assert.assertEquals(failedReason, output.getFailedReason());

        manager.updateJobError(jobId, null, null, null, null);
        manager.updateJobError(jobId, failedStepId, null, failedStack, failedReason);
        output = manager.getJob(jobId).getOutput();
        Assert.assertEquals(failedStepId, output.getFailedStepId());
        Assert.assertNull(output.getFailedSegmentId());
        Assert.assertEquals(failedStack, output.getFailedStack());
        Assert.assertEquals(failedReason, output.getFailedReason());
    }

    @Test
    public void testGetJobDetail() {
        val segmentId = RandomUtil.randomUUIDStr();
        val segmentId2 = RandomUtil.randomUUIDStr();
        val errMsg = "test output";

        val manager = NExecutableManager.getInstance(jobService.getConfig(), getProject());
        val executable = new SucceedChainedTestExecutable();
        executable.setProject(getProject());
        executable.setId(RandomUtil.randomUUIDStr());
        executable.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        val sparkExecutable = new NSparkExecutable();
        sparkExecutable.setProject(getProject());
        sparkExecutable.setParam(NBatchConstants.P_SEGMENT_IDS, segmentId + "," + segmentId2);
        sparkExecutable.setParam(NBatchConstants.P_INDEX_COUNT, "10");
        sparkExecutable.setId(RandomUtil.randomUUIDStr());
        executable.addTask(sparkExecutable);

        val build1 = new NStageForBuild(RandomUtil.randomUUIDStr());
        build1.setProject(getProject());
        val build2 = new NStageForMerge(RandomUtil.randomUUIDStr());
        build2.setProject(getProject());
        val build3 = new NStageForSnapshot(RandomUtil.randomUUIDStr());
        build3.setProject(getProject());
        final StageBase logicStep1 = (StageBase) sparkExecutable.addStage(build1);
        final StageBase logicStep2 = (StageBase) sparkExecutable.addStage(build2);
        final StageBase logicStep3 = (StageBase) sparkExecutable.addStage(build3);
        sparkExecutable.setStageMap();

        manager.addJob(executable);

        val jobId = executable.getId();
        var failedStepId = logicStep2.getId();
        var failedSegmentId = segmentId;
        var failedStack = ExceptionUtils.getStackTrace(new NoRetryException("date format not match"));
        var failedReason = "date format not match";
        var failedResolve = JobExceptionResolve.JOB_DATE_FORMAT_NOT_MATCH_ERROR.toExceptionResolve();
        var failedCode = JobErrorCode.JOB_DATE_FORMAT_NOT_MATCH_ERROR.toErrorCode();
        manager.updateJobOutput(sparkExecutable.getId(), ExecutableState.ERROR, null, null, "test output");
        manager.updateJobError(jobId, failedStepId, failedSegmentId, failedStack, failedReason);

        ExceptionReason.setLang("en");
        var jobDetail = jobService.getJobDetail(getProject(), executable.getId());
        Assert.assertEquals(1, jobDetail.size());
        var stepResponse = jobDetail.get(0);
        Assert.assertEquals(failedStepId, stepResponse.getFailedStepId());
        Assert.assertEquals(failedSegmentId, stepResponse.getFailedSegmentId());
        Assert.assertEquals(failedStack, stepResponse.getFailedStack());
        Assert.assertEquals(JobExceptionReason.JOB_DATE_FORMAT_NOT_MATCH_ERROR.toExceptionReason().getReason(),
                stepResponse.getFailedReason());
        Assert.assertEquals(logicStep2.getName(), stepResponse.getFailedStepName());
        Assert.assertEquals(failedResolve.getResolve(), stepResponse.getFailedResolve());
        Assert.assertEquals(failedCode.getLocalizedString(), stepResponse.getFailedCode());
    }

    @Test
    public void testGetDuration() throws InterruptedException {
        val manager = NExecutableManager.getInstance(jobService.getConfig(), getProject());
        val executable = new SucceedTestExecutable();
        executable.setProject(getProject());
        executable.setId(RandomUtil.randomUUIDStr());
        executable.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        manager.addJob(executable);

        var output = manager.getOutput(executable.getId());
        final long[] duration = { AbstractExecutable.getDuration(output) };
        Assert.assertEquals(0, duration[0]);

        ((DefaultOutput) output).setStartTime(System.currentTimeMillis());
        org.apache.kylin.job.execution.Output finalOutput = output;
        Awaitility.await().atMost(1000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            duration[0] = AbstractExecutable.getDuration(finalOutput);
            Assert.assertTrue(duration[0] >= 10);
        });

        manager.updateJobOutput(executable.getId(), ExecutableState.RUNNING);
        output = manager.getOutput(executable.getId());
        duration[0] = AbstractExecutable.getDuration(output);
        Assert.assertTrue((System.currentTimeMillis() - output.getStartTime()) >= duration[0]);

        val durationFromExecutable = executable.getDuration();
        Assert.assertTrue(durationFromExecutable >= duration[0]);
    }

    @Test
    public void testGetDurationWithoutWaiteTimeFromTwoSegment() throws JsonProcessingException {
        val segmentId = RandomUtil.randomUUIDStr();
        val segmentId2 = RandomUtil.randomUUIDStr();

        val manager = NExecutableManager.getInstance(jobService.getConfig(), getProject());
        val executable = new SucceedChainedTestExecutable();
        executable.setProject(getProject());
        executable.setId(RandomUtil.randomUUIDStr());
        executable.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        val sparkExecutable = new NSparkExecutable();
        sparkExecutable.setProject(getProject());
        sparkExecutable.setParam(NBatchConstants.P_SEGMENT_IDS, segmentId + "," + segmentId2);
        sparkExecutable.setParam(NBatchConstants.P_INDEX_COUNT, "10");
        sparkExecutable.setId(RandomUtil.randomUUIDStr());
        executable.addTask(sparkExecutable);

        val build1 = new NStageForBuild(RandomUtil.randomUUIDStr());
        build1.setProject(getProject());
        val build2 = new NStageForMerge(RandomUtil.randomUUIDStr());
        build2.setProject(getProject());
        val build3 = new NStageForSnapshot(RandomUtil.randomUUIDStr());
        build3.setProject(getProject());
        final StageBase logicStep1 = (StageBase) sparkExecutable.addStage(build1);
        final StageBase logicStep2 = (StageBase) sparkExecutable.addStage(build2);
        final StageBase logicStep3 = (StageBase) sparkExecutable.addStage(build3);
        sparkExecutable.setStageMap();

        manager.addJob(executable);

        manager.updateJobOutput(executable.getId(), ExecutableState.RUNNING);
        manager.updateJobOutput(sparkExecutable.getId(), ExecutableState.RUNNING);
        manager.updateStageStatus(logicStep1.getId(), null, ExecutableState.RUNNING, null, null);
        manager.updateStageStatus(logicStep2.getId(), null, ExecutableState.RUNNING, null, null);
        manager.updateStageStatus(logicStep3.getId(), null, ExecutableState.RUNNING, null, null);

        val durationWithoutWaiteTime = executable.getDurationFromStepOrStageDurationSum();

        val sumDuration = ((ChainedExecutable) executable).getTasks().stream().map(exe -> exe.getDuration())
                .mapToLong(Long::valueOf).sum();
        Assert.assertTrue(sumDuration >= durationWithoutWaiteTime);
    }

    @Test
    public void testGetDurationWithoutWaiteTimeFromSingleSegment() throws JsonProcessingException {
        val segmentId = RandomUtil.randomUUIDStr();

        val manager = NExecutableManager.getInstance(jobService.getConfig(), getProject());
        val executable = new SucceedChainedTestExecutable();
        executable.setProject(getProject());
        executable.setId(RandomUtil.randomUUIDStr());
        executable.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        val sparkExecutable = new NSparkExecutable();
        sparkExecutable.setProject(getProject());
        sparkExecutable.setParam(NBatchConstants.P_SEGMENT_IDS, segmentId);
        sparkExecutable.setParam(NBatchConstants.P_INDEX_COUNT, "10");
        sparkExecutable.setId(RandomUtil.randomUUIDStr());
        executable.addTask(sparkExecutable);

        val build1 = new NStageForBuild(RandomUtil.randomUUIDStr());
        build1.setProject(getProject());
        val build2 = new NStageForMerge(RandomUtil.randomUUIDStr());
        build2.setProject(getProject());
        val build3 = new NStageForSnapshot(RandomUtil.randomUUIDStr());
        build3.setProject(getProject());
        final StageBase logicStep1 = (StageBase) sparkExecutable.addStage(build1);
        final StageBase logicStep2 = (StageBase) sparkExecutable.addStage(build2);
        final StageBase logicStep3 = (StageBase) sparkExecutable.addStage(build3);
        sparkExecutable.setStageMap();

        manager.addJob(executable);

        Map<String, String> info = Maps.newHashMap();

        manager.updateJobOutput(executable.getId(), ExecutableState.RUNNING, info);
        manager.updateJobOutput(sparkExecutable.getId(), ExecutableState.RUNNING);
        manager.updateStageStatus(logicStep1.getId(), null, ExecutableState.RUNNING, null, null);
        manager.updateStageStatus(logicStep2.getId(), null, ExecutableState.RUNNING, null, null);
        manager.updateStageStatus(logicStep3.getId(), null, ExecutableState.RUNNING, null, null);

        val durationWithoutWaiteTime = executable.getDurationFromStepOrStageDurationSum();

        val stagesMap = ((ChainedStageExecutable) ((ChainedExecutable) executable).getTasks().get(0)).getStagesMap();

        Awaitility.await().atMost(1000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            var sumDuration = 0L;
            for (Map.Entry<String, List<StageBase>> entry : stagesMap.entrySet()) {
                sumDuration = entry.getValue().stream().map(stage -> stage.getOutput(entry.getKey()))
                        .map(AbstractExecutable::getDuration).mapToLong(Long::valueOf).sum();
            }
            Assert.assertTrue(sumDuration != 0);
            Assert.assertTrue(sumDuration >= durationWithoutWaiteTime);
        });
    }
}

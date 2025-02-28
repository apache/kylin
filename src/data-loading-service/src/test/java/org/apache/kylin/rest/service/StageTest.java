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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.engine.spark.job.NSparkExecutable;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.job.JobContext;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.dao.ExecutableOutputPO;
import org.apache.kylin.job.dao.JobInfoDao;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ChainedExecutable;
import org.apache.kylin.job.execution.ChainedStageExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.StageExecutable;
import org.apache.kylin.job.execution.SucceedChainedTestExecutable;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.response.ExecutableStepResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import lombok.val;
import lombok.var;

public class StageTest extends NLocalFileMetadataTestCase {

    @InjectMocks
    private final JobInfoService jobInfoService = Mockito.spy(new JobInfoService());

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
    public void setUp() {
        JobContextUtil.cleanUp();
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        createTestMetadata();
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);

        JobInfoDao jobInfoDao = JobContextUtil.getJobInfoDao(getTestConfig());
        ReflectionTestUtils.setField(jobInfoService, "jobInfoDao", jobInfoDao);
        ReflectionTestUtils.setField(jobInfoService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(jobInfoService, "projectService", projectService);

        ReflectionTestUtils.setField(jobInfoService, "modelService", modelService);

        JobContext jobContext = JobContextUtil.getJobContext(getTestConfig());
        try {
            // need not start job scheduler
            jobContext.destroy();
        } catch (Exception e) {
            throw new RuntimeException("Destroy jobContext failed.", e);
        }
    }

    @After
    public void tearDown() {
        JobContextUtil.cleanUp();
        cleanupTestMetadata();
    }

    private String getProject() {
        return "default";
    }

    @Test
    public void testGetDiscardJobDetail() {
        val segmentId = RandomUtil.randomUUIDStr();
        val segmentId2 = RandomUtil.randomUUIDStr();
        val errMsg = "test output";

        val manager = ExecutableManager.getInstance(getTestConfig(), getProject());
        val executable = new SucceedChainedTestExecutable();
        executable.setProject(getProject());
        executable.setId(RandomUtil.randomUUIDStr());
        executable.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        executable.setJobType(JobTypeEnum.INC_BUILD);

        val sparkExecutable = new NSparkExecutable();
        sparkExecutable.setProject(getProject());
        sparkExecutable.setParam(NBatchConstants.P_SEGMENT_IDS, segmentId + "," + segmentId2);
        sparkExecutable.setParam(NBatchConstants.P_INDEX_COUNT, "10");
        sparkExecutable.setId(RandomUtil.randomUUIDStr());
        executable.addTask(sparkExecutable);

        val build1 = new StageExecutable();
        build1.setProject(getProject());
        val build2 = new StageExecutable();
        build2.setProject(getProject());
        val build3 = new StageExecutable();
        build3.setProject(getProject());
        final StageExecutable logicStep1 = (StageExecutable) sparkExecutable.addStage(build1);
        final StageExecutable logicStep2 = (StageExecutable) sparkExecutable.addStage(build2);
        final StageExecutable logicStep3 = (StageExecutable) sparkExecutable.addStage(build3);
        sparkExecutable.setStageMap();

        manager.addJob(executable);

        manager.discardJob(executable.getId());

        val jobDetail = jobInfoService.getJobDetail(getProject(), executable.getId());
        jobDetail.forEach(res -> {
            Assert.assertEquals(JobStatusEnum.DISCARDED, res.getStatus());
            if (CollectionUtils.isNotEmpty(res.getSubStages())) {
                res.getSubStages().forEach(subRes -> Assert.assertEquals(JobStatusEnum.DISCARDED, subRes.getStatus()));
            }
            if (MapUtils.isNotEmpty(res.getSegmentSubStages())) {
                val segmentStages = res.getSegmentSubStages();
                val segmentIds = Sets.newHashSet(segmentId, segmentId2);
                val stageIds = Sets.newHashSet(logicStep1.getId(), logicStep2.getId(), logicStep3.getId());
                Assert.assertTrue(segmentStages.keySet().containsAll(segmentIds));
                Assert.assertTrue(segmentIds.containsAll(segmentStages.keySet()));
                for (Map.Entry<String, ExecutableStepResponse.SubStages> entry : segmentStages.entrySet()) {
                    entry.getValue().getStage().forEach(stageRes -> {
                        Assert.assertTrue(stageIds.contains(stageRes.getId()));
                        Assert.assertEquals(JobStatusEnum.DISCARDED, stageRes.getStatus());
                    });
                }
            }
        });
    }

    @Test
    public void testConvertToExecutableState() {
        ExecutableState executableState = jobInfoService.convertToExecutableState(null);
        Assert.assertNull(executableState);
        executableState = jobInfoService.convertToExecutableState(ExecutableState.PAUSED.toString());
        Assert.assertEquals(ExecutableState.PAUSED, executableState);
    }

    @Test
    public void testUpdateStagePaused() throws ExecuteException {
        val segmentId = RandomUtil.randomUUIDStr();
        val segmentId2 = RandomUtil.randomUUIDStr();
        val errMsg = "test output";

        val manager = ExecutableManager.getInstance(getTestConfig(), getProject());
        val executable = new SucceedChainedTestExecutable();
        executable.setProject(getProject());
        executable.setId(RandomUtil.randomUUIDStr());
        executable.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        executable.setJobType(JobTypeEnum.INC_BUILD);

        val sparkExecutable = new NSparkExecutable();
        sparkExecutable.setProject(getProject());
        sparkExecutable.setParam(NBatchConstants.P_SEGMENT_IDS, segmentId + "," + segmentId2);
        sparkExecutable.setParam(NBatchConstants.P_INDEX_COUNT, "10");
        sparkExecutable.setId(RandomUtil.randomUUIDStr());
        executable.addTask(sparkExecutable);

        val build1 = new StageExecutable();
        build1.setProject(getProject());
        Assert.assertNull(build1.doWork(null));
        val build2 = new StageExecutable(RandomUtil.randomUUIDStr());
        build2.setProject(getProject());
        Assert.assertNull(build2.doWork(null));
        val build3 = new StageExecutable(new Object());
        build3.setProject(getProject());
        Assert.assertNull(build3.doWork(null));
        final StageExecutable logicStep1 = (StageExecutable) sparkExecutable.addStage(build1);
        final StageExecutable logicStep2 = (StageExecutable) sparkExecutable.addStage(build2);
        final StageExecutable logicStep3 = (StageExecutable) sparkExecutable.addStage(build3);
        sparkExecutable.setStageMap();

        manager.addJob(executable);

        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.RUNNING, null, null, false);

        manager.updateStagePaused(executable);

        var output1 = manager.getOutput(logicStep1.getId(), segmentId);
        Assert.assertEquals(ExecutableState.PAUSED, output1.getState());
        var output2 = manager.getOutput(logicStep2.getId(), segmentId);
        Assert.assertEquals(ExecutableState.READY, output2.getState());
        var output3 = manager.getOutput(logicStep3.getId(), segmentId);
        Assert.assertEquals(ExecutableState.READY, output3.getState());

        var output12 = manager.getOutput(logicStep1.getId(), segmentId2);
        Assert.assertEquals(ExecutableState.READY, output12.getState());
        var output22 = manager.getOutput(logicStep2.getId(), segmentId2);
        Assert.assertEquals(ExecutableState.READY, output22.getState());
        var output32 = manager.getOutput(logicStep3.getId(), segmentId2);
        Assert.assertEquals(ExecutableState.READY, output32.getState());
    }

    @Test
    public void testGetWaiteTime() throws IOException {
        val segmentId = RandomUtil.randomUUIDStr();
        val segmentId2 = RandomUtil.randomUUIDStr();
        val errMsg = "test output";

        val manager = ExecutableManager.getInstance(getTestConfig(), getProject());
        val executable = new SucceedChainedTestExecutable();
        executable.setProject(getProject());
        executable.setId(RandomUtil.randomUUIDStr());
        executable.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        executable.setJobType(JobTypeEnum.INC_BUILD);

        val sparkExecutable = new NSparkExecutable();
        sparkExecutable.setProject(getProject());
        sparkExecutable.setParam(NBatchConstants.P_SEGMENT_IDS, segmentId + "," + segmentId2);
        sparkExecutable.setParam(NBatchConstants.P_INDEX_COUNT, "10");
        sparkExecutable.setId(RandomUtil.randomUUIDStr());
        executable.addTask(sparkExecutable);

        val build1 = new StageExecutable();
        build1.setProject(getProject());
        val build2 = new StageExecutable();
        build2.setProject(getProject());
        val build3 = new StageExecutable();
        build3.setProject(getProject());
        final StageExecutable logicStep1 = (StageExecutable) sparkExecutable.addStage(build1);
        final StageExecutable logicStep2 = (StageExecutable) sparkExecutable.addStage(build2);
        final StageExecutable logicStep3 = (StageExecutable) sparkExecutable.addStage(build3);
        sparkExecutable.setStageMap();

        manager.addJob(executable);

        manager.updateJobOutput(executable.getId(), ExecutableState.PENDING, null, null, null);
        manager.updateJobOutput(executable.getId(), ExecutableState.RUNNING, null, null, null);
        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.RUNNING, null, null, false);
        manager.updateJobOutput(executable.getId(), ExecutableState.SUCCEED, null, null, null);
        manager.makeStageSuccess(sparkExecutable.getId());

        var info = manager.getWaiteTime(manager.getExecutablePO(executable.getJobId()), executable);
        Map<String, String> waiteTime = JsonUtil.readValueAsMap(info.getOrDefault(NBatchConstants.P_WAITE_TIME, "{}"));
        Assert.assertEquals(3, waiteTime.size());
        Assert.assertTrue(Long.parseLong(waiteTime.get(sparkExecutable.getId())) > 0);
        Assert.assertTrue(Long.parseLong(waiteTime.get(segmentId)) > 0);
        Assert.assertEquals(0, Long.parseLong(waiteTime.get(segmentId2)));
    }

    @Test
    public void testUpdateStageStatus() {
        val segmentId = RandomUtil.randomUUIDStr();
        val segmentId2 = RandomUtil.randomUUIDStr();
        val errMsg = "test output";

        val manager = ExecutableManager.getInstance(getTestConfig(), getProject());
        val executable = new SucceedChainedTestExecutable();
        executable.setId(RandomUtil.randomUUIDStr());
        executable.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        executable.setJobType(JobTypeEnum.INC_BUILD);

        val sparkExecutable = new NSparkExecutable();
        sparkExecutable.setParam(NBatchConstants.P_SEGMENT_IDS, segmentId + "," + segmentId2);
        sparkExecutable.setParam(NBatchConstants.P_INDEX_COUNT, "10");
        sparkExecutable.setId(RandomUtil.randomUUIDStr());
        executable.addTask(sparkExecutable);

        val build1 = new StageExecutable();
        val build2 = new StageExecutable();
        val build3 = new StageExecutable();
        final StageExecutable logicStep1 = (StageExecutable) sparkExecutable.addStage(build1);
        final StageExecutable logicStep2 = (StageExecutable) sparkExecutable.addStage(build2);
        final StageExecutable logicStep3 = (StageExecutable) sparkExecutable.addStage(build3);
        sparkExecutable.setStageMap();

        manager.addJob(executable);

        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.SUCCEED, null, errMsg);
        var output1 = manager.getOutput(logicStep1.getId(), segmentId);
        Assert.assertEquals(ExecutableState.SUCCEED, output1.getState());
        Assert.assertEquals(output1.getShortErrMsg(), errMsg);
        Assert.assertTrue(MapUtils.isEmpty(output1.getExtra()));

        manager.updateStageStatus(logicStep1.getId(), null, ExecutableState.ERROR, null, errMsg);
        output1 = manager.getOutput(logicStep1.getId(), segmentId);
        Assert.assertEquals(ExecutableState.SUCCEED, output1.getState());
        Assert.assertEquals(output1.getShortErrMsg(), errMsg);
        Assert.assertTrue(MapUtils.isEmpty(output1.getExtra()));
        var output2 = manager.getOutput(logicStep1.getId(), segmentId2);
        Assert.assertEquals(ExecutableState.ERROR, output2.getState());
        Assert.assertEquals(output2.getShortErrMsg(), errMsg);
        Assert.assertTrue(MapUtils.isEmpty(output2.getExtra()));

        var outputLogicStep2 = manager.getOutput(logicStep2.getId(), segmentId);
        Assert.assertEquals(ExecutableState.READY, outputLogicStep2.getState());
        Assert.assertNull(outputLogicStep2.getShortErrMsg());
        Assert.assertTrue(MapUtils.isEmpty(outputLogicStep2.getExtra()));
    }

    /**
     * useless for yinglong
     */
    @Ignore
    @Test
    public void testUpdateStageStatusNoSaveCache() {
        val segmentId = RandomUtil.randomUUIDStr();
        val segmentId2 = RandomUtil.randomUUIDStr();

        val manager = ExecutableManager.getInstance(jobInfoService.getConfig(), getProject());
        val executable = new SucceedChainedTestExecutable();

        executable.setId(RandomUtil.randomUUIDStr());
        executable.setJobType(JobTypeEnum.INC_BUILD);

        val sparkExecutable = new NSparkExecutable();
        sparkExecutable.setParam(NBatchConstants.P_SEGMENT_IDS, segmentId + "," + segmentId2);
        sparkExecutable.setId(RandomUtil.randomUUIDStr());
        executable.addTask(sparkExecutable);

        val build1 = new StageExecutable();
        val build2 = new StageExecutable();
        val build3 = new StageExecutable();
        sparkExecutable.addStage(build1);
        sparkExecutable.addStage(build2);
        sparkExecutable.addStage(build3);
        sparkExecutable.setStageMap();

        manager.addJob(executable);

        List<AbstractExecutable> tasks = executable.getTasks();
        tasks.forEach(task -> {
            final Map<String, List<StageExecutable>> tasksMap = ((ChainedStageExecutable) task).getStagesMap();
            for (Map.Entry<String, List<StageExecutable>> entry : tasksMap.entrySet()) {
                Optional.ofNullable(entry.getValue()).orElse(Lists.newArrayList())//
                        .forEach(stage -> //
                manager.updateStageStatus(stage.getId(), entry.getKey(), ExecutableState.DISCARDED, null, null));
            }
        });

        Assert.assertEquals(1, manager.getAllJobs().get(0).getMvcc());
    }

    @Test
    public void testSetStageOutput() {
        ExecutableManager manager = ExecutableManager.getInstance(getTestConfig(), getProject());

        var taskOrJobId = RandomUtil.randomUUIDStr();
        var jobOutput = new ExecutableOutputPO();
        var newStatus = ExecutableState.RUNNING;
        Map<String, String> updateInfo = Maps.newHashMap();
        var failedMsg = "123";
        var isRestart = false;

        jobOutput.setStatus(ExecutableState.PAUSED.toString());
        newStatus = ExecutableState.ERROR;
        var flag = manager.setStageOutput(jobOutput, taskOrJobId, newStatus, updateInfo, failedMsg, isRestart);
        Assert.assertFalse(flag);
        Assert.assertEquals("PAUSED", jobOutput.getStatus());

        jobOutput.setStatus(ExecutableState.SKIP.toString());
        newStatus = ExecutableState.SUCCEED;
        flag = manager.setStageOutput(jobOutput, taskOrJobId, newStatus, updateInfo, failedMsg, isRestart);
        Assert.assertFalse(flag);
        Assert.assertEquals("SKIP", jobOutput.getStatus());

        jobOutput.setStatus(ExecutableState.DISCARDED.toString());
        newStatus = ExecutableState.RUNNING;
        flag = manager.setStageOutput(jobOutput, taskOrJobId, newStatus, updateInfo, failedMsg, isRestart);
        Assert.assertFalse(flag);
        Assert.assertEquals("DISCARDED", jobOutput.getStatus());

        jobOutput.setStatus(ExecutableState.READY.toString());
        newStatus = ExecutableState.SUCCEED;
        flag = manager.setStageOutput(jobOutput, taskOrJobId, newStatus, updateInfo, failedMsg, isRestart);
        Assert.assertTrue(flag);
        Assert.assertEquals("SUCCEED", jobOutput.getStatus());
        Assert.assertEquals(failedMsg, jobOutput.getFailedMsg());
        Assert.assertEquals("0", jobOutput.getInfo().get(NBatchConstants.P_INDEX_SUCCESS_COUNT));

        jobOutput.setStatus(ExecutableState.SKIP.toString());
        newStatus = ExecutableState.READY;
        isRestart = true;
        updateInfo.put(NBatchConstants.P_INDEX_SUCCESS_COUNT, "123");
        flag = manager.setStageOutput(jobOutput, taskOrJobId, newStatus, updateInfo, failedMsg, isRestart);
        Assert.assertTrue(flag);
        Assert.assertEquals("READY", jobOutput.getStatus());
        Assert.assertEquals(0, jobOutput.getStartTime());
        Assert.assertEquals(0, jobOutput.getEndTime());
        val successIndexCountString = jobOutput.getInfo().get(NBatchConstants.P_INDEX_SUCCESS_COUNT);
        Assert.assertEquals("123", successIndexCountString);
    }

    @Test
    public void testMakeStageSuccess() {
        String segmentId = RandomUtil.randomUUIDStr();

        ExecutableManager manager = ExecutableManager.getInstance(getTestConfig(), getProject());
        SucceedChainedTestExecutable executable = new SucceedChainedTestExecutable();
        executable.setId(RandomUtil.randomUUIDStr());
        executable.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        executable.setJobType(JobTypeEnum.INC_BUILD);

        NSparkExecutable sparkExecutable = new NSparkExecutable();
        sparkExecutable.setParam(NBatchConstants.P_SEGMENT_IDS, segmentId);
        sparkExecutable.setParam(NBatchConstants.P_INDEX_COUNT, "10");
        sparkExecutable.setId(RandomUtil.randomUUIDStr());
        executable.addTask(sparkExecutable);

        StageExecutable build1 = new StageExecutable();
        StageExecutable build2 = new StageExecutable();
        StageExecutable build3 = new StageExecutable();
        final StageExecutable stage1 = (StageExecutable) sparkExecutable.addStage(build1);
        final StageExecutable stage2 = (StageExecutable) sparkExecutable.addStage(build2);
        final StageExecutable stage3 = (StageExecutable) sparkExecutable.addStage(build3);
        val stageIds = Sets.newHashSet(stage1.getId(), stage2.getId(), stage3.getId());
        sparkExecutable.setStageMap();
        manager.addJob(executable);

        manager.makeStageSuccess(executable.getId());
        var job = manager.getJob(executable.getId());
        var stagesMap = ((ChainedStageExecutable) ((ChainedExecutable) job).getTasks().get(0)).getStagesMap();
        Assert.assertEquals(1, stagesMap.size());
        Assert.assertTrue(stagesMap.containsKey(segmentId));
        var stageBases = stagesMap.get(segmentId);
        Assert.assertEquals(3, stageBases.size());
        stageBases.forEach(stage -> Assert.assertEquals(ExecutableState.READY, stage.getStatus(segmentId)));

        manager.makeStageSuccess(sparkExecutable.getId());
        job = manager.getJob(executable.getId());
        stagesMap = ((ChainedStageExecutable) ((ChainedExecutable) job).getTasks().get(0)).getStagesMap();
        Assert.assertEquals(1, stagesMap.size());
        Assert.assertTrue(stagesMap.containsKey(segmentId));
        stageBases = stagesMap.get(segmentId);
        Assert.assertEquals(3, stageBases.size());
        stageBases.forEach(stage -> Assert.assertEquals(ExecutableState.SUCCEED, stage.getStatus(segmentId)));

        val actualIds = stageBases.stream().map(AbstractExecutable::getId).collect(Collectors.toSet());

        compareStageIds(stageIds, actualIds);

        manager.makeStageSuccess("do-nothing" + sparkExecutable.getId());
    }

    private void compareStageIds(Set<String> expectedIds, Set<String> actualIds) {
        Assert.assertEquals(expectedIds.size(), actualIds.size());
        Assert.assertTrue(expectedIds.containsAll(actualIds));
        Assert.assertTrue(actualIds.containsAll(expectedIds));
    }

    @Test
    public void testMakeStageError() {
        String segmentId = RandomUtil.randomUUIDStr();

        ExecutableManager manager = ExecutableManager.getInstance(getTestConfig(), getProject());
        SucceedChainedTestExecutable executable = new SucceedChainedTestExecutable();
        executable.setId(RandomUtil.randomUUIDStr());
        executable.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        executable.setJobType(JobTypeEnum.INC_BUILD);

        NSparkExecutable sparkExecutable = new NSparkExecutable();
        sparkExecutable.setParam(NBatchConstants.P_SEGMENT_IDS, segmentId);
        sparkExecutable.setParam(NBatchConstants.P_INDEX_COUNT, "10");
        sparkExecutable.setId(RandomUtil.randomUUIDStr());
        executable.addTask(sparkExecutable);

        StageExecutable build1 = new StageExecutable();
        StageExecutable build2 = new StageExecutable();
        StageExecutable build3 = new StageExecutable();
        final StageExecutable stage1 = (StageExecutable) sparkExecutable.addStage(build1);
        final StageExecutable stage2 = (StageExecutable) sparkExecutable.addStage(build2);
        final StageExecutable stage3 = (StageExecutable) sparkExecutable.addStage(build3);
        val stageIds = Sets.newHashSet(stage1.getId(), stage2.getId(), stage3.getId());
        sparkExecutable.setStageMap();

        manager.addJob(executable);

        manager.updateStageStatus(stage1.getId(), segmentId, ExecutableState.RUNNING, null, "test output");
        manager.updateStageStatus(stage2.getId(), segmentId, ExecutableState.RUNNING, null, "test output");
        manager.updateStageStatus(stage3.getId(), segmentId, ExecutableState.RUNNING, null, "test output");

        manager.makeStageError(executable.getId());
        var job = manager.getJob(executable.getId());
        var stagesMap = ((ChainedStageExecutable) ((ChainedExecutable) job).getTasks().get(0)).getStagesMap();
        Assert.assertEquals(1, stagesMap.size());
        Assert.assertTrue(stagesMap.containsKey(segmentId));
        var stageBases = stagesMap.get(segmentId);
        Assert.assertEquals(3, stageBases.size());
        stageBases.forEach(stage -> Assert.assertEquals(ExecutableState.RUNNING, stage.getStatus(segmentId)));

        manager.makeStageError(sparkExecutable.getId());
        job = manager.getJob(executable.getId());
        stagesMap = ((ChainedStageExecutable) ((ChainedExecutable) job).getTasks().get(0)).getStagesMap();
        Assert.assertEquals(1, stagesMap.size());
        Assert.assertTrue(stagesMap.containsKey(segmentId));
        stageBases = stagesMap.get(segmentId);
        Assert.assertEquals(3, stageBases.size());
        stageBases.forEach(stage -> Assert.assertEquals(ExecutableState.ERROR, stage.getStatus(segmentId)));

        val actualIds = stageBases.stream().map(AbstractExecutable::getId).collect(Collectors.toSet());

        compareStageIds(stageIds, actualIds);

        manager.makeStageError("do-nothing" + sparkExecutable.getId());
    }

    @Test
    public void testToJobStatus() {
        var result = ExecutableState.SKIP.toJobStatus();
        Assert.assertEquals(JobStatusEnum.SKIP, result);

        result = ExecutableState.READY.toJobStatus();
        Assert.assertEquals(JobStatusEnum.PENDING, result);

        result = ExecutableState.RUNNING.toJobStatus();
        Assert.assertEquals(JobStatusEnum.RUNNING, result);

        result = ExecutableState.ERROR.toJobStatus();
        Assert.assertEquals(JobStatusEnum.ERROR, result);

        result = ExecutableState.SUCCEED.toJobStatus();
        Assert.assertEquals(JobStatusEnum.FINISHED, result);

        result = ExecutableState.PAUSED.toJobStatus();
        Assert.assertEquals(JobStatusEnum.STOPPED, result);

        result = ExecutableState.SUICIDAL.toJobStatus();
        Assert.assertEquals(JobStatusEnum.DISCARDED, result);

        result = ExecutableState.DISCARDED.toJobStatus();
        Assert.assertEquals(JobStatusEnum.DISCARDED, result);

        result = ExecutableState.WARNING.toJobStatus();
        Assert.assertEquals(JobStatusEnum.WARNING, result);
    }

    @Test
    public void testToPO() {
        String segmentId = RandomUtil.randomUUIDStr();

        SucceedChainedTestExecutable executable = new SucceedChainedTestExecutable();
        executable.setId(RandomUtil.randomUUIDStr());
        executable.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        NSparkExecutable sparkExecutable = new NSparkExecutable();
        sparkExecutable.setParam(NBatchConstants.P_SEGMENT_IDS, segmentId);
        sparkExecutable.setParam(NBatchConstants.P_INDEX_COUNT, "10");
        sparkExecutable.setId(RandomUtil.randomUUIDStr());
        executable.addTask(sparkExecutable);

        StageExecutable build1 = new StageExecutable();
        StageExecutable build2 = new StageExecutable();
        StageExecutable build3 = new StageExecutable();
        final StageExecutable stage1 = (StageExecutable) sparkExecutable.addStage(build1);
        final StageExecutable stage2 = (StageExecutable) sparkExecutable.addStage(build2);
        final StageExecutable stage3 = (StageExecutable) sparkExecutable.addStage(build3);
        val stageIds = Sets.newHashSet(stage1.getId(), stage2.getId(), stage3.getId());
        sparkExecutable.setStageMap();

        val executablePO = ExecutableManager.toPO(executable, getProject());
        Assert.assertEquals(executablePO.getId(), executable.getId());
        Assert.assertEquals(1, executablePO.getTasks().size());
        executablePO.getTasks().forEach(po -> {
            Assert.assertEquals(1, po.getStagesMap().size());
            Assert.assertTrue(po.getStagesMap().containsKey(segmentId));
            Assert.assertTrue(Sets.newHashSet(segmentId).containsAll(po.getStagesMap().keySet()));
            po.getStagesMap().values().forEach(list -> {
                Assert.assertEquals(3, list.size());
                list.forEach(stage -> Assert.assertTrue(stageIds.contains(stage.getId())));
            });
        });
    }
}

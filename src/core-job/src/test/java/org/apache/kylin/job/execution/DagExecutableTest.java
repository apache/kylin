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

package org.apache.kylin.job.execution;

import static org.apache.kylin.job.execution.AbstractExecutable.DEPENDENT_FILES;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.awaitility.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import lombok.val;
import lombok.var;

@MetadataInfo
class DagExecutableTest {

    private NExecutableManager manager;
    private ExecutableContext context;

    private static final String DEFAULT_PROJECT = "default";

    @BeforeEach
    void setup() {
        manager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), DEFAULT_PROJECT);
        context = new ExecutableContext(Maps.newConcurrentMap(), Maps.newConcurrentMap(),
                KylinConfig.getInstanceFromEnv(), 0);
        for (String jobPath : manager.getJobs()) {
            manager.deleteJob(jobPath);
        }
    }

    @Test
    void testCheckPreviousStepFailed() {
        val job = new DefaultExecutable();
        job.setProject(DEFAULT_PROJECT);
        val executable1 = new SucceedDagTestExecutable();
        executable1.setProject(DEFAULT_PROJECT);
        job.addTask(executable1);
        val executable2 = new SucceedDagTestExecutable();
        executable2.setProject(DEFAULT_PROJECT);
        job.addTask(executable2);
        job.setJobType(JobTypeEnum.INDEX_BUILD);
        executable1.setNextSteps(Sets.newHashSet(executable2.getId()));
        executable2.setPreviousStep(executable1.getId());
        manager.addJob(job);

        final Map<String, Executable> dagExecutablesMap = job.getTasks().stream()
                .collect(Collectors.toMap(Executable::getId, executable -> executable));

        try {
            job.executeDagExecutable(dagExecutablesMap, executable2, context);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof RuntimeException);
            assertEquals("java.lang.IllegalStateException: invalid subtask state, sub task:"
                    + executable1.getDisplayName() + ", state:" + executable1.getStatus(), e.getMessage());
            assertEquals(ExecutableState.READY, executable1.getStatus());
        }
    }

    @Test
    void executeStep() {
        val job = new DefaultExecutable();
        job.setProject(DEFAULT_PROJECT);
        val executable1 = new SucceedDagTestExecutable();
        executable1.setProject(DEFAULT_PROJECT);
        job.addTask(executable1);
        val executable2 = new SucceedDagTestExecutable();
        executable2.setProject(DEFAULT_PROJECT);
        job.addTask(executable2);
        job.setJobType(JobTypeEnum.INDEX_BUILD);
        executable1.setNextSteps(Sets.newHashSet(executable2.getId()));
        executable2.setPreviousStep(executable1.getId());
        manager.addJob(job);
        manager.updateJobOutput(executable1.getId(), ExecutableState.RUNNING);
        manager.updateJobOutput(executable1.getId(), ExecutableState.SUCCEED);

        final Map<String, Executable> dagExecutablesMap = job.getTasks().stream()
                .collect(Collectors.toMap(Executable::getId, executable -> executable));

        job.executeDagExecutable(dagExecutablesMap, executable2, context);
        assertEquals(ExecutableState.SUCCEED, executable2.getStatus());
    }

    @Test
    void executeStepWithSuccess() {
        val job = new DefaultExecutable();
        job.setProject(DEFAULT_PROJECT);
        val executable1 = new SucceedDagTestExecutable();
        executable1.setProject(DEFAULT_PROJECT);
        job.addTask(executable1);
        val executable2 = new SucceedDagTestExecutable();
        executable2.setProject(DEFAULT_PROJECT);
        job.addTask(executable2);
        job.setJobType(JobTypeEnum.INDEX_BUILD);
        executable1.setNextSteps(Sets.newHashSet(executable2.getId()));
        executable2.setPreviousStep(executable1.getId());
        manager.addJob(job);
        manager.updateJobOutput(executable1.getId(), ExecutableState.RUNNING);
        manager.updateJobOutput(executable1.getId(), ExecutableState.SUCCEED);
        manager.updateJobOutput(executable2.getId(), ExecutableState.RUNNING);
        manager.updateJobOutput(executable2.getId(), ExecutableState.SUCCEED);

        final Map<String, Executable> dagExecutablesMap = job.getTasks().stream()
                .collect(Collectors.toMap(Executable::getId, executable -> executable));

        job.executeDagExecutable(dagExecutablesMap, executable2, context);
    }

    @Test
    void executeStepWithErrorState() {
        val job = new DefaultExecutable();
        job.setProject(DEFAULT_PROJECT);
        val executable1 = new SucceedDagTestExecutable();
        executable1.setProject(DEFAULT_PROJECT);
        job.addTask(executable1);
        val executable2 = new FailedDagTestExecutable();
        executable2.setProject(DEFAULT_PROJECT);
        job.addTask(executable2);
        job.setJobType(JobTypeEnum.INDEX_BUILD);
        executable1.setNextSteps(Sets.newHashSet(executable2.getId()));
        executable2.setPreviousStep(executable1.getId());
        manager.addJob(job);
        manager.updateJobOutput(executable1.getId(), ExecutableState.RUNNING);
        manager.updateJobOutput(executable1.getId(), ExecutableState.SUCCEED);
        manager.updateJobOutput(executable2.getId(), ExecutableState.RUNNING);
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            manager.updateJobOutput(executable2.getId(), ExecutableState.ERROR);
            return null;
        }, DEFAULT_PROJECT, 1, UnitOfWork.DEFAULT_EPOCH_ID);

        final Map<String, Executable> dagExecutablesMap = job.getTasks().stream()
                .collect(Collectors.toMap(Executable::getId, executable -> executable));

        try {
            job.executeDagExecutable(dagExecutablesMap, executable2, context);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof RuntimeException);
            assertEquals("java.lang.IllegalStateException: invalid subtask state, sub task:"
                    + executable2.getDisplayName() + ", state:" + executable2.getStatus(), e.getMessage());
            assertEquals(ExecutableState.ERROR, executable2.getStatus());
        }
    }

    @Test
    void executeNextSteps() {
        val job = new DefaultExecutable();
        job.setProject(DEFAULT_PROJECT);
        val executable1 = new SucceedDagTestExecutable();
        executable1.setProject(DEFAULT_PROJECT);
        job.addTask(executable1);
        val executable2 = new SucceedDagTestExecutable();
        executable2.setProject(DEFAULT_PROJECT);
        job.addTask(executable2);
        val executable3 = new SucceedDagTestExecutable();
        executable3.setProject(DEFAULT_PROJECT);
        job.addTask(executable3);
        job.setJobType(JobTypeEnum.INDEX_BUILD);
        executable1.setNextSteps(Sets.newHashSet(executable2.getId()));
        executable2.setPreviousStep(executable1.getId());
        executable2.setNextSteps(Sets.newHashSet(executable3.getId()));
        executable3.setPreviousStep(executable2.getId());
        manager.addJob(job);
        manager.updateJobOutput(executable1.getId(), ExecutableState.RUNNING);
        manager.updateJobOutput(executable1.getId(), ExecutableState.SUCCEED);
        manager.updateJobOutput(executable2.getId(), ExecutableState.RUNNING);
        manager.updateJobOutput(executable2.getId(), ExecutableState.SUCCEED);

        final Map<String, Executable> dagExecutablesMap = job.getTasks().stream()
                .collect(Collectors.toMap(Executable::getId, executable -> executable));

        job.executeDagExecutable(dagExecutablesMap, executable2, context);

        assertEquals(ExecutableState.SUCCEED, executable3.getStatus());
    }

    @Test
    void dagExecuteSingleExecutableList() throws ExecuteException {
        val job = new DefaultExecutable();
        job.setProject(DEFAULT_PROJECT);
        val executable1 = new SucceedDagTestExecutable();
        executable1.setProject(DEFAULT_PROJECT);
        job.addTask(executable1);
        val executable2 = new SucceedDagTestExecutable();
        executable2.setProject(DEFAULT_PROJECT);
        job.addTask(executable2);
        val executable3 = new SucceedDagTestExecutable();
        executable3.setProject(DEFAULT_PROJECT);
        job.addTask(executable3);

        job.setJobType(JobTypeEnum.INDEX_BUILD);
        executable1.setNextSteps(Sets.newHashSet(executable2.getId()));
        executable2.setPreviousStep(executable1.getId());
        executable2.setNextSteps(Sets.newHashSet(executable3.getId()));
        executable3.setPreviousStep(executable2.getId());
        manager.addJob(job);

        final Map<String, Executable> dagExecutablesMap = job.getTasks().stream()
                .collect(Collectors.toMap(Executable::getId, executable -> executable));

        job.dagExecute(Lists.newArrayList(executable1), dagExecutablesMap, context);

        assertEquals(ExecutableState.SUCCEED, executable1.getStatus());
        assertEquals(ExecutableState.SUCCEED, executable2.getStatus());
        assertEquals(ExecutableState.SUCCEED, executable3.getStatus());
    }

    @Test
    void dagExecute() throws ExecuteException {
        val job = new DefaultExecutable();
        job.setProject(DEFAULT_PROJECT);
        val executable1 = new SucceedDagTestExecutable();
        executable1.setProject(DEFAULT_PROJECT);
        job.addTask(executable1);

        val executable2 = new SucceedDagTestExecutable();
        executable2.setProject(DEFAULT_PROJECT);
        job.addTask(executable2);
        val executable22 = new SucceedDagTestExecutable();
        executable22.setProject(DEFAULT_PROJECT);
        job.addTask(executable22);
        val executable222 = new SucceedDagTestExecutable();
        executable222.setProject(DEFAULT_PROJECT);
        job.addTask(executable222);

        val executable3 = new SucceedDagTestExecutable();
        executable3.setProject(DEFAULT_PROJECT);
        job.addTask(executable3);
        val executable33 = new SucceedDagTestExecutable();
        executable33.setProject(DEFAULT_PROJECT);
        job.addTask(executable33);
        val executable333 = new SucceedDagTestExecutable();
        executable333.setProject(DEFAULT_PROJECT);
        job.addTask(executable333);

        val executable01 = new SucceedDagTestExecutable();
        executable01.setProject(DEFAULT_PROJECT);
        job.addTask(executable01);

        val executable02 = new SucceedDagTestExecutable();
        executable02.setProject(DEFAULT_PROJECT);
        job.addTask(executable02);
        val executable022 = new SucceedDagTestExecutable();
        executable022.setProject(DEFAULT_PROJECT);
        job.addTask(executable022);
        val executable0222 = new SucceedDagTestExecutable();
        executable0222.setProject(DEFAULT_PROJECT);
        job.addTask(executable0222);

        val executable03 = new SucceedDagTestExecutable();
        executable03.setProject(DEFAULT_PROJECT);
        job.addTask(executable03);
        val executable033 = new SucceedDagTestExecutable();
        executable033.setProject(DEFAULT_PROJECT);
        job.addTask(executable033);
        val executable0333 = new SucceedDagTestExecutable();
        executable0333.setProject(DEFAULT_PROJECT);
        job.addTask(executable0333);

        job.setJobType(JobTypeEnum.INDEX_BUILD);
        executable1.setNextSteps(Sets.newHashSet(executable2.getId(), executable3.getId()));

        executable2.setPreviousStep(executable1.getId());
        executable2.setNextSteps(Sets.newHashSet(executable22.getId(), executable222.getId()));
        executable22.setPreviousStep(executable2.getId());
        executable222.setPreviousStep(executable2.getId());

        executable3.setPreviousStep(executable1.getId());
        executable3.setNextSteps(Sets.newHashSet(executable33.getId(), executable333.getId()));
        executable33.setPreviousStep(executable3.getId());
        executable333.setPreviousStep(executable3.getId());

        executable01.setNextSteps(Sets.newHashSet(executable02.getId(), executable03.getId()));

        executable02.setPreviousStep(executable01.getId());
        executable02.setNextSteps(Sets.newHashSet(executable022.getId(), executable0222.getId()));
        executable022.setPreviousStep(executable02.getId());
        executable0222.setPreviousStep(executable02.getId());

        executable03.setPreviousStep(executable01.getId());
        executable03.setNextSteps(Sets.newHashSet(executable033.getId(), executable0333.getId()));
        executable033.setPreviousStep(executable03.getId());
        executable0333.setPreviousStep(executable03.getId());
        manager.addJob(job);

        final Map<String, Executable> dagExecutablesMap = job.getTasks().stream()
                .collect(Collectors.toMap(Executable::getId, executable -> executable));

        job.dagExecute(Lists.newArrayList(executable1, executable01), dagExecutablesMap, context);

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(ExecutableState.SUCCEED, executable1.getStatus());
            assertEquals(ExecutableState.SUCCEED, executable2.getStatus());
            assertEquals(ExecutableState.SUCCEED, executable22.getStatus());
            assertEquals(ExecutableState.SUCCEED, executable222.getStatus());
            assertEquals(ExecutableState.SUCCEED, executable3.getStatus());
            assertEquals(ExecutableState.SUCCEED, executable33.getStatus());
            assertEquals(ExecutableState.SUCCEED, executable333.getStatus());

            assertEquals(ExecutableState.SUCCEED, executable01.getStatus());
            assertEquals(ExecutableState.SUCCEED, executable02.getStatus());
            assertEquals(ExecutableState.SUCCEED, executable022.getStatus());
            assertEquals(ExecutableState.SUCCEED, executable0222.getStatus());
            assertEquals(ExecutableState.SUCCEED, executable03.getStatus());
            assertEquals(ExecutableState.SUCCEED, executable033.getStatus());
            assertEquals(ExecutableState.SUCCEED, executable0333.getStatus());
        });
    }

    @Test
    void dagSchedule() throws ExecuteException {
        val job = new DefaultExecutable();
        job.setProject(DEFAULT_PROJECT);
        val executable1 = new SucceedDagTestExecutable();
        executable1.setProject(DEFAULT_PROJECT);
        job.addTask(executable1);
        val executable2 = new SucceedDagTestExecutable();
        executable2.setProject(DEFAULT_PROJECT);
        job.addTask(executable2);
        val executable3 = new SucceedDagTestExecutable();
        executable3.setProject(DEFAULT_PROJECT);
        job.addTask(executable3);

        job.setJobType(JobTypeEnum.INDEX_BUILD);
        executable1.setNextSteps(Sets.newHashSet(executable2.getId()));
        executable2.setPreviousStep(executable1.getId());
        executable2.setNextSteps(Sets.newHashSet(executable3.getId()));
        executable3.setPreviousStep(executable2.getId());
        manager.addJob(job);

        List<Executable> executables = job.getTasks().stream().map(task -> ((Executable) task))
                .collect(Collectors.toList());
        job.dagSchedule(executables, context);

        assertEquals(ExecutableState.SUCCEED, executable1.getStatus());
        assertEquals(ExecutableState.SUCCEED, executable2.getStatus());
        assertEquals(ExecutableState.SUCCEED, executable3.getStatus());
    }

    @Test
    void chainedSchedule() throws ExecuteException {
        val job = new DefaultExecutable();
        job.setProject(DEFAULT_PROJECT);
        val executable1 = new SucceedDagTestExecutable();
        executable1.setProject(DEFAULT_PROJECT);
        job.addTask(executable1);
        val executable2 = new SucceedDagTestExecutable();
        executable2.setProject(DEFAULT_PROJECT);
        job.addTask(executable2);
        val executable3 = new SucceedDagTestExecutable();
        executable3.setProject(DEFAULT_PROJECT);
        job.addTask(executable3);
        job.setJobType(JobTypeEnum.INDEX_BUILD);
        manager.addJob(job);

        List<Executable> executables = job.getTasks().stream().map(task -> ((Executable) task))
                .collect(Collectors.toList());
        job.chainedSchedule(executables, context);

        assertEquals(ExecutableState.SUCCEED, executable1.getStatus());
        assertEquals(ExecutableState.SUCCEED, executable2.getStatus());
        assertEquals(ExecutableState.SUCCEED, executable3.getStatus());
    }

    @Test
    void chainedScheduleFailed() {
        val job = new DefaultExecutable();
        job.setProject(DEFAULT_PROJECT);
        val executable1 = new SucceedDagTestExecutable();
        executable1.setProject(DEFAULT_PROJECT);
        job.addTask(executable1);
        val executable2 = new SucceedDagTestExecutable();
        executable2.setProject(DEFAULT_PROJECT);
        job.addTask(executable2);
        val executable3 = new SucceedDagTestExecutable();
        executable3.setProject(DEFAULT_PROJECT);
        job.addTask(executable3);
        job.setJobType(JobTypeEnum.INDEX_BUILD);
        manager.addJob(job);

        manager.updateJobOutput(executable2.getId(), ExecutableState.RUNNING);
        manager.updateJobOutput(executable2.getId(), ExecutableState.SUCCEED);
        manager.updateJobOutput(executable3.getId(), ExecutableState.RUNNING);
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            manager.updateJobOutput(executable3.getId(), ExecutableState.ERROR);
            return null;
        }, DEFAULT_PROJECT, 1, UnitOfWork.DEFAULT_EPOCH_ID);

        List<Executable> executables = job.getTasks().stream().map(task -> ((Executable) task))
                .collect(Collectors.toList());
        try {
            job.chainedSchedule(executables, context);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof IllegalStateException);
            assertEquals("invalid subtask state, sub task:" + executable3.getDisplayName() + ", state:"
                    + executable3.getStatus(), e.getMessage());
            assertEquals(ExecutableState.SUCCEED, executable1.getStatus());
            assertEquals(ExecutableState.SUCCEED, executable2.getStatus());
        }

    }

    @Test
    void doWork() throws ExecuteException {
        val job = new DefaultExecutable();
        job.setProject(DEFAULT_PROJECT);
        val executable1 = new SucceedDagTestExecutable();
        executable1.setProject(DEFAULT_PROJECT);
        job.addTask(executable1);
        val executable2 = new SucceedDagTestExecutable();
        executable2.setProject(DEFAULT_PROJECT);
        job.addTask(executable2);
        val executable3 = new SucceedDagTestExecutable();
        executable3.setProject(DEFAULT_PROJECT);
        job.addTask(executable3);

        job.setJobType(JobTypeEnum.INDEX_BUILD);
        executable1.setNextSteps(Sets.newHashSet(executable2.getId()));
        executable2.setPreviousStep(executable1.getId());
        executable2.setNextSteps(Sets.newHashSet(executable3.getId()));
        executable3.setPreviousStep(executable2.getId());
        manager.addJob(job);

        val executeResult = job.doWork(context);
        assertEquals(ExecuteResult.State.SUCCEED, executeResult.state());
        assertEquals("succeed", executeResult.output());
        assertNull(executeResult.getThrowable());

        assertEquals(ExecutableState.SUCCEED, executable1.getStatus());
        assertEquals(ExecutableState.SUCCEED, executable2.getStatus());
        assertEquals(ExecutableState.SUCCEED, executable3.getStatus());
    }

    @Test
    void doWorkChained() throws ExecuteException {
        val job = new DefaultExecutable();
        job.setProject(DEFAULT_PROJECT);
        val executable1 = new SucceedDagTestExecutable();
        executable1.setProject(DEFAULT_PROJECT);
        job.addTask(executable1);
        val executable2 = new SucceedDagTestExecutable();
        executable2.setProject(DEFAULT_PROJECT);
        job.addTask(executable2);
        val executable3 = new SucceedDagTestExecutable();
        executable3.setProject(DEFAULT_PROJECT);
        job.addTask(executable3);

        job.setJobType(JobTypeEnum.INDEX_BUILD);
        executable1.setNextSteps(Sets.newHashSet(executable2.getId()));
        executable2.setPreviousStep(executable1.getId());
        executable2.setNextSteps(Sets.newHashSet(executable3.getId()));
        executable3.setPreviousStep(executable2.getId());

        job.setJobSchedulerMode(JobSchedulerModeEnum.CHAIN);
        manager.addJob(job);

        val executeResult = job.doWork(context);
        assertEquals(ExecuteResult.State.SUCCEED, executeResult.state());
        assertEquals("succeed", executeResult.output());
        assertNull(executeResult.getThrowable());

        assertEquals(ExecutableState.SUCCEED, executable1.getStatus());
        assertEquals(ExecutableState.SUCCEED, executable2.getStatus());
        assertEquals(ExecutableState.SUCCEED, executable3.getStatus());
    }

    @Test
    void doWorkDag() throws ExecuteException {
        val job = new DefaultExecutable();
        job.setProject(DEFAULT_PROJECT);
        val executable1 = new SucceedDagTestExecutable();
        executable1.setProject(DEFAULT_PROJECT);
        job.addTask(executable1);
        val executable2 = new SucceedDagTestExecutable();
        executable2.setProject(DEFAULT_PROJECT);
        job.addTask(executable2);
        val executable3 = new SucceedDagTestExecutable();
        executable3.setProject(DEFAULT_PROJECT);
        job.addTask(executable3);

        job.setJobType(JobTypeEnum.INDEX_BUILD);
        executable1.setNextSteps(Sets.newHashSet(executable2.getId()));
        executable2.setPreviousStep(executable1.getId());
        executable2.setNextSteps(Sets.newHashSet(executable3.getId()));
        executable3.setPreviousStep(executable2.getId());

        job.setJobSchedulerMode(JobSchedulerModeEnum.DAG);
        manager.addJob(job);

        val executeResult = job.doWork(context);
        assertEquals(ExecuteResult.State.SUCCEED, executeResult.state());
        assertEquals("succeed", executeResult.output());
        assertNull(executeResult.getThrowable());

        assertEquals(ExecutableState.SUCCEED, executable1.getStatus());
        assertEquals(ExecutableState.SUCCEED, executable2.getStatus());
        assertEquals(ExecutableState.SUCCEED, executable3.getStatus());
    }

    @Test
    void dagExecuteDuration() throws ExecuteException {
        val job = new DefaultExecutable();
        job.setProject(DEFAULT_PROJECT);
        val executable1 = new SucceedDagTestExecutable();
        executable1.setProject(DEFAULT_PROJECT);
        job.addTask(executable1);

        val executable2 = new SucceedDagTestExecutable();
        executable2.setProject(DEFAULT_PROJECT);
        job.addTask(executable2);
        val executable22 = new FiveSecondSucceedDagTestExecutable();
        executable22.setProject(DEFAULT_PROJECT);
        job.addTask(executable22);
        val executable222 = new FiveSecondSucceedDagTestExecutable(1);
        executable222.setProject(DEFAULT_PROJECT);
        job.addTask(executable222);

        job.setJobType(JobTypeEnum.INDEX_BUILD);
        executable1.setNextSteps(Sets.newHashSet(executable2.getId()));

        executable2.setPreviousStep(executable1.getId());
        executable2.setNextSteps(Sets.newHashSet(executable22.getId(), executable222.getId()));
        executable22.setPreviousStep(executable2.getId());
        executable222.setPreviousStep(executable2.getId());

        job.setJobSchedulerMode(JobSchedulerModeEnum.DAG);

        manager.addJob(job);
        job.doWork(context);

        await().atMost(new Duration(120, TimeUnit.SECONDS)).untilAsserted(() -> {
            assertEquals(ExecutableState.SUCCEED, executable1.getStatus());
            assertEquals(ExecutableState.SUCCEED, executable2.getStatus());
            assertEquals(ExecutableState.SUCCEED, executable22.getStatus());
            assertEquals(ExecutableState.SUCCEED, executable222.getStatus());
        });

        val durationFromStepOrStageDurationSum = job.getDurationFromStepOrStageDurationSum();
        val expected = executable1.getDuration() + executable2.getDuration() + executable22.getDuration();
        assertEquals(expected, durationFromStepOrStageDurationSum);
    }

    @Test
    void chainedExecuteDuration() throws ExecuteException {
        val job = new DefaultExecutable();
        job.setProject(DEFAULT_PROJECT);
        val executable1 = new SucceedDagTestExecutable();
        executable1.setProject(DEFAULT_PROJECT);
        job.addTask(executable1);

        val executable2 = new SucceedDagTestExecutable();
        executable2.setProject(DEFAULT_PROJECT);
        job.addTask(executable2);
        val executable22 = new FiveSecondSucceedDagTestExecutable();
        executable22.setProject(DEFAULT_PROJECT);
        job.addTask(executable22);
        val executable222 = new FiveSecondSucceedDagTestExecutable(1);
        executable222.setProject(DEFAULT_PROJECT);
        job.addTask(executable222);

        job.setJobType(JobTypeEnum.INDEX_BUILD);
        executable1.setNextSteps(Sets.newHashSet(executable2.getId()));

        executable2.setPreviousStep(executable1.getId());
        executable2.setNextSteps(Sets.newHashSet(executable22.getId(), executable222.getId()));
        executable22.setPreviousStep(executable2.getId());
        executable222.setPreviousStep(executable2.getId());

        manager.addJob(job);
        job.doWork(context);

        assertEquals(ExecutableState.SUCCEED, executable1.getStatus());
        assertEquals(ExecutableState.SUCCEED, executable2.getStatus());
        assertEquals(ExecutableState.SUCCEED, executable22.getStatus());
        assertEquals(ExecutableState.SUCCEED, executable222.getStatus());

        val durationFromStepOrStageDurationSum = job.getDurationFromStepOrStageDurationSum();
        val expected = executable1.getDuration() + executable2.getDuration() + executable22.getDuration()
                + executable222.getDuration();
        assertEquals(expected, durationFromStepOrStageDurationSum);
    }

    @Test
    void getTaskDurationSingleSegment() {
        val job = new DefaultExecutable();
        job.setProject(DEFAULT_PROJECT);
        val task = new TestWithStageExecutable();
        task.setProject(DEFAULT_PROJECT);
        val stage1 = new SuccessTestStage();
        stage1.setProject(DEFAULT_PROJECT);
        val stage2 = new SuccessTestStage();
        stage2.setProject(DEFAULT_PROJECT);
        val stage3 = new SuccessTestStage();
        stage3.setProject(DEFAULT_PROJECT);
        job.addTask(task);
        task.addStage(stage1);
        task.addStage(stage2);
        task.addStage(stage3);
        task.setStageMap();
        job.setJobType(JobTypeEnum.INDEX_BUILD);
        manager.addJob(job);

        manager.updateJobOutput(job.getId(), ExecutableState.RUNNING);
        manager.updateJobOutput(task.getId(), ExecutableState.RUNNING);
        manager.updateStageStatus(stage1.getId(), task.getId(), ExecutableState.RUNNING, null, null);
        manager.updateStageStatus(stage2.getId(), task.getId(), ExecutableState.RUNNING, null, null);
        manager.updateStageStatus(stage3.getId(), task.getId(), ExecutableState.RUNNING, null, null);
        manager.saveUpdatedJob();
        await().pollDelay(Duration.ONE_SECOND).until(() -> true);
        manager.updateJobOutput(job.getId(), ExecutableState.SUCCEED);
        manager.updateJobOutput(task.getId(), ExecutableState.SUCCEED);
        manager.updateStageStatus(stage1.getId(), task.getId(), ExecutableState.SUCCEED, null, null);
        await().pollDelay(Duration.ONE_SECOND).until(() -> true);
        manager.updateStageStatus(stage2.getId(), task.getId(), ExecutableState.SUCCEED, null, null);
        await().pollDelay(Duration.ONE_SECOND).until(() -> true);
        manager.updateStageStatus(stage3.getId(), task.getId(), ExecutableState.SUCCEED, null, null);
        manager.saveUpdatedJob();

        val taskDuration = task.getTaskDurationToTest(task);
        val expected = AbstractExecutable.getDuration(stage1.getOutput(task.getId()))
                + AbstractExecutable.getDuration(stage2.getOutput(task.getId()))
                + AbstractExecutable.getDuration(stage3.getOutput(task.getId()));
        assertEquals(expected, taskDuration);
    }

    @Test
    void getTaskDuration() {
        val job = new DefaultExecutable();
        job.setProject(DEFAULT_PROJECT);
        val task = new TestWithStageExecutable();
        task.setProject(DEFAULT_PROJECT);
        val stage1 = new SuccessTestStage();
        stage1.setProject(DEFAULT_PROJECT);
        val stage2 = new SuccessTestStage();
        stage2.setProject(DEFAULT_PROJECT);
        val stage3 = new SuccessTestStage();
        stage3.setProject(DEFAULT_PROJECT);

        job.addTask(task);
        task.setSegmentIds(Sets.newHashSet(RandomUtil.randomUUIDStr(), RandomUtil.randomUUIDStr()));
        task.addStage(stage1);
        task.addStage(stage2);
        task.addStage(stage3);
        task.setStageMap();
        job.setJobType(JobTypeEnum.INDEX_BUILD);
        manager.addJob(job);

        manager.updateJobOutput(job.getId(), ExecutableState.RUNNING);
        manager.updateJobOutput(task.getId(), ExecutableState.RUNNING);
        manager.updateStageStatus(stage1.getId(), task.getId(), ExecutableState.RUNNING, null, null);
        manager.updateStageStatus(stage2.getId(), task.getId(), ExecutableState.RUNNING, null, null);
        manager.updateStageStatus(stage3.getId(), task.getId(), ExecutableState.RUNNING, null, null);
        manager.saveUpdatedJob();
        await().pollDelay(Duration.ONE_SECOND).until(() -> true);
        manager.updateJobOutput(job.getId(), ExecutableState.SUCCEED);
        manager.updateJobOutput(task.getId(), ExecutableState.SUCCEED);
        manager.updateStageStatus(stage1.getId(), task.getId(), ExecutableState.SUCCEED, null, null);
        await().pollDelay(Duration.ONE_SECOND).until(() -> true);
        manager.updateStageStatus(stage2.getId(), task.getId(), ExecutableState.SUCCEED, null, null);
        await().pollDelay(Duration.ONE_SECOND).until(() -> true);
        manager.updateStageStatus(stage3.getId(), task.getId(), ExecutableState.SUCCEED, null, null);
        manager.saveUpdatedJob();

        val taskDuration = task.getTaskDurationToTest(task);
        val expected = task.getDuration();
        assertEquals(expected, taskDuration);
    }

    @Test
    void updateJobError() {
        val job = new DefaultExecutable();
        job.setProject(DEFAULT_PROJECT);
        val task1 = new FailedDagTestExecutable();
        task1.setProject(DEFAULT_PROJECT);
        val task2 = new SucceedDagTestExecutable();
        task2.setProject(DEFAULT_PROJECT);
        job.addTask(task1);
        job.addTask(task2);
        job.setJobType(JobTypeEnum.INDEX_BUILD);
        manager.addJob(job);

        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            manager.updateJobOutput(task1.getId(), ExecutableState.ERROR);
            return null;
        }, DEFAULT_PROJECT, 1, UnitOfWork.DEFAULT_EPOCH_ID);

        val stack = ExceptionUtils.getStackTrace(new MockJobException());
        val reason = new MockJobException().getMessage();
        manager.updateJobError(job.getId(), task1.getId() + "_00", null, stack, reason);

        var output = job.getOutput();
        assertEquals(task1.getId() + "_00", output.getFailedStepId());
        assertNull(output.getFailedSegmentId());
        assertEquals(stack, output.getFailedStack());
        assertEquals(reason, output.getFailedReason());

        manager.updateJobError(job.getId(), task2.getId() + "_00", null, stack, reason);

        output = job.getOutput();
        assertEquals(task1.getId() + "_00", output.getFailedStepId());
        assertNull(output.getFailedSegmentId());
        assertEquals(stack, output.getFailedStack());
        assertEquals(reason, output.getFailedReason());
    }

    @Test
    void getDependentFiles() {
        val job = new DefaultExecutable();
        job.setProject(DEFAULT_PROJECT);
        manager.addJob(job);
        var dependentFiles = job.getDependentFiles();
        assertTrue(CollectionUtils.isEmpty(dependentFiles));

        val info = Maps.<String, String> newHashMap();
        info.put(DEPENDENT_FILES, "12");
        manager.updateJobOutput(job.getId(), ExecutableState.RUNNING, info);
        dependentFiles = job.getDependentFiles();
        assertEquals(1, dependentFiles.size());
        assertTrue(dependentFiles.contains("12"));
    }

    @Test
    void updateStepStatus() {
        val job = new DefaultExecutable();
        job.setProject(DEFAULT_PROJECT);
        val executable1 = new SucceedDagTestExecutable();
        executable1.setProject(DEFAULT_PROJECT);
        job.addTask(executable1);
        manager.addJob(job);
        executable1.killApplicationIfExistsOrUpdateStepStatus();
        assertEquals(ExecutableState.PAUSED, executable1.getStatus());
    }

    @Test
    void getOtherPipelineRunningStep() {
        val job = new DefaultExecutable();
        job.setProject(DEFAULT_PROJECT);
        val executable1 = new SucceedDagTestExecutable();
        executable1.setProject(DEFAULT_PROJECT);
        job.addTask(executable1);

        val executable2 = new SucceedDagTestExecutable();
        executable2.setProject(DEFAULT_PROJECT);
        job.addTask(executable2);
        val executable22 = new SucceedDagTestExecutable();
        executable22.setProject(DEFAULT_PROJECT);
        job.addTask(executable22);
        val executable222 = new SucceedDagTestExecutable();
        executable222.setProject(DEFAULT_PROJECT);
        job.addTask(executable222);

        val executable3 = new SucceedDagTestExecutable();
        executable3.setProject(DEFAULT_PROJECT);
        job.addTask(executable3);
        val executable33 = new SucceedDagTestExecutable();
        executable33.setProject(DEFAULT_PROJECT);
        job.addTask(executable33);
        val executable333 = new SucceedDagTestExecutable();
        executable333.setProject(DEFAULT_PROJECT);
        job.addTask(executable333);

        job.setJobType(JobTypeEnum.INDEX_BUILD);
        executable1.setNextSteps(Sets.newHashSet(executable2.getId(), executable3.getId()));

        executable2.setPreviousStep(executable1.getId());
        executable2.setNextSteps(Sets.newHashSet(executable22.getId(), executable222.getId()));
        executable22.setPreviousStep(executable2.getId());
        executable222.setPreviousStep(executable2.getId());

        executable3.setPreviousStep(executable1.getId());
        executable3.setNextSteps(Sets.newHashSet(executable33.getId(), executable333.getId()));
        executable33.setPreviousStep(executable3.getId());
        executable333.setPreviousStep(executable3.getId());

        job.setJobSchedulerMode(JobSchedulerModeEnum.DAG);
        manager.addJob(job);

        manager.updateJobOutput(executable2.getId(), ExecutableState.RUNNING);
        manager.updateJobOutput(executable3.getId(), ExecutableState.RUNNING);

        var otherPipelineRunningSteps = executable3.getOtherPipelineRunningStep();
        assertEquals(1, otherPipelineRunningSteps.size());
        val otherPipelineRunningStep = otherPipelineRunningSteps.get(0);
        assertEquals(executable2.getId(), otherPipelineRunningStep.getId());
        assertEquals(executable2.getStatus(), otherPipelineRunningStep.getStatus());

        manager.updateJobOutput(executable2.getId(), ExecutableState.SUCCEED);
        manager.updateJobOutput(executable22.getId(), ExecutableState.RUNNING);
        manager.updateJobOutput(executable222.getId(), ExecutableState.RUNNING);
        manager.updateJobOutput(executable3.getId(), ExecutableState.RUNNING);
        otherPipelineRunningSteps = executable3.getOtherPipelineRunningStep();
        assertEquals(2, otherPipelineRunningSteps.size());
        val otherPipelineRunningStepIds = otherPipelineRunningSteps.stream().map(AbstractExecutable::getId)
                .collect(Collectors.toList());
        assertTrue(otherPipelineRunningStepIds.contains(executable22.getId()));
        assertTrue(otherPipelineRunningStepIds.contains(executable222.getId()));
    }

    @Test
    void getOtherPipelineRunningStepChain() {
        val job = new DefaultExecutable();
        job.setProject(DEFAULT_PROJECT);
        val executable1 = new SucceedDagTestExecutable();
        executable1.setProject(DEFAULT_PROJECT);
        job.addTask(executable1);

        val executable2 = new SucceedDagTestExecutable();
        executable2.setProject(DEFAULT_PROJECT);
        job.addTask(executable2);
        val executable22 = new SucceedDagTestExecutable();
        executable22.setProject(DEFAULT_PROJECT);
        job.addTask(executable22);
        val executable222 = new SucceedDagTestExecutable();
        executable222.setProject(DEFAULT_PROJECT);
        job.addTask(executable222);

        val executable3 = new SucceedDagTestExecutable();
        executable3.setProject(DEFAULT_PROJECT);
        job.addTask(executable3);
        val executable33 = new SucceedDagTestExecutable();
        executable33.setProject(DEFAULT_PROJECT);
        job.addTask(executable33);
        val executable333 = new SucceedDagTestExecutable();
        executable333.setProject(DEFAULT_PROJECT);
        job.addTask(executable333);

        job.setJobType(JobTypeEnum.INDEX_BUILD);
        executable1.setNextSteps(Sets.newHashSet(executable2.getId(), executable3.getId()));

        executable2.setPreviousStep(executable1.getId());
        executable2.setNextSteps(Sets.newHashSet(executable22.getId(), executable222.getId()));
        executable22.setPreviousStep(executable2.getId());
        executable222.setPreviousStep(executable2.getId());

        executable3.setPreviousStep(executable1.getId());
        executable3.setNextSteps(Sets.newHashSet(executable33.getId(), executable333.getId()));
        executable33.setPreviousStep(executable3.getId());
        executable333.setPreviousStep(executable3.getId());

        manager.addJob(job);

        manager.updateJobOutput(executable2.getId(), ExecutableState.RUNNING);
        manager.updateJobOutput(executable3.getId(), ExecutableState.RUNNING);

        val otherPipelineRunningSteps = executable3.getOtherPipelineRunningStep();
        assertTrue(CollectionUtils.isEmpty(otherPipelineRunningSteps));
    }
}

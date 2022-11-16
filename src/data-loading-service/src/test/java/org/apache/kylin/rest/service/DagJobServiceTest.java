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

import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_RESTART_CHECK_SEGMENT_STATUS;
import static org.awaitility.Awaitility.await;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.engine.spark.job.NSparkExecutable;
import org.apache.kylin.job.constant.JobActionEnum;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.DefaultExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobSchedulerModeEnum;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.MockJobException;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.execution.SucceedDagTestExecutable;
import org.apache.kylin.job.execution.SuccessTestStage;
import org.apache.kylin.job.execution.TestWithStageExecutable;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.metadata.model.SegmentStatusEnumToDisplay;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.rest.response.NDataSegmentResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import io.kyligence.kap.guava20.shaded.common.collect.Lists;
import io.kyligence.kap.guava20.shaded.common.collect.Sets;
import lombok.val;

@MetadataInfo
class DagJobServiceTest {
    @InjectMocks
    private final JobService jobService = Mockito.spy(new JobService());

    @InjectMocks
    private final ModelService modelService = Mockito.spy(new ModelService());
    @Mock
    private final AclUtil aclUtil = Mockito.spy(AclUtil.class);
    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    private NExecutableManager manager;

    private static final String DEFAULT_PROJECT = "default";

    @BeforeEach
    void setup() {
        manager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), DEFAULT_PROJECT);
        for (String jobPath : manager.getJobs()) {
            manager.deleteJob(jobPath);
        }
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(jobService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(modelService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(jobService, "modelService", modelService);
    }

    @Test
    void checkSegmentState() {
        val job = Mockito.mock(DefaultExecutable.class);
        Mockito.doReturn(JobTypeEnum.SNAPSHOT_BUILD).when(job).getJobType();

        jobService.checkSegmentState(DEFAULT_PROJECT, JobActionEnum.PAUSE.name(), job);
        jobService.checkSegmentState(DEFAULT_PROJECT, JobActionEnum.RESTART.name(), job);

        Mockito.doReturn(JobTypeEnum.INC_BUILD).when(job).getJobType();
        Mockito.doReturn(null).when(job).getSegmentIds();
        jobService.checkSegmentState(DEFAULT_PROJECT, JobActionEnum.RESTART.name(), job);

        val segmentId = RandomUtil.randomUUIDStr();
        Mockito.doReturn(Sets.newHashSet(segmentId)).when(job).getSegmentIds();
        val modelId = RandomUtil.randomUUIDStr();
        Mockito.doReturn(modelId).when(job).getTargetModelId();

        val nDataSegmentResponse = Mockito.mock(NDataSegmentResponse.class);
        Mockito.doReturn(Lists.newArrayList(nDataSegmentResponse)).when(modelService).getSegmentsResponse(modelId,
                DEFAULT_PROJECT, "0", "" + (Long.MAX_VALUE - 1), "", null, null, false, "sortBy", false, null, null);
        jobService.checkSegmentState(DEFAULT_PROJECT, JobActionEnum.RESTART.name(), job);

        Mockito.doReturn(segmentId).when(nDataSegmentResponse).getId();
        jobService.checkSegmentState(DEFAULT_PROJECT, JobActionEnum.RESTART.name(), job);

        try {
            Mockito.doReturn(SegmentStatusEnumToDisplay.ONLINE_HDFS).when(nDataSegmentResponse).getStatusToDisplay();
            jobService.checkSegmentState(DEFAULT_PROJECT, JobActionEnum.RESTART.name(), job);
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof KylinException);
            val expected = new KylinException(JOB_RESTART_CHECK_SEGMENT_STATUS);
            Assertions.assertEquals(expected.getErrorCodeString(), ((KylinException) e).getErrorCodeString());
            Assertions.assertEquals(expected.getLocalizedMessage(), e.getLocalizedMessage());
        }
    }

    @Test
    void getJobDetail() {
        val job = new DefaultExecutable();
        job.setProject(DEFAULT_PROJECT);
        val task1 = new TestWithStageExecutable();
        task1.setProject(DEFAULT_PROJECT);
        val task2 = new SucceedDagTestExecutable();
        task2.setProject(DEFAULT_PROJECT);
        val task3 = new TestWithStageExecutable();
        task3.setProject(DEFAULT_PROJECT);
        job.addTask(task1);
        job.addTask(task2);
        job.addTask(task3);

        val stage11 = new SuccessTestStage();
        stage11.setProject(DEFAULT_PROJECT);
        val stage12 = new SuccessTestStage();
        stage12.setProject(DEFAULT_PROJECT);
        val stage13 = new SuccessTestStage();
        stage13.setProject(DEFAULT_PROJECT);
        task1.addStage(stage11);
        task1.addStage(stage12);
        task1.addStage(stage13);
        task1.setStageMap();

        val stage31 = new SuccessTestStage();
        stage31.setProject(DEFAULT_PROJECT);
        val stage32 = new SuccessTestStage();
        stage32.setProject(DEFAULT_PROJECT);
        val stage33 = new SuccessTestStage();
        stage33.setProject(DEFAULT_PROJECT);
        task3.addStage(stage31);
        task3.addStage(stage32);
        task3.addStage(stage33);
        task3.setStageMap();

        job.setJobType(JobTypeEnum.INDEX_BUILD);
        manager.addJob(job);

        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            manager.updateJobOutput(task1.getId(), ExecutableState.ERROR);
            manager.updateStageStatus(stage11.getId(), task1.getId(), ExecutableState.ERROR, null, null);
            return null;
        }, DEFAULT_PROJECT, 1, UnitOfWork.DEFAULT_EPOCH_ID);

        val stack = ExceptionUtils.getStackTrace(new MockJobException());
        val reason = new MockJobException().getMessage();
        manager.updateJobError(job.getId(), stage11.getId(), null, stack, reason);

        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            manager.updateJobOutput(task3.getId(), ExecutableState.ERROR);
            manager.updateStageStatus(stage31.getId(), task3.getId(), ExecutableState.ERROR, null, null);
            return null;
        }, DEFAULT_PROJECT, 1, UnitOfWork.DEFAULT_EPOCH_ID);

        val jobDetail = jobService.getJobDetail(DEFAULT_PROJECT, job.getId());
        Assertions.assertEquals(task1.getId(), jobDetail.get(0).getId());
        Assertions.assertEquals(JobStatusEnum.ERROR, jobDetail.get(0).getStatus());
        Assertions.assertEquals(task1.getStagesMap().get(task1.getId()).size(), jobDetail.get(0).getSubStages().size());
        Assertions.assertEquals(stage11.getId(), jobDetail.get(0).getSubStages().get(0).getId());
        Assertions.assertEquals(stage11.getStatus(task1.getId()).toJobStatus(),
                jobDetail.get(0).getSubStages().get(0).getStatus());
        Assertions.assertEquals(JobStatusEnum.ERROR, jobDetail.get(0).getSubStages().get(0).getStatus());

        Assertions.assertEquals(task3.getId(), jobDetail.get(2).getId());
        Assertions.assertEquals(JobStatusEnum.ERROR, jobDetail.get(2).getStatus());
        Assertions.assertEquals(task3.getStagesMap().get(task3.getId()).size(), jobDetail.get(2).getSubStages().size());
        Assertions.assertEquals(stage31.getId(), jobDetail.get(2).getSubStages().get(0).getId());
        Assertions.assertEquals(stage31.getStatus(task3.getId()).toJobStatus(),
                jobDetail.get(2).getSubStages().get(0).getStatus());
        Assertions.assertEquals(JobStatusEnum.ERROR, jobDetail.get(2).getSubStages().get(0).getStatus());
    }

    @Test
    void getJobDetailDag() {
        val job = new DefaultExecutable();
        job.setProject(DEFAULT_PROJECT);
        val task1 = new TestWithStageExecutable();
        task1.setProject(DEFAULT_PROJECT);
        val task2 = new SucceedDagTestExecutable();
        task2.setProject(DEFAULT_PROJECT);
        val task3 = new TestWithStageExecutable();
        task3.setProject(DEFAULT_PROJECT);
        job.addTask(task1);
        job.addTask(task2);
        job.addTask(task3);

        val stage11 = new SuccessTestStage();
        stage11.setProject(DEFAULT_PROJECT);
        val stage12 = new SuccessTestStage();
        stage12.setProject(DEFAULT_PROJECT);
        val stage13 = new SuccessTestStage();
        stage13.setProject(DEFAULT_PROJECT);
        task1.addStage(stage11);
        task1.addStage(stage12);
        task1.addStage(stage13);
        task1.setStageMap();

        val stage31 = new SuccessTestStage();
        stage31.setProject(DEFAULT_PROJECT);
        val stage32 = new SuccessTestStage();
        stage32.setProject(DEFAULT_PROJECT);
        val stage33 = new SuccessTestStage();
        stage33.setProject(DEFAULT_PROJECT);
        task3.addStage(stage31);
        task3.addStage(stage32);
        task3.addStage(stage33);
        task3.setStageMap();

        job.setJobType(JobTypeEnum.INDEX_BUILD);
        job.setJobSchedulerMode(JobSchedulerModeEnum.DAG);
        manager.addJob(job);

        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            manager.updateJobOutput(task1.getId(), ExecutableState.ERROR);
            manager.updateStageStatus(stage11.getId(), task1.getId(), ExecutableState.ERROR, null, null);
            return null;
        }, DEFAULT_PROJECT, 1, UnitOfWork.DEFAULT_EPOCH_ID);

        val stack = ExceptionUtils.getStackTrace(new MockJobException());
        val reason = new MockJobException().getMessage();
        manager.updateJobError(job.getId(), stage11.getId(), null, stack, reason);

        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            manager.updateJobOutput(task3.getId(), ExecutableState.ERROR);
            manager.updateStageStatus(stage31.getId(), task3.getId(), ExecutableState.ERROR, null, null);
            return null;
        }, DEFAULT_PROJECT, 1, UnitOfWork.DEFAULT_EPOCH_ID);

        val jobDetail = jobService.getJobDetail(DEFAULT_PROJECT, job.getId());
        Assertions.assertEquals(task1.getId(), jobDetail.get(0).getId());
        Assertions.assertEquals(JobStatusEnum.ERROR, jobDetail.get(0).getStatus());
        Assertions.assertEquals(task1.getStagesMap().get(task1.getId()).size(), jobDetail.get(0).getSubStages().size());
        Assertions.assertEquals(stage11.getId(), jobDetail.get(0).getSubStages().get(0).getId());
        Assertions.assertEquals(stage11.getStatus(task1.getId()).toJobStatus(),
                jobDetail.get(0).getSubStages().get(0).getStatus());
        Assertions.assertEquals(JobStatusEnum.ERROR, jobDetail.get(0).getSubStages().get(0).getStatus());

        Assertions.assertEquals(task3.getId(), jobDetail.get(2).getId());
        Assertions.assertEquals(JobStatusEnum.STOPPED, jobDetail.get(2).getStatus());
        Assertions.assertEquals(task3.getStagesMap().get(task3.getId()).size(), jobDetail.get(2).getSubStages().size());
        Assertions.assertEquals(stage31.getId(), jobDetail.get(2).getSubStages().get(0).getId());
        Assertions.assertNotEquals(stage31.getStatus(task3.getId()).toJobStatus(),
                jobDetail.get(2).getSubStages().get(0).getStatus());
        Assertions.assertEquals(JobStatusEnum.STOPPED, jobDetail.get(2).getSubStages().get(0).getStatus());
    }

    @Test
    void updateStepStatus() throws Exception {
        val config = KylinConfig.getInstanceFromEnv();
        val sparkMaster = config.getSparkMaster();
        val scheduler = NDefaultScheduler.getInstance(DEFAULT_PROJECT);
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()));
        val context = scheduler.getContext();
        config.setProperty("kylin.engine.spark-conf.spark.master", "local");
        val executable = new NSparkExecutable();
        executable.setProject(DEFAULT_PROJECT);
        new Thread(() -> {
            try {
                Assertions.assertNull(context.getRunningJobThread(executable));
                executable.killApplicationIfExistsOrUpdateStepStatus();
                Assertions.assertNull(context.getRunningJobThread(executable));

                context.addRunningJob(executable);
                Assertions.assertNotNull(context.getRunningJobThread(executable));
                executable.killApplicationIfExistsOrUpdateStepStatus();
                Assertions.assertNull(context.getRunningJobThread(executable));
            } finally {
                config.setProperty("kylin.engine.spark-conf.spark.master", sparkMaster);
                context.removeRunningJob(executable);
            }
        }).start();
        await().untilAsserted(() -> Assertions.assertEquals(sparkMaster, config.getSparkMaster()));
        scheduler.shutdown();
    }
}

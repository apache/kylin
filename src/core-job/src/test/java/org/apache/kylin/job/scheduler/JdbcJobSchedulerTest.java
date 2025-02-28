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
package org.apache.kylin.job.scheduler;

import static org.apache.kylin.common.util.TestUtils.getTestConfig;
import static org.awaitility.Awaitility.await;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kylin.common.AbstractTestCase;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.job.JobContext;
import org.apache.kylin.job.dao.ExecutableOutputPO;
import org.apache.kylin.job.dao.JobInfoDao;
import org.apache.kylin.job.domain.JobLock;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.SucceedChainedTestExecutable;
import org.apache.kylin.job.mapper.JobLockMapper;
import org.apache.kylin.job.rest.JobMapperFilter;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

@MetadataInfo
class JdbcJobSchedulerTest extends AbstractTestCase {
    private static final String PROJECT = "default";

    private JobInfoDao jobInfoDao;
    private JobContext jobContext;

    @BeforeEach
    public void setup() {
        KylinConfig config = getTestConfig();
        overwriteSystemProp("kylin.job.max-concurrent-jobs", "2");
        overwriteSystemProp("kylin.job.slave-lock-renew-sec", "3");
        jobContext = JobContextUtil.getJobContext(config);
        jobInfoDao = JobContextUtil.getJobInfoDao(config);
    }

    @AfterEach
    public void clean() {
        JobContextUtil.cleanUp();
    }

    @Test
    void happyPath() {
        String jobId = mockJob();
        Assertions.assertEquals(jobInfoDao.getExecutablePOByUuid(jobId).getOutput().getStatus(),
                ExecutableState.READY.name());
        await().atMost(3, TimeUnit.SECONDS).until(() -> jobInfoDao.getExecutablePOByUuid(jobId).getOutput().getStatus()
                .equals(ExecutableState.RUNNING.name()));
        await().atMost(2, TimeUnit.SECONDS).until(() -> jobInfoDao.getExecutablePOByUuid(jobId).getOutput().getStatus()
                .equals(ExecutableState.SUCCEED.name()));
        //release lock
        await().atMost(5, TimeUnit.SECONDS).until(() -> jobContext.getJobLockMapper().selectByJobId(jobId) == null);
    }

    @Test
    void oneJobCanNotRunOnTwoNodesTest() throws Exception {
        JobContext secondJobContext = mockJobContext("127.0.0.1:7071");
        String jobId = mockJob();
        await().atMost(5, TimeUnit.SECONDS).until(() -> jobInfoDao.getExecutablePOByUuid(jobId).getOutput().getStatus()
                .equals(ExecutableState.RUNNING.name()));
        Assertions.assertEquals(secondJobContext.getJobScheduler().getRunningJob().size()
                + jobContext.getJobScheduler().getRunningJob().size(), 1);

        secondJobContext.destroy();
    }

    @Test
    void JobsScheduledOnTwoNode() throws Exception {
        overwriteSystemProp("kylin.job.max-concurrent-jobs", "3");
        JobContext secondJobContext = mockJobContext("127.0.0.1:7071");
        System.setProperty("COST_TIME", "3000");
        for (int i = 0; i < 3; i++) {
            mockJob();
        }
        JobMapperFilter filter = new JobMapperFilter();
        filter.setStatuses(ExecutableState.RUNNING);
        await().atMost(5, TimeUnit.SECONDS).until(() -> jobInfoDao.getJobInfoListByFilter(filter).size() == 3);
        Assertions.assertEquals(secondJobContext.getJobScheduler().getRunningJob().size()
                + jobContext.getJobScheduler().getRunningJob().size(), 3);
        Assertions.assertTrue(jobContext.getJobScheduler().getRunningJob().size() > 0
                || secondJobContext.getJobScheduler().getRunningJob().size() > 0);

        secondJobContext.destroy();
        System.clearProperty("COST_TIME");
    }

    @Test
    void testLockExpiredAndJobNotFinal() {
        String jobId = mockJob();
        JobLock lock = new JobLock(jobId, PROJECT, 1, JobLock.JobTypeEnum.OFFLINE);
        lock.setLockNode("mock_node");
        lock.setLockExpireTime(new Date());
        int expect = jobContext.getJobLockMapper().insert(lock);
        Assertions.assertEquals(1, expect);
        await().atMost(5, TimeUnit.SECONDS).until(() -> jobInfoDao.getExecutablePOByUuid(jobId).getOutput().getStatus()
                .equals(ExecutableState.SUCCEED.name()));
    }

    @Test
    void testConcurrentJobWithPriority() {
        KylinConfig config = getTestConfig();
        config.setProperty("kylin.job.slave-pull-batch-size", "1");
        jobContext.getJobScheduler().destroy();

        String p0_0 = mockJobWithPriority(0);
        String p1_0 = mockJobWithPriority(1);
        String p2_0 = mockJobWithPriority(2);
        String p0_1 = mockJobWithPriority(0);
        String p1_1 = mockJobWithPriority(1);
        jobContext.getJobScheduler().start();
        await().atMost(1, TimeUnit.MINUTES).until(() -> jobInfoDao.getExecutablePOByUuid(p0_0).getOutput().getStatus()
                .equals(ExecutableState.SUCCEED.name())
                && jobInfoDao.getExecutablePOByUuid(p1_0).getOutput().getStatus().equals(ExecutableState.SUCCEED.name())
                && jobInfoDao.getExecutablePOByUuid(p2_0).getOutput().getStatus().equals(ExecutableState.SUCCEED.name())
                && jobInfoDao.getExecutablePOByUuid(p0_1).getOutput().getStatus().equals(ExecutableState.SUCCEED.name())
                && jobInfoDao.getExecutablePOByUuid(p1_1).getOutput().getStatus()
                        .equals(ExecutableState.SUCCEED.name()));
        Assertions.assertTrue(jobInfoDao.getExecutablePOByUuid(p0_0).getOutput().getStartTime() < jobInfoDao
                .getExecutablePOByUuid(p1_0).getOutput().getStartTime());
        Assertions.assertTrue(jobInfoDao.getExecutablePOByUuid(p0_0).getOutput().getStartTime() < jobInfoDao
                .getExecutablePOByUuid(p1_1).getOutput().getStartTime());
        Assertions.assertTrue(jobInfoDao.getExecutablePOByUuid(p0_1).getOutput().getStartTime() < jobInfoDao
                .getExecutablePOByUuid(p1_0).getOutput().getStartTime());
        Assertions.assertTrue(jobInfoDao.getExecutablePOByUuid(p0_1).getOutput().getStartTime() < jobInfoDao
                .getExecutablePOByUuid(p1_1).getOutput().getStartTime());
        Assertions.assertTrue(jobInfoDao.getExecutablePOByUuid(p1_0).getOutput().getStartTime() < jobInfoDao
                .getExecutablePOByUuid(p2_0).getOutput().getStartTime());
        Assertions.assertTrue(jobInfoDao.getExecutablePOByUuid(p1_1).getOutput().getStartTime() < jobInfoDao
                .getExecutablePOByUuid(p2_0).getOutput().getStartTime());
    }

    @Test
    void testFindNonLockIdListInOrder() {
        jobContext.getJobScheduler().destroy();
        Map<String, Integer> jobMap = Maps.newHashMap();
        for (int i = 0; i < 20; i++) {
            int p = i % 5;
            JobLock lock = new JobLock();
            String id = "mock_lock_id_" + i;
            lock.setLockId(id);
            lock.setProject(PROJECT);
            lock.setLockNode("mock_node");
            lock.setPriority(p);
            lock.setLockExpireTime(new Date());
            jobContext.getJobLockMapper().insert(lock);
            jobMap.put(id, p);
        }
        JdbcJobScheduler originScheduler = jobContext.getJobScheduler();
        originScheduler.destroy();
        JdbcJobScheduler jobScheduler = Mockito.spy(originScheduler);
        Mockito.when(jobScheduler.hasRunningJob()).thenReturn(true);
        jobContext.setJobScheduler(jobScheduler);
        List<String> order1 = jobContext.getJobScheduler().findNonLockIdListInOrder(20,
                Collections.singletonList(PROJECT));
        List<String> order2 = jobContext.getJobScheduler().findNonLockIdListInOrder(20,
                Collections.singletonList(PROJECT));
        boolean hasDiff = false;
        int currentPriority = 0;
        for (int i = 0; i < order1.size(); i++) {
            String jobId1 = order1.get(i);
            String jobId2 = order2.get(i);
            int priority1 = jobMap.get(jobId1);
            int priority2 = jobMap.get(jobId2);
            Assertions.assertEquals(priority1, priority2);
            Assertions.assertTrue(priority1 >= currentPriority);
            currentPriority = priority1;
            hasDiff |= !jobId1.equals(jobId2);
        }
        Assertions.assertTrue(hasDiff);
    }

    @Test
    void testFindNonLockIdListWithProject() {
        jobContext.getJobScheduler().destroy();
        JobLock lock = new JobLock();
        String id = "mock_lock";
        lock.setLockId(id);
        lock.setProject(PROJECT);
        lock.setPriority(3);
        jobContext.getJobLockMapper().insert(lock);

        List<String> jobIdList;
        
        jobIdList = jobContext.getJobScheduler().findNonLockIdListInOrder(5, Collections.emptyList());
        Assertions.assertTrue(jobIdList.isEmpty());
        
        List<String> allProjects = NProjectManager.getInstance(getTestConfig()).listAllProjects().stream()
                .map(ProjectInstance::getName).collect(Collectors.toList());
        String otherProject = allProjects.stream().filter(project -> !project.equals(PROJECT)).findFirst().get();
        jobIdList = jobContext.getJobScheduler().findNonLockIdListInOrder(5, Collections.singletonList(otherProject));
        Assertions.assertTrue(jobIdList.isEmpty());

        jobIdList = jobContext.getJobScheduler().findNonLockIdListInOrder(5, Collections.singletonList(PROJECT));
        Assertions.assertEquals(1, jobIdList.size());

        jobIdList = jobContext.getJobScheduler().findNonLockIdListInOrder(5, allProjects);
        Assertions.assertEquals(1, jobIdList.size());
    }

    @Test
    void testJobProducedAndDeleted() {
        // mock job, not persist in metadata
        AbstractExecutable job = mockExecutable();
        // insert job lock, without lock node
        String jobId = job.getJobId();
        JobLock lock = new JobLock(jobId, PROJECT, 1, JobLock.JobTypeEnum.OFFLINE);
        int expect = jobContext.getJobLockMapper().insert(lock);
        Assertions.assertEquals(1, expect);
        await().atMost(60, TimeUnit.SECONDS).until(() -> jobContext.getJobLockMapper().selectByJobId(jobId) == null);
    }

    @Test
    void testResumeRunningJobs() {
        KylinConfig config = getTestConfig();
        // Stop schedule
        JobContextUtil.stopScheduler();
        // Init job mappers without schedule
        JobInfoDao dao = JobContextUtil.getJobInfoDao(config);
        JobLockMapper mapper = (JobLockMapper) ReflectionTestUtils.getField(dao, "jobLockMapper");

        String jobId = mockJob();
        jobInfoDao.updateJob(jobId, job -> {
            ExecutableOutputPO jobOutput = job.getOutput();
            jobOutput.setStatus(ExecutableState.RUNNING.name());
            jobOutput.addStartTime(System.currentTimeMillis());
            job.getTasks().forEach(task -> task.getOutput().setStatus(ExecutableState.PENDING.name()));
            return true;
        });
        mapper.insertSelective(new JobLock(jobId, PROJECT, 3, JobLock.JobTypeEnum.OFFLINE));
        // init schedule
        JobContextUtil.getJobContext(config);

        await().atMost(2, TimeUnit.SECONDS).until(() -> jobInfoDao.getExecutablePOByUuid(jobId).getOutput().getStatus()
                .equals(ExecutableState.READY.name()));
        await().atMost(2, TimeUnit.SECONDS).until(() -> jobInfoDao.getExecutablePOByUuid(jobId).getOutput().getStatus()
                .equals(ExecutableState.PENDING.name()));
        await().atMost(5, TimeUnit.SECONDS).until(() -> jobInfoDao.getExecutablePOByUuid(jobId).getOutput().getStatus()
                .equals(ExecutableState.RUNNING.name()));
        await().atMost(2, TimeUnit.SECONDS).until(() -> jobInfoDao.getExecutablePOByUuid(jobId).getOutput().getStatus()
                .equals(ExecutableState.SUCCEED.name()));
        //release lock
        await().atMost(5, TimeUnit.SECONDS).until(() -> jobContext.getJobLockMapper().selectByJobId(jobId) == null);
    }

    private String mockJob() {
        ExecutableManager manager = ExecutableManager.getInstance(getTestConfig(), PROJECT);
        AbstractExecutable job = mockExecutable();
        manager.addJob(job);
        return job.getJobId();
    }

    private AbstractExecutable mockExecutable() {
        SucceedChainedTestExecutable job = new SucceedChainedTestExecutable();
        job.setProject(PROJECT);
        job.setName("mocked job");
        job.setTargetSubject("12345678");
        job.setJobType(JobTypeEnum.INC_BUILD);
        return job;
    }

    private String mockJobWithPriority(int priority) {
        ExecutableManager manager = ExecutableManager.getInstance(getTestConfig(), PROJECT);
        SucceedChainedTestExecutable job = new SucceedChainedTestExecutable();
        job.setProject(PROJECT);
        job.setName("mocked job");
        job.setTargetSubject("12345678");
        job.setJobType(JobTypeEnum.INC_BUILD);
        job.setPriority(priority);
        manager.addJob(job);
        return job.getJobId();
    }

    private JobContext mockJobContext(String serverNode) {
        JobContext secondJobContext = new JobContext();
        secondJobContext.setKylinConfig(getTestConfig());
        secondJobContext.setJobInfoMapper(jobContext.getJobInfoMapper());
        secondJobContext.setJobLockMapper(jobContext.getJobLockMapper());
        secondJobContext.setTransactionManager(jobContext.getTransactionManager());
        secondJobContext.init();
        ReflectionTestUtils.setField(secondJobContext, "serverNode", serverNode);
        return secondJobContext;
    }
}

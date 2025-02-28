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

import static org.apache.kylin.common.persistence.ResourceStore.GLOBAL_PROJECT;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.constant.LogConstant;
import org.apache.kylin.common.logging.SetLogCategory;
import org.apache.kylin.common.util.ExecutorServiceUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.ThreadUtils;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.job.JobContext;
import org.apache.kylin.job.core.AbstractJobExecutable;
import org.apache.kylin.job.core.lock.JdbcJobLock;
import org.apache.kylin.job.core.lock.LockAcquireListener;
import org.apache.kylin.job.core.lock.LockException;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.domain.JobInfo;
import org.apache.kylin.job.domain.JobLock;
import org.apache.kylin.job.domain.PriorityFistRandomOrderJob;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.rest.JobMapperFilter;
import org.apache.kylin.job.runners.JobCheckUtil;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.job.util.JobInfoUtil;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.utils.StreamingUtils;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.resourcegroup.ResourceGroupManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Getter;
import lombok.val;

public class JdbcJobScheduler implements JobScheduler {

    private static final Logger logger = LoggerFactory.getLogger(LogConstant.SCHEDULE_CATEGORY);

    private final JobContext jobContext;

    private final AtomicBoolean isMaster;

    // job id -> (executable, job scheduled time)
    private final Map<String, Pair<AbstractJobExecutable, Long>> runningJobMap;

    private final Map<String, PriorityQueue<JobInfo>> readyJobCache;

    private JdbcJobLock masterLock;

    private ScheduledExecutorService master;

    private ScheduledExecutorService slave;

    private ThreadPoolExecutor executorPool;

    private final int consumerMaxThreads;

    public JdbcJobScheduler(JobContext jobContext) {
        this.jobContext = jobContext;
        this.isMaster = new AtomicBoolean(false);
        this.runningJobMap = Maps.newConcurrentMap();
        this.readyJobCache = Maps.newHashMap();
        this.consumerMaxThreads = jobContext.getKylinConfig().getNodeMaxConcurrentJobLimit();
    }

    @Override
    public void publishJob() {
        // master lock
        masterLock = new JdbcJobLock(JobScheduler.MASTER_SCHEDULER, jobContext.getServerNode(),
                jobContext.getKylinConfig().getJobSchedulerMasterRenewalSec(),
                jobContext.getKylinConfig().getJobSchedulerMasterRenewalRatio(), jobContext.getLockClient(),
                new MasterAcquireListener());
        // standby: acquire master lock
        master.schedule(this::standby, 0, TimeUnit.SECONDS);

        // master: publish job
        master.schedule(this::produceJob, 0, TimeUnit.SECONDS);
    }

    @Override
    public void subscribeJob() {
        // slave: subscribe job
        slave.schedule(this::consumeJob, 0, TimeUnit.SECONDS);
    }

    // public for UT
    public boolean hasRunningJob() {
        return !runningJobMap.isEmpty();
    }

    public Map<String, Pair<AbstractJobExecutable, Long>> getRunningJob() {
        return Collections.unmodifiableMap(runningJobMap);
    }

    @Override
    public String getJobNode(String jobId) {
        return JobContextUtil.getJobSchedulerHost(jobId);
    }

    @Override
    public boolean isMaster() {
        return isMaster.get();
    }

    @Override
    public String getJobMaster() {
        return jobContext.getJobLockMapper().selectByJobId(MASTER_SCHEDULER).getLockNode();
    }

    public void start() {
        // standby: acquire JSM
        // publish job:  READY -> PENDING
        master = ThreadUtils.newDaemonSingleThreadScheduledExecutor("JdbcJobScheduler-Master");

        // subscribe job: PENDING -> RUNNING
        slave = ThreadUtils.newDaemonSingleThreadScheduledExecutor("JdbcJobScheduler-Slave");

        int maxThreads = this.consumerMaxThreads <= 0 ? 1 : this.consumerMaxThreads;
        // execute job: RUNNING -> FINISHED
        executorPool = ThreadUtils.newDaemonScalableThreadPool("JdbcJobScheduler-Executor", 1, maxThreads, 5,
                TimeUnit.MINUTES);

        publishJob();
        subscribeJob();
    }

    // for UT
    public void destroy() {

        if (Objects.nonNull(masterLock)) {
            try {
                masterLock.tryRelease();
            } catch (LockException e) {
                logger.warn("Something's wrong when removing master lock");
            }
        }

        if (Objects.nonNull(master)) {
            master.shutdownNow();
        }

        if (Objects.nonNull(slave)) {
            slave.shutdownNow();
        }

        if (Objects.nonNull(executorPool)) {
            ExecutorServiceUtil.shutdownGracefully(executorPool, 60);
        }
    }

    private void standby() {
        // init master lock
        try {
            if (jobContext.getJobLockMapper().selectByJobId(JobScheduler.MASTER_SCHEDULER) == null) {
                jobContext.getJobLockMapper().insertSelective(
                        new JobLock(JobScheduler.MASTER_SCHEDULER, "_global", 0, JobLock.JobTypeEnum.MASTER));
            }
        } catch (Exception e) {
            logger.error("Try insert 'master_scheduler' failed.", e);
        }

        try {
            masterLock.tryAcquire();
        } catch (LockException e) {
            logger.error("Something's wrong when acquiring master lock.", e);
        }
    }

    private void produceJob() {
        long delaySec = jobContext.getKylinConfig().getJobSchedulerMasterPollIntervalSec();
        try {
            // only master can publish job
            if (!isMaster.get()) {
                return;
            }

            releaseExpiredLock();

            List<JobInfo> processingJobInfoList = getProcessingJobInfoWithOrder();
            Map<String, Integer> projectRunningCountMap = Maps.newHashMap();
            Map<String, Set<String>> projectRunningRecModelMap = Maps.newHashMap();
            Map<String, Integer> projectRunningIndexPlannerBuildJobMap = Maps.newHashMap();
            collectProcessingJobInfo(processingJobInfoList, projectRunningCountMap, projectRunningRecModelMap,
                    projectRunningIndexPlannerBuildJobMap);
            Map<String, Integer> projectProduceCountMap = getProjectProduceCount(projectRunningCountMap);
            Map<String, Integer> projectProduceIndexPlannerBuildJobCountMap = getProjectProduceCount(
                    projectRunningIndexPlannerBuildJobMap, JobTypeEnum.Category.REC);

            boolean produced = false;
            for (Map.Entry<String, Integer> entry : projectProduceCountMap.entrySet()) {
                String project = entry.getKey();
                int produceCount = entry.getValue();
                if (produceCount == 0) {
                    logger.info("Project {} has reached max concurrent limit", project);
                    continue;
                }
                PriorityQueue<JobInfo> projectReadyJobCache = readyJobCache.get(project);

                if (CollectionUtils.isEmpty(projectReadyJobCache)) {
                    continue;
                }
                produced = true;
                if (produceCount > projectReadyJobCache.size()) {
                    produceCount = projectReadyJobCache.size();
                }
                // force catchup metadata before produce jobs
                StreamingUtils.replayAuditlog();
                logger.info("Begin to produce job for project: {}, product count: {}", project, produceCount);

                for (int i = 0; i < produceCount; i++) {
                    produceJobForProject(produceCount, projectReadyJobCache, projectRunningRecModelMap.get(project),
                            projectProduceIndexPlannerBuildJobCountMap);
                }
            }
            if (produced) {
                // maybe more jobs exist, publish job immediately
                delaySec = 0;
            }
        } catch (Exception e) {
            logger.error("Something's wrong when publishing job", e);
        } finally {
            master.schedule(this::produceJob, delaySec, TimeUnit.SECONDS);
        }
    }

    private void collectProcessingJobInfo(List<JobInfo> processingJobInfoList,
            Map<String, Integer> projectRunningCountMap, Map<String, Set<String>> projectRunningRecModelMap,
            Map<String, Integer> projectRunningIndexPlannerBuildJobMap) {
        for (JobInfo processingJobInfo : processingJobInfoList) {
            if (!projectRunningCountMap.containsKey(processingJobInfo.getProject())) {
                projectRunningRecModelMap.put(processingJobInfo.getProject(), new HashSet<>());
            }
            if (ExecutableState.READY.name().equals(processingJobInfo.getJobStatus())) {
                addReadyJobToCache(processingJobInfo);
            } else {
                // count running job by project
                String project = processingJobInfo.getProject();
                if (!projectRunningCountMap.containsKey(project)) {
                    projectRunningCountMap.put(project, 0);
                    projectRunningIndexPlannerBuildJobMap.put(project, 0);
                }
                projectRunningCountMap.put(project, projectRunningCountMap.get(project) + 1);
                if (!projectRunningRecModelMap.get(project).contains(processingJobInfo.getModelId())
                        && isIndexPlannerRecJob(processingJobInfo)) {
                    projectRunningRecModelMap.get(project).add(processingJobInfo.getModelId());
                }
                if (isIndexPlannerBuildJob(processingJobInfo)
                        && projectRunningIndexPlannerBuildJobMap.containsKey(project)) {
                    projectRunningIndexPlannerBuildJobMap.put(project,
                            projectRunningIndexPlannerBuildJobMap.get(project) + 1);
                }
            }
        }
    }

    private void produceJobForProject(int produceCount, PriorityQueue<JobInfo> projectReadyJobCache,
            Set<String> runningRecModels, Map<String, Integer> projectRunningIndexPlannerBuildJob) {
        int i = 0;
        while (i < produceCount) {
            if (projectReadyJobCache.isEmpty()) {
                break;
            }
            JobInfo jobInfo = projectReadyJobCache.poll();
            if (doProduce(jobInfo, runningRecModels, projectRunningIndexPlannerBuildJob)) {
                i++;
            }
        }
    }

    private boolean doProduce(JobInfo jobInfo, Set<String> runningRecModels,
            Map<String, Integer> projectRunningIndexPlannerBuildJob) {
        try {

            if (markSuicideJobWithTransaction(jobInfo)) {
                logger.info("Suicide job = {} on produce", jobInfo.getJobId());
                return false;
            }

            if (isIndexPlannerBuildJob(jobInfo)) {
                if (projectRunningIndexPlannerBuildJob.get(jobInfo.getProject()).intValue() > 0) {
                    projectRunningIndexPlannerBuildJob.put(jobInfo.getProject(),
                            projectRunningIndexPlannerBuildJob.get(jobInfo.getProject()).intValue() - 1);
                } else {
                    logger.info("Project {} has reached max concurrent limit for index planner fill index job",
                            jobInfo.getProject());
                    return false;
                }
            }
            if (isIndexPlannerRecJob(jobInfo)) {
                if (runningRecModels.contains(jobInfo.getModelId())) {
                    return false;
                } else {
                    runningRecModels.add(jobInfo.getModelId());
                }
            }

            return JobContextUtil.withTxAndRetry(() -> {
                String jobId = jobInfo.getJobId();
                JobLock lock = jobContext.getJobLockMapper().selectByJobId(jobId);
                if (lock == null && jobContext.getJobLockMapper().insertSelective(new JobLock(jobId,
                        jobInfo.getProject(), jobInfo.getPriority(), JobLock.JobTypeEnum.OFFLINE)) == 0) {
                    logger.error("Create job lock for [{}] failed!", jobId);
                    return false;
                }
                ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), jobInfo.getProject()).publishJob(jobId,
                        (AbstractExecutable) getJobExecutable(jobInfo));
                logger.debug("Job {} has bean produced successfully", jobInfo.getJobId());
                return true;
            });
        } catch (Exception e) {
            logger.error("Failed to produce job: {}", jobInfo.getJobId(), e);
            return false;
        }
    }

    private static boolean markSuicideJobWithTransaction(JobInfo jobInfo) {
        return EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> JobCheckUtil.markSuicideJob(jobInfo),
                jobInfo.getProject());
    }

    private List<JobInfo> getProcessingJobInfoWithOrder() {
        JobMapperFilter jobMapperFilter = new JobMapperFilter();
        jobMapperFilter.setStatuses(ExecutableState.READY, ExecutableState.PENDING, ExecutableState.RUNNING);
        jobMapperFilter.setOrderByFiled("priority,create_time");
        jobMapperFilter.setOrderType("ASC");
        return jobContext.getJobInfoMapper().selectByJobFilter(jobMapperFilter);
    }

    private void addReadyJobToCache(JobInfo jobInfo) {
        String project = jobInfo.getProject();
        readyJobCache.computeIfAbsent(project, k -> new PriorityQueue<>());
        if (!readyJobCache.get(project).contains(jobInfo)) {
            readyJobCache.get(project).add(jobInfo);
        }
    }

    private Map<String, Integer> getProjectProduceCount(Map<String, Integer> projectRunningCountMap, String jobType) {
        Map<String, Integer> projectProduceCount = Maps.newHashMap();
        NProjectManager projectManager = NProjectManager.getInstance(jobContext.getKylinConfig());
        List<ProjectInstance> allProjects = projectManager.listAllProjects();
        for (ProjectInstance projectInstance : allProjects) {
            String project = projectInstance.getName();
            projectInstance.getConfig().getMaxConcurrentJobLimit();
            int projectMaxConcurrent;
            if (JobTypeEnum.Category.REC.equals(jobType)) {
                projectMaxConcurrent = projectInstance.getConfig().getMaxConcurrentFillIndexJobLimit();
            } else {
                projectMaxConcurrent = projectInstance.getConfig().getMaxConcurrentJobLimit();
            }
            int projectRunningCount = projectRunningCountMap.getOrDefault(project, 0);
            if (projectRunningCount < projectMaxConcurrent) {
                projectProduceCount.put(project, projectMaxConcurrent - projectRunningCount);
                continue;
            }
            projectProduceCount.put(project, 0);
        }
        // _global is a virtual project, used to schedule cron jobs, such as SecondStorageLowCardinalityJob.
        int globalRunningCount = projectRunningCountMap.getOrDefault(GLOBAL_PROJECT, 0);
        projectProduceCount.put(GLOBAL_PROJECT, globalRunningCount == 0 ? 1 : 0);
        return projectProduceCount;
    }

    private Map<String, Integer> getProjectProduceCount(Map<String, Integer> projectRunningCountMap) {
        return getProjectProduceCount(projectRunningCountMap, JobTypeEnum.Category.ALL);
    }

    private void releaseExpiredLock() {
        int batchSize = jobContext.getKylinConfig().getJobSchedulerMasterPollBatchSize();
        JobMapperFilter filter = new JobMapperFilter();
        List<String> jobIds = jobContext.getJobLockMapper().findExpiredORNonLockIdList(batchSize);
        if (jobIds.isEmpty()) {
            return;
        }
        filter.setJobIds(jobIds);
        List<JobInfo> jobs = jobContext.getJobInfoMapper().selectByJobFilter(filter);
        List<String> jobInfoIds = jobs.stream().map(JobInfo::getJobId).collect(Collectors.toList());
        List<String> toRemoveLocks = Lists.newArrayList(jobIds).stream().filter(jobId -> !jobInfoIds.contains(jobId))
                .collect(Collectors.toList());
        for (JobInfo job : jobs) {
            ExecutablePO po = JobInfoUtil.deserializeExecutablePO(job);
            if (po != null) {
                ExecutableState state = ExecutableState.valueOf(po.getOutput().getStatus());
                if (state.isNotProgressing() || state.isFinalState()) {
                    toRemoveLocks.add(job.getJobId());
                }
            }
        }
        if (!toRemoveLocks.isEmpty()) {
            jobContext.getJobLockMapper().batchRemoveLock(toRemoveLocks);
        }
    }

    private void consumeJob() {
        long delay = jobContext.getKylinConfig().getSchedulerPollIntervalSecond();
        try {
            // The number of tasks to be obtained cannot exceed the free slots of the 'executorPool'
            int exeFreeSlots = this.consumerMaxThreads - this.runningJobMap.size();
            if (exeFreeSlots <= 0) {
                logger.info("No free slots to execute job");
                return;
            }
            int batchSize = jobContext.getKylinConfig().getJobSchedulerSlavePollBatchSize();
            if (exeFreeSlots < batchSize) {
                batchSize = exeFreeSlots;
            }
            List<String> projects = ResourceGroupManager.getInstance(jobContext.getKylinConfig())
                    .listProjectWithPermission();
            List<String> jobIdList = findNonLockIdListInOrder(batchSize, projects);

            if (CollectionUtils.isEmpty(jobIdList)) {
                return;
            }
            // for slave node, force catchup metadata before execute jobs
            if (!isMaster.get()) {
                StreamingUtils.replayAuditlog();
            }
            // submit job
            jobIdList.forEach(jobId -> {
                // check 'runningJobMap', avoid processing submitted tasks
                if (runningJobMap.containsKey(jobId)) {
                    logger.warn("Job {} has already been submitted", jobId);
                    return;
                }
                Pair<JobInfo, AbstractJobExecutable> job = fetchJob(jobId);
                if (null == job) {
                    return;
                }
                final JobInfo jobInfo = job.getFirst();
                final AbstractJobExecutable jobExecutable = job.getSecond();
                if (!canSubmitJob(jobId, jobInfo, jobExecutable)) {
                    return;
                }
                // record running job id, and job scheduled time
                runningJobMap.put(jobId, new Pair<>(jobExecutable, System.currentTimeMillis()));
                executorPool.execute(() -> executeJob(jobExecutable, jobInfo));
            });
        } catch (Exception e) {
            logger.error("Something's wrong when consuming job", e);
        } finally {
            logger.info("{} running jobs in current scheduler", getRunningJob().size());
            slave.schedule(this::consumeJob, delay, TimeUnit.SECONDS);
        }
    }

    public List<String> findNonLockIdListInOrder(int batchSize, List<String> projects) {
        KylinConfig config = jobContext.getKylinConfig();
        List<String> projectsAndGlobal = new ArrayList<>(projects);
        projectsAndGlobal.add(GLOBAL_PROJECT);
        if (projectsAndGlobal.size() == NProjectManager.getInstance(config).listAllProjects().size() + 1) {
            projectsAndGlobal = null;
        }
        List<PriorityFistRandomOrderJob> jobIdList = jobContext.getJobLockMapper().findNonLockIdList(batchSize,
                projectsAndGlobal);
        // Shuffle jobs avoiding jobLock conflict.
        // At the same time, we should ensure the overall order.
        if (hasRunningJob()) {
            Collections.sort(jobIdList);
        }
        return jobIdList.stream().map(PriorityFistRandomOrderJob::getJobId).collect(Collectors.toList());
    }

    private Pair<JobInfo, AbstractJobExecutable> fetchJob(String jobId) {
        try {
            JobInfo jobInfo = jobContext.getJobInfoMapper().selectByJobId(jobId);
            if (jobInfo == null) {
                logger.warn("can not find job info {}", jobId);
                return null;
            }
            AbstractJobExecutable jobExecutable = getJobExecutable(jobInfo);
            return new Pair<>(jobInfo, jobExecutable);
        } catch (Exception e) {
            logger.error("Fetch job failed, job id: {}", jobId, e);
            return null;
        }
    }

    private boolean canSubmitJob(String jobId, JobInfo jobInfo, AbstractJobExecutable jobExecutable) {
        try {
            if (JobScheduler.MASTER_SCHEDULER.equals(jobId)) {
                return false;
            }
            if (Objects.isNull(jobInfo)) {
                logger.warn("Job not found: {}", jobId);
                return false;
            }
            if (!jobContext.getResourceAcquirer().tryAcquire(jobExecutable)) {
                return false;
            }
        } catch (Exception e) {
            logger.error("Error when preparing to submit job: {}", jobId, e);
            return false;
        }
        return true;
    }

    private void executeJob(AbstractJobExecutable jobExecutable, JobInfo jobInfo) {
        JdbcJobLock jobLock = null;
        try (JobExecutor jobExecutor = new JobExecutor(jobContext, jobExecutable);
                SetLogCategory ignore = new SetLogCategory(LogConstant.BUILD_CATEGORY)) {
            // Check job status
            AbstractExecutable executable = (AbstractExecutable) jobExecutable;
            ExecutableState jobStatus = executable.getStatus();
            if (ExecutableState.PENDING != jobStatus) {
                logger.warn("Unexpected status for {} <{}>, should not execute job", jobExecutable.getJobId(),
                        jobStatus);
                if (ExecutableState.RUNNING == jobStatus) {
                    jobLock = tryJobLock(jobExecutable);
                    if (jobLock != null) {
                        //Maybe other node crashed during job execution, resume job status from running to ready.
                        logger.warn("Resume <RUNNING> job {}", jobExecutable.getJobId());
                        ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), executable.getProject())
                                .resumeJob(jobExecutable.getJobId(), true);
                    }
                }
                return;
            }

            jobLock = tryJobLock(jobExecutable);
            if (null == jobLock) {
                return;
            }
            if (jobContext.isProjectReachQuotaLimit(jobExecutable.getProject()) && JobCheckUtil
                    .stopJobIfStorageQuotaLimitReached(jobContext, jobInfo.getProject(), jobInfo.getJobId())) {
                return;
            }
            // heavy action
            jobExecutor.execute();
        } catch (Exception e) {
            logger.error("Execute job failed {}", jobExecutable.getJobId(), e);
        } finally {
            if (jobLock != null) {
                stopJobLockRenewAfterExecute(jobLock);
            }
            runningJobMap.remove(jobExecutable.getJobId());
        }
    }

    private JdbcJobLock tryJobLock(AbstractJobExecutable jobExecutable) throws LockException {
        JdbcJobLock jobLock = new JdbcJobLock(jobExecutable.getJobId(), jobContext.getServerNode(),
                jobContext.getKylinConfig().getJobSchedulerJobRenewalSec(),
                jobContext.getKylinConfig().getJobSchedulerJobRenewalRatio(), jobContext.getLockClient(),
                new JobAcquireListener(jobExecutable));
        if (!jobLock.tryAcquire()) {
            logger.info("Acquire job lock failed.");
            return null;
        }
        return jobLock;
    }

    private void stopJobLockRenewAfterExecute(JdbcJobLock jobLock) {
        try {
            String jobId = jobLock.getLockId();
            JobInfo jobInfo = jobContext.getJobInfoMapper().selectByJobId(jobId);
            AbstractExecutable jobExecutable = (AbstractExecutable) getJobExecutable(jobInfo);
            if (jobExecutable.getStatusInMem() == ExecutableState.RUNNING) {
                logger.error("Unexpected status for {} <{}>, mark job error", jobId, jobExecutable.getStatusInMem());
                markErrorJob(jobId, jobExecutable.getProject());
            }
        } catch (Exception e) {
            logger.error("Fail to check status before stop renew job lock {}", jobLock.getLockId(), e);
        } finally {
            jobLock.stopRenew();
        }
    }

    private void markErrorJob(String jobId, String project) {
        try {
            val manager = ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            manager.errorJob(jobId);
        } catch (Exception e) {
            logger.warn("[UNEXPECTED_THINGS_HAPPENED] project {} job {} should be error but mark failed", project,
                    jobId, e);
        }
    }

    private boolean isIndexPlannerRecJob(JobInfo jobInfo) {
        if (!JobTypeEnum.Category.REC.equals(JobTypeEnum.getEnumByName(jobInfo.getJobType()).getCategory())) {
            return false;
        }
        ExecutablePO po = JobInfoUtil.deserializeExecutablePO(jobInfo);
        return po != null && StringUtils
                .equalsIgnoreCase(po.getParams().get(NBatchConstants.P_PLANNER_AUTO_APPROVE_ENABLED), "true");
    }

    private boolean isIndexPlannerBuildJob(JobInfo jobInfo) {
        if (!JobTypeEnum.Category.BUILD.equals(JobTypeEnum.getEnumByName(jobInfo.getJobType()).getCategory())) {
            return false;
        }
        ExecutablePO po = JobInfoUtil.deserializeExecutablePO(jobInfo);
        return po != null && StringUtils
                .equalsIgnoreCase(po.getParams().get(NBatchConstants.P_PLANNER_AUTO_APPROVE_ENABLED), "true");
    }

    private AbstractJobExecutable getJobExecutable(JobInfo jobInfo) {
        ExecutablePO executablePO = JobInfoUtil.deserializeExecutablePO(jobInfo);
        return ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), jobInfo.getProject())
                .fromPO(executablePO);
    }

    private class MasterAcquireListener implements LockAcquireListener {

        @Override
        public void onSucceed() {
            if (isMaster.compareAndSet(false, true)) {
                logger.info("Job scheduler become master.");
            } else {
                logger.debug("Job scheduler keep on master");
            }
        }

        @Override
        public void onFailed() {
            if (isMaster.compareAndSet(true, false)) {
                logger.info("Job scheduler fallback standby.");
            } else {
                logger.debug("Job scheduler keep on standby");
            }
            // standby
            master.schedule(JdbcJobScheduler.this::standby, masterLock.getRenewalSec(), TimeUnit.SECONDS);
        }
    }

    @Getter
    private static class JobAcquireListener implements LockAcquireListener {

        private final AbstractJobExecutable jobExecutable;

        JobAcquireListener(AbstractJobExecutable jobExecutable) {
            this.jobExecutable = jobExecutable;
        }

        @Override
        public void onSucceed() {
            // do nothing
        }

        @Override
        public void onFailed() {
            // do nothing
        }
    }

}

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

package org.apache.kylin.streaming.jobs.scheduler;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.ExecutorServiceUtil;
import org.apache.kylin.guava30.shaded.common.base.Predicate;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.job.JobContext;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.core.lock.JdbcJobLock;
import org.apache.kylin.job.core.lock.LockAcquireListener;
import org.apache.kylin.job.core.lock.LockException;
import org.apache.kylin.job.domain.JobLock;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.cube.utils.StreamingUtils;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.resourcegroup.ResourceGroupManager;
import org.apache.kylin.streaming.constants.StreamingConstants;
import org.apache.kylin.streaming.jobs.thread.StreamingJobRunner;
import org.apache.kylin.streaming.manager.StreamingJobManager;
import org.apache.kylin.streaming.metadata.StreamingJobMeta;
import org.apache.kylin.streaming.util.JobKiller;
import org.apache.kylin.streaming.util.MetaInfoUpdater;
import org.springframework.util.CollectionUtils;

import lombok.Getter;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StreamingScheduler {
    @Getter
    private AtomicBoolean initialized = new AtomicBoolean(false);

    @Getter
    private AtomicBoolean hasStarted = new AtomicBoolean(false);

    private JobContext jobContext = JobContextUtil.getJobContext(KylinConfig.getInstanceFromEnv());

    private final Map<String, JdbcJobLock> streamingJobLockCache = new ConcurrentHashMap<>();

    private ExecutorService jobPool;
    private Map<String, StreamingJobRunner> runnerMap = Maps.newHashMap();
    private Map<String, AbstractMap.SimpleEntry<AtomicInteger, AtomicInteger>> retryMap = Maps.newHashMap();

    private static final Map<String, StreamingScheduler> INSTANCE_MAP = Maps.newConcurrentMap();
    private ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
    private static final List<JobStatusEnum> STARTABLE_STATUS_LIST = Arrays.asList(JobStatusEnum.ERROR,
            JobStatusEnum.STOPPED, JobStatusEnum.NEW, JobStatusEnum.LAUNCHING_ERROR);

    private static StreamingJobStatusWatcher jobStatusUpdater = new StreamingJobStatusWatcher();

    public StreamingScheduler() {
        init();
    }

    public static synchronized StreamingScheduler getInstance() {
        return Singletons.getInstance(StreamingScheduler.class);
    }

    public synchronized void init() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();

        if (!config.isJobNode() && !config.isDataLoadingNode()) {
            log.info("server mode: {}, no need to run job scheduler", config.getServerMode());
            return;
        }
        if (!UnitOfWork.isAlreadyInTransaction())
            log.info("Initializing Job Engine ....");

        if (!initialized.compareAndSet(false, true)) {
            return;
        }

        if (config.isStreamingEnabled()) {
            int maxPoolSize = config.getNodeMaxStreamingConcurrentJobLimit();
            ThreadFactory executorThreadFactory = new BasicThreadFactory.Builder()
                    .namingPattern("StreamingJobWorker-%d").uncaughtExceptionHandler((t, e) -> {
                        log.error("Something wrong happened when building threadFactory of streaming.", e);
                        throw new IllegalStateException(e);
                    }).build();

            jobPool = new ThreadPoolExecutor(maxPoolSize, maxPoolSize * 2, Long.MAX_VALUE, TimeUnit.DAYS,
                    new SynchronousQueue<>(), executorThreadFactory);
            log.debug("New StreamingScheduler created : {}", System.identityHashCode(StreamingScheduler.this));
            scheduledExecutorService.scheduleWithFixedDelay(this::retryJob, 5, 1, TimeUnit.MINUTES);
            jobStatusUpdater.schedule();
        }
        resumeJobs(config);
        hasStarted.set(true);
    }

    public synchronized void submitJob(String project, String modelId, JobTypeEnum jobType) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        if (!config.isStreamingEnabled()) {
            return;
        }
        String jobId = StreamingUtils.getJobId(modelId, jobType.name());
        JdbcJobLock jobLock = tryJobLock(jobId, project);
        if (jobLock == null) {
            throw new IllegalStateException(String.format(Locale.ROOT, "Job %s is locked by other node.", jobId));
        }
        try {
            var jobMeta = StreamingJobManager.getInstance(config, project).getStreamingJobByUuid(jobId);
            checkJobStartStatus(jobMeta, jobId);
            JobKiller.killProcess(jobMeta);

            killYarnApplication(project, jobId, modelId);

            Predicate<NDataSegment> predicate = item -> (item.getStatus() == SegmentStatusEnum.NEW
                    || item.getStorageBytesSize() == 0) && item.getAdditionalInfo() != null;
            if (JobTypeEnum.STREAMING_BUILD == jobType) {
                deleteBrokenSegment(project, modelId, item -> predicate.apply(item)
                        && !item.getAdditionalInfo().containsKey(StreamingConstants.FILE_LAYER));
            } else if (JobTypeEnum.STREAMING_MERGE == jobType) {
                deleteBrokenSegment(project, modelId, item -> predicate.apply(item)
                        && item.getAdditionalInfo().containsKey(StreamingConstants.FILE_LAYER));
            }
            MetaInfoUpdater.updateJobState(project, jobId, JobStatusEnum.STARTING);
            StreamingJobRunner jobRunner = new StreamingJobRunner(project, modelId, jobType);
            runnerMap.put(jobId, jobRunner);
            jobPool.execute(jobRunner);
            if (!StreamingUtils.isJobOnCluster(config)) {
                MetaInfoUpdater.updateJobState(project, jobId,
                        Sets.newHashSet(JobStatusEnum.RUNNING, JobStatusEnum.ERROR), JobStatusEnum.RUNNING);
            }
        } catch (Exception e) {
            log.error("Submit streaming job failed, jobId: {}", jobId, e);
            throw e;
        } finally {
            try {
                jobLock.tryRelease();
            } catch (LockException ex) {
                log.error("Release streaming job lock failed.", ex);
            }
        }
    }

    private JdbcJobLock tryJobLock(String jobId, String project) {
        JdbcJobLock jobLock = streamingJobLockCache.computeIfAbsent(jobId,
                id -> new JdbcJobLock(jobId, jobContext.getServerNode(),
                        jobContext.getKylinConfig().getJobSchedulerJobRenewalSec(),
                        jobContext.getKylinConfig().getJobSchedulerJobRenewalRatio(), jobContext.getLockClient(),
                        new StreamingJobLockListener(jobId, streamingJobLockCache)));
        if (!jobLock.isLocked()) {
            if (jobContext.getJobLockMapper().selectByJobId(jobId) == null) {
                jobContext.getJobLockMapper()
                        .insertSelective(new JobLock(jobId, project, 3, JobLock.JobTypeEnum.STREAMING));
            }
            synchronized (jobLock) {
                try {
                    if (!jobLock.isLocked() && !jobLock.tryAcquire()) {
                        log.info("Acquire job lock failed, jobId: {}", jobId);
                        return null;
                    }
                } catch (LockException e) {
                    log.info("Acquire job lock failed, jobId: {}, error: {}", jobId, e);
                    return null;
                }
            }
        }
        return jobLock;
    }

    private static class StreamingJobLockListener implements LockAcquireListener {
        private String jobId;
        private Map<String, JdbcJobLock> streamingJobLockCache;

        public StreamingJobLockListener(String jobId, Map<String, JdbcJobLock> streamingJobLockCache) {
            this.jobId = jobId;
            this.streamingJobLockCache = streamingJobLockCache;
        }

        @Override
        public void onSucceed() {
            streamingJobLockCache.get(jobId).setLocked(true);
        }

        @Override
        public void onFailed() {
            streamingJobLockCache.get(jobId).setLocked(false);
        }
    }

    public synchronized void stopJob(String project, String modelId, JobTypeEnum jobType) {
        String jobId = StreamingUtils.getJobId(modelId, jobType.name());

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        boolean existed = applicationExisted(jobId);
        if (existed) {
            StreamingJobManager jobMgr = StreamingJobManager.getInstance(config, project);
            JobStatusEnum status = jobMgr.getStreamingJobByUuid(jobId).getCurrentStatus();
            if (JobStatusEnum.ERROR == status || JobStatusEnum.STOPPED == status) {
                return;
            }
            MetaInfoUpdater.updateJobState(project, jobId, JobStatusEnum.STOPPING);
            doStop(project, modelId, jobType);
        } else {
            doStop(project, modelId, jobType);
            if (StreamingUtils.isJobOnCluster(config)) {
                MetaInfoUpdater.updateJobState(project, jobId,
                        Sets.newHashSet(JobStatusEnum.STOPPED, JobStatusEnum.ERROR), JobStatusEnum.ERROR);
            } else {
                MetaInfoUpdater.updateJobState(project, jobId,
                        Sets.newHashSet(JobStatusEnum.STOPPED, JobStatusEnum.ERROR), JobStatusEnum.STOPPED);
            }
        }
    }

    private void doStop(String project, String modelId, JobTypeEnum jobType) {
        String jobId = StreamingUtils.getJobId(modelId, jobType.name());
        StreamingJobRunner runner = runnerMap.get(jobId);
        synchronized (runnerMap) {
            if (Objects.isNull(runner)) {
                runner = new StreamingJobRunner(project, modelId, jobType);
                runner.init();
                runnerMap.put(jobId, runner);
            }
        }
        runner.stop();
    }

    public void forceShutdown() {
        log.info("Shutting down DefaultScheduler ....");
        releaseResources();
        ExecutorServiceUtil.forceShutdown(scheduledExecutorService);
        ExecutorServiceUtil.forceShutdown(jobPool);
    }

    private void releaseResources() {
        initialized.set(false);
        hasStarted.set(false);
        INSTANCE_MAP.clear();
    }

    private void checkJobStartStatus(StreamingJobMeta jobMeta, String jobId) {
        if (!STARTABLE_STATUS_LIST.contains(jobMeta.getCurrentStatus())) {
            throw new KylinException(ServerErrorCode.JOB_START_FAILURE, jobId);
        }
    }

    public void retryJob() {
        val config = KylinConfig.getInstanceFromEnv();
        List<String> prjList = ResourceGroupManager.getInstance(config).listProjectWithPermission();
        prjList.forEach(project -> {
            StreamingJobManager mgr = StreamingJobManager.getInstance(config, project);
            List<StreamingJobMeta> jobMetaList = mgr.listAllStreamingJobMeta();
            List<StreamingJobMeta> retryJobMetaList = jobMetaList.stream()
                    .filter(meta -> "true".equals(meta.getParams().getOrDefault(
                            StreamingConstants.STREAMING_RETRY_ENABLE, config.getStreamingJobRetryEnabled())))
                    .collect(Collectors.toList());
            retryJobMetaList.forEach(meta -> {
                JobStatusEnum status = meta.getCurrentStatus();
                String modelId = meta.getModelId();
                String jobId = StreamingUtils.getJobId(modelId, meta.getJobType().name());
                if (retryMap.containsKey(jobId) || status == JobStatusEnum.ERROR) {
                    boolean canRestart = !applicationExisted(jobId);
                    if (canRestart) {
                        if (!retryMap.containsKey(jobId)) {
                            if (status == JobStatusEnum.ERROR) {
                                retryMap.put(jobId,
                                        new AbstractMap.SimpleEntry<>(
                                                new AtomicInteger(config.getStreamingJobRetryInterval()),
                                                new AtomicInteger(1)));
                            }
                        } else {
                            int targetCnt = retryMap.get(jobId).getKey().get();
                            int currCnt = retryMap.get(jobId).getValue().get();
                            log.debug("targetCnt=" + targetCnt + ",currCnt=" + currCnt + " jobId=" + jobId);

                            if (targetCnt <= config.getStreamingJobMaxRetryInterval()) {
                                retryMap.get(jobId).getValue().incrementAndGet();
                                if (targetCnt == currCnt && status == JobStatusEnum.ERROR) {
                                    log.info("begin to restart job:" + modelId + "_" + meta.getJobType());
                                    restartJob(config, meta, jobId, targetCnt);
                                }
                            }

                        }
                    } else {
                        if (status == JobStatusEnum.RUNNING && retryMap.containsKey(jobId)) {
                            log.debug("remove jobId=" + jobId);
                            retryMap.remove(jobId);
                        }
                    }
                }
            });
        });
    }

    private void restartJob(KylinConfig config, StreamingJobMeta meta, String jobId, int targetCnt) {
        try {
            submitJob(meta.getProject(), meta.getModelId(), meta.getJobType());
            if (targetCnt < config.getStreamingJobMaxRetryInterval()) {
                retryMap.get(jobId).getKey().addAndGet(config.getStreamingJobRetryInterval());
            }
            retryMap.get(jobId).getValue().set(0);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void deleteBrokenSegment(String project, String dataflowId, Predicate<NDataSegment> predicate) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NDataflowManager dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            NDataflow df = dfMgr.getDataflow(dataflowId);
            Segments<NDataSegment> segments = df.getSegments();
            List<NDataSegment> toRemoveSegs = segments.stream().filter(item -> predicate.apply(item))
                    .collect(Collectors.toList());
            if (!toRemoveSegs.isEmpty()) {
                val dfUpdate = new NDataflowUpdate(dataflowId);
                dfUpdate.setToRemoveSegs(toRemoveSegs.toArray(new NDataSegment[0]));
                dfMgr.updateDataflow(dfUpdate);
            }
            return 0;
        }, project);
    }

    public boolean applicationExisted(String jobId) {
        return JobKiller.applicationExisted(jobId);
    }

    public void killYarnApplication(String project, String jobId, String modelId) {
        boolean isExists = applicationExisted(jobId);
        if (isExists) {
            JobKiller.killApplication(jobId);
            isExists = applicationExisted(jobId);
            if (isExists) {
                String model = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                        .getDataModelDesc(modelId).getAlias();
                throw new KylinException(ServerErrorCode.REPEATED_START_ERROR,
                        String.format(Locale.ROOT, MsgPicker.getMsg().getJobStartFailure(), model));
            }
        }
    }

    private void killJob(String project, String modelId, JobTypeEnum jobTypeEnum) {
        killJob(project, modelId, jobTypeEnum, JobStatusEnum.ERROR);
    }

    public void killJob(String project, String modelId, JobTypeEnum jobTypeEnum, JobStatusEnum status) {
        killJob(project, StreamingUtils.getJobId(modelId, jobTypeEnum.name()), status);
    }

    private void killJob(String project, String jobId, JobStatusEnum status) {
        val config = KylinConfig.getInstanceFromEnv();
        var jobMeta = StreamingJobManager.getInstance(config, project).getStreamingJobByUuid(jobId);
        JobKiller.killProcess(jobMeta);
        JobKiller.killApplication(jobId);

        MetaInfoUpdater.updateJobState(project, jobId, status);
    }

    private void resumeJobs(KylinConfig config) {
        List<String> prjList = ResourceGroupManager.getInstance(config).listProjectWithPermission();
        prjList.forEach(project -> {
            StreamingJobManager mgr = StreamingJobManager.getInstance(config, project);
            List<StreamingJobMeta> jobMetaList = mgr.listAllStreamingJobMeta();

            if (CollectionUtils.isEmpty(jobMetaList)) {
                return;
            }
            List<StreamingJobMeta> retryJobMetaList = jobMetaList.stream()
                    .filter(meta -> JobStatusEnum.STARTING == meta.getCurrentStatus()
                            || JobStatusEnum.STOPPING == meta.getCurrentStatus()
                            || JobStatusEnum.RUNNING == meta.getCurrentStatus()
                            || JobStatusEnum.ERROR == meta.getCurrentStatus())
                    .collect(Collectors.toList());

            retryJobMetaList.forEach(meta -> {
                val modelId = meta.getModelId();
                val jobType = meta.getJobType();
                val jobId = StreamingUtils.getJobId(modelId, jobType.name());
                JdbcJobLock jobLock = tryJobLock(jobId, project);
                if (jobLock == null) {
                    log.info("Skip to resume job : {}, because it is locked by other node.", jobId);
                    return;
                }
                try {
                    if (meta.isSkipListener()) {
                        skipJobListener(project, StreamingUtils.getJobId(modelId, jobType.name()), false);
                    }
                    if (JobStatusEnum.RUNNING == meta.getCurrentStatus()
                            || JobStatusEnum.STARTING == meta.getCurrentStatus()) {
                        killJob(project, meta.getModelId(), meta.getJobType(), JobStatusEnum.STOPPED);
                        submitJob(project, modelId, jobType);
                    } else {
                        killJob(project, meta.getModelId(), meta.getJobType());
                        jobLock.tryRelease();
                    }
                } catch (Exception e) {
                    log.error("Error when resume job : {}", jobId, e);
                } finally {
                    try {
                        jobLock.tryRelease();
                    } catch (LockException ex) {
                        log.error("Release streaming job lock failed", ex);
                    }
                }
            });
        });
    }

    public void skipJobListener(String project, String uuid, boolean skip) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            val config = KylinConfig.getInstanceFromEnv();
            val mgr = StreamingJobManager.getInstance(config, project);
            mgr.updateStreamingJob(uuid, copyForWrite -> {
                if (copyForWrite != null) {
                    copyForWrite.setSkipListener(skip);
                }
            });
            return null;
        }, project);
    }

}

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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.cube.model.CubeBuildTypeEnum;
import org.apache.kylin.engine.EngineFactory;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.common.HadoopShellExecutable;
import org.apache.kylin.engine.mr.common.MapReduceExecutable;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.JobInstance;
import org.apache.kylin.job.Scheduler;
import org.apache.kylin.job.SchedulerFactory;
import org.apache.kylin.job.common.ShellExecutable;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.constant.JobStepStatusEnum;
import org.apache.kylin.job.constant.JobTimeFilterEnum;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.exception.JobException;
import org.apache.kylin.job.exception.SchedulerException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.Output;
import org.apache.kylin.job.lock.DistributedJobLock;
import org.apache.kylin.job.lock.JobLock;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.source.ISource;
import org.apache.kylin.source.SourceFactory;
import org.apache.kylin.source.SourcePartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * @author ysong1
 */

@EnableAspectJAutoProxy(proxyTargetClass = true)
@Component("jobService")
public class JobService extends BasicService implements InitializingBean {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(JobService.class);

    private JobLock jobLock;

    @Autowired
    private AccessService accessService;

    /*
    * (non-Javadoc)
    *
    * @see
    * org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
    */
    @SuppressWarnings("unchecked")
    @Override
    public void afterPropertiesSet() throws Exception {

        String timeZone = getConfig().getTimeZone();
        TimeZone tzone = TimeZone.getTimeZone(timeZone);
        TimeZone.setDefault(tzone);

        final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        final Scheduler<AbstractExecutable> scheduler = (Scheduler<AbstractExecutable>) SchedulerFactory.scheduler(kylinConfig.getSchedulerType());

        jobLock = (JobLock) ClassUtil.newInstance(kylinConfig.getJobControllerLock());

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    scheduler.init(new JobEngineConfig(kylinConfig), jobLock);
                    if (!scheduler.hasStarted()) {
                        logger.info("scheduler has not been started");
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    scheduler.shutdown();
                } catch (SchedulerException e) {
                    logger.error("error occurred to shutdown scheduler", e);
                }
            }
        }));
    }

    public List<JobInstance> listAllJobs(final String cubeName, final String projectName, final List<JobStatusEnum> statusList, final Integer limitValue, final Integer offsetValue, final JobTimeFilterEnum timeFilter) throws IOException, JobException {
        Integer limit = (null == limitValue) ? 30 : limitValue;
        Integer offset = (null == offsetValue) ? 0 : offsetValue;
        List<JobInstance> jobs = listAllJobs(cubeName, projectName, statusList, timeFilter);
        Collections.sort(jobs);

        if (jobs.size() <= offset) {
            return Collections.emptyList();
        }

        if ((jobs.size() - offset) < limit) {
            return jobs.subList(offset, jobs.size());
        }

        return jobs.subList(offset, offset + limit);
    }

    public List<JobInstance> listAllJobs(final String cubeName, final String projectName, final List<JobStatusEnum> statusList, final JobTimeFilterEnum timeFilter) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        long currentTimeMillis = calendar.getTimeInMillis();
        long timeStartInMillis = getTimeStartInMillis(calendar, timeFilter);
        return listCubeJobInstance(cubeName, projectName, statusList, timeStartInMillis, currentTimeMillis);
    }

    @Deprecated
    public List<JobInstance> listAllJobs(final String cubeName, final String projectName, final List<JobStatusEnum> statusList, final Integer limitValue, final Integer offsetValue) throws IOException, JobException {
        Integer limit = (null == limitValue) ? 30 : limitValue;
        Integer offset = (null == offsetValue) ? 0 : offsetValue;
        List<JobInstance> jobs = listAllJobs(cubeName, projectName, statusList);
        Collections.sort(jobs);

        if (jobs.size() <= offset) {
            return Collections.emptyList();
        }

        if ((jobs.size() - offset) < limit) {
            return jobs.subList(offset, jobs.size());
        }

        return jobs.subList(offset, offset + limit);
    }

    public List<JobInstance> listAllJobs(final String cubeName, final String projectName, final List<JobStatusEnum> statusList) {
        return listCubeJobInstance(cubeName, projectName, statusList);
    }

    private List<JobInstance> listCubeJobInstance(final String cubeName, final String projectName, List<JobStatusEnum> statusList, final long timeStartInMillis, final long timeEndInMillis) {
        Set<ExecutableState> states = convertStatusEnumToStates(statusList);
        final Map<String, Output> allOutputs = getExecutableManager().getAllOutputs(timeStartInMillis, timeEndInMillis);
        return Lists.newArrayList(FluentIterable.from(listAllCubingJobs(cubeName, projectName, states, timeStartInMillis, timeEndInMillis, allOutputs)).transform(new Function<CubingJob, JobInstance>() {
            @Override
            public JobInstance apply(CubingJob cubingJob) {
                return parseToJobInstance(cubingJob, allOutputs);
            }
        }));
    }

    private List<JobInstance> listCubeJobInstance(final String cubeName, final String projectName, List<JobStatusEnum> statusList) {
        Set<ExecutableState> states = convertStatusEnumToStates(statusList);
        final Map<String, Output> allOutputs = getExecutableManager().getAllOutputs();
        return Lists.newArrayList(FluentIterable.from(listAllCubingJobs(cubeName, projectName, states, allOutputs)).transform(new Function<CubingJob, JobInstance>() {
            @Override
            public JobInstance apply(CubingJob cubingJob) {
                return parseToJobInstance(cubingJob, allOutputs);
            }
        }));
    }

    private Set<ExecutableState> convertStatusEnumToStates(List<JobStatusEnum> statusList) {
        Set<ExecutableState> states;
        if (statusList == null || statusList.isEmpty()) {
            states = EnumSet.allOf(ExecutableState.class);
        } else {
            states = Sets.newHashSet();
            for (JobStatusEnum status : statusList) {
                states.add(parseToExecutableState(status));
            }
        }
        return states;
    }

    private long getTimeStartInMillis(Calendar calendar, JobTimeFilterEnum timeFilter) {
        switch (timeFilter) {
        case LAST_ONE_DAY:
            calendar.add(Calendar.DAY_OF_MONTH, -1);
            return calendar.getTimeInMillis();
        case LAST_ONE_WEEK:
            calendar.add(Calendar.WEEK_OF_MONTH, -1);
            return calendar.getTimeInMillis();
        case LAST_ONE_MONTH:
            calendar.add(Calendar.MONTH, -1);
            return calendar.getTimeInMillis();
        case LAST_ONE_YEAR:
            calendar.add(Calendar.YEAR, -1);
            return calendar.getTimeInMillis();
        case ALL:
            return 0;
        default:
            throw new RuntimeException("illegal timeFilter for job history:" + timeFilter);
        }
    }

    private ExecutableState parseToExecutableState(JobStatusEnum status) {
        switch (status) {
        case DISCARDED:
            return ExecutableState.DISCARDED;
        case ERROR:
            return ExecutableState.ERROR;
        case FINISHED:
            return ExecutableState.SUCCEED;
        case NEW:
            return ExecutableState.READY;
        case PENDING:
            return ExecutableState.READY;
        case RUNNING:
            return ExecutableState.RUNNING;
        default:
            throw new RuntimeException("illegal status:" + status);
        }
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'OPERATION') or hasPermission(#cube, 'MANAGEMENT')")
    public JobInstance submitJob(CubeInstance cube, long startDate, long endDate, long startOffset, long endOffset, //
            Map<Integer, Long> sourcePartitionOffsetStart, Map<Integer, Long> sourcePartitionOffsetEnd, CubeBuildTypeEnum buildType, boolean force, String submitter) throws IOException, JobException {

        if (cube.getStatus() == RealizationStatusEnum.DESCBROKEN) {
            throw new BadRequestException("Broken cube " + cube.getName() + " can't be built");
        }

        checkCubeDescSignature(cube);
        DefaultChainedExecutable job;
        if (buildType == CubeBuildTypeEnum.BUILD) {
            ISource source = SourceFactory.tableSource(cube);
            SourcePartition sourcePartition = new SourcePartition(startDate, endDate, startOffset, endOffset, sourcePartitionOffsetStart, sourcePartitionOffsetEnd);
            sourcePartition = source.parsePartitionBeforeBuild(cube, sourcePartition);
            CubeSegment newSeg = getCubeManager().appendSegment(cube, sourcePartition);
            lockSegment(newSeg.getUuid());
            job = EngineFactory.createBatchCubingJob(newSeg, submitter);
        } else if (buildType == CubeBuildTypeEnum.MERGE) {
            CubeSegment newSeg = getCubeManager().mergeSegments(cube, startDate, endDate, startOffset, endOffset, force);
            lockSegment(newSeg.getUuid());
            job = EngineFactory.createBatchMergeJob(newSeg, submitter);
        } else if (buildType == CubeBuildTypeEnum.REFRESH) {
            CubeSegment refreshSeg = getCubeManager().refreshSegment(cube, startDate, endDate, startOffset, endOffset);
            lockSegment(refreshSeg.getUuid());
            job = EngineFactory.createBatchCubingJob(refreshSeg, submitter);
        } else {
            throw new JobException("invalid build type:" + buildType);
        }
        getExecutableManager().addJob(job);
        JobInstance jobInstance = getSingleJobInstance(job);

        accessService.init(jobInstance, null);
        accessService.inherit(jobInstance, cube);

        return jobInstance;
    }

    private void checkCubeDescSignature(CubeInstance cube) {
        if (!cube.getDescriptor().checkSignature())
            throw new IllegalStateException("Inconsistent cube desc signature for " + cube.getDescriptor());
    }

    public JobInstance getJobInstance(String uuid) throws IOException, JobException {
        return getSingleJobInstance(getExecutableManager().getJob(uuid));
    }

    public Output getOutput(String id) {
        return getExecutableManager().getOutput(id);
    }

    private JobInstance getSingleJobInstance(AbstractExecutable job) {
        if (job == null) {
            return null;
        }
        Preconditions.checkState(job instanceof CubingJob, "illegal job type, id:" + job.getId());
        CubingJob cubeJob = (CubingJob) job;
        final JobInstance result = new JobInstance();
        result.setName(job.getName());
        result.setRelatedCube(CubingExecutableUtil.getCubeName(cubeJob.getParams()));
        result.setRelatedSegment(CubingExecutableUtil.getSegmentId(cubeJob.getParams()));
        result.setLastModified(cubeJob.getLastModified());
        result.setSubmitter(cubeJob.getSubmitter());
        result.setUuid(cubeJob.getId());
        result.setType(CubeBuildTypeEnum.BUILD);
        result.setStatus(parseToJobStatus(job.getStatus()));
        result.setMrWaiting(cubeJob.getMapReduceWaitTime() / 1000);
        result.setDuration(cubeJob.getDuration() / 1000);
        for (int i = 0; i < cubeJob.getTasks().size(); ++i) {
            AbstractExecutable task = cubeJob.getTasks().get(i);
            result.addStep(parseToJobStep(task, i, getExecutableManager().getOutput(task.getId())));
        }
        return result;
    }

    private JobInstance parseToJobInstance(AbstractExecutable job, Map<String, Output> outputs) {
        if (job == null) {
            return null;
        }
        Preconditions.checkState(job instanceof CubingJob, "illegal job type, id:" + job.getId());
        CubingJob cubeJob = (CubingJob) job;
        Output output = outputs.get(job.getId());
        final JobInstance result = new JobInstance();
        result.setName(job.getName());
        result.setRelatedCube(CubingExecutableUtil.getCubeName(cubeJob.getParams()));
        result.setRelatedSegment(CubingExecutableUtil.getSegmentId(cubeJob.getParams()));
        result.setLastModified(output.getLastModified());
        result.setSubmitter(cubeJob.getSubmitter());
        result.setUuid(cubeJob.getId());
        result.setType(CubeBuildTypeEnum.BUILD);
        result.setStatus(parseToJobStatus(output.getState()));
        result.setMrWaiting(AbstractExecutable.getExtraInfoAsLong(output, CubingJob.MAP_REDUCE_WAIT_TIME, 0L) / 1000);
        result.setExecStartTime(AbstractExecutable.getStartTime(output));
        result.setExecEndTime(AbstractExecutable.getEndTime(output));
        result.setDuration(AbstractExecutable.getDuration(result.getExecStartTime(), result.getExecEndTime()) / 1000);
        for (int i = 0; i < cubeJob.getTasks().size(); ++i) {
            AbstractExecutable task = cubeJob.getTasks().get(i);
            result.addStep(parseToJobStep(task, i, outputs.get(task.getId())));
        }
        return result;
    }

    private JobInstance.JobStep parseToJobStep(AbstractExecutable task, int i, Output stepOutput) {
        Preconditions.checkNotNull(stepOutput);
        JobInstance.JobStep result = new JobInstance.JobStep();
        result.setId(task.getId());
        result.setName(task.getName());
        result.setSequenceID(i);
        result.setStatus(parseToJobStepStatus(stepOutput.getState()));
        for (Map.Entry<String, String> entry : stepOutput.getExtra().entrySet()) {
            if (entry.getKey() != null && entry.getValue() != null) {
                result.putInfo(entry.getKey(), entry.getValue());
            }
        }
        result.setExecStartTime(AbstractExecutable.getStartTime(stepOutput));
        result.setExecEndTime(AbstractExecutable.getEndTime(stepOutput));
        if (task instanceof ShellExecutable) {
            result.setExecCmd(((ShellExecutable) task).getCmd());
        }
        if (task instanceof MapReduceExecutable) {
            result.setExecCmd(((MapReduceExecutable) task).getMapReduceParams());
            result.setExecWaitTime(AbstractExecutable.getExtraInfoAsLong(stepOutput, MapReduceExecutable.MAP_REDUCE_WAIT_TIME, 0L) / 1000);
        }
        if (task instanceof HadoopShellExecutable) {
            result.setExecCmd(((HadoopShellExecutable) task).getJobParams());
        }
        return result;
    }

    private JobStatusEnum parseToJobStatus(ExecutableState state) {
        switch (state) {
        case READY:
            return JobStatusEnum.PENDING;
        case RUNNING:
            return JobStatusEnum.RUNNING;
        case ERROR:
            return JobStatusEnum.ERROR;
        case DISCARDED:
            return JobStatusEnum.DISCARDED;
        case SUCCEED:
            return JobStatusEnum.FINISHED;
        case STOPPED:
        default:
            throw new RuntimeException("invalid state:" + state);
        }
    }

    private JobStepStatusEnum parseToJobStepStatus(ExecutableState state) {
        switch (state) {
        case READY:
            return JobStepStatusEnum.PENDING;
        case RUNNING:
            return JobStepStatusEnum.RUNNING;
        case ERROR:
            return JobStepStatusEnum.ERROR;
        case DISCARDED:
            return JobStepStatusEnum.DISCARDED;
        case SUCCEED:
            return JobStepStatusEnum.FINISHED;
        case STOPPED:
        default:
            throw new RuntimeException("invalid state:" + state);
        }
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#job, 'ADMINISTRATION') or hasPermission(#job, 'OPERATION') or hasPermission(#job, 'MANAGEMENT')")
    public void resumeJob(JobInstance job) throws IOException, JobException {
        lockSegment(job.getRelatedSegment());

        getExecutableManager().resumeJob(job.getId());
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#job, 'ADMINISTRATION') or hasPermission(#job, 'OPERATION') or hasPermission(#job, 'MANAGEMENT')")
    public JobInstance cancelJob(JobInstance job) throws IOException, JobException {
        CubeInstance cubeInstance = getCubeManager().getCube(job.getRelatedCube());
        final String segmentIds = job.getRelatedSegment();
        for (String segmentId : StringUtils.split(segmentIds)) {
            final CubeSegment segment = cubeInstance.getSegmentById(segmentId);
            if (segment != null && (segment.getStatus() == SegmentStatusEnum.NEW || segment.getDateRangeEnd() == 0)) {
                // Remove this segments
                CubeUpdate cubeBuilder = new CubeUpdate(cubeInstance);
                cubeBuilder.setToRemoveSegs(segment);
                getCubeManager().updateCube(cubeBuilder);
            }
        }
        getExecutableManager().discardJob(job.getId());

        //release the segment lock when discarded the job but the job hasn't scheduled
        releaseSegmentLock(job.getRelatedSegment());

        return job;
    }

    private void lockSegment(String segmentId) throws JobException {
        if (jobLock instanceof DistributedJobLock) {
            if (!((DistributedJobLock) jobLock).lockWithName(segmentId, getServerName())) {
                throw new JobException("Fail to get the segment lock, the segment may be building in another job server");
            }
        }
    }

    private void releaseSegmentLock(String segmentId) {
        if (jobLock instanceof DistributedJobLock) {
            ((DistributedJobLock) jobLock).unlockWithName(segmentId);
        }
    }

    private String getServerName() {
        String serverName = null;
        try {
            serverName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            logger.error("fail to get the hostname");
        }
        return serverName;
    }
}

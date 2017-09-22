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
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;
import org.apache.directory.api.util.Strings;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.cube.model.CubeBuildTypeEnum;
import org.apache.kylin.engine.EngineFactory;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.common.JobInfoConverter;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.JobInstance;
import org.apache.kylin.job.Scheduler;
import org.apache.kylin.job.SchedulerFactory;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.constant.JobTimeFilterEnum;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.exception.SchedulerException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.Output;
import org.apache.kylin.job.lock.JobLock;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentRange.TSRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.source.ISource;
import org.apache.kylin.source.SourceFactory;
import org.apache.kylin.source.SourcePartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.stereotype.Component;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * @author ysong1
 */

@EnableAspectJAutoProxy(proxyTargetClass = true)
@Component("jobService")
public class JobService extends BasicService implements InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(JobService.class);

    private JobLock jobLock;

    @Autowired
    @Qualifier("accessService")
    private AccessService accessService;

    @Autowired
    private AclEvaluate aclEvaluate;

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
        final Scheduler<AbstractExecutable> scheduler = (Scheduler<AbstractExecutable>) SchedulerFactory
                .scheduler(kylinConfig.getSchedulerType());

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

    private ExecutableState parseToExecutableState(JobStatusEnum status) {
        Message msg = MsgPicker.getMsg();

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
            case STOPPED:
                return ExecutableState.STOPPED;
            default:
                throw new BadRequestException(String.format(msg.getILLEGAL_EXECUTABLE_STATE(), status));
        }
    }

    private long getTimeStartInMillis(Calendar calendar, JobTimeFilterEnum timeFilter) {
        Message msg = MsgPicker.getMsg();

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
                throw new BadRequestException(String.format(msg.getILLEGAL_TIME_FILTER(), timeFilter));
        }
    }

    public JobInstance submitJob(CubeInstance cube, TSRange tsRange, SegmentRange segRange, //
            Map<Integer, Long> sourcePartitionOffsetStart, Map<Integer, Long> sourcePartitionOffsetEnd,
            CubeBuildTypeEnum buildType, boolean force, String submitter) throws IOException {
        aclEvaluate.checkProjectOperationPermission(cube);
        JobInstance jobInstance = submitJobInternal(cube, tsRange, segRange, sourcePartitionOffsetStart,
                sourcePartitionOffsetEnd, buildType, force, submitter);

        accessService.init(jobInstance, null);
        accessService.inherit(jobInstance, cube);

        return jobInstance;
    }

    public JobInstance submitJobInternal(CubeInstance cube, TSRange tsRange, SegmentRange segRange, //
                                         Map<Integer, Long> sourcePartitionOffsetStart, Map<Integer, Long> sourcePartitionOffsetEnd, //
                                         CubeBuildTypeEnum buildType, boolean force, String submitter) throws IOException {
        Message msg = MsgPicker.getMsg();

        if (cube.getStatus() == RealizationStatusEnum.DESCBROKEN) {
            throw new BadRequestException(String.format(msg.getBUILD_BROKEN_CUBE(), cube.getName()));
        }

        checkCubeDescSignature(cube);
        DefaultChainedExecutable job;

        CubeSegment newSeg = null;
        try {
            if (buildType == CubeBuildTypeEnum.BUILD) {
                ISource source = SourceFactory.getSource(cube);
                SourcePartition src = new SourcePartition(tsRange, segRange, sourcePartitionOffsetStart, sourcePartitionOffsetEnd);
                src = source.enrichSourcePartitionBeforeBuild(cube, src);
                newSeg = getCubeManager().appendSegment(cube, src);
                job = EngineFactory.createBatchCubingJob(newSeg, submitter);
            } else if (buildType == CubeBuildTypeEnum.MERGE) {
                newSeg = getCubeManager().mergeSegments(cube, tsRange, segRange, force);
                job = EngineFactory.createBatchMergeJob(newSeg, submitter);
            } else if (buildType == CubeBuildTypeEnum.REFRESH) {
                newSeg = getCubeManager().refreshSegment(cube, tsRange, segRange);
                job = EngineFactory.createBatchCubingJob(newSeg, submitter);
            } else {
                throw new BadRequestException(String.format(msg.getINVALID_BUILD_TYPE(), buildType));
            }

            getExecutableManager().addJob(job);

        } catch (Exception e) {
            if (newSeg != null) {
                logger.error("Job submission might failed for NEW segment {}, will clean the NEW segment from cube",
                        newSeg.getName());
                try {
                    // Remove this segments
                    CubeUpdate cubeBuilder = new CubeUpdate(cube);
                    cubeBuilder.setToRemoveSegs(newSeg);
                    getCubeManager().updateCube(cubeBuilder);
                } catch (Exception ee) {
                    // swallow the exception
                    logger.error("Clean New segment failed, ignoring it", e);
                }
            }
            throw e;
        }

        JobInstance jobInstance = getSingleJobInstance(job);

        return jobInstance;
    }

    private void checkCubeDescSignature(CubeInstance cube) {
        Message msg = MsgPicker.getMsg();

        if (!cube.getDescriptor().checkSignature())
            throw new BadRequestException(
                    String.format(msg.getINCONSISTENT_CUBE_DESC_SIGNATURE(), cube.getDescriptor()));
    }

    public JobInstance getJobInstance(String uuid) {
        return getSingleJobInstance(getExecutableManager().getJob(uuid));
    }

    public Output getOutput(String id) {
        return getExecutableManager().getOutput(id);
    }

    protected JobInstance getSingleJobInstance(AbstractExecutable job) {
        Message msg = MsgPicker.getMsg();

        if (job == null) {
            return null;
        }
        if (!(job instanceof CubingJob)) {
            throw new BadRequestException(String.format(msg.getILLEGAL_JOB_TYPE(), job.getId()));
        }

        CubingJob cubeJob = (CubingJob) job;
        final JobInstance result = new JobInstance();
        result.setName(job.getName());
        result.setRelatedCube(CubingExecutableUtil.getCubeName(cubeJob.getParams()));
        result.setRelatedSegment(CubingExecutableUtil.getSegmentId(cubeJob.getParams()));
        result.setLastModified(cubeJob.getLastModified());
        result.setSubmitter(cubeJob.getSubmitter());
        result.setUuid(cubeJob.getId());
        result.setType(CubeBuildTypeEnum.BUILD);
        result.setStatus(JobInfoConverter.parseToJobStatus(job.getStatus()));
        result.setMrWaiting(cubeJob.getMapReduceWaitTime() / 1000);
        result.setDuration(cubeJob.getDuration() / 1000);
        for (int i = 0; i < cubeJob.getTasks().size(); ++i) {
            AbstractExecutable task = cubeJob.getTasks().get(i);
            result.addStep(JobInfoConverter.parseToJobStep(task, i, getExecutableManager().getOutput(task.getId())));
        }
        return result;
    }

    public void resumeJob(JobInstance job) {
        aclEvaluate.checkProjectOperationPermission(job);
        getExecutableManager().resumeJob(job.getId());
    }

    public void rollbackJob(JobInstance job, String stepId) {
        aclEvaluate.checkProjectOperationPermission(job);
        getExecutableManager().rollbackJob(job.getId(), stepId);
    }

    public JobInstance cancelJob(JobInstance job) throws IOException {
        aclEvaluate.checkProjectOperationPermission(job);
        if (null == job.getRelatedCube() || null == getCubeManager().getCube(job.getRelatedCube()) || null == job.getRelatedSegment()) {
            getExecutableManager().discardJob(job.getId());
            return job;
        }
        CubeInstance cubeInstance = getCubeManager().getCube(job.getRelatedCube());
        // might not a cube job
        final String segmentIds = job.getRelatedSegment();
        for (String segmentId : StringUtils.split(segmentIds)) {
            final CubeSegment segment = cubeInstance.getSegmentById(segmentId);
            if (segment != null && (segment.getStatus() == SegmentStatusEnum.NEW || segment.getTSRange().end.v == 0)) {
                // Remove this segments
                CubeUpdate cubeBuilder = new CubeUpdate(cubeInstance);
                cubeBuilder.setToRemoveSegs(segment);
                getCubeManager().updateCube(cubeBuilder);
            }
        }
        getExecutableManager().discardJob(job.getId());

        return job;
    }

    public JobInstance pauseJob(JobInstance job) {
        aclEvaluate.checkProjectOperationPermission(job);
        getExecutableManager().pauseJob(job.getId());
        return job;
    }

    public void dropJob(JobInstance job) throws IOException {
        aclEvaluate.checkProjectOperationPermission(job);
        getExecutableManager().deleteJob(job.getId());
    }

    /**
     * currently only support substring match
     *
     * @return
     */
    public List<JobInstance> searchJobs(final String cubeNameSubstring, final String projectName,
                                        final List<JobStatusEnum> statusList, final Integer limitValue, final Integer offsetValue,
                                        final JobTimeFilterEnum timeFilter) {
        Integer limit = (null == limitValue) ? 30 : limitValue;
        Integer offset = (null == offsetValue) ? 0 : offsetValue;
        List<JobInstance> jobs = searchJobsByCubeName(cubeNameSubstring, projectName, statusList, timeFilter);
        Collections.sort(jobs);

        if (jobs.size() <= offset) {
            return Collections.emptyList();
        }

        if ((jobs.size() - offset) < limit) {
            return jobs.subList(offset, jobs.size());
        }

        return jobs.subList(offset, offset + limit);
    }

    public List<JobInstance> searchJobsByCubeName(final String cubeNameSubstring, final String projectName,
                                                  final List<JobStatusEnum> statusList, final JobTimeFilterEnum timeFilter) {
        return innerSearchCubingJobs(cubeNameSubstring, null, projectName, statusList, timeFilter);
    }

    public List<JobInstance> searchJobsByJobName(final String jobName, final String projectName,
                                                 final List<JobStatusEnum> statusList, final JobTimeFilterEnum timeFilter) {
        return innerSearchCubingJobs(null, jobName, projectName, statusList, timeFilter);
    }

    public List<JobInstance> innerSearchCubingJobs(final String cubeName, final String jobName,
            final String projectName, final List<JobStatusEnum> statusList, final JobTimeFilterEnum timeFilter) {
        if (null == projectName) {
            aclEvaluate.checkIsGlobalAdmin();
        } else {
            aclEvaluate.checkProjectOperationPermission(projectName);
        }
        // prepare time range
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        long timeStartInMillis = getTimeStartInMillis(calendar, timeFilter);
        long timeEndInMillis = Long.MAX_VALUE;
        Set<ExecutableState> states = convertStatusEnumToStates(statusList);
        final Map<String, Output> allOutputs = getExecutableManager().getAllOutputs(timeStartInMillis, timeEndInMillis);

        return Lists
                .newArrayList(
                        FluentIterable
                                .from(innerSearchCubingJobs(cubeName, jobName, states, timeStartInMillis,
                                        timeEndInMillis, allOutputs, false, projectName))
                                .transform(new Function<CubingJob, JobInstance>() {
                                    @Override
                                    public JobInstance apply(CubingJob cubingJob) {
                                        return JobInfoConverter.parseToJobInstance(cubingJob, allOutputs);
                                    }
                                }));
    }

    public List<CubingJob> innerSearchCubingJobs(final String cubeName, final String jobName,
                                                 final Set<ExecutableState> statusList, long timeStartInMillis, long timeEndInMillis,
                                                 final Map<String, Output> allOutputs, final boolean nameExactMatch, final String projectName) {
        List<CubingJob> results = Lists.newArrayList(FluentIterable.from(
                getExecutableManager().getAllAbstractExecutables(timeStartInMillis, timeEndInMillis, CubingJob.class))
                .filter(new Predicate<AbstractExecutable>() {
                    @Override
                    public boolean apply(AbstractExecutable executable) {
                        if (executable instanceof CubingJob) {
                            if (StringUtils.isEmpty(cubeName)) {
                                return true;
                            }
                            String executableCubeName = CubingExecutableUtil.getCubeName(executable.getParams());
                            if (executableCubeName == null)
                                return true;
                            if (nameExactMatch)
                                return executableCubeName.equalsIgnoreCase(cubeName);
                            else
                                return executableCubeName.toLowerCase().contains(cubeName.toLowerCase());
                        } else {
                            return false;
                        }
                    }
                }).transform(new Function<AbstractExecutable, CubingJob>() {
                    @Override
                    public CubingJob apply(AbstractExecutable executable) {
                        return (CubingJob) executable;
                    }
                }).filter(Predicates.and(new Predicate<CubingJob>() {
                    @Override
                    public boolean apply(CubingJob executable) {
                        if (null == projectName || null == getProjectManager().getProject(projectName)) {
                            return true;
                        } else {
                            return projectName.equalsIgnoreCase(executable.getProjectName());
                        }
                    }
                }, new Predicate<CubingJob>() {
                    @Override
                    public boolean apply(CubingJob executable) {
                        try {
                            Output output = allOutputs.get(executable.getId());
                            if (output == null){
                                return false;
                            }
                            
                            ExecutableState state = output.getState();
                            boolean ret = statusList.contains(state);
                            return ret;
                        } catch (Exception e) {
                            throw e;
                        }
                    }
                }, new Predicate<CubingJob>() {
                    @Override
                    public boolean apply(@Nullable CubingJob cubeJob) {
                        if (cubeJob == null) {
                            return false;
                        }

                        if (Strings.isEmpty(jobName)) {
                            return true;
                        }

                        if (nameExactMatch) {
                            return cubeJob.getName().equalsIgnoreCase(jobName);
                        } else {
                            return cubeJob.getName().toLowerCase().contains(jobName.toLowerCase());
                        }
                    }
                })));
        return results;
    }

    public List<CubingJob> listJobsByRealizationName(final String realizationName, final String projectName,
                                                     final Set<ExecutableState> statusList) {
        return innerSearchCubingJobs(realizationName, null, statusList, 0L, Long.MAX_VALUE,
                getExecutableManager().getAllOutputs(), true, projectName);
    }

    public List<CubingJob> listJobsByRealizationName(final String realizationName, final String projectName) {
        return listJobsByRealizationName(realizationName, projectName, EnumSet.allOf(ExecutableState.class));
    }

}

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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.lock.DistributedLock;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.zookeeper.KylinServerDiscovery;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.cube.model.CubeBuildTypeEnum;
import org.apache.kylin.engine.EngineFactory;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.LookupSnapshotBuildJob;
import org.apache.kylin.engine.mr.common.CubeJobLockUtil;
import org.apache.kylin.engine.mr.common.JobInfoConverter;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.engine.spark.job.NSparkBatchOptimizeJobCheckpointBuilder;
import org.apache.kylin.engine.spark.job.NSparkCubingJob;
import org.apache.kylin.engine.spark.metadata.cube.source.SourceFactory;
import org.apache.kylin.job.JobInstance;
import org.apache.kylin.job.JobSearchResult;
import org.apache.kylin.job.Scheduler;
import org.apache.kylin.job.SchedulerFactory;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.constant.JobTimeFilterEnum;
import org.apache.kylin.job.dao.ExecutableOutputPO;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.exception.JobException;
import org.apache.kylin.job.exception.SchedulerException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.CheckpointExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.Output;
import org.apache.kylin.job.lock.zookeeper.ZookeeperJobLock;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentRange.TSRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.source.ISource;
import org.apache.kylin.source.SourceManager;
import org.apache.kylin.source.SourcePartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Sets;

/**
 * @author ysong1
 */

@EnableAspectJAutoProxy(proxyTargetClass = true)
@Component("jobService")
public class JobService extends BasicService implements InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(JobService.class);

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

        // In case of that kylin.server.cluster-name is not set,
        // this method have to be called first to avoid the influence of the change of kylin.metadata.url
        String clusterName = kylinConfig.getClusterName();
        logger.info("starting to initialize an instance in cluster {}", clusterName);

        final Scheduler<AbstractExecutable> scheduler = (Scheduler<AbstractExecutable>) SchedulerFactory
                .scheduler(kylinConfig.getSchedulerType());

        if (kylinConfig.getServerSelfDiscoveryEnabled()) {
            KylinServerDiscovery.getInstance();
        }
        logger.info("Cluster servers: {}", Lists.newArrayList(kylinConfig.getRestServers()));
        
        scheduler.init(new JobEngineConfig(kylinConfig), new ZookeeperJobLock());

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
            throw new BadRequestException(String.format(Locale.ROOT, msg.getILLEGAL_EXECUTABLE_STATE(), status));
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
        case CURRENT_DAY:
            calendar.add(Calendar.DAY_OF_MONTH, 0);
            calendar.set(Calendar.HOUR_OF_DAY, 0);
            calendar.set(Calendar.MINUTE, 0);
            calendar.set(Calendar.SECOND, 0);
            return calendar.getTimeInMillis();
        case ALL:
            return 0;
        default:
            throw new BadRequestException(String.format(Locale.ROOT, msg.getILLEGAL_TIME_FILTER(), timeFilter));
        }
    }

    public JobInstance submitJob(CubeInstance cube, TSRange tsRange, SegmentRange segRange, //
            Map<Integer, Long> sourcePartitionOffsetStart, Map<Integer, Long> sourcePartitionOffsetEnd,
            CubeBuildTypeEnum buildType, boolean force, String submitter, Integer priorityOffset) throws IOException {
        aclEvaluate.checkProjectOperationPermission(cube);
        JobInstance jobInstance = submitJobInternal(cube, tsRange, segRange, sourcePartitionOffsetStart,
                sourcePartitionOffsetEnd, buildType, force, submitter, priorityOffset);

        return jobInstance;
    }

    public JobInstance submitJobInternal(CubeInstance cube, TSRange tsRange, SegmentRange segRange, //
            Map<Integer, Long> sourcePartitionOffsetStart, Map<Integer, Long> sourcePartitionOffsetEnd, //
            CubeBuildTypeEnum buildType, boolean force, String submitter, Integer priorityOffset) throws IOException {
        Message msg = MsgPicker.getMsg();

        if (cube.getStatus() == RealizationStatusEnum.DESCBROKEN) {
            throw new BadRequestException(String.format(Locale.ROOT, msg.getBUILD_BROKEN_CUBE(), cube.getName()));
        }

        checkCubeDescSignature(cube);
        checkAllowBuilding(cube);

        if (buildType == CubeBuildTypeEnum.BUILD || buildType == CubeBuildTypeEnum.REFRESH) {
            checkAllowParallelBuilding(cube);
        }

        DefaultChainedExecutable job;

        CubeSegment newSeg = null;
        try {
            if (buildType == CubeBuildTypeEnum.BUILD) {
                //TODO: Clean the code for org.apache.kylin.source.ISource and org.apache.kylin.engine.spark.metadata.cube.source.ISource
                SourcePartition src;
                if (cube.getSourceType() == ISourceAware.ID_CSV) {
                    ISource source = SourceFactory.getCSVSource();
                    src = new SourcePartition(tsRange, segRange, sourcePartitionOffsetStart, sourcePartitionOffsetEnd);
                    src = source.enrichSourcePartitionBeforeBuild(cube, src);
                } else {
                    ISource source = SourceManager.getSource(cube);
                    src = new SourcePartition(tsRange, segRange, sourcePartitionOffsetStart, sourcePartitionOffsetEnd);
//                    src = source.enrichSourcePartitionBeforeBuild(cube, src);
                }
                newSeg = getCubeManager().appendSegment(cube, src);
                job = EngineFactory.createBatchCubingJob(newSeg, submitter, priorityOffset);

            } else if (buildType == CubeBuildTypeEnum.MERGE) {
                newSeg = getCubeManager().mergeSegments(cube, tsRange, segRange, force);
                job = EngineFactory.createBatchMergeJob(newSeg, submitter);
            } else if (buildType == CubeBuildTypeEnum.REFRESH) {
                newSeg = getCubeManager().refreshSegment(cube, tsRange, segRange);
                job = EngineFactory.createBatchCubingJob(newSeg, submitter, priorityOffset);
            } else {
                throw new BadRequestException(String.format(Locale.ROOT, msg.getINVALID_BUILD_TYPE(), buildType));
            }
            newSeg.setLastBuildJobID(job.getId());
            CubeInstance cubeCopy = cube.latestCopyForWrite();
            CubeUpdate update = new CubeUpdate(cubeCopy);
            update.setToUpdateSegs(newSeg);
            getCubeManager().updateCube(update);

            getExecutableManager().addJob(job);

        } catch (Exception e) {
            if (newSeg != null) {
                logger.error("Job submission might failed for NEW segment {}, will clean the NEW segment from cube",
                        newSeg.getName());
                try {
                    // Remove this segment
                    getCubeManager().updateCubeDropSegments(cube, newSeg);
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

    public Pair<JobInstance, List<JobInstance>> submitOptimizeJob(CubeInstance cube, Set<Long> cuboidsRecommend,
            String submitter) throws IOException, JobException {

        Pair<JobInstance, List<JobInstance>> result = submitOptimizeJobInternal(cube, cuboidsRecommend, submitter);
        return result;
    }

    private Pair<JobInstance, List<JobInstance>> submitOptimizeJobInternal(CubeInstance cube,
            Set<Long> cuboidsRecommend, String submitter) throws IOException {
        Message msg = MsgPicker.getMsg();

        if (cube.getStatus() == RealizationStatusEnum.DESCBROKEN) {
            throw new BadRequestException(String.format(Locale.ROOT, msg.getBUILD_BROKEN_CUBE(), cube.getName()));
        }

        checkCubeDescSignature(cube);
        checkAllowOptimization(cube, cuboidsRecommend);

        CubeSegment[] optimizeSegments = null;
        try {
            /** Add optimize segments */
            optimizeSegments = getCubeManager().optimizeSegments(cube, cuboidsRecommend);
            List<JobInstance> optimizeJobInstances = Lists.newLinkedList();

            /** Add optimize jobs */
            List<AbstractExecutable> optimizeJobList = Lists.newArrayListWithExpectedSize(optimizeSegments.length);
            for (CubeSegment optimizeSegment : optimizeSegments) {
                DefaultChainedExecutable optimizeJob = EngineFactory.createBatchOptimizeJob(optimizeSegment, submitter);
                getExecutableManager().addJob(optimizeJob);

                optimizeJobList.add(optimizeJob);
                optimizeJobInstances.add(getSingleJobInstance(optimizeJob));
            }

            /** Add checkpoint job for batch jobs */
            CheckpointExecutable checkpointJob = new NSparkBatchOptimizeJobCheckpointBuilder(cube, submitter).build();
            checkpointJob.addTaskListForCheck(optimizeJobList);

            getExecutableManager().addJob(checkpointJob);

            return new Pair(getCheckpointJobInstance(checkpointJob), optimizeJobInstances);
        } catch (Exception e) {
            if (optimizeSegments != null) {
                logger.error("Job submission might failed for NEW segments {}, will clean the NEW segments from cube",
                        optimizeSegments);
                try {
                    // Remove this segments
                    getCubeManager().updateCubeDropSegments(cube, optimizeSegments);
                } catch (Exception ee) {
                    // swallow the exception
                    logger.error("Clean New segments failed, ignoring it", e);
                }
            }
            throw e;
        }
    }

    public JobInstance submitRecoverSegmentOptimizeJob(CubeSegment segment, String submitter)
            throws IOException, JobException {
        CubeInstance cubeInstance = segment.getCubeInstance();

        checkCubeDescSignature(cubeInstance);

        String cubeName = cubeInstance.getName();
        List<JobInstance> jobInstanceList = searchJobsByCubeName(cubeName, null,
                Lists.newArrayList(JobStatusEnum.NEW, JobStatusEnum.PENDING, JobStatusEnum.ERROR),
                JobTimeFilterEnum.ALL, JobSearchMode.CHECKPOINT_ONLY);
        if (jobInstanceList.size() > 1) {
            throw new IllegalStateException("Exist more than one CheckpointExecutable for cube " + cubeName);
        } else if (jobInstanceList.size() == 0) {
            throw new IllegalStateException("There's no CheckpointExecutable for cube " + cubeName);
        }
        CheckpointExecutable checkpointExecutable = (CheckpointExecutable) getExecutableManager()
                .getJob(jobInstanceList.get(0).getId());

        AbstractExecutable toBeReplaced = null;
        for (AbstractExecutable taskForCheck : checkpointExecutable.getSubTasksForCheck()) {
            if (taskForCheck instanceof CubingJob) {
                CubingJob subCubingJob = (CubingJob) taskForCheck;
                String segmentName = CubingExecutableUtil.getSegmentName(subCubingJob.getParams());
                if (segmentName != null && segmentName.equals(segment.getName())) {
                    String segmentID = CubingExecutableUtil.getSegmentId(subCubingJob.getParams());
                    CubeSegment beingOptimizedSegment = cubeInstance.getSegmentById(segmentID);
                    if (beingOptimizedSegment != null) { // beingOptimizedSegment exists & should not be recovered
                        throw new IllegalStateException("Segment " + beingOptimizedSegment.getName() + "-"
                                + beingOptimizedSegment.getUuid()
                                + " still exists. Please delete it or discard the related optimize job first!!!");
                    }
                    toBeReplaced = taskForCheck;
                    break;
                }
            }
        }
        if (toBeReplaced == null) {
            throw new IllegalStateException("There's no CubingJob for segment " + segment.getName()
                    + " in CheckpointExecutable " + checkpointExecutable.getName());
        }

        /** Add CubingJob for the related segment **/
        CubeSegment optimizeSegment = getCubeManager().appendSegment(cubeInstance, segment.getTSRange());

        DefaultChainedExecutable optimizeJob = EngineFactory.createBatchOptimizeJob(optimizeSegment, submitter);

        getExecutableManager().addJob(optimizeJob);

        JobInstance optimizeJobInstance = getSingleJobInstance(optimizeJob);

        /** Update the checkpoint job */
        checkpointExecutable.getSubTasksForCheck().set(checkpointExecutable.getSubTasksForCheck().indexOf(toBeReplaced),
                optimizeJob);

        getExecutableManager().updateCheckpointJob(checkpointExecutable.getId(),
                checkpointExecutable.getSubTasksForCheck());

        return optimizeJobInstance;
    }

//    public JobInstance submitLookupSnapshotJob(CubeInstance cube, String lookupTable, List<String> segmentIDs,
//            String submitter) throws IOException {
//        Message msg = MsgPicker.getMsg();
//        TableDesc tableDesc = getTableManager().getTableDesc(lookupTable, cube.getProject());
//        if (tableDesc.isView()) {
//            throw new BadRequestException(
//                    String.format(Locale.ROOT, msg.getREBUILD_SNAPSHOT_OF_VIEW(), tableDesc.getName()));
//        }
//        LookupSnapshotBuildJob job = new LookupSnapshotJobBuilder(cube, lookupTable, segmentIDs, submitter).build();
//        getExecutableManager().addJob(job);
//
//        JobInstance jobInstance = getLookupSnapshotBuildJobInstance(job);
//        return jobInstance;
//    }

    private void checkCubeDescSignature(CubeInstance cube) {
        Message msg = MsgPicker.getMsg();

        if (!cube.getDescriptor().checkSignature())
            throw new BadRequestException(
                    String.format(Locale.ROOT, msg.getINCONSISTENT_CUBE_DESC_SIGNATURE(), cube.getDescriptor()));
    }

    private void checkAllowBuilding(CubeInstance cube) {
        if (cube.getConfig().isCubePlannerEnabled()) {
            Segments<CubeSegment> readyPendingSegments = cube.getSegments(SegmentStatusEnum.READY_PENDING);
            if (readyPendingSegments.size() > 0) {
                throw new BadRequestException("The cube " + cube.getName() + " has READY_PENDING segments "
                        + readyPendingSegments + ". It's not allowed for building");
            }
        }
    }

    private void checkAllowParallelBuilding(CubeInstance cube) {
        if (cube.getConfig().isCubePlannerEnabled()) {
            if (cube.getCuboids() == null) {
                Segments<CubeSegment> cubeSegments = cube.getSegments();
                if (cubeSegments.size() > 0 && cubeSegments.getSegments(SegmentStatusEnum.READY).size() <= 0) {
                    throw new BadRequestException("The cube " + cube.getName() + " has segments " + cubeSegments
                            + ", but none of them is READY. It's not allowed for parallel building");
                }
            }
        }
    }

    private void checkAllowOptimization(CubeInstance cube, Set<Long> cuboidsRecommend) {
        Segments<CubeSegment> buildingSegments = cube.getBuildingSegments();
        if (buildingSegments.size() > 0) {
            throw new BadRequestException("The cube " + cube.getName() + " has building segments " + buildingSegments
                    + ". It's not allowed for optimization");
        }
        long baseCuboid = cube.getCuboidScheduler().getBaseCuboidId();
        if (!cuboidsRecommend.contains(baseCuboid)) {
            throw new BadRequestException("The recommend cuboids should contain the base cuboid " + baseCuboid);
        }
        Set<Long> currentCuboidSet = cube.getCuboidScheduler().getAllCuboidIds();
        if (currentCuboidSet.equals(cuboidsRecommend)) {
            throw new BadRequestException(
                    "The recommend cuboids are the same as the current cuboids. It's no need to do optimization.");
        }
    }

    /**
     * update the spark job yarnAppUrl.
     */
    public void updateSparkJobInfo(String project, String taskId, String yarnAppUrl) {
        ExecutableManager executableManager = getExecutableManager();
        Map<String, String> extraInfo = Maps.newHashMap();
        extraInfo.put(ExecutableConstants.YARN_APP_URL, yarnAppUrl);

        executableManager.updateJobOutput(project, taskId, null, extraInfo, null, null);
    }

    public JobInstance getJobInstance(String uuid) {
        AbstractExecutable job = getExecutableManager().getJob(uuid);
        if (job instanceof CheckpointExecutable) {
            return getCheckpointJobInstance(job);
        } else {
            return getSingleJobInstance(job);
        }
    }

    public Output getOutput(String id) {
        return getExecutableManager().getOutput(id);
    }

    public String getJobStepOutput(String jobId, String stepId) {
        ExecutableManager executableManager = getExecutableManager();
        if (executableManager.getOutputFromHDFSByJobId(jobId, stepId) == null) {
            return executableManager.getOutput(stepId).getVerboseMsg();
        }
        return executableManager.getOutputFromHDFSByJobId(jobId, stepId).getVerboseMsg();
    }

    public String getAllJobStepOutput(String jobId, String stepId) {
        ExecutableManager executableManager = getExecutableManager();
        if (executableManager.getOutputFromHDFSByJobId(jobId, stepId, Integer.MAX_VALUE) == null) {
            return executableManager.getOutput(stepId).getVerboseMsg();
        }
        return executableManager.getOutputFromHDFSByJobId(jobId, stepId, Integer.MAX_VALUE).getVerboseMsg();
    }

    protected JobInstance getSingleJobInstance(AbstractExecutable job) {
        Message msg = MsgPicker.getMsg();

        if (job == null) {
            return null;
        }

        DefaultChainedExecutable cubeJob;

        if (job instanceof CubingJob) {
            cubeJob = (CubingJob) job;
        } else {
            throw new BadRequestException(String.format(Locale.ROOT, msg.getILLEGAL_JOB_TYPE(), job.getId()));
        }

        CubeInstance cube = CubeManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getCube(CubingExecutableUtil.getCubeName(cubeJob.getParams()));
        Output output = cubeJob.getOutput();
        final JobInstance result = new JobInstance();
        result.setName(job.getName());
        result.setProjectName(cubeJob.getProjectName());
        if (cube != null) {
            result.setRelatedCube(cube.getName());
            result.setDisplayCubeName(cube.getDisplayName());
        } else {
            String cubeName = CubingExecutableUtil.getCubeName(cubeJob.getParams());
            result.setRelatedCube(cubeName);
            result.setDisplayCubeName(cubeName);
        }
        result.setRelatedSegment(CubingExecutableUtil.getSegmentId(cubeJob.getParams()));
        result.setRelatedSegmentName(CubingExecutableUtil.getSegmentName(cubeJob.getParams()));
        result.setLastModified(cubeJob.getLastModified());
        result.setSubmitter(cubeJob.getSubmitter());
        result.setUuid(cubeJob.getId());
        result.setExecStartTime(cubeJob.getStartTime());
        result.setExecEndTime(cubeJob.getEndTime());
        result.setExecInterruptTime(cubeJob.getInterruptTime());
        result.setType(CubeBuildTypeEnum.BUILD);
        result.setStatus(JobInfoConverter.parseToJobStatus(job.getStatus()));
        result.setMrWaiting(cubeJob.getMapReduceWaitTime() / 1000);
        result.setBuildInstance(AbstractExecutable.getBuildInstance(output));
        result.setDuration(cubeJob.getDuration() / 1000);
        for (int i = 0; i < cubeJob.getTasks().size(); ++i) {
            AbstractExecutable task = cubeJob.getTasks().get(i);
            result.addStep(JobInfoConverter.parseToJobStep(task, i, getExecutableManager().getOutput(task.getId())));
        }
        return result;
    }

    protected JobInstance getLookupSnapshotBuildJobInstance(LookupSnapshotBuildJob job) {
        if (job == null) {
            return null;
        }
        Output output = job.getOutput();
        final JobInstance result = new JobInstance();
        result.setName(job.getName());
        result.setProjectName(job.getProjectName());
        result.setRelatedCube(CubingExecutableUtil.getCubeName(job.getParams()));
        result.setRelatedSegment(CubingExecutableUtil.getSegmentId(job.getParams()));
        result.setRelatedSegmentName(CubingExecutableUtil.getSegmentName(job.getParams()));
        result.setLastModified(job.getLastModified());
        result.setSubmitter(job.getSubmitter());
        result.setUuid(job.getId());
        result.setExecStartTime(job.getStartTime());
        result.setExecEndTime(job.getEndTime());
        result.setExecInterruptTime(job.getInterruptTime());
        result.setType(CubeBuildTypeEnum.BUILD);
        result.setStatus(JobInfoConverter.parseToJobStatus(job.getStatus()));
        result.setBuildInstance(AbstractExecutable.getBuildInstance(output));
        result.setDuration(job.getDuration() / 1000);
        for (int i = 0; i < job.getTasks().size(); ++i) {
            AbstractExecutable task = job.getTasks().get(i);
            result.addStep(JobInfoConverter.parseToJobStep(task, i, getExecutableManager().getOutput(task.getId())));
        }
        return result;
    }

    protected JobInstance getCheckpointJobInstance(AbstractExecutable job) {
        Message msg = MsgPicker.getMsg();

        if (job == null) {
            return null;
        }
        if (!(job instanceof CheckpointExecutable)) {
            throw new BadRequestException(String.format(Locale.ROOT, msg.getILLEGAL_JOB_TYPE(), job.getId()));
        }

        CheckpointExecutable checkpointExecutable = (CheckpointExecutable) job;
        Output output = checkpointExecutable.getOutput();
        final JobInstance result = new JobInstance();
        result.setName(job.getName());
        result.setProjectName(checkpointExecutable.getProjectName());
        result.setRelatedCube(CubingExecutableUtil.getCubeName(job.getParams()));
        result.setDisplayCubeName(CubingExecutableUtil.getCubeName(job.getParams()));
        result.setLastModified(job.getLastModified());
        result.setSubmitter(job.getSubmitter());
        result.setUuid(job.getId());
        result.setExecStartTime(job.getStartTime());
        result.setExecEndTime(job.getEndTime());
        result.setExecInterruptTime(job.getInterruptTime());
        result.setType(CubeBuildTypeEnum.CHECKPOINT);
        result.setStatus(JobInfoConverter.parseToJobStatus(job.getStatus()));
        result.setBuildInstance(AbstractExecutable.getBuildInstance(output));
        result.setDuration(job.getDuration() / 1000);
        for (int i = 0; i < checkpointExecutable.getTasks().size(); ++i) {
            AbstractExecutable task = checkpointExecutable.getTasks().get(i);
            result.addStep(JobInfoConverter.parseToJobStep(task, i, getExecutableManager().getOutput(task.getId())));
        }
        return result;
    }

    public void resumeJob(JobInstance job) {
        aclEvaluate.checkProjectOperationPermission(job);
        getExecutableManager().resumeJob(job.getId());
    }

//    public void resubmitJob(JobInstance job) throws IOException {
//        aclEvaluate.checkProjectOperationPermission(job);
//
////        Coordinator coordinator = Coordinator.getInstance();
//        CubeManager cubeManager = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
//        String cubeName = job.getRelatedCube();
//        CubeInstance cubeInstance = cubeManager.getCube(cubeName);
//
//        String segmentName = job.getRelatedSegmentName();
//        try {
//            Pair<Long, Long> segmentRange = CubeSegment.parseSegmentName(segmentName);
//            logger.info("submit streaming segment build, cube:{} segment:{}", cubeName, segmentName);
//            CubeSegment newSeg = coordinator.getCubeManager().appendSegment(cubeInstance,
//                    new SegmentRange.TSRange(segmentRange.getFirst(), segmentRange.getSecond()));
//
//            DefaultChainedExecutable executable = new StreamingCubingEngine().createStreamingCubingJob(newSeg, aclEvaluate.getCurrentUserName());
//            coordinator.getExecutableManager().addJob(executable);
//            CubingJob cubingJob = (CubingJob) executable;
//            newSeg.setLastBuildJobID(cubingJob.getId());
//
//            SegmentBuildState.BuildState state = new SegmentBuildState.BuildState();
//            state.setBuildStartTime(System.currentTimeMillis());
//            state.setState(SegmentBuildState.BuildState.State.BUILDING);
//            state.setJobId(cubingJob.getId());
//            coordinator.getStreamMetadataStore().updateSegmentBuildState(cubeName, segmentName, state);
//        } catch (Exception e) {
//            logger.error("streaming job submit fail, cubeName:" + cubeName + " segment:" + segmentName, e);
//            throw e;
//        }
//    }

    public void rollbackJob(JobInstance job, String stepId) {
        aclEvaluate.checkProjectOperationPermission(job);
        getExecutableManager().rollbackJob(job.getId(), stepId);
    }

    public void cancelJob(JobInstance job) throws IOException {
        aclEvaluate.checkProjectOperationPermission(job);
        if (null == job.getRelatedCube() || null == getCubeManager().getCube(job.getRelatedCube())
                || null == job.getRelatedSegment()) {
            getExecutableManager().discardJob(job.getId());
            return;
        }

        logger.info("Cancel job [" + job.getId() + "] trigger by "
                + SecurityContextHolder.getContext().getAuthentication().getName());
        if (job.getStatus() == JobStatusEnum.FINISHED) {
            throw new IllegalStateException(
                    "The job " + job.getId() + " has already been finished and cannot be discarded.");
        }

        AbstractExecutable executable = getExecutableManager().getJob(job.getId());

        if (job.getStatus() != JobStatusEnum.DISCARDED) {
            if (executable instanceof CubingJob) {
                String segmentName = job.getRelatedSegmentName();
                CubeSegment segment = getCubeManager().getCube(job.getRelatedCube()).getSegment(segmentName, SegmentStatusEnum.NEW);
                String segmentIdentifier = segment.getStorageLocationIdentifier();
                cancelCubingJobInner((CubingJob) executable);
                //Clean up job tmp and segment storage from hdfs after job be discarded
                if (executable instanceof NSparkCubingJob) {
                    ((NSparkCubingJob) executable).cleanupAfterJobDiscard(segmentName, segmentIdentifier);
                }
                //release global mr hive dict lock if exists
                if (executable.getStatus().isFinalState()) {
                    try {
                        DistributedLock lock = KylinConfig.getInstanceFromEnv().getDistributedLockFactory().lockForCurrentThread();
                        if(lock.isLocked(CubeJobLockUtil.getLockPath(executable.getCubeName(), job.getId()))){//release cube job dict lock if exists
                            lock.purgeLocks(CubeJobLockUtil.getLockPath(executable.getCubeName(), null));
                            logger.info("{} unlock cube job dict lock path({}) success", job.getId(), CubeJobLockUtil.getLockPath(executable.getCubeName(), null));

                            if (lock.isLocked(CubeJobLockUtil.getEphemeralLockPath(executable.getCubeName()))) {//release cube job Ephemeral lock if exists
                                lock.purgeLocks(CubeJobLockUtil.getEphemeralLockPath(executable.getCubeName()));
                                logger.info("{} unlock cube job ephemeral lock path({}) success", job.getId(), CubeJobLockUtil.getEphemeralLockPath(executable.getCubeName()));
                            }
                        }
                    }catch (Exception e){
                        logger.error("get some error when release cube {} job {} job id {} " , executable.getCubeName(), job.getName(), job.getId());
                    }
                }
            } else if (executable instanceof CheckpointExecutable) {
                cancelCheckpointJobInner((CheckpointExecutable) executable);
            } else {
                getExecutableManager().discardJob(executable.getId());
            }
        }
    }

    private void cancelCubingJobInner(CubingJob cubingJob) throws IOException {
        CubeInstance cubeInstance = getCubeManager().getCube(CubingExecutableUtil.getCubeName(cubingJob.getParams()));
        // might not a cube job
        final String segmentIds = CubingExecutableUtil.getSegmentId(cubingJob.getParams());
        if (!StringUtils.isEmpty(segmentIds)) {
            for (String segmentId : StringUtils.split(segmentIds)) {
                final CubeSegment segment = cubeInstance.getSegmentById(segmentId);
                if (segment != null
                        && (segment.getStatus() == SegmentStatusEnum.NEW || segment.getTSRange().end.v == 0)) {
                    // Remove this segment
                    getCubeManager().updateCubeDropSegments(cubeInstance, segment);
                }
            }
        }
        getExecutableManager().discardJob(cubingJob.getId());
    }

    private void cancelCheckpointJobInner(CheckpointExecutable checkpointExecutable) throws IOException {
        List<String> segmentIdList = Lists.newLinkedList();
        List<String> jobIdList = Lists.newLinkedList();
        jobIdList.add(checkpointExecutable.getId());
        setRelatedIdList(checkpointExecutable, segmentIdList, jobIdList);

        CubeInstance cubeInstance = getCubeManager()
                .getCube(CubingExecutableUtil.getCubeName(checkpointExecutable.getParams()));
        if (!segmentIdList.isEmpty()) {
            List<CubeSegment> toRemoveSegments = Lists.newLinkedList();
            for (String segmentId : segmentIdList) {
                final CubeSegment segment = cubeInstance.getSegmentById(segmentId);
                if (segment != null && segment.getStatus() != SegmentStatusEnum.READY) {
                    toRemoveSegments.add(segment);
                }
            }

            getCubeManager().dropOptmizingSegments(cubeInstance, toRemoveSegments.toArray(new CubeSegment[] {}));
        }

        for (String jobId : jobIdList) {
            getExecutableManager().discardJob(jobId);
        }
    }

    private void setRelatedIdList(CheckpointExecutable checkpointExecutable, List<String> segmentIdList,
            List<String> jobIdList) {
        for (AbstractExecutable taskForCheck : checkpointExecutable.getSubTasksForCheck()) {
            jobIdList.add(taskForCheck.getId());
            if (taskForCheck instanceof CubingJob) {
                segmentIdList.addAll(Lists
                        .newArrayList(StringUtils.split(CubingExecutableUtil.getSegmentId(taskForCheck.getParams()))));
            } else if (taskForCheck instanceof CheckpointExecutable) {
                setRelatedIdList((CheckpointExecutable) taskForCheck, segmentIdList, jobIdList);
            }
        }
    }

    public void pauseJob(JobInstance job) {
        aclEvaluate.checkProjectOperationPermission(job);
        logger.info("Pause job [" + job.getId() + "] trigger by "
                + SecurityContextHolder.getContext().getAuthentication().getName());
        if (job.getStatus().isComplete()) {
            throw new IllegalStateException(
                    "The job " + job.getId() + " has already been finished and cannot be stopped.");
        }
        getExecutableManager().pauseJob(job.getId());
    }

    public void dropJob(JobInstance job) {
        aclEvaluate.checkProjectOperationPermission(job);
        if (job.getRelatedCube() != null && getCubeManager().getCube(job.getRelatedCube()) != null) {
            if (job.getStatus() != JobStatusEnum.FINISHED && job.getStatus() != JobStatusEnum.DISCARDED) {
                throw new BadRequestException(
                        "Only FINISHED and DISCARDED job can be deleted. Please wait for the job finishing or discard the job!!!");
            }
        }
        getExecutableManager().deleteJob(job.getId());
        logger.info("Delete job [" + job.getId() + "] trigger by + "
                + SecurityContextHolder.getContext().getAuthentication().getName());
    }

    //******************************** Job search apis for Job controller V1 *******************************************
    /**
    * currently only support substring match
    *
    * @return
    */
    public List<JobInstance> searchJobs(final String cubeNameSubstring, final String projectName,
            final List<JobStatusEnum> statusList, final Integer limitValue, final Integer offsetValue,
            final JobTimeFilterEnum timeFilter, JobSearchMode jobSearchMode) {
        Integer limit = (null == limitValue) ? 30 : limitValue;
        Integer offset = (null == offsetValue) ? 0 : offsetValue;
        List<JobInstance> jobs = searchJobsByCubeName(cubeNameSubstring, projectName, statusList, timeFilter,
                jobSearchMode);

        Collections.sort(jobs);

        if (jobs.size() <= offset) {
            return Collections.emptyList();
        }

        if ((jobs.size() - offset) < limit) {
            return jobs.subList(offset, jobs.size());
        }

        return jobs.subList(offset, offset + limit);
    }

    /**
     * it loads all metadata of "execute" and "execute_output",
     * and parses all job instances within the scope of the given filters
     * @param cubeNameSubstring the full name or keyword of the cube
     * @param projectName the project name
     * @param statusList set of status of the job which will to filter
     * @param timeFilter the full name or keyword of the job
     * @param jobSearchMode the job search mode
     * @return List of search results searched by the method
     */
    public List<JobInstance> searchJobsByCubeName(final String cubeNameSubstring, final String projectName,
        final List<JobStatusEnum> statusList, final JobTimeFilterEnum timeFilter,
        final JobSearchMode jobSearchMode) {
        if (StringUtils.isEmpty(projectName)) {
            aclEvaluate.checkIsGlobalAdmin();
        } else {
            aclEvaluate.checkProjectOperationPermission(projectName);
        }
        // prepare time range
        Calendar calendar = Calendar.getInstance(TimeZone.getDefault(), Locale.ROOT);
        calendar.setTime(new Date());
        long timeStartInMillis = getTimeStartInMillis(calendar, timeFilter);
        long timeEndInMillis = Long.MAX_VALUE;
        Set<ExecutableState> states = convertStatusEnumToStates(statusList);
        final Map<String, Output> allOutputs = getExecutableManager()
            .getAllOutputs(timeStartInMillis, timeEndInMillis);
        final List<AbstractExecutable> allExecutables = getExecutableManager()
            .getAllExecutables(timeStartInMillis, timeEndInMillis);
        return innerSearchJobs(jobSearchMode, projectName, cubeNameSubstring, null, states,
            allOutputs, allExecutables, false);
    }

    /**
     * returns list of job within the scope of the given filters
     *
     * @param jobSearchMode the job search mode
     * @param projectName the project name
     * @param cubeName the full name or keyword of the cube
     * @param jobName the full name or keyword of the job
     * @param statusList set of status of the job which will to filter
     * @param allOutputs map of executable output data with type DefaultOutput parsed from ExecutableOutputPO
     * @param allExecutables list of all executable
     * @param nameExactMatch if true, match full name
     */
    public List<JobInstance> innerSearchJobs(JobSearchMode jobSearchMode, final String projectName,
        final String cubeName, final String jobName, final Set<ExecutableState> statusList,
        final Map<String, Output> allOutputs,
        final List<AbstractExecutable> allExecutables,
        final boolean nameExactMatch) {
        return allExecutables.stream()
            .filter(executable -> checkJobType(executable, jobSearchMode))
            .filter(executable -> checkProject(executable, projectName))
            .filter(executable -> checkCubeName(executable, cubeName, nameExactMatch))
            .filter(executable -> checkJobStatus(executable, statusList, allOutputs))
            .filter(executable -> checkJobName(executable, jobName, nameExactMatch))
            .map(job -> JobInfoConverter.parseToJobInstanceQuietly(job, allOutputs))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    }

    public List<CubingJob> innerSearchCubingJobs(final String cubeName, final String jobName,
        final Set<ExecutableState> statusList, long timeStartInMillis, long timeEndInMillis,
        final Map<String, Output> allOutputs, final boolean nameExactMatch, final String projectName) {
        return getExecutableManager()
            .getAllExecutables(timeStartInMillis, timeEndInMillis).stream()
            .filter(executable -> checkJobType(executable, JobSearchMode.CUBING_ONLY))
            .filter(executable -> checkProject(executable, projectName))
            .filter(executable -> checkCubeName(executable, cubeName, nameExactMatch))
            .filter(executable -> checkJobStatus(executable, statusList, allOutputs))
            .filter(executable -> checkJobName(executable, jobName, nameExactMatch))
            .map(job -> (CubingJob)job)
            .collect(Collectors.toList());
    }

    //****************************** Job search apis for Job controller V1 end *****************************************

    //******************************** Job search apis for Job controller V2 *******************************************
    public List<JobInstance> searchJobsV2(final String cubeNameSubstring, final String projectName,
            final List<JobStatusEnum> statusList, final Integer limitValue, final Integer offsetValue,
            final JobTimeFilterEnum timeFilter, JobSearchMode jobSearchMode) {
        Integer limit = (null == limitValue) ? 30 : limitValue;
        Integer offset = (null == offsetValue) ? 0 : offsetValue;
        List<JobSearchResult> jobSearchResultList = searchJobsByCubeNameV2(cubeNameSubstring, projectName, statusList,
                timeFilter, jobSearchMode);

        Collections.sort(jobSearchResultList);

        if (jobSearchResultList.size() <= offset) {
            return Collections.emptyList();
        }

        // Fetch instance data of jobs for the searched job results
        List<JobSearchResult> subJobSearchResultList;
        if ((jobSearchResultList.size() - offset) < limit) {
            subJobSearchResultList = jobSearchResultList.subList(offset, jobSearchResultList.size());
        } else {
            subJobSearchResultList = jobSearchResultList.subList(offset, offset + limit);
        }

        List<JobInstance> jobInstanceList = new ArrayList<>();
        for (JobSearchResult result : subJobSearchResultList) {
            JobInstance jobInstance = getJobInstance(result.getId());
            jobInstanceList.add(jobInstance);
        }

        return jobInstanceList;
    }

    public Map<JobStatusEnum, Integer> searchJobsOverview(final String cubeNameSubstring, final String projectName,
            final List<JobStatusEnum> statusList, final JobTimeFilterEnum timeFilter, JobSearchMode jobSearchMode) {
        // TODO: can be optimized here
        List<JobSearchResult> jobSearchResultList = searchJobsByCubeNameV2(cubeNameSubstring, projectName, statusList,
                timeFilter, jobSearchMode);
        Map<JobStatusEnum, Integer> jobOverview = new HashMap<>();
        if (statusList == null || statusList.isEmpty()) {
            for (JobStatusEnum status : JobStatusEnum.values()) {
                jobOverview.put(status, 0);
            }
        } else {
            for (JobStatusEnum status : statusList) {
                jobOverview.put(status, 0);
            }
        }
        for (JobSearchResult result : jobSearchResultList) {
            jobOverview.put(result.getJobStatus(), jobOverview.get(result.getJobStatus()) + 1);
        }
        return jobOverview;
    }

    /**
     * it loads all cache for digest metadata of "execute" and "execute_output",
     * and returns the search results within the scope of the given filters
     *
     * @param cubeNameSubstring the full name or keyword of the cube
     * @param projectName the project name
     * @param statusList set of status of the job which will to filter
     * @param timeFilter the full name or keyword of the job
     * @param jobSearchMode the job search mode
     * @return List of search results searched by the method
     */
    public List<JobSearchResult> searchJobsByCubeNameV2(final String cubeNameSubstring, final String projectName,
            final List<JobStatusEnum> statusList, final JobTimeFilterEnum timeFilter,
            final JobSearchMode jobSearchMode) {
        if (StringUtils.isEmpty(projectName)) {
            aclEvaluate.checkIsGlobalAdmin();
        } else {
            aclEvaluate.checkProjectOperationPermission(projectName);
        }
        // prepare time range
        Calendar calendar = Calendar.getInstance(TimeZone.getDefault(), Locale.ROOT);
        calendar.setTime(new Date());
        long timeStartInMillis = getTimeStartInMillis(calendar, timeFilter);
        long timeEndInMillis = Long.MAX_VALUE;
        Set<ExecutableState> states = convertStatusEnumToStates(statusList);
        final Map<String, ExecutableOutputPO> allOutputDigests = getExecutableManager()
            .getAllOutputDigests(timeStartInMillis, timeEndInMillis);
        final List<AbstractExecutable> allExecutables = getExecutableManager()
            .getAllExecutableDigests(timeStartInMillis, timeEndInMillis);
        return innerSearchJobsV2(jobSearchMode, projectName, cubeNameSubstring, null, states,
            allOutputDigests, allExecutables, false);

    }

    /**
     * Called by searchJobsByCubeNameV2, it loads all cache of digest metadata of "execute"
     * and returns list of cubing job within the scope of the given filters
     *
     * @param jobSearchMode the job search mode
     * @param projectName the project name
     * @param cubeName the full name or keyword of the cube
     * @param jobName the full name or keyword of the job
     * @param statusList set of status of the job which will to filter
     * @param allExecutableOutputPO map of executable output data with type ExecutableOutputPO
     * @param allExecutables list of all executables
     * @param nameExactMatch if true, match full name
     *
     */
    public List<JobSearchResult> innerSearchJobsV2(JobSearchMode jobSearchMode, final String projectName,
        final String cubeName, final String jobName, final Set<ExecutableState> statusList,
        final Map<String, ExecutableOutputPO> allExecutableOutputPO,
        final List<AbstractExecutable> allExecutables,
        final boolean nameExactMatch) {
        return allExecutables.stream()
            .filter(executable -> checkJobType(executable, jobSearchMode))
            .filter(executable -> checkProject(executable, projectName))
            .filter(executable -> checkCubeName(executable, cubeName, nameExactMatch))
            .filter(executable -> checkJobStatusV2(executable, statusList, allExecutableOutputPO))
            .filter(executable -> checkJobName(executable, jobName, nameExactMatch))
            .map(job -> JobInfoConverter.parseToJobSearchResult((DefaultChainedExecutable)job, allExecutableOutputPO))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    }

    //****************************** Job search apis for Job controller V2 end *****************************************

    private boolean checkJobType(final AbstractExecutable executable, final JobSearchMode jobSearchMode) {
        switch (jobSearchMode) {
        case CHECKPOINT_ONLY:
            return executable instanceof CheckpointExecutable;
        case ALL:
            return executable instanceof CheckpointExecutable || executable instanceof CubingJob;
        case CUBING_ONLY:
        default:
            return executable instanceof CubingJob;
        }
    }

    private boolean checkProject(final AbstractExecutable executable, final String projectName) {
        if (null == projectName || null == getProjectManager().getProject(projectName)) {
            return true;
        } else {
            return projectName.equalsIgnoreCase(executable.getProjectName());
        }
    }

    private boolean checkCubeName(final AbstractExecutable executable, final String cubeName,
        final boolean nameExactMatch) {
        if (StringUtils.isEmpty(cubeName)) {
            return true;
        }
        String executableCubeName = CubingExecutableUtil.getCubeName(executable.getParams());
        if (executableCubeName == null)
            return false;
        if (nameExactMatch)
            return executableCubeName.equalsIgnoreCase(cubeName);
        else
            return executableCubeName.toLowerCase(Locale.ROOT)
                .contains(cubeName.toLowerCase(Locale.ROOT));
    }

    private boolean checkJobStatus(final AbstractExecutable executable,
        final Set<ExecutableState> statusList,
        final Map<String, Output> allOutputs) {
        try {
            Output output = allOutputs.get(executable.getId());
            if (output == null) {
                return false;
            }
            ExecutableState state = output.getState();
            return statusList.contains(state);
        } catch (Exception e) {
            throw e;
        }
    }

    private boolean checkJobStatusV2(final AbstractExecutable executable,
        final Set<ExecutableState> statusList,
        final Map<String, ExecutableOutputPO> allExecutableOutputPO) {
        try {
            ExecutableOutputPO executableOutputPO = allExecutableOutputPO
                .get(executable.getId());
            ExecutableState state = ExecutableState.valueOf(executableOutputPO.getStatus());
            return statusList.contains(state);
        } catch (Exception e) {
            throw e;
        }
    }
    private boolean checkJobName(final AbstractExecutable executable, final String jobName,
        final boolean nameExactMatch) {
        if (executable == null) {
            return false;
        }
        if (StringUtils.isEmpty(jobName)) {
            return true;
        }
        if (nameExactMatch) {
            return executable.getName().equalsIgnoreCase(jobName);
        } else {
            return executable.getName().toLowerCase(Locale.ROOT)
                .contains(jobName.toLowerCase(Locale.ROOT));
        }
    }

    public List<CubingJob> listJobsByRealizationName(final String realizationName, final String projectName,
            final Set<ExecutableState> statusList) {
        return innerSearchCubingJobs(realizationName, null, statusList, 0L, Long.MAX_VALUE,
                getExecutableManager().getAllOutputs(), true, projectName);
    }

    public List<CubingJob> listJobsByRealizationName(final String realizationName, final String projectName) {
        return listJobsByRealizationName(realizationName, projectName, EnumSet.allOf(ExecutableState.class));
    }

    public enum JobSearchMode {
        CUBING_ONLY, CHECKPOINT_ONLY, ALL
    }
}

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

import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_ACTION_ILLEGAL;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_NOT_EXIST;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_RESTART_CHECK_SEGMENT_STATUS;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_STATUS_ILLEGAL;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_UPDATE_STATUS_FAILED;
import static org.apache.kylin.query.util.AsyncQueryUtil.ASYNC_QUERY_JOB_ID_PRE;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.cluster.ClusterManagerFactory;
import org.apache.kylin.cluster.IClusterManager;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.constant.LogConstant;
import org.apache.kylin.common.exception.ErrorCode;
import org.apache.kylin.common.exception.ExceptionReason;
import org.apache.kylin.common.exception.ExceptionResolve;
import org.apache.kylin.common.exception.JobErrorCode;
import org.apache.kylin.common.exception.JobExceptionReason;
import org.apache.kylin.common.exception.JobExceptionResolve;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.logging.SetLogCategory;
import org.apache.kylin.common.metrics.MetricsCategory;
import org.apache.kylin.common.metrics.MetricsGroup;
import org.apache.kylin.common.metrics.MetricsName;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.metadata.Epoch;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.persistence.transaction.UnitOfWorkContext;
import org.apache.kylin.common.scheduler.EventBusFactory;
import org.apache.kylin.common.scheduler.JobDiscardNotifier;
import org.apache.kylin.common.scheduler.JobReadyNotifier;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.StringHelper;
import org.apache.kylin.job.common.JobUtil;
import org.apache.kylin.job.common.ShellExecutable;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.constant.JobActionEnum;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.constant.JobTimeFilterEnum;
import org.apache.kylin.job.dao.ExecutableOutputPO;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.dao.JobStatistics;
import org.apache.kylin.job.dao.JobStatisticsManager;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ChainedExecutable;
import org.apache.kylin.job.execution.ChainedStageExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobSchedulerModeEnum;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.execution.Output;
import org.apache.kylin.job.execution.StageBase;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.epoch.EpochManager;
import org.apache.kylin.metadata.model.FusionModel;
import org.apache.kylin.metadata.model.FusionModelManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.SegmentSecondStorageStatusEnum;
import org.apache.kylin.metadata.model.SegmentStatusEnumToDisplay;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.ISmartApplicationListenerForSystem;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.JobFilter;
import org.apache.kylin.rest.request.JobUpdateRequest;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.ExecutableResponse;
import org.apache.kylin.rest.response.ExecutableStepResponse;
import org.apache.kylin.rest.response.JobStatisticsResponse;
import org.apache.kylin.rest.response.NDataSegmentResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.BuildAsyncProfileHelper;
import org.apache.kylin.rest.util.PagingUtil;
import org.apache.kylin.rest.util.SparkHistoryUIUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.http.HttpHeaders;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;

import io.kyligence.kap.secondstorage.SecondStorageUtil;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.val;
import lombok.var;

@Component("jobService")
public class JobService extends BasicService implements JobSupporter, ISmartApplicationListenerForSystem {

    @Autowired
    private ProjectService projectService;

    private AclEvaluate aclEvaluate;

    @Autowired
    private ModelService modelService;

    private static final Logger logger = LoggerFactory.getLogger(LogConstant.BUILD_CATEGORY);

    private static final Map<String, String> jobTypeMap = Maps.newHashMap();
    private static final String LAST_MODIFIED = "last_modified";
    private static final String CREATE_TIME = "create_time";
    private static final String DURATION = "duration";
    private static final String TOTAL_DURATION = "total_duration";
    private static final String TARGET_SUBJECT = "target_subject";
    private static final String JOB_NAME = "job_name";
    private static final String JOB_STATUS = "job_status";
    private static final String PROJECT = "project";

    public static final String EXCEPTION_CODE_PATH = "exception_to_code.json";
    public static final String EXCEPTION_CODE_DEFAULT = "KE-030001000";

    public static final String JOB_STEP_PREFIX = "job_step_";
    public static final String YARN_APP_SEPARATOR = "_";
    public static final String BUILD_JOB_PROFILING_PARAMETER = "kylin.engine.async-profiler-enabled";
    public static final String CHINESE_LANGUAGE = "zh";
    public static final String CHINESE_SIMPLE_LANGUAGE = "zh-CN";
    public static final String CHINESE_HK_LANGUAGE = "zh-HK";
    public static final String CHINESE_TW_LANGUAGE = "zh-TW";

    static {
        jobTypeMap.put("INDEX_REFRESH", "Refresh Data");
        jobTypeMap.put("INDEX_MERGE", "Merge Data");
        jobTypeMap.put("INDEX_BUILD", "Build Index");
        jobTypeMap.put("INC_BUILD", "Load Data");
        jobTypeMap.put("TABLE_SAMPLING", "Sample Table");
    }

    @Autowired
    public JobService setAclEvaluate(AclEvaluate aclEvaluate) {
        this.aclEvaluate = aclEvaluate;
        return this;
    }

    @VisibleForTesting
    public ExecutableResponse convert(AbstractExecutable executable) {
        ExecutableResponse executableResponse = ExecutableResponse.create(executable);
        executableResponse.setStatus(executable.getStatus().toJobStatus());
        return executableResponse;
    }

    private List<ExecutablePOSortBean> filterAndSortExecutablePO(final JobFilter jobFilter, List<ExecutablePO> jobs) {
        Preconditions.checkNotNull(jobFilter);
        Preconditions.checkNotNull(jobs);
        Comparator<ExecutablePOSortBean> comparator = nullsLastPropertyComparator(
                StringUtils.isEmpty(jobFilter.getSortBy()) ? LAST_MODIFIED : jobFilter.getSortBy(),
                !jobFilter.isReverse());
        Set<JobStatusEnum> matchedJobStatusEnums = jobFilter.getStatuses().stream().map(JobStatusEnum::valueOf)
                .collect(Collectors.toSet());
        Set<ExecutableState> matchedExecutableStates = matchedJobStatusEnums.stream().map(this::parseToExecutableState)
                .collect(Collectors.toSet());
        boolean streamingEnabled = KylinConfig.getInstanceFromEnv().streamingEnabled();
        Map<String, NDataModelManager> modelManagerMaps = Maps.newHashMap();
        return jobs.stream().filter(((Predicate<ExecutablePO>) (executablePO -> {
            if (CollectionUtils.isEmpty(jobFilter.getStatuses())) {
                return true;
            }
            ExecutableState state = ExecutableState.valueOf(executablePO.getOutput().getStatus());
            return matchedExecutableStates.contains(state) || matchedJobStatusEnums.contains(state.toJobStatus());
        })).and(executablePO -> {
            String subject = StringUtils.trim(jobFilter.getKey());
            if (StringUtils.isEmpty(subject)) {
                return true;
            }
            return StringUtils.containsIgnoreCase(JobUtil.deduceTargetSubject(executablePO), subject)
                    || StringUtils.containsIgnoreCase(executablePO.getId(), subject);
        }).and(executablePO -> {
            List<String> jobNames = jobFilter.getJobNames();
            if (CollectionUtils.isEmpty(jobNames)) {
                return true;
            }
            return jobNames.contains(executablePO.getName());
        }).and(executablePO -> {
            String subject = jobFilter.getSubject();
            if (StringUtils.isEmpty(subject)) {
                return true;
            }
            //if filter on uuid, then it must be accurate
            return executablePO.getTargetModel().equals(jobFilter.getSubject().trim());
        }).and(executablePO -> {
            if (streamingEnabled) {
                return true;
            }
            //filter out batch job of fusion model
            val mgr = getManager(NDataModelManager.class, executablePO.getProject());
            val model = mgr.getDataModelDesc(executablePO.getTargetModel());
            return model == null || !model.isFusionModel();
        })).map(executablePO -> {
            var modelManager = modelManagerMaps.get(executablePO.getProject());
            if (modelManager == null) {
                modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(),
                        executablePO.getProject());
                modelManagerMaps.put(executablePO.getProject(), modelManager);
            }
            return createExecutablePOSortBean(executablePO, modelManager);
        }).sorted(comparator).collect(Collectors.toList());
    }

    private DataResult<List<ExecutableResponse>> filterAndSort(final JobFilter jobFilter, List<ExecutablePO> jobs,
            int offset, int limit) {
        val beanList = filterAndSortExecutablePO(jobFilter, jobs);
        List<ExecutableResponse> result = PagingUtil.cutPage(beanList, offset, limit).stream()
                .map(ExecutablePOSortBean::getExecutablePO)
                .map(executablePO -> getManager(NExecutableManager.class, executablePO.getProject())
                        .fromPO(executablePO))
                .map(executable -> {
                    val convert = convert(executable);
                    val segments = getSegments(executable);
                    convert.setSegments(segments);
                    return convert;
                }).collect(Collectors.toList());
        List<ExecutableResponse> sortAndPagedResult = sortTotalDurationList(result, jobFilter);
        return new DataResult<>(sortAndPagedResult, beanList.size(), offset, limit);
    }

    public List<ExecutableResponse.SegmentResponse> getSegments(AbstractExecutable executable) {
        if (SecondStorageUtil.isModelEnable(executable.getProject(), executable.getTargetModelId())) {
            return modelService
                    .getSegmentsResponseByJob(executable.getTargetModelId(), executable.getProject(), executable)
                    .stream()
                    .map(dataSegmentResponse -> new ExecutableResponse.SegmentResponse(dataSegmentResponse.getId(),
                            dataSegmentResponse.getStatusToDisplay()))
                    .collect(Collectors.toList());
        }
        return Lists.newArrayList();
    }

    //---------------------------------------
    // Used to handle durations boundary cases
    //----------------------------------------
    private List<ExecutableResponse> sortTotalDurationList(List<ExecutableResponse> result, final JobFilter jobFilter) {
        //constructing objects takes time
        Message msg = MsgPicker.getMsg();
        if (StringUtils.isEmpty(jobFilter.getSortBy())) {
            return result;
        }
        final String sortBy = jobFilter.getSortBy();
        switch (sortBy) {
        case DURATION:
        case TOTAL_DURATION:
            Collections.sort(result, nullsLastPropertyComparator(sortBy, !jobFilter.isReverse()));
            break;
        case LAST_MODIFIED:
        case TARGET_SUBJECT:
        case JOB_NAME:
        case JOB_STATUS:
        case PROJECT:
        case CREATE_TIME:
            break;
        default:
            throw new KylinException(INVALID_PARAMETER,
                    String.format(Locale.ROOT, msg.getIllegalSortByFilter(), sortBy));
        }
        return result;
    }

    private List<ExecutableResponse> filterAndSort(final JobFilter jobFilter, List<ExecutablePO> jobs) {
        val beanList = filterAndSortExecutablePO(jobFilter, jobs).stream()//
                .map(in -> in.getExecutablePO())
                .map(executablePO -> getManager(NExecutableManager.class, executablePO.getProject())
                        .fromPO(executablePO))
                .map(this::convert).collect(Collectors.toList());
        return sortTotalDurationList(beanList, jobFilter);
    }

    private List<ExecutablePO> listExecutablePO(final JobFilter jobFilter) {
        JobTimeFilterEnum filterEnum = JobTimeFilterEnum.getByCode(jobFilter.getTimeFilter());
        Preconditions.checkNotNull(filterEnum, "Can not find the JobTimeFilterEnum by code: %s",
                jobFilter.getTimeFilter());

        NExecutableManager executableManager = getManager(NExecutableManager.class, jobFilter.getProject());
        // prepare time range
        Calendar calendar = Calendar.getInstance(TimeZone.getDefault(), Locale.getDefault(Locale.Category.FORMAT));
        calendar.setTime(new Date());
        long timeStartInMillis = getTimeStartInMillis(calendar, filterEnum);
        long timeEndInMillis = Long.MAX_VALUE;
        return executableManager.getAllJobs(timeStartInMillis, timeEndInMillis);
    }

    public List<ExecutableResponse> listJobs(final JobFilter jobFilter) {
        aclEvaluate.checkProjectOperationPermission(jobFilter.getProject());
        return filterAndSort(jobFilter, listExecutablePO(jobFilter));
    }

    public DataResult<List<ExecutableResponse>> listJobs(final JobFilter jobFilter, int offset, int limit) {
        aclEvaluate.checkProjectOperationPermission(jobFilter.getProject());
        return filterAndSort(jobFilter, listExecutablePO(jobFilter), offset, limit);
    }

    public List<ExecutableResponse> addOldParams(List<ExecutableResponse> executableResponseList) {
        executableResponseList.forEach(executableResponse -> {
            ExecutableResponse.OldParams oldParams = new ExecutableResponse.OldParams();
            NDataModel nDataModel = modelService.getManager(NDataModelManager.class, executableResponse.getProject())
                    .getDataModelDesc(executableResponse.getTargetModel());
            String modelName = Objects.isNull(nDataModel) ? null : nDataModel.getAlias();

            List<ExecutableStepResponse> stepResponseList = getJobDetail(executableResponse.getProject(),
                    executableResponse.getId());
            stepResponseList.forEach(stepResponse -> {
                ExecutableStepResponse.OldParams stepOldParams = new ExecutableStepResponse.OldParams();
                stepOldParams.setExecWaitTime(stepResponse.getWaitTime());
                stepResponse.setOldParams(stepOldParams);
            });

            oldParams.setProjectName(executableResponse.getProject());
            oldParams.setRelatedCube(modelName);
            oldParams.setDisplayCubeName(modelName);
            oldParams.setUuid(executableResponse.getId());
            oldParams.setType(jobTypeMap.get(executableResponse.getJobName()));
            oldParams.setName(executableResponse.getJobName());
            oldParams.setExecInterruptTime(0L);
            oldParams.setMrWaiting(executableResponse.getWaitTime());

            executableResponse.setOldParams(oldParams);
            executableResponse.setSteps(stepResponseList);
        });

        return executableResponseList;
    }

    @VisibleForTesting
    public List<ProjectInstance> getReadableProjects() {
        return projectService.getReadableProjects(null, false);
    }

    public DataResult<List<ExecutableResponse>> listGlobalJobs(final JobFilter jobFilter, int offset, int limit) {
        List<ExecutablePO> jobs = new ArrayList<>();
        for (ProjectInstance project : getReadableProjects()) {
            jobFilter.setProject(project.getName());
            jobs.addAll(listExecutablePO(jobFilter));
        }
        jobFilter.setProject(null);

        return filterAndSort(jobFilter, jobs, offset, limit);
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
            throw new KylinException(INVALID_PARAMETER, msg.getIllegalTimeFilter());
        }
    }

    private ExecutableState parseToExecutableState(JobStatusEnum status) {
        Message msg = MsgPicker.getMsg();
        switch (status) {
        case SUICIDAL:
        case DISCARDED:
            return ExecutableState.SUICIDAL;
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
            return ExecutableState.PAUSED;
        default:
            throw new KylinException(INVALID_PARAMETER, msg.getIllegalExecutableState());
        }
    }

    private void dropJob(String project, String jobId) {
        NExecutableManager executableManager = getManager(NExecutableManager.class, project);
        executableManager.deleteJob(jobId);
    }

    private void jobActionValidate(String jobId, String project, String action) {
        JobActionEnum.validateValue(action.toUpperCase(Locale.ROOT));

        AbstractExecutable job = getManager(NExecutableManager.class, project).getJob(jobId);
        if (SecondStorageUtil.isModelEnable(project, job.getTargetModelId())
                && job.getJobSchedulerMode().equals(JobSchedulerModeEnum.DAG)) {
            checkSegmentState(project, action, job);
        }
    }

    @VisibleForTesting
    public void jobActionValidateToTest(String jobId, String project, String action) {
        jobActionValidate(jobId, project, action);
    }

    public void checkSegmentState(String project, String action, AbstractExecutable job) {
        if (!JobActionEnum.RESTART.equals(JobActionEnum.valueOf(action))) {
            return;
        }

        val buildJobTypes = Sets.newHashSet(JobTypeEnum.INC_BUILD, JobTypeEnum.INDEX_BUILD, JobTypeEnum.INDEX_REFRESH,
                JobTypeEnum.SUB_PARTITION_BUILD, JobTypeEnum.SUB_PARTITION_REFRESH, JobTypeEnum.INDEX_MERGE);
        val segmentHalfOnlineStatuses = Sets.newHashSet(SegmentStatusEnumToDisplay.ONLINE_HDFS,
                SegmentStatusEnumToDisplay.ONLINE_OBJECT_STORAGE, SegmentStatusEnumToDisplay.ONLINE_TIERED_STORAGE);
        val segmentMayHalfOnlineStatuses = Sets.newHashSet(SegmentStatusEnumToDisplay.LOADING,
                SegmentStatusEnumToDisplay.WARNING);
        if (buildJobTypes.contains(job.getJobType()) && CollectionUtils.isNotEmpty(job.getSegmentIds())) {
            List<NDataSegmentResponse> segmentsResponseByJob = modelService.getSegmentsResponse(job.getTargetModelId(),
                    project, "0", "" + (Long.MAX_VALUE - 1), "", null, null, false, "sortBy", false, null, null);

            val onlineSegmentCount = segmentsResponseByJob.stream()
                    .filter(segmentResponse -> job.getSegmentIds().contains(segmentResponse.getId()))
                    .filter(segmentResponse -> {
                        val statusSecondStorageToDisplay = segmentResponse.getStatusSecondStorageToDisplay();
                        val statusToDisplay = segmentResponse.getStatusToDisplay();
                        return segmentHalfOnlineStatuses.contains(statusToDisplay)
                                || (segmentMayHalfOnlineStatuses.contains(statusToDisplay)
                                        && SegmentSecondStorageStatusEnum.LOADED == statusSecondStorageToDisplay);
                    }).count();
            if (onlineSegmentCount != 0) {
                throw new KylinException(JOB_RESTART_CHECK_SEGMENT_STATUS);
            }
        }
    }

    @VisibleForTesting
    public void updateJobStatus(String jobId, String project, String action) throws IOException {
        val executableManager = getManager(NExecutableManager.class, project);
        UnitOfWorkContext.UnitTask afterUnitTask = () -> EventBusFactory.getInstance()
                .postWithLimit(new JobReadyNotifier(project));
        jobActionValidate(jobId, project, action);
        switch (JobActionEnum.valueOf(action.toUpperCase(Locale.ROOT))) {
        case RESUME:
            SecondStorageUtil.checkJobResume(project, jobId);
            executableManager.updateJobError(jobId, null, null, null, null);
            executableManager.resumeJob(jobId);
            UnitOfWork.get().doAfterUnit(afterUnitTask);
            MetricsGroup.hostTagCounterInc(MetricsName.JOB_RESUMED, MetricsCategory.PROJECT, project);
            break;
        case RESTART:
            SecondStorageUtil.checkJobRestart(project, jobId);
            executableManager.updateJobError(jobId, null, null, null, null);
            executableManager.addFrozenJob(jobId);
            executableManager.restartJob(jobId);
            UnitOfWorkContext unitOfWorkContext = UnitOfWork.get();
            unitOfWorkContext.doAfterUnit(afterUnitTask);
            unitOfWorkContext.doAfterUnit(() -> executableManager.removeFrozenJob(jobId));
            break;
        case DISCARD:
            discardJob(project, jobId);
            JobTypeEnum jobTypeEnum = executableManager.getJob(jobId).getJobType();
            String jobType = jobTypeEnum == null ? "" : jobTypeEnum.name();
            UnitOfWork.get().doAfterUnit(
                    () -> EventBusFactory.getInstance().postAsync(new JobDiscardNotifier(project, jobType)));
            break;
        case PAUSE:
            SecondStorageUtil.checkJobPause(project, jobId);
            executableManager.pauseJob(jobId);
            break;
        default:
            throw new IllegalStateException("This job can not do this action: " + action);
        }

    }

    private void discardJob(String project, String jobId) {
        AbstractExecutable job = getManager(NExecutableManager.class, project).getJob(jobId);
        if (ExecutableState.SUCCEED == job.getStatus()) {
            throw new KylinException(JOB_UPDATE_STATUS_FAILED, "DISCARD", jobId, job.getStatus());
        }
        if (ExecutableState.DISCARDED == job.getStatus()) {
            return;
        }
        getManager(NExecutableManager.class, project).discardJob(job.getId());
    }

    /**
     * for 3x api, jobId is unique.
     *
     * @param jobId
     * @return
     */
    public String getProjectByJobId(String jobId) {
        Preconditions.checkNotNull(jobId);

        for (ProjectInstance projectInstance : getReadableProjects()) {
            NExecutableManager executableManager = getManager(NExecutableManager.class, projectInstance.getName());
            if (Objects.nonNull(executableManager.getJob(jobId))) {
                return projectInstance.getName();
            }
        }
        return null;
    }

    /**
     * for 3x api
     *
     * @param jobId
     * @return
     */
    public ExecutableResponse getJobInstance(String jobId) {
        Preconditions.checkNotNull(jobId);
        String project = getProjectByJobId(jobId);
        if (StringUtils.isEmpty(project)) {
            throw new KylinException(JOB_NOT_EXIST, jobId);
        }
        NExecutableManager executableManager = getManager(NExecutableManager.class, project);
        AbstractExecutable executable = executableManager.getJob(jobId);

        return convert(executable);
    }

    /**
     * for 3x api
     *
     * @param project
     * @param job
     * @param action
     * @return
     * @throws IOException
     */
    public ExecutableResponse manageJob(String project, ExecutableResponse job, String action) {
        Preconditions.checkNotNull(project);
        Preconditions.checkNotNull(job);
        Preconditions.checkArgument(!StringUtils.isBlank(action));

        if (JobActionEnum.DISCARD == JobActionEnum.valueOf(action)) {
            return job;
        }

        return EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            updateJobStatus(job.getId(), project, action);
            return getJobInstance(job.getId());
        }, project, UnitOfWork.DEFAULT_MAX_RETRY, UnitOfWork.DEFAULT_EPOCH_ID, job.getId());

    }

    public List<ExecutableStepResponse> getJobDetail(String project, String jobId) {
        aclEvaluate.checkProjectOperationPermission(project);
        NExecutableManager executableManager = getManager(NExecutableManager.class, project);
        //executableManager.getJob only reply ChainedExecutable
        AbstractExecutable executable = executableManager.getJob(jobId);
        if (executable == null) {
            throw new KylinException(JOB_NOT_EXIST, jobId);
        }

        // waite time in output
        Map<String, String> waiteTimeMap;
        val output = executable.getOutput();
        try (SetLogCategory ignored = new SetLogCategory(LogConstant.BUILD_CATEGORY)) {
            waiteTimeMap = JsonUtil.readValueAsMap(output.getExtra().getOrDefault(NBatchConstants.P_WAITE_TIME, "{}"));
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            waiteTimeMap = Maps.newHashMap();
        }
        final String targetSubject = executable.getTargetSubject();
        List<ExecutableStepResponse> executableStepList = new ArrayList<>();
        List<? extends AbstractExecutable> tasks = ((ChainedExecutable) executable).getTasks();
        for (AbstractExecutable task : tasks) {
            final ExecutableStepResponse executableStepResponse = parseToExecutableStep(task,
                    executableManager.getOutput(task.getId()), waiteTimeMap, output.getState());
            if (task.getStatus() == ExecutableState.ERROR
                    && StringUtils.startsWith(output.getFailedStepId(), task.getId())) {
                executableStepResponse.setFailedStepId(output.getFailedStepId());
                executableStepResponse.setFailedSegmentId(output.getFailedSegmentId());
                executableStepResponse.setFailedStack(output.getFailedStack());
                executableStepResponse.setFailedStepName(task.getName());

                setExceptionResolveAndCodeAndReason(output, executableStepResponse);
            }
            if (executable.getJobSchedulerMode().equals(JobSchedulerModeEnum.DAG)
                    && task.getStatus() == ExecutableState.ERROR
                    && !StringUtils.startsWith(output.getFailedStepId(), task.getId())) {
                executableStepResponse.setStatus(JobStatusEnum.STOPPED);
            }
            if (task instanceof ChainedStageExecutable) {
                Map<String, List<StageBase>> stagesMap = Optional
                        .ofNullable(((ChainedStageExecutable) task).getStagesMap()).orElse(Maps.newHashMap());

                Map<String, ExecutableStepResponse.SubStages> stringSubStageMap = Maps.newHashMap();
                List<ExecutableStepResponse> subStages = Lists.newArrayList();

                for (Map.Entry<String, List<StageBase>> entry : stagesMap.entrySet()) {
                    String segmentId = entry.getKey();
                    ExecutableStepResponse.SubStages segmentSubStages = new ExecutableStepResponse.SubStages();

                    List<StageBase> stageBases = Optional.ofNullable(entry.getValue()).orElse(Lists.newArrayList());
                    List<ExecutableStepResponse> stageResponses = Lists.newArrayList();
                    for (StageBase stage : stageBases) {
                        val stageResponse = parseStageToExecutableStep(task, stage,
                                executableManager.getOutput(stage.getId(), segmentId));
                        if (executable.getJobSchedulerMode().equals(JobSchedulerModeEnum.DAG)
                                && stage.getStatus(segmentId) == ExecutableState.ERROR
                                && !StringUtils.startsWith(output.getFailedStepId(), stage.getId())) {
                            stageResponse.setStatus(JobStatusEnum.STOPPED);
                        }
                        setStage(subStages, stageResponse);
                        stageResponses.add(stageResponse);

                        if (StringUtils.equals(output.getFailedStepId(), stage.getId())) {
                            executableStepResponse.setFailedStepName(stage.getName());
                        }
                    }

                    // table sampling and snapshot table don't have some segment
                    if (!StringUtils.equals(task.getId(), segmentId)) {
                        setSegmentSubStageParams(project, targetSubject, task, segmentId, segmentSubStages, stageBases,
                                stageResponses, waiteTimeMap, output.getState());
                        stringSubStageMap.put(segmentId, segmentSubStages);
                    }
                }
                if (MapUtils.isNotEmpty(stringSubStageMap)) {
                    executableStepResponse.setSegmentSubStages(stringSubStageMap);
                }
                if (CollectionUtils.isNotEmpty(subStages)) {
                    executableStepResponse.setSubStages(subStages);
                    if (MapUtils.isEmpty(stringSubStageMap) || stringSubStageMap.size() == 1) {
                        val taskDuration = subStages.stream() //
                                .map(ExecutableStepResponse::getDuration) //
                                .mapToLong(Long::valueOf).sum();
                        executableStepResponse.setDuration(taskDuration);

                    }
                }
            }
            executableStepList.add(executableStepResponse);
        }
        if (executable.getStatus() == ExecutableState.DISCARDED) {
            executableStepList.forEach(executableStepResponse -> {
                executableStepResponse.setStatus(JobStatusEnum.DISCARDED);
                Optional.ofNullable(executableStepResponse.getSubStages()).orElse(Lists.newArrayList())
                        .forEach(subtask -> subtask.setStatus(JobStatusEnum.DISCARDED));
                val subStageMap = //
                        Optional.ofNullable(executableStepResponse.getSegmentSubStages()).orElse(Maps.newHashMap());
                for (Map.Entry<String, ExecutableStepResponse.SubStages> entry : subStageMap.entrySet()) {
                    entry.getValue().getStage().forEach(stage -> stage.setStatus(JobStatusEnum.DISCARDED));
                }
            });
        }
        return executableStepList;

    }

    public void setExceptionResolveAndCodeAndReason(Output output, ExecutableStepResponse executableStepResponse) {
        try (SetLogCategory ignored = new SetLogCategory(LogConstant.BUILD_CATEGORY)) {
            val exceptionCode = getExceptionCode(output);
            executableStepResponse.setFailedResolve(ExceptionResolve.getResolve(exceptionCode));
            executableStepResponse.setFailedCode(ErrorCode.getLocalizedString(exceptionCode));
            if (StringUtils.equals(exceptionCode, EXCEPTION_CODE_DEFAULT)) {
                val reason = StringUtils.isBlank(output.getFailedReason())
                        ? JobExceptionReason.JOB_BUILDING_ERROR.toExceptionReason().getReason()
                        : JobExceptionReason.JOB_BUILDING_ERROR.toExceptionReason().getReason() + ": "
                                + output.getFailedReason();
                executableStepResponse.setFailedReason(reason);
            } else {
                executableStepResponse.setFailedReason(ExceptionReason.getReason(exceptionCode));
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            executableStepResponse
                    .setFailedResolve(JobExceptionResolve.JOB_BUILDING_ERROR.toExceptionResolve().getResolve());
            executableStepResponse.setFailedCode(JobErrorCode.JOB_BUILDING_ERROR.toErrorCode().getLocalizedString());
            executableStepResponse
                    .setFailedReason(JobExceptionReason.JOB_BUILDING_ERROR.toExceptionReason().getReason());
        }
    }

    public String getExceptionCode(Output output) {
        try (SetLogCategory ignored = new SetLogCategory(LogConstant.BUILD_CATEGORY)) {
            var exceptionOrExceptionMessage = output.getFailedReason();

            if (StringUtils.isBlank(exceptionOrExceptionMessage)) {
                if (StringUtils.isBlank(output.getFailedStack())) {
                    return EXCEPTION_CODE_DEFAULT;
                }
                exceptionOrExceptionMessage = output.getFailedStack().split("\n")[0];
            }

            val exceptionCodeStream = getClass().getClassLoader().getResource(EXCEPTION_CODE_PATH).openStream();
            val exceptionCodes = JsonUtil.readValue(exceptionCodeStream, Map.class);
            for (Object o : exceptionCodes.entrySet()) {
                val exceptionCode = (Map.Entry) o;
                if (StringUtils.contains(exceptionOrExceptionMessage, String.valueOf(exceptionCode.getKey()))
                        || StringUtils.contains(String.valueOf(exceptionCode.getKey()), exceptionOrExceptionMessage)) {
                    val code = exceptionCodes.getOrDefault(exceptionCode.getKey(), EXCEPTION_CODE_DEFAULT);
                    return String.valueOf(code);
                }
            }
            return EXCEPTION_CODE_DEFAULT;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return EXCEPTION_CODE_DEFAULT;
        }
    }

    private void setSegmentSubStageParams(String project, String targetSubject, AbstractExecutable task,
            String segmentId, ExecutableStepResponse.SubStages segmentSubStages, List<StageBase> stageBases,
            List<ExecutableStepResponse> stageResponses, Map<String, String> waiteTimeMap, ExecutableState jobState) {
        segmentSubStages.setStage(stageResponses);

        // when job restart, taskStartTime is zero
        if (CollectionUtils.isNotEmpty(stageResponses)) {
            val taskStartTime = task.getStartTime();
            var firstStageStartTime = stageResponses.get(0).getExecStartTime();
            if (taskStartTime != 0 && firstStageStartTime == 0) {
                firstStageStartTime = System.currentTimeMillis();
            }
            long waitTime = Long.parseLong(waiteTimeMap.getOrDefault(segmentId, "0"));
            if (jobState != ExecutableState.PAUSED) {
                waitTime = firstStageStartTime - taskStartTime + waitTime;
            }
            segmentSubStages.setWaitTime(waitTime);
        }

        val execStartTime = stageResponses.stream()//
                .filter(ex -> ex.getStatus() != JobStatusEnum.PENDING)//
                .map(ExecutableStepResponse::getExecStartTime)//
                .min(Long::compare).orElse(0L);
        segmentSubStages.setExecStartTime(execStartTime);

        // If this segment has running stage, this segment is running, this segment doesn't have end time
        // If this task is running and this segment has pending stage, this segment is running, this segment doesn't have end time
        val stageStatuses = stageResponses.stream().map(ExecutableStepResponse::getStatus).collect(Collectors.toSet());
        if (!stageStatuses.contains(JobStatusEnum.RUNNING)
                && !(task.getStatus() == ExecutableState.RUNNING && stageStatuses.contains(JobStatusEnum.PENDING))) {
            val execEndTime = stageResponses.stream()//
                    .map(ExecutableStepResponse::getExecEndTime)//
                    .max(Long::compare).orElse(0L);
            segmentSubStages.setExecEndTime(execEndTime);
        }

        val segmentDuration = stageResponses.stream() //
                .map(ExecutableStepResponse::getDuration) //
                .mapToLong(Long::valueOf).sum();
        segmentSubStages.setDuration(segmentDuration);

        final Segments<NDataSegment> segmentsByRange = modelService.getSegmentsByRange(targetSubject, project, "", "");
        final NDataSegment segment = segmentsByRange.stream()//
                .filter(seg -> StringUtils.equals(seg.getId(), segmentId))//
                .findFirst().orElse(null);
        if (null != segment) {
            val segRange = segment.getSegRange();
            segmentSubStages.setName(segment.getName());
            segmentSubStages.setStartTime(Long.parseLong(segRange.getStart().toString()));
            segmentSubStages.setEndTime(Long.parseLong(segRange.getEnd().toString()));
        }

        /*
         * In the segment details, the progress formula of each segment
         *
         * CurrentProgress = numberOfStepsCompleted / totalNumberOfSteps，Accurate to single digit percentage。
         * This step only retains the steps in the parallel part of the Segment，
         * Does not contain other public steps, such as detection resources, etc.。
         *
         * Among them, the progress of the "BUILD_LAYER"
         *   step = numberOfCompletedIndexes / totalNumberOfIndexesToBeConstructed,
         * the progress of other steps will not be refined
         */
        val stepCount = stageResponses.isEmpty() ? 1 : stageResponses.size();
        val stepRatio = (float) ExecutableResponse.calculateSuccessStage(task, segmentId, stageBases, true) / stepCount;
        segmentSubStages.setStepRatio(stepRatio);

        // Put warning message into segment_sub_stages.info if exists
        Optional<ExecutableStepResponse> warningStageRes = stageResponses.stream().filter(stageRes ->
                stageRes.getStatus() == JobStatusEnum.WARNING).findFirst();
        warningStageRes.ifPresent(res -> segmentSubStages.getInfo().put(NBatchConstants.P_WARNING_CODE,
                res.getInfo().getOrDefault(NBatchConstants.P_WARNING_CODE, null)));
    }

    private void setStage(List<ExecutableStepResponse> responses, ExecutableStepResponse newResponse) {
        final ExecutableStepResponse oldResponse = responses.stream()
                .filter(response -> response.getId().equals(newResponse.getId()))//
                .findFirst().orElse(null);
        if (null != oldResponse) {
            /*
             * As long as there is a task executing, the step of this step is executing;
             * when all Segments are completed, the status of this step is changed to complete.
             *
             * if one segment is skip, other segment is success, the status of this step is success
             */
            Set<JobStatusEnum> jobStatusEnums = Sets.newHashSet(JobStatusEnum.ERROR, JobStatusEnum.STOPPED,
                    JobStatusEnum.DISCARDED);
            Set<JobStatusEnum> jobFinishOrSkip = Sets.newHashSet(JobStatusEnum.FINISHED, JobStatusEnum.SKIP, JobStatusEnum.WARNING);
            if (oldResponse.getStatus() != newResponse.getStatus()
                    && !jobStatusEnums.contains(oldResponse.getStatus())) {
                if (jobStatusEnums.contains(newResponse.getStatus())) {
                    oldResponse.setStatus(newResponse.getStatus());
                } else if (jobFinishOrSkip.contains(newResponse.getStatus())
                        && jobFinishOrSkip.contains(oldResponse.getStatus())) {
                    oldResponse.setStatus(JobStatusEnum.FINISHED);
                } else {
                    oldResponse.setStatus(JobStatusEnum.RUNNING);
                }
            }

            if (newResponse.getExecStartTime() != 0) {
                oldResponse.setExecStartTime(Math.min(newResponse.getExecStartTime(), oldResponse.getExecStartTime()));
            }
            oldResponse.setExecEndTime(Math.max(newResponse.getExecEndTime(), oldResponse.getExecEndTime()));

            val successIndex = oldResponse.getSuccessIndexCount() + newResponse.getSuccessIndexCount();
            oldResponse.setSuccessIndexCount(successIndex);
            val index = oldResponse.getIndexCount() + newResponse.getIndexCount();
            oldResponse.setIndexCount(index);
        } else {
            ExecutableStepResponse res = new ExecutableStepResponse();
            res.setId(newResponse.getId());
            res.setName(newResponse.getName());
            res.setSequenceID(newResponse.getSequenceID());
            res.setExecStartTime(newResponse.getExecStartTime());
            res.setExecEndTime(newResponse.getExecEndTime());
            res.setDuration(newResponse.getDuration());
            res.setWaitTime(newResponse.getWaitTime());
            res.setIndexCount(newResponse.getIndexCount());
            res.setSuccessIndexCount(newResponse.getSuccessIndexCount());
            res.setStatus(newResponse.getStatus());
            res.setCmdType(newResponse.getCmdType());
            responses.add(res);
        }
    }

    private ExecutableStepResponse parseStageToExecutableStep(AbstractExecutable task, StageBase stageBase,
            Output stageOutput) {
        ExecutableStepResponse result = new ExecutableStepResponse();
        result.setId(stageBase.getId());
        result.setName(stageBase.getName());
        result.setSequenceID(stageBase.getStepId());

        if (stageOutput == null) {
            try (SetLogCategory ignored = new SetLogCategory(LogConstant.BUILD_CATEGORY)) {
                logger.warn("Cannot found output for task: id={}", stageBase.getId());
            }
            return result;
        }
        for (Map.Entry<String, String> entry : stageOutput.getExtra().entrySet()) {
            if (entry.getKey() != null && entry.getValue() != null) {
                result.putInfo(entry.getKey(), entry.getValue());
            }
        }
        result.setStatus(stageOutput.getState().toJobStatus());
        result.setExecStartTime(AbstractExecutable.getStartTime(stageOutput));
        result.setExecEndTime(AbstractExecutable.getEndTime(stageOutput));
        result.setCreateTime(AbstractExecutable.getCreateTime(stageOutput));

        result.setDuration(AbstractExecutable.getStageDuration(stageOutput, task.getParent()));

        val indexCount = Optional.ofNullable(task.getParam(NBatchConstants.P_INDEX_COUNT)).orElse("0");
        result.setIndexCount(Long.parseLong(indexCount));
        if (result.getStatus() == JobStatusEnum.FINISHED) {
            result.setSuccessIndexCount(Long.parseLong(indexCount));
        } else {
            val successIndexCount = stageOutput.getExtra().getOrDefault(NBatchConstants.P_INDEX_SUCCESS_COUNT, "0");
            result.setSuccessIndexCount(Long.parseLong(successIndexCount));
        }
        return result;
    }

    // for ut
    @VisibleForTesting
    public ExecutableStepResponse parseToExecutableStep(AbstractExecutable task, Output stepOutput,
            Map<String, String> waiteTimeMap, ExecutableState jobState) {
        ExecutableStepResponse result = new ExecutableStepResponse();
        result.setId(task.getId());
        result.setName(task.getName());
        result.setSequenceID(task.getStepId());

        if (stepOutput == null) {
            try (SetLogCategory ignored = new SetLogCategory(LogConstant.BUILD_CATEGORY)) {
                logger.warn("Cannot found output for task: id={}", task.getId());
            }
            return result;
        }

        result.setStatus(stepOutput.getState().toJobStatus());
        for (Map.Entry<String, String> entry : stepOutput.getExtra().entrySet()) {
            if (entry.getKey() != null && entry.getValue() != null) {
                result.putInfo(entry.getKey(), entry.getValue());
            }
        }
        if (KylinConfig.getInstanceFromEnv().isHistoryServerEnable()
                && result.getInfo().containsKey(ExecutableConstants.YARN_APP_ID)) {
            result.putInfo(ExecutableConstants.SPARK_HISTORY_APP_URL,
                    SparkHistoryUIUtil.getHistoryTrackerUrl(result.getInfo().get(ExecutableConstants.YARN_APP_ID)));
        }
        result.setExecStartTime(AbstractExecutable.getStartTime(stepOutput));
        result.setExecEndTime(AbstractExecutable.getEndTime(stepOutput));
        result.setCreateTime(AbstractExecutable.getCreateTime(stepOutput));

        result.setDuration(AbstractExecutable.getDuration(stepOutput));
        // if resume job, need sum of waite time
        long waiteTime = Long.parseLong(waiteTimeMap.getOrDefault(task.getId(), "0"));
        if (jobState != ExecutableState.PAUSED) {
            val taskWaitTime = task.getWaitTime();
            // Refactoring: When task Wait Time is equal to waite Time, waiteTimeMap saves the latest waiting time
            if (taskWaitTime != waiteTime) {
                waiteTime = taskWaitTime + waiteTime;
            }
        }
        result.setWaitTime(waiteTime);

        if (task instanceof ShellExecutable) {
            result.setExecCmd(((ShellExecutable) task).getCmd());
        }
        result.setShortErrMsg(stepOutput.getShortErrMsg());
        result.setPreviousStep(task.getPreviousStep());
        result.setNextSteps(task.getNextSteps());
        return result;
    }

    private void batchUpdateJobStatus0(List<String> jobIds, String project, String action,
            List<String> filterStatuses) {
        val jobs = getJobsByStatus(project, jobIds, filterStatuses);
        for (val job : jobs) {
            EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                updateJobStatus(job.getId(), project, action);
                return null;
            }, project, UnitOfWork.DEFAULT_MAX_RETRY, UnitOfWork.DEFAULT_EPOCH_ID, job.getId());
        }
    }

    public void updateJobError(String project, String jobId, String failedStepId, String failedSegmentId,
            String failedStack, String failedReason) {
        if (StringUtils.isBlank(failedStepId)) {
            return;
        }
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            val executableManager = getManager(NExecutableManager.class, project);
            executableManager.updateJobError(jobId, failedStepId, failedSegmentId, failedStack, failedReason);
            return null;
        }, project, UnitOfWork.DEFAULT_MAX_RETRY, UnitOfWork.DEFAULT_EPOCH_ID, jobId);
    }

    public void updateStageStatus(String project, String taskId, String segmentId, String status,
            Map<String, String> updateInfo, String errMsg) {
        final ExecutableState newStatus = convertToExecutableState(status);
        val jobId = NExecutableManager.extractJobId(taskId);
        val jobManager = getManager(NExecutableManager.class, project);
        boolean isFrozenJob = jobManager.isFrozenJob(jobId);
        if (isFrozenJob) {
            return;
        }
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            val executableManager = getManager(NExecutableManager.class, project);
            executableManager.updateStageStatus(taskId, segmentId, newStatus, updateInfo, errMsg);
            executableManager.saveUpdatedJob();
            return null;
        }, project, UnitOfWork.DEFAULT_MAX_RETRY, UnitOfWork.DEFAULT_EPOCH_ID, jobId);
    }

    public ExecutableState convertToExecutableState(String status) {
        if (StringUtils.isBlank(status)) {
            return null;
        }
        return ExecutableState.valueOf(status);
    }

    public void batchUpdateJobStatus(List<String> jobIds, String project, String action, List<String> filterStatuses) {
        aclEvaluate.checkProjectOperationPermission(project);
        batchUpdateJobStatus0(jobIds, project, action, filterStatuses);
    }

    public void batchUpdateGlobalJobStatus(List<String> jobIds, String action, List<String> filterStatuses) {
        try (SetLogCategory ignored = new SetLogCategory(LogConstant.BUILD_CATEGORY)) {
            logger.info("Owned projects is {}", projectService.getOwnedProjects());
        }
        for (String project : projectService.getOwnedProjects()) {
            aclEvaluate.checkProjectOperationPermission(project);
            batchUpdateJobStatus0(jobIds, project, action, filterStatuses);
        }
    }

    private void batchDropJob0(String project, List<String> jobIds, List<String> filterStatuses) {
        val jobs = getJobsByStatus(project, jobIds, filterStatuses);
        jobs.forEach(job -> EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NExecutableManager executableManager = getManager(NExecutableManager.class, project);
            executableManager.checkJobCanBeDeleted(job.getId());
            dropJob(project, job.getId());
            return null;
        }, project, UnitOfWork.DEFAULT_MAX_RETRY, UnitOfWork.DEFAULT_EPOCH_ID, job.getId()));
    }

    private List<AbstractExecutable> getJobsByStatus(String project, List<String> jobIds, List<String> filterStatuses) {
        Preconditions.checkNotNull(project);

        val executableManager = getManager(NExecutableManager.class, project);
        List<ExecutableState> executableStates = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(filterStatuses)) {
            for (String status : filterStatuses) {
                JobStatusEnum jobStatus = JobStatusEnum.getByName(status);
                if (Objects.nonNull(jobStatus)) {
                    executableStates.add(parseToExecutableState(jobStatus));
                }
            }
        }

        return executableManager.getExecutablesByStatus(jobIds, executableStates);
    }

    public void batchDropJob(String project, List<String> jobIds, List<String> filterStatuses) {
        aclEvaluate.checkProjectOperationPermission(project);
        batchDropJob0(project, jobIds, filterStatuses);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#ae, 'ADMINISTRATION')")
    public void batchDropGlobalJob(List<String> jobIds, List<String> filterStatuses) {
        for (String project : projectService.getOwnedProjects()) {
            aclEvaluate.checkProjectOperationPermission(project);
            batchDropJob0(project, jobIds, filterStatuses);
        }
    }

    public JobStatisticsResponse getJobStats(String project, long startTime, long endTime) {
        aclEvaluate.checkProjectOperationPermission(project);
        JobStatisticsManager manager = getManager(JobStatisticsManager.class, project);
        Pair<Integer, JobStatistics> stats = manager.getOverallJobStats(startTime, endTime);
        JobStatistics jobStatistics = stats.getSecond();
        return new JobStatisticsResponse(stats.getFirst(), jobStatistics.getTotalDuration(),
                jobStatistics.getTotalByteSize());
    }

    public Map<String, Integer> getJobCount(String project, long startTime, long endTime, String dimension) {
        aclEvaluate.checkProjectOperationPermission(project);
        JobStatisticsManager manager = getManager(JobStatisticsManager.class, project);
        if (dimension.equals("model")) {
            return manager.getJobCountByModel(startTime, endTime);
        }

        return manager.getJobCountByTime(startTime, endTime, dimension);
    }

    public Map<String, Double> getJobDurationPerByte(String project, long startTime, long endTime, String dimension) {
        aclEvaluate.checkProjectOperationPermission(project);
        JobStatisticsManager manager = getManager(JobStatisticsManager.class, project);
        if (dimension.equals("model")) {
            return manager.getDurationPerByteByModel(startTime, endTime);
        }

        return manager.getDurationPerByteByTime(startTime, endTime, dimension);
    }

    public Map<String, Object> getEventsInfoGroupByModel(String project) {
        aclEvaluate.checkProjectOperationPermission(project);
        Map<String, Object> result = Maps.newHashMap();
        result.put("data", null);
        result.put("size", 0);
        return result;
    }

    public String getJobOutput(String project, String jobId) {
        return getJobOutput(project, jobId, jobId);
    }

    public String getJobOutput(String project, String jobId, String stepId) {
        aclEvaluate.checkProjectOperationPermission(project);
        val executableManager = getManager(NExecutableManager.class, project);
        return executableManager.getOutputFromHDFSByJobId(jobId, stepId).getVerboseMsg();
    }

    public Map<String, Object> getStepOutput(String project, String jobId, String stepId) {
        aclEvaluate.checkProjectOperationPermission(project);
        val executableManager = getManager(NExecutableManager.class, project);
        Output output = executableManager.getOutputFromHDFSByJobId(jobId, stepId);
        Map<String, Object> result = new HashMap<>();
        result.put("cmd_output", output.getVerboseMsg());

        Map<String, String> info = output.getExtra();
        List<String> servers = Lists.newArrayList();
        if (info != null && info.get("nodes") != null) {
            servers = Lists.newArrayList(info.get("nodes").split(","));
        }
        List<String> nodes = servers.stream().map(server -> {
            String[] split = server.split(":");
            return split[0] + ":" + split[1];
        }).collect(Collectors.toList());
        result.put("nodes", nodes);
        return result;
    }

    @SneakyThrows
    public InputStream getAllJobOutput(String project, String jobId, String stepId) {
        aclEvaluate.checkProjectOperationPermission(project);
        val executableManager = getManager(NExecutableManager.class, project);
        val output = executableManager.getOutputFromHDFSByJobId(jobId, stepId, Integer.MAX_VALUE);
        return Optional.ofNullable(output.getVerboseMsgStream()).orElse(
                IOUtils.toInputStream(Optional.ofNullable(output.getVerboseMsg()).orElse(StringUtils.EMPTY), "UTF-8"));
    }

    /**
     * update the spark job info, such as yarnAppId, yarnAppUrl.
     *
     * @param project
     * @param jobId
     * @param taskId
     * @param yarnAppId
     * @param yarnAppUrl
     */
    public void updateSparkJobInfo(String project, String jobId, String taskId, String yarnAppId, String yarnAppUrl) {
        if (jobId.contains(ASYNC_QUERY_JOB_ID_PRE)) {
            return;
        }

        Map<String, String> extraInfo = Maps.newHashMap();
        extraInfo.put(ExecutableConstants.YARN_APP_ID, yarnAppId);
        extraInfo.put(ExecutableConstants.YARN_APP_URL, yarnAppUrl);
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            val executableManager = getManager(NExecutableManager.class, project);
            executableManager.updateJobOutput(taskId, null, extraInfo, null, null);
            return null;
        }, project, UnitOfWork.DEFAULT_MAX_RETRY, UnitOfWork.DEFAULT_EPOCH_ID, jobId);
    }

    public void updateSparkTimeInfo(String project, String jobId, String taskId, String waitTime, String buildTime) {

        Map<String, String> extraInfo = Maps.newHashMap();
        extraInfo.put(ExecutableConstants.YARN_JOB_WAIT_TIME, waitTime);
        extraInfo.put(ExecutableConstants.YARN_JOB_RUN_TIME, buildTime);

        if (jobId.contains(ASYNC_QUERY_JOB_ID_PRE)) {
            return;
        }

        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            val executableManager = getManager(NExecutableManager.class, project);
            executableManager.updateJobOutput(taskId, null, extraInfo, null, null);
            return null;
        }, project, UnitOfWork.DEFAULT_MAX_RETRY, UnitOfWork.DEFAULT_EPOCH_ID, jobId);
    }

    public void checkJobStatus(List<String> jobStatuses) {
        if (CollectionUtils.isEmpty(jobStatuses)) {
            return;
        }
        jobStatuses.forEach(this::checkJobStatus);
    }

    public void checkJobStatus(String jobStatus) {
        if (Objects.isNull(JobStatusEnum.getByName(jobStatus))) {
            throw new KylinException(JOB_STATUS_ILLEGAL);
        }
    }

    public void checkJobStatusAndAction(String jobStatus, String action) {
        checkJobStatus(jobStatus);
        JobActionEnum.validateValue(action);
        JobStatusEnum jobStatusEnum = JobStatusEnum.valueOf(jobStatus);
        if (!jobStatusEnum.checkAction(JobActionEnum.valueOf(action))) {
            throw new KylinException(JOB_ACTION_ILLEGAL, jobStatus, jobStatusEnum.getValidActions());
        }

    }

    public void checkJobStatusAndAction(JobUpdateRequest jobUpdateRequest) {
        List<String> jobIds = jobUpdateRequest.getJobIds();
        List<String> jobStatuses = jobUpdateRequest.getStatuses() == null ? Lists.newArrayList()
                : jobUpdateRequest.getStatuses();
        jobIds.stream().map(this::getJobInstance).map(ExecutableResponse::getStatus).map(JobStatusEnum::toString)
                .forEach(jobStatuses::add);
        checkJobStatusAndAction(jobStatuses, jobUpdateRequest.getAction());
    }

    private void checkJobStatusAndAction(List<String> jobStatuses, String action) {
        if (CollectionUtils.isEmpty(jobStatuses)) {
            return;
        }
        for (String jobStatus : jobStatuses) {
            checkJobStatusAndAction(jobStatus, action);
        }
    }

    private ExecutablePOSortBean createExecutablePOSortBean(ExecutablePO executablePO, NDataModelManager modelManager) {
        ExecutablePOSortBean sortBean = new ExecutablePOSortBean();
        sortBean.setProject(executablePO.getProject());
        sortBean.setJobName(executablePO.getName());
        sortBean.setId(executablePO.getId());
        sortBean.setTargetSubject(sortBean.getTargetModelAlias(executablePO, modelManager));
        sortBean.setLastModified(executablePO.getLastModified());
        sortBean.setCreateTime(executablePO.getCreateTime());
        sortBean.setTotalDuration(sortBean.computeTotalDuration(executablePO));
        sortBean.setDuration(sortBean.getDurationByPO(executablePO));
        sortBean.setExecutablePO(executablePO);
        return sortBean;
    }

    @Override
    public void stopBatchJob(String project, TableDesc tableDesc) {
        for (NDataModel tableRelatedModel : getManager(NDataflowManager.class, project)
                .getModelsUsingTable(tableDesc)) {
            stopBatchJobByModel(project, tableRelatedModel.getId());
        }
    }

    private void stopBatchJobByModel(String project, String modelId) {

        NDataModel model = getManager(NDataModelManager.class, project).getDataModelDesc(modelId);
        FusionModelManager fusionModelManager = FusionModelManager.getInstance(KylinConfig.getInstanceFromEnv(),
                project);
        FusionModel fusionModel = fusionModelManager.getFusionModel(modelId);
        if (!model.isFusionModel() || Objects.isNull(fusionModel)) {
            try (SetLogCategory ignored = new SetLogCategory(LogConstant.BUILD_CATEGORY)) {
                logger.warn("model is not fusion model or fusion model is null, {}", modelId);
            }
            return;
        }

        NExecutableManager executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(),
                project);
        executableManager.getJobs().stream().map(executableManager::getJob).filter(
                job -> StringUtils.equalsIgnoreCase(job.getTargetModelId(), fusionModel.getBatchModel().getUuid()))
                .forEach(job -> {
                    Set<ExecutableState> matchedExecutableStates = Stream
                            .of(JobStatusEnum.FINISHED, JobStatusEnum.ERROR, JobStatusEnum.DISCARDED)
                            .map(this::parseToExecutableState).collect(Collectors.toSet());
                    if (!matchedExecutableStates.contains(job.getOutput().getState())) {
                        executableManager.discardJob(job.getId());
                    }
                });
    }

    public void startProfileByProject(String project, String jobStepId, String params) {
        if (!KylinConfig.getInstanceFromEnv().buildJobProfilingEnabled()) {
            throw new KylinException(JobErrorCode.PROFILING_NOT_ENABLED, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getProfilingNotEnabled(), BUILD_JOB_PROFILING_PARAMETER));
        }
        BuildAsyncProfileHelper.startProfile(project, jobStepId, params);
    }

    public void dumpProfileByProject(String project, String jobStepId, String params,
            Pair<InputStream, String> jobOutputAndDownloadFile) {
        if (!KylinConfig.getInstanceFromEnv().buildJobProfilingEnabled()) {
            throw new KylinException(JobErrorCode.PROFILING_NOT_ENABLED, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getProfilingNotEnabled(), BUILD_JOB_PROFILING_PARAMETER));
        }
        InputStream jobOutput = BuildAsyncProfileHelper.dump(project, jobStepId, params);
        jobOutputAndDownloadFile.setFirst(jobOutput);
        String downloadFilename = String.format(Locale.ROOT, "%s_%s_dump.tar.gz", project, jobStepId);
        jobOutputAndDownloadFile.setSecond(downloadFilename);
    }

    public void startProfileByYarnAppId(String yarnAppId, String params) {
        if (!KylinConfig.getInstanceFromEnv().buildJobProfilingEnabled()) {
            throw new KylinException(JobErrorCode.PROFILING_NOT_ENABLED, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getProfilingNotEnabled(), BUILD_JOB_PROFILING_PARAMETER));
        }
        Pair<String, String> projectNameAndJobStepId = getProjectNameAndJobStepId(yarnAppId);
        BuildAsyncProfileHelper.startProfile(projectNameAndJobStepId.getFirst(), projectNameAndJobStepId.getSecond(),
                params);
    }

    public void dumpProfileByYarnAppId(String yarnAppId, String params,
            Pair<InputStream, String> jobOutputAndDownloadFile) {
        if (!KylinConfig.getInstanceFromEnv().buildJobProfilingEnabled()) {
            throw new KylinException(JobErrorCode.PROFILING_NOT_ENABLED, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getProfilingNotEnabled(), BUILD_JOB_PROFILING_PARAMETER));
        }
        Pair<String, String> projectNameAndJobStepId = getProjectNameAndJobStepId(yarnAppId);
        InputStream jobOutput = BuildAsyncProfileHelper.dump(projectNameAndJobStepId.getFirst(),
                projectNameAndJobStepId.getSecond(), params);
        jobOutputAndDownloadFile.setFirst(jobOutput);
        String downloadFilename = String.format(Locale.ROOT, "%s_%s_dump.tar.gz", projectNameAndJobStepId.getFirst(),
                projectNameAndJobStepId.getSecond());
        jobOutputAndDownloadFile.setSecond(downloadFilename);
    }

    /*
     * return as [projectName, jobStepId]
     */
    public Pair<String, String> getProjectNameAndJobStepId(String yarnAppId) {
        IClusterManager iClusterManager = ClusterManagerFactory.create(KylinConfig.getInstanceFromEnv());
        if (yarnAppId.contains(YARN_APP_SEPARATOR)) {
            // yarnAppId such as application_{timestamp}_30076
            String[] splits = yarnAppId.split(YARN_APP_SEPARATOR);
            if (splits.length == 3) {
                String appId = splits[2];
                // build applicationName such as job_step_{jobId}_01, sometimes maybe job_step_{jobId}_00
                String applicationName = iClusterManager.getApplicationNameById(Integer.parseInt(appId));

                if (applicationName.contains(JOB_STEP_PREFIX)) {
                    String jobStepId = StringUtils.replace(applicationName, JOB_STEP_PREFIX, "");
                    String jobId = applicationName.split(YARN_APP_SEPARATOR)[2];
                    String projectName = getProjectByJobId(jobId);
                    return Pair.newPair(projectName, jobStepId);
                } else {
                    throw new KylinException(JobErrorCode.PROFILING_STATUS_ERROR,
                            String.format(Locale.ROOT, MsgPicker.getMsg().getProfilingJobFinishedError()));
                }
            } else {
                throw new KylinException(JobErrorCode.PROFILING_STATUS_ERROR,
                        String.format(Locale.ROOT, MsgPicker.getMsg().getProfilingYarnAppIdError()));
            }
        } else {
            throw new KylinException(JobErrorCode.PROFILING_STATUS_ERROR,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getProfilingYarnAppIdError()));
        }
    }

    public void setResponseLanguage(HttpServletRequest request) {
        aclEvaluate.checkIsGlobalAdmin();
        String languageToHandle = request.getHeader(HttpHeaders.ACCEPT_LANGUAGE);
        if (languageToHandle == null) {
            ErrorCode.setMsg("cn");
            MsgPicker.setMsg("cn");
            return;
        }
        // The user's browser may contain multiple language preferences, such as xx,xx;ss,ss
        String language = StringHelper.dropFirstSuffix(StringHelper.dropFirstSuffix(languageToHandle, ";"), ",");
        if (CHINESE_LANGUAGE.equals(language) || CHINESE_SIMPLE_LANGUAGE.equals(language)
                || CHINESE_HK_LANGUAGE.equals(language) || CHINESE_TW_LANGUAGE.equals(language)) {
            ErrorCode.setMsg("cn");
            MsgPicker.setMsg("cn");
        } else {
            ErrorCode.setMsg("en");
            MsgPicker.setMsg("en");
        }
    }

    @Override
    public void onApplicationEvent(ApplicationEvent event) {
        if (event instanceof ContextClosedEvent) {
            try (SetLogCategory ignored = new SetLogCategory(LogConstant.BUILD_CATEGORY)) {
                logger.info("Stop Kylin node, kill job on yarn for yarn cluster mode");
            }
            EpochManager epochManager = EpochManager.getInstance();
            KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            List<Epoch> ownedEpochs = epochManager.getOwnedEpochs();

            for (Epoch epoch : ownedEpochs) {
                String project = epoch.getEpochTarget();
                NExecutableManager executableManager = NExecutableManager.getInstance(kylinConfig, project);
                if (executableManager != null) {
                    List<ExecutablePO> allJobs = executableManager.getAllJobs();
                    for (ExecutablePO executablePO : allJobs) {
                        executableManager.cancelRemoteJob(executablePO);
                    }
                }
            }
        }
    }

    @Override
    public int getOrder() {
        return HIGHEST_PRECEDENCE;
    }

    @Setter
    @Getter
    static class ExecutablePOSortBean {

        private String project;

        private String id;

        @JsonProperty("job_name")
        private String jobName;

        @JsonProperty("last_modified")
        private long lastModified;

        @JsonProperty("target_subject")
        private String targetSubject;

        @JsonProperty("create_time")
        private long createTime;

        @JsonProperty("total_duration")
        private long totalDuration;

        @JsonProperty("duration")
        private long duration;

        private ExecutablePO executablePO;

        private long computeTotalDuration(ExecutablePO executablePO) {
            List<ExecutablePO> tasks = executablePO.getTasks();
            if (CollectionUtils.isEmpty(tasks)) {
                return 0L;
            }

            long taskCreateTime = executablePO.getOutput().getCreateTime();
            ExecutableState state = ExecutableState.valueOf(executablePO.getOutput().getStatus());
            if (state.isProgressing()) {
                return System.currentTimeMillis() - taskCreateTime;
            }
            long jobDuration = 0L;
            for (ExecutablePO subTask : tasks) {
                if (subTask.getOutput().getStartTime() == 0L) {
                    break;
                }
                jobDuration = getExecutablePOEndTime(subTask) - taskCreateTime;
            }
            return jobDuration == 0L ? getExecutablePOEndTime(executablePO) - taskCreateTime : jobDuration;
        }

        private long getExecutablePOEndTime(ExecutablePO executablePO) {
            long time = executablePO.getOutput().getEndTime();
            return time == 0L ? System.currentTimeMillis() : time;
        }

        public long getTaskDuration(ExecutablePO subTask) {
            ExecutableOutputPO output = subTask.getOutput();
            if (output.getDuration() != 0) {
                var taskDuration = output.getDuration();
                if (ExecutableState.RUNNING == ExecutableState.valueOf(output.getStatus())) {
                    taskDuration = (taskDuration + System.currentTimeMillis() - output.getLastRunningStartTime());
                }
                return taskDuration;
            }
            if (output.getStartTime() == 0) {
                return 0;
            }
            return (output.getEndTime() == 0 ? System.currentTimeMillis() - output.getStartTime()
                    : output.getEndTime() - output.getStartTime());
        }

        public long getDurationByPO(ExecutablePO executablePO) {
            long jobDuration = getTaskDuration(executablePO);
            List<ExecutablePO> tasks = executablePO.getTasks();
            if (tasks != null && CollectionUtils.isNotEmpty(tasks)) {
                jobDuration = 0;
                for (ExecutablePO subTask : tasks) {
                    long taskDuration = getTaskDuration(subTask);
                    if (MapUtils.isNotEmpty(subTask.getStagesMap()) && subTask.getStagesMap().size() == 1) {
                        val jobAtomicDuration = new AtomicLong(0);
                        for (Map.Entry<String, List<ExecutablePO>> entry : subTask.getStagesMap().entrySet()) {
                            entry.getValue().stream().forEach(po -> jobAtomicDuration.addAndGet(getTaskDuration(po)));
                        }
                        taskDuration = jobAtomicDuration.get();
                    }
                    jobDuration += taskDuration;
                }
            }
            return jobDuration;
        }

        public String getTargetModelAlias(ExecutablePO executablePO, NDataModelManager modelManager) {
            if (executablePO == null || executablePO.getJobType() == null) {
                return null;
            }
            switch (executablePO.getJobType()) {
            case TABLE_SAMPLING:
                return executablePO.getTargetModel();
            case SNAPSHOT_REFRESH:
            case SNAPSHOT_BUILD:
                var state = ExecutableState.valueOf(executablePO.getOutput().getStatus());
                if (state == ExecutableState.SUCCEED || state == ExecutableState.DISCARDED
                        || state == ExecutableState.SUICIDAL) {
                    TableDesc tableDesc = NTableMetadataManager
                            .getInstance(KylinConfig.getInstanceFromEnv(), executablePO.getProject())
                            .getTableDesc(executablePO.getTargetModel());
                    if (tableDesc == null || tableDesc.getLastSnapshotPath() == null) {
                        return null;
                    }
                }
                return executablePO.getTargetModel();
            case SECOND_STORAGE_NODE_CLEAN:
                return executablePO.getProject();
            default:
                NDataModel dataModelDesc = modelManager.getDataModelDesc(executablePO.getTargetModel());
                if (dataModelDesc != null) {
                    if (modelManager.isModelBroken(executablePO.getTargetModel())) {
                        return modelManager.getDataModelDescWithoutInit(executablePO.getTargetModel()).getAlias();
                    } else {
                        return dataModelDesc.getFusionModelAlias();
                    }
                }
                return null;
            }
        }
    }
}

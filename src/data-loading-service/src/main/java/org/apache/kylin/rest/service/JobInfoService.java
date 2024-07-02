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

import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_ACTION_ILLEGAL;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_NOT_EXIST;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_STATUS_ILLEGAL;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_UPDATE_STATUS_FAILED;
import static org.apache.kylin.query.util.AsyncQueryUtil.ASYNC_QUERY_JOB_ID_PRE;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
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
import org.apache.kylin.common.mail.MailNotificationType;
import org.apache.kylin.common.metrics.MetricsCategory;
import org.apache.kylin.common.metrics.MetricsGroup;
import org.apache.kylin.common.metrics.MetricsName;
import org.apache.kylin.common.scheduler.EventBusFactory;
import org.apache.kylin.common.scheduler.JobDiscardNotifier;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.engine.spark.job.NSparkExecutable;
import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.job.common.ShellExecutable;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.constant.JobActionEnum;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.constant.JobStatusUtil;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.dao.JobInfoDao;
import org.apache.kylin.job.domain.JobInfo;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ChainedExecutable;
import org.apache.kylin.job.execution.ChainedStageExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobSchedulerModeEnum;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.Output;
import org.apache.kylin.job.execution.StageExecutable;
import org.apache.kylin.job.rest.JobFilter;
import org.apache.kylin.job.rest.JobMapperFilter;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.job.util.JobInfoUtil;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.FusionModel;
import org.apache.kylin.metadata.model.FusionModelManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.JobUpdateRequest;
import org.apache.kylin.rest.request.SparkJobUpdateRequest;
import org.apache.kylin.rest.response.ExecutableResponse;
import org.apache.kylin.rest.response.ExecutableStepResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.JobDriverUIUtil;
import org.apache.kylin.rest.util.JobFilterUtil;
import org.apache.kylin.rest.util.SparkHistoryUIUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import lombok.SneakyThrows;
import lombok.val;
import lombok.var;

@Service("jobInfoService")
public class JobInfoService extends BasicService implements JobSupporter {

    private static final Logger logger = LoggerFactory.getLogger(LogConstant.BUILD_CATEGORY);

    private static final String PARSE_ERROR_MSG = "Error parsing the executablePO: ";
    public static final String EXCEPTION_CODE_PATH = "exception_to_code.json";
    public static final String EXCEPTION_CODE_DEFAULT = "KE-030001000";

    private static final Map<String, String> JOB_TYPE_MAP = Maps.newHashMap();

    static {
        JOB_TYPE_MAP.put("INDEX_REFRESH", "Refresh Data");
        JOB_TYPE_MAP.put("INDEX_MERGE", "Merge Data");
        JOB_TYPE_MAP.put("INDEX_BUILD", "Build Index");
        JOB_TYPE_MAP.put("INC_BUILD", "Load Data");
        JOB_TYPE_MAP.put("TABLE_SAMPLING", "Sample Table");
    }

    @Autowired
    private ProjectService projectService;

    @Autowired
    private JobInfoDao jobInfoDao;

    private AclEvaluate aclEvaluate;

    @Autowired
    private ModelService modelService;

    @Autowired
    private TableExtService tableExtService;

    @Autowired
    public JobInfoService setAclEvaluate(AclEvaluate aclEvaluate) {
        this.aclEvaluate = aclEvaluate;
        return this;
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

    public List<JobStatusEnum> parseJobStatus(List<String> strValues) {
        return strValues.stream().map(strValue -> JobStatusEnum.valueOf(strValue)).collect(Collectors.toList());
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

    /**
     * for 3x api
     *
     * @param jobId
     * @return
     */
    public ExecutableResponse getJobInstance(String jobId) {
        Preconditions.checkNotNull(jobId);
        ExecutablePO executablePO = jobInfoDao.getExecutablePOByUuid(jobId);
        if (executablePO == null) {
            throw new KylinException(JOB_NOT_EXIST, jobId);
        }
        ExecutableManager executableManager = getManager(ExecutableManager.class, executablePO.getProject());
        AbstractExecutable executable = executableManager.fromPO(executablePO);
        return convert(executable, executablePO);
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
            ExecutableManager executableManager = getManager(ExecutableManager.class, projectInstance.getName());
            if (Objects.nonNull(executableManager.getJob(jobId))) {
                return projectInstance.getName();
            }
        }
        return null;
    }

    @VisibleForTesting
    public List<ProjectInstance> getReadableProjects() {
        return projectService.getReadableProjects(null, false);
    }

    public List<ExecutableResponse> listJobs(final JobFilter jobFilter) {
        return listJobs(jobFilter, -1, -1);
    }

    // TODO model == null || !model.isFusionModel();
    public List<ExecutableResponse> listJobs(final JobFilter jobFilter, int offset, int limit) {
        // TODO check permission when 'project' is empty
        if (StringUtils.isNotEmpty(jobFilter.getProject())) {
            aclEvaluate.checkProjectOperationPermission(jobFilter.getProject());
        }
        JobMapperFilter jobMapperFilter = JobFilterUtil.getJobMapperFilter(jobFilter, offset, limit, modelService,
                tableExtService, projectService);
        List<JobInfo> jobInfoList = jobInfoDao.getJobInfoListByFilter(jobMapperFilter);
        List<ExecutableResponse> result = jobInfoList.stream().map(JobInfoUtil::deserializeExecutablePO)
                .map(executablePO -> {
                    AbstractExecutable executable = getManager(ExecutableManager.class, executablePO.getProject())
                            .fromPO(executablePO);
                    val convert = this.convert(executable, executablePO);
                    return convert;
                }).collect(Collectors.toList());
        sortByDurationIfNeed(result, jobFilter.getSortBy(), jobMapperFilter.getOrderType());
        return result;
    }

    public void sortByDurationIfNeed(List<ExecutableResponse> list, final String orderByField, final String orderType) {
        // when job is running, System.currentTimeMillis goto compute duration, and this need sort by real-time
        // job_duration_millis is metadata db column, means duration
        Comparator<ExecutableResponse> comparator = null;
        if ("duration".equalsIgnoreCase(orderByField)) {
            comparator = (o1, o2) -> (int) ("ASC".equalsIgnoreCase(orderType) ? o1.getDuration() - o2.getDuration()
                    : o2.getDuration() - o1.getDuration());
        } else if ("total_duration".equalsIgnoreCase(orderByField)) {
            comparator = (o1,
                    o2) -> (int) ("ASC".equalsIgnoreCase(orderType) ? o1.getTotalDuration() - o2.getTotalDuration()
                            : o2.getTotalDuration() - o1.getTotalDuration());
        } else {
            return;
        }

        list.sort(comparator);
    }

    public long countJobs(final JobFilter jobFilter) {
        // TODO check permission when 'project' is empty
        if (StringUtils.isNotEmpty(jobFilter.getProject())) {
            aclEvaluate.checkProjectOperationPermission(jobFilter.getProject());
        }
        JobMapperFilter jobMapperFilter = JobFilterUtil.getJobMapperFilter(jobFilter, 0, 0, modelService,
                tableExtService, projectService);
        return jobInfoDao.countByFilter(jobMapperFilter);
    }

    public List<ExecutableStepResponse> getJobDetail(String project, String jobId) {
        aclEvaluate.checkProjectOperationPermission(project);
        ExecutablePO executablePO = jobInfoDao.getExecutablePOByUuid(jobId);
        if (executablePO == null) {
            throw new KylinException(JOB_NOT_EXIST, jobId);
        }
        AbstractExecutable executable = null;
        ExecutableManager executableManager = getManager(ExecutableManager.class, project);
        try {
            executable = executableManager.fromPO(executablePO);
        } catch (Exception e) {
            logger.error(PARSE_ERROR_MSG, e);
            return Collections.emptyList();
        }

        // waite time in output
        Map<String, String> waiteTimeMap;
        val output = executable.getOutput(executablePO);
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
            final ExecutableStepResponse executableStepResponse = parseToExecutableStep(task, executablePO,
                    waiteTimeMap, output.getState());
            if (task.getStatusInMem() == ExecutableState.ERROR) {
                executableStepResponse.setFailedStepId(output.getFailedStepId());
                executableStepResponse.setFailedSegmentId(output.getFailedSegmentId());
                executableStepResponse.setFailedStack(output.getFailedStack());
                executableStepResponse.setFailedStepName(task.getName());

                setExceptionResolveAndCodeAndReason(output, executableStepResponse);
            }
            if (executable.getJobSchedulerMode() == JobSchedulerModeEnum.DAG
                    && task.getStatus() == ExecutableState.ERROR
                    && !org.apache.commons.lang3.StringUtils.startsWith(output.getFailedStepId(), task.getId())) {
                executableStepResponse.setStatus(JobStatusEnum.STOPPED);
            }
            if (task instanceof ChainedStageExecutable) {
                Map<String, List<StageExecutable>> stagesMap = Optional
                        .ofNullable(((ChainedStageExecutable) task).getStagesMap()).orElse(Maps.newHashMap());

                Map<String, ExecutableStepResponse.SubStages> stringSubStageMap = Maps.newHashMap();
                List<ExecutableStepResponse> subStages = Lists.newArrayList();

                for (Map.Entry<String, List<StageExecutable>> entry : stagesMap.entrySet()) {
                    String segmentId = entry.getKey();
                    ExecutableStepResponse.SubStages segmentSubStages = new ExecutableStepResponse.SubStages();

                    List<StageExecutable> stageExecutables = Optional.ofNullable(entry.getValue())
                            .orElse(Lists.newArrayList());
                    List<ExecutableStepResponse> stageResponses = Lists.newArrayList();
                    for (StageExecutable stage : stageExecutables) {
                        val stageResponse = parseStageToExecutableStep(task, stage,
                                executableManager.getOutput(stage.getId(), segmentId));
                        if (executable.getJobSchedulerMode() == JobSchedulerModeEnum.DAG
                                && stage.getStatus(segmentId) == ExecutableState.ERROR
                                && !org.apache.commons.lang3.StringUtils.startsWith(output.getFailedStepId(),
                                        stage.getId())) {
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
                        setSegmentSubStageParams(project, targetSubject, task, segmentId, segmentSubStages,
                                stageExecutables,
                                stageResponses, waiteTimeMap, output.getState(), executablePO);
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
        if (executable.getStatusInMem() == ExecutableState.DISCARDED) {
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

    public void batchDropJob(String project, List<String> jobIds, List<String> filterStatuses) {
        aclEvaluate.checkProjectOperationPermission(project);
        val jobs = getJobsByStatus(project, jobIds, filterStatuses);
        batchDropJob0(project, jobs);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#ae, 'ADMINISTRATION')")
    public void batchDropGlobalJob(List<String> jobIds, List<String> filterStatuses) {
        val jobs = getJobsByStatus(null, jobIds, filterStatuses);
        Map<String, List<AbstractExecutable>> projectJobMap = new HashMap<>();
        for (AbstractExecutable job : jobs) {
            String project = job.getProject();
            if (!projectJobMap.containsKey(project)) {
                projectJobMap.put(project, Lists.newArrayList());
            }
            projectJobMap.get(project).add(job);
        }
        projectJobMap.entrySet().stream().forEach(entry -> {
            String project = entry.getKey();
            aclEvaluate.checkProjectOperationPermission(project);
            batchDropJob0(project, entry.getValue());
        });
    }

    private void batchDropJob0(String project, List<AbstractExecutable> jobs) {
        ExecutableManager executableManager = getManager(ExecutableManager.class, project);
        List<String> jobIdsToBeDelete = Lists.newArrayList();
        jobs.forEach(job -> {
            executableManager.checkJobCanBeDeleted(job);
            jobIdsToBeDelete.add(job.getJobId());
        });
        JobContextUtil.withTxAndRetry(() -> {
            executableManager.deleteJobByIdList(jobIdsToBeDelete);
            return true;
        });
    }

    public void batchUpdateJobStatus(List<String> jobIds, String project, String action, List<String> filterStatuses)
            throws IOException {
        List<ExecutableState> filterStates = JobStatusUtil.mapJobStatusToScheduleState(filterStatuses);
        val executablePos = jobInfoDao.getExecutablePoByStatus(project, jobIds, filterStates);
        if (null == project) {
            executablePos
                    .forEach(executablePO -> aclEvaluate.checkProjectOperationPermission(executablePO.getProject()));
        } else {
            aclEvaluate.checkProjectOperationPermission(project);
        }
        for (ExecutablePO executablePO : executablePos) {
            updateJobStatus(executablePO.getId(), executablePO, executablePO.getProject(), action);
        }
    }

    @Transactional
    public ExecutableResponse manageJob(String project, ExecutableResponse job, String action) throws IOException {
        Preconditions.checkNotNull(project);
        Preconditions.checkNotNull(job);
        Preconditions.checkArgument(!StringUtils.isBlank(action));

        if (JobActionEnum.DISCARD == JobActionEnum.valueOf(action)) {
            return job;
        }
        ExecutablePO executablePO = jobInfoDao.getExecutablePOByUuid(job.getId());
        updateJobStatus(job.getId(), executablePO, project, action);
        return getJobInstance(job.getId());
    }
    private void jobActionValidate(String action) {
        JobActionEnum.validateValue(action.toUpperCase(Locale.ROOT));
    }

    @VisibleForTesting
    public void jobActionValidateToTest(String action) {
        jobActionValidate(action);
    }

    @VisibleForTesting
    public void updateJobStatus(String jobId, ExecutablePO executablePO, String project, String action)
            throws IOException {
        val executableManager = getManager(ExecutableManager.class, project);
        AbstractExecutable executable = executableManager.fromPO(executablePO);
        jobActionValidate(action);
        switch (JobActionEnum.valueOf(action.toUpperCase(Locale.ROOT))) {
        case RESUME:
            executableManager.resumeJob(jobId);
            MetricsGroup.hostTagCounterInc(MetricsName.JOB_RESUMED, MetricsCategory.PROJECT, project);
            break;
        case RESTART:
            killExistApplication(executable);
            executableManager.restartJob(jobId);
            break;
        case DISCARD:
            discardJob(project, jobId, executable);
            JobTypeEnum jobTypeEnum = executableManager.getJob(jobId).getJobType();
            String jobType = jobTypeEnum == null ? "" : jobTypeEnum.name();
            EventBusFactory.getInstance().postAsync(new JobDiscardNotifier(project, jobType));
            break;
        case PAUSE:
            executableManager.pauseJob(jobId);
            killExistApplication(executable);
            break;
        default:
            throw new IllegalStateException("This job can not do this action: " + action);
        }
    }

    public void updateJobError(String project, String jobId, String failedStepId, String failedSegmentId,
            String failedStack, String failedReason) {
        if (StringUtils.isBlank(failedStepId)) {
            return;
        }
        JobContextUtil.withTxAndRetry(() -> {
            val executableManager = getManager(ExecutableManager.class, project);
            executableManager.updateJobError(jobId, failedStepId, failedSegmentId, failedStack, failedReason);
            return true;
        });
    }

    public void updateStageStatus(String project, String taskId, String segmentId, String status,
            Map<String, String> updateInfo, String errMsg) {
        final ExecutableState newStatus = convertToExecutableState(status);
        val executableManager = getManager(ExecutableManager.class, project);
        executableManager.updateStageStatus(taskId, segmentId, newStatus, updateInfo, errMsg);
    }

    public ExecutableState convertToExecutableState(String status) {
        if (StringUtils.isBlank(status)) {
            return null;
        }
        return ExecutableState.valueOf(status);
    }

    /**
     * update the spark job info, such as yarnAppId, yarnAppUrl.
     *
     */
    public void updateSparkJobInfo(SparkJobUpdateRequest request) {
        if (request.getJobId().contains(ASYNC_QUERY_JOB_ID_PRE)) {
            return;
        }
        val executableManager = getManager(ExecutableManager.class, request.getProject());
        Map<String, String> extraInfo = Maps.newHashMap();
        extraInfo.put(ExecutableConstants.YARN_APP_ID, request.getYarnAppId());
        extraInfo.put(ExecutableConstants.YARN_APP_URL, request.getYarnAppUrl());
        extraInfo.put(ExecutableConstants.QUEUE_NAME, request.getQueueName());
        extraInfo.put(ExecutableConstants.CORES, request.getCores());
        extraInfo.put(ExecutableConstants.MEMORY, request.getMemory());

        JobContextUtil.withTxAndRetry(() -> {
            executableManager.updateJobOutput(request.getTaskId(), null, extraInfo, null, null);
            return true;
        });
    }

    public void updateSparkTimeInfo(String project, String jobId, String taskId, String waitTime, String buildTime) {
        val executableManager = getManager(ExecutableManager.class, project);
        Map<String, String> extraInfo = Maps.newHashMap();
        extraInfo.put(ExecutableConstants.YARN_JOB_WAIT_TIME, waitTime);
        extraInfo.put(ExecutableConstants.YARN_JOB_RUN_TIME, buildTime);

        if (jobId.contains(ASYNC_QUERY_JOB_ID_PRE)) {
            return;
        }

        JobContextUtil.withTxAndRetry(() -> {
            executableManager.updateJobOutput(taskId, null, extraInfo, null, null);
            return true;
        });
    }

    public String getJobOutput(String project, String jobId) {
        return getJobOutput(project, jobId, jobId);
    }

    public String getJobOutput(String project, String jobId, String stepId) {
        aclEvaluate.checkProjectOperationPermission(project);
        val executableManager = getManager(ExecutableManager.class, project);
        return executableManager.getOutputFromHDFSByJobId(jobId, stepId).getVerboseMsg();
    }

    public void killExistApplication(String project, String jobId) {
        AbstractExecutable job = getManager(ExecutableManager.class, project).getJob(jobId);
        killExistApplication(job);
    }

    public void killExistApplication(AbstractExecutable job) {
        if (job instanceof ChainedExecutable) {
            // if job's task is running spark job, will kill this application
            ((ChainedExecutable) job).getTasks().stream() //
                    .filter(task -> task.getStatusInMem() == ExecutableState.RUNNING) //
                    .filter(task -> task instanceof NSparkExecutable) //
                    .forEach(task -> ((NSparkExecutable) task).killOrphanApplicationIfExists(task.getId()));
        }
    }

    private void discardJob(String project, String jobId, AbstractExecutable job) {
        if (ExecutableState.SUCCEED == job.getStatusInMem()) {
            throw new KylinException(JOB_UPDATE_STATUS_FAILED, "DISCARD", jobId, job.getStatusInMem());
        }
        if (ExecutableState.DISCARDED == job.getStatusInMem()) {
            return;
        }
        killExistApplication(job);
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            getManager(ExecutableManager.class, project).discardJob(job.getId());
            return true;
        }, project);

        if (getConfig().isMailEnabled()) {
            job.notifyUser(MailNotificationType.JOB_DISCARDED);
        }
    }

    private List<AbstractExecutable> getJobsByStatus(String project, List<String> jobIds, List<String> filterStatuses) {
        List<ExecutableState> filterStates = JobStatusUtil.mapJobStatusToScheduleState(filterStatuses);
        return jobInfoDao.getExecutablePoByStatus(project, jobIds, filterStates).stream().map(
                executablePO -> getManager(ExecutableManager.class, executablePO.getProject()).fromPO(executablePO))
                .collect(Collectors.toList());
    }

    @VisibleForTesting
    public ExecutableResponse convert(AbstractExecutable executable, ExecutablePO executablePO) {
        ExecutableResponse executableResponse = ExecutableResponse.create(executable, executablePO);
        executableResponse.setSchedulerState(executable.getStatusInMem());
        executableResponse.setStatus(executable.getStatusInMem().toJobStatus());
        return executableResponse;
    }

    @VisibleForTesting
    public ExecutableStepResponse parseToExecutableStep(AbstractExecutable task, ExecutablePO po,
            Map<String, String> waiteTimeMap, ExecutableState jobState) {
        ExecutableStepResponse result = new ExecutableStepResponse();
        result.setId(task.getId());
        result.setName(task.getName());
        result.setSequenceID(task.getStepId());

        ExecutableManager executableManager = getManager(ExecutableManager.class, task.getProject());
        Output stepOutput = executableManager.getOutput(task.getId(), po);

        if (stepOutput == null) {
            try (SetLogCategory ignored = new SetLogCategory(LogConstant.BUILD_CATEGORY)) {
                logger.warn("Cannot found output for task: id={}", task.getId());
            }
            return result;
        }
        result.setSchedulerState(stepOutput.getState());
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

        if (result.getInfo().containsKey(ExecutableConstants.YARN_APP_URL)) {
            result.putInfo(ExecutableConstants.PROXY_APP_URL,
                    JobDriverUIUtil.getProxyUrl(task.getProject(), task.getId()));
            NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
            ProjectInstance prjInstance = projectManager.getProject(task.getProject());
            if (prjInstance != null && prjInstance.getConfig().isProxyJobSparkUIEnabled()) {
                result.putInfo(ExecutableConstants.YARN_APP_URL,
                        JobDriverUIUtil.getProxyUrl(task.getProject(), task.getId()));
            }
        }

        result.setExecStartTime(AbstractExecutable.getStartTime(stepOutput));
        result.setExecEndTime(AbstractExecutable.getEndTime(stepOutput));
        result.setCreateTime(AbstractExecutable.getCreateTime(stepOutput));

        result.setDuration(AbstractExecutable.getDuration(stepOutput));
        // if resume job, need sum of waite time
        long waiteTime = Long.parseLong(waiteTimeMap.getOrDefault(task.getId(), "0"));
        if (jobState != ExecutableState.PAUSED) {
            val taskWaitTime = task.getWaitTime(po);
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

    private ExecutableStepResponse parseStageToExecutableStep(AbstractExecutable task, StageExecutable stageExecutable,
            Output stageOutput) {
        ExecutableStepResponse result = new ExecutableStepResponse();
        result.setId(stageExecutable.getId());
        result.setName(stageExecutable.getName());
        result.setSequenceID(stageExecutable.getStepId());

        if (stageOutput == null) {
            try (SetLogCategory ignored = new SetLogCategory(LogConstant.BUILD_CATEGORY)) {
                logger.warn("Cannot found output for task: id={}", stageExecutable.getId());
            }
            return result;
        }
        for (Map.Entry<String, String> entry : stageOutput.getExtra().entrySet()) {
            if (entry.getKey() != null && entry.getValue() != null) {
                result.putInfo(entry.getKey(), entry.getValue());
            }
        }
        result.setSchedulerState(stageOutput.getState());
        result.setStatus(stageOutput.getState().toJobStatus());
        result.setExecStartTime(AbstractExecutable.getStartTime(stageOutput));
        result.setExecEndTime(AbstractExecutable.getEndTime(stageOutput));
        result.setCreateTime(AbstractExecutable.getCreateTime(stageOutput));

        result.setDuration(AbstractExecutable.getStageDuration(stageOutput, task.getParent()));

        String indexCount = Optional.ofNullable(task.getParam(NBatchConstants.P_INDEX_COUNT)).orElse("0");
        result.setIndexCount(Long.parseLong(indexCount));
        if (result.getStatus() == JobStatusEnum.FINISHED) {
            result.setSuccessIndexCount(Long.parseLong(indexCount));
        } else {
            val successIndexCount = stageOutput.getExtra().getOrDefault(NBatchConstants.P_INDEX_SUCCESS_COUNT, "0");
            result.setSuccessIndexCount(Long.parseLong(successIndexCount));
        }
        return result;
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
            Set<JobStatusEnum> jobFinishOrSkip = Sets.newHashSet(JobStatusEnum.FINISHED, JobStatusEnum.SKIP,
                    JobStatusEnum.WARNING);
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

    private void setSegmentSubStageParams(String project, String targetSubject, AbstractExecutable task,
            String segmentId, ExecutableStepResponse.SubStages segmentSubStages, List<StageExecutable> stageExecutables,
            List<ExecutableStepResponse> stageResponses, Map<String, String> waiteTimeMap, ExecutableState jobState,
            ExecutablePO executablePO) {
        segmentSubStages.setStage(stageResponses);

        // when job restart, taskStartTime is zero
        if (CollectionUtils.isNotEmpty(stageResponses)) {
            val taskStartTime = task.getOutput(executablePO).getStartTime();
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

        // If this segment has running stage, this segment is running,
        // this segment doesn't have end time
        // If this task is running and this segment has pending stage,
        // this segment is running, this segment doesn't have end time
        val stageStatuses = stageResponses.stream().map(ExecutableStepResponse::getStatus).collect(Collectors.toSet());
        if (!stageStatuses.contains(JobStatusEnum.RUNNING) && !(task.getStatusInMem() == ExecutableState.RUNNING
                && stageStatuses.contains(JobStatusEnum.PENDING))) {
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
        val stepRatio = (float) ExecutableResponse.calculateSuccessStage(task, segmentId, stageExecutables, true,
                executablePO) / stepCount;
        segmentSubStages.setStepRatio(stepRatio);

        // Put warning message into segment_sub_stages.info if exists
        Optional<ExecutableStepResponse> warningStageRes = stageResponses.stream()
                .filter(stageRes -> stageRes.getStatus() == JobStatusEnum.WARNING).findFirst();
        warningStageRes.ifPresent(res -> segmentSubStages.getInfo().put(NBatchConstants.P_WARNING_CODE,
                res.getInfo().getOrDefault(NBatchConstants.P_WARNING_CODE, null)));
    }

    @SneakyThrows
    public InputStream getAllJobOutput(String project, String jobId, String stepId) {
        aclEvaluate.checkProjectOperationPermission(project);
        val executableManager = getManager(ExecutableManager.class, project);
        val output = executableManager.getOutputFromHDFSByJobId(jobId, stepId, Integer.MAX_VALUE);
        return Optional.ofNullable(output.getVerboseMsgStream()).orElse(
                IOUtils.toInputStream(Optional.ofNullable(output.getVerboseMsg()).orElse(StringUtils.EMPTY), "UTF-8"));
    }

    public List<ExecutableResponse> addOldParams(List<ExecutableResponse> executableResponseList) {
        executableResponseList.forEach(executableResponse -> {
            ExecutableResponse.OldParams oldParams = new ExecutableResponse.OldParams();
            NDataModel nDataModel = getManager(NDataModelManager.class, executableResponse.getProject())
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
            oldParams.setType(JOB_TYPE_MAP.get(executableResponse.getJobName()));
            oldParams.setName(executableResponse.getJobName());
            oldParams.setExecInterruptTime(0L);
            oldParams.setMrWaiting(executableResponse.getWaitTime());

            executableResponse.setOldParams(oldParams);
            executableResponse.setSteps(stepResponseList);
        });

        return executableResponseList;
    }

    public List<String> getFusionModelsByTableDesc(String project, TableDesc tableDesc) {
        List<String> fusionModelIds = Lists.newArrayList();
        for (NDataModel tableRelatedModel : getManager(NDataflowManager.class, project)
                .getModelsUsingTable(tableDesc)) {
            String modelId = tableRelatedModel.getId();
            NDataModel model = getManager(NDataModelManager.class, project).getDataModelDesc(modelId);
            FusionModelManager fusionModelManager = FusionModelManager.getInstance(KylinConfig.getInstanceFromEnv(),
                    project);
            FusionModel fusionModel = fusionModelManager.getFusionModel(modelId);
            if (model.isFusionModel() && Objects.nonNull(fusionModel)) {
                fusionModelIds.add(modelId);
            }
            try (SetLogCategory ignored = new SetLogCategory(LogConstant.BUILD_CATEGORY)) {
                logger.warn("model is not fusion model or fusion model is null, {}", modelId);
            }
        }
        return fusionModelIds;
    }

    public List<String> getBatchModelJobIdsOfFusionModel(String project, List<String> fusionModelIds) {
        List<String> batchModelIds = Lists.newArrayList();
        for (String fusionModelId : fusionModelIds) {
            FusionModelManager fusionModelManager = FusionModelManager.getInstance(KylinConfig.getInstanceFromEnv(),
                    project);
            FusionModel fusionModel = fusionModelManager.getFusionModel(fusionModelId);
            if (Objects.nonNull(fusionModel)) {
                batchModelIds.add(fusionModel.getBatchModel().getUuid());
            }
        }

        if (batchModelIds.isEmpty()) {
            return Lists.newArrayList();
        }

        JobMapperFilter jobMapperFilter = new JobMapperFilter();
        jobMapperFilter.setProject(project);
        jobMapperFilter.setModelIds(batchModelIds);
        List<ExecutableState> ignoreStates = Lists.newArrayList(ExecutableState.SUCCEED, ExecutableState.ERROR,
                ExecutableState.DISCARDED, ExecutableState.SUICIDAL);
        List<ExecutableState> states = Arrays.stream(ExecutableState.values())
                .filter(state -> !ignoreStates.contains(state)).collect(Collectors.toList());
        jobMapperFilter.setStatuses(states);
        List<JobInfo> jobInfoList = jobInfoDao.getJobInfoListByFilter(jobMapperFilter);
        return jobInfoList.stream().map(jobInfo -> jobInfo.getJobId()).collect(Collectors.toList());
    }

    public void stopBatchJob(String project, TableDesc tableDesc) {
        List<String> fusionModelIds = getFusionModelsByTableDesc(project, tableDesc);
        List<String> jobIdList = getBatchModelJobIdsOfFusionModel(project, fusionModelIds);
        JobContextUtil.remoteDiscardJob(project, jobIdList);
    }

    public String getOriginTrackUrlByProjectAndStepId(String project, String stepId) {
        String trackUrl = null;
        try {
            ExecutableManager executableManager = getManager(ExecutableManager.class, project);
            Output stepOutput = executableManager.getOutput(stepId);
            trackUrl = stepOutput.getExtra().get(ExecutableConstants.YARN_APP_URL);
        } catch (Exception e) {
            logger.warn("get trackUrl failed", e);
        }
        return trackUrl;
    }
}

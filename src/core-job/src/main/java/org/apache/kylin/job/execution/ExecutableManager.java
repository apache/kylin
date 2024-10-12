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

import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_DOWNLOAD_FILE;
import static org.apache.kylin.common.exception.ServerErrorCode.FILE_NOT_EXIST;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_STATE_TRANSFER_ILLEGAL;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_UPDATE_STATUS_FAILED;
import static org.apache.kylin.job.constant.ExecutableConstants.YARN_APP_IDS;
import static org.apache.kylin.job.constant.ExecutableConstants.YARN_APP_IDS_DELIMITER;
import static org.apache.kylin.job.execution.AbstractExecutable.RUNTIME_INFO;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.SequenceInputStream;
import java.lang.reflect.Constructor;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.scheduler.EventBusFactory;
import org.apache.kylin.common.scheduler.JobAddedNotifier;
import org.apache.kylin.common.scheduler.JobReadyNotifier;
import org.apache.kylin.common.util.AddressUtil;
import org.apache.kylin.common.util.Array;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.ShellException;
import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.job.JobContext;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.dao.ExecutableOutputPO;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.dao.JobInfoDao;
import org.apache.kylin.job.domain.JobInfo;
import org.apache.kylin.job.factory.JobFactory;
import org.apache.kylin.job.rest.JobMapperFilter;
import org.apache.kylin.job.runners.JobCheckUtil;
import org.apache.kylin.job.scheduler.JdbcJobScheduler;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.job.util.JobInfoUtil;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ExecutableManager {

    private static final Logger logger = LoggerFactory.getLogger(ExecutableManager.class);
    /** Dummy value to reflection */
    private static final Object DUMMY_OBJECT = new Object();
    private static final String PARSE_ERROR_MSG = "Error parsing the executablePO: ";
    private static final int LOG_DEFAULT_DISPLAY_HEAD_AND_TAIL_SIZE = 100;
    private static final String SHOULD_DESTROY_PROCESS_SIGNAL = "SHOULD_DESTROY_PROCESS_SIGNAL";
    private final KylinConfig config;
    private String project;

    private JobInfoDao jobInfoDao;

    private static final Set<String> REMOVE_INFO = Sets.newHashSet(ExecutableConstants.YARN_APP_ID,
            ExecutableConstants.YARN_APP_URL, ExecutableConstants.YARN_JOB_WAIT_TIME,
            ExecutableConstants.YARN_JOB_RUN_TIME, ExecutableConstants.QUEUE_NAME, ExecutableConstants.CORES,
            ExecutableConstants.MEMORY);

    public static ExecutableManager getInstance(KylinConfig config, String project) {
        if (null == project) {
            throw new IllegalStateException();
        }
        return config.getManager(project, ExecutableManager.class);
    }

    static ExecutableManager newInstance(KylinConfig config, String project) {
        return new ExecutableManager(config, project);
    }

    private ExecutableManager(KylinConfig config, String project) {
        log.trace("Using metadata url: {}", config);
        this.config = config;
        this.project = project;
        this.jobInfoDao = JobContextUtil.getJobInfoDao(this.config);
    }

    public static ExecutablePO toPO(AbstractExecutable executable, String project) {
        ExecutablePO result = new ExecutablePO();
        result.setProject(project);
        result.setName(executable.getName());
        result.setUuid(executable.getId());
        result.setType(executable.getClass().getName());
        result.setParams(executable.getParams());
        result.setJobType(executable.getJobType());
        result.setTargetModel(executable.getTargetSubject());
        result.setTargetSegments(executable.getTargetSegments());
        result.setTargetPartitions(executable.getTargetPartitions());
        result.getOutput().setResumable(executable.isResumable());
        result.setPriority(executable.getPriority());
        result.setTag(executable.getTag());
        result.setJobSchedulerMode(executable.getJobSchedulerMode());
        result.setPreviousStep(executable.getPreviousStep());
        result.setNextSteps(executable.getNextSteps());
        Map<String, Object> runTimeInfo = executable.getRunTimeInfo();
        if (runTimeInfo != null && runTimeInfo.size() > 0) {
            Set<NDataSegment> segments = (HashSet<NDataSegment>) runTimeInfo.get(RUNTIME_INFO);
            if (segments != null) {
                result.getSegments().addAll(segments);
            }
        }
        if (executable instanceof ChainedExecutable) {
            List<ExecutablePO> tasks = Lists.newArrayList();
            for (AbstractExecutable task : ((ChainedExecutable) executable).getTasks()) {
                tasks.add(toPO(task, project));
            }
            result.setTasks(tasks);
            if (executable instanceof DefaultExecutableOnModel) {
                val handler = ((DefaultExecutableOnModel) executable).getHandler();
                if (handler != null) {
                    result.setHandlerType(handler.getClass().getName());
                }
            }
        }
        if (executable instanceof ChainedStageExecutable) {
            Map<String, List<ExecutablePO>> taskMap = Maps.newHashMap();
            final Map<String, List<StageBase>> tasksMap = Optional
                    .ofNullable(((ChainedStageExecutable) executable).getStagesMap()).orElse(Maps.newHashMap());
            for (Map.Entry<String, List<StageBase>> entry : tasksMap.entrySet()) {
                final List<ExecutablePO> executables = entry.getValue().stream().map(stage -> toPO(stage, project))
                        .collect(Collectors.toList());
                taskMap.put(entry.getKey(), executables);
            }
            if (MapUtils.isNotEmpty(taskMap)) {
                result.setStagesMap(taskMap);
            }
        }
        return result;
    }

    private ExecutablePO copyForWrite(ExecutablePO po) {
        // No need to copy, just return the origin object
        // This will be rewrite after metadata is refactored
        return po;
    }

    public void checkAndSubmitCronJob(String factory, JobTypeEnum jobType) {
        List<JobInfo> processingJobs = fetchJobsByFilter(
                JobMapperFilter.builder().jobNames(Collections.singletonList(jobType.name())).project(project)
                        .statuses(Arrays.asList(ExecutableState.PENDING, ExecutableState.READY, ExecutableState.RUNNING,
                                ExecutableState.PAUSED))
                        .build());
        if (processingJobs.size() > 1) {
            log.warn(
                    "There are too many jobs in pending/ready/running/paused state, skip submitting job:{} for project:{}",
                    jobType, project);
            return;
        } else if (processingJobs.size() == 1) {
            log.info("The last job has not finished, skip submitting job:{} for project:{}", jobType, project);
            return;
        }

        AbstractExecutable cronJob = JobFactory.createJobWithDefaultParams(factory, jobType);
        if (cronJob != null) {
            EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project).addJob(cronJob);
                return null;
            }, project);
        } else {
            log.warn("Cannot create cron job:{} for project:{}", jobType, project);
        }
    }

    // only for test
    public void addJob(AbstractExecutable executable) {
        val po = toPO(executable, project);
        addJob(po);
    }

    public void addJob(ExecutablePO executablePO) {
        ExecutablePO copy = copyForWrite(executablePO);
        addJobOutput(copy);
        jobInfoDao.addJob(copy);

        String jobType = copy.getJobType() == null ? "" : copy.getJobType().name();
        // dispatch job-created message out
        if (KylinConfig.getInstanceFromEnv().isUTEnv()) {
            EventBusFactory.getInstance().postAsync(new JobReadyNotifier(project));
            EventBusFactory.getInstance().postAsync(new JobAddedNotifier(project, jobType));
        } else {
            UnitOfWork.get().doAfterUnit(() -> {
                EventBusFactory.getInstance().postAsync(new JobReadyNotifier(project));
                EventBusFactory.getInstance().postAsync(new JobAddedNotifier(project, jobType));
            });
        }
    }

    private void addJobOutput(ExecutablePO executable) {
        ExecutableOutputPO executableOutputPO = new ExecutableOutputPO();
        executable.setOutput(executableOutputPO);
        if (CollectionUtils.isNotEmpty(executable.getTasks())) {
            for (ExecutablePO subTask : executable.getTasks()) {
                addJobOutput(subTask);
            }
        }
        if (MapUtils.isNotEmpty(executable.getStagesMap())) {
            for (Map.Entry<String, List<ExecutablePO>> entry : executable.getStagesMap().entrySet()) {
                entry.getValue().forEach(this::addJobOutput);
            }
        }
    }

    // checked invoked by job-tx
    public void updateJobOutput(String taskOrJobId, ExecutableState newStatus) {
        updateJobOutput(taskOrJobId, newStatus, null, null, null);
    }

    // checked invoked by job-tx
    public void updateJobOutput(String taskOrJobId, ExecutableState newStatus, Map<String, String> updateInfo) {
        updateJobOutput(taskOrJobId, newStatus, updateInfo, null, null);
    }

    // checked invoked by job-tx
    public void updateJobOutput(String taskOrJobId, ExecutableState newStatus, Map<String, String> updateInfo,
            Set<String> removeInfo, String output) {
        updateJobOutput(taskOrJobId, newStatus, updateInfo, removeInfo, output, 0);
    }

    // checked invoked by job-tx
    public void updateJobOutput(String taskOrJobId, ExecutableState newStatus, Map<String, String> updateInfo,
            Set<String> removeInfo, String output, long byteSize) {
        updateJobOutput(taskOrJobId, newStatus, updateInfo, removeInfo, output, byteSize, null);
    }

    // checked invoked by job-tx
    public void updateJobOutput(String taskOrJobId, ExecutableState newStatus, Map<String, String> updateInfo,
            Set<String> removeInfo, String output, long byteSize, String failedMsg) {
        val jobId = extractJobId(taskOrJobId);

        Map<String, Boolean> shouldDestroyProcessSignalMap = Maps.newHashMap();

        jobInfoDao.updateJob(jobId, job -> {
            ExecutableOutputPO jobOutput;
            ExecutablePO taskOrJob = Objects.equals(taskOrJobId, jobId) ? job
                    : job.getTasks().stream().filter(po -> po.getId().equals(taskOrJobId)).findFirst().orElse(null);
            jobOutput = taskOrJob.getOutput();
            assertOutputNotNull(jobOutput, taskOrJobId);
            ExecutableState oldStatus = ExecutableState.valueOf(jobOutput.getStatus());
            if (newStatus != null && oldStatus != newStatus) {
                if (!ExecutableState.isValidStateTransfer(oldStatus, newStatus)) {
                    logger.warn(
                            "[UNEXPECTED_THINGS_HAPPENED] wrong job state transfer! There is no valid state transfer from: {} to: {}, job id: {}",
                            oldStatus, newStatus, taskOrJobId);
                    throw new KylinException(JOB_STATE_TRANSFER_ILLEGAL);
                }
                jobOutput.setStatus(String.valueOf(newStatus));
                updateJobStatus(jobOutput, oldStatus, newStatus);
                logger.info("Job id: {} from {} to {}", taskOrJobId, oldStatus, newStatus);
            }
            Map<String, String> info = Maps.newHashMap(jobOutput.getInfo());
            Optional.ofNullable(updateInfo).ifPresent(info::putAll);
            Optional.ofNullable(removeInfo).ifPresent(set -> set.forEach(info::remove));
            if (ExecutableState.READY == newStatus) {
                Optional.ofNullable(REMOVE_INFO).ifPresent(set -> set.forEach(info::remove));
            }
            // check if job is running on current node
            if (hasRunningJob(jobId)) {
                String oldNodeInfo = info.get("node_info");
                String newNodeInfo = config.getServerAddress();
                if (Objects.nonNull(oldNodeInfo) && !Objects.equals(oldNodeInfo, newNodeInfo)
                        && !Objects.equals(taskOrJobId, jobId)) {
                    logger.info("The node running job has changed. Job id: {}, Step name: {}, Switch from {} to {}.",
                            jobId, taskOrJob.getName(), oldNodeInfo, newNodeInfo);
                }
                info.put("node_info", newNodeInfo);
                info.put("host_name", AddressUtil.getHostName());
            }
            jobOutput.setInfo(info);
            String appId = info.get(ExecutableConstants.YARN_APP_ID);
            if (StringUtils.isNotEmpty(appId)) {
                logger.info("Add application id {} to {}.", appId, jobId);
                job.addYarnApplicationJob(appId);
            }
            Optional.ofNullable(output).ifPresent(jobOutput::setContent);
            jobOutput.setLastModified(System.currentTimeMillis());

            if (byteSize > 0) {
                jobOutput.setByteSize(byteSize);
            }

            jobOutput.setFailedMsg(failedMsg);

            if (needDestroyProcess(oldStatus, newStatus)) {
                logger.debug("need kill {}, from {} to {}", taskOrJobId, oldStatus, newStatus);
                // kill spark-submit process
                shouldDestroyProcessSignalMap.put(SHOULD_DESTROY_PROCESS_SIGNAL, true);
                //TODO KE-37319
                // cancelJob(job, taskOrJobId);
            }
            return true;
        });

        // split from job-tx, this tx is repeatable-read, and db-locked before job-task thread tx.
        if (shouldDestroyProcessSignalMap.getOrDefault(SHOULD_DESTROY_PROCESS_SIGNAL, false)) {
            destroyProcess(taskOrJobId);
        }

    }

    private boolean hasRunningJob(String jobId) {
        JobContext jobContext = JobContextUtil.getJobContext(config);
        JdbcJobScheduler jobScheduler = jobContext.getJobScheduler();
        return null != jobScheduler && jobScheduler.getRunningJob().containsKey(jobId);
    }

    public static String extractJobId(String taskOrJobId) {
        val jobIdPair = taskOrJobId.split("_");
        return jobIdPair[0];
    }

    private void assertOutputNotNull(ExecutableOutputPO output, String idOrPath) {
        Preconditions.checkArgument(output != null, "there is no related output for job :" + idOrPath);
    }

    private void assertOutputNotNull(ExecutableOutputPO output, String idOrPath, String segmentOrStepId) {
        Preconditions.checkArgument(output != null,
                "there is no related output for job :" + idOrPath + " , segmentOrStep : " + segmentOrStepId);
    }

    public void cancelJobSubTasks(AbstractExecutable executable) {
        if (executable instanceof DefaultExecutable) {
            List<AbstractExecutable> subTasks = ((DefaultExecutable) executable).getTasks();
            subTasks.forEach(task -> {
                if (ExecutableState.RUNNING == task.getStatus()) {
                    logger.info("Cancel subtask [{}]", task.getDisplayName());
                    task.cancelJob();
                }
            });
        }
    }

    private void updateJobStatus(ExecutableOutputPO jobOutput, ExecutableState oldStatus, ExecutableState newStatus) {
        long time = System.currentTimeMillis();

        if (oldStatus == ExecutableState.RUNNING) {
            jobOutput.addEndTime(time);
            jobOutput.addDuration(time);
            return;
        }

        switch (newStatus) {
        case RUNNING:
            jobOutput.addStartTime(time);
            jobOutput.addLastRunningStartTime(time);
            break;
        case SKIP:
        case SUICIDAL:
        case DISCARDED:
            jobOutput.addStartTime(time);
            jobOutput.addEndTime(time);
            break;
        default:
            break;
        }
    }

    private boolean needDestroyProcess(ExecutableState from, ExecutableState to) {
        if (from != ExecutableState.RUNNING || to == null) {
            return false;
        }
        return to == ExecutableState.PAUSED || to == ExecutableState.READY || to == ExecutableState.DISCARDED
                || to == ExecutableState.ERROR || to == ExecutableState.SUICIDAL;
    }

    public void destroyProcess(String jobId) {
        EventBusFactory.getInstance().postSync(new CliCommandExecutor.JobKilled(jobId));
    }

    public void destroyAllProcess() {
        if (KylinConfig.getInstanceFromEnv().isUTEnv()) {
            return;
        }
        List<String> jobs = getJobs();
        for (String job : jobs) {
            destroyProcess(job);
        }
    }

    private void killRemoteProcess(ExecutablePO executablePO, CliCommandExecutor exe) {
        if (!executablePO.getOutput().getStatus().equalsIgnoreCase(ExecutableState.RUNNING.toString()))
            return;
        val info = executablePO.getOutput().getInfo();
        val pid = info.get("process_id");
        if (StringUtils.isNotEmpty(pid)) {
            String nodeInfo = info.get("node_info");
            String host = nodeInfo.split(":")[0];
            if (!host.equals(AddressUtil.getLocalInstance().split(":")[0])
                    && !host.equals(config.getServerAddress().split(":")[0])) {
                exe.setRunAtRemote(host, config.getRemoteSSHPort(), config.getRemoteSSHUsername(),
                        config.getRemoteSSHPassword());
            } else {
                exe.setRunAtRemote(null, config.getRemoteSSHPort(), config.getRemoteSSHUsername(),
                        config.getRemoteSSHPassword());
            }
            try {
                logger.info("will kill job pid is {}", pid);
                exe.execute("kill -9 " + pid, null);
            } catch (ShellException e) {
                logger.warn("failed to kill remote driver {} on {}", nodeInfo, pid, e);
            }
        }
    }

    public CliCommandExecutor getCliCommandExecutor() {
        CliCommandExecutor exec = new CliCommandExecutor();
        val config = KylinConfig.getInstanceFromEnv();
        exec.setRunAtRemote(config.getRemoteHadoopCliHostname(), config.getRemoteSSHPort(),
                config.getRemoteSSHUsername(), config.getRemoteSSHPassword());
        return exec;
    }

    public void checkJobCanBeDeleted(AbstractExecutable executable) {
        ExecutableState status = executable.getStatusInMem();
        if (ExecutableState.SUCCEED != status && ExecutableState.DISCARDED != status
                && ExecutableState.SUICIDAL != status) {
            throw new IllegalStateException(
                    "Cannot drop running job " + executable.getDisplayName() + ", please discard it first.");
        }
    }

    public AbstractExecutable getJob(String id) {
        if (id == null) {
            return null;
        }
        ExecutablePO executablePO = jobInfoDao.getExecutablePOByUuid(id);
        return getJob(id, executablePO);
    }

    public AbstractExecutable getJob(String id, ExecutablePO executablePO) {
        if (id == null || executablePO == null || !id.equals(executablePO.getId())) {
            return null;
        }
        try {
            return fromPO(executablePO);
        } catch (Exception e) {
            logger.error(PARSE_ERROR_MSG, e);
            return null;
        }
    }

    public AbstractExecutable fromPO(ExecutablePO executablePO) {
        if (executablePO == null) {
            logger.warn("executablePO is null");
            return null;
        }
        String type = executablePO.getType();
        Preconditions.checkArgument(StringUtils.isNotEmpty(type),
                "Cannot parse this job: " + executablePO.getId() + ", the type is empty");
        try {
            Class<? extends AbstractExecutable> clazz = ClassUtil.forName(type, AbstractExecutable.class);
            // no construction method to create a random number ID
            Constructor<? extends AbstractExecutable> constructor = clazz.getConstructor(Object.class);
            AbstractExecutable result = constructor.newInstance(DUMMY_OBJECT);
            result.setId(executablePO.getUuid());
            result.setName(executablePO.getName());
            String jobProject = null == executablePO.getProject() ? project : executablePO.getProject();
            result.setProject(jobProject);
            result.setParams(executablePO.getParams());
            result.setJobType(executablePO.getJobType());
            result.setTargetSubject(executablePO.getTargetModel());
            result.setTargetSegments(executablePO.getTargetSegments());
            result.setResumable(executablePO.getOutput().isResumable());
            result.setTargetPartitions(executablePO.getTargetPartitions());
            result.setPriority(executablePO.getPriority());
            result.setTag(executablePO.getTag());
            result.setPo(executablePO);
            result.setJobSchedulerMode(executablePO.getJobSchedulerMode());
            result.setPreviousStep(executablePO.getPreviousStep());
            result.setNextSteps(executablePO.getNextSteps());
            List<ExecutablePO> tasks = executablePO.getTasks();
            if (tasks != null && !tasks.isEmpty()) {
                Preconditions.checkArgument(result instanceof ChainedExecutable);
                for (ExecutablePO subTask : tasks) {
                    final AbstractExecutable abstractExecutable = fromPO(subTask);
                    if (abstractExecutable instanceof ChainedStageExecutable
                            && MapUtils.isNotEmpty(subTask.getStagesMap())) {
                        for (Map.Entry<String, List<ExecutablePO>> entry : subTask.getStagesMap().entrySet()) {
                            final List<StageBase> executables = entry.getValue().stream().map(po -> {
                                final AbstractExecutable executable = fromPO(po);
                                return (StageBase) executable;
                            }).collect(Collectors.toList());
                            ((ChainedStageExecutable) abstractExecutable).setStageMapWithSegment(entry.getKey(),
                                    executables);
                        }
                    }
                    ((ChainedExecutable) result).addTask(abstractExecutable);
                }
                if (result instanceof DefaultExecutableOnModel) {
                    val handlerType = executablePO.getHandlerType();
                    if (handlerType != null) {
                        Class<? extends ExecutableHandler> hClazz = ClassUtil.forName(handlerType,
                                ExecutableHandler.class);
                        Constructor<? extends ExecutableHandler> hConstructor = hClazz.getConstructor(String.class,
                                String.class, String.class, String.class, String.class);
                        String segmentId = CollectionUtils.isNotEmpty(result.getTargetSegments())
                                ? result.getTargetSegments().get(0)
                                : null;
                        ExecutableHandler executableHandler = hConstructor.newInstance(project,
                                result.getTargetSubject(), result.getSubmitter(), segmentId, result.getId());
                        ((DefaultExecutableOnModel) result).setHandler(executableHandler);
                    }
                }
            }
            return result;
        } catch (Throwable e) {
            logger.error("Cannot parse this job...", e);
            throw new IllegalStateException("Cannot parse this job: " + executablePO.getId(), e);
        }
    }

    public long getCreateTime(String id) {
        ExecutablePO executablePO = jobInfoDao.getExecutablePOByUuid(extractJobId(id));
        if (executablePO == null) {
            return 0L;
        }
        return executablePO.getOutput().getCreateTime();
    }

    public Output getOutput(String id) {
        val jobOutput = getJobOutput(id);
        assertOutputNotNull(jobOutput, id);
        return parseOutput(jobOutput);
    }

    public Output getOutput(String id, String segmentId) {
        val jobOutput = getJobOutput(id, segmentId);
        assertOutputNotNull(jobOutput, id, segmentId);
        return parseOutput(jobOutput);
    }

    public Output getOutput(String id, ExecutablePO executablePO, String segmentId) {
        val jobOutput = getJobOutput(id, executablePO, segmentId);
        assertOutputNotNull(jobOutput, id, segmentId);
        return parseOutput(jobOutput);
    }

    public Output getOutput(String id, ExecutablePO executablePO) {
        val jobOutput = getJobOutput(id, executablePO);
        assertOutputNotNull(jobOutput, id);
        return parseOutput(jobOutput);
    }

    //TODO this will directly access db, need cache?
    @VisibleForTesting
    public ExecutableOutputPO getJobOutput(String taskOrJobId) {
        val jobId = extractJobId(taskOrJobId);
        val executablePO = jobInfoDao.getExecutablePOByUuid(jobId);
        ExecutableOutputPO jobOutput = getExecutableOutputPO(taskOrJobId, executablePO);
        assertOutputNotNull(jobOutput, taskOrJobId);
        return jobOutput;
    }

    public ExecutableOutputPO getJobOutput(String taskOrJobId, String segmentId) {
        val jobId = extractJobId(taskOrJobId);
        val executablePO = jobInfoDao.getExecutablePOByUuid(jobId);
        return getJobOutput(taskOrJobId, executablePO, segmentId);
    }

    public ExecutableOutputPO getJobOutput(String taskOrJobId, ExecutablePO executablePO) {
        val jobId = extractJobId(taskOrJobId);
        ExecutableOutputPO jobOutput = getExecutableOutputPO(taskOrJobId, executablePO);
        assertOutputNotNull(jobOutput, taskOrJobId);
        return jobOutput;
    }

    public ExecutableOutputPO getJobOutput(String taskOrJobId, ExecutablePO executablePO, String segmentId) {
        val jobId = extractJobId(taskOrJobId);
        ExecutableOutputPO jobOutput = getExecutableOutputPO(taskOrJobId, executablePO);
        if (Objects.isNull(jobOutput)) {
            logger.trace("get job output from taskOrJobId : {} and segmentId : {}", taskOrJobId, segmentId);
            final Map<String, List<ExecutablePO>> stageMap = executablePO.getTasks().stream()
                    .map(ExecutablePO::getStagesMap)
                    .filter(map -> MapUtils.isNotEmpty(map) && map.containsKey(segmentId)).findFirst()
                    .orElse(Maps.newHashMap());
            jobOutput = stageMap.getOrDefault(segmentId, Lists.newArrayList()).stream()
                    .filter(po -> po.getId().equals(taskOrJobId)).findFirst().map(ExecutablePO::getOutput).orElse(null);
        }
        assertOutputNotNull(jobOutput, taskOrJobId, segmentId);
        return jobOutput;
    }

    private DefaultOutput parseOutput(ExecutableOutputPO jobOutput) {
        final DefaultOutput result = new DefaultOutput();
        result.setExtra(jobOutput.getInfo());
        result.setState(ExecutableState.valueOf(jobOutput.getStatus()));
        result.setVerboseMsg(jobOutput.getContent());
        result.setVerboseMsgStream(jobOutput.getContentStream());
        result.setLastModified(jobOutput.getLastModified());
        result.setStartTime(jobOutput.getStartTime());
        result.setEndTime(jobOutput.getEndTime());
        result.setWaitTime(jobOutput.getWaitTime());
        result.setDuration(jobOutput.getDuration());
        result.setLastRunningStartTime(jobOutput.getLastRunningStartTime());
        result.setCreateTime(jobOutput.getCreateTime());
        result.setByteSize(jobOutput.getByteSize());
        result.setShortErrMsg(jobOutput.getFailedMsg());
        result.setFailedStepId(jobOutput.getFailedStepId());
        result.setFailedSegmentId(jobOutput.getFailedSegmentId());
        result.setFailedStack(jobOutput.getFailedStack());
        result.setFailedReason(jobOutput.getFailedReason());
        return result;
    }

    public ExecutableOutputPO getExecutableOutputPO(String taskOrJobId, ExecutablePO executablePO) {
        ExecutableOutputPO jobOutput;
        if (Objects.isNull(executablePO)) {
            jobOutput = new ExecutableOutputPO();
        } else if (Objects.equals(taskOrJobId, executablePO.getId())) {
            jobOutput = executablePO.getOutput();
        } else {
            jobOutput = executablePO.getTasks().stream().filter(po -> po.getId().equals(taskOrJobId)).findFirst()
                    .map(ExecutablePO::getOutput).orElse(null);
        }
        return jobOutput;
    }

    /** just used to update job error mess */
    public void updateJobError(String taskOrJobId, String failedStepId, String failedSegmentId, String failedStack,
            String failedReason) {
        val jobId = extractJobId(taskOrJobId);

        jobInfoDao.updateJob(jobId, job -> {
            ExecutableOutputPO jobOutput = job.getOutput();
            val subTasks = job.getTasks();
            if (null != subTasks) {
                val errorTaskAndNotFailedStepIdTaskCount = subTasks.stream()
                        .filter(task -> ExecutableState.valueOf(task.getOutput().getStatus()) == ExecutableState.ERROR)
                        .filter(task -> !StringUtils.startsWith(failedStepId, task.getId())).count();
                if (errorTaskAndNotFailedStepIdTaskCount != 0) {
                    return false;
                }
            }
            if (jobOutput.getFailedReason() == null || failedReason == null) {
                jobOutput.setFailedStepId(failedStepId);
                jobOutput.setFailedSegmentId(failedSegmentId);
                jobOutput.setFailedStack(failedStack);
                jobOutput.setFailedReason(failedReason);
            }
            return true;
        });
    }

    public void resumeJob(String jobId) {
        resumeJob(jobId, false);
    }

    public void resumeJob(String jobId, boolean force) {
        JobContextUtil.withTxAndRetry(() -> {
            resumeJob(jobId, getJob(jobId), force);
            return true;
        });
    }

    //for ut
    public void resumeJob(String jobId, AbstractExecutable job) {
        resumeJob(jobId, job, false);
    }

    public void cancelRemoteJob(ExecutablePO executablePO) {
        if (executablePO.getOutput().getStatus().equalsIgnoreCase(ExecutableState.RUNNING.toString())) {
            // when start or restart KE, will resumeAllRunningJobs,
            // don't need to interrupt job thread, so job id is null
            //TODO KE-37319
            // cancelJob(executablePO, null);
        }
    }

    private void resumeJob(String jobId, AbstractExecutable job, boolean force) {
        updateJobError(jobId, null, null, null, null);
        if (Objects.isNull(job)) {
            return;
        }
        if (!job.getStatusInMem().isNotProgressing() && !force) {
            throw new KylinException(JOB_UPDATE_STATUS_FAILED, "RESUME", jobId, job.getStatusInMem());
        }

        if (job instanceof DefaultExecutable) {
            List<? extends AbstractExecutable> tasks = ((DefaultExecutable) job).getTasks();
            tasks.stream()
                    .filter(task -> task.getStatusInMem().isNotProgressing()
                            || task.getStatusInMem() == ExecutableState.RUNNING)
                    .forEach(task -> updateJobOutput(task.getId(), ExecutableState.READY));
            tasks.forEach(task -> {
                if (task instanceof ChainedStageExecutable) {
                    final Map<String, List<StageBase>> tasksMap = ((ChainedStageExecutable) task).getStagesMap();
                    if (MapUtils.isNotEmpty(tasksMap)) {
                        for (Map.Entry<String, List<StageBase>> entry : tasksMap.entrySet()) {
                            // update running stage to ready
                            Optional.ofNullable(entry.getValue()).orElse(Lists.newArrayList())//
                                    .stream() //
                                    .filter(stage -> stage.getStatusInMem(entry.getKey()) == ExecutableState.RUNNING
                                            || stage.getStatusInMem(entry.getKey()).isNotProgressing())//
                                    .forEach(stage -> //
                            updateStageStatus(stage.getId(), entry.getKey(), ExecutableState.READY, null, null));
                        }
                    }
                }
            });
        }

        updateJobOutput(jobId, ExecutableState.READY);
    }

    // READY -> PENDING
    public void publishJob(String jobId, AbstractExecutable job) {
        if (Objects.isNull(job) || job.getStatusInMem() != ExecutableState.READY) {
            return;
        }
        JobContextUtil.withTxAndRetry(() -> {
            if (job instanceof DefaultExecutable) {
                List<? extends AbstractExecutable> tasks = ((DefaultExecutable) job).getTasks();
                tasks.stream().filter(task -> task.getStatusInMem() == ExecutableState.READY)
                        .forEach(task -> updateJobOutput(task.getId(), ExecutableState.PENDING));
                tasks.forEach(task -> {
                    if (task instanceof ChainedStageExecutable) {
                        final Map<String, List<StageBase>> tasksMap = ((ChainedStageExecutable) task).getStagesMap();
                        if (MapUtils.isNotEmpty(tasksMap)) {
                            for (Map.Entry<String, List<StageBase>> entry : tasksMap.entrySet()) {
                                // update running stage to ready
                                Optional.ofNullable(entry.getValue()).orElse(Lists.newArrayList())//
                                        .stream() //
                                        .filter(stage -> stage.getStatusInMem(entry.getKey()) == ExecutableState.READY)
                                        .forEach(stage -> updateStageStatus(stage.getId(), entry.getKey(),
                                                ExecutableState.PENDING, null, null));
                            }
                        }
                    }
                });
            }
            updateJobOutput(jobId, ExecutableState.PENDING);
            return true;
        });
    }

    /** just used to update stage */
    public void updateStageStatus(String taskOrJobId, String segmentId, ExecutableState newStatus,
            Map<String, String> updateInfo, String failedMsg) {
        JobContextUtil.withTxAndRetry(() -> {
            updateStageStatus(taskOrJobId, segmentId, newStatus, updateInfo, failedMsg, false);
            return true;
        });
    }

    public void updateStageStatus(String taskOrJobId, String segmentId, ExecutableState newStatus,
            Map<String, String> updateInfo, String failedMsg, Boolean isRestart) {
        val jobId = extractJobId(taskOrJobId);
        jobInfoDao.updateJob(jobId, job -> {
            final List<Map<String, List<ExecutablePO>>> collect = job.getTasks().stream()//
                    .map(ExecutablePO::getStagesMap)//
                    .filter(MapUtils::isNotEmpty)//
                    .collect(Collectors.toList());
            if (CollectionUtils.isEmpty(collect)) {
                return false;
            }

            final Map<String, List<ExecutablePO>> stageMapFromSegment = collect.stream()//
                    .filter(map -> map.containsKey(segmentId))//
                    .findFirst().orElse(null);
            if (MapUtils.isNotEmpty(stageMapFromSegment)) {
                ExecutablePO stage = stageMapFromSegment.getOrDefault(segmentId, Lists.newArrayList()).stream()
                        .filter(po -> po.getId().equals(taskOrJobId))//
                        .findFirst().orElse(null);
                if (stage == null) {
                    return false; // local spark job?
                }

                ExecutableOutputPO stageOutput = stage.getOutput();
                assertOutputNotNull(stageOutput, taskOrJobId, segmentId);
                return setStageOutput(stageOutput, taskOrJobId, newStatus, updateInfo, failedMsg, isRestart);
            } else {
                for (Map<String, List<ExecutablePO>> stageMap : collect) {
                    for (Map.Entry<String, List<ExecutablePO>> entry : stageMap.entrySet()) {
                        final ExecutablePO stage = entry.getValue().stream()
                                .filter(po -> po.getId().equals(taskOrJobId))//
                                .findFirst().orElse(null);
                        if (null == stage) {
                            // local spark job(RESOURCE_DETECT): waiteForResource
                            return false;
                        }
                        ExecutableOutputPO stageOutput = stage.getOutput();
                        assertOutputNotNull(stageOutput, taskOrJobId);
                        val flag = setStageOutput(stageOutput, taskOrJobId, newStatus, updateInfo, failedMsg,
                                isRestart);
                        if (!flag) {
                            return false;
                        }
                    }
                }
            }
            return true;
        });
    }

    public boolean setStageOutput(ExecutableOutputPO jobOutput, String taskOrJobId, ExecutableState newStatus,
            Map<String, String> updateInfo, String failedMsg, Boolean isRestart) {
        ExecutableState oldStatus = ExecutableState.valueOf(jobOutput.getStatus());
        if (newStatus != null && oldStatus != newStatus) {
            if (!ExecutableState.isValidStateTransfer(oldStatus, newStatus)) {
                logger.warn(
                        "[UNEXPECTED_THINGS_HAPPENED] wrong job state transfer! There is no valid state transfer from: {} to: {}, job id: {}",
                        oldStatus, newStatus, taskOrJobId);
            }
            // DISCARDED must not be transferred to any others status
            if ((oldStatus == ExecutableState.PAUSED && newStatus == ExecutableState.ERROR)
                    || (oldStatus == ExecutableState.SKIP && newStatus == ExecutableState.SUCCEED)
                    || (oldStatus == ExecutableState.WARNING && newStatus == ExecutableState.SUCCEED)
                    || oldStatus == ExecutableState.DISCARDED) {
                return false;
            }
            if (isRestart || (oldStatus != ExecutableState.SUCCEED && oldStatus != ExecutableState.SKIP)) {
                jobOutput.setStatus(String.valueOf(newStatus));
                updateJobStatus(jobOutput, oldStatus, newStatus);
                logger.info("Job id: {} from {} to {}", taskOrJobId, oldStatus, newStatus);
            }
        }

        Map<String, String> info = Maps.newHashMap(jobOutput.getInfo());
        Optional.ofNullable(updateInfo).ifPresent(map -> {
            final int indexSuccessCount = Integer
                    .parseInt(map.getOrDefault(NBatchConstants.P_INDEX_SUCCESS_COUNT, "0"));
            info.put(NBatchConstants.P_INDEX_SUCCESS_COUNT, String.valueOf(indexSuccessCount));

            // Add warning_code to stage output info if exists
            String warningCode;
            if ((warningCode = map.get(NBatchConstants.P_WARNING_CODE)) != null) {
                info.put(NBatchConstants.P_WARNING_CODE, warningCode);
            }
        });
        jobOutput.setInfo(info);
        jobOutput.setLastModified(System.currentTimeMillis());
        jobOutput.setFailedMsg(failedMsg);
        return true;
    }

    public void restartJob(String jobId) {
        JobContextUtil.withTxAndRetry(() -> {
            restartJob(jobId, getJob(jobId));
            return true;
        });
    }

    // for ut
    public void restartJob(String jobId, AbstractExecutable jobToRestart) {
        updateJobError(jobId, null, null, null, null);
        if (Objects.isNull(jobToRestart)) {
            return;
        }
        if (jobToRestart.getStatusInMem().isFinalState()) {
            throw new KylinException(JOB_UPDATE_STATUS_FAILED, "RESTART", jobId, jobToRestart.getStatusInMem());
        }

        // to redesign: merge executableDao ops
        updateJobReady(jobId, jobToRestart);
        jobInfoDao.updateJob(jobId, job -> {
            job.getOutput().setResumable(false);
            job.getOutput().resetTime();
            job.getTasks().forEach(task -> {
                task.getOutput().setResumable(false);
                task.getOutput().resetTime();
                if (MapUtils.isNotEmpty(task.getStagesMap())) {
                    for (Map.Entry<String, List<ExecutablePO>> entry : task.getStagesMap().entrySet()) {
                        Optional.ofNullable(entry.getValue()).orElse(Lists.newArrayList()).forEach(executablePO -> {
                            executablePO.getOutput().setResumable(false);
                            executablePO.getOutput().resetTime();
                            Map<String, String> stageInfo = Maps.newHashMap(executablePO.getOutput().getInfo());
                            stageInfo.put(NBatchConstants.P_INDEX_SUCCESS_COUNT, "0");
                            executablePO.getOutput().setInfo(stageInfo);
                        });
                    }
                }
            });
            return true;
        });
    }

    private void updateJobReady(String jobId, AbstractExecutable job) {
        if (job == null) {
            return;
        }
        if (job instanceof DefaultExecutable) {
            List<? extends AbstractExecutable> tasks = ((DefaultExecutable) job).getTasks();
            tasks.stream().filter(task -> task.getStatusInMem() != ExecutableState.READY)
                    .forEach(task -> updateJobOutput(task.getId(), ExecutableState.READY));
            tasks.forEach(task -> {
                if (task instanceof ChainedStageExecutable) {
                    final Map<String, List<StageBase>> tasksMap = ((ChainedStageExecutable) task).getStagesMap();
                    if (MapUtils.isNotEmpty(tasksMap)) {
                        for (Map.Entry<String, List<StageBase>> entry : tasksMap.entrySet()) {
                            Optional.ofNullable(entry.getValue()).orElse(Lists.newArrayList()) //
                                    .stream()//
                                    .filter(stage -> stage.getStatusInMem(entry.getKey()) != ExecutableState.READY)
                                    .forEach(stage -> // when restart, reset stage
                            updateStageStatus(stage.getId(), entry.getKey(), ExecutableState.READY, null, null, true));
                        }
                    }
                }

            });
        }
        // restart waite time
        Map<String, String> info = Maps.newHashMap();
        info.put(NBatchConstants.P_WAITE_TIME, "{}");
        updateJobOutput(jobId, ExecutableState.READY, info);
    }

    // for ut
    public void discardJob(String jobId, AbstractExecutable job) {
        if (job == null) {
            return;
        }
        JobContextUtil.withTxAndRetry(() -> {
            if (job instanceof DefaultExecutable) {
                List<? extends AbstractExecutable> tasks = ((DefaultExecutable) job).getTasks();
                tasks.forEach(task -> {
                    if (task instanceof ChainedStageExecutable) {
                        final Map<String, List<StageBase>> tasksMap = ((ChainedStageExecutable) task).getStagesMap();
                        if (MapUtils.isNotEmpty(tasksMap)) {
                            for (Map.Entry<String, List<StageBase>> entry : tasksMap.entrySet()) {
                                Optional.ofNullable(entry.getValue()).orElse(Lists.newArrayList())//
                                        .forEach(stage -> //
                                updateStageStatus(stage.getId(), entry.getKey(), ExecutableState.DISCARDED, null,
                                        null));
                            }
                        }
                    }
                });
            }
            updateJobOutput(jobId, ExecutableState.DISCARDED);

            job.cancelJob();
            return true;
        });
    }

    public void pauseJob(String jobId) {
        JobContextUtil.withTxAndRetry(() -> {
            ExecutablePO executablePO = jobInfoDao.getExecutablePOByUuid(jobId);
            pauseJob(jobId, executablePO, fromPO(executablePO));
            return true;
        });
    }

    // for ut
    public void pauseJob(String jobId, ExecutablePO executablePO, AbstractExecutable job) {
        JobContextUtil.withTxAndRetry(() -> {
            if (job == null) {
                return false;
            }
            if (!job.getStatusInMem().isProgressing()) {
                throw new KylinException(JOB_UPDATE_STATUS_FAILED, "PAUSE", jobId, job.getStatusInMem());
            }
            updateStagePaused(job);
            Map<String, String> info = getWaiteTime(executablePO, job);
            updateJobOutput(jobId, ExecutableState.PAUSED, info, null, null, 0, null);
            // pauseJob may happen when the job has not been scheduled
            // then call this hook after updateJobOutput
            job.onExecuteStopHook();

            return true;
        });
    }

    public void errorJob(String jobId) {
        JobContextUtil.withTxAndRetry(() -> {
            AbstractExecutable job = getJob(jobId);
            errorJob(jobId, job);
            return true;
        });
    }

    private void errorJob(String jobId, AbstractExecutable job) {
        if (job == null) {
            return;
        }

        if (job instanceof DefaultExecutable) {
            List<? extends AbstractExecutable> tasks = ((DefaultExecutable) job).getTasks();
            tasks.stream().filter(task -> task.getStatus() != ExecutableState.ERROR)
                    .filter(task -> task.getStatus() != ExecutableState.SUCCEED)
                    .forEach(task -> updateJobOutput(task.getId(), ExecutableState.ERROR));
            tasks.forEach(task -> {
                if (task instanceof ChainedStageExecutable) {
                    final Map<String, List<StageBase>> tasksMap = ((ChainedStageExecutable) task).getStagesMap();
                    if (MapUtils.isNotEmpty(tasksMap)) {
                        for (Map.Entry<String, List<StageBase>> entry : tasksMap.entrySet()) {
                            Optional.ofNullable(entry.getValue()).orElse(Lists.newArrayList())//
                                    .stream()
                                    .filter(stage -> stage.getStatus(entry.getKey()) != ExecutableState.ERROR
                                            && stage.getStatus(entry.getKey()) != ExecutableState.SUCCEED)
                                    .forEach(stage -> //
                            updateStageStatus(stage.getId(), entry.getKey(), ExecutableState.ERROR, null, null));
                        }
                    }
                }
            });
        }

        updateJobOutput(jobId, ExecutableState.ERROR);
    }

    public void updateStagePaused(AbstractExecutable job) {
        if (job instanceof DefaultExecutable) {
            List<? extends AbstractExecutable> tasks = ((DefaultExecutable) job).getTasks();
            tasks.forEach(task -> {
                if (task instanceof ChainedStageExecutable) {
                    final Map<String, List<StageBase>> tasksMap = ((ChainedStageExecutable) task).getStagesMap();
                    if (MapUtils.isNotEmpty(tasksMap)) {
                        for (Map.Entry<String, List<StageBase>> entry : tasksMap.entrySet()) {
                            // pause running stage
                            Optional.ofNullable(entry.getValue()).orElse(Lists.newArrayList())//
                                    .stream() //
                                    .filter(stage -> stage.getStatus(entry.getKey()) == ExecutableState.RUNNING)//
                                    .forEach(stage -> //
                            updateStageStatus(stage.getId(), entry.getKey(), ExecutableState.PAUSED, null, null));
                        }
                    }
                }
            });
        }
    }

    public Map<String, String> getWaiteTime(ExecutablePO executablePO, AbstractExecutable job) {
        try {
            val oldInfo = Optional.ofNullable(job.getOutput().getExtra()).orElse(Maps.newHashMap());
            Map<String, String> info = Maps.newHashMap(oldInfo);
            if (job instanceof DefaultExecutable) {
                Map<String, String> waiteTime = JsonUtil
                        .readValueAsMap(info.getOrDefault(NBatchConstants.P_WAITE_TIME, "{}"));

                final List<AbstractExecutable> tasks = ((DefaultExecutable) job).getTasks();
                for (AbstractExecutable task : tasks) {
                    val waitTime = task.getWaitTime();
                    val oldWaitTime = Long.parseLong(waiteTime.getOrDefault(task.getId(), "0"));
                    waiteTime.put(task.getId(), String.valueOf(waitTime + oldWaitTime));
                    if (task instanceof ChainedStageExecutable) {
                        final ChainedStageExecutable stageExecutable = (ChainedStageExecutable) task;
                        Map<String, List<StageBase>> stageMap = Optional.ofNullable(stageExecutable.getStagesMap())
                                .orElse(Maps.newHashMap());
                        val taskStartTime = task.getStartTime();
                        for (Map.Entry<String, List<StageBase>> entry : stageMap.entrySet()) {
                            final String segmentId = entry.getKey();
                            if (waiteTime.containsKey(segmentId)) {
                                break;
                            }
                            final List<StageBase> stageBases = Optional.ofNullable(entry.getValue())
                                    .orElse(Lists.newArrayList());
                            if (CollectionUtils.isNotEmpty(stageBases)) {
                                final StageBase firstStage = stageBases.get(0);
                                val firstStageStartTime = getOutput(firstStage.getId(), executablePO, segmentId)
                                        .getStartTime();
                                val stageWaiteTIme = firstStageStartTime - taskStartTime > 0
                                        ? firstStageStartTime - taskStartTime
                                        : 0;
                                val oldStageWaiteTIme = Long.parseLong(waiteTime.getOrDefault(segmentId, "0"));
                                waiteTime.put(segmentId, String.valueOf(stageWaiteTIme + oldStageWaiteTIme));
                            }
                        }
                    }
                }
                info.put(NBatchConstants.P_WAITE_TIME, JsonUtil.writeValueAsString(waiteTime));
            }
            return info;
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            return null;
        }
    }

    public void discardJob(String jobId) {
        discardJob(jobId, getJob(jobId));
    }

    public void deleteAllJobsOfProject() {
        JobContextUtil.withTxAndRetry(() -> {
            jobInfoDao.deleteJobsByProject(project);
            return true;
        });
    }

    public void updateJobOutputToHDFS(String resPath, ExecutableOutputPO obj) {
        DataOutputStream dout = null;
        try {
            Path path = new Path(resPath);
            FileSystem fs = HadoopUtil.getWorkingFileSystem();
            dout = fs.create(path, true);
            if (KylinConfig.getInstanceFromEnv().isJobTmpDirALLPermissionEnabled()) {
                fs.setPermission(path.getParent(), new FsPermission(FsAction.ALL, FsAction.READ, FsAction.ALL));
            }
            JsonUtil.writeValue(dout, obj);
        } catch (Exception e) {
            // the operation to update output to hdfs failed, next task should not be interrupted.
            logger.error("update job output [{}] to HDFS failed.", resPath, e);
        } finally {
            IOUtils.closeQuietly(dout);
        }
    }

    public ExecutableOutputPO getJobOutputFromHDFS(String resPath) {
        DataInputStream din = null;
        try {
            val path = new Path(resPath);
            FileSystem fs = HadoopUtil.getWorkingFileSystem();
            if (!fs.exists(path)) {
                val executableOutputPO = new ExecutableOutputPO();
                executableOutputPO.setContent("job output not found, please check kylin.log");
                return executableOutputPO;
            }

            din = fs.open(path);
            return JsonUtil.readValue(din, ExecutableOutputPO.class);
        } catch (Exception e) {
            // If the output file on hdfs is corrupt, give an empty output
            logger.error("get job output [{}] from HDFS failed.", resPath, e);
            val executableOutputPO = new ExecutableOutputPO();
            executableOutputPO.setContent("job output broken, please check kylin.log");
            return executableOutputPO;
        } finally {
            IOUtils.closeQuietly(din);
        }
    }

    public void makeStageSuccess(String taskOrJobId) {
        JobContextUtil.withTxAndRetry(() -> {
            val jobId = extractJobId(taskOrJobId);
            AbstractExecutable job = getJob(jobId);
            makeStageSuccess(taskOrJobId, job);
            return true;
        });
    }

    private void makeStageSuccess(String taskOrJobId, AbstractExecutable job) {
        if (job == null) {
            return;
        }
        if (job instanceof DefaultExecutable) {
            List<? extends AbstractExecutable> tasks = ((DefaultExecutable) job).getTasks();
            tasks.forEach(task -> {
                if (task instanceof ChainedStageExecutable && StringUtils.equals(taskOrJobId, task.getId())) {
                    final Map<String, List<StageBase>> tasksMap = ((ChainedStageExecutable) task).getStagesMap();
                    if (MapUtils.isNotEmpty(tasksMap)) {
                        for (Map.Entry<String, List<StageBase>> entry : tasksMap.entrySet()) {
                            Optional.ofNullable(entry.getValue()).orElse(Lists.newArrayList())//
                                    .stream() //
                                    .filter(stage -> stage.getStatus(entry.getKey()) != ExecutableState.SUCCEED)//
                                    .forEach(stage -> //
                            updateStageStatus(stage.getId(), entry.getKey(), ExecutableState.SUCCEED, null, null));
                        }
                    }
                }
            });
        }
    }

    public void makeStageError(String taskOrJobId) {
        JobContextUtil.withTxAndRetry(() -> {
            val jobId = extractJobId(taskOrJobId);
            AbstractExecutable job = getJob(jobId);
            makeStageError(taskOrJobId, job);
            return true;
        });
    }

    private void makeStageError(String taskOrJobId, AbstractExecutable job) {
        if (job == null) {
            return;
        }
        if (job instanceof DefaultExecutable) {
            List<? extends AbstractExecutable> tasks = ((DefaultExecutable) job).getTasks();
            tasks.forEach(task -> {
                if (task instanceof ChainedStageExecutable && StringUtils.equals(taskOrJobId, task.getId())) {
                    final Map<String, List<StageBase>> tasksMap = ((ChainedStageExecutable) task).getStagesMap();
                    if (MapUtils.isNotEmpty(tasksMap)) {
                        for (Map.Entry<String, List<StageBase>> entry : tasksMap.entrySet()) {
                            Optional.ofNullable(entry.getValue()).orElse(Lists.newArrayList())//
                                    .stream() //
                                    .filter(stage -> stage.getStatus(entry.getKey()) == ExecutableState.RUNNING)//
                                    .forEach(stage -> //
                            updateStageStatus(stage.getId(), entry.getKey(), ExecutableState.ERROR, null, null));
                        }
                    }
                }
            });
        }
    }

    public long countCuttingInJobByModel(String model, AbstractExecutable job) {
        return getPartialExecutables(model).stream() //
                .filter(e -> e.getTargetSubject() != null) //
                .filter(e -> e.getTargetSubject().equals(model))
                .filter(executable -> executable.getCreateTime() > job.getCreateTime()).count();
    }

    public List<AbstractExecutable> getAllExecutables() {
        List<AbstractExecutable> ret = Lists.newArrayList();
        for (ExecutablePO po : jobInfoDao.getJobs(project)) {
            try {
                AbstractExecutable ae = fromPO(po);
                ret.add(ae);
            } catch (Exception e) {
                logger.error(PARSE_ERROR_MSG, e);
            }
        }
        return ret;
    }

    public List<AbstractExecutable> getPartialExecutables(String modelId) {
        List<AbstractExecutable> ret = Lists.newArrayList();
        List<ExecutablePO> executablePOList = getExecutablePOByModelId(project, modelId);
        for (ExecutablePO po : executablePOList) {
            try {
                AbstractExecutable ae = fromPO(po);
                ret.add(ae);
            } catch (Exception e) {
                logger.error(PARSE_ERROR_MSG, e);
            }
        }
        return ret;
    }

    private List<ExecutablePO> getExecutablePOByModelId(String project, String modelId) {
        JobMapperFilter jobMapperFilter = new JobMapperFilter();
        jobMapperFilter.setProject(project);
        if (null != modelId) {
            jobMapperFilter.setModelIds(Lists.newArrayList(modelId));
        }
        List<JobInfo> jobInfoList = jobInfoDao.getJobInfoListByFilter(jobMapperFilter);
        if (CollectionUtils.isEmpty(jobInfoList)) {
            return Lists.newArrayList();
        }
        return jobInfoList.stream().map(jobInfo -> JobInfoUtil.deserializeExecutablePO(jobInfo))
                .collect(Collectors.toList());
    }

    public long countByModelAndStatus(String model, Predicate<ExecutableState> predicate) {
        return countByModelAndStatus(model, predicate, null);
    }

    public long countByModelAndStatus(String model, Predicate<ExecutableState> predicate, JobTypeEnum... jobTypes) {
        return listExecByModelAndStatus(model, predicate, jobTypes).size();
    }

    public List<AbstractExecutable> listExecByModelAndStatus(String model, Predicate<ExecutableState> predicate,
            JobTypeEnum... jobTypes) {
        return listExecutablePOByModelAndStatus(model, predicate, jobTypes).stream().map(this::fromPO)
                .collect(Collectors.toList());
    }

    public List<ExecutablePO> listExecutablePOByModelAndStatus(String model, Predicate<ExecutableState> predicate,
            List<ExecutablePO> jobs, JobTypeEnum... jobTypes) {
        boolean allPass = Array.isEmpty(jobTypes);
        return jobs.stream() //
                .filter(job -> (null == model) || (null != job.getTargetModel() && job.getTargetModel().equals(model)))
                .filter(job -> predicate.test(ExecutableState.valueOf(job.getOutput().getStatus()))) //
                .filter(job -> allPass || Lists.newArrayList(jobTypes).contains(job.getJobType())) //
                .collect(Collectors.toList());
    }

    public List<ExecutablePO> listExecutablePOByModelAndStatus(String model, Predicate<ExecutableState> predicate,
            JobTypeEnum... jobTypes) {
        return listExecutablePOByModelAndStatus(model, predicate, getExecutablePOByModelId(project, model), jobTypes);
    }

    public void setJobResumable(final String taskOrJobId) {
        JobContextUtil.withTxAndRetry(() -> {
            final String jobId = extractJobId(taskOrJobId);
            AbstractExecutable job = getJob(jobId);
            setJobResumable(taskOrJobId, job);
            return true;
        });
    }

    private void setJobResumable(final String taskOrJobId, AbstractExecutable job) {
        final String jobId = extractJobId(taskOrJobId);
        if (Objects.isNull(job)) {
            return;
        }
        if (Objects.equals(taskOrJobId, jobId)) {
            jobInfoDao.updateJob(jobId, executablePO -> {
                executablePO.getOutput().setResumable(true);
                return true;
            });
        } else {
            jobInfoDao.updateJob(jobId, executablePO -> {
                executablePO.getTasks().stream().filter(o -> Objects.equals(taskOrJobId, o.getId()))
                        .forEach(t -> t.getOutput().setResumable(true));
                return true;
            });
        }
    }

    public List<AbstractExecutable> listMultiPartitionModelExec(String model, Predicate<ExecutableState> predicate,
            JobTypeEnum jobType, Set<Long> targetPartitions, Set<String> segmentIds) {
        return getPartialExecutables(model).stream().filter(e -> e.getTargetSubject() != null) //
                .filter(e -> e.getTargetSubject().equals(model)) //
                .filter(e -> predicate.test(e.getStatusInMem())) //
                .filter(e -> {
                    /**
                     *  Select jobs which partition is overlap.
                     *  Attention: Refresh/Index build job will include all partitions.
                     */
                    boolean checkAllPartition = CollectionUtils.isEmpty(targetPartitions)
                            || JobTypeEnum.INDEX_REFRESH == e.getJobType() //
                            || JobTypeEnum.INDEX_REFRESH == jobType //
                            || JobTypeEnum.INDEX_BUILD == e.getJobType() //
                            || JobTypeEnum.INDEX_BUILD == jobType;
                    if (checkAllPartition) {
                        return true;
                    }
                    return !Sets.intersection(e.getTargetPartitions(), targetPartitions).isEmpty();
                }).filter(e -> {
                    if (CollectionUtils.isEmpty(segmentIds)) {
                        return true;
                    }
                    return !Sets.intersection(new HashSet<>(e.getTargetSegments()), segmentIds).isEmpty();
                }).collect(Collectors.toList());
    }

    public List<ExecutablePO> getExecutablePOsByStatus(List<String> jobIds, List<ExecutableState> executableStates) {
        JobMapperFilter jobMapperFilter = new JobMapperFilter();
        jobMapperFilter.setProject(project);
        if (CollectionUtils.isNotEmpty(jobIds)) {
            jobMapperFilter.setJobIds(jobIds);
        }

        if (CollectionUtils.isNotEmpty(executableStates)) {
            jobMapperFilter.setStatuses(executableStates);
        }
        List<JobInfo> jobInfoList = jobInfoDao.getJobInfoListByFilter(jobMapperFilter);
        return jobInfoList.stream().map(jobInfo -> JobInfoUtil.deserializeExecutablePO(jobInfo))
                .collect(Collectors.toList());
    }

    public List<ExecutablePO> getExecutablePOsByStatus(List<ExecutableState> statuses) {
        return getExecutablePOsByStatus(null, statuses);
    }

    public List<AbstractExecutable> getExecutablesByStatus(List<String> jobIds, List<ExecutableState> statuses) {
        return getExecutablePOsByStatus(jobIds, statuses).stream().map(this::fromPO).collect(Collectors.toList());
    }

    public List<AbstractExecutable> getExecutablesByStatus(ExecutableState status) {
        return getExecutablesByStatus(null, Lists.newArrayList(status));
    }

    public List<AbstractExecutable> getExecutablesByJobType(Set<JobTypeEnum> RELATED_JOBS) {
        List<String> jobTypeNames = RELATED_JOBS.stream().map(jobTypeEnum -> jobTypeEnum.name())
                .collect(Collectors.toList());
        JobMapperFilter jobMapperFilter = new JobMapperFilter();
        jobMapperFilter.setJobNames(jobTypeNames);
        List<JobInfo> jobInfoList = jobInfoDao.getJobInfoListByFilter(jobMapperFilter);
        return jobInfoList.stream().map(jobInfo -> JobInfoUtil.deserializeExecutablePO(jobInfo)).map(this::fromPO)
                .collect(Collectors.toList());
    }

    public ExecutablePO getExecutablePO(String jobId) {
        return jobInfoDao.getExecutablePOByUuid(jobId);
    }

    public List<String> getJobs() {
        return getAllJobs().stream().sorted(Comparator.comparing(ExecutablePO::getCreateTime))
                .sorted(Comparator.comparing(ExecutablePO::getPriority)).map(RootPersistentEntity::resourceName)
                .collect(Collectors.toList());
    }

    public List<ExecutablePO> getAllJobs() {
        return jobInfoDao.getJobs(project);
    }

    public List<ExecutablePO> getAllJobs(long timeStartInMillis, long timeEndInMillis) {
        return jobInfoDao.getJobs(project, timeStartInMillis, timeEndInMillis);
    }

    public List<AbstractExecutable> getAllExecutables(long timeStartInMillis, long timeEndInMillis) {
        return getAllJobs(timeStartInMillis, timeEndInMillis).stream().map(executablePO -> fromPO(executablePO))
                .collect(Collectors.toList());
    }

    public Output getOutputFromHDFSByJobId(String jobId) {
        return getOutputFromHDFSByJobId(jobId, jobId);
    }

    public Output getOutputFromHDFSByJobId(String jobId, String stepId) {
        return getOutputFromHDFSByJobId(jobId, stepId, LOG_DEFAULT_DISPLAY_HEAD_AND_TAIL_SIZE);
    }

    /**
     * get job output from hdfs json file;
     * if json file contains logPath,
     * the logPath is spark driver log hdfs path(*.json.log), read sample data from log file.
     *
     * @param jobId
     * @return
     */
    public Output getOutputFromHDFSByJobId(String jobId, String stepId, int nLines) {
        String outputStorePath = KylinConfig.getInstanceFromEnv().getJobTmpOutputStorePath(project, stepId);
        ExecutableOutputPO jobOutput = getJobOutputFromHDFS(outputStorePath);
        assertOutputNotNull(jobOutput, outputStorePath);

        if (Objects.nonNull(jobOutput.getLogPath())) {
            if (isHdfsPathExists(jobOutput.getLogPath())) {
                if (nLines == LOG_DEFAULT_DISPLAY_HEAD_AND_TAIL_SIZE) {
                    jobOutput.setContent(getSampleDataFromHDFS(jobOutput.getLogPath(), nLines));
                } else {
                    jobOutput.setContentStream(getLogStream(jobOutput.getLogPath()));
                }
            } else if (StringUtils.isEmpty(jobOutput.getContent()) && Objects.nonNull(getJob(jobId))
                    && getJob(jobId).getStatusInMem() == ExecutableState.RUNNING) {
                jobOutput.setContent("Wait a moment ... ");
            }
        }

        return parseOutput(jobOutput);
    }

    public InputStream getLogStream(String resPath) {
        try {
            FileSystem fs = HadoopUtil.getWorkingFileSystem();

            Path path = new Path(resPath);
            if (!fs.exists(path)) {
                return null;
            }
            return fs.open(path);
        } catch (IOException e) {
            logger.error("get FileSystem from hdfs log file [{}] failed!", resPath, e);
            throw new KylinException(FAILED_DOWNLOAD_FILE, e);
        }
    }

    /**
     * check the hdfs path exists.
     *
     * @param hdfsPath
     * @return
     */
    public boolean isHdfsPathExists(String hdfsPath) {
        if (StringUtils.isBlank(hdfsPath)) {
            return false;
        }

        Path path = new Path(hdfsPath);
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        try {
            return fs.exists(path);
        } catch (IOException e) {
            logger.error("check the hdfs path [{}] exists failed, ", hdfsPath, e);
        }

        return false;
    }

    /**
     * get sample data from hdfs log file.
     * specified the lines, will get the first num lines and last num lines.
     *
     * @param resPath
     * @return
     */
    public String getSampleDataFromHDFS(String resPath, final int nLines) {
        try {
            Path path = new Path(resPath);
            FileSystem fs = HadoopUtil.getWorkingFileSystem();
            if (!fs.exists(path)) {
                return null;
            }

            FileStatus fileStatus = fs.getFileStatus(path);
            try (FSDataInputStream din = fs.open(path);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(din, Charset.defaultCharset()))) {

                String line;
                StringBuilder sampleData = new StringBuilder();
                for (int i = 0; i < nLines && (line = reader.readLine()) != null; i++) {
                    if (sampleData.length() > 0) {
                        sampleData.append('\n');
                    }
                    sampleData.append(line);
                }

                int offset = sampleData.toString().getBytes(Charset.defaultCharset()).length + 1;
                if (offset < fileStatus.getLen()) {
                    sampleData.append("\n================================================================\n");
                    sampleData.append(tailHdfsFileInputStream(din, offset, fileStatus.getLen(), nLines));
                }
                return sampleData.toString();
            }
        } catch (IOException e) {
            logger.error("get sample data from hdfs log file [{}] failed!", resPath, e);
            return null;
        }
    }

    /**
     * get sample data from hdfs log file.
     * specified the lines, will get the first num lines and last num lines.
     * @return
     */
    public String getSampleDataFromBothHDFS(String firstPath, String lastPath, final int nLines) {
        try {
            Path fPath = new Path(firstPath);
            Path lPath = new Path(lastPath);
            FileSystem fs = HadoopUtil.getWorkingFileSystem();
            if (!fs.exists(fPath) || !fs.exists(lPath)) {
                return null;
            }

            FileStatus lastFileStatus = fs.getFileStatus(lPath);
            try (FSDataInputStream fdin = fs.open(fPath);
                    BufferedReader fReader = new BufferedReader(new InputStreamReader(fdin, Charset.defaultCharset()));
                    FSDataInputStream ldin = fs.open(lPath)) {

                String line;
                StringBuilder sampleData = new StringBuilder();
                // read Head nLines from firstPath
                for (int i = 0; i < nLines && (line = fReader.readLine()) != null; i++) {
                    if (sampleData.length() > 0) {
                        sampleData.append('\n');
                    }
                    sampleData.append(line);
                }

                int offset = sampleData.toString().getBytes(Charset.defaultCharset()).length + 1;
                if (offset < lastFileStatus.getLen()) {
                    sampleData.append("\n================================================================\n");
                    sampleData.append(tailHdfsFileInputStream(ldin, offset, lastFileStatus.getLen(), nLines));
                }
                return sampleData.toString();
            }
        } catch (IOException e) {
            logger.error("get sample data from hdfs log file [{}, {}] failed!", firstPath, lastPath, e);
            return null;
        }
    }

    /**
     * get the last N_LINES lines from the end of hdfs file input stream;
     * reference: https://olapio.atlassian.net/wiki/spaces/PD/pages/1306918958
     *
     * @param hdfsDin
     * @param startPos
     * @param endPos
     * @param nLines
     * @return
     * @throws IOException
     */
    private String tailHdfsFileInputStream(FSDataInputStream hdfsDin, final long startPos, final long endPos,
            final int nLines) throws IOException {
        Preconditions.checkNotNull(hdfsDin);
        Preconditions.checkArgument(startPos < endPos && startPos >= 0);
        Preconditions.checkArgument(nLines >= 0);

        Deque<String> deque = new ArrayDeque<>();
        int buffSize = 8192;
        byte[] byteBuf = new byte[buffSize];

        long pos = endPos;

        // cause by log last char is \n
        hdfsDin.seek(pos - 1);
        int lastChar = hdfsDin.read();
        if ('\n' == lastChar) {
            pos--;
        }

        int bytesRead = (int) ((pos - startPos) % buffSize);
        if (bytesRead == 0) {
            bytesRead = buffSize;
        }

        pos -= bytesRead;
        int lines = nLines;
        while (lines > 0 && pos >= startPos) {
            bytesRead = hdfsDin.read(pos, byteBuf, 0, bytesRead);

            int last = bytesRead;
            for (int i = bytesRead - 1; i >= 0 && lines > 0; i--) {
                if (byteBuf[i] == '\n') {
                    deque.push(new String(byteBuf, i, last - i, StandardCharsets.UTF_8));
                    lines--;
                    last = i;
                }
            }

            if (lines > 0 && last > 0) {
                deque.push(new String(byteBuf, 0, last, StandardCharsets.UTF_8));
            }

            bytesRead = buffSize;
            pos -= bytesRead;
        }

        StringBuilder sb = new StringBuilder();
        while (!deque.isEmpty()) {
            sb.append(deque.pop());
        }

        return sb.length() > 0 && sb.charAt(0) == '\n' ? sb.substring(1) : sb.toString();
    }

    //for ut
    @VisibleForTesting
    public List<AbstractExecutable> getRunningExecutables(String project, String model) {
        if (StringUtils.isNotBlank(model)) {
            return listExecByModelAndStatus(model, ExecutableState::isRunning, null);
        } else {
            JobMapperFilter jobMapperFilter = new JobMapperFilter();
            List<ExecutableState> runningStates = Lists.newArrayList();
            for (ExecutableState executableState : ExecutableState.values()) {
                if (executableState.isRunning()) {
                    runningStates.add(executableState);
                }
            }
            jobMapperFilter.setStatuses(runningStates);
            jobMapperFilter.setProject(project);
            return jobInfoDao.getJobInfoListByFilter(jobMapperFilter).stream()
                    .map(jobInfo -> JobInfoUtil.deserializeExecutablePO(jobInfo)).map(this::fromPO)
                    .collect(Collectors.toList());
        }
    }

    //for ut
    public void deleteJob(String jobId) {
        checkJobCanBeDeleted(fromPO(jobInfoDao.getExecutablePOByUuid(jobId)));
        jobInfoDao.dropJob(jobId);
    }

    public void deleteJobByIdList(List<String> jobIdList) {
        jobInfoDao.dropJobByIdList(jobIdList);
    }

    //for ut
    @VisibleForTesting
    public void deleteAllJob() {
        jobInfoDao.dropAllJobs();
    }

    public void suicideJob(String jobId) {
        JobContextUtil.withTxAndRetry(() -> {
            AbstractExecutable job = getJob(jobId);
            suicideJob(jobId, job);
            return true;
        });
    }

    private void suicideJob(String jobId, AbstractExecutable job) {
        if (job == null) {
            return;
        }

        if (job instanceof DefaultExecutable) {
            List<? extends AbstractExecutable> tasks = ((DefaultExecutable) job).getTasks();
            tasks.stream().filter(task -> task.getStatus() != ExecutableState.SUICIDAL)
                    .filter(task -> task.getStatus() != ExecutableState.SUCCEED)
                    .forEach(task -> updateJobOutput(task.getId(), ExecutableState.SUICIDAL));
            tasks.forEach(task -> {
                if (task instanceof ChainedStageExecutable) {
                    final Map<String, List<StageBase>> tasksMap = ((ChainedStageExecutable) task).getStagesMap();
                    if (MapUtils.isNotEmpty(tasksMap)) {
                        for (Map.Entry<String, List<StageBase>> entry : tasksMap.entrySet()) {
                            Optional.ofNullable(entry.getValue()).orElse(Lists.newArrayList())//
                                    .stream()
                                    .filter(stage -> stage.getStatus(entry.getKey()) != ExecutableState.SUICIDAL
                                            && stage.getStatus(entry.getKey()) != ExecutableState.SUCCEED)
                                    .forEach(stage -> //
                            updateStageStatus(stage.getId(), entry.getKey(), ExecutableState.SUICIDAL, null, null));
                        }
                    }
                }
            });
        }
        updateJobOutput(jobId, ExecutableState.SUICIDAL);

        job.cancelJob();
    }

    public void suicideRunningJobByJobType(String project, String targetModelId, List<String> jobType) {
        List<JobInfo> existingJobs = ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .fetchNotFinalJobsByTypes(project, jobType, Lists.newArrayList(targetModelId));
        ExecutableManager execMgr = ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        if (CollectionUtils.isNotEmpty(existingJobs)) {
            existingJobs.forEach(jobInfo -> execMgr.suicideJob(jobInfo.getJobId()));
        }
    }

    public void checkSuicideJobOfModel(String project, String modelId) {
        JobMapperFilter jobMapperFilter = new JobMapperFilter();
        jobMapperFilter.setProject(project);
        jobMapperFilter.setModelIds(Lists.newArrayList(modelId));
        jobMapperFilter.setStatuses(ExecutableState.ERROR, ExecutableState.PAUSED);
        List<JobInfo> errorJobInfoList = ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .fetchJobsByFilter(jobMapperFilter);
        if (CollectionUtils.isEmpty(errorJobInfoList)) {
            log.info("No job need to suicide, project: {}, model id: {}", project, modelId);
            return;
        }
        ExecutableManager executableManager = ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        for (JobInfo jobInfo : errorJobInfoList) {
            String jobId = jobInfo.getJobId();
            JobContextUtil.withTxAndRetry(() -> {
                AbstractExecutable job = executableManager.getJob(jobId);
                if (JobCheckUtil.checkSuicide(job)) {
                    executableManager.suicideJob(jobId);
                    log.info("Suicide job: {}, project: {}, model id: {}", jobId, project, modelId);
                }
                return true;
            });
        }
    }

    // for ut
    public void resumeAllRunningJobs() {
        val jobs = jobInfoDao.getJobs(project);
        CliCommandExecutor exe = getCliCommandExecutor();
        for (ExecutablePO executablePO : jobs) {
            try {
                jobInfoDao.updateJob(executablePO.getUuid(), this::resumeRunningJob);
            } catch (Exception e) {
                logger.warn("Failed to resume running job {}", executablePO.getUuid(), e);
            }
            killRemoteProcess(executablePO, exe);
        }
    }

    private boolean resumeRunningJob(ExecutablePO po) {
        boolean result = false;
        if (po.getOutput().getStatus().equalsIgnoreCase(ExecutableState.RUNNING.toString())) {
            Map<String, String> info = Maps.newHashMap();
            if (Objects.nonNull(po.getOutput().getInfo())) {
                info.putAll(po.getOutput().getInfo());
            }
            Optional.ofNullable(REMOVE_INFO).ifPresent(set -> set.forEach(info::remove));
            po.getOutput().setInfo(info);
            po.getOutput().setStatus(ExecutableState.READY.toString());
            po.getOutput().addEndTime(System.currentTimeMillis());
            result = true;
        }
        for (ExecutablePO task : Optional.ofNullable(po.getTasks()).orElse(Lists.newArrayList())) {
            result = resumeRunningJob(task) || result;
        }
        return result;
    }

    public List<AbstractExecutable> getExecutablesByStatusList(Set<ExecutableState> statusSet) {
        Preconditions.checkNotNull(statusSet);
        List<ExecutablePO> filterJobs = Lists.newArrayList(jobInfoDao.getJobs(project));
        if (CollectionUtils.isNotEmpty(statusSet)) {
            filterJobs.removeIf(job -> !statusSet.contains(ExecutableState.valueOf(job.getOutput().getStatus())));
        }
        return filterJobs.stream().map(this::fromPO).collect(Collectors.toList());
    }

    public List<AbstractExecutable> getPartialExecutablesByStatusList(Set<ExecutableState> statusSet, String modelId) {
        Preconditions.checkNotNull(statusSet);
        List<ExecutablePO> filterJobs = getExecutablePOByModelId(project, modelId);
        if (CollectionUtils.isNotEmpty(statusSet)) {
            filterJobs.removeIf(job -> !statusSet.contains(ExecutableState.valueOf(job.getOutput().getStatus())));
        }
        return filterJobs.stream().map(this::fromPO).collect(Collectors.toList());
    }

    public List<ExecutablePO> getRunningJobs(int priority) {
        return jobInfoDao.getJobs(null).stream().filter(po -> {
            Output output = getOutput(po.getId());
            return ExecutablePO.isHigherPriority(po.getPriority(), priority) && output.getState().isProgressing();
        }).collect(Collectors.toList());
    }

    public Set<String> getYarnApplicationJobs(String id) {
        ExecutablePO executablePO = jobInfoDao.getExecutablePOByUuid(id);
        String appIds = executablePO.getOutput().getInfo().getOrDefault(YARN_APP_IDS, "");
        return StringUtils.isEmpty(appIds) ? new TreeSet<>()
                : new TreeSet<>(Arrays.asList(appIds.split(YARN_APP_IDS_DELIMITER)));
    }

    public long getLastSuccessExecDurationByModel(String modelId, List<ExecutablePO> jobs, JobTypeEnum... jobTypes) {
        List<ExecutablePO> executables = listExecutablePOByModelAndStatus(modelId,
                state -> ExecutableState.SUCCEED == state, jobs, jobTypes);
        if (CollectionUtils.isEmpty(executables)) {
            return 0L;
        }
        return executables.stream().max(Comparator.comparingLong(exec -> exec.getOutput().getEndTime()))
                .map(exec -> AbstractExecutable.getDuration(getOutput(exec.getId()))).orElse(0L);
    }

    public long getMaxDurationRunningExecDurationByModel(String modelId, List<ExecutablePO> jobs,
            JobTypeEnum... jobTypes) {
        List<ExecutablePO> executables = listExecutablePOByModelAndStatus(modelId,
                state -> ExecutableState.RUNNING == state, jobs, jobTypes);
        if (CollectionUtils.isEmpty(executables)) {
            return 0L;
        }
        return executables.stream().map(exec -> AbstractExecutable.getDuration(getOutput(exec.getId())))
                .max(Long::compareTo).orElse(0L);
    }

    public List<ExecutablePO> listPartialExec(String modelId, Predicate<ExecutableState> predicate,
            JobTypeEnum... jobTypes) {
        if (jobTypes == null) {
            return Lists.newArrayList();
        }
        List<JobTypeEnum> jobTypeList = Lists.newArrayList(jobTypes);
        return getExecutablePOByModelId(project, modelId).stream() //
                .filter(job -> job.getJobType() != null) //
                .filter(job -> jobTypeList.contains(job.getJobType())) //
                .filter(job -> predicate.test(ExecutableState.valueOf(job.getOutput().getStatus()))) //
                .collect(Collectors.toList());
    }

    public Output getStreamingOutputFromHDFS(String jobId) {
        return getStreamingOutputFromHDFS(jobId, LOG_DEFAULT_DISPLAY_HEAD_AND_TAIL_SIZE);
    }

    /**
     * get job output from hdfs log file
     * If the input value is 100, sample data will be returned
     * If the input value is not 100, the log InputStream will be returned
     *
     * @param jobId Current Job ID
     * @param nLines return msg line
     * @return job output
     */
    public Output getStreamingOutputFromHDFS(String jobId, int nLines) {

        Preconditions.checkArgument(StringUtils.isNotEmpty(jobId), "The jobId is empty");

        ExecutableOutputPO jobOutput = new ExecutableOutputPO();

        // streaming job driver log in hdfs directory
        String outputStoreDirPath = KylinConfig.getInstanceFromEnv().getStreamingJobTmpOutputStorePath(project, jobId);
        if (!isHdfsPathExists(outputStoreDirPath)) {
            logger.warn("The job log file on HDFS has not been generated yet, jobId: {}, filePath: {}", jobId,
                    outputStoreDirPath);
            jobOutput.setContent("");
            return parseOutput(jobOutput);
        }

        List<String> jobStartedList = getFilePathsFromHDFSDir(outputStoreDirPath);
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(jobStartedList),
                "The current job has not been started and no log has been generated: " + outputStoreDirPath);

        // get latest started job
        List<String> logFilePathList = getFilePathsFromHDFSDir(jobStartedList.get(jobStartedList.size() - 1), false);
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(logFilePathList),
                "There is no file in the current job HDFS directory: " + jobStartedList.get(jobStartedList.size() - 1));

        // get latest and first driver.{timestamp}.log
        String latestLogFilePath = logFilePathList.get(logFilePathList.size() - 1);
        String firstLogFilePath = logFilePathList.get(0);
        if (nLines == LOG_DEFAULT_DISPLAY_HEAD_AND_TAIL_SIZE) {
            jobOutput.setContent(getSampleDataFromBothHDFS(firstLogFilePath, latestLogFilePath,
                    LOG_DEFAULT_DISPLAY_HEAD_AND_TAIL_SIZE));
        } else {
            jobOutput.setContentStream(mergeHdfsFile(logFilePathList));
        }
        return parseOutput(jobOutput);
    }

    /**
     * List File Paths(order by filePath asc) From HDFS DIR resPath
     * If recursion is required, recursion the path
     *
     * @param resPath HDFS DIR PATH
     * @param recursive Recursive or not
     * @return List File Paths From HDFS DIR
     */
    public List<String> getFilePathsFromHDFSDir(String resPath, boolean recursive) {
        try {
            List<String> fileList = Lists.newArrayList();
            FileSystem fs = HadoopUtil.getWorkingFileSystem();
            Path path = new Path(resPath);
            RemoteIterator<LocatedFileStatus> files = fs.listFiles(path, recursive);
            while (files.hasNext()) {
                fileList.add(files.next().getPath().toString());
            }
            Collections.sort(fileList);
            return fileList;
        } catch (IOException e) {
            logger.error("get file paths from hdfs [{}] failed!", resPath, e);
            throw new KylinException(FILE_NOT_EXIST, e);
        }
    }

    public List<String> getFilePathsFromHDFSDir(String resPath) {
        try {
            List<String> fileList = Lists.newArrayList();
            FileSystem fs = HadoopUtil.getWorkingFileSystem();
            Path path = new Path(resPath);
            FileStatus[] fileStatuses = fs.listStatus(path);

            for (FileStatus fileStatus : fileStatuses) {
                fileList.add(fileStatus.getPath().toString());
            }
            Collections.sort(fileList);
            return fileList;
        } catch (IOException e) {
            logger.error("get file paths from hdfs [{}] failed!", resPath, e);
            throw new KylinException(FILE_NOT_EXIST, e);
        }
    }

    /**
     * merge sorted inputStreams
     * @param logPathList
     * @return
     */
    public InputStream mergeHdfsFile(List<String> logPathList) {
        Vector<InputStream> inputStreamVector = new Vector<>();
        logPathList.forEach(path -> inputStreamVector.add(getLogStream(path)));
        return new SequenceInputStream(inputStreamVector.elements());
    }

    // for ut only
    @VisibleForTesting
    public void removeBreakPoints(String taskOrJobId) {
        val jobId = extractJobId(taskOrJobId);
        val executablePO = jobInfoDao.getExecutablePOByUuid(jobId);
        if (Objects.isNull(executablePO)) {
            return;
        }

        if (Objects.equals(taskOrJobId, jobId)) {
            jobInfoDao.updateJob(jobId, job -> {
                job.getParams().remove(NBatchConstants.P_BREAK_POINT_LAYOUTS);
                return true;
            });
        } else {
            jobInfoDao.updateJob(jobId, job -> {
                job.getTasks().stream().filter(t -> t.getId().equals(taskOrJobId))
                        .forEach(t -> t.getParams().remove(NBatchConstants.P_BREAK_POINT_LAYOUTS));
                return true;
            });
        }
    }

    public List<JobInfo> fetchNotFinalJobsByTypes(String project, List<String> jobTypes, List<String> subjects) {
        return fetchJobsByTypesAndStates(project, jobTypes, subjects, ExecutableState.getNotFinalStates());
    }

    public List<JobInfo> fetchJobsByTypesAndStates(String project, List<String> jobTypes, List<String> subjects,
            List<ExecutableState> states) {
        JobMapperFilter mapperFilter = JobMapperFilter.builder().jobNames(jobTypes).statuses(states).subjects(subjects)
                .project(project).build();
        return jobInfoDao.getJobInfoListByFilter(mapperFilter);
    }

    public List<AbstractExecutable> getNotFinalExecutablesByType(List<JobTypeEnum> jobTypeEnums) {
        JobMapperFilter jobMapperFilter = new JobMapperFilter();
        jobMapperFilter.setProject(project);
        jobMapperFilter.setStatuses(ExecutableState.getNotFinalStates());
        if (CollectionUtils.isNotEmpty(jobTypeEnums)) {
            jobMapperFilter.setJobNames(
                    jobTypeEnums.stream().map(jobTypeEnum -> jobTypeEnum.name()).collect(Collectors.toList()));
        }
        List<JobInfo> jobInfoList = jobInfoDao.getJobInfoListByFilter(jobMapperFilter);
        return jobInfoList.stream().map(jobInfo -> fromPO(JobInfoUtil.deserializeExecutablePO(jobInfo)))
                .collect(Collectors.toList());
    }

    public List<JobInfo> fetchJobsByFilter(JobMapperFilter filter) {
        return jobInfoDao.getJobInfoListByFilter(filter);
    }

    public List<AbstractExecutable> jobInfoToExecutable(List<JobInfo> jobInfoList) {
        return jobInfoList.stream().map(JobInfoUtil::deserializeExecutablePO).map(executablePO -> fromPO(executablePO))
                .collect(Collectors.toList());
    }
}

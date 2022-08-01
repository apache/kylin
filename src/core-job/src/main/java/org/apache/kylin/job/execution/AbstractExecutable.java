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

import static org.apache.kylin.job.constant.ExecutableConstants.MR_JOB_ID;
import static org.apache.kylin.job.constant.ExecutableConstants.YARN_APP_ID;
import static org.apache.kylin.job.constant.ExecutableConstants.YARN_APP_URL;

import java.util.Arrays;
import java.util.Collections;
import java.util.IllegalFormatException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.metrics.MetricsCategory;
import org.apache.kylin.common.metrics.MetricsGroup;
import org.apache.kylin.common.metrics.MetricsName;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.MailHelper;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.common.util.ThrowableUtils;
import org.apache.kylin.job.constant.JobIssueEnum;
import org.apache.kylin.job.dao.ExecutableOutputPO;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.exception.JobStoppedException;
import org.apache.kylin.job.exception.JobStoppedNonVoluntarilyException;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.guava20.shaded.common.base.MoreObjects;
import io.kyligence.kap.shaded.curator.org.apache.curator.shaded.com.google.common.base.Throwables;
import lombok.Getter;
import lombok.Setter;
import lombok.val;
import lombok.var;
import lombok.experimental.Delegate;

/**
 */
public abstract class AbstractExecutable implements Executable {

    public interface Callback {
        void process() throws Exception;

        default void onProcessError(Throwable throwable) {
        }
    }

    protected static final String SUBMITTER = "submitter";
    protected static final String NOTIFY_LIST = "notify_list";
    protected static final String PARENT_ID = "parentId";
    public static final String RUNTIME_INFO = "runtimeInfo";
    public static final String DEPENDENT_FILES = "dependentFiles";
    public static final String SPARK_YARN_QUEUE = "spark.yarn.queue";

    protected static final Logger logger = LoggerFactory.getLogger(AbstractExecutable.class);

    protected int retry = 0;

    @Getter
    @Setter
    private String name;
    @Getter
    @Setter
    private JobTypeEnum jobType;

    @Getter
    @Setter
    private String logPath;

    @Setter
    @Getter
    private String targetSubject; //uuid of the model or table identity if table sampling

    @Setter
    @Getter
    private List<String> targetSegments = Lists.newArrayList();//uuid of related segments

    @Getter
    @Setter
    private String id;

    @Getter
    @Setter
    private boolean resumable = false;

    @Delegate
    private ExecutableParams executableParams = new ExecutableParams();
    protected String project;
    protected ExecutableContext context;

    @Getter
    @Setter
    private Map<String, Object> runTimeInfo = Maps.newHashMap();

    @Setter
    @Getter
    private Set<Long> targetPartitions = Sets.newHashSet();

    public boolean isBucketJob() {
        return CollectionUtils.isNotEmpty(targetPartitions);
    }

    @Getter
    @Setter
    private int priority = ExecutablePO.DEFAULT_PRIORITY;

    @Getter
    @Setter
    private Object tag;

    @Getter
    @Setter
    private int stepId = -1;

    public String getTargetModelAlias() {
        val modelManager = NDataModelManager.getInstance(getConfig(), getProject());
        NDataModel dataModelDesc = NDataModelManager.getInstance(getConfig(), getProject())
                .getDataModelDesc(targetSubject);
        if (dataModelDesc != null) {
            if (modelManager.isModelBroken(targetSubject)) {
                return modelManager.getDataModelDescWithoutInit(targetSubject).getAlias();
            } else {
                return dataModelDesc.getFusionModelAlias();
            }
        }

        return null;
    }

    public String getTargetModelId() {
        val modelManager = NDataModelManager.getInstance(getConfig(), getProject());
        NDataModel dataModelDesc = NDataModelManager.getInstance(getConfig(), getProject())
                .getDataModelDesc(targetSubject);
        if (dataModelDesc == null)
            return null;
        return modelManager.isModelBroken(targetSubject)
                ? modelManager.getDataModelDescWithoutInit(targetSubject).getId()
                : dataModelDesc.getId();
    }

    public String getTargetSubjectAlias() {
        return getTargetModelAlias();
    }

    public AbstractExecutable() {
        setId(RandomUtil.randomUUIDStr());
    }

    public AbstractExecutable(Object notSetId) {
    }

    public void cancelJob() {
    }

    /**
     * jude it will cause segment-holes or not if discard this job
     * @return true by default
     */
    public boolean safetyIfDiscard() {
        return true;
    }

    protected KylinConfig getConfig() {
        return KylinConfig.getInstanceFromEnv();
    }

    protected NExecutableManager getManager() {
        return getExecutableManager(project);
    }

    /**
     * for job steps, they need to update step status and throw exception
     * so they need to use wrapWithCheckQuit
     */
    protected void wrapWithCheckQuit(Callback f) throws JobStoppedException {
        boolean tryAgain = true;

        while (tryAgain) {
            checkNeedQuit(true);

            // in this short period user might changed job state, say restart
            // if a worker thread is unaware of this, it may go ahead and register step 1 as succeed here.
            // However the user expects a total RESTART

            tryAgain = false;
            try {
                EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                    checkNeedQuit(false);
                    f.process();
                    return null;
                }, project, UnitOfWork.DEFAULT_MAX_RETRY, getEpochId(), getTempLockName());
            } catch (Exception e) {
                if (Throwables.getCausalChain(e).stream().anyMatch(x -> x instanceof JobStoppedException)) {
                    // "in this short period user might changed job state" happens
                    logger.info("[LESS_LIKELY_THINGS_HAPPENED] JobStoppedException thrown from in a UnitOfWork", e);
                    tryAgain = true;
                } else {
                    throw e;
                }
            }
        }
    }

    protected void onExecuteStart() throws JobStoppedException {
        wrapWithCheckQuit(() -> {
            updateJobOutput(project, getId(), ExecutableState.RUNNING, null, null, null);
        });
    }

    protected void onExecuteFinished(ExecuteResult result) throws ExecuteException {
        logger.info("Execute finished {}, state:{}", this.getDisplayName(), result.state());
        MetricsGroup.hostTagCounterInc(MetricsName.JOB_STEP_ATTEMPTED, MetricsCategory.PROJECT, project, retry);
        if (result.succeed()) {
            wrapWithCheckQuit(() -> {
                updateJobOutput(project, getId(), ExecutableState.SUCCEED, result.getExtraInfo(), result.output(),
                        null);
            });
        } else if (result.skip()) {
            wrapWithCheckQuit(() -> {
                updateJobOutput(project, getId(), ExecutableState.SKIP, result.getExtraInfo(), result.output(), null);
            });
        } else {
            MetricsGroup.hostTagCounterInc(MetricsName.JOB_FAILED_STEP_ATTEMPTED, MetricsCategory.PROJECT, project,
                    retry);
            wrapWithCheckQuit(() -> {
                updateJobOutput(project, getId(), ExecutableState.ERROR, result.getExtraInfo(), result.getErrorMsg(),
                        result.getShortErrMsg(), this::onExecuteErrorHook);
            });
            throw new ExecuteException(result.getThrowable());
        }
    }

    protected void onExecuteStopHook() {
        onExecuteErrorHook(getId());
    }

    protected void onExecuteErrorHook(String jobId) {
        // At present, only instance of DefaultChainedExecutableOnModel take full advantage of this method.
    }

    protected long getEpochId() {
        return context == null ? -1 : context.getEpochId();
    }

    public void updateJobOutput(String project, String jobId, ExecutableState newStatus, Map<String, String> info,
            String output, Consumer<String> hook) {
        updateJobOutput(project, jobId, newStatus, info, output, null, hook);
    }

    public void updateJobOutput(String project, String jobId, ExecutableState newStatus, Map<String, String> info,
            String output, String failedMsg, Consumer<String> hook) {
        updateJobOutput(project, jobId, newStatus, info, output, this.getLogPath(), failedMsg, hook);
    }

    public void updateJobOutput(String project, String jobId, ExecutableState newStatus, Map<String, String> info,
            String output, String logPath, String failedMsg, Consumer<String> hook) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NExecutableManager executableManager = getExecutableManager(project);
            val existedInfo = executableManager.getOutput(jobId).getExtra();
            if (info != null) {
                existedInfo.putAll(info);
            }

            //The output will be stored in HDFS,not in RS
            if (this instanceof ChainedStageExecutable) {
                if (newStatus == ExecutableState.SUCCEED) {
                    executableManager.makeStageSuccess(jobId);
                } else if (newStatus == ExecutableState.ERROR) {
                    executableManager.makeStageError(jobId);
                }
            }
            executableManager.updateJobOutput(jobId, newStatus, existedInfo, null, null, 0, failedMsg);
            if (hook != null) {
                hook.accept(jobId);
            }
            return null;
        }, project, UnitOfWork.DEFAULT_MAX_RETRY, getEpochId(), getTempLockName());

        //write output to HDFS
        updateJobOutputToHDFS(project, jobId, output, logPath);
    }

    private static void updateJobOutputToHDFS(String project, String jobId, String output, String logPath) {
        NExecutableManager nExecutableManager = getExecutableManager(project);
        ExecutableOutputPO jobOutput = nExecutableManager.getJobOutput(jobId);
        if (null != output) {
            jobOutput.setContent(output);
        }
        if (null != logPath) {
            jobOutput.setLogPath(logPath);
        }
        String outputHDFSPath = KylinConfig.getInstanceFromEnv().getJobTmpOutputStorePath(project, jobId);

        nExecutableManager.updateJobOutputToHDFS(outputHDFSPath, jobOutput);
    }

    protected static NExecutableManager getExecutableManager(String project) {
        return NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
    }

    @Override
    public final ExecuteResult execute(ExecutableContext executableContext) throws ExecuteException {
        logger.info("Executing AbstractExecutable {}", this.getDisplayName());
        this.context = executableContext;
        ExecuteResult result;

        onExecuteStart();

        do {
            if (retry > 0) {
                pauseOnRetry();
                logger.info("Retrying for the {}th time ", retry);
            }

            try {
                result = wrapWithExecuteException(() -> doWork(executableContext));
            } catch (JobStoppedException jse) {
                // job quits voluntarily or non-voluntarily, in this case, the job is "finished"
                // we createSucceed() to run onExecuteFinished()
                result = ExecuteResult.createSucceed();
            } catch (Exception e) {
                result = ExecuteResult.createError(e);
            }

            retry++;

        } while (needRetry(this.retry, result.getThrowable())); //exception in ExecuteResult should handle by user itself.
        //check exception in result to avoid retry on ChainedExecutable(only need retry on subtask actually)

        onExecuteFinished(result);
        return result;
    }

    protected void checkNeedQuit(boolean applyChange) throws JobStoppedException {
        // non voluntarily
        abortIfJobStopped(applyChange);
    }

    /**
     * For non-chained executable, depend on its parent(instance of DefaultChainedExecutable).
     */
    public boolean checkSuicide() {
        final AbstractExecutable parent = getParent();
        if (parent == null) {
            return false;
        } else {
            return parent.checkSuicide();
        }
    }

    // If job need check external status change, override this method, by default return true.
    protected boolean needCheckState() {
        return true;
    }

    /**
     * will throw exception if necessary!
     */
    public void abortIfJobStopped(boolean applyChange) throws JobStoppedException {
        if (!needCheckState()) {
            return;
        }

        boolean aborted = EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            boolean abort = false;
            val parent = getParent();
            ExecutableState state = parent.getStatus();
            switch (state) {
            case READY:
            case PAUSED:
            case DISCARDED:
                //if a job is restarted(all steps' status changed to READY) or paused or discarded, the old thread may still be alive and attempt to update job output
                //in this case the old thread should fail itself by calling this
                if (applyChange) {
                    logger.debug("abort {} because parent job is {}", getId(), state);
                    updateJobOutput(project, getId(), state, null, null, null);
                }
                abort = true;
                break;
            default:
                break;
            }
            return abort;
        }, project, UnitOfWork.DEFAULT_MAX_RETRY, getEpochId(), getTempLockName());
        if (aborted) {
            throw new JobStoppedNonVoluntarilyException();
        }
    }

    // Retry will happen in below cases:
    // 1) if property "kylin.job.retry-exception-classes" is not set or is null, all jobs with exceptions will retry according to the retry times.
    // 2) if property "kylin.job.retry-exception-classes" is set and is not null, only jobs with the specified exceptions will retry according to the retry times.
    public boolean needRetry(int retry, Throwable t) {
        if (t == null || this instanceof DefaultChainedExecutable)
            return false;

        if (retry > KylinConfig.getInstanceFromEnv().getJobRetry())
            return false;

        if (ThrowableUtils.isInterruptedException(t))
            return false;

        return isRetryableException(t.getClass().getName());
    }

    // pauseOnRetry should only works when retry has been triggered
    private void pauseOnRetry() {
        int interval = KylinConfig.getInstanceFromEnv().getJobRetryInterval();
        logger.info("Pause {} milliseconds before retry", interval);
        try {
            TimeUnit.MILLISECONDS.sleep(interval);
        } catch (InterruptedException e) {
            logger.error("Job retry was interrupted, details: {}", e);
            Thread.currentThread().interrupt();
        }
    }

    private static boolean isRetryableException(String exceptionName) {
        String[] jobRetryExceptions = KylinConfig.getInstanceFromEnv().getJobRetryExceptions();
        return ArrayUtils.isEmpty(jobRetryExceptions) || ArrayUtils.contains(jobRetryExceptions, exceptionName);
    }

    protected abstract ExecuteResult doWork(ExecutableContext context) throws ExecuteException;

    @Override
    public boolean isRunnable() {
        return this.getStatus() == ExecutableState.READY;
    }

    public String getDisplayName() {
        return this.name + " (" + this.id + ")";
    }

    @Override
    public final ExecutableState getStatus() {
        NExecutableManager manager = getManager();
        return manager.getOutput(this.getId()).getState();
    }

    public final long getLastModified() {
        return getLastModified(getOutput());
    }

    public static long getLastModified(Output output) {
        return output.getLastModified();
    }

    public final long getByteSize() {
        return getByteSize(getOutput());
    }

    public static long getByteSize(Output output) {
        return output.getByteSize();
    }

    public void notifyUserIfNecessary(NDataLayout[] addOrUpdateCuboids) {
        boolean hasEmptyLayout = false;
        for (NDataLayout dataCuboid : addOrUpdateCuboids) {
            if (dataCuboid.getRows() == 0) {
                hasEmptyLayout = true;
                break;
            }
        }
        if (hasEmptyLayout) {
            notifyUserJobIssue(JobIssueEnum.LOAD_EMPTY_DATA);
        }
    }

    public final void notifyUserJobIssue(JobIssueEnum jobIssue) {
        Preconditions.checkState(
                (this instanceof DefaultChainedExecutable) || this.getParent() instanceof DefaultChainedExecutable);
        val projectConfig = NProjectManager.getInstance(getConfig()).getProject(project).getConfig();
        boolean needNotification = true;
        switch (jobIssue) {
        case JOB_ERROR:
            needNotification = projectConfig.getJobErrorNotificationEnabled();
            break;
        case LOAD_EMPTY_DATA:
            needNotification = projectConfig.getJobDataLoadEmptyNotificationEnabled();
            break;
        case SOURCE_RECORDS_CHANGE:
            needNotification = projectConfig.getJobSourceRecordsChangeNotificationEnabled();
            break;
        default:
            throw new IllegalArgumentException(String.format(Locale.ROOT, "no process for jobIssue: %s.", jobIssue));
        }
        if (!needNotification) {
            return;
        }
        List<String> users;
        users = getAllNotifyUsers(projectConfig);
        if (this instanceof DefaultChainedExecutable) {
            MailHelper.notifyUser(projectConfig, EmailNotificationContent.createContent(jobIssue, this), users);
        } else {
            MailHelper.notifyUser(projectConfig, EmailNotificationContent.createContent(jobIssue, this.getParent()),
                    users);
        }
    }

    public void setSparkYarnQueueIfEnabled(String project, String yarnQueue) {
        ProjectInstance proj = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(project);
        KylinConfig config = proj.getConfig();
        // TODO check if valid queue
        if (config.isSetYarnQueueInTaskEnabled() && config.getYarnQueueInTaskAvailable().contains(yarnQueue)) {
            this.setSparkYarnQueue(yarnQueue);
        }
    }

    public final AbstractExecutable getParent() {
        return getManager().getJob(getParam(PARENT_ID));
    }

    public final String getProject() {
        if (project == null) {
            throw new IllegalStateException("project is not set for abstract executable " + getId());
        }
        return project;
    }

    public final void setProject(String project) {
        this.project = project;
    }

    @Override
    public final Output getOutput() {
        return getManager().getOutput(getId());
    }

    //will modify input info
    public Map<String, String> makeExtraInfo(Map<String, String> info) {
        if (info == null) {
            return Maps.newHashMap();
        }

        // post process
        if (info.containsKey(MR_JOB_ID) && !info.containsKey(YARN_APP_ID)) {
            String jobId = info.get(MR_JOB_ID);
            if (jobId.startsWith("job_")) {
                info.put(YARN_APP_ID, jobId.replace("job_", "application_"));
            }
        }

        if (info.containsKey(YARN_APP_ID)
                && !org.apache.commons.lang3.StringUtils.isEmpty(getConfig().getJobTrackingURLPattern())) {
            String pattern = getConfig().getJobTrackingURLPattern();
            try {
                String newTrackingURL = String.format(Locale.ROOT, pattern, info.get(YARN_APP_ID));
                info.put(YARN_APP_URL, newTrackingURL);
            } catch (IllegalFormatException ife) {
                logger.error("Illegal tracking url pattern: {}", getConfig().getJobTrackingURLPattern());
            }
        }

        return info;
    }

    public static long getStartTime(Output output) {
        return output.getStartTime();
    }

    public static long getEndTime(Output output) {
        return output.getEndTime();
    }

    protected final Map<String, String> getExtraInfo() {
        return getOutput().getExtra();
    }

    public final long getStartTime() {
        return getStartTime(getOutput());
    }

    public final long getCreateTime() {
        return getManager().getCreateTime(getId());
    }

    public static long getCreateTime(Output output) {
        return output.getCreateTime();
    }

    public final long getEndTime() {
        return getEndTime(getOutput());
    }

    // just using to get job duration in get job list
    public long getDurationFromStepOrStageDurationSum() {
        var duration = getDuration();
        if (this instanceof ChainedExecutable) {
            val tasks = ((ChainedExecutable) this).getTasks();
            val jobAtomicDuration = new AtomicLong(0);
            tasks.forEach(task -> {
                var taskDuration = task.getDuration();
                if (task instanceof ChainedStageExecutable) {
                    val stagesMap = ((ChainedStageExecutable) task).getStagesMap();
                    if (stagesMap.size() == 1) {
                        for (Map.Entry<String, List<StageBase>> entry : stagesMap.entrySet()) {
                            taskDuration = entry.getValue().stream()
                                    .map(stage -> getDuration(stage.getOutput(entry.getKey()))) //
                                    .mapToLong(Long::valueOf) //
                                    .sum();
                        }
                    }
                }
                jobAtomicDuration.addAndGet(taskDuration);
            });
            duration = jobAtomicDuration.get();
        }
        return duration;
    }

    public long getDuration() {
        return getDuration(getOutput());
    }

    public static long getDuration(Output output) {
        if (output.getDuration() != 0) {
            var duration = output.getDuration();
            if (ExecutableState.RUNNING == output.getState()) {
                duration = duration + System.currentTimeMillis() - output.getLastModified();
            }
            return duration;
        }
        if (output.getStartTime() == 0) {
            return 0;
        }
        return output.getEndTime() == 0 ? System.currentTimeMillis() - output.getStartTime()
                : output.getEndTime() - output.getStartTime();
    }

    public long getWaitTime() {
        Output output = getOutput();
        long startTime = output.getStartTime();

        long lastTaskEndTime = output.getCreateTime();
        var lastTaskStatus = getStatus();

        int stepId = getStepId();

        // get end_time of last task
        if (getParent() instanceof DefaultChainedExecutable) {
            val parentExecutable = (DefaultChainedExecutable) getParent();
            val lastExecutable = parentExecutable.getSubTaskByStepId(stepId - 1);

            lastTaskEndTime = lastExecutable.map(AbstractExecutable::getEndTime)
                    .orElse(parentExecutable.getOutput().getCreateTime());

            lastTaskStatus = lastExecutable.map(AbstractExecutable::getStatus).orElse(parentExecutable.getStatus());
        }

        //if last task is not end, wait_time is 0
        if (stepId > 0 && (lastTaskEndTime == 0 || lastTaskStatus != ExecutableState.SUCCEED)) {
            return 0;
        }

        if (startTime == 0) {
            if (getParent() != null && getParent().getStatus() == ExecutableState.DISCARDED) {
                // job is discarded before started
                startTime = getParent().getEndTime();
            } else {
                //the job/task is not started, use the current time
                startTime = System.currentTimeMillis();
            }
        }

        return startTime - lastTaskEndTime;
    }

    public long getTotalDurationTime() {
        return getDuration() + getWaitTime();
    }

    public final Set<String> getDependentFiles() {
        val value = getExtraInfo().getOrDefault(DEPENDENT_FILES, "");
        if (StringUtils.isEmpty(value)) {
            return Sets.newHashSet();
        }
        return Sets.newHashSet(value.split(","));
    }

    /**
     * job get DISCARD or PAUSE without job fetcher's awareness
     *
     * SUICIDE is not the case, as it is awared by job fetcher
     *
     */
    protected final boolean isStoppedNonVoluntarily() {
        Preconditions.checkState(getParent() == null);
        final ExecutableState status = getOutput().getState();
        return status.isStoppedNonVoluntarily();
    }

    protected boolean needRetry() {
        return this.retry <= getConfig().getJobRetry();
    }

    public Set<String> getDependencies(KylinConfig config) {
        return Sets.newHashSet();
    }

    private static int computeTableAnalyzeMemory() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        return config.getSparkEngineDriverMemoryTableSampling();
    }

    private static int computeSnapshotAnalyzeMemory() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        return config.getSparkEngineDriverMemorySnapshotBuilding();
    }

    public int computeStepDriverMemory() {
        if (getJobType() == JobTypeEnum.TABLE_SAMPLING) {
            return computeTableAnalyzeMemory();
        }

        if (getJobType() == JobTypeEnum.SNAPSHOT_BUILD || getJobType() == JobTypeEnum.SNAPSHOT_REFRESH) {
            return computeSnapshotAnalyzeMemory();
        }

        String layouts = getParam(NBatchConstants.P_LAYOUT_IDS);
        if (layouts != null) {
            return computeDriverMemory(StringUtil.splitAndTrim(layouts, ",").length);
        }
        return 0;
    }

    public static Integer computeDriverMemory(Integer cuboidNum) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        int[] driverMemoryStrategy = config.getSparkEngineDriverMemoryStrategy();
        List strategy = Lists.newArrayList(cuboidNum);
        Arrays.stream(driverMemoryStrategy).forEach(x -> strategy.add(Integer.valueOf(x)));
        Collections.sort(strategy);
        int index = strategy.indexOf(cuboidNum);
        int driverMemoryMaximum = config.getSparkEngineDriverMemoryMaximum();
        int driverMemoryBase = config.getSparkEngineDriverMemoryBase();

        driverMemoryBase += driverMemoryBase * index;
        return Math.min(driverMemoryBase, driverMemoryMaximum);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("id", getId()).add("name", getName()).add("state", getStatus())
                .toString();
    }

    public <T> T wrapWithExecuteException(final Callable<T> lambda) throws ExecuteException {
        Exception exception = null;
        try {
            return lambda.call();
        } catch (ExecuteException e) {
            exception = e;
            throw e;
        } catch (Exception e) {
            exception = e;
            throw new ExecuteException(e);
        } finally {
            if (null != exception) {
                wrapWithExecuteExceptionUpdateJobError(exception);
            }
        }
    }

    protected void wrapWithExecuteExceptionUpdateJobError(Exception exception) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            getExecutableManager(project).updateJobError(getId(), getId(), null,
                    ExceptionUtils.getStackTrace(exception), exception.getMessage());
            return null;
        }, project, UnitOfWork.DEFAULT_MAX_RETRY, getEpochId(), getTempLockName());
    }

    public final String getTempLockName() {
        if (getParentId() == null) {
            return getId();
        }
        return getParentId();
    }
}

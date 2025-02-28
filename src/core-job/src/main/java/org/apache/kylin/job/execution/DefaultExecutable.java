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

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.mail.MailNotificationType;
import org.apache.kylin.common.scheduler.EventBusFactory;
import org.apache.kylin.common.scheduler.JobFinishedNotifier;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.job.JobContext;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.exception.ExecuteRuntimeException;
import org.apache.kylin.job.exception.JobStoppedException;

import lombok.val;

/**
 */
public class DefaultExecutable extends AbstractExecutable implements ChainedExecutable, DagExecutable {

    private final List<AbstractExecutable> subTasks = Lists.newArrayList();

    public DefaultExecutable() {
        super();
    }

    public DefaultExecutable(Object notSetId) {
        super(notSetId);
    }

    /**
     * @param config This param will use in overridable method
     */
    public Set<String> getMetadataDumpList(KylinConfig config) {
        return Collections.emptySet();
    }

    @Override
    public ExecuteResult doWork(JobContext context) throws ExecuteException {
        List<Executable> executables = getTasks().stream().map(Executable.class::cast).collect(Collectors.toList());
        switch (getJobSchedulerMode()) {
        case DAG:
            logger.info("Execute in DAG mode.");
            dagSchedule(executables, context);
            break;
        case CHAIN:
        default:
            logger.info("Execute in CHAIN mode.");
            chainedSchedule(executables, context);
            break;
        }
        return ExecuteResult.createSucceed();

    }

    @Override
    public void chainedSchedule(List<Executable> executables, JobContext context) throws ExecuteException {
        for (Executable subTask : executables) {
            executeStep(subTask, context);
        }
    }

    @Override
    public void dagSchedule(List<Executable> executables, JobContext context) throws ExecuteException {
        // top step
        final List<Executable> dagTopExecutables = executables.stream()
                .filter(executable -> StringUtils.isBlank(executable.getPreviousStep())).collect(Collectors.toList());
        // all step map
        final Map<String, Executable> dagExecutablesMap = executables.stream()
                .collect(Collectors.toMap(Executable::getId, executable -> executable));
        resetJobErrorMessage();
        dagExecute(dagTopExecutables, dagExecutablesMap, context);
        waitAllDagExecutablesFinished(executables);
    }

    private void resetJobErrorMessage() {
        getManager().updateJobError(getId(), null, null, null, null);
    }

    public void dagExecute(List<Executable> dagExecutables, Map<String, Executable> dagExecutablesMap,
            JobContext context) throws ExecuteException {
        try {
            if (dagExecutables.size() == 1) {
                logger.info("dagExecute execute single : {}", dagExecutables.get(0).getDisplayName());
                executeDagExecutable(dagExecutablesMap, dagExecutables.get(0), context);
                return;
            }
            dagExecutables
                    .forEach(executable -> new ExecutableThread(this, dagExecutablesMap, context, executable).start());
        } catch (Exception e) {
            throw new ExecuteException(e);
        }
    }

    public void executeDagExecutable(Map<String, Executable> dagExecutablesMap, Executable executable,
            JobContext context) {
        try {
            logger.info("execute dag executable : {}-{} -> {}", Thread.currentThread().getName(),
                    Thread.currentThread().getId(), executable.getDisplayName());
            // check previous step
            checkPreviousStep(dagExecutablesMap, executable);

            // execute
            executeStep(executable, context);

            // run next steps, if it has
            executeNextSteps(dagExecutablesMap, executable, context);
        } catch (Exception e) {
            throw new ExecuteRuntimeException(e);
        }
    }

    private void executeNextSteps(Map<String, Executable> dagExecutablesMap, Executable executable, JobContext context)
            throws ExecuteException {
        final Set<String> nextSteps = executable.getNextSteps();
        if (CollectionUtils.isNotEmpty(nextSteps)) {
            List<Executable> nextExecutables = nextSteps.stream().map(dagExecutablesMap::get)
                    .collect(Collectors.toList());
            dagExecute(nextExecutables, dagExecutablesMap, context);
        }
    }

    public void waitAllDagExecutablesFinished(List<Executable> dagExecutables) {
        while (true) {
            try {
                val runningCount = dagExecutables.stream()
                        .filter(executable -> executable.getStatus() == ExecutableState.RUNNING
                                || executable.getStatus() == ExecutableState.PENDING
                                || executable.getStatus() == ExecutableState.READY)
                        .count();
                if (runningCount == 0) {
                    logger.debug("{} all next step finished", dagExecutables.get(0).getPreviousStep());
                    break;
                }
                Set<ExecutableState> finishedState = Sets.newHashSet(ExecutableState.ERROR, ExecutableState.PAUSED,
                        ExecutableState.DISCARDED);
                long peerStepErrorCount = dagExecutables.stream()
                        .filter(step -> finishedState.contains(step.getStatus())).count();
                if (peerStepErrorCount != 0) {
                    logger.debug("{} next step has error", dagExecutables.get(0).getPreviousStep());
                    break;
                }
                TimeUnit.SECONDS.sleep(10);
            } catch (InterruptedException e) {
                logger.error("wait all next step success has error : {}", e.getMessage());
                Thread.currentThread().interrupt();
            }
        }
    }

    private void executeStep(Executable executable, JobContext context) throws ExecuteException {
        if (executable.isRunnable()) {
            executable.execute(context);
        } else if (executable.getStatus().isNotBad()) {
            logger.info("step {} is already succeed, skip it.", executable.getDisplayName());
        } else {
            throw new IllegalStateException("invalid subtask state, sub task:" + executable.getDisplayName()
                    + ", state:" + executable.getStatus());
        }
    }

    private void checkPreviousStep(Map<String, Executable> dagExecutablesMap, Executable executable) {
        if (StringUtils.isNotBlank(executable.getPreviousStep())) {
            final String previousStep = executable.getPreviousStep();
            final Executable previousExecutable = dagExecutablesMap.get(previousStep);
            if (ExecutableState.SUCCEED != previousExecutable.getStatus()) {
                throw new IllegalStateException("invalid subtask state, sub task:" + previousExecutable.getDisplayName()
                        + ", state:" + previousExecutable.getStatus());
            }
        }
    }

    @Override
    protected boolean needCheckState() {
        return false;
    }

    @Override
    protected void onExecuteStart() throws JobStoppedException {
        if (isStoppedNonVoluntarily() && ExecutableState.PENDING != getOutput().getState()) {
            //onExecuteStart will turn PENDING to RUNNING
            return;
        }
        updateJobOutput(project, getId(), ExecutableState.RUNNING, null, null, null);
    }

    @Override
    protected void onExecuteFinished(ExecuteResult result) throws JobStoppedException {
        ExecutableState state = checkState();
        logger.info("Job finished {}, state:{}", this.getDisplayName(), state);

        switch (state) {
        case SUCCEED:
            updateToFinalState(ExecutableState.SUCCEED, this::afterUpdateOutput, result.getShortErrMsg());
            onStatusChange(MailNotificationType.JOB_FINISHED);
            break;
        case DISCARDED:
            updateToFinalState(ExecutableState.DISCARDED, this::onExecuteDiscardHook, result.getShortErrMsg());
            break;
        case SUICIDAL:
            updateToFinalState(ExecutableState.SUICIDAL, this::onExecuteSuicidalHook, result.getShortErrMsg());
            onStatusChange(MailNotificationType.JOB_ERROR);
            break;
        case ERROR:
        case PAUSED:
        case READY:
            if (isStoppedNonVoluntarily()) {
                logger.info("Execute finished  {} which is stopped nonvoluntarily, state: {}", this.getDisplayName(),
                        getOutput().getState());
                break;
            }
            Consumer<String> hook = null;
            Map<String, String> info = null;
            String output = null;
            String shortErrMsg = null;
            if (state == ExecutableState.ERROR) {
                logger.warn("[UNEXPECTED_THINGS_HAPPENED] Unexpected ERROR state discovered here!!! {}",
                        result.getErrorMsg());
                info = result.getExtraInfo();
                output = result.getErrorMsg();
                hook = this::onExecuteErrorHook;
                shortErrMsg = result.getShortErrMsg();
            }
            updateJobOutput(getProject(), getId(), state, info, output, shortErrMsg, hook);
            if (state == ExecutableState.ERROR) {
                onStatusChange(MailNotificationType.JOB_ERROR);
            }
            break;
        default:
            throw new IllegalArgumentException("Illegal state when job finished: " + state);
        }

        // dispatch job-finished message out
        EventBusFactory.getInstance()
                .postSync(new JobFinishedNotifier(getId(), getProject(), getTargetSubject(), getDuration(),
                        state.toString(), getJobType().toString(), this.getSegmentIds(), this.getLayoutIds(),
                        getTargetPartitions(), getWaitTime(), this.getClass().getName(), this.getSubmitter(),
                        result.succeed(), getJobStartTime(), getJobEndTime(), getTag(), result.getThrowable()));
    }

    private ExecutableState checkState() {
        List<? extends Executable> jobs = getTasks();
        boolean allSucceed = true;
        boolean hasError = false;
        boolean hasDiscarded = false;
        boolean hasSuicidal = false;
        boolean hasPaused = false;
        for (Executable task : jobs) {
            logger.info("Sub-task finished {}, state: {}", task.getDisplayName(), task.getStatus());
            boolean taskSucceed = false;
            switch (task.getStatus()) {
            case RUNNING:
            case ERROR:
                hasError = true;
                break;
            case DISCARDED:
                hasDiscarded = true;
                break;
            case SUICIDAL:
                hasSuicidal = true;
                break;
            case PAUSED:
                hasPaused = true;
                break;
            case SUCCEED:
            case SKIP:
            case WARNING:
                taskSucceed = true;
                break;
            default:
                break;
            }
            allSucceed &= taskSucceed;
        }

        ExecutableState state;
        if (allSucceed) {
            state = ExecutableState.SUCCEED;
        } else if (hasDiscarded) {
            state = ExecutableState.DISCARDED;
        } else if (hasSuicidal) {
            state = ExecutableState.SUICIDAL;
        } else if (hasError) {
            state = ExecutableState.ERROR;
        } else if (hasPaused) {
            state = ExecutableState.PAUSED;
        } else {
            state = ExecutableState.PENDING;
        }
        return state;
    }

    private long getJobStartTime() {
        return subTasks.stream().map(AbstractExecutable::getStartTime).filter(t -> t != 0)
                .min(Comparator.comparingLong(t -> t)).orElse(0L);
    }

    private long getJobEndTime() {
        return subTasks.stream().map(AbstractExecutable::getEndTime).filter(t -> t != 0)
                .max(Comparator.comparingLong(t -> t)).orElse(System.currentTimeMillis());
    }

    @Override
    public long getWaitTime(ExecutablePO po) {
        return subTasks.stream().map(task -> task.getWaitTime(po)).mapToLong(Long::longValue).sum();
    }

    @Override
    public long getWaitTime() {
        return subTasks.stream().map(AbstractExecutable::getWaitTime).mapToLong(Long::longValue).sum();
    }

    @Override
    public long getDuration() {
        return subTasks.stream().map(AbstractExecutable::getDuration).mapToLong(Long::longValue).sum();
    }

    Optional<AbstractExecutable> getSubTaskByStepId(int stepId) {
        if (stepId < 0 || stepId >= subTasks.size()) {
            return Optional.empty();
        }
        return Optional.ofNullable(subTasks.get(stepId));
    }

    protected void onExecuteDiscardHook(String jobId) {
        // Hook method, default action is doing nothing
    }

    protected void onExecuteSuicidalHook(String jobId) {
        // Hook method, default action is doing nothing
    }

    private void updateToFinalState(ExecutableState finalState, Consumer<String> hook, String failedMsg) {
        //to final state, regardless of isStoppedNonVoluntarily, otherwise a paused job might fail to suicide
        if (!getOutput().getState().isFinalState()) {
            updateJobOutput(getProject(), getId(), finalState, null, null, failedMsg, hook);
        }
    }

    @Override
    public List<AbstractExecutable> getTasks() {
        return subTasks;
    }

    @Override
    protected boolean needRetry() {
        return false;
    }

    @Override
    public void addTask(AbstractExecutable executable) {
        int stepId = subTasks.size();

        executable.setId(getId() + "_" + String.format(Locale.ROOT, "%02d", stepId));
        executable.setParent(this);
        executable.setStepId(stepId);
        this.subTasks.add(executable);
    }

    @Override
    public <T extends AbstractExecutable> T getTask(Class<T> clz) {
        List<AbstractExecutable> tasks = getTasks();
        for (AbstractExecutable task : tasks) {
            if (task.getClass().equals(clz)) {
                return (T) task;
            }
        }
        return null;
    }

    protected void afterUpdateOutput(String jobId) {
        // just implement it
    }

    protected boolean onStatusChange(MailNotificationType notificationType) {
        KylinConfig config = getConfig();
        if (config.isMailEnabled()) {
            return super.notifyUser(notificationType);
        }
        return false;
    }
}

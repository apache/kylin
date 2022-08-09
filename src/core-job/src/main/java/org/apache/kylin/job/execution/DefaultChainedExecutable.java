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

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.scheduler.EventBusFactory;
import org.apache.kylin.common.scheduler.JobFinishedNotifier;
import org.apache.kylin.job.constant.JobIssueEnum;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.exception.JobStoppedException;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;

import io.kyligence.kap.guava20.shaded.common.collect.Lists;

/**
 */
public class DefaultChainedExecutable extends AbstractExecutable implements ChainedExecutable {

    private final List<AbstractExecutable> subTasks = Lists.newArrayList();

    public DefaultChainedExecutable() {
        super();
    }

    public DefaultChainedExecutable(Object notSetId) {
        super(notSetId);
    }

    public Set<String> getMetadataDumpList(KylinConfig config) {
        return Collections.emptySet();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        List<? extends Executable> executables = getTasks();
        for (Executable subTask : executables) {
            if (subTask.isRunnable()) {
                subTask.execute(context);
            } else if (ExecutableState.SUCCEED == subTask.getStatus()) {
                logger.info("step {} is already succeed, skip it.", subTask.getDisplayName());
            } else {
                throw new IllegalStateException("invalid subtask state, sub task:" + subTask.getDisplayName()
                        + ", state:" + subTask.getStatus());
            }
        }
        return ExecuteResult.createSucceed();

    }

    @Override
    protected boolean needCheckState() {
        return false;
    }

    @Override
    protected void onExecuteStart() throws JobStoppedException {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {

            if (isStoppedNonVoluntarily() && ExecutableState.READY != getOutput().getState()) //onExecuteStart will turn READY to RUNNING
                return null;

            updateJobOutput(project, getId(), ExecutableState.RUNNING, null, null, null);
            return null;
        }, project, UnitOfWork.DEFAULT_MAX_RETRY, getEpochId(), getTempLockName());
    }

    @Override
    protected void onExecuteFinished(ExecuteResult result) throws JobStoppedException {
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
                hasError = true;
                break;
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
            case SKIP:
            case SUCCEED:
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
            state = ExecutableState.READY;
        }

        logger.info("Job finished {}, state:{}", this.getDisplayName(), state);

        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            switch (state) {
            case SUCCEED:
                updateToFinalState(ExecutableState.SUCCEED, this::afterUpdateOutput, result.getShortErrMsg());
                break;
            case DISCARDED:
                updateToFinalState(ExecutableState.DISCARDED, this::onExecuteDiscardHook, result.getShortErrMsg());
                break;
            case SUICIDAL:
                updateToFinalState(ExecutableState.SUICIDAL, this::onExecuteSuicidalHook, result.getShortErrMsg());
                break;
            case ERROR:
            case PAUSED:
            case READY:
                if (isStoppedNonVoluntarily()) {
                    logger.info("Execute finished  {} which is stopped nonvoluntarily, state: {}",
                            this.getDisplayName(), getOutput().getState());
                    return null;
                }
                Consumer<String> hook = null;
                Map<String, String> info = null;
                String output = null;
                String shortErrMsg = null;
                if (state == ExecutableState.ERROR) {
                    logger.warn("[UNEXPECTED_THINGS_HAPPENED] Unexpected ERROR state discovered here!!!");
                    notifyUserJobIssue(JobIssueEnum.JOB_ERROR);
                    info = result.getExtraInfo();
                    output = result.getErrorMsg();
                    hook = this::onExecuteErrorHook;
                    shortErrMsg = result.getShortErrMsg();
                }
                updateJobOutput(getProject(), getId(), state, info, output, shortErrMsg, hook);
                break;
            default:
                throw new IllegalArgumentException("Illegal state when job finished: " + state);
            }
            return null;

        }, project, UnitOfWork.DEFAULT_MAX_RETRY, getEpochId(), getTempLockName());

        // dispatch job-finished message out
        EventBusFactory.getInstance()
                .postSync(new JobFinishedNotifier(getId(), getProject(), getTargetSubject(), getDuration(),
                        state.toString(), getJobType().toString(), this.getSegmentIds(), this.getLayoutIds(),
                        getTargetPartitions(), getWaitTime(), this.getClass().getName(), this.getSubmitter(),
                        result.succeed(), getJobStartTime(), getJobEndTime(), getTag(), result.getThrowable()));
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

}

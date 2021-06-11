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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.lock.DistributedLockFactory;
import org.apache.kylin.job.exception.ExecuteException;

import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

/**
 */
public class DefaultChainedExecutable extends AbstractExecutable implements ChainedExecutable {

    public static final Integer DEFAULT_PRIORITY = 10;


    private final List<AbstractExecutable> subTasks = Lists.newArrayList();

    public DefaultChainedExecutable() {
        super();
    }

    protected void initConfig(KylinConfig config) {
        super.initConfig(config);
        for (AbstractExecutable sub : subTasks) {
            sub.initConfig(config);
        }
    }

    public void setProjectName(String name) {
        setParam(MetadataConstants.P_PROJECT_NAME, name);
    }

    public String getProjectName() {
        return getParam(MetadataConstants.P_PROJECT_NAME);
    }

    public long getMapReduceWaitTime() {
        return getExtraInfoAsLong(MetadataConstants.MAP_REDUCE_WAIT_TIME, 0L);
    }

    public void setMapReduceWaitTime(long t) {
        addExtraInfo(MetadataConstants.MAP_REDUCE_WAIT_TIME, t + "");
    }

    public Set<String> getMetadataDumpList(KylinConfig config) {
        return Collections.emptySet();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        List<? extends Executable> executables = getTasks();
        final int size = executables.size();
        for (int i = 0; i < size; ++i) {
            Executable subTask = executables.get(i);
            ExecutableState state = subTask.getStatus();
            if (state == ExecutableState.RUNNING) {
                // there is already running subtask, no need to start a new subtask
                break;
            } else if (state == ExecutableState.STOPPED) {
                // the job is paused
                break;
            } else if (state == ExecutableState.ERROR) {
                throw new IllegalStateException(
                        "invalid subtask state, subtask:" + subTask.getName() + ", state:" + subTask.getStatus());
            }
            if (subTask.isRunnable()) {
                return subTask.execute(context);
            }
        }
        return new ExecuteResult(ExecuteResult.State.SUCCEED);
    }

    @Override
    protected void onExecuteStart(ExecutableContext executableContext) {
        final long startTime = getStartTime();
        if (startTime > 0) {
            getManager().updateJobOutput(getParam(MetadataConstants.P_PROJECT_NAME), getId(), ExecutableState.RUNNING, null, null, getLogPath());
        } else {
            Map<String, String> info = Maps.newHashMap();
            info.put(START_TIME, Long.toString(System.currentTimeMillis()));
            getManager().updateJobOutput(getParam(MetadataConstants.P_PROJECT_NAME), getId(), ExecutableState.RUNNING, info, null, getLogPath());
        }
        getManager().addJobInfo(getId(), BUILD_INSTANCE, DistributedLockFactory.processAndHost());
    }

    @Override
    protected void onExecuteError(Throwable exception, ExecutableContext executableContext) {
        super.onExecuteError(exception, executableContext);
        onStatusChange(executableContext, ExecuteResult.createError(exception), ExecutableState.ERROR);
    }

    @Override
    protected void onExecuteFinished(ExecuteResult result, ExecutableContext executableContext) {
        ExecutableManager mgr = getManager();

        if (isDiscarded()) {
            setEndTime(System.currentTimeMillis());
            onStatusChange(executableContext, result, ExecutableState.DISCARDED);
        } else if (isPaused()) {
            setEndTime(System.currentTimeMillis());
            onStatusChange(executableContext, result, ExecutableState.STOPPED);
        } else if (result.succeed()) {
            List<? extends Executable> jobs = getTasks();
            boolean allSucceed = true;
            boolean hasError = false;
            boolean hasDiscarded = false;
            for (Executable task : jobs) {
                if (task.getStatus() == ExecutableState.RUNNING) {
                    logger.error(
                            "There shouldn't be a running subtask[jobId: {}, jobName: {}], \n"
                                    + "it might cause endless state, will retry to fetch subtask's state.",
                            task.getId(), task.getName());
                    getManager().updateJobOutput(getParam(MetadataConstants.P_PROJECT_NAME), task.getId(), ExecutableState.ERROR, null,
                            "killed due to inconsistent state", getLogPath());
                    hasError = true;
                }

                final ExecutableState status = task.getStatus();

                if (status == ExecutableState.ERROR) {
                    hasError = true;
                }
                if (status != ExecutableState.SUCCEED) {
                    allSucceed = false;
                }
                if (status == ExecutableState.DISCARDED) {
                    hasDiscarded = true;
                }
            }
            if (allSucceed) {
                setEndTime(System.currentTimeMillis());
                mgr.updateJobOutput(getParam(MetadataConstants.P_PROJECT_NAME), getId(), ExecutableState.SUCCEED, null, null, getLogPath());
                onStatusChange(executableContext, result, ExecutableState.SUCCEED);
            } else if (hasError) {
                setEndTime(System.currentTimeMillis());
                mgr.updateJobOutput(getParam(MetadataConstants.P_PROJECT_NAME), getId(), ExecutableState.ERROR, null, null, getLogPath());
                onStatusChange(executableContext, result, ExecutableState.ERROR);
            } else if (hasDiscarded) {
                setEndTime(System.currentTimeMillis());
                mgr.updateJobOutput(getParam(MetadataConstants.P_PROJECT_NAME), getId(), ExecutableState.DISCARDED, null, null, getLogPath());
            } else {
                mgr.updateJobOutput(getParam(MetadataConstants.P_PROJECT_NAME), getId(), ExecutableState.READY, null, null, getLogPath());
            }
        } else {
            setEndTime(System.currentTimeMillis());
            mgr.updateJobOutput(getParam(MetadataConstants.P_PROJECT_NAME), getId(), ExecutableState.ERROR, null, result.output(), getLogPath());
            onStatusChange(executableContext, result, ExecutableState.ERROR);
        }
    }

    protected void onStatusChange(ExecutableContext context, ExecuteResult result, ExecutableState state) {
        super.notifyUserStatusChange(context, state);
    }

    @Override
    public List<AbstractExecutable> getTasks() {
        return subTasks;
    }

    public final AbstractExecutable getTaskByName(String name) {
        for (AbstractExecutable task : subTasks) {
            if (task.getName() != null && task.getName().equalsIgnoreCase(name)) {
                return task;
            }
        }
        return null;
    }

    @Override
    public void addTask(AbstractExecutable executable) {
        executable.setParentExecutable(this);
        executable.setParent(this);
        executable.setId(getId() + "-" + String.format(Locale.ROOT, "%02d", subTasks.size()));
        this.subTasks.add(executable);
    }

    @Override
    public int getDefaultPriority() {
        return DEFAULT_PRIORITY;
    }

    public String findExtraInfo(String key, String dft) {
        return findExtraInfo(key, dft, false);
    }

    public String findExtraInfoBackward(String key, String dft) {
        return findExtraInfo(key, dft, true);
    }

    private String findExtraInfo(String key, String dft, boolean backward) {
        ArrayList<AbstractExecutable> tasks = new ArrayList<AbstractExecutable>(getTasks());

        if (backward) {
            Collections.reverse(tasks);
        }

        for (AbstractExecutable child : tasks) {
            Output output = getManager().getOutput(child.getId());
            String value = output.getExtra().get(key);
            if (value != null)
                return value;
        }
        return dft;
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
}

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

import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.manager.ExecutableManager;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 */
public class DefaultChainedExecutable extends AbstractExecutable implements ChainedExecutable {

    private final List<AbstractExecutable> subTasks = Lists.newArrayList();

    protected final ExecutableManager jobService = ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv());

    public DefaultChainedExecutable() {
        super();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        List<? extends Executable> executables = getTasks();
        final int size = executables.size();
        for (int i = 0; i < size; ++i) {
            Executable subTask = executables.get(i);
            if (subTask.isRunnable()) {
                return subTask.execute(context);
            }
        }
        return new ExecuteResult(ExecuteResult.State.SUCCEED, null);
    }

    @Override
    protected void onExecuteStart(ExecutableContext executableContext) {
        Map<String, String> info = Maps.newHashMap();
        info.put(START_TIME, Long.toString(System.currentTimeMillis()));
        final long startTime = getStartTime();
        if (startTime > 0) {
            jobService.updateJobOutput(getId(), ExecutableState.RUNNING, null, null);
        } else {
            jobService.updateJobOutput(getId(), ExecutableState.RUNNING, info, null);
        }
    }

    @Override
    protected void onExecuteFinished(ExecuteResult result, ExecutableContext executableContext) {
        if (isDiscarded()) {
            setEndTime(System.currentTimeMillis());
            notifyUserStatusChange(executableContext, ExecutableState.DISCARDED);
        } else if (result.succeed()) {
            List<? extends Executable> jobs = getTasks();
            boolean allSucceed = true;
            boolean hasError = false;
            for (Executable task : jobs) {
                final ExecutableState status = task.getStatus();
                if (status == ExecutableState.ERROR) {
                    hasError = true;
                }
                if (status != ExecutableState.SUCCEED) {
                    allSucceed = false;
                }
            }
            if (allSucceed) {
                setEndTime(System.currentTimeMillis());
                jobService.updateJobOutput(getId(), ExecutableState.SUCCEED, null, null);
                notifyUserStatusChange(executableContext, ExecutableState.SUCCEED);
            } else if (hasError) {
                setEndTime(System.currentTimeMillis());
                jobService.updateJobOutput(getId(), ExecutableState.ERROR, null, null);
                notifyUserStatusChange(executableContext, ExecutableState.ERROR);
            } else {
                jobService.updateJobOutput(getId(), ExecutableState.READY, null, null);
            }
        } else {
            setEndTime(System.currentTimeMillis());
            jobService.updateJobOutput(getId(), ExecutableState.ERROR, null, result.output());
            notifyUserStatusChange(executableContext, ExecutableState.ERROR);
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
        executable.setId(getId() + "-" + String.format("%02d", subTasks.size()));
        this.subTasks.add(executable);
    }
}

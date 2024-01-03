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

import java.util.Map;

import org.apache.kylin.common.logging.SetLogCategory;
import org.apache.kylin.common.util.SetThreadName;

import lombok.val;

public class ExecutableThread extends Thread {
    private Map<String, Executable> dagExecutablesMap;
    private ExecutableContext context;
    private DefaultExecutable dagExecutable;
    private Executable executable;

    public ExecutableThread() {
    }

    public ExecutableThread(DefaultExecutable dagExecutable, Map<String, Executable> dagExecutablesMap,
            ExecutableContext context, Executable executable) {
        this.dagExecutable = dagExecutable;
        this.dagExecutablesMap = dagExecutablesMap;
        this.context = context;
        this.executable = executable;
    }

    @Override
    public void run() {
        //only the first 8 chars of the job uuid
        val jobIdSimple = dagExecutable.getId().split("-")[0];
        val project = dagExecutable.getProject();
        try (SetThreadName ignored = new SetThreadName("JobWorker(project:%s,jobid:%s)", project, jobIdSimple);
                SetLogCategory logCategory = new SetLogCategory("schedule")) {
            context.addRunningJob(executable);
            context.addRunningJobThread(executable);
            dagExecutable.executeDagExecutable(dagExecutablesMap, executable, context);
        } finally {
            context.removeRunningJob(executable);
        }
    }
}

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

package org.apache.kylin.job.scheduler;

import static org.apache.kylin.job.execution.ExecutableThread.JOB_THREAD_NAME_PATTERN;

import java.util.Locale;

import org.apache.kylin.job.JobContext;
import org.apache.kylin.job.core.AbstractJobExecutable;
import org.apache.kylin.job.execution.AbstractExecutable;

public class JobExecutor implements AutoCloseable {

    private final JobContext jobContext;
    private final AbstractJobExecutable jobExecutable;

    private final String originThreadName;

    public JobExecutor(JobContext jobContext, AbstractJobExecutable jobExecutable) {
        this.jobContext = jobContext;
        this.jobExecutable = jobExecutable;

        this.originThreadName = Thread.currentThread().getName();
        setThreadName();

    }

    public AbstractJobExecutable getJobExecutable() {
        return jobExecutable;
    }

    public void execute() throws Exception {
        // TODO
        if (jobExecutable instanceof AbstractExecutable) {
            ((AbstractExecutable) jobExecutable).execute(jobContext);
        } else {
            jobExecutable.execute();
        }
    }

    private void setThreadName() {
        String project = jobExecutable.getProject();
        String jobFlag = jobExecutable.getJobId().split("-")[0];
        Thread.currentThread().setName(String.format(Locale.ROOT, JOB_THREAD_NAME_PATTERN, project, jobFlag));
    }

    private void setbackThreadName() {
        Thread.currentThread().setName(originThreadName);
    }

    @Override
    public void close() throws Exception {
        jobContext.getResourceAcquirer().release(jobExecutable);
        // setback thread name
        setbackThreadName();
    }
}

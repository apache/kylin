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

package org.apache.kylin.streaming.jobs.thread;

import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.cube.utils.StreamingUtils;
import org.apache.kylin.streaming.jobs.impl.StreamingJobLauncher;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StreamingJobRunner implements Runnable {

    private String project;
    private String modelId;
    private JobTypeEnum jobType;
    private StreamingJobLauncher jobLauncher;

    public StreamingJobRunner(String project, String modelId, JobTypeEnum jobType) {
        this.project = project;
        this.modelId = modelId;
        this.jobType = jobType;
        jobLauncher = new StreamingJobLauncher();
    }

    @Override
    public void run() {
        launchJob();
    }

    public void stop() {
        stopJob();
    }

    public void init() {
        jobLauncher.init(project, modelId, jobType);
    }

    private void launchJob() {
        jobLauncher.init(project, modelId, jobType);
        jobLauncher.launch();
    }

    private void stopJob() {
        if (!jobLauncher.isInitialized()) {
            log.warn("Streaming job launcher {} not exists...", StreamingUtils.getJobId(modelId, jobType.name()));
            return;
        }
        jobLauncher.stop();
    }
}

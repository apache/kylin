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
package org.apache.kylin.job.impl.threadpool;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.val;

public class ExecutableDurationContext {
    @Getter
    private String project;
    @Getter
    private String jobId;
    @Getter
    private List<Record> stepRecords;
    @Getter
    private Record record;

    public ExecutableDurationContext(String project, String jobId) {
        this.project = project;
        this.jobId = jobId;
        val manager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        DefaultChainedExecutable job = (DefaultChainedExecutable) manager.getJob(jobId);
        record = new Record(job.getStatus(), job.getDuration(), job.getWaitTime(), job.getCreateTime());
        val steps = job.getTasks();
        stepRecords = steps.stream()
                .map(step -> new Record(step.getStatus(), step.getDuration(), step.getWaitTime(), step.getCreateTime()))
                .collect(Collectors.toList());
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Record {
        private ExecutableState state;
        private long duration;
        private long waitTime;
        private long createTime;
    }
}

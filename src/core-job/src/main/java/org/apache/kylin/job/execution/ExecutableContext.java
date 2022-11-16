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
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.apache.kylin.common.KylinConfig;

import io.kyligence.kap.guava20.shaded.common.collect.Maps;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 */
@Slf4j
public class ExecutableContext {

    @Getter
    @Setter
    private long epochId;
    @Getter
    @Setter
    private volatile boolean reachQuotaLimit = false;
    @Getter
    @Setter
    private volatile boolean isLicenseOverCapacity = false;

    private final ConcurrentMap<String, Thread> runningJobThreads;
    private final ConcurrentMap<String, Executable> runningJobs;
    private final ConcurrentMap<String, Long> runningJobInfos;
    private final KylinConfig kylinConfig;

    public ExecutableContext(ConcurrentMap<String, Executable> runningJobs, ConcurrentMap<String, Long> runningJobInfos,
            KylinConfig kylinConfig, long epochId) {
        this.runningJobThreads = Maps.newConcurrentMap();
        this.runningJobs = runningJobs;
        this.runningJobInfos = runningJobInfos;
        this.kylinConfig = kylinConfig;
        this.epochId = epochId;
    }

    public KylinConfig getConfig() {
        return kylinConfig;
    }

    public void addRunningJob(Executable executable) {
        runningJobThreads.put(executable.getId(), Thread.currentThread());
        runningJobs.put(executable.getId(), executable);
        runningJobInfos.put(executable.getId(), System.currentTimeMillis());
    }

    public void removeRunningJob(Executable executable) {
        runningJobThreads.remove(executable.getId());
        runningJobs.remove(executable.getId());
        runningJobInfos.remove(executable.getId());
    }

    public Thread getRunningJobThread(Executable executable) {
        return runningJobThreads.get(executable.getId());
    }

    public Map<String, Executable> getRunningJobs() {
        return Collections.unmodifiableMap(runningJobs);
    }

    public Map<String, Long> getRunningJobInfos() {
        return Collections.unmodifiableMap(runningJobInfos);
    }
}

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
package org.apache.kylin.job.factory;

import java.util.Map;
import java.util.Set;

import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.job.JobBucket;

import org.apache.kylin.guava30.shaded.common.collect.Maps;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 *
 **/
@Slf4j
public abstract class JobFactory {

    static final Map<String, JobFactory> implementations = Maps.newHashMap();

    public static void register(String jobName, JobFactory impl) {
        implementations.put(jobName, impl);
    }

    @AllArgsConstructor
    @RequiredArgsConstructor
    @Getter
    public static class JobBuildParams {
        private final Set<NDataSegment> segments;
        private final Set<LayoutEntity> layouts;
        private final String submitter;
        private final JobTypeEnum jobType;
        private final String jobId;
        private final Set<LayoutEntity> toBeDeletedLayouts;
        private final Set<String> ignoredSnapshotTables;
        private final Set<Long> partitions;
        private final Set<JobBucket> buckets;
        private Map<String, String> extParams;
    }

    public static AbstractExecutable createJob(String factory, JobBuildParams jobBuildParams) {
        if (!implementations.containsKey(factory)) {
            log.error("JobFactory doesn't contain this factory:{}", factory);
            return null;
        }
        return implementations.get(factory).create(jobBuildParams);
    }

    protected abstract AbstractExecutable create(JobBuildParams jobBuildParams);

}

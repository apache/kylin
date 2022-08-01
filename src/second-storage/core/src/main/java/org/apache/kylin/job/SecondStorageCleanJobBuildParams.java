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

package org.apache.kylin.job;

import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.factory.JobFactory;
import org.apache.kylin.job.model.JobParam;

import java.util.Objects;
import java.util.Set;

public class SecondStorageCleanJobBuildParams extends JobFactory.JobBuildParams {
    private String project;
    private String modelId;
    private String dataflowId;
    private Set<Long> secondStorageDeleteLayoutIds;

    public SecondStorageCleanJobBuildParams(Set<NDataSegment> segments, JobParam jobParam, JobTypeEnum jobType) {
        super(segments,
                jobParam.getProcessLayouts(),
                jobParam.getOwner(),
                jobType,
                jobParam.getJobId(),
                null,
                jobParam.getIgnoredSnapshotTables(),
                null,
                null);
    }

    public String getProject() {
        return project;
    }

    public SecondStorageCleanJobBuildParams setProject(String project) {
        this.project = project;
        return this;
    }

    public String getModelId() {
        return modelId;
    }

    public SecondStorageCleanJobBuildParams setModelId(String modelId) {
        this.modelId = modelId;
        return this;
    }

    public String getDataflowId() {
        return dataflowId;
    }

    public SecondStorageCleanJobBuildParams setDataflowId(String dataflowId) {
        this.dataflowId = dataflowId;
        return this;
    }

    public void setSecondStorageDeleteLayoutIds(Set<Long> secondStorageDeleteLayoutIds) {
        if (Objects.nonNull(secondStorageDeleteLayoutIds)) {
            this.secondStorageDeleteLayoutIds = secondStorageDeleteLayoutIds;
        }
    }

    public Set<Long> getSecondStorageDeleteLayoutIds() {
        return this.secondStorageDeleteLayoutIds;
    }
}

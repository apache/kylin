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

import java.util.Collections;
import java.util.Set;

import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.factory.JobFactory;
import org.apache.kylin.job.model.JobParam;

public class SecondStorageRefreshSecondaryIndexJobParams extends JobFactory.JobBuildParams {
    private String project;
    private String modelId;
    private Set<Integer> newIndexes;
    private Set<Integer> toBeDeleteIndexed;

    public SecondStorageRefreshSecondaryIndexJobParams(JobParam jobParam, JobTypeEnum jobType) {
        super(Collections.emptySet(), jobParam.getProcessLayouts(), jobParam.getOwner(), jobType, jobParam.getJobId(),
                null, null, null, null);
    }

    public String getProject() {
        return project;
    }

    public SecondStorageRefreshSecondaryIndexJobParams setProject(String project) {
        this.project = project;
        return this;
    }

    public String getModelId() {
        return modelId;
    }

    public SecondStorageRefreshSecondaryIndexJobParams setModelId(String modelId) {
        this.modelId = modelId;
        return this;
    }

    public void setNewIndexes(Set<Integer> newIndexes) {
        this.newIndexes = newIndexes;
    }

    public void setToBeDeleteIndexed(Set<Integer> toBeDeleteIndexed) {
        this.toBeDeleteIndexed = toBeDeleteIndexed;
    }

    public Set<Integer> getNewIndexes() {
        return newIndexes;
    }

    public Set<Integer> getToBeDeleteIndexed() {
        return toBeDeleteIndexed;
    }
}

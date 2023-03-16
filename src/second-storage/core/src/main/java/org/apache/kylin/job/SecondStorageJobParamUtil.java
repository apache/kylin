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

import static org.apache.kylin.job.common.ExecutableUtil.registerImplementation;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.cube.model.LayoutEntity;

import org.apache.kylin.guava30.shaded.common.collect.Sets;

public class SecondStorageJobParamUtil {

    static {
        registerImplementation(JobTypeEnum.EXPORT_TO_SECOND_STORAGE, new SecondStorageJobUtil());
        registerImplementation(JobTypeEnum.SECOND_STORAGE_MODEL_CLEAN, new SecondStorageCleanJobUtil());
        registerImplementation(JobTypeEnum.SECOND_STORAGE_SEGMENT_CLEAN, new SecondStorageCleanJobUtil());
        registerImplementation(JobTypeEnum.SECOND_STORAGE_NODE_CLEAN, new SecondStorageCleanJobUtil());
        registerImplementation(JobTypeEnum.SECOND_STORAGE_INDEX_CLEAN, new SecondStorageCleanJobUtil());
        registerImplementation(JobTypeEnum.SECOND_STORAGE_REFRESH_SECONDARY_INDEXES, new SecondStorageRefreshSecondaryIndexUtil());
    }

    private SecondStorageJobParamUtil() {
        throw new IllegalStateException("Utility class");
    }

    public static JobParam of(String project, String model, String owner, Stream<String> segmentIDs) {
        final JobParam param = new JobParam(model, owner);
        param.setProject(project);
        param.withTargetSegments(segmentIDs.collect(Collectors.toSet()))
                .withJobTypeEnum(JobTypeEnum.EXPORT_TO_SECOND_STORAGE);
        param.getCondition().put(JobParam.ConditionConstant.REFRESH_ALL_LAYOUTS, Boolean.FALSE);
        return param;
    }

    public static JobParam projectCleanParam(String project, String owner) {
        JobParam param = new JobParam("", owner);
        param.setJobTypeEnum(JobTypeEnum.SECOND_STORAGE_NODE_CLEAN);
        param.setProject(project);
        return param;
    }

    public static JobParam modelCleanParam(String project, String model, String owner) {
        JobParam param = new JobParam(model, owner);
        param.setProject(project);
        param.setJobTypeEnum(JobTypeEnum.SECOND_STORAGE_MODEL_CLEAN);
        return param;
    }

    public static JobParam segmentCleanParam(String project, String model, String owner, Set<String> ids) {
        JobParam param = new JobParam(model, owner);
        param.setProject(project);
        param.withTargetSegments(ids);
        param.setJobTypeEnum(JobTypeEnum.SECOND_STORAGE_SEGMENT_CLEAN);
        return param;
    }

    /**
     * build delete layout table parameters
     *
     * PRD_KE-34597 add index clean job
     *
     * @param project project name
     * @param model model id
     * @param owner owner
     * @param needDeleteLayoutIds required delete ids of layout
     * @return job parameters
     */
    public static JobParam layoutCleanParam(String project, String model, String owner, Set<Long> needDeleteLayoutIds,
                                            Set<String> segmentIds) {
        JobParam param = new JobParam(model, owner);
        param.setProject(project);
        param.withTargetSegments(segmentIds);
        param.setSecondStorageDeleteLayoutIds(needDeleteLayoutIds);
        param.setJobTypeEnum(JobTypeEnum.SECOND_STORAGE_INDEX_CLEAN);
        return param;
    }

    public static JobParam refreshSecondaryIndexParam(String project, String model, String owner, LayoutEntity layout,
                                                      Set<Integer> newColumns, Set<Integer> deleteColumns) {
        JobParam param = new JobParam(model, owner);
        param.setProject(project);
        param.setProcessLayouts(Sets.newHashSet(layout));
        param.getCondition().put(JobTypeEnum.SECOND_STORAGE_REFRESH_SECONDARY_INDEXES.name() + "_ADD", newColumns);
        param.getCondition().put(JobTypeEnum.SECOND_STORAGE_REFRESH_SECONDARY_INDEXES.name() + "_DELETE",
                deleteColumns);
        param.setJobTypeEnum(JobTypeEnum.SECOND_STORAGE_REFRESH_SECONDARY_INDEXES);
        return param;
    }
}

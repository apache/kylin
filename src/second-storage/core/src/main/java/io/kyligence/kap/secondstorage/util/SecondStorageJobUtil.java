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

package io.kyligence.kap.secondstorage.util;

import com.google.common.collect.Sets;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.JobErrorCode;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class SecondStorageJobUtil {
    public static final Set<JobTypeEnum> SECOND_STORAGE_JOBS = Sets.newHashSet(JobTypeEnum.EXPORT_TO_SECOND_STORAGE,
            JobTypeEnum.SECOND_STORAGE_SEGMENT_CLEAN, JobTypeEnum.SECOND_STORAGE_MODEL_CLEAN);

    public static List<AbstractExecutable> findSecondStorageJobByProject(String project) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NExecutableManager executableManager = NExecutableManager.getInstance(config, project);
        return executableManager.getJobs().stream().map(executableManager::getJob)
                .filter(job -> SECOND_STORAGE_JOBS.contains(job.getJobType()))
                .collect(Collectors.toList());
    }

    public static void validateSegment(String project, String modelId, List<String> segmentIds) {
        List<AbstractExecutable> jobs = findSecondStorageJobByProject(project).stream()
                .filter(job -> SecondStorageUtil.RUNNING_STATE.contains(job.getStatus()))
                .filter(job -> Objects.equals(job.getTargetModelId(), modelId))
                .filter(job -> {
                    List<String> targetSegments = Optional.ofNullable(job.getTargetSegments()).orElse(Collections.emptyList());
                    return targetSegments.stream().anyMatch(segmentIds::contains);
                })
                .collect(Collectors.toList());
        if (!jobs.isEmpty()) {
            throw new KylinException(JobErrorCode.SECOND_STORAGE_JOB_EXISTS,
                    MsgPicker.getMsg().getSecondStorageConcurrentOperate());
        }
    }

    public static void validateModel(String project, String modelId) {
        List<AbstractExecutable> jobs = findSecondStorageJobByProject(project).stream()
                .filter(job -> SecondStorageUtil.RUNNING_STATE.contains(job.getStatus()))
                .filter(job -> Objects.equals(job.getTargetModelId(), modelId))
                .collect(Collectors.toList());
        if (!jobs.isEmpty()) {
            throw new KylinException(JobErrorCode.SECOND_STORAGE_JOB_EXISTS,
                    MsgPicker.getMsg().getSecondStorageConcurrentOperate());
        }
    }
}

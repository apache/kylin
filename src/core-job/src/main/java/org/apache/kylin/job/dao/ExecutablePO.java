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

package org.apache.kylin.job.dao;

import static org.apache.kylin.job.constant.ExecutableConstants.YARN_APP_IDS_DELIMITER;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobSchedulerModeEnum;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.cube.model.NDataSegment;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;
import lombok.val;
import lombok.var;

/**
 */
@Setter
@Getter
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class ExecutablePO extends RootPersistentEntity {
    public static final int HIGHEST_PRIORITY = 0;
    public static final int DEFAULT_PRIORITY = 3;
    public static final int LOWEST_PRIORITY = 4;

    private static final String V4_0_TYPE_PREFIX = "io.kyligence.kap.engine.spark.job";
    private static final String V5_0_TYPE_PREFIX = "org.apache.kylin.engine.spark.job";

    @JsonProperty("name")
    private String name;

    @JsonProperty("tasks")
    private List<ExecutablePO> tasks;

    @JsonProperty("type")
    private String type;

    @JsonProperty("handler_type")
    private String handlerType;

    @JsonProperty("params")
    private Map<String, String> params = Maps.newHashMap();

    @JsonProperty("segments")
    private Set<NDataSegment> segments = Sets.newHashSet();

    @JsonProperty("job_type")
    private JobTypeEnum jobType;

    @JsonProperty("data_range_start")
    private long dataRangeStart;

    @JsonProperty("data_range_end")
    private long dataRangeEnd;

    @JsonProperty("target_model")
    private String targetModel;

    @JsonProperty("target_segments")
    private List<String> targetSegments;

    @JsonProperty("output")
    private ExecutableOutputPO output = new ExecutableOutputPO();

    @JsonProperty("project")
    private String project;

    @JsonProperty("target_partitions")
    private Set<Long> targetPartitions = Sets.newHashSet();

    @JsonProperty("priority")
    private int priority = DEFAULT_PRIORITY;

    @JsonProperty("tag")
    private Object tag;

    @JsonProperty("stages_map")
    private Map<String, List<ExecutablePO>> stagesMap;

    @JsonProperty("job_scheduler_mode")
    private JobSchedulerModeEnum jobSchedulerMode = JobSchedulerModeEnum.CHAIN;

    @JsonProperty("previous_step")
    private String previousStep;

    @JsonProperty("next_steps")
    private Set<String> nextSteps = Sets.newHashSet();

    public void setPriority(int p) {
        priority = isPriorityValid(p) ? p : DEFAULT_PRIORITY;
    }

    public static boolean isPriorityValid(int priority) {
        return priority >= HIGHEST_PRIORITY && priority <= LOWEST_PRIORITY;
    }

    public static boolean isHigherPriority(int p1, int p2) {
        return p1 < p2;
    }

    public void addYarnApplicationJob(String appId) {
        String oldAppIds = output.getInfo().getOrDefault(ExecutableConstants.YARN_APP_IDS, "");
        Set<String> appIds = new HashSet<>(Arrays.asList(oldAppIds.split(YARN_APP_IDS_DELIMITER)));
        if (!appIds.contains(appId)) {
            String newAppIds = oldAppIds + (StringUtils.isEmpty(oldAppIds) ? "" : YARN_APP_IDS_DELIMITER) + appId;
            output.getInfo().put(ExecutableConstants.YARN_APP_IDS, newAppIds);
        }
    }

    public String getType() {
        return backwardConvertType(type);
    }

    public String getTargetModelId() {
        return AbstractExecutable.getTargetModelId(getProject(), getTargetModel());
    }

    public long getDurationByPO() {
        long jobDuration = getTaskDuration();
        List<ExecutablePO> subTasks = getTasks();
        if (CollectionUtils.isNotEmpty(subTasks)) {
            jobDuration = 0;
            for (ExecutablePO subTask : subTasks) {
                long taskDuration = subTask.getTaskDuration();
                if (MapUtils.isNotEmpty(subTask.getStagesMap()) && subTask.getStagesMap().size() == 1) {
                    val jobAtomicDuration = new AtomicLong(0);
                    for (Map.Entry<String, List<ExecutablePO>> entry : subTask.getStagesMap().entrySet()) {
                        entry.getValue().forEach(po -> jobAtomicDuration.addAndGet(po.getTaskDuration()));
                    }
                    taskDuration = jobAtomicDuration.get();
                }
                jobDuration += taskDuration;
            }
        }
        return jobDuration;
    }

    private long getTaskDuration() {
        ExecutableOutputPO jobOutput = getOutput();
        if (jobOutput.getDuration() != 0) {
            var taskDuration = jobOutput.getDuration();
            if (ExecutableState.RUNNING == ExecutableState.valueOf(jobOutput.getStatus())) {
                taskDuration = (taskDuration + System.currentTimeMillis() - jobOutput.getLastRunningStartTime());
            }
            return taskDuration;
        }
        if (jobOutput.getStartTime() == 0) {
            return 0;
        }
        return (jobOutput.getEndTime() == 0 ? System.currentTimeMillis() - jobOutput.getStartTime()
                : jobOutput.getEndTime() - jobOutput.getStartTime());
    }

    private String backwardConvertType(String oldType) {
        if (oldType != null && oldType.startsWith(V4_0_TYPE_PREFIX)) {
            return oldType.replace(V4_0_TYPE_PREFIX, V5_0_TYPE_PREFIX);
        }
        return oldType;
    }

}

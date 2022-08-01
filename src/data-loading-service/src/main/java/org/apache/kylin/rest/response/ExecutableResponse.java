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

package org.apache.kylin.rest.response;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.SecondStorageCleanJobUtil;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ChainedExecutable;
import org.apache.kylin.job.execution.ChainedStageExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.StageBase;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.engine.spark.job.NSparkSnapshotJob;
import org.apache.kylin.engine.spark.job.NTableSamplingJob;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;

import com.clearspring.analytics.util.Lists;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.google.common.collect.Maps;

import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.val;
import lombok.var;

@Setter
@Getter
public class ExecutableResponse implements Comparable<ExecutableResponse> {

    @JsonProperty("id")
    private String id;
    @JsonProperty("last_modified")
    private long lastModified;
    @JsonProperty("duration")
    private long duration;
    @JsonProperty("total_duration")
    private long totalDuration;
    @JsonProperty("exec_start_time")
    private long execStartTime;
    @JsonManagedReference
    @JsonProperty("steps")
    private List<ExecutableStepResponse> steps;
    @JsonProperty("job_status")
    private JobStatusEnum status;
    @JsonProperty("job_name")
    private String jobName;
    @JsonProperty("data_range_start")
    private long dataRangeStart;
    @JsonProperty("data_range_end")
    private long dataRangeEnd;
    @JsonProperty("target_model")
    private String targetModel;
    @JsonProperty("target_segments")
    private List<String> targetSegments;
    @JsonProperty("step_ratio")
    private float stepRatio;
    @JsonProperty("create_time")
    private long createTime;
    @JsonProperty("wait_time")
    private long waitTime;
    @JsonProperty("target_subject")
    private String targetSubject;
    @JsonProperty("target_subject_error")
    private boolean targetSubjectError = false;
    @JsonProperty("project")
    private String project;
    @JsonProperty("submitter")
    private String submitter;
    @JsonProperty("exec_end_time")
    private long execEndTime;
    @JsonProperty("discard_safety")
    private boolean discardSafety;
    @JsonProperty("tag")
    private Object tag;
    @JsonProperty("snapshot_data_range")
    private String snapshotDataRange;
    private static final String SNAPSHOT_FULL_RANGE = "FULL";
    private static final String SNAPSHOT_INC_RANGE = "INC";

    private static ExecutableResponse newInstance(AbstractExecutable abstractExecutable) {
        ExecutableResponse executableResponse = new ExecutableResponse();
        executableResponse.setDataRangeEnd(abstractExecutable.getDataRangeEnd());
        executableResponse.setDataRangeStart(abstractExecutable.getDataRangeStart());
        executableResponse.setJobName(abstractExecutable.getName());
        executableResponse.setId(abstractExecutable.getId());
        executableResponse.setExecStartTime(abstractExecutable.getStartTime());
        executableResponse.setCreateTime(abstractExecutable.getCreateTime());
        executableResponse.setDuration(abstractExecutable.getDurationFromStepOrStageDurationSum());
        executableResponse.setLastModified(abstractExecutable.getLastModified());
        executableResponse.setTargetModel(abstractExecutable.getTargetSubject());
        executableResponse.setTargetSegments(abstractExecutable.getTargetSegments());
        executableResponse.setTargetSubject(abstractExecutable.getTargetSubjectAlias());
        executableResponse.setWaitTime(abstractExecutable.getWaitTime());
        executableResponse.setSubmitter(abstractExecutable.getSubmitter());
        executableResponse.setExecEndTime(abstractExecutable.getEndTime());
        executableResponse.setDiscardSafety(abstractExecutable.safetyIfDiscard());
        executableResponse.setTotalDuration(executableResponse.getWaitTime() + executableResponse.getDuration());
        executableResponse.setTag(abstractExecutable.getTag());
        return executableResponse;
    }

    public static ExecutableResponse create(AbstractExecutable abstractExecutable) {
        ExecutableResponse executableResponse = newInstance(abstractExecutable);
        if (abstractExecutable instanceof NTableSamplingJob) {
            NTableSamplingJob samplingJob = (NTableSamplingJob) abstractExecutable;
            executableResponse.setDataRangeEnd(Long.MAX_VALUE);
            executableResponse.setTargetSubject(samplingJob.getParam(NBatchConstants.P_TABLE_NAME));
            if (NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), abstractExecutable.getProject())
                    .getTableDesc(executableResponse.getTargetSubject()) == null) {
                executableResponse.setTargetSubject(executableResponse.getTargetSubject() + " deleted");
                executableResponse.setTargetSubjectError(true);
            }
        } else if (abstractExecutable instanceof NSparkSnapshotJob) {
            NSparkSnapshotJob snapshotJob = (NSparkSnapshotJob) abstractExecutable;
            executableResponse.setDataRangeEnd(Long.MAX_VALUE);
            executableResponse.setTargetSubject(snapshotJob.getParam(NBatchConstants.P_TABLE_NAME));
            executableResponse.setSnapshotDataRange(getDataRangeBySnapshotJob(snapshotJob));

            TableDesc tableDesc = NTableMetadataManager
                    .getInstance(KylinConfig.getInstanceFromEnv(), abstractExecutable.getProject())
                    .getTableDesc(executableResponse.getTargetSubject());
            if (snapshotJob.getStatus().isFinalState()
                    && (tableDesc == null || tableDesc.getLastSnapshotPath() == null)) {
                executableResponse.setTargetSubject("The snapshot is deleted");
                executableResponse.setTargetSubjectError(true);
            }
        } else if (SecondStorageCleanJobUtil.isProjectCleanJob(abstractExecutable)) {
            executableResponse.setTargetSubject(abstractExecutable.getProject());
        } else {
            val dataflow = NDataflowManager
                    .getInstance(KylinConfig.getInstanceFromEnv(), abstractExecutable.getProject())
                    .getDataflow(abstractExecutable.getTargetSubject());
            if (dataflow == null) {
                executableResponse.setTargetSubject("The model is deleted");
                executableResponse.setTargetSubjectError(true);
            } else if (dataflow.checkBrokenWithRelatedInfo()) {
                executableResponse.setTargetSubject(executableResponse.getTargetSubject() + " broken");
                executableResponse.setTargetSubjectError(true);
            }
        }

        val stepRatio = calculateStepRatio(abstractExecutable);
        executableResponse.setStepRatio(stepRatio);
        executableResponse.setProject(abstractExecutable.getProject());
        return executableResponse;
    }

    /**
     * Single Segment situation:
     *
     * CurrentProgress = numberOfCompletedSteps / totalNumberOfSteps, accurate to the single digit percentage.
     *
     * Among them, the progress of the "BUILD_LAYER"
     * step = numberOfCompletedIndexes / totalNumberOfIndexesToBeConstructed,
     * the progress of other steps will not be refined
     * ----------------------------------------------------------------------------------------------------------
     * multi segment situation:
     *
     * CurrentProgress =
     *   [numberOfPublicStepsCompleted + (numberOfSegmentStepsCompleted / numberOfSegments)] / totalNumberOfSteps
     *
     * Another: "BUILD_LAYER" are not refined
     */
    public static float calculateStepRatio(AbstractExecutable abstractExecutable) {
        List<? extends AbstractExecutable> tasks = ((ChainedExecutable) abstractExecutable).getTasks();
        var successSteps = 0D;
        var stageCount = 0;
        for (AbstractExecutable task : tasks) {
            if (task instanceof ChainedStageExecutable) {
                final ChainedStageExecutable stageExecutable = (ChainedStageExecutable) task;
                Map<String, List<StageBase>> stageMap = Optional.ofNullable(stageExecutable.getStagesMap())
                        .orElse(Maps.newHashMap());
                var taskMapStageCount = Optional.of(stageMap.values()).orElse(Lists.newArrayList()).stream().findFirst()
                        .orElse(Lists.newArrayList()).size();

                if (0 != taskMapStageCount) {
                    // calculate sum step count, second step and stage is duplicate
                    stageCount = taskMapStageCount - 1;
                    successSteps += calculateSuccessStageInTaskMap(task, stageMap);
                    continue;
                }
            }
            if (ExecutableState.SUCCEED == task.getStatus() || ExecutableState.SKIP == task.getStatus()) {
                successSteps++;
            }
        }
        val stepCount = tasks.size() + stageCount;
        var stepRatio = (float) successSteps / stepCount;
        // in case all steps are succeeded, but the job is not succeeded, the stepRatio should be 99%
        if (stepRatio == 1 && ExecutableState.SUCCEED != abstractExecutable.getStatus()) {
            stepRatio = 0.99F;
        }
        return stepRatio;
    }

    /** calculate stage count from segment */
    public static double calculateSuccessStageInTaskMap(AbstractExecutable task,
            Map<String, List<StageBase>> stageMap) {
        var successStages = 0D;
        boolean calculateIndexExecRadio = stageMap.size() == 1;
        for (Map.Entry<String, List<StageBase>> entry : stageMap.entrySet()) {
            double count = calculateSuccessStage(task, entry.getKey(), entry.getValue(), calculateIndexExecRadio);
            successStages += count;
        }
        return successStages / stageMap.size();
    }

    public static double calculateSuccessStage(AbstractExecutable task, String segmentId, List<StageBase> stageBases,
            boolean calculateIndexExecRadio) {
        var successStages = 0D;
        for (StageBase stage : stageBases) {
            if (ExecutableState.SUCCEED == stage.getStatus(segmentId)
                    || stage.getStatus(segmentId) == ExecutableState.SKIP) {
                successStages += 1;
                continue;
            }

            final String indexCountString = task.getParam(NBatchConstants.P_INDEX_COUNT);
            final String indexSuccess = stage.getOutput(segmentId).getExtra()
                    .getOrDefault(NBatchConstants.P_INDEX_SUCCESS_COUNT, "");
            if (calculateIndexExecRadio && StringUtils.isNotBlank(indexCountString)
                    && StringUtils.isNotBlank(indexSuccess)) {
                final int indexCount = Integer.parseInt(indexCountString);
                final int indexSuccessCount = Integer.parseInt(indexSuccess);
                successStages += (double) indexSuccessCount / indexCount;
            }
        }

        return successStages;
    }

    @SneakyThrows
    private static String getDataRangeBySnapshotJob(NSparkSnapshotJob snapshotJob) {
        boolean increment = false;
        if ("true".equals(snapshotJob.getParam(NBatchConstants.P_INCREMENTAL_BUILD))) {
            increment = true;
        }
        String partitionToBuild = snapshotJob.getParam(NBatchConstants.P_SELECTED_PARTITION_VALUE);
        String partitionCol = snapshotJob.getParam(NBatchConstants.P_SELECTED_PARTITION_COL);
        if (partitionCol == null) {
            return SNAPSHOT_FULL_RANGE;
        }
        if (partitionToBuild != null) {
            List<String> partitions = JsonUtil.readValueAsList(partitionToBuild);
            partitions.sort(String::compareTo);
            return JsonUtil.writeValueAsString(partitions);
        }
        if (increment) {
            return SNAPSHOT_INC_RANGE;
        } else {
            return SNAPSHOT_FULL_RANGE;
        }
    }

    @Override
    public int compareTo(ExecutableResponse o) {
        return Long.compare(o.lastModified, this.lastModified);
    }

    /**
     * for 3x rest api
     */
    @JsonUnwrapped
    private OldParams oldParams;

    @Getter
    @Setter
    public static class OldParams {
        @JsonProperty("project_name")
        private String projectName;

        @JsonProperty("related_cube")
        private String relatedCube;

        @JsonProperty("display_cube_name")
        private String displayCubeName;

        @JsonProperty("uuid")
        private String uuid;

        @JsonProperty("type")
        private String type;

        @JsonProperty("name")
        private String name;

        @JsonProperty("exec_interrupt_time")
        private long execInterruptTime;

        @JsonProperty("mr_waiting")
        private long mrWaiting;
    }
}

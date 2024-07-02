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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.engine.spark.job.InternalTableLoadingJob;
import org.apache.kylin.engine.spark.job.NSparkSnapshotJob;
import org.apache.kylin.engine.spark.job.NTableSamplingJob;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ChainedExecutable;
import org.apache.kylin.job.execution.ChainedStageExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobSchedulerModeEnum;
import org.apache.kylin.job.execution.Output;
import org.apache.kylin.job.execution.StageExecutable;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.SegmentStatusEnumToDisplay;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.table.InternalTableDesc;
import org.apache.kylin.metadata.table.InternalTableManager;

import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
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
    @JsonProperty("scheduler_state")
    private ExecutableState schedulerState;
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
    @JsonProperty("job_scheduler_mode")
    private JobSchedulerModeEnum jobSchedulerMode;
    @JsonProperty("segments")
    private List<SegmentResponse> segments;
    private static final String SNAPSHOT_FULL_RANGE = "FULL";
    private static final String SNAPSHOT_INC_RANGE = "INC";

    @JsonProperty("version")
    protected String version;

    @JsonProperty("related_segment")
    public String getRelatedSegment() {
        return CollectionUtils.isEmpty(targetSegments) ? "" : String.join(",", targetSegments);
    }

    @JsonProperty("progress")
    public double getProgress() {
        int completedStepCount = 0;

        for (ExecutableStepResponse step : this.getSteps()) {
            if (step.getStatus() == JobStatusEnum.FINISHED) {
                completedStepCount++;
            }
        }
        if (steps.isEmpty()) {
            return 0.0;
        }
        return 100.0 * completedStepCount / steps.size();
    }

    public List<ExecutableStepResponse> getSteps() {
        if (steps == null) {
            steps = Collections.emptyList();
        }
        return steps;
    }

    private static ExecutableResponse newInstance(AbstractExecutable abstractExecutable, ExecutablePO executablePO) {
        Output output = abstractExecutable.getOutput(executablePO);
        ExecutableResponse executableResponse = new ExecutableResponse();
        executableResponse.setDataRangeEnd(abstractExecutable.getDataRangeEnd());
        executableResponse.setDataRangeStart(abstractExecutable.getDataRangeStart());
        executableResponse.setJobName(abstractExecutable.getName());
        executableResponse.setId(abstractExecutable.getId());
        executableResponse.setExecStartTime(AbstractExecutable.getStartTime(output));
        executableResponse.setCreateTime(AbstractExecutable.getCreateTime(output));
        executableResponse.setDuration(abstractExecutable.getDurationFromStepOrStageDurationSum(executablePO));
        executableResponse.setLastModified(AbstractExecutable.getLastModified(output));
        executableResponse.setTargetModel(abstractExecutable.getTargetSubject());
        executableResponse.setTargetSegments(abstractExecutable.getTargetSegments());
        executableResponse.setTargetSubject(abstractExecutable.getTargetSubjectAlias());
        executableResponse.setWaitTime(abstractExecutable.getWaitTime(executablePO));
        executableResponse.setSubmitter(abstractExecutable.getSubmitter());
        executableResponse.setExecEndTime(AbstractExecutable.getEndTime(output));
        executableResponse.setDiscardSafety(abstractExecutable.safetyIfDiscard());
        executableResponse.setTotalDuration(executableResponse.getWaitTime() + executableResponse.getDuration());
        executableResponse.setTag(abstractExecutable.getTag());
        executableResponse.setJobSchedulerMode(abstractExecutable.getJobSchedulerMode());
        return executableResponse;
    }

    public static ExecutableResponse create(AbstractExecutable abstractExecutable, ExecutablePO executablePO) {
        ExecutableResponse executableResponse = newInstance(abstractExecutable, executablePO);
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
            if (snapshotJob.getStatusInMem().isFinalState()
                    && (tableDesc == null || tableDesc.getLastSnapshotPath() == null)) {
                executableResponse.setTargetSubject("The snapshot is deleted");
                executableResponse.setTargetSubjectError(true);
            }
        } else if (abstractExecutable instanceof InternalTableLoadingJob) {
            InternalTableLoadingJob internalTableJob = (InternalTableLoadingJob) abstractExecutable;
            if ("false".equals(internalTableJob.getParam("incrementalBuild"))
                    || "true".equals(internalTableJob.getParam("deletePartition"))) {
                executableResponse.setDataRangeEnd(Long.MAX_VALUE);
            } else {
                executableResponse.setDataRangeStart(Long.parseLong(internalTableJob.getParam("startTime")));
                executableResponse.setDataRangeEnd(Long.parseLong(internalTableJob.getParam("endTime")));
            }
            executableResponse.setTargetSubject(internalTableJob.getParam(NBatchConstants.P_TABLE_NAME));
            InternalTableDesc internalTableDesc = InternalTableManager
                    .getInstance(KylinConfig.getInstanceFromEnv(), abstractExecutable.getProject())
                    .getInternalTableDesc(executableResponse.getTargetSubject());
            if (internalTableDesc == null || internalTableDesc.getLocation() == null) {
                executableResponse
                        .setTargetSubject(executableResponse.getTargetSubject() + "not exist or has been deleted");
                executableResponse.setTargetSubjectError(true);
            }
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

        val stepRatio = calculateStepRatio(abstractExecutable, executablePO);
        executableResponse.setStepRatio(stepRatio);
        executableResponse.setProject(abstractExecutable.getProject());
        return executableResponse;
    }

    /**
     * Single Segment situation:
     * <p>
     * CurrentProgress = numberOfCompletedSteps / totalNumberOfSteps, accurate to the single digit percentage.
     * <p>
     * Among them, the progress of the "BUILD_LAYER"
     * step = numberOfCompletedIndexes / totalNumberOfIndexesToBeConstructed,
     * the progress of other steps will not be refined
     * ----------------------------------------------------------------------------------------------------------
     * multi segment situation:
     * <p>
     * CurrentProgress =
     * [numberOfPublicStepsCompleted + (numberOfSegmentStepsCompleted / numberOfSegments)] / totalNumberOfSteps
     * <p>
     * Another: "BUILD_LAYER" are not refined
     */
    public static float calculateStepRatio(AbstractExecutable abstractExecutable, ExecutablePO executablePO) {
        List<? extends AbstractExecutable> tasks = ((ChainedExecutable) abstractExecutable).getTasks();
        var successSteps = 0D;
        var stageCount = 0;
        for (AbstractExecutable task : tasks) {
            if (task instanceof ChainedStageExecutable) {
                final ChainedStageExecutable stageExecutable = (ChainedStageExecutable) task;
                Map<String, List<StageExecutable>> stageMap = Optional.ofNullable(stageExecutable.getStagesMap())
                        .orElse(Maps.newHashMap());
                var taskMapStageCount = Optional.of(stageMap.values()).orElse(Lists.newArrayList()).stream().findFirst()
                        .orElse(Lists.newArrayList()).size();

                if (0 != taskMapStageCount) {
                    // calculate sum step count, second step and stage is duplicate
                    stageCount = taskMapStageCount - 1;
                    successSteps += calculateSuccessStageInTaskMap(task, stageMap, executablePO);
                    continue;
                }
            }

            if (task.getStatusInMem().isNotBad()) {
                successSteps++;
            }
        }
        val stepCount = tasks.size() + stageCount;
        var stepRatio = (float) successSteps / stepCount;
        // in case all steps are succeeded, but the job is not succeeded, the stepRatio should be 99%
        if (stepRatio == 1 && ExecutableState.SUCCEED != abstractExecutable.getStatusInMem()) {
            stepRatio = 0.99F;
        }
        return stepRatio;
    }

    /** calculate stage count from segment */
    public static double calculateSuccessStageInTaskMap(AbstractExecutable task,
            Map<String, List<StageExecutable>> stageMap,
            ExecutablePO executablePO) {
        var successStages = 0D;
        boolean calculateIndexExecRadio = stageMap.size() == 1;
        for (Map.Entry<String, List<StageExecutable>> entry : stageMap.entrySet()) {
            double count = calculateSuccessStage(task, entry.getKey(), entry.getValue(), calculateIndexExecRadio,
                    executablePO);
            successStages += count;
        }
        return successStages / stageMap.size();
    }

    public static double calculateSuccessStage(AbstractExecutable task, String segmentId,
            List<StageExecutable> stageExecutables,
            boolean calculateIndexExecRadio, ExecutablePO executablePO) {
        var successStages = 0D;
        for (StageExecutable stage : stageExecutables) {
            if (ExecutableState.SUCCEED == stage.getStatusInMem(segmentId)
                    || ExecutableState.SKIP == stage.getStatusInMem(segmentId)
                    || ExecutableState.WARNING == stage.getStatusInMem(segmentId)) {
                successStages += 1;
                continue;
            }

            final String indexCountString = task.getParam(NBatchConstants.P_INDEX_COUNT);
            final String indexSuccess = stage.getOutput(segmentId, executablePO).getExtra()
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

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SegmentResponse {
        @JsonProperty("id")
        private String id; // Sequence ID within NDataflow
        @JsonProperty("status_to_display")
        private SegmentStatusEnumToDisplay statusToDisplay;
    }
}

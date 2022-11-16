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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.constant.JobStepCmdTypeEnum;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;

import io.kyligence.kap.guava20.shaded.common.collect.Sets;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ExecutableStepResponse {

    @JsonProperty("id")
    private String id;

    @JsonProperty("name")
    private String name;

    @JsonProperty("sequence_id")
    private int sequenceID;

    @JsonProperty("exec_cmd")
    private String execCmd;

    @JsonProperty("exec_start_time")
    private long execStartTime;
    @JsonProperty("exec_end_time")
    private long execEndTime;

    @JsonProperty("duration")
    private long duration;

    @JsonProperty("wait_time")
    private long waitTime;

    @JsonProperty("create_time")
    private long createTime;

    @JsonProperty("index_count")
    private long indexCount;

    @JsonProperty("success_index_count")
    private long successIndexCount;

    @JsonProperty("step_status")
    private JobStatusEnum status = JobStatusEnum.PENDING;

    @JsonProperty("cmd_type")
    private JobStepCmdTypeEnum cmdType = JobStepCmdTypeEnum.SHELL_CMD_HADOOP;

    @JsonProperty("info")
    private ConcurrentHashMap<String, String> info = new ConcurrentHashMap<String, String>();

    @JsonProperty("previous_step")
    private String previousStep;

    @JsonProperty("next_steps")
    private Set<String> nextSteps = Sets.newHashSet();

    @JsonProperty("failed_msg")
    private String shortErrMsg;

    @JsonProperty("failed_step_name")
    private String failedStepName;

    @JsonProperty("failed_step_id")
    private String failedStepId;

    @JsonProperty("failed_segment_id")
    private String failedSegmentId;

    @JsonProperty("failed_stack")
    private String failedStack;

    @JsonProperty("failed_resolve")
    private String failedResolve;

    @JsonProperty("failed_reason")
    private String failedReason;

    @JsonProperty("failed_code")
    private String failedCode;

    public void putInfo(String key, String value) {
        getInfo().put(key, value);
    }

    /**
     * for 3x rest api
     */
    @JsonUnwrapped
    private OldParams oldParams;

    @Getter
    @Setter
    public static class OldParams {
        @JsonProperty("exec_wait_time")
        private long execWaitTime;
    }

    @JsonProperty("sub_stages")
    private List<ExecutableStepResponse> subStages;

    @JsonProperty("segment_sub_stages")
    private Map<String, SubStages> segmentSubStages;

    @Getter
    @Setter
    public static class SubStages {
        @JsonProperty("duration")
        private long duration;
        @JsonProperty("wait_time")
        private long waitTime;
        @JsonProperty("step_ratio")
        private float stepRatio;
        @JsonProperty("name")
        private String name;
        @JsonProperty("start_time")
        private Long startTime;
        @JsonProperty("end_time")
        private Long endTime;
        @JsonProperty("exec_start_time")
        private long execStartTime;
        @JsonProperty("exec_end_time")
        private long execEndTime;
        @JsonProperty("stage")
        private List<ExecutableStepResponse> stage;
    }
}

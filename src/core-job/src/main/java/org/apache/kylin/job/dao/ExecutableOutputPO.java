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

import java.io.InputStream;
import java.io.Serializable;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import lombok.Getter;
import lombok.Setter;

/**
 *
 */
@Setter
@Getter
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class ExecutableOutputPO implements Serializable {

    @JsonIgnore
    private InputStream contentStream;

    @JsonProperty("content")
    private String content;

    @JsonProperty("log_path")
    private String logPath;

    @JsonProperty("status")
    private String status = "READY";

    @JsonProperty("info")
    private Map<String, String> info = Maps.newHashMap();

    @JsonProperty("last_modified")
    private long lastModified;

    @JsonProperty("createTime")
    private long createTime = System.currentTimeMillis();

    @JsonProperty("start_time")
    private long startTime;

    @JsonProperty("end_time")
    private long endTime;

    @JsonProperty("wait_time")
    private long waitTime;

    @JsonProperty("duration")
    private long duration;

    @JsonProperty("last_running_start_time")
    private long lastRunningStartTime;

    @JsonProperty("is_resumable")
    private boolean resumable = false;

    @JsonProperty("byte_size")
    private long byteSize;

    @JsonProperty("failed_msg")
    private String failedMsg;

    @JsonProperty("failed_step_id")
    private String failedStepId;

    @JsonProperty("failed_segment_id")
    private String failedSegmentId;

    @JsonProperty("failed_stack")
    private String failedStack;

    @JsonProperty("failed_reason")
    private String failedReason;

    public void addStartTime(long time) {
        //when ready -> suicidal/discarded, the start time is 0
        if (startTime == 0) {
            startTime = time;
        }
        endTime = 0;
    }

    public void addEndTime(long time) {
        Preconditions.checkArgument(startTime > 0L);
        endTime = time;
    }

    public void addDuration(long time) {
        if (time != 0 && time > lastRunningStartTime) {
            duration = duration + time - lastRunningStartTime;
        }
    }

    public void addLastRunningStartTime(long time) {
        lastRunningStartTime = time;
    }

    public void resetTime() {
        createTime = System.currentTimeMillis();
        startTime = 0L;
        endTime = 0L;
        waitTime = 0L;
        duration = 0L;
        lastRunningStartTime = 0L;
    }
}

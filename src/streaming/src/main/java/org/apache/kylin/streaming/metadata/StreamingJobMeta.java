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

package org.apache.kylin.streaming.metadata;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.cube.utils.StreamingUtils;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.streaming.constants.StreamingConstants;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class StreamingJobMeta extends RootPersistentEntity {

    @JsonProperty("model_alias")
    private String modelName;
    @JsonProperty("owner")
    private String owner;
    @JsonProperty("model_id")
    private String modelId;
    @JsonProperty("last_start_time")
    private String lastStartTime;
    @JsonProperty("last_end_time")
    private String lastEndTime;
    @JsonProperty("last_update_time")
    private String lastUpdateTime;
    @JsonProperty("last_batch_count")
    private Integer lastBatchCount;
    @JsonProperty("subscribe")
    private String topicName;
    @JsonProperty("fact_table")
    private String factTableName;
    @JsonProperty("job_status")
    private JobStatusEnum currentStatus;
    @JsonProperty("job_type")
    private JobTypeEnum jobType;
    @JsonProperty("process_id")
    private String processId;
    @JsonProperty("node_info")
    private String nodeInfo;
    @JsonProperty("job_execution_id")
    private Integer jobExecutionId;
    @JsonProperty("yarn_app_id")
    private String yarnAppId;
    @JsonProperty("yarn_app_url")
    private String yarnAppUrl;
    @JsonProperty("params")
    private Map<String, String> params = Maps.newHashMap();
    @JsonProperty("project")
    private String project;
    @JsonProperty("skip_listener")
    private boolean skipListener;
    @JsonProperty("action")
    private String action;

    public static StreamingJobMeta create(NDataModel model, JobStatusEnum status, JobTypeEnum jobType) {
        StreamingJobMeta meta = new StreamingJobMeta();
        Calendar calendar = Calendar.getInstance(TimeZone.getDefault(), Locale.getDefault(Locale.Category.FORMAT));
        meta.setCreateTime(calendar.getTimeInMillis());
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",
                Locale.getDefault(Locale.Category.FORMAT));
        meta.setLastUpdateTime(format.format(calendar.getTime()));
        meta.setCurrentStatus(status);
        meta.setJobType(jobType);
        meta.setModelId(model.getUuid());
        meta.setModelName(model.getAlias());
        meta.setFactTableName(model.getRootFactTableName());
        meta.setTopicName(model.getRootFactTable().getTableDesc().getKafkaConfig().getSubscribe());
        meta.setOwner(model.getOwner());
        meta.setUuid(StreamingUtils.getJobId(model.getUuid(), jobType.name()));
        initJobParams(meta, jobType);
        return meta;
    }

    private static void initJobParams(StreamingJobMeta jobMeta, JobTypeEnum jobType) {
        jobMeta.params.put(StreamingConstants.SPARK_MASTER, StreamingConstants.SPARK_MASTER_DEFAULT);
        jobMeta.params.put(StreamingConstants.SPARK_DRIVER_MEM, StreamingConstants.SPARK_DRIVER_MEM_DEFAULT);
        jobMeta.params.put(StreamingConstants.SPARK_EXECUTOR_INSTANCES,
                StreamingConstants.SPARK_EXECUTOR_INSTANCES_DEFAULT);
        jobMeta.params.put(StreamingConstants.SPARK_EXECUTOR_CORES, StreamingConstants.SPARK_EXECUTOR_CORES_DEFAULT);
        jobMeta.params.put(StreamingConstants.SPARK_EXECUTOR_MEM, StreamingConstants.SPARK_EXECUTOR_MEM_DEFAULT);
        jobMeta.params.put(StreamingConstants.SPARK_SHUFFLE_PARTITIONS,
                StreamingConstants.SPARK_SHUFFLE_PARTITIONS_DEFAULT);

        if (JobTypeEnum.STREAMING_BUILD == jobType) {
            jobMeta.params.put(StreamingConstants.STREAMING_DURATION, StreamingConstants.STREAMING_DURATION_DEFAULT);
            jobMeta.params.put(StreamingConstants.STREAMING_MAX_OFFSETS_PER_TRIGGER,
                    StreamingConstants.STREAMING_MAX_OFFSETS_PER_TRIGGER_DEFAULT);
        } else if (JobTypeEnum.STREAMING_MERGE == jobType) {
            jobMeta.params.put(StreamingConstants.STREAMING_SEGMENT_MAX_SIZE,
                    StreamingConstants.STREAMING_SEGMENT_MAX_SIZE_DEFAULT);
            jobMeta.params.put(StreamingConstants.STREAMING_SEGMENT_MERGE_THRESHOLD,
                    StreamingConstants.STREAMING_SEGMENT_MERGE_THRESHOLD_DEFAULT);
        }
        jobMeta.params.put(StreamingConstants.STREAMING_RETRY_ENABLE, "false");
    }

    public static String concatResourcePath(String name, String project, String jobType) {
        return new StringBuilder().append("/").append(project).append(ResourceStore.STREAMING_RESOURCE_ROOT).append("/")
                .append(name).append("_" + jobType.toLowerCase(Locale.ROOT)).toString();
    }

    @Override
    public String getResourcePath() {
        return concatResourcePath(getUuid(), project, jobType.name());
    }
}

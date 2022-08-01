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

import org.apache.kylin.metadata.model.AutoMergeTimeEnum;
import org.apache.kylin.metadata.model.RetentionRange;
import org.apache.kylin.metadata.model.VolatileRange;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.secondstorage.response.SecondStorageNode;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class ProjectConfigResponse {

    @JsonProperty("project")
    private String project;
    @JsonProperty("description")
    private String description;
    @JsonProperty("default_database")
    private String defaultDatabase;
    @JsonProperty("semi_automatic_mode")
    private boolean semiAutomaticMode;

    @JsonProperty("storage_quota_size")
    private long storageQuotaSize;

    @JsonProperty("push_down_enabled")
    private boolean pushDownEnabled;
    @JsonProperty("scd2_enabled")
    private boolean scd2Enabled;
    @JsonProperty("snapshot_manual_management_enabled")
    private boolean snapshotManualManagementEnabled;
    @JsonProperty("second_storage_enabled")
    private Boolean secondStorageEnabled;
    @JsonProperty("second_storage_nodes")
    private List<SecondStorageNode> secondStorageNodes;
    @JsonProperty("runner_class_name")
    private String runnerClassName;
    @JsonProperty("converter_class_names")
    private String converterClassNames;

    @JsonProperty("auto_merge_enabled")
    private boolean autoMergeEnabled = true;
    @JsonProperty("auto_merge_time_ranges")
    private List<AutoMergeTimeEnum> autoMergeTimeRanges;
    @JsonProperty("volatile_range")
    private VolatileRange volatileRange;
    @JsonProperty("create_empty_segment_enabled")
    private boolean createEmptySegmentEnabled = false;

    @JsonProperty("retention_range")
    private RetentionRange retentionRange;

    @JsonProperty("job_error_notification_enabled")
    private boolean jobErrorNotificationEnabled;
    @JsonProperty("data_load_empty_notification_enabled")
    private boolean dataLoadEmptyNotificationEnabled;
    @JsonProperty("job_notification_emails")
    private List<String> jobNotificationEmails;

    @JsonProperty("threshold")
    private int favoriteQueryThreshold;
    @JsonProperty("tips_enabled")
    private boolean favoriteQueryTipsEnabled;

    @JsonProperty("frequency_time_window")
    private String frequencyTimeWindow = "MONTH";
    @JsonProperty("low_frequency_threshold")
    private long lowFrequencyThreshold;

    @JsonProperty("yarn_queue")
    private String yarnQueue;

    @JsonProperty("expose_computed_column")
    private boolean exposeComputedColumn;

    @JsonProperty("kerberos_project_level_enabled")
    private boolean kerberosProjectLevelEnabled;

    @JsonProperty("principal")
    private String principal;

    @JsonProperty("favorite_rules")
    private Map<String, Object> favoriteRules;

    @JsonProperty("multi_partition_enabled")
    private boolean multiPartitionEnabled;

    @JsonProperty("query_history_download_max_size")
    private int queryHistoryDownloadMaxSize;

    @JsonProperty("jdbc_source_name")
    private String jdbcSourceName;
    @JsonProperty("jdbc_source_user")
    private String jdbcSourceUser;
    @JsonProperty("jdbc_source_pass")
    private String jdbcSourcePass;
    @JsonProperty("jdbc_source_connection_url")
    private String jdbcSourceConnectionUrl;
    @JsonProperty("jdbc_source_enable")
    private boolean jdbcSourceEnable;
    @JsonProperty("jdbc_source_driver")
    private String jdbcSourceDriver;

    public void setFrequencyTimeWindow(int frequencyTimeWindow) {
        switch (frequencyTimeWindow) {
            case 1:
                this.frequencyTimeWindow = "DAY";
                break;
            case 7:
                this.frequencyTimeWindow = "WEEK";
                break;
            case 30:
                this.frequencyTimeWindow = "MONTH";
                break;
            default:
                break;
        }

    }
}

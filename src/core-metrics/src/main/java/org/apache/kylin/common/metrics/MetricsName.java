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

package org.apache.kylin.common.metrics;

import java.util.Arrays;
import java.util.List;

import org.apache.kylin.common.util.ClusterConstant;

public enum MetricsName {

    //summary
    SUMMARY_COUNTER("summary_exec_total_times"), //
    SUMMARY_DURATION("summary_exec_total_duration"), //

    //project
    PROJECT_GAUGE("project_num_gauge", true), //need a gauge instead of a counter!
    //storage statistic
    PROJECT_STORAGE_SIZE("storage_size_gauge", true), //
    PROJECT_GARBAGE_SIZE("garbage_size_gauge", true), //

    //model
    MODEL_GAUGE("model_num_gauge", true), //
    HEALTHY_MODEL_GAUGE("non_broken_model_num_gauge", true), //
    OFFLINE_MODEL_GAUGE("offline_model_num_gauge", true), //
    MODEL_SEGMENTS("model_segments_num_gauge", true), //
    MODEL_STORAGE("model_total_storage_gauge", true), //
    MODEL_LAST_QUERY_TIME("model_last_query_time_gauge", true), //
    MODEL_QUERY_COUNT("model_query_count_gauge", true), //
    MODEL_BUILD_DURATION("model_build_duration", true), //
    MODEL_WAIT_DURATION("model_wait_duration", true), //
    MODEL_INDEX_NUM_GAUGE("model_index_num_gauge", true), //
    MODEL_EXPANSION_RATE_GAUGE("model_expansion_rate_gauge", true), //
    MODEL_BUILD_DURATION_HISTOGRAM("model_build_duration_histogram", true), //

    //query
    QUERY("query_total_times", ClusterConstant.ALL, ClusterConstant.QUERY), //
    QUERY_TOTAL_DURATION("query_total_duration", ClusterConstant.ALL,
            ClusterConstant.QUERY), QUERY_SLOW("gt10s_query_total_times", ClusterConstant.ALL, ClusterConstant.QUERY), //
    QUERY_FAILED("failed_query_total_times", ClusterConstant.ALL, ClusterConstant.QUERY), //
    QUERY_PUSH_DOWN("pushdown_query_total_times", ClusterConstant.ALL, ClusterConstant.QUERY), //
    QUERY_PUSH_DOWN_RATIO("pushdown_query_ratio", ClusterConstant.ALL, ClusterConstant.QUERY), //
    QUERY_CONSTANTS("query_constants_total_times", ClusterConstant.ALL, ClusterConstant.QUERY), //
    QUERY_CONSTANTS_RATIO("query_constants_ratio", ClusterConstant.ALL, ClusterConstant.QUERY), //
    QUERY_TIMEOUT("timeout_query_total_times", ClusterConstant.ALL, ClusterConstant.QUERY), //
    QUERY_LATENCY("query_latency", ClusterConstant.ALL, ClusterConstant.QUERY), //
    QUERY_SLOW_RATE("gt10s_query_rate", ClusterConstant.ALL, ClusterConstant.QUERY), //
    QUERY_FAILED_RATE("failed_query_rate", ClusterConstant.ALL, ClusterConstant.QUERY), //
    QUERY_PUSH_DOWN_RATE("pushdown_query_rate", ClusterConstant.ALL, ClusterConstant.QUERY), //
    QUERY_CONSTANTS_RATE("query_constants_rate", ClusterConstant.ALL, ClusterConstant.QUERY), //
    QUERY_TIMEOUT_RATE("timeout_query_rate", ClusterConstant.ALL, ClusterConstant.QUERY), //
    QUERY_CACHE("cache_query_total_times", ClusterConstant.ALL, ClusterConstant.QUERY), //
    QUERY_CACHE_RATIO("cache_query_ratio", ClusterConstant.ALL, ClusterConstant.QUERY), //
    QUERY_AGG_INDEX("agg_index_query_total_times", ClusterConstant.ALL, ClusterConstant.QUERY), //
    QUERY_AGG_INDEX_RATIO("agg_index_query_ratio", ClusterConstant.ALL, ClusterConstant.QUERY), //
    QUERY_TABLE_INDEX("table_index_query_total_times", ClusterConstant.ALL, ClusterConstant.QUERY), //
    QUERY_TABLE_INDEX_RATIO("table_index_query_ratio", ClusterConstant.ALL, ClusterConstant.QUERY), //
    QUERY_SCAN_BYTES("query_scan_bytes", ClusterConstant.ALL, ClusterConstant.QUERY), //
    QUERY_LT_1S("lt_1s_total_times", ClusterConstant.ALL, ClusterConstant.QUERY), //
    QUERY_1S_3S("bw_1s_3s_total_times", ClusterConstant.ALL, ClusterConstant.QUERY), //
    QUERY_3S_5S("bw_3s_5s_total_times", ClusterConstant.ALL, ClusterConstant.QUERY), //
    QUERY_5S_10S("bw_5s_10s_total_times", ClusterConstant.ALL, ClusterConstant.QUERY), //
    QUERY_LT_1S_RATIO("lt_1s_ratio", ClusterConstant.ALL, ClusterConstant.QUERY), //
    QUERY_1S_3S_RATIO("bw_1s_3s_ratio", ClusterConstant.ALL, ClusterConstant.QUERY), //
    QUERY_3S_5S_RATIO("bw_3s_5s_ratio", ClusterConstant.ALL, ClusterConstant.QUERY), //
    QUERY_5S_10S_RATIO("bw_5s_10s_ratio", ClusterConstant.ALL, ClusterConstant.QUERY), //
    QUERY_SLOW_RATIO("gt10s_query_ratio", ClusterConstant.ALL, ClusterConstant.QUERY), //

    //job
    JOB("job_created_total_times", ClusterConstant.ALL, ClusterConstant.JOB), //
    JOB_DURATION("finished_jobs_total_duration", ClusterConstant.ALL, ClusterConstant.JOB), //
    JOB_FINISHED("job_finished_total_times", ClusterConstant.ALL, ClusterConstant.JOB), //
    JOB_DURATION_HISTOGRAM("job_duration", ClusterConstant.ALL, ClusterConstant.JOB), //
    JOB_STEP_ATTEMPTED("job_step_attempted_total_times", ClusterConstant.ALL, ClusterConstant.JOB), //
    JOB_FAILED_STEP_ATTEMPTED("failed_job_step_attempted_total_times", ClusterConstant.ALL, ClusterConstant.JOB), //
    JOB_RESUMED("job_resumed_total_times", ClusterConstant.ALL, ClusterConstant.JOB), //
    JOB_DISCARDED("job_discarded_total_times", ClusterConstant.ALL, ClusterConstant.JOB), //
    JOB_ERROR("error_job_total_times", ClusterConstant.ALL, ClusterConstant.JOB), //
    JOB_ERROR_GAUGE("error_job_num_gauge", true), //
    JOB_RUNNING_GAUGE("running_job_num_gauge", true), //
    JOB_PENDING_GAUGE("pending_job_num_gauge", true), //
    JOB_WAIT_DURATION("job_wait_duration", ClusterConstant.ALL, ClusterConstant.JOB), //

    // host
    QUERY_HOST("query_num_per_host"), //
    QUERY_SCAN_BYTES_HOST("query_scan_bytes_per_host"), //
    QUERY_TIME_HOST("query_time_per_host"), //

    //garbage management
    STORAGE_CLEAN("storage_clean_total_times"), //
    STORAGE_CLEAN_DURATION("storage_clean_total_duration"), //
    STORAGE_CLEAN_FAILED("failed_storage_clean_total_times"), //

    //metadata management
    METADATA_CLEAN("metadata_clean_total_times"), //
    METADATA_BACKUP("metadata_backup_total_times"), //
    METADATA_BACKUP_DURATION("metadata_backup_total_duration"), //
    METADATA_BACKUP_FAILED("failed_metadata_backup_total_times"), //
    METADATA_OPS_CRON("metadata_ops_total_times"), //
    METADATA_OPS_CRON_SUCCESS("metadata_success_ops_total_times"), //

    // JVM
    HEAP_MAX("jvm_memory_heap_max"), //
    HEAP_USED("jvm_memory_heap_used"), //
    HEAP_USAGE("jvm_memory_heap_usage"), //
    JVM_GC("jvm_garbage_collection"), //
    JVM_AVAILABLE_CPU("jvm_available_cpu_count"), //

    //spark context
    SPARDER_RESTART("sparder_restart_total_times"), //

    //transaction
    TRANSACTION_RETRY_COUNTER("transaction_retry_total_times"), //
    TRANSACTION_LATENCY("transaction_latency"), //

    //user management
    USER_GAUGE("user_num_gauge", true), //

    //table management
    TABLE_GAUGE("table_num_gauge", true), //

    //database management
    DB_GAUGE("db_num_gauge", true), //

    //spark query load
    QUERY_LOAD("query_load", ClusterConstant.ALL, ClusterConstant.QUERY), //
    CPU_CORES("cpu_cores", ClusterConstant.ALL, ClusterConstant.QUERY),

    // ################################################################# Used in prometheus
    JOB_COUNT("job_count", ClusterConstant.ALL, ClusterConstant.JOB), JOB_TOTAL_DURATION("job_total_duration",
            ClusterConstant.ALL, ClusterConstant.JOB), SUCCESSFUL_JOB_COUNT("successful_job_count", ClusterConstant.ALL,
                    ClusterConstant.JOB), ERROR_JOB_COUNT("error_job_count", ClusterConstant.ALL,
                            ClusterConstant.JOB), TERMINATED_JOB_COUNT("terminated_job_count", ClusterConstant.ALL,
                                    ClusterConstant.JOB), JOB_COUNT_LT_5("job_count_lt_5", ClusterConstant.ALL,
                                            ClusterConstant.JOB), JOB_COUNT_5_10("job_count_5_10", ClusterConstant.ALL,
                                                    ClusterConstant.JOB), JOB_COUNT_10_30("job_count_10_30",
                                                            ClusterConstant.ALL, ClusterConstant.JOB), JOB_COUNT_30_60(
                                                                    "job_count_30_60", ClusterConstant.ALL,
                                                                    ClusterConstant.JOB), JOB_COUNT_GT_60(
                                                                            "job_count_gt_60", ClusterConstant.ALL,
                                                                            ClusterConstant.JOB);

    private final String value;

    private final boolean onlyLeader;

    private final List<String> clusterList;

    MetricsName(String value) {
        this(value, false, ClusterConstant.ALL, ClusterConstant.JOB, ClusterConstant.QUERY);
    }

    MetricsName(String value, boolean onlyLeader) {
        this(value, onlyLeader, new String[0]);
    }

    MetricsName(String value, String... clusterList) {
        this(value, false, clusterList);
    }

    MetricsName(String value, boolean onlyLeader, String... clusterList) {
        this.value = value;
        this.onlyLeader = onlyLeader;
        this.clusterList = Arrays.asList(clusterList);
    }

    public String getVal() {
        return this.value;
    }

    public boolean support(String cluster, boolean isLeader) {
        if (onlyLeader) {
            return isLeader;
        }

        return clusterList.contains(cluster);
    }

    public static MetricsName getMetricsName(String value) {
        for (MetricsName metricsName : MetricsName.values()) {
            if (metricsName.getVal().equals(value)) {
                return metricsName;
            }
        }

        return null;
    }
}

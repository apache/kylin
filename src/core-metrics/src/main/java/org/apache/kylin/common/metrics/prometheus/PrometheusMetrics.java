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
package org.apache.kylin.common.metrics.prometheus;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.Getter;

@Getter
public enum PrometheusMetrics {

    JVM_DB_CONNECTIONS("ke_db_connections", Type.INSTANCE_METRIC), //

    SPARK_TASKS("spark_tasks", Type.INSTANCE_METRIC), //
    SPARK_TASK_UTILIZATION("spark_tasks_utilization", Type.INSTANCE_METRIC), //

    QUERY_SECONDS("ke_queries_seconds", Type.PROJECT_METRIC), //
    QUERY_SCAN_BYTES("ke_queries_scan_bytes", Type.PROJECT_METRIC), //
    QUERY_RESULT_ROWS("ke_queries_result_rows", Type.PROJECT_METRIC), //
    QUERY_JOBS("ke_queries_jobs", Type.PROJECT_METRIC), //
    QUERY_STAGES("ke_queries_stages", Type.PROJECT_METRIC), //
    QUERY_TASKS("ke_queries_tasks", Type.PROJECT_METRIC), //

    SPARDER_UP("ke_sparder_up", Type.INSTANCE_METRIC), //

    JOB_COUNTS("ke_job_counts", Type.PROJECT_METRIC), //
    JOB_MINUTES("ke_job_minutes", Type.PROJECT_METRIC), //
    JOB_LONG_RUNNING("ke_long_running_jobs", Type.PROJECT_METRIC), //

    MODEL_BUILD_DURATION("ke_model_build_minutes", Type.PROJECT_METRIC | Type.MODEL_METRIC);

    private static class Type {
        public static final int GLOBAL = 0;
        public static final int INSTANCE_METRIC = 0x0001;
        public static final int PROJECT_METRIC = 0x0002;
        public static final int MODEL_METRIC = 0x0004;
    }

    private final String value;
    private final int types;

    PrometheusMetrics(String value, int types) {
        this.value = value;
        this.types = types;
    }

    public static Set<PrometheusMetrics> listModelMetrics() {
        return Stream.of(PrometheusMetrics.values()).filter(metric -> (metric.getTypes() & Type.MODEL_METRIC) != 0)
                .collect(Collectors.toSet());
    }
}

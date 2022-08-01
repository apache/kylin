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

package org.apache.kylin.common.constant;

import static org.apache.kylin.common.constant.Constants.KYLIN_SOURCE_JDBC_SOURCE_ENABLE_KEY;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.Getter;

@Getter
public enum NonCustomProjectLevelConfig {
    // project setting
    RECOMMENDATION_AUTO_MODE("kylin.metadata.semi-automatic-mode"), OLD_RECOMMENDATION_AUTO_MODE(
            "kap.metadata.semi-automatic-mode"),

    STORAGE_QUOTA_SIZE("kylin.storage.quota-in-giga-bytes"),

    FREQUENCY_TIME_WINDOW_IN_DAYS("kylin.cube.frequency-time-window"), LOW_FREQUENCY_THRESHOLD(
            "kylin.cube.low-frequency-threshold"),

    PUSH_DOWN_ENABLED("kylin.query.pushdown-enabled"),

    JOB_DATA_LOAD_EMPTY_NOTIFICATION_ENABLED(
            "kylin.job.notification-on-empty-data-load"), JOB_ERROR_NOTIFICATION_ENABLED(
                    "kylin.job.notification-on-job-error"), NOTIFICATION_ADMIN_EMAILS(
                            "kylin.job.notification-admin-emails"),

    ENGINE_SPARK_YARN_QUEUE("kylin.engine.spark-conf.spark.yarn.queue"),

    MULTI_PARTITION_ENABLED("kylin.model.multi-partition-enabled"),

    SNAPSHOT_MANUAL_MANAGEMENT_ENABLED("kylin.snapshot.manual-management-enabled"),

    EXPOSE_COMPUTED_COLUMN("kylin.query.metadata.expose-computed-column"), OLD_EXPOSE_COMPUTED_COLUMN(
            "kap.query.metadata.expose-computed-column"),

    QUERY_NON_EQUI_JOIN_MODEL_ENABLED("kylin.query.non-equi-join-model-enabled"),

    KYLIN_SOURCE_JDBC_SOURCE_ENABLE(KYLIN_SOURCE_JDBC_SOURCE_ENABLE_KEY),

    // extra
    DATASOURCE_TYPE("kylin.source.default");

    private final String value;

    NonCustomProjectLevelConfig(String value) {
        this.value = value;
    }

    public static Set<String> listAllConfigNames() {
        return Stream.of(NonCustomProjectLevelConfig.values()).map(NonCustomProjectLevelConfig::getValue)
                .collect(Collectors.toSet());
    }
}

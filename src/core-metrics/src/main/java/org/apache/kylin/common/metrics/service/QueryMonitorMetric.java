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
package org.apache.kylin.common.metrics.service;

import java.util.Map;

import org.apache.kylin.shaded.influxdb.org.influxdb.annotation.Column;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class QueryMonitorMetric extends MonitorMetric {
    public static final String QUERY_MONITOR_METRIC_TABLE = "tb_query";

    @JsonProperty("response_time")
    @Column(name = "response_time")
    private Long lastResponseTime = -1L;

    @JsonProperty("error_accumulated")
    @Column(name = "error_accumulated")
    private Integer errorAccumulated = 0;

    @JsonProperty("spark_restarting")
    @Column(name = "spark_restarting")
    private Boolean sparkRestarting = false;

    @Override
    public Map<String, Object> getFields() {
        Map<String, Object> fields = super.getFields();
        fields.put("response_time", this.getLastResponseTime());
        fields.put("error_accumulated", this.getErrorAccumulated());
        fields.put("spark_restarting", this.getSparkRestarting());
        return fields;
    }

    @Override
    public String getTable() {
        return QUERY_MONITOR_METRIC_TABLE;
    }
}

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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.guava30.shaded.common.collect.Maps;

import org.apache.kylin.shaded.influxdb.org.influxdb.annotation.Column;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class MonitorMetric implements MonitorMetricOperation {
    @JsonProperty("host")
    @Column(name = "host", tag = true)
    private String host;

    @JsonProperty
    @Column(name = "ip", tag = true)
    private String ip;

    @JsonProperty("port")
    @Column(name = "port", tag = true)
    private String port;

    @JsonProperty("pid")
    @Column(name = "pid", tag = true)
    private String pid;

    @JsonProperty("node_type")
    @Column(name = "node_type", tag = true)
    private String nodeType;

    @JsonProperty("create_time")
    @Column(name = "create_time")
    private Long createTime;

    @JsonIgnore
    public String getInstanceName() {
        return host + ":" + port;
    }

    @JsonIgnore
    public String getIpPort() {
        return ip + ":" + port;
    }

    @Override
    public Map<String, String> getTags() {
        Map<String, String> tags = Maps.newHashMap();
        tags.put("host", this.getHost());
        tags.put("ip", this.getIp());
        tags.put("port", String.valueOf(this.getPort()));
        tags.put("pid", String.valueOf(this.getPid()));
        tags.put("node_type", String.valueOf(this.getNodeType()));
        return tags;
    }

    @Override
    public Map<String, Object> getFields() {
        Map<String, Object> fields = Maps.newHashMap();
        fields.put("create_time", this.getCreateTime());
        return fields;
    }

    @Override
    public String getTable() {
        return "None";
    }
}

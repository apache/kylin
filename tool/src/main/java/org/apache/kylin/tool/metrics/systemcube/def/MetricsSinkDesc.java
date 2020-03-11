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

package org.apache.kylin.tool.metrics.systemcube.def;

import java.util.Map;

import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.IStorageAware;
import org.apache.kylin.metrics.lib.impl.hive.HiveReservoirReporter;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.metrics.lib.impl.kafka.KafkaReservoirReporter;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class MetricsSinkDesc {

    @JsonProperty("sink")
    private String sinkType;

    @JsonProperty("storage_type")
    protected int storageType = IStorageAware.ID_SHARDED_HBASE;

    @JsonProperty("cube_desc_override_properties")
    protected Map<String, String> cubeDescOverrideProperties = Maps.newHashMap();

    @JsonProperty("table_properties")
    protected Map<String, String> tableProperties = Maps.newHashMap();

    public int getStorageType() {
        return storageType;
    }

    public int getSourceType() {
        if (sinkType.equalsIgnoreCase("hive")) {
            return ISourceAware.ID_HIVE;
        } else if (sinkType.equalsIgnoreCase("kafka")) {
            return ISourceAware.ID_KAFKA;
        } else {
            return ISourceAware.ID_HIVE;
        }
    }

    public String getTableNameForMetrics(String subject) {
        if (sinkType.equalsIgnoreCase("hive")) {
            return HiveReservoirReporter.getTableFromSubject(subject);
        } else if (sinkType.equalsIgnoreCase("kafka")) {
            return KafkaReservoirReporter.getTableFromSubject(subject);
        } else {
            return HiveReservoirReporter.getTableFromSubject(subject);
        }
    }

    public Map<String, String> getCubeDescOverrideProperties() {
        return cubeDescOverrideProperties;
    }

    public void setCubeDescOverrideProperties(Map<String, String> cubeDescOverrideProperties) {
        this.cubeDescOverrideProperties = cubeDescOverrideProperties;
    }

    public String getSinkType() {
        return sinkType;
    }

    public boolean useKafka(){
        return sinkType.equalsIgnoreCase("kafka");
    }
    public boolean useHive(){
        return sinkType.equalsIgnoreCase("hive");
    }


    public Map<String, String> getTableProperties() {
        return tableProperties;
    }
}

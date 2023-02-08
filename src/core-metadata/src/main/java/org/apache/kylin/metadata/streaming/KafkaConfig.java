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

package org.apache.kylin.metadata.streaming;

import java.io.Serializable;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.metadata.MetadataConstants;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import lombok.Data;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
@Data
public class KafkaConfig extends RootPersistentEntity implements Serializable {
    @JsonProperty("database")
    private String database;

    @JsonProperty("name")
    private String name;

    @JsonProperty("project")
    private String project;

    @JsonProperty("kafka_bootstrap_servers")
    private String kafkaBootstrapServers;

    @JsonProperty("subscribe")
    private String subscribe;

    @JsonProperty("starting_offsets")
    private String startingOffsets;

    @JsonProperty("batch_table_identity")
    private String batchTable;

    @JsonProperty("parser_name")
    private String parserName;

    private Map<String, String> kafkaParam;

    public KafkaConfig() {
    }

    public KafkaConfig(KafkaConfig other) {
        this.uuid = other.uuid;
        this.lastModified = other.lastModified;
        this.createTime = other.createTime;
        this.name = other.name;
        this.kafkaBootstrapServers = other.kafkaBootstrapServers;
        this.subscribe = other.subscribe;
        this.startingOffsets = other.startingOffsets;
        this.project = other.project;
        this.parserName = other.parserName;
        this.batchTable = other.batchTable;
    }

    public Map<String, String> getKafkaParam() {
        Preconditions.checkState(
                this.kafkaBootstrapServers != null && this.subscribe != null && this.startingOffsets != null,
                "table are not streaming table");
        if (kafkaParam == null) {
            kafkaParam = Maps.<String, String> newHashMap();
            kafkaParam.put("kafka.bootstrap.servers", this.kafkaBootstrapServers);
            kafkaParam.put("subscribe", subscribe);
            kafkaParam.put("startingOffsets", startingOffsets);
            kafkaParam.put("failOnDataLoss", "false");
        }
        return kafkaParam;
    }

    public String getIdentity() {
        return resourceName();
    }

    @Override
    public String resourceName() {
        String originIdentity = String.format(Locale.ROOT, "%s.%s", this.database, this.name);
        return originIdentity.toUpperCase(Locale.ROOT);
    }

    public String getResourcePath() {
        return concatResourcePath(resourceName(), project);
    }

    public static String concatResourcePath(String name, String project) {
        return new StringBuilder().append("/").append(project).append(ResourceStore.KAFKA_RESOURCE_ROOT).append("/")
                .append(name).append(MetadataConstants.FILE_SURFIX).toString();
    }

    public String getBatchTableAlias() {
        return this.batchTable.split("\\.")[1];
    }

    public boolean hasBatchTable() {
        return StringUtils.isNotEmpty(this.batchTable);
    }
}

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

package org.apache.kylin.rest.request;

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.streaming.KafkaConfig;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class StreamingRequest {

    @JsonProperty("project")
    private String project;

    @JsonProperty("table_desc")
    private TableDesc tableDesc;

    @JsonProperty("kafka_config")
    private KafkaConfig kafkaConfig;

    @JsonProperty("successful")
    private boolean successful;

    @JsonProperty("message")
    private String message;

    // CollectKafkaStats.JSON_MESSAGE /CollectKafkaStats.BINARY_MESSAGE
    @JsonProperty("message_type")
    private String messageType;

    @JsonProperty("fuzzy_key")
    private String fuzzyKey;

    @JsonProperty("cluster_index")
    private int clusterIndex;

    @JsonProperty("messages")
    private List<String> messages = new ArrayList<>();

}

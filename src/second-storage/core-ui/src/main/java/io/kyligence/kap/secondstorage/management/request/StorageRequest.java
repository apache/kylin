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
package io.kyligence.kap.secondstorage.management.request;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;


@Data
public class StorageRequest {
    @JsonProperty("project")
    private String project;
    @JsonProperty("model")
    private String model;
    @JsonProperty("model_name")
    private String modelName;
    @JsonProperty("segment_ids")
    private List<String> segmentIds;
    @JsonProperty("segment_names")
    private List<String> segmentNames;
    @JsonProperty("type")
    private StorageType type = StorageType.CLICKHOUSE;

    public List<String> getSegmentIds() {
        return segmentIds == null ? Collections.emptyList(): segmentIds;
    }

    public List<String> getSegmentNames() {
        return segmentNames == null ? Collections.emptyList(): segmentNames;
    }

    public enum StorageType {
        CLICKHOUSE
    }

    private static final Set<StorageType> SUPPORTED_STORAGE =
            Stream.of(StorageType.CLICKHOUSE).collect(Collectors.toSet());

    public boolean storageTypeSupported() {
        return SUPPORTED_STORAGE.contains(type);
    }
}

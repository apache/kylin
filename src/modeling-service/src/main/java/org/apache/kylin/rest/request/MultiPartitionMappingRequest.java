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

import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.insensitive.ProjectInsensitiveRequest;
import org.apache.kylin.metadata.model.MultiPartitionKeyMappingImpl;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
public class MultiPartitionMappingRequest implements ProjectInsensitiveRequest {
    @JsonProperty("project")
    private String project;

    @JsonProperty("alias_columns")
    private List<String> aliasCols;

    @JsonProperty("multi_partition_columns")
    private List<String> partitionCols;

    @JsonProperty("value_mapping")
    private List<MappingRequest<List<String>, List<String>>> valueMapping;

    public MultiPartitionKeyMappingImpl convertToMultiPartitionMapping() {
        return new MultiPartitionKeyMappingImpl(partitionCols, aliasCols, mappingsToPairs());
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class MappingRequest<T1, T2> {
        T1 origin;
        T2 target;
    }

    public List<Pair<List<String>, List<String>>> mappingsToPairs() {
        return this.valueMapping != null ? this.valueMapping.stream()
                .map(mapping -> Pair.newPair(mapping.origin, mapping.target)).collect(Collectors.toList()) : null;
    }

}

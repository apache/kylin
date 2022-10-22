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
package org.apache.kylin.rest.response;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.ModelDimensionDesc;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Sets;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class NDataModelOldParams implements Serializable {
    @JsonProperty("name")
    private String name;

    @JsonProperty("lookups")
    private List<JoinTableDesc> joinTables;

    @JsonProperty("is_streaming")
    private boolean streaming;

    @JsonProperty("size_kb")
    private long sizeKB;

    @JsonProperty("input_records_count")
    private long inputRecordCnt = 0L;

    @JsonProperty("input_records_size")
    private long inputRecordSizeBytes = 0L;

    @JsonProperty("project")
    private String projectName;

    @JsonProperty("dimensions")
    private List<ModelDimensionDesc> dimensions;

    public void setDimensions(List<NDataModelResponse.SimplifiedNamedColumn> simplifiedDimensions) {
        if (CollectionUtils.isEmpty(simplifiedDimensions)) {
            return;
        }

        Map<String, Set<String>> dimCandidate = new HashMap<>();
        for (NDataModelResponse.SimplifiedNamedColumn dimension : simplifiedDimensions) {
            String[] tableColumn = dimension.getAliasDotColumn().split("\\.");
            String table = tableColumn[0];
            String column = tableColumn[1];
            addCandidate(dimCandidate, table, column);
        }

        List<ModelDimensionDesc> dims = new ArrayList<>();
        for (Map.Entry<String, Set<String>> dimensionEntry : dimCandidate.entrySet()) {
            ModelDimensionDesc dimension = new ModelDimensionDesc();
            dimension.setTable(dimensionEntry.getKey());
            dimension.setColumns(dimensionEntry.getValue().toArray(new String[0]));
            dims.add(dimension);
        }
        this.dimensions = dims;
    }

    private static void addCandidate(Map<String, Set<String>> tblColMap, String table, String column) {
        tblColMap.computeIfAbsent(table, k -> Sets.newHashSet());
        tblColMap.get(table).add(column);
    }
}

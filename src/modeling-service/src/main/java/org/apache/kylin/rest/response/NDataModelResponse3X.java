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

import java.util.List;
import java.util.Set;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.rest.constant.ModelStatusToDisplayEnum;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Sets;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class NDataModelResponse3X extends NDataModel {
    @JsonProperty("status")
    private String status;

    @JsonProperty("last_build_end")
    private String lastBuildEnd;

    @JsonProperty("storage")
    private long storage;

    @JsonProperty("source")
    private long source;

    @JsonProperty("expansion_rate")
    private String expansionrate;

    @JsonProperty("usage")
    private long usage;

    @JsonProperty("model_broken")
    private boolean modelBroken;

    @JsonProperty("root_fact_table_deleted")
    private boolean rootFactTableDeleted = false;

    @JsonProperty("segments")
    private List<NDataSegmentResponse> segments;

    @JsonProperty("recommendations_count")
    private int recommendationsCount;

    @JsonProperty("available_indexes_count")
    private long availableIndexesCount;

    @JsonProperty("empty_indexes_count")
    private long emptyIndexesCount;

    @JsonProperty("segment_holes")
    private List<SegmentRange> segmentHoles;

    @JsonProperty("total_indexes")
    private long totalIndexes;

    private long lastModify;

    @JsonProperty("simplified_dimensions")
    private List<NDataModel.NamedColumn> namedColumns;

    @JsonProperty("all_measures")
    private List<NDataModel.Measure> measures;

    @JsonProperty("simplified_measures")
    private List<SimplifiedMeasure> simplifiedMeasures;

    @JsonProperty("name")
    private String name;

    @JsonProperty("lookups")
    private List<JoinTableDesc> joinTables;

    @JsonProperty("is_streaming")
    private boolean streaming;

    @JsonProperty("size_kb")
    private long sizeKB;

    @JsonProperty("input_records_count")
    private long inputRecordCnt;

    @JsonProperty("input_records_size")
    private long inputRecordSizeBytes;

    @JsonProperty("project")
    private String projectName;

    @JsonProperty("unauthorized_tables")
    private Set<String> unauthorizedTables = Sets.newHashSet();

    @JsonProperty("unauthorized_columns")
    private Set<String> unauthorizedColumns = Sets.newHashSet();

    @JsonProperty("visible")
    public boolean isVisible() {
        return unauthorizedTables.isEmpty() && unauthorizedColumns.isEmpty();
    }

    public enum ModelStatus3XEnum {
        READY, DISABLED, WARNING, DESCBROKEN;

        public static ModelStatus3XEnum convert(ModelStatusToDisplayEnum modelStatusToDisplayEnum) {
            if (null == modelStatusToDisplayEnum) {
                return null;
            }

            switch (modelStatusToDisplayEnum) {
            case ONLINE:
                return READY;
            case OFFLINE:
                return DISABLED;
            case WARNING:
                return WARNING;
            case BROKEN:
                return DESCBROKEN;
            default:
                break;
            }
            return null;
        }
    }

    public static NDataModelResponse3X convert(NDataModelResponse nDataModelResponse) throws Exception {
        NDataModelResponse3X nDataModelResponse3X = JsonUtil.readValue(JsonUtil.writeValueAsString(nDataModelResponse),
                NDataModelResponse3X.class);
        ModelStatus3XEnum newStatus = ModelStatus3XEnum.convert(nDataModelResponse.getStatus());
        nDataModelResponse3X.setStatus(null == newStatus ? null : newStatus.name());
        nDataModelResponse3X.setMvcc(nDataModelResponse.getMvcc());

        return nDataModelResponse3X;
    }
}

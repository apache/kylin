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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.springframework.beans.BeanUtils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@EqualsAndHashCode(callSuper = true)
public class NDataModelLiteResponse extends NDataModelResponse {

    @JsonProperty("empty_model")
    private boolean emptyModel;

    @JsonProperty("partition_column_in_dims")
    private boolean partitionColumnInDims;

    @JsonProperty("batch_id")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private String batchId;

    @JsonProperty("streaming_indexes")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private Long streamingIndexes;

    @JsonProperty("batch_partition_desc")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private PartitionDesc batchPartitionDesc;

    @SuppressWarnings("rawtypes")
    @JsonProperty("batch_segment_holes")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private List<SegmentRange> batchSegmentHoles;

    @JsonProperty("batch_segments")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private List<NDataSegmentResponse> batchSegments = new ArrayList<>();

    @JsonProperty("simplified_tables")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Override
    public List<SimplifiedTableResponse> getSimpleTables() {
        return Collections.emptyList();
    }

    @JsonProperty("selected_columns")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Override
    public List<SimplifiedNamedColumn> getSelectedColumns() {
        return Collections.emptyList();
    }

    @JsonProperty("simplified_dimensions")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Override
    public List<SimplifiedNamedColumn> getNamedColumns() {
        return Collections.emptyList();
    }

    @JsonProperty("all_named_columns")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Override
    public List<NamedColumn> getAllNamedColumns() {
        return Collections.emptyList();
    }

    @JsonProperty("all_measures")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Override
    public List<Measure> getMeasures() {
        return Collections.emptyList();
    }

    @JsonProperty("simplified_measures")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Override
    public List<SimplifiedMeasure> getSimplifiedMeasures() {
        return Collections.emptyList();
    }

    @JsonProperty("segments")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Override
    public List<NDataSegmentResponse> getSegments() {
        return Collections.emptyList();
    }

    public NDataModelLiteResponse(NDataModelResponse response, NDataModel dataModel) {
        super(dataModel);
        if (dataModel.isFusionModel()) {
            FusionModelResponse fusionModelResponse = (FusionModelResponse) response;
            this.setBatchId(fusionModelResponse.getBatchId());
            this.setBatchPartitionDesc(fusionModelResponse.getBatchPartitionDesc());
            this.setStreamingIndexes(fusionModelResponse.getStreamingIndexes());
            this.setBatchSegmentHoles(fusionModelResponse.getBatchSegmentHoles());
        }
        BeanUtils.copyProperties(response, this);
    }
}

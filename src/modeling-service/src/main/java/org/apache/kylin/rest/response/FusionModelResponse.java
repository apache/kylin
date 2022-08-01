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
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.FusionModel;
import org.apache.kylin.metadata.model.FusionModelManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.rest.constant.ModelStatusToDisplayEnum;
import org.apache.kylin.rest.util.ModelUtils;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.val;

@Setter
@Getter
public class FusionModelResponse extends NDataModelResponse {

    @JsonProperty("batch_id")
    private String batchId;

    @JsonProperty("streaming_indexes")
    private long streamingIndexes;

    @EqualsAndHashCode.Include
    @JsonProperty("batch_partition_desc")
    private PartitionDesc batchPartitionDesc;

    @JsonProperty("batch_segments")
    private List<NDataSegmentResponse> batchSegments = new ArrayList<>();

    @JsonProperty("batch_segment_holes")
    private List<SegmentRange> batchSegmentHoles;

    public FusionModelResponse(NDataModel dataModel) {
        super(dataModel);
    }

    @Override
    protected void computedDisplayInfo(NDataModel modelDesc) {
        NDataflowManager dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), this.getProject());
        NDataflow streamingDataflow = dfManager.getDataflow(modelDesc.getUuid());
        FusionModel fusionModel = FusionModelManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject())
                .getFusionModel(modelDesc.getFusionId());
        val batchModel = fusionModel.getBatchModel();
        if (batchModel.isBroken() || modelDesc.isBroken()) {
            this.setStatus(ModelStatusToDisplayEnum.BROKEN);
        }
        this.setBatchId(batchModel.getUuid());
        this.setFusionId(modelDesc.getFusionId());
        this.setBatchId(fusionModel.getBatchModel().getUuid());
        NDataflow batchDataflow = dfManager.getDataflow(batchId);
        this.setLastBuildTime(getMaxLastBuildTime(batchDataflow, streamingDataflow));
        this.setStorage(getTotalStorage(batchDataflow, streamingDataflow));
        this.setSource(getTotalSource(batchDataflow, streamingDataflow));
        this.setBatchSegmentHoles(calculateTotalSegHoles(batchDataflow));
        this.setSegmentHoles(calculateTotalSegHoles(streamingDataflow));
        this.setExpansionrate(ModelUtils.computeExpansionRate(this.getStorage(), this.getSource()));
        this.setUsage(getTotalUsage(streamingDataflow));
        this.setInconsistentSegmentCount(getTotalInconsistentSegmentCount(batchDataflow, streamingDataflow));
        if (!modelDesc.isBroken() && !batchModel.isBroken()) {
            this.setHasSegments(CollectionUtils.isNotEmpty(streamingDataflow.getSegments())
                    || CollectionUtils.isNotEmpty(batchDataflow.getSegments()));
            this.setBatchPartitionDesc(fusionModel.getBatchModel().getPartitionDesc());
            NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(),
                    this.getProject());
            IndexPlan batchIndex = indexPlanManager.getIndexPlan(batchId);
            IndexPlan streamingIndex = indexPlanManager.getIndexPlan(modelDesc.getUuid());
            this.setAvailableIndexesCount(getTotalAvailableIndexesCount(batchIndex, streamingIndex));
            this.setTotalIndexes(getIndexesCount(batchIndex));
            this.setStreamingIndexes(getIndexesCount(streamingIndex));
            this.setEmptyIndexesCount(this.getTotalIndexes() - this.getAvailableIndexesCount());
            this.setHasBaseAggIndex(streamingIndex.containBaseAggLayout());
            this.setHasBaseTableIndex(streamingIndex.containBaseTableLayout());
        }
    }

    private long getMaxLastBuildTime(NDataflow batchDataflow, NDataflow streamingDataflow) {
        return Math.max(batchDataflow.getLastBuildTime(), streamingDataflow.getLastBuildTime());
    }

    private long getTotalStorage(NDataflow batchDataflow, NDataflow streamingDataflow) {
        return batchDataflow.getStorageBytesSize() + streamingDataflow.getStorageBytesSize();
    }

    private long getTotalSource(NDataflow batchDataflow, NDataflow streamingDataflow) {
        return batchDataflow.getSourceBytesSize() + streamingDataflow.getSourceBytesSize();
    }

    private List<SegmentRange> calculateTotalSegHoles(NDataflow batchDataflow) {
        NDataflowManager dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), this.getProject());
        return dfManager.calculateSegHoles(batchDataflow.getUuid());
    }

    private long getTotalUsage(NDataflow streamingDataflow) {
        return streamingDataflow.getQueryHitCount();
    }

    private long getTotalInconsistentSegmentCount(NDataflow batchDataflow, NDataflow streamingDataflow) {
        return (long) batchDataflow.getSegments(SegmentStatusEnum.WARNING).size()
                + (long) streamingDataflow.getSegments(SegmentStatusEnum.WARNING).size();
    }

    private long getTotalAvailableIndexesCount(IndexPlan batchIndex, IndexPlan streamingIndex) {
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(),
                this.getProject());
        return indexPlanManager.getAvailableIndexesCount(getProject(), batchIndex.getId())
                + indexPlanManager.getAvailableIndexesCount(getProject(), streamingIndex.getId());
    }

    private long getIndexesCount(IndexPlan indexPlan) {
        return indexPlan.getAllLayouts().size();
    }
}

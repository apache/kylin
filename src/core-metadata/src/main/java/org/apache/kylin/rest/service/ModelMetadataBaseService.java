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

package org.apache.kylin.rest.service;

import static org.apache.kylin.common.exception.code.ErrorCodeServer.MODEL_ID_NOT_EXIST;

import java.util.Locale;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.NDataSegDetails;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.cube.model.PartitionStatusEnum;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.request.DataFlowUpdateRequest;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ModelMetadataBaseService {

    public String getModelNameById(String modelId, String project) {
        NDataModel nDataModel = getModelById(modelId, project);
        if (null != nDataModel) {
            return nDataModel.getAlias();
        }
        return null;
    }

    private NDataModel getModelById(String modelId, String project) {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        NDataModel nDataModel = modelManager.getDataModelDesc(modelId);
        if (null == nDataModel) {
            throw new KylinException(MODEL_ID_NOT_EXIST, modelId);
        }
        return nDataModel;
    }

    public void updateIndex(String project, long epochId, String modelId, Set<Long> toBeDeletedLayoutIds,
            boolean deleteAuto, boolean deleteManual) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project).updateIndexPlan(modelId,
                    copyForWrite -> copyForWrite.removeLayouts(toBeDeletedLayoutIds, deleteAuto, deleteManual));
            return null;
        }, epochId, project);
    }

    public void updateDataflow(DataFlowUpdateRequest dataFlowUpdateRequest) {
        String project = dataFlowUpdateRequest.getProject();
        NDataflowUpdate update = dataFlowUpdateRequest.getDataflowUpdate();

        // After serialization and deserialization， layout will lost dataSegDetails, try to restore it here.
        NDataSegDetails[] dataSegDetails = dataFlowUpdateRequest.getDataSegDetails();
        Integer[] layoutCounts = dataFlowUpdateRequest.getLayoutCounts();
        if (dataSegDetails != null && layoutCounts != null) {
            int layoutCount = 0;
            for (int i = 0; i < dataSegDetails.length; i++) {
                for (int j = layoutCount; j < layoutCount + layoutCounts[i]; j++) {
                    update.getToAddOrUpdateLayouts()[j].setSegDetails(dataSegDetails[i]);
                }
                layoutCount += layoutCounts[i];
            }
        }

        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NDataflowManager dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            dfManager.updateDataflow(update);

            // for canceling SUB_PARTITION_BUILD job
            if (null != dataFlowUpdateRequest.getToRemoveSegmentPartitions()) {
                Set<String> segments = dataFlowUpdateRequest.getToRemoveSegmentPartitions().getFirst();
                Set<Long> partitions = dataFlowUpdateRequest.getToRemoveSegmentPartitions().getSecond();
                // remove partition in layouts
                dfManager.removeLayoutPartition(update.getDataflowId(), partitions, segments);
                // remove partition in segments
                dfManager.removeSegmentPartition(update.getDataflowId(), partitions, segments);
                log.info(String.format(Locale.ROOT, "Remove partitions [%s] in segment [%s] cause to cancel job.",
                        partitions, segments));
            }
            // for canceling SUB_PARTITION_REFRESH job
            if (null != dataFlowUpdateRequest.getResetToReadyPartitions()) {
                Set<String> segments = dataFlowUpdateRequest.getToRemoveSegmentPartitions().getFirst();
                Set<Long> partitions = dataFlowUpdateRequest.getToRemoveSegmentPartitions().getSecond();
                NDataflow df = dfManager.getDataflow(update.getDataflowId());
                for (String id : segments) {
                    NDataSegment segment = df.getSegment(id).copy();
                    segment.getMultiPartitions().forEach(partition -> {
                        if (partitions.contains(partition.getPartitionId())
                                && PartitionStatusEnum.REFRESH == partition.getStatus()) {
                            partition.setStatus(PartitionStatusEnum.READY);
                        }
                    });
                    val dfUpdate = new NDataflowUpdate(df.getId());
                    dfUpdate.setToUpdateSegs(segment);
                    dfManager.updateDataflow(dfUpdate);
                    log.info(String.format(Locale.ROOT,
                            "Change partitions [%s] in segment [%s] status to READY cause to cancel job.", partitions,
                            id));
                }
            }

            return null;
        }, project);
    }

    public void updateDataflow(String project, String dfId, String segmentId, long maxBucketId) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project).updateDataflow(dfId,
                    copyForWrite -> copyForWrite.getSegment(segmentId).setMaxBucketId(maxBucketId));
            return null;
        }, project);
    }

    public void updateIndexPlan(String project, String uuid, IndexPlan indexplan, String action) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            if ("setLayoutBucketNumMapping".equals(action)) {
                NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project).updateIndexPlan(uuid,
                        copyForWrite -> copyForWrite.setLayoutBucketNumMapping(indexplan.getLayoutBucketNumMapping()));
            } else if ("removeTobeDeleteIndexIfNecessary".equals(action)) {
                NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project).updateIndexPlan(uuid,
                        IndexPlan::removeTobeDeleteIndexIfNecessary);
            } else {
                throw new IllegalStateException(String.format(Locale.ROOT, "action {%s} is nor illegal !!", action));
            }
            return null;
        }, project);
    }

    public void updateDataflowStatus(String project, String uuid, RealizationStatusEnum status) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project).updateDataflowStatus(uuid, status);
            return null;
        }, project);
    }
}

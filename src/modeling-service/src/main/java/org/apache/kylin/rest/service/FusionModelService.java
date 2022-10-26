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

import java.util.List;
import java.util.Locale;

import org.apache.commons.lang.ArrayUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.scheduler.EventBusFactory;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.FusionModel;
import org.apache.kylin.metadata.model.FusionModelManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.rest.aspect.Transaction;
import org.apache.kylin.rest.request.IndexesToSegmentsRequest;
import org.apache.kylin.rest.request.ModelRequest;
import org.apache.kylin.rest.request.OwnerChangeRequest;
import org.apache.kylin.rest.response.BuildBaseIndexResponse;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.JobInfoResponse;
import org.apache.kylin.rest.response.JobInfoResponseWithFailure;
import org.apache.kylin.rest.response.NDataModelResponse;
import org.apache.kylin.rest.service.params.IncrementBuildSegmentParams;
import org.apache.kylin.streaming.event.StreamingJobKillEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service("fusionModelService")
public class FusionModelService extends AbstractModelService implements TableFusionModelSupporter {

    @Autowired
    private ModelService modelService;

    @Autowired(required = false)
    @Qualifier("modelBuildService")
    private ModelBuildSupporter modelBuildService;

    public JobInfoResponse incrementBuildSegmentsManually(IncrementBuildSegmentParams params) throws Exception {
        val model = getManager(NDataModelManager.class, params.getProject()).getDataModelDesc(params.getModelId());
        if (model.isFusionModel()) {
            val streamingModel = getManager(NDataModelManager.class, params.getProject())
                    .getDataModelDesc(model.getFusionId());
            IncrementBuildSegmentParams copy = JsonUtil.deepCopyQuietly(params, IncrementBuildSegmentParams.class);
            String oldAliasName = streamingModel.getRootFactTableRef().getTableName();
            String tableName = model.getRootFactTableRef().getTableName();
            copy.getPartitionDesc().changeTableAlias(oldAliasName, tableName);
            return modelBuildService.incrementBuildSegmentsManually(copy);
        }
        return modelBuildService.incrementBuildSegmentsManually(params);
    }

    @Transaction(project = 1)
    public void dropModel(String modelId, String project) {
        val model = getManager(NDataModelManager.class, project).getDataModelDesc(modelId);
        if (model.fusionModelStreamingPart()) {
            val fusionModelManager = FusionModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            val fusionModel = fusionModelManager.getFusionModel(modelId);
            String batchId = fusionModel.getBatchModel().getUuid();
            fusionModelManager.dropModel(modelId);
            modelService.dropModel(batchId, project);
        }
        modelService.dropModel(modelId, project);
    }

    void innerDopModel(String modelId, String project) {
        val model = getManager(NDataModelManager.class, project).getDataModelDesc(modelId);
        if (model == null) {
            return;
        }

        if (model.isFusionModel()) {
            val fusionModelManager = FusionModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            val fusionModel = fusionModelManager.getFusionModel(modelId);
            if (model.fusionModelBatchPart()) {
                String streamingId = model.getFusionId();
                fusionModelManager.dropModel(streamingId);
                modelService.innerDropModel(streamingId, project);
            } else {
                String batchId = fusionModel.getBatchModel().getUuid();
                fusionModelManager.dropModel(modelId);
                modelService.innerDropModel(batchId, project);
            }
        }
        modelService.innerDropModel(modelId, project);
    }

    @Transaction(project = 0)
    public BuildBaseIndexResponse updateDataModelSemantic(String project, ModelRequest request) {
        val model = getManager(NDataModelManager.class, request.getProject()).getDataModelDesc(request.getUuid());
        if (model.isFusionModel()) {
            val fusionModelManager = FusionModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            String batchId = fusionModelManager.getFusionModel(request.getUuid()).getBatchModel().getUuid();
            ModelRequest copy = JsonUtil.deepCopyQuietly(request, ModelRequest.class);
            String tableName = model.getRootFactTableRef().getTableDesc().getKafkaConfig().getBatchTable();

            copy.setAlias(FusionModel.getBatchName(model.getAlias(), model.getUuid()));
            copy.setRootFactTableName(tableName);
            copy.setUuid(batchId);

            String tableAlias = model.getRootFactTableRef().getTableDesc().getKafkaConfig().getBatchTableAlias();
            String oldAliasName = model.getRootFactTableRef().getTableName();
            convertModel(copy, tableAlias, oldAliasName);
            modelService.updateDataModelSemantic(project, copy);
        }
        if (model.isStreaming()) {
            request.setWithBaseIndex(false);
        }
        return modelService.updateDataModelSemantic(project, request);
    }

    private void convertModel(ModelRequest copy, String tableName, String oldAliasName) {
        copy.getSimplifiedJoinTableDescs().stream()
                .forEach(x -> x.getSimplifiedJoinDesc().changeFKTableAlias(oldAliasName, tableName));
        copy.getSimplifiedDimensions().stream().forEach(x -> x.changeTableAlias(oldAliasName, tableName));
        copy.getSimplifiedMeasures().stream().forEach(x -> x.changeTableAlias(oldAliasName, tableName));
        copy.getPartitionDesc().changeTableAlias(oldAliasName, tableName);
    }

    @Transaction(project = 0)
    public void renameDataModel(String project, String modelId, String newAlias, String description) {
        val model = getManager(NDataModelManager.class, project).getDataModelDesc(modelId);
        if (model.isFusionModel()) {
            val fusionModelManager = FusionModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            String batchId = fusionModelManager.getFusionModel(modelId).getBatchModel().getUuid();
            modelService.renameDataModel(project, batchId, FusionModel.getBatchName(newAlias, modelId), description);
        }
        modelService.renameDataModel(project, modelId, newAlias, description);
        if (model.isStreaming() || model.isFusionModel()) {
            // Sync update of streaming job meta model name
            EventBusFactory.getInstance().postSync(new NDataModel.ModelRenameEvent(project, modelId, newAlias));
        }
    }

    @Transaction(project = 0)
    public void updateModelOwner(String project, String modelId, OwnerChangeRequest ownerChangeRequest) {
        val model = getManager(NDataModelManager.class, project).getDataModelDesc(modelId);
        if (model.isFusionModel()) {
            val fusionModelManager = FusionModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            String batchId = fusionModelManager.getFusionModel(modelId).getBatchModel().getUuid();
            OwnerChangeRequest batchRequest = JsonUtil.deepCopyQuietly(ownerChangeRequest, OwnerChangeRequest.class);
            modelService.updateModelOwner(project, batchId, batchRequest);
        }
        modelService.updateModelOwner(project, modelId, ownerChangeRequest);
    }

    public Pair<String, String[]> convertSegmentIdWithName(String modelId, String project, String[] segIds,
            String[] segNames) {
        if (ArrayUtils.isEmpty(segNames)) {
            return new Pair<>(modelId, segIds);
        }
        val dataModel = modelService.getModelById(modelId, project);
        String targetModelId = modelId;
        if (dataModel.isFusionModel()) {
            boolean existedInStreaming = modelService.checkSegmentsExistByName(targetModelId, project, segNames, false);
            if (!existedInStreaming) {
                targetModelId = getBatchModel(modelId, project).getUuid();
            }
        }
        String[] segmentIds = modelService.convertSegmentIdWithName(targetModelId, project, segIds, segNames);
        return new Pair<>(targetModelId, segmentIds);
    }

    public JobInfoResponseWithFailure addIndexesToSegments(String modelId,
            IndexesToSegmentsRequest buildSegmentsRequest) {
        String targetModelId = modelId;
        NDataModel dataModel = modelService.getModelById(modelId, buildSegmentsRequest.getProject());
        if (dataModel.getModelType() == NDataModel.ModelType.HYBRID) {
            boolean existedInStreaming = modelService.checkSegmentsExistById(targetModelId,
                    buildSegmentsRequest.getProject(), buildSegmentsRequest.getSegmentIds().toArray(new String[0]),
                    false);
            if (existedInStreaming) {
                throw new KylinException(ServerErrorCode.SEGMENT_UNSUPPORTED_OPERATOR,
                        String.format(Locale.ROOT, MsgPicker.getMsg().getFixStreamingSegment()));
            } else {
                targetModelId = getBatchModel(modelId, buildSegmentsRequest.getProject()).getUuid();
            }
        } else if (dataModel.getModelType() == NDataModel.ModelType.STREAMING) {
            throw new KylinException(ServerErrorCode.SEGMENT_UNSUPPORTED_OPERATOR,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getFixStreamingSegment()));
        }

        return modelBuildService.addIndexesToSegments(buildSegmentsRequest.getProject(), targetModelId,
                buildSegmentsRequest.getSegmentIds(), buildSegmentsRequest.getIndexIds(),
                buildSegmentsRequest.isParallelBuildBySegment(), buildSegmentsRequest.getPriority(),
                buildSegmentsRequest.isPartialBuild(), buildSegmentsRequest.getYarnQueue(),
                buildSegmentsRequest.getTag());
    }

    private NDataModel getBatchModel(String fusionModelId, String project) {
        val fusionModelManager = FusionModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val fusionModel = fusionModelManager.getFusionModel(fusionModelId);
        return fusionModel.getBatchModel();
    }

    public void stopStreamingJob(String modelId, String project) {
        val model = getManager(NDataModelManager.class, project).getDataModelDesc(modelId);
        if (model.fusionModelBatchPart()) {
            val streamingId = model.getFusionId();
            EventBusFactory.getInstance().postSync(new StreamingJobKillEvent(project, streamingId));
        }
        if (model.isStreaming()) {
            EventBusFactory.getInstance().postSync(new StreamingJobKillEvent(project, modelId));
        }
    }

    @Override
    public void onDropModel(String modelId, String project, boolean ignoreType) {
        innerDopModel(modelId, project);
    }

    @Override
    public void onStopStreamingJob(String modelId, String project) {
        stopStreamingJob(modelId, project);
    }

    public void setModelUpdateEnabled(DataResult<List<NDataModel>> dataResult) {
        val dataModelList = dataResult.getValue();
        dataModelList.stream().filter(model -> model.isStreaming()).forEach(model -> {
            if (model.isBroken()) {
                ((NDataModelResponse) model).setModelUpdateEnabled(false);
            } else {
                ((NDataModelResponse) model).setModelUpdateEnabled(
                        !FusionIndexService.checkStreamingJobAndSegments(model.getProject(), model.getUuid()));
            }
        });
    }

    public boolean modelExists(String modelAlias, String project) {
        return getManager(NDataModelManager.class, project).listAllModelAlias().contains(modelAlias);
    }
}

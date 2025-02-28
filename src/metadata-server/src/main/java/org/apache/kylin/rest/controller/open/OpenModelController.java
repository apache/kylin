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

package org.apache.kylin.rest.controller.open;

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_PARAMETER;
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_UPDATE_MODEL;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;
import static org.apache.kylin.common.exception.ServerErrorCode.MODEL_CONFIG_KEY_EXIST;
import static org.apache.kylin.common.exception.ServerErrorCode.MODEL_CONFIG_KEY_NOT_EXIST;
import static org.apache.kylin.common.exception.ServerErrorCode.MODEL_CONFIG_NOT_EXIST;
import static org.apache.kylin.common.exception.ServerErrorCode.UNSUPPORTED_STREAMING_OPERATION;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.INDEX_PARAMETER_INVALID;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.PROJECT_MULTI_PARTITION_DISABLE;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableSet;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.exception.LookupTableException;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.ModelAttributeEnum;
import org.apache.kylin.rest.controller.NBasicController;
import org.apache.kylin.rest.controller.NModelController;
import org.apache.kylin.rest.request.ModelCloneRequest;
import org.apache.kylin.rest.request.ModelConfigRequest;
import org.apache.kylin.rest.request.ModelParatitionDescRequest;
import org.apache.kylin.rest.request.ModelRequest;
import org.apache.kylin.rest.request.ModelUpdateRequest;
import org.apache.kylin.rest.request.MultiPartitionMappingRequest;
import org.apache.kylin.rest.request.OpenModelConfigRequest;
import org.apache.kylin.rest.request.OpenModelRequest;
import org.apache.kylin.rest.request.PartitionColumnRequest;
import org.apache.kylin.rest.request.UpdateMultiPartitionValueRequest;
import org.apache.kylin.rest.response.BuildBaseIndexResponse;
import org.apache.kylin.rest.response.ComputedColumnConflictResponse;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.IndexResponse;
import org.apache.kylin.rest.response.ModelConfigResponse;
import org.apache.kylin.rest.response.NModelDescResponse;
import org.apache.kylin.rest.response.OpenGetIndexResponse;
import org.apache.kylin.rest.response.OpenGetIndexResponse.IndexDetail;
import org.apache.kylin.rest.response.SynchronizedCommentsResponse;
import org.apache.kylin.rest.service.FusionIndexService;
import org.apache.kylin.rest.service.FusionModelService;
import org.apache.kylin.rest.service.ModelService;
import org.apache.kylin.rest.service.ModelTdsService;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.apache.kylin.tool.bisync.SyncContext;
import org.apache.kylin.tool.bisync.model.SyncModel;
import org.apache.kylin.util.DataRangeUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import io.swagger.annotations.ApiOperation;
import lombok.val;

@Controller
@RequestMapping(value = "/api/models", produces = { HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class OpenModelController extends NBasicController {

    private static final String LAST_MODIFY = "last_modified";
    private static final String USAGE = "usage";
    private static final String DATA_SIZE = "data_size";
    private static final String ALIAS = "alias";
    private static final Set<String> INDEX_SORT_BY_SET = ImmutableSet.of(USAGE, LAST_MODIFY, DATA_SIZE);
    private static final Set<String> INDEX_SOURCE_SET = Arrays.stream(IndexEntity.Source.values()).map(Enum::name)
            .collect(Collectors.toSet());
    private static final Set<String> INDEX_STATUS_SET = Arrays.stream(IndexEntity.Status.values()).map(Enum::name)
            .collect(Collectors.toSet());
    public static final String MODEL_ID = "modelId";

    public static final String FACT_TABLE = "fact_table";

    private static final String MODEL_NAME = "model_name";

    @Autowired
    private NModelController modelController;

    @Autowired
    @Qualifier("modelTdsService")
    private ModelTdsService tdsService;

    @Autowired
    private FusionIndexService fusionIndexService;

    @Autowired
    private FusionModelService fusionModelService;

    @Autowired
    private ModelService modelService;

    @ApiOperation(value = "createModel", tags = { "AI" })
    @PostMapping
    @ResponseBody
    public EnvelopeResponse<BuildBaseIndexResponse> createModel(@RequestBody ModelRequest modelRequest) {
        modelRequest.setProject(checkProjectName(modelRequest.getProject()));
        checkRequiredArg(ALIAS, modelRequest.getRawAlias());
        modelService.checkCCEmpty(modelRequest);
        modelRequest.toUpperCaseModelRequest();
        Pair<ModelRequest, ComputedColumnConflictResponse> pair = modelService.checkCCConflict(modelRequest);
        EnvelopeResponse<BuildBaseIndexResponse> response = modelController.createModel(pair.getFirst());
        response.getData().setCcConflict(pair.getSecond());
        return response;
    }

    @ApiOperation(value = "getModels", tags = { "AI" })
    @GetMapping(value = "")
    @ResponseBody
    public EnvelopeResponse<DataResult<List<NDataModel>>> getModels(@RequestParam(value = "project") String project,
            @RequestParam(value = "model_id", required = false) String modelId, //
            @RequestParam(value = "model_name", required = false) String modelAlias, //
            @RequestParam(value = "exact", required = false, defaultValue = "true") boolean exactMatch,
            @RequestParam(value = "owner", required = false) String owner, //
            @RequestParam(value = "status", required = false) List<String> status, //
            @RequestParam(value = "table", required = false) String table, //
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer limit,
            @RequestParam(value = "sort_by", required = false, defaultValue = "last_modify") String sortBy,
            @RequestParam(value = "reverse", required = false, defaultValue = "true") Boolean reverse,
            @RequestParam(value = "model_alias_or_owner", required = false) String modelAliasOrOwner,
            @RequestParam(value = "last_modify_from", required = false) Long lastModifyFrom,
            @RequestParam(value = "last_modify_to", required = false) Long lastModifyTo,
            @RequestParam(value = "only_normal_dim", required = false, defaultValue = "true") boolean onlyNormalDim,
            @RequestParam(value = "lite", required = false, defaultValue = "false") boolean lite) {
        String projectName = checkProjectName(project);
        return modelController.getModels(modelId, modelAlias, exactMatch, projectName, owner, status, table, offset,
                limit, sortBy, reverse, modelAliasOrOwner, Collections.singletonList(ModelAttributeEnum.BATCH),
                lastModifyFrom, lastModifyTo, onlyNormalDim, lite);
    }

    @ApiOperation(value = "getIndexes", tags = { "AI" })
    @GetMapping(value = "/{model_name:.+}/indexes")
    @ResponseBody
    public EnvelopeResponse<OpenGetIndexResponse> getIndexes(@RequestParam(value = "project") String project,
            @PathVariable(value = "model_name") String modelAlias,
            @RequestParam(value = "status", required = false) List<String> status,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer limit,
            @RequestParam(value = "sources", required = false) List<String> sources,
            @RequestParam(value = "sort_by", required = false, defaultValue = "last_modified") String sortBy,
            @RequestParam(value = "key", required = false, defaultValue = "") String key,
            @RequestParam(value = "reverse", required = false, defaultValue = "true") Boolean reverse,
            @RequestParam(value = "batch_index_ids", required = false) List<Long> batchIndexIds) {
        String projectName = checkProjectName(project);
        NDataModel model = modelService.getModel(modelAlias, projectName);
        checkNonNegativeIntegerArg("page_offset", offset);
        checkNonNegativeIntegerArg("page_size", limit);
        List<IndexEntity.Status> statuses = checkIndexStatus(status);
        String modifiedSortBy = checkIndexSortBy(sortBy);
        List<IndexEntity.Source> modifiedSources = checkSources(sources);
        List<IndexResponse> indexes = fusionIndexService.getIndexesWithRelatedTables(projectName, model.getUuid(), key,
                statuses, modifiedSortBy, reverse, modifiedSources, batchIndexIds);
        List<IndexResponse> listDataResult = DataResult.get(indexes, offset, limit).getValue();

        OpenGetIndexResponse response = new OpenGetIndexResponse();
        response.setModelId(model.getUuid());
        response.setModelAlias(model.getAlias());
        response.setProject(projectName);
        response.setOwner(model.getOwner());
        response.setLimit(limit);
        response.setOffset(offset);
        response.setTotalSize(indexes.size());
        List<IndexDetail> detailList = Lists.newArrayList();
        listDataResult.forEach(indexResponse -> detailList.add(IndexDetail.newIndexDetail(indexResponse)));
        response.setIndexDetailList(detailList);
        if (CollectionUtils.isNotEmpty(batchIndexIds)) {
            Set<Long> batchIndexIdsSet = indexes.stream() //
                    .filter(index -> index.getIndexRange() == null || index.getIndexRange() == IndexEntity.Range.BATCH)
                    .map(IndexResponse::getId).collect(Collectors.toSet());

            List<Long> absentBatchIndexIds = batchIndexIds.stream() //
                    .filter(id -> !batchIndexIdsSet.contains(id)) //
                    .collect(Collectors.toList());
            response.setAbsentBatchIndexIds(absentBatchIndexIds);
        }
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    static List<IndexEntity.Status> checkIndexStatus(List<String> statusList) {
        if (statusList == null || statusList.isEmpty()) {
            return Lists.newArrayList();
        }
        List<IndexEntity.Status> statuses = Lists.newArrayList();
        statusList.forEach(status -> {
            if (status != null) {
                String s = status.toUpperCase(Locale.ROOT);
                if (INDEX_STATUS_SET.contains(s)) {
                    statuses.add(IndexEntity.Status.valueOf(s));
                } else {
                    throw new KylinException(INDEX_PARAMETER_INVALID, "status", String.join(", ", INDEX_STATUS_SET));
                }
            }
        });
        return statuses;
    }

    static List<IndexEntity.Source> checkSources(List<String> sources) {
        if (sources == null || sources.isEmpty()) {
            return Lists.newArrayList();
        }
        List<IndexEntity.Source> sourceList = Lists.newArrayList();
        sources.forEach(source -> {
            if (source != null) {
                String s = source.toUpperCase(Locale.ROOT);
                if (INDEX_SOURCE_SET.contains(s)) {
                    sourceList.add(IndexEntity.Source.valueOf(s));
                } else {
                    throw new KylinException(INDEX_PARAMETER_INVALID, "sources", String.join(", ", INDEX_SOURCE_SET));
                }
            }
        });
        return sourceList;
    }

    static String checkIndexSortBy(String sortBy) {
        if (sortBy == null) {
            return LAST_MODIFY;
        }
        sortBy = sortBy.toLowerCase(Locale.ROOT).trim();
        if (sortBy.length() == 0) {
            return LAST_MODIFY;
        }
        if (INDEX_SORT_BY_SET.contains(sortBy)) {
            return sortBy;
        }
        throw new KylinException(INDEX_PARAMETER_INVALID, "sort_by", String.join(", ", INDEX_SORT_BY_SET));
    }

    @ApiOperation(value = "getModelDesc", tags = { "AI" })
    @GetMapping(value = "/{project}/{model}/model_desc")
    @ResponseBody
    public EnvelopeResponse<NModelDescResponse> getModelDesc(@PathVariable("project") String project,
            @PathVariable("model") String modelAlias) {
        String projectName = checkProjectName(project);
        val dataModel = modelService.getModel(modelAlias, projectName);
        if (dataModel.isStreaming()) {
            throw new KylinException(UNSUPPORTED_STREAMING_OPERATION,
                    MsgPicker.getMsg().getStreamingOperationNotSupport());
        }
        NModelDescResponse result = modelService.getModelDesc(dataModel.getAlias(), projectName);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, result, "");
    }

    @ApiOperation(value = "update partition for single-partition model and forward compatible", tags = { "DW" })
    @PutMapping(value = "/{project}/{model}/partition_desc")
    @ResponseBody
    public EnvelopeResponse<String> updatePartitionDesc(@PathVariable("project") String project,
            @PathVariable("model") String modelAlias,
            @RequestBody ModelParatitionDescRequest modelParatitionDescRequest) {
        String projectName = checkProjectName(project);
        String partitionDateFormat = null;
        if (modelParatitionDescRequest.getPartitionDesc() != null) {
            checkRequiredArg("partition_date_column",
                    modelParatitionDescRequest.getPartitionDesc().getPartitionDateColumn());
            checkRequiredArg("partition_date_format",
                    modelParatitionDescRequest.getPartitionDesc().getPartitionDateFormat());
            partitionDateFormat = modelParatitionDescRequest.getPartitionDesc().getPartitionDateFormat();
        }
        DataRangeUtils.validateDataRange(modelParatitionDescRequest.getStart(), modelParatitionDescRequest.getEnd(),
                partitionDateFormat);
        val dataModel = modelService.getModel(modelAlias, projectName);
        modelService.updateModelPartitionColumn(projectName, dataModel.getAlias(), modelParatitionDescRequest);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "deleteModel", tags = { "AI" })
    @DeleteMapping(value = "/{model_name:.+}")
    @ResponseBody
    public EnvelopeResponse<String> deleteModel(@PathVariable("model_name") String modelAlias,
            @RequestParam("project") String project) {
        String projectName = checkProjectName(project);
        String modelId = modelService.getModelWithoutBrokenCheck(modelAlias, projectName).getId();
        return modelController.deleteModel(modelId, projectName);
    }

    @ApiOperation(value = "updateMultiPartitionMapping", tags = { "QE" })
    @PutMapping(value = "/{model_name:.+}/multi_partition/mapping")
    @ResponseBody
    public EnvelopeResponse<String> updateMultiPartitionMapping(@PathVariable("model_name") String modelAlias,
            @RequestBody MultiPartitionMappingRequest mappingRequest) {
        checkValidityOfMultiPartitionMappingRequest(mappingRequest);
        String projectName = checkProjectName(mappingRequest.getProject());
        checkProjectMLP(projectName);
        mappingRequest.setProject(projectName);
        val modelId = modelService.getModel(modelAlias, mappingRequest.getProject()).getId();
        return modelController.updateMultiPartitionMapping(modelId, mappingRequest);
    }

    @ApiOperation(value = "addMultiPartitionValues", notes = "Add URL: {model}", tags = { "DW" })
    @PostMapping(value = "/{model_name:.+}/segments/multi_partition/sub_partition_values")
    @ResponseBody
    public EnvelopeResponse<String> addMultiPartitionValues(@PathVariable("model_name") String modelAlias,
            @RequestBody UpdateMultiPartitionValueRequest request) {
        String projectName = checkProjectName(request.getProject());
        checkProjectMLP(projectName);
        val modelId = modelService.getModel(modelAlias, projectName).getId();
        return modelController.addMultiPartitionValues(modelId, request);
    }

    @ApiOperation(value = "update partition for multi partition and single partition", tags = { "DW" })
    @PutMapping(value = "/{model_name:.+}/partition")
    @ResponseBody
    public EnvelopeResponse<String> updatePartitionSemantic(@PathVariable("model_name") String modelAlias,
            @RequestBody PartitionColumnRequest param) throws Exception {
        String projectName = checkProjectName(param.getProject());
        if (param.getMultiPartitionDesc() != null) {
            checkProjectMLP(projectName);
        }
        param.setProject(projectName);
        val modelId = modelService.getModel(modelAlias, param.getProject()).getId();
        return modelController.updatePartitionSemantic(modelId, param);
    }

    @ApiOperation(value = "export model", tags = { "QE" }, notes = "Add URL: {model}")
    @GetMapping(value = "/{model_name:.+}/export")
    @ResponseBody
    public void exportModel(@PathVariable("model_name") String modelAlias,
            @RequestParam(value = "project") String project, @RequestParam(value = "export_as") SyncContext.BI exportAs,
            @RequestParam(value = "element", required = false, defaultValue = "AGG_INDEX_COL") SyncContext.ModelElement element,
            @RequestParam(value = "server_host", required = false) String serverHost,
            @RequestParam(value = "server_port", required = false) Integer serverPort, HttpServletRequest request,
            HttpServletResponse response) throws IOException {
        String projectName = checkProjectName(project);
        String modelId = modelService.getModel(modelAlias, projectName).getId();
        String host = getHost(serverHost, request.getServerName());
        int port = getPort(serverPort, request.getServerPort());

        SyncContext syncContext = tdsService.prepareSyncContext(projectName, modelId, exportAs, element, host, port);
        SyncModel syncModel = tdsService.exportModel(syncContext);
        tdsService.preCheckNameConflict(syncModel);
        tdsService.dumpSyncModel(syncContext, syncModel, response);
    }

    @ApiOperation(value = "bi export", tags = { "QE" })
    @GetMapping(value = "/bi_export")
    @ResponseBody
    public void biExport(@RequestParam("model_name") String modelAlias, @RequestParam(value = "project") String project,
            @RequestParam(value = "export_as") SyncContext.BI exportAs,
            @RequestParam(value = "element", required = false, defaultValue = "AGG_INDEX_COL") SyncContext.ModelElement element,
            @RequestParam(value = "server_host", required = false) String serverHost,
            @RequestParam(value = "server_port", required = false) Integer serverPort,
            @RequestParam(value = "dimensions", required = false) List<String> dimensions,
            @RequestParam(value = "measures", required = false) List<String> measures, HttpServletRequest request,
            HttpServletResponse response) throws IOException {
        String projectName = checkProjectName(project);
        String modelId = modelService.getModel(modelAlias, projectName).getId();
        String host = getHost(serverHost, request.getServerName());
        int port = getPort(serverPort, request.getServerPort());
        if (dimensions == null) {
            // no need filter of given dimensions
            dimensions = ImmutableList.of();
        }
        if (measures == null) {
            // no need filter of given measures
            measures = ImmutableList.of();
        }

        SyncContext syncContext = tdsService.prepareSyncContext(projectName, modelId, exportAs, element, host, port);
        SyncModel syncModel = AclPermissionUtil.isAdmin()
                ? tdsService.exportTDSDimensionsAndMeasuresByAdmin(syncContext, dimensions, measures)
                : tdsService.exportTDSDimensionsAndMeasuresByNormalUser(syncContext, dimensions, measures);
        tdsService.preCheckNameConflict(syncModel);
        tdsService.dumpSyncModel(syncContext, syncModel, response);
    }

    @ApiOperation(value = "updateModelName", tags = { "AI" })
    @PutMapping(value = "/{model_name}/name")
    @ResponseBody
    public EnvelopeResponse<String> updateModelName(@PathVariable("model_name") String modelAlias,
            @RequestBody ModelUpdateRequest modelRenameRequest) {
        checkRequiredArg("new_model_name", modelRenameRequest.getNewModelName());
        String projectName = checkProjectName(modelRenameRequest.getProject());
        String modelId = modelService.getModel(modelAlias, projectName).getId();
        checkRequiredArg(NModelController.MODEL_ID, modelId);
        return modelController.updateModelName(modelId, modelRenameRequest);
    }

    @ApiOperation(value = "updateModelStatus", tags = { "AI" })
    @PutMapping(value = "/{model_name}/status")
    @ResponseBody
    public EnvelopeResponse<String> updateModelStatus(@PathVariable("model_name") String modelAlias,
            @RequestBody ModelUpdateRequest modelRenameRequest) {
        String projectName = checkProjectName(modelRenameRequest.getProject());
        String modelId = modelService.getModel(modelAlias, projectName).getId();
        return modelController.updateModelStatus(modelId, modelRenameRequest);
    }

    private void checkValidityOfMultiPartitionMappingRequest(MultiPartitionMappingRequest mappingRequest) {
        checkListRequiredArg("alias_columns", mappingRequest.getAliasCols());
        checkListRequiredArg("multi_partition_columns", mappingRequest.getPartitionCols());
        checkListRequiredArg("value_mapping", mappingRequest.getValueMapping());
        for (MultiPartitionMappingRequest.MappingRequest<List<String>, List<String>> value_mapping : mappingRequest
                .getValueMapping()) {
            checkListRequiredArg("origin", value_mapping.getOrigin());
            checkListRequiredArg("target", value_mapping.getTarget());
        }
    }

    private void checkProjectMLP(String project) {
        ProjectInstance projectInstance = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getProject(project);
        if (!projectInstance.getConfig().isMultiPartitionEnabled()) {
            throw new KylinException(PROJECT_MULTI_PARTITION_DISABLE, projectInstance.getName());
        }
    }

    static void checkMLP(String fieldName, List<String[]> subPartitionValues) {
        if (subPartitionValues.isEmpty()) {
            throw new KylinException(INVALID_PARAMETER, String.format(Locale.ROOT, "'%s' cannot be empty.", fieldName));
        }
    }

    @ApiOperation(value = "updateModelSemantic", tags = { "AI" })
    @PutMapping(value = "/modification")
    @ResponseBody
    public EnvelopeResponse<BuildBaseIndexResponse> updateSemantic(@RequestBody OpenModelRequest request) {
        String projectName = checkProjectName(request.getProject());
        request.setProject(projectName);
        NDataModel model = modelService.getModel(request.getModelName(), request.getProject());
        request.setUuid(model.getId());
        request.setOwner(model.getOwner());
        request.setManagementType(model.getManagementType());
        request.setCanvas(model.getCanvas());
        String partitionColumnFormat = modelService.getPartitionColumnFormatById(request.getProject(), request.getId());
        DataRangeUtils.validateDataRange(request.getStart(), request.getEnd(), partitionColumnFormat);
        modelService.validatePartitionDesc(request.getPartitionDesc());
        checkRequiredArg(MODEL_ID, request.getUuid());
        try {
            BuildBaseIndexResponse response = BuildBaseIndexResponse.EMPTY;
            if (request.getBrokenReason() == NDataModel.BrokenReason.SCHEMA) {
                modelService.repairBrokenModel(request.getProject(), request);
            } else {
                response = fusionModelService.updateDataModelSemantic(request.getProject(), request);
            }
            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
        } catch (LookupTableException e) {
            throw new KylinException(FAILED_UPDATE_MODEL, e);
        } catch (Exception e) {
            Throwable root = ExceptionUtils.getRootCause(e) == null ? e : ExceptionUtils.getRootCause(e);
            throw new KylinException(FAILED_UPDATE_MODEL, root);
        }
    }

    @ApiOperation(value = "comments synchronization", tags = { "AI" })
    @PostMapping(value = "/comments_synchronization")
    @ResponseBody
    public EnvelopeResponse<SynchronizedCommentsResponse> commentsSynchronization(
            @RequestBody ModelRequest modelRequest) {
        modelRequest.setProject(checkProjectName(modelRequest.getProject()));
        checkRequiredArg(ALIAS, modelRequest.getRawAlias());
        checkRequiredArg(FACT_TABLE, modelRequest.getRootFactTableName());
        modelRequest.toUpperCaseModelRequest();
        SynchronizedCommentsResponse synchronizedCommentsResponse = new SynchronizedCommentsResponse();
        synchronizedCommentsResponse.syncComment(modelRequest);
        modelService.checkBeforeModelSave(synchronizedCommentsResponse.getModelRequest());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, synchronizedCommentsResponse, "");
    }

    @ApiOperation(value = "cloneModel", tags = { "AI" })
    @PostMapping(value = "/{model_name:.+}/clone")
    @ResponseBody
    public EnvelopeResponse<String> cloneModel(@PathVariable("model_name") String modelAlias,
            @RequestBody ModelCloneRequest request) {
        String projectName = checkProjectName(request.getProject());
        checkRequiredArg(MODEL_NAME, modelAlias);
        String modelId = modelService.getModel(modelAlias, projectName).getId();
        return modelController.cloneModel(modelId, request);
    }

    @ApiOperation(value = "getModelConfig", tags = { "AI" }, notes = "AI")
    @GetMapping(value = "/config")
    @ResponseBody
    public EnvelopeResponse<ModelConfigResponse> getModelConfig(@RequestParam(value = "model") String modelAlias,
            @RequestParam(value = "project") String project) {
        String projectName = checkProjectName(project);
        ModelConfigResponse modelConfig = checkModelConfig(modelAlias, projectName);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, modelConfig, "");
    }

    @ApiOperation(value = "add ModelConfig", tags = { "AI" })
    @PostMapping(value = "/config")
    @ResponseBody
    public EnvelopeResponse<ModelConfigResponse> addModelConfig(@RequestBody OpenModelConfigRequest request) {
        String projectName = checkProjectName(request.getProject());
        ModelConfigResponse modelConfig = checkModelConfig(request.getModel(), projectName);

        checkConfigWhetherExist(modelConfig.getOverrideProps(), request.getCustomSettings().keySet(), true);
        modelConfig.getOverrideProps().putAll(request.getCustomSettings());

        ModelConfigRequest modelConfigRequest = newModelConfigRequestByConfig(modelConfig, projectName);
        modelService.updateModelConfig(projectName, modelConfig.getModel(), modelConfigRequest);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, modelConfig, "");
    }

    @ApiOperation(value = "update ModelConfig", tags = { "AI" })
    @PutMapping(value = "/config")
    @ResponseBody
    public EnvelopeResponse<ModelConfigResponse> updateModelConfig(@RequestBody OpenModelConfigRequest request) {
        String projectName = checkProjectName(request.getProject());
        ModelConfigResponse modelConfig = checkModelConfig(request.getModel(), projectName);

        checkConfigWhetherExist(modelConfig.getOverrideProps(), request.getCustomSettings().keySet(), false);
        modelConfig.getOverrideProps().putAll(request.getCustomSettings());

        ModelConfigRequest modelConfigRequest = newModelConfigRequestByConfig(modelConfig, projectName);
        modelService.updateModelConfig(projectName, modelConfig.getModel(), modelConfigRequest);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, modelConfig, "");
    }

    @ApiOperation(value = "delete ModelConfig", tags = { "AI" })
    @DeleteMapping(value = "/config")
    @ResponseBody
    public EnvelopeResponse<ModelConfigResponse> deleteModelConfig(@RequestBody OpenModelConfigRequest request) {
        String projectName = checkProjectName(request.getProject());
        ModelConfigResponse modelConfig = checkModelConfig(request.getModel(), projectName);

        checkConfigWhetherExist(modelConfig.getOverrideProps(), request.getDeleteCustomSettings(), false);
        request.getDeleteCustomSettings().forEach(key -> modelConfig.getOverrideProps().remove(key));

        ModelConfigRequest modelConfigRequest = newModelConfigRequestByConfig(modelConfig, projectName);
        modelService.updateModelConfig(projectName, modelConfig.getModel(), modelConfigRequest);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, modelConfig, "");
    }

    /**
     * @param exist {true} when overrideProps exist throw first param exist,{false} when overrideProps not exist throw first param not exist
     * @return
     */
    private void checkConfigWhetherExist(LinkedHashMap<String, String> overrideProps,
            Collection<String> customSettingKeys, boolean exist) {
        String key = null;
        for (String customSettingKey : customSettingKeys) {
            if (exist == overrideProps.containsKey(customSettingKey)) {
                key = customSettingKey;
                break;
            }
        }

        if (key != null) {
            if (exist) {
                throw new KylinException(MODEL_CONFIG_KEY_EXIST,
                        String.format(Locale.ROOT, MsgPicker.getMsg().getModelConfigKeyExist(), key));
            } else {
                throw new KylinException(MODEL_CONFIG_KEY_NOT_EXIST,
                        String.format(Locale.ROOT, MsgPicker.getMsg().getModelConfigKeyNotExist(), key));
            }
        }
        if (customSettingKeys.isEmpty()) {
            throw new KylinException(EMPTY_PARAMETER,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getConfigMapEmpty()));
        }
    }

    public ModelConfigResponse checkModelConfig(String modelAlias, String projectName) {
        NDataModel model = modelService.getModel(modelAlias, projectName);
        String modelName = model.getAlias();
        ModelConfigResponse modelConfig = modelService.getModelConfig(projectName, modelName).stream()
                .filter(config -> modelName.equalsIgnoreCase(config.getAlias())).findFirst().orElse(null);
        if (modelConfig == null) {
            throw new KylinException(MODEL_CONFIG_NOT_EXIST, String.format(Locale.ROOT,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getModelConfigExist(), modelName)));
        }
        return modelConfig;
    }

    private ModelConfigRequest newModelConfigRequestByConfig(ModelConfigResponse modelConfig, String project) {
        ModelConfigRequest modelConfigRequest = new ModelConfigRequest();
        modelConfigRequest.setProject(project);
        modelConfigRequest.setAutoMergeEnabled(modelConfig.getAutoMergeEnabled());
        modelConfigRequest.setRetentionRange(modelConfig.getRetentionRange());
        modelConfigRequest.setVolatileRange(modelConfig.getVolatileRange());
        modelConfigRequest.setAutoMergeTimeRanges(modelConfig.getAutoMergeTimeRanges());
        modelConfigRequest.setOverrideProps(modelConfig.getOverrideProps());
        return modelConfigRequest;
    }

}

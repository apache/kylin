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

package io.kyligence.kap.rest.controller.open;

import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;
import static org.apache.kylin.common.exception.ServerErrorCode.UNSUPPORTED_STREAMING_OPERATION;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.INDEX_PARAMETER_INVALID;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.MODEL_NAME_NOT_EXIST;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.PROJECT_MULTI_PARTITION_DISABLE;
import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.rest.constant.ModelAttributeEnum;
import org.apache.kylin.rest.controller.NBasicController;
import org.apache.kylin.rest.controller.NModelController;
import org.apache.kylin.rest.request.ModelParatitionDescRequest;
import org.apache.kylin.rest.request.ModelRequest;
import org.apache.kylin.rest.request.ModelUpdateRequest;
import org.apache.kylin.rest.request.MultiPartitionMappingRequest;
import org.apache.kylin.rest.request.PartitionColumnRequest;
import org.apache.kylin.rest.request.UpdateMultiPartitionValueRequest;
import org.apache.kylin.rest.response.BuildBaseIndexResponse;
import org.apache.kylin.rest.response.IndexResponse;
import org.apache.kylin.rest.response.NModelDescResponse;
import org.apache.kylin.rest.response.OpenGetIndexResponse;
import org.apache.kylin.rest.response.OpenGetIndexResponse.IndexDetail;
import org.apache.kylin.rest.service.FusionIndexService;
import org.apache.kylin.rest.service.ModelService;
import org.apache.kylin.tool.bisync.SyncContext;
import org.springframework.beans.factory.annotation.Autowired;
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

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

    @Autowired
    private NModelController modelController;

    @Autowired
    private FusionIndexService fusionIndexService;

    @Autowired
    private ModelService modelService;

    @ApiOperation(value = "createModel", tags = { "AI" })
    @PostMapping
    @ResponseBody
    public EnvelopeResponse<BuildBaseIndexResponse> createModel(@RequestBody ModelRequest modelRequest) {
        modelRequest.setProject(checkProjectName(modelRequest.getProject()));
        checkRequiredArg(ALIAS, modelRequest.getRawAlias());
        return modelController.createModel(modelRequest);
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
            @RequestParam(value = "only_normal_dim", required = false, defaultValue = "true") boolean onlyNormalDim) {
        String projectName = checkProjectName(project);
        return modelController.getModels(modelId, modelAlias, exactMatch, projectName, owner, status, table, offset,
                limit, sortBy, reverse, modelAliasOrOwner, Arrays.asList(ModelAttributeEnum.BATCH), lastModifyFrom,
                lastModifyTo, onlyNormalDim);
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
        NDataModel model = getModel(modelAlias, projectName);
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
                    .filter(index -> index.getIndexRange() == null || index.getIndexRange() == IndexEntity.Range.BATCH) //
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

    @VisibleForTesting
    public NDataModel getModel(String modelAlias, String project) {
        NDataModel model = modelService.getManager(NDataModelManager.class, project).listAllModels().stream() //
                .filter(dataModel -> dataModel.getUuid().equals(modelAlias) //
                        || dataModel.getAlias().equalsIgnoreCase(modelAlias))
                .findFirst().orElse(null);

        if (model == null) {
            throw new KylinException(MODEL_NAME_NOT_EXIST, modelAlias);
        }
        if (model.isBroken()) {
            throw new KylinException(ServerErrorCode.MODEL_BROKEN,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getBrokenModelOperationDenied(), modelAlias));
        }
        return model;
    }

    @ApiOperation(value = "getModelDesc", tags = { "AI" })
    @GetMapping(value = "/{project}/{model}/model_desc")
    @ResponseBody
    public EnvelopeResponse<NModelDescResponse> getModelDesc(@PathVariable("project") String project,
            @PathVariable("model") String modelAlias) {
        String projectName = checkProjectName(project);
        val dataModel = getModel(modelAlias, projectName);
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
        validateDataRange(modelParatitionDescRequest.getStart(), modelParatitionDescRequest.getEnd(),
                partitionDateFormat);
        val dataModel = getModel(modelAlias, projectName);
        modelService.updateModelPartitionColumn(projectName, dataModel.getAlias(), modelParatitionDescRequest);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "deleteModel", tags = { "AI" })
    @DeleteMapping(value = "/{model_name:.+}")
    @ResponseBody
    public EnvelopeResponse<String> deleteModel(@PathVariable("model_name") String modelAlias,
            @RequestParam("project") String project) {
        String projectName = checkProjectName(project);
        String modelId = getModel(modelAlias, projectName).getId();
        return modelController.deleteModel(modelId, projectName);
    }

    @ApiOperation(value = "updateMultiPartitionMapping", tags = { "QE" })
    @PutMapping(value = "/{model_name:.+}/multi_partition/mapping")
    @ResponseBody
    public EnvelopeResponse<String> updateMultiPartitionMapping(@PathVariable("model_name") String modelAlias,
            @RequestBody MultiPartitionMappingRequest mappingRequest) {
        String projectName = checkProjectName(mappingRequest.getProject());
        checkProjectMLP(projectName);
        mappingRequest.setProject(projectName);
        val modelId = getModel(modelAlias, mappingRequest.getProject()).getId();
        return modelController.updateMultiPartitionMapping(modelId, mappingRequest);
    }

    @ApiOperation(value = "addMultiPartitionValues", notes = "Add URL: {model}", tags = { "DW" })
    @PostMapping(value = "/{model_name:.+}/segments/multi_partition/sub_partition_values")
    @ResponseBody
    public EnvelopeResponse<String> addMultiPartitionValues(@PathVariable("model_name") String modelAlias,
            @RequestBody UpdateMultiPartitionValueRequest request) {
        String projectName = checkProjectName(request.getProject());
        checkProjectMLP(projectName);
        val modelId = getModel(modelAlias, projectName).getId();
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
        val modelId = getModel(modelAlias, param.getProject()).getId();
        return modelController.updatePartitionSemantic(modelId, param);
    }

    @ApiOperation(value = "export model", tags = { "QE" }, notes = "Add URL: {model}")
    @GetMapping(value = "/{model_name:.+}/export")
    @ResponseBody
    public void exportModel(@PathVariable("model_name") String modelAlias,
            @RequestParam(value = "project") String project, @RequestParam(value = "export_as") SyncContext.BI exportAs,
            @RequestParam(value = "element", required = false, defaultValue = "AGG_INDEX_COL") SyncContext.ModelElement element,
            @RequestParam(value = "server_host", required = false) String host,
            @RequestParam(value = "server_port", required = false) Integer port, HttpServletRequest request,
            HttpServletResponse response) throws IOException {
        String projectName = checkProjectName(project);
        String modelId = getModel(modelAlias, projectName).getId();
        modelController.exportModel(modelId, projectName, exportAs, element, host, port, request, response);
    }

    @ApiOperation(value = "bi export", tags = { "QE" })
    @GetMapping(value = "/bi_export")
    @ResponseBody
    public void biExport(@RequestParam("model_name") String modelAlias, @RequestParam(value = "project") String project,
            @RequestParam(value = "export_as") SyncContext.BI exportAs,
            @RequestParam(value = "element", required = false, defaultValue = "AGG_INDEX_COL") SyncContext.ModelElement element,
            @RequestParam(value = "server_host", required = false) String host,
            @RequestParam(value = "server_port", required = false) Integer port,
            @RequestParam(value = "dimensions", required = false) List<String> dimensions,
            @RequestParam(value = "measures", required = false) List<String> measures, HttpServletRequest request,
            HttpServletResponse response) throws IOException {
        String projectName = checkProjectName(project);
        String modelId = getModel(modelAlias, projectName).getId();
        modelController.biExport(modelId, projectName, exportAs, element, host, port, dimensions, measures, request,
                response);
    }

    @ApiOperation(value = "updateModelName", tags = { "AI" })
    @PutMapping(value = "/{model_name}/name")
    @ResponseBody
    public EnvelopeResponse<String> updateModelName(@PathVariable("model_name") String modelAlias,
            @RequestBody ModelUpdateRequest modelRenameRequest) {
        String projectName = checkProjectName(modelRenameRequest.getProject());
        String modelId = getModel(modelAlias, projectName).getId();
        checkRequiredArg(NModelController.MODEL_ID, modelId);
        return modelController.updateModelName(modelId, modelRenameRequest);
    }

    @ApiOperation(value = "updateModelStatus", tags = { "AI" })
    @PutMapping(value = "/{model_name}/status")
    @ResponseBody
    public EnvelopeResponse<String> updateModelStatus(@PathVariable("model_name") String modelAlias,
            @RequestBody ModelUpdateRequest modelRenameRequest) {
        String projectName = checkProjectName(modelRenameRequest.getProject());
        String modelId = getModel(modelAlias, projectName).getId();
        return modelController.updateModelStatus(modelId, modelRenameRequest);
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
}

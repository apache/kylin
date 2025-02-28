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
import static org.apache.kylin.common.exception.ServerErrorCode.MODEL_BROKEN;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.MODEL_NAME_EMPTY;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.MODEL_NAME_NOT_EXIST;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.schema.SchemaChangeCheckResult;
import org.apache.kylin.rest.constant.ModelStatusToDisplayEnum;
import org.apache.kylin.rest.controller.NBasicController;
import org.apache.kylin.rest.controller.NMetaStoreController;
import org.apache.kylin.rest.request.ModelImportRequest;
import org.apache.kylin.rest.request.ModelPreviewRequest;
import org.apache.kylin.rest.request.OpenModelPreviewRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ModelPreviewResponse;
import org.apache.kylin.rest.service.MetaStoreService;
import org.apache.kylin.rest.service.ModelService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

import io.swagger.annotations.ApiOperation;
import lombok.val;

@Controller
@RequestMapping(value = "/api/metastore", produces = { HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class OpenMetaStoreController extends NBasicController {

    @Autowired
    private ModelService modelService;

    @Autowired
    private NMetaStoreController metaStoreController;

    @Autowired
    private MetaStoreService metaStoreService;

    @ApiOperation(value = "previewModels", tags = { "MID" })
    @GetMapping(value = "/previews/models")
    @ResponseBody
    public EnvelopeResponse<List<ModelPreviewResponse>> previewModels(@RequestParam(value = "project") String project,
            @RequestParam(value = "model_names", required = false, defaultValue = "") List<String> modelNames) {
        String projectName = checkProjectName(project);
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        List<String> modelIds = new ArrayList<>();
        List<String> notExistModelNames = new ArrayList<>();
        for (String modelName : modelNames) {
            val modelDesc = modelManager.getDataModelDescByAlias(modelName);
            if (Objects.isNull(modelDesc)) {
                notExistModelNames.add(modelName);
                continue;
            }
            modelIds.add(modelDesc.getId());
        }
        if (!notExistModelNames.isEmpty()) {
            String joinedModelNames = StringUtils.join(notExistModelNames, ",");
            throw new KylinException(MODEL_NAME_NOT_EXIST, joinedModelNames);
        }

        List<ModelPreviewResponse> simplifiedModels = metaStoreService.getPreviewModels(projectName, modelIds);
        List<ModelPreviewResponse> nonBrokenModels = simplifiedModels.stream()
                .filter(modelPreviewResponse -> modelPreviewResponse.getStatus() != ModelStatusToDisplayEnum.BROKEN)
                .collect(Collectors.toList());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, nonBrokenModels, "");
    }

    @ApiOperation(value = "exportModelMetadata", tags = { "MID" })
    @PostMapping(value = "/backup/models")
    @ResponseBody
    public EnvelopeResponse<String> exportModelMetadata(@RequestParam(value = "project") String project,
            @RequestBody OpenModelPreviewRequest request, HttpServletResponse response) throws Exception {
        String projectName = checkProjectName(project);
        checkExportModelsValid(projectName, request);
        ModelPreviewRequest modelPreviewRequest = convertToModelPreviewRequest(projectName, request);
        metaStoreController.exportModelMetadata(projectName, modelPreviewRequest, response);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "uploadAndCheckModelMetadata", tags = { "MID" })
    @PostMapping(value = "/validation/models")
    @ResponseBody
    public EnvelopeResponse<SchemaChangeCheckResult> uploadAndCheckModelMetadata(
            @RequestParam(value = "project") String project, @RequestPart("file") MultipartFile uploadFile,
            @RequestPart(value = "request", required = false) ModelImportRequest request) throws Exception {
        return metaStoreController.uploadAndCheckModelMetadata(project, uploadFile, request);
    }

    @ApiOperation(value = "importModelMetadata", tags = { "MID" })
    @PostMapping(value = "/import/models")
    @ResponseBody
    public EnvelopeResponse<String> importModelMetadata(@RequestParam(value = "project") String project,
            @RequestPart(value = "file") MultipartFile metadataFile, @RequestPart("request") ModelImportRequest request)
            throws Exception {
        String projectName = checkProjectName(project);
        metaStoreController.importModelMetadata(projectName, metadataFile, request);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    private ModelPreviewRequest convertToModelPreviewRequest(String project, OpenModelPreviewRequest request) {
        // have checked model names exist
        NDataModelManager modelManager = modelService.getManager(NDataModelManager.class, project);
        List<String> modelIds = request.getNames().stream()
                .map(name -> modelManager.getDataModelDescByAlias(name).getUuid()).collect(Collectors.toList());
        ModelPreviewRequest modelPreviewRequest = new ModelPreviewRequest();
        modelPreviewRequest.setIds(modelIds);
        modelPreviewRequest.setExportRecommendations(request.isExportRecommendations());
        modelPreviewRequest.setExportOverProps(request.isExportOverProps());
        modelPreviewRequest.setExportMultiplePartitionValues(request.isExportMultiplePartitionValues());
        return modelPreviewRequest;
    }

    private void checkRequestModelNamesNotEmpty(OpenModelPreviewRequest request) {
        List<String> modelNames = request.getNames();
        if (CollectionUtils.isEmpty(modelNames)) {
            throw new KylinException(MODEL_NAME_EMPTY);
        }
    }

    private void checkExportModelsValid(String project, OpenModelPreviewRequest request) {
        checkRequestModelNamesNotEmpty(request);
        NDataModelManager modelManager = modelService.getManager(NDataModelManager.class, project);
        for (String modelName : request.getNames()) {
            val modelDesc = modelManager.getDataModelDescByAlias(modelName);
            if (Objects.isNull(modelDesc)) {
                throw new KylinException(MODEL_NAME_NOT_EXIST, modelName);
            }
            if (modelDesc.isBroken()) {
                throw new KylinException(MODEL_BROKEN,
                        String.format(Locale.ROOT, "Broken model cannot be exported. Model name: [%s].", modelName));
            }
        }
    }
}

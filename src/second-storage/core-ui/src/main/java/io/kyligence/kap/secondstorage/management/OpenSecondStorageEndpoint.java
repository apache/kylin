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

package io.kyligence.kap.secondstorage.management;

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.MODEL_NAME_NOT_EXIST;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.SEGMENT_EMPTY_PARAMETER;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.rest.controller.NBasicController;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.JobInfoResponse;
import org.apache.kylin.rest.service.ModelService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import org.apache.kylin.guava30.shaded.common.collect.Lists;

import io.kyligence.kap.secondstorage.management.request.ModelEnableRequest;
import io.kyligence.kap.secondstorage.management.request.ModelModifyRequest;
import io.kyligence.kap.secondstorage.management.request.ProjectLoadResponse;
import io.kyligence.kap.secondstorage.management.request.ProjectRecoveryResponse;
import io.kyligence.kap.secondstorage.management.request.RecoverRequest;
import io.kyligence.kap.secondstorage.management.request.StorageRequest;
import io.swagger.annotations.ApiOperation;
import lombok.val;
import lombok.extern.slf4j.Slf4j;


@RestController
@RequestMapping(value = "/api/storage", produces = {HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON})
@Slf4j
@ConditionalOnProperty({"kylin.second-storage.class"})
public class OpenSecondStorageEndpoint extends NBasicController {
    private static final String MODEL_ARG_NAME = "model_name";
    private static final String MODEL_ENABLE = "enabled";

    private static final String NULLABLE_STRING = "Nullable(String)";

    private static final String LOW_CARDINALITY_STRING = "LowCardinality(Nullable(String))";

    @Autowired
    @Qualifier("modelService")
    private ModelService modelService;

    @Autowired
    @Qualifier("secondStorageEndpoint")
    private SecondStorageEndpoint secondStorageEndpoint;

    @Autowired
    @Qualifier("secondStorageService")
    private SecondStorageService secondStorageService;

    public OpenSecondStorageEndpoint setModelService(final ModelService modelService) {
        this.modelService = modelService;
        return this;
    }

    public OpenSecondStorageEndpoint setSecondStorageService(final SecondStorageService secondStorageService) {
        this.secondStorageService = secondStorageService;
        return this;
    }

    public OpenSecondStorageEndpoint setSecondStorageEndpoint(final SecondStorageEndpoint secondStorageEndpoint) {
        this.secondStorageEndpoint = secondStorageEndpoint;
        return this;
    }

    @ApiOperation(value = "loadSegments")
    @PostMapping(value = "/segments")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponse> loadStorage(@RequestBody StorageRequest request) {
        checkStorageRequest(request);
        return secondStorageEndpoint.loadStorage(request);
    }

    @ApiOperation(value = "cleanSegments")
    @DeleteMapping(value = "/segments")
    @ResponseBody
    public EnvelopeResponse<Void> cleanStorage(@RequestBody StorageRequest request) {
        checkStorageRequest(request);
        List<String> segIds = convertSegmentIdWithName(request);
        request.setSegmentNames(Lists.newArrayList());
        return secondStorageEndpoint.cleanStorage(request, segIds);
    }

    private void checkStorageRequest(StorageRequest request) {
        String project = checkProjectName(request.getProject());
        checkRequiredArg(MODEL_ARG_NAME, request.getModelName());
        val model = secondStorageEndpoint.checkoutModelName(project, request.getModelName());
        request.setModel(model.getUuid());
        request.setProject(project);
        secondStorageEndpoint.checkSecondStorageEnabled(request.getProject(), model);
        checkSegmentParms(request.getSegmentIds().toArray(new String[0]),
                request.getSegmentNames().toArray(new String[0]));
    }

    protected List<String> convertSegmentIdWithName(StorageRequest request) {
        String[] segIds = modelService.convertSegmentIdWithName(request.getModel(), request.getProject(),
                request.getSegmentIds().toArray(new String[0]),
                request.getSegmentNames().toArray(new String[0]));
        modelService.checkSegmentsExistById(request.getModel(), request.getProject(), segIds);

        if (segIds == null)
            throw new KylinException(SEGMENT_EMPTY_PARAMETER);

        return Lists.newArrayList(segIds);
    }

    @PostMapping(value = "/recovery/model", produces = {HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON})
    public EnvelopeResponse<Void> recoverModel(@RequestBody RecoverRequest request) {
        checkProjectName(request.getProject());
        checkRequiredArg("modelName", request.getModelName());
        secondStorageEndpoint.checkModel(request.getProject(), request.getModelName());
        secondStorageService.importSingleModel(request.getProject(), request.getModelName());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, null, "");
    }

    @PostMapping(value = "/recovery/project", produces = {HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON})
    public EnvelopeResponse<ProjectRecoveryResponse> recoverProject(@RequestBody RecoverRequest request) {
        checkProjectName(request.getProject());
        secondStorageService.isProjectAdmin(request.getProject());
        ProjectLoadResponse response = secondStorageService.projectLoadData(Arrays.asList(request.getProject()));
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response.getLoads().get(0), "");
    }

    @ApiOperation(value = "enableModelWithModelName")
    @PostMapping(value = "/models/state", produces = {HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON})
    @ResponseBody
    public EnvelopeResponse<JobInfoResponse> enableStorage(@RequestBody ModelEnableRequest modelEnableRequest) {
        modelEnableRequest.setProject(checkProjectName(modelEnableRequest.getProject()));
        checkRequiredArg(MODEL_ARG_NAME, modelEnableRequest.getModelName());
        checkRequiredArg(MODEL_ENABLE, modelEnableRequest.getEnabled());
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), modelEnableRequest.getProject());
        val model = modelManager.getDataModelDescByAlias(modelEnableRequest.getModelName());
        if (Objects.isNull(model)) {
            throw new KylinException(MODEL_NAME_NOT_EXIST, modelEnableRequest.getModelName());
        }
        modelEnableRequest.setModel(model.getUuid());
        return secondStorageEndpoint.enableStorage(modelEnableRequest);
    }

    @ApiOperation(value = "modifyColumnDatatype")
    @PostMapping(value = "/models/modify")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponse> modifyColumn(@RequestBody ModelModifyRequest request) {
        String projectName = checkProjectName(request.getProject());
        checkRequiredArg(MODEL_ARG_NAME, request.getModelName());
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), projectName);
        val model = modelManager.getDataModelDescByAlias(request.getModelName());
        if (Objects.isNull(model)) {
            throw new KylinException(MODEL_NAME_NOT_EXIST, request.getModelName());
        }
        checkDatatype(request.getDatatype());
        if (StringUtils.isEmpty(request.getColumn())) {
            throw new KylinException(INVALID_PARAMETER, String.format(MsgPicker.getMsg().getParameterEmpty(), "column"));
        }
        secondStorageService.modifyColumn(projectName, model.getId(), request.getColumn(), request.getDatatype());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, null, "");
    }

    public void checkDatatype(String datatype) {
        if (StringUtils.isEmpty(datatype)) {
            throw new KylinException(INVALID_PARAMETER, String.format(MsgPicker.getMsg().getParameterEmpty(), "datatype"));
        }
        if (!NULLABLE_STRING.equals(datatype) && !LOW_CARDINALITY_STRING.equals(datatype)) {
            throw new KylinException(INVALID_PARAMETER, String.format(MsgPicker.getMsg().getInvalidLowCardinalityDataType()));
        }
    }
}

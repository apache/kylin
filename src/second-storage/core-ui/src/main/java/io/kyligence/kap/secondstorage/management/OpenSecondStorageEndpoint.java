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

import com.google.common.collect.Lists;
import org.apache.kylin.rest.controller.NBasicController;
import org.apache.kylin.rest.response.JobInfoResponse;
import org.apache.kylin.rest.service.ModelService;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.management.request.ProjectLoadResponse;
import io.kyligence.kap.secondstorage.management.request.ProjectRecoveryResponse;
import io.kyligence.kap.secondstorage.management.request.RecoverRequest;
import io.kyligence.kap.secondstorage.management.request.StorageRequest;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;
import static org.apache.kylin.common.exception.ServerErrorCode.SECOND_STORAGE_PROJECT_STATUS_ERROR;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.SEGMENT_EMPTY_PARAMETER;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

@RestController
@RequestMapping(value = "/api/storage", produces = {HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON})
@Slf4j
@ConditionalOnProperty({"kylin.second-storage.class"})
public class OpenSecondStorageEndpoint extends NBasicController {
    private static final String MODEL_ARG_NAME = "model_name";

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
        checkProjectName(request.getProject());
        checkRequiredArg(MODEL_ARG_NAME, request.getModelName());
        String modelId = modelService.getModelDesc(request.getModelName(), request.getProject()).getUuid();
        request.setModel(modelId);

        checkSecondStorageEnabled(request);
        checkSegmentParms(request.getSegmentIds().toArray(new String[0]),
                request.getSegmentNames().toArray(new String[0]));
        return secondStorageEndpoint.loadStorage(request);
    }

    @ApiOperation(value = "cleanSegments")
    @DeleteMapping(value = "/segments")
    @ResponseBody
    public EnvelopeResponse<Void> cleanStorage(@RequestBody StorageRequest request) {
        checkProjectName(request.getProject());
        checkRequiredArg(MODEL_ARG_NAME, request.getModelName());
        String modelId = modelService.getModelDesc(request.getModelName(), request.getProject()).getUuid();
        request.setModel(modelId);

        checkSecondStorageEnabled(request);
        checkSegmentParms(request.getSegmentIds().toArray(new String[0]),
                request.getSegmentNames().toArray(new String[0]));
        List<String> segIds = convertSegmentIdWithName(request);
        request.setSegmentNames(Lists.newArrayList());
        return secondStorageEndpoint.cleanStorage(request, segIds);
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

    private void checkSecondStorageEnabled(StorageRequest request) {
        if (!SecondStorageUtil.isProjectEnable(request.getProject())) {
            throw new KylinException(SECOND_STORAGE_PROJECT_STATUS_ERROR,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getSecondStorageProjectEnabled(), request.getProject()));

        }

        if (!SecondStorageUtil.isModelEnable(request.getProject(), request.getModel())) {
            throw new KylinException(SECOND_STORAGE_PROJECT_STATUS_ERROR,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getSecondStorageModelEnabled(), request.getModelName()));
        }
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
}

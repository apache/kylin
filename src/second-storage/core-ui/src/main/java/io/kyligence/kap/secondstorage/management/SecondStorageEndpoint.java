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

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_PROJECT_NAME;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;
import static org.apache.kylin.common.exception.ServerErrorCode.SECOND_STORAGE_PROJECT_STATUS_ERROR;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.MODEL_NAME_NOT_EXIST;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.rest.controller.NBasicController;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.JobInfoResponse;
import org.apache.kylin.rest.service.ModelService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import io.kyligence.kap.secondstorage.SecondStorageNodeHelper;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.enums.LockTypeEnum;
import io.kyligence.kap.secondstorage.management.request.ModelEnableRequest;
import io.kyligence.kap.secondstorage.management.request.ProjectCleanRequest;
import io.kyligence.kap.secondstorage.management.request.ProjectEnableRequest;
import io.kyligence.kap.secondstorage.management.request.ProjectLoadRequest;
import io.kyligence.kap.secondstorage.management.request.ProjectLockOperateRequest;
import io.kyligence.kap.secondstorage.management.request.ProjectNodeRequest;
import io.kyligence.kap.secondstorage.management.request.ProjectRecoveryResponse;
import io.kyligence.kap.secondstorage.management.request.ProjectTableSyncResponse;
import io.kyligence.kap.secondstorage.management.request.SecondStorageIndexResponse;
import io.kyligence.kap.secondstorage.management.request.SecondStorageMetadataRequest;
import io.kyligence.kap.secondstorage.management.request.StorageRequest;
import io.kyligence.kap.secondstorage.management.request.UpdateIndexRequest;
import io.kyligence.kap.secondstorage.management.request.UpdateIndexResponse;
import io.swagger.annotations.ApiOperation;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping(value = "/api/storage", produces = {HTTP_VND_APACHE_KYLIN_JSON})
@Slf4j
@ConditionalOnProperty({"kylin.second-storage.class"})
public class SecondStorageEndpoint extends NBasicController {
    private static final String MODEL_ARG = "model";
    private static final String MODEL_ARG_NAME = "model_name";
    private static final String LAYOUT_ID_ARG_NAME = "layout_id";

    @Autowired
    @Qualifier("modelService")
    private ModelService modelService;

    @Autowired
    @Qualifier("secondStorageService")
    private SecondStorageService secondStorageService;

    public SecondStorageEndpoint setSecondStorageService(SecondStorageService secondStorageService) {
        this.secondStorageService = secondStorageService;
        return this;
    }

    public SecondStorageEndpoint setModelService(final ModelService modelService) {
        this.modelService = modelService;
        return this;
    }

    @ApiOperation(value = "loadSegments")
    @PostMapping(value = "/segments")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponse> loadStorage(@RequestBody StorageRequest request) {
        checkProjectName(request.getProject());
        checkRequiredArg(MODEL_ARG, request.getModel());
        checkSegmentParms(request.getSegmentIds().toArray(new String[0]),
                request.getSegmentNames().toArray(new String[0]));
        return internalLoadIntoStorage(request);
    }

    @ApiOperation(value = "cleanSegments")
    @DeleteMapping(value = "/segments")
    @ResponseBody
    public EnvelopeResponse<Void> cleanStorage(StorageRequest request,
                                         @RequestParam(name="segment_ids") List<String> segmentIds) {
        request.setSegmentIds(segmentIds);
        checkProjectName(request.getProject());
        checkRequiredArg(MODEL_ARG, request.getModel());
        checkSegmentParms(request.getSegmentIds().toArray(new String[0]),
                request.getSegmentNames().toArray(new String[0]));
        secondStorageService.triggerSegmentsClean(request.getProject(), request.getModel(), new HashSet<>(request.getSegmentIds()));
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, null, "");
    }

    @ApiOperation(value = "enableModel")
    @PostMapping(value = "/model/state")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponse> enableStorage(@RequestBody ModelEnableRequest modelEnableRequest) {
        checkProjectName(modelEnableRequest.getProject());
        checkRequiredArg(MODEL_ARG, modelEnableRequest.getModel());
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), modelEnableRequest.getProject());
        val model = modelManager.getDataModelDesc(modelEnableRequest.getModel());
        checkModel(modelEnableRequest.getProject(), model.getAlias());
        val jobInfo = secondStorageService.changeModelSecondStorageState(modelEnableRequest.getProject(),
                modelEnableRequest.getModel(), modelEnableRequest.getEnabled());
        JobInfoResponse jobInfoResponse = new JobInfoResponse();
        jobInfoResponse.setJobs(Collections.singletonList(jobInfo.orElse(null)));
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, jobInfoResponse, "");
    }

    @ApiOperation(value = "enableProject")
    @PostMapping(value = "/project/state", produces = {HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON})
    public EnvelopeResponse<JobInfoResponse> enableProjectStorage(@RequestBody ProjectEnableRequest projectEnableRequest) {
        String projectName = checkProjectName(projectEnableRequest.getProject());
        val jobInfo = secondStorageService.changeProjectSecondStorageState(projectName,
                projectEnableRequest.getNewNodes(),
                projectEnableRequest.isEnabled());
        JobInfoResponse jobInfoResponse = new JobInfoResponse();
        jobInfoResponse.setJobs(Collections.singletonList(jobInfo.orElse(null)));
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, jobInfoResponse, "");
    }

    @ApiOperation(value = "deleteProjectNodes")
    @DeleteMapping(value = "/project/state", produces = {HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON})
    public EnvelopeResponse<List<String>> deleteProjectNodes(ProjectNodeRequest request,
                                                             @RequestParam(name = "shard_names") List<String> shardNames) {
        checkProjectName(request.getProject());
        request.setShardNames(shardNames);

        if (!SecondStorageUtil.isProjectEnable(request.getProject())) {
            throw new KylinException(INVALID_PARAMETER, String.format(Locale.ROOT, "Project %s is not enable second storage", request.getProject()));
        }

        if (CollectionUtils.isEmpty(request.getShardNames())) {
            throw new KylinException(INVALID_PARAMETER, "shard_names is empty");
        }

        List<String> shards = SecondStorageNodeHelper.getAllPairs();
        if (!shards.containsAll(request.getShardNames())) {
            throw new KylinException(INVALID_PARAMETER, String.format(Locale.ROOT, "Second storage shard names not contains %s", request.getShardNames()));
        }

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, this.secondStorageService.deleteProjectSecondStorageNode(request.getProject(), request.getShardNames(), request.isForce()), "");
    }

    @ApiOperation(value = "disableProjectStorageValidation")
    @PostMapping(value="/project/state/validation")
    public EnvelopeResponse<List<String>> validateProjectStorage(@RequestBody ProjectEnableRequest projectEnableRequest) {
        checkProjectName(projectEnableRequest.getProject());
        List<String> models = secondStorageService.getAllSecondStorageModel(projectEnableRequest.getProject());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, models, "");
    }

    @ApiOperation(value = "listSecondStorageNodes")
    @GetMapping(value = "/nodes")
    @ResponseBody
    public EnvelopeResponse<Map<String, List<NodeData>>> listNodes() {
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, secondStorageService.listAvailableNodes(), "");
    }

    @ApiOperation(value = "listSecondStorageNodesByProject")
    @GetMapping(value = "/project/nodes", produces = {HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON})
    @ResponseBody
    public EnvelopeResponse<List<ProjectNode>> projectNodes(String project) {
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, secondStorageService.projectNodes(project), "");
    }

    private EnvelopeResponse<JobInfoResponse> internalLoadIntoStorage(StorageRequest request) {
        SecondStorageUtil.validateProjectLock(request.getProject(), Arrays.asList(LockTypeEnum.LOAD.name()));
        String[] segIds = modelService.convertSegmentIdWithName(request.getModel(), request.getProject(),
                request.getSegmentIds().toArray(new String[0]),
                request.getSegmentNames().toArray(new String[0]));

        if (ArrayUtils.isEmpty(segIds)) {
            throw new KylinException(INVALID_PARAMETER, MsgPicker.getMsg().getInvalidRefreshSegment());
        }

        if (!request.storageTypeSupported()) {
            throw new KylinException(INVALID_PARAMETER, "");
        }

        JobInfoResponse response = new JobInfoResponse();
        response.setJobs(modelService.exportSegmentToSecondStorage(request.getProject(), request.getModel(), segIds));
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    @PostMapping(value = "/lock/operate", produces = {HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON})
    public EnvelopeResponse<Void> lockOperate(@RequestBody ProjectLockOperateRequest request) {
        checkProjectName(request.getProject());
        secondStorageService.lockOperate(request.getProject(), request.getLockTypes(), request.getOperateType());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, null, "");
    }

    @GetMapping(value = "/lock/list", produces = {HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON})
    @ResponseBody
    public EnvelopeResponse<List<ProjectLock>> lockList(@RequestParam("project") String project) {
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, secondStorageService.lockList(project), "");
    }

    @GetMapping(value = "/jobs/all", produces = {HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON})
    @ResponseBody
    public EnvelopeResponse<List<String>> getAllSecondStorageJobs() {
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, secondStorageService.getAllSecondStorageJobs(), "");
    }

    @GetMapping(value = "/jobs/project", produces = {HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON})
    @ResponseBody
    public EnvelopeResponse<List<String>> getProjectSecondStorageJobs(String project) {
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, secondStorageService.getProjectSecondStorageJobs(project), "");
    }

    @PostMapping(value = "/sizeInNode", produces = {HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON})
    public EnvelopeResponse<Void> sizeInNode(@RequestBody SecondStorageMetadataRequest request) {
        checkProjectName(request.getProject());
        if (!SecondStorageUtil.isProjectEnable(request.getProject())) {
            throw new KylinException(SECOND_STORAGE_PROJECT_STATUS_ERROR,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getSecondStorageProjectEnabled(), request.getProject()));

        }
        secondStorageService.sizeInNode(request.getProject());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, null, "");
    }

    @GetMapping(value = "/table/sync", produces = {HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON})
    public EnvelopeResponse<ProjectTableSyncResponse> tableSync(String project) {
        checkProjectName(project);
        if (!SecondStorageUtil.isProjectEnable(project)) {
            throw new KylinException(SECOND_STORAGE_PROJECT_STATUS_ERROR,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getSecondStorageProjectEnabled(), project));

        }
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, secondStorageService.tableSync(project, true), "");
    }

    @PostMapping(value = "/project/load", produces = {HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON})
    public EnvelopeResponse<List<ProjectRecoveryResponse>> projectLoad(@RequestBody ProjectLoadRequest request) {
        checkProjects(request.getProjects());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                secondStorageService.projectLoadData(request.getProjects()).getLoads(), "");
    }

    @PostMapping(value = "/project/clean", produces = {HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON})
    public EnvelopeResponse<Map<String, Map<String, String>>> projectClean(@RequestBody ProjectCleanRequest request) {
        checkProjects(request.getProjects());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, secondStorageService.projectClean(request.getProjects()), "");
    }

    @PostMapping(value = "/config/refresh", produces = {HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON})
    public EnvelopeResponse<ProjectRecoveryResponse> refreshConf() {
        secondStorageService.refreshConf();
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, null, "");
    }
    @PostMapping(value = "/reset", produces = {HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON})
    public EnvelopeResponse<Void> resetStorage() {
        secondStorageService.resetStorage();
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, null, "");
    }

    @PostMapping(value = "/node/status", produces = {HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON})
    public EnvelopeResponse<Void> updateNodeStatus(@RequestBody Map<String, Map<String, Boolean>> nodeStatusMap) {
        secondStorageService.updateNodeStatus(nodeStatusMap);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, null, "");
    }

    @ApiOperation(value = "updateSecondStorageIndex")
    @PostMapping(value = "/index", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<UpdateIndexResponse> updateIndex(@RequestBody UpdateIndexRequest updateIndexRequest) {
        val model = checkProjectAndModel(updateIndexRequest.getProject(), updateIndexRequest.getModelName());
        val project = model.getProject();
        checkSecondStorageEnabled(project, model);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, secondStorageService.updateIndexByColumnName(project,
                model.getId(), updateIndexRequest.getPrimaryIndexes(), updateIndexRequest.getSecondaryIndexes()), "");
    }

    @ApiOperation(value = "listSecondStorageIndex")
    @GetMapping(value = "/index", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<List<SecondStorageIndexResponse>> listIndex(@RequestParam("project") String project,
            @RequestParam("model_name") String modelName) {
        val model = checkProjectAndModel(project, modelName);
        checkSecondStorageEnabled(model.getProject(), model);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                secondStorageService.listIndex(model.getProject(), model.getId()), "");
    }

    @ApiOperation(value = "materializeSecondaryIndex")
    @PostMapping(value = "/index/secondary/materialize", produces = { HTTP_VND_APACHE_KYLIN_JSON,
            HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<UpdateIndexResponse> materializeSecondaryIndex(
            @RequestBody UpdateIndexRequest updateIndexRequest) {
        val model = checkProjectAndModel(updateIndexRequest.getProject(), updateIndexRequest.getModelName());
        checkSecondStorageEnabled(model.getProject(), model);
        checkRequiredArg(LAYOUT_ID_ARG_NAME, updateIndexRequest.getLayoutId());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, new UpdateIndexResponse(secondStorageService
                .materializeSecondaryIndex(model.getProject(), model.getId(), updateIndexRequest.getLayoutId())), "");
    }

    @ApiOperation(value = "deletePrimaryIndex")
    @DeleteMapping(value = "/index/primary", produces = { HTTP_VND_APACHE_KYLIN_JSON,
            HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<Void> deletePrimaryIndex(@RequestParam("project") String project,
            @RequestParam("model_name") String modelName, @RequestParam("layout_id") Long layoutId) {
        val model = checkProjectAndModel(project, modelName);
        checkRequiredArg(LAYOUT_ID_ARG_NAME, layoutId);
        String modelId = model.getId();
        secondStorageService.deletePrimaryIndex(model.getProject(), modelId, layoutId);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, null, "");
    }

    @ApiOperation(value = "deleteSecondaryIndex")
    @DeleteMapping(value = "/index/secondary", produces = { HTTP_VND_APACHE_KYLIN_JSON,
            HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<UpdateIndexResponse> deleteSecondaryIndex(@RequestParam("project") String project,
            @RequestParam("model_name") String modelName, @RequestParam("layout_id") Long layoutId) {
        val model = checkProjectAndModel(project, modelName);
        checkRequiredArg(LAYOUT_ID_ARG_NAME, layoutId);
        String modelId = model.getId();
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, new UpdateIndexResponse(
                secondStorageService.deleteSecondaryIndex(model.getProject(), modelId, layoutId)), "");
    }

    public NDataModel checkoutModelName(String project, String modelName) {
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val model = modelManager.getDataModelDescByAlias(modelName);
        if (Objects.isNull(model)) {
            throw new KylinException(MODEL_NAME_NOT_EXIST, modelName);
        }
        return model;
    }

    public void checkModel(String project, String modelName) {
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val model = modelManager.getDataModelDescByAlias(modelName);
        if (Objects.isNull(model)) {
            throw new KylinException(MODEL_NAME_NOT_EXIST, modelName);
        }
    }

    public void checkProjects(List<String> projects) {
        if (CollectionUtils.isEmpty(projects)) {
            throw new KylinException(EMPTY_PROJECT_NAME, MsgPicker.getMsg().getEmptyProjectName());
        }

        for (String project : projects) {
            checkProjectName(project);
        }
    }

    private NDataModel checkProjectAndModel(String project, String modelName) {
        project = checkProjectName(project);
        checkRequiredArg(MODEL_ARG_NAME, modelName);
        return checkoutModelName(project, modelName);
    }

    public void checkSecondStorageEnabled(String project, NDataModel model) {
        if (!SecondStorageUtil.isProjectEnable(project)) {
            throw new KylinException(SECOND_STORAGE_PROJECT_STATUS_ERROR,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getSecondStorageProjectEnabled(), project));
        }

        if (!SecondStorageUtil.isModelEnable(project, model.getUuid())) {
            throw new KylinException(SECOND_STORAGE_PROJECT_STATUS_ERROR,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getSecondStorageModelEnabled(), model.getAlias()));
        }
    }
}

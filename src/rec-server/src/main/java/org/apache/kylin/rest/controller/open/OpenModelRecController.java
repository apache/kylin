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
import static org.apache.kylin.common.exception.code.ErrorCodeServer.MODEL_NOT_EXIST_SEGMENTS;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.code.ErrorCodeServer;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.rest.aspect.WaitForSyncBeforeRPC;
import org.apache.kylin.rest.controller.NBasicController;
import org.apache.kylin.rest.request.OpenBatchApproveRecItemsRequest;
import org.apache.kylin.rest.request.OpenSqlAccelerateRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.OpenAccSqlResponse;
import org.apache.kylin.rest.response.OpenRecApproveResponse;
import org.apache.kylin.rest.response.OpenSuggestionResponse;
import org.apache.kylin.rest.service.ModelRecService;
import org.apache.kylin.rest.service.ModelService;
import org.apache.kylin.rest.service.OptRecApproveService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping(value = "/api/models", produces = { HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class OpenModelRecController extends NBasicController {

    private static final Logger logger = LoggerFactory.getLogger(OpenModelRecController.class);

    @Autowired
    private ModelRecService modelRecService;

    @Autowired
    private OptRecApproveService optRecApproveService;

    @Autowired
    private ModelService modelService;

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @ApiOperation(value = "batchApproveRecommendations", tags = { "AI" })
    @PutMapping(value = "/recommendations/batch")
    @ResponseBody
    @WaitForSyncBeforeRPC
    public EnvelopeResponse<OpenRecApproveResponse> batchApproveRecommendations(
            @RequestBody OpenBatchApproveRecItemsRequest request) {
        checkRequiredArg("filter_by_models", request.isFilterByModes());
        String projectName = checkProjectName(request.getProject());
        checkProjectNotSemiAuto(projectName);
        boolean filterByModels = request.isFilterByModes();
        if (request.getRecActionType() == null || StringUtils.isEmpty(request.getRecActionType().trim())) {
            request.setRecActionType("all");
        }
        List<OpenRecApproveResponse.RecToIndexResponse> approvedModelIndexes;
        List<String> modelIds = Lists.newArrayList();
        if (filterByModels) {
            if (CollectionUtils.isEmpty(request.getModelNames())) {
                throw new KylinException(ErrorCodeServer.MODEL_NAME_EMPTY);
            }
            for (String modelName : request.getModelNames()) {
                modelIds.add(modelService.getModel(modelName, projectName).getUuid());
            }
        }
        approvedModelIndexes = optRecApproveService.batchApprove(projectName, modelIds, request.getRecActionType(),
                filterByModels, request.isDiscardTableIndex());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                new OpenRecApproveResponse(projectName, approvedModelIndexes), "");
    }

    @ApiOperation(value = "suggestModels", tags = { "AI" })
    @PostMapping(value = "/model_suggestion")
    @ResponseBody
    public EnvelopeResponse<OpenSuggestionResponse> suggestModels(@RequestBody OpenSqlAccelerateRequest request) {
        String projectName = checkProjectName(request.getProject());
        checkSqlIsNotNull(request.getSqls());
        if (request.isWithModelOnline() && !request.isWithEmptySegment()) {
            throw new KylinException(MODEL_NOT_EXIST_SEGMENTS);
        }
        request.setProject(projectName);
        checkProjectNotSemiAuto(request.getProject());
        request.setForce2CreateNewModel(true);
        request.setAcceptRecommendation(true);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, modelRecService.suggestOrOptimizeModels(request),
                "");
    }

    @ApiOperation(value = "optimizeModels", tags = { "AI" })
    @PostMapping(value = "/model_optimization")
    @ResponseBody
    @WaitForSyncBeforeRPC
    public EnvelopeResponse<OpenSuggestionResponse> optimizeModels(@RequestBody OpenSqlAccelerateRequest request) {
        String projectName = checkProjectName(request.getProject());
        checkSqlIsNotNull(request.getSqls());
        request.setProject(projectName);
        checkProjectNotSemiAuto(request.getProject());
        checkNotEmpty(request.getSqls());
        request.setForce2CreateNewModel(false);
        request.setWithOptimalModel(true);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, modelRecService.suggestOrOptimizeModels(request),
                "");
    }

    @ApiOperation(value = "/accelerateSql", tags = { "AI" })
    @PostMapping(value = { "/accelerate_sqls", "/sql_acceleration" })
    @ResponseBody
    @WaitForSyncBeforeRPC
    public EnvelopeResponse<OpenAccSqlResponse> accelerateSqls(@RequestBody OpenSqlAccelerateRequest request) {
        String projectName = checkProjectName(request.getProject());
        checkSqlIsNotNull(request.getSqls());
        request.setProject(projectName);
        request.setForce2CreateNewModel(false);
        request.setWithOptimalModel(true);
        checkProjectNotSemiAuto(request.getProject());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, modelRecService.suggestAndOptimizeModels(request),
                "");
    }
}

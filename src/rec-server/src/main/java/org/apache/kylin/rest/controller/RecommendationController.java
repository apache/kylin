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

package org.apache.kylin.rest.controller;

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;

import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.rest.aspect.WaitForSyncBeforeRPC;
import org.apache.kylin.rest.request.OptRecRequest;
import org.apache.kylin.rest.request.RecCountUpdateRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.OptRecDetailResponse;
import org.apache.kylin.rest.response.OptRecLayoutsResponse;
import org.apache.kylin.rest.service.ModelSmartService;
import org.apache.kylin.rest.service.OptRecService;
import org.apache.kylin.rest.service.ProjectSmartService;
import org.apache.kylin.rest.service.RawRecService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
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
import org.springframework.web.servlet.View;

import io.swagger.annotations.ApiOperation;
import lombok.val;

@Controller
@RequestMapping(value = "/api/recommendations", produces = { HTTP_VND_APACHE_KYLIN_JSON })
public class RecommendationController extends NBasicController {
    private static final Logger logger = LoggerFactory.getLogger("smart");
    private static final String MODEL_ID = "modelId";
    private static final String REC_COUNT_ACTION = "action";

    @Autowired
    @Qualifier("optRecService")
    private OptRecService optRecService;

    @Autowired
    private RawRecService rawRecService;

    @Autowired
    private ProjectSmartService projectService;

    @Autowired
    private ModelSmartService modelSmartService;

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Deprecated
    @PostMapping(value = "/{model:.+}")
    public String approveOptimizeRecommendations(HttpServletRequest request) {
        request.setAttribute(View.RESPONSE_STATUS_ATTRIBUTE, HttpStatus.PERMANENT_REDIRECT);
        return "redirect:/api/models/recommendations";
    }

    @ApiOperation(value = "validateOptimizeRecommendations", tags = { "AI" }, notes = "Add URL: {model}")
    @PostMapping(value = "/{model:.+}/validation")
    @ResponseBody
    public EnvelopeResponse<OptRecDetailResponse> validateOptimizeRecommendations(@PathVariable("model") String modelId,
            @RequestBody OptRecRequest request) {
        checkProjectName(request.getProject());
        checkProjectNotSemiAuto(request.getProject());
        checkRequiredArg(MODEL_ID, modelId);
        OptRecDetailResponse optRecDetailResponse = optRecService.validateSelectedRecItems(request.getProject(),
                modelId, request.getRecItemsToAddLayout(), request.getRecItemsToRemoveLayout());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, optRecDetailResponse, "");
    }

    @ApiOperation(value = "cleanOptimizeRecommendations", tags = { "AI" }, notes = "Add URL: {model}")
    @DeleteMapping(value = "/{model:.+}/all")
    @ResponseBody
    public EnvelopeResponse<String> cleanOptimizeRecommendations(@PathVariable("model") String modelId,
            @RequestParam("project") String project) {
        checkProjectName(project);
        checkProjectNotSemiAuto(project);
        checkRequiredArg(MODEL_ID, modelId);
        optRecService.clean(project, modelId);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "removeOptimizeRecommendationsV2", tags = { "AI" }, notes = "Add URL: {model}")
    @DeleteMapping(value = "/{model:.+}")
    @ResponseBody
    public EnvelopeResponse<String> deleteOptimizeRecommendationsV2(@PathVariable(value = "model") String modelId,
            @RequestParam(value = "project") String project,
            @RequestParam(value = "recs_to_remove_layout", required = false) List<Integer> layoutsToRemove,
            @RequestParam(value = "recs_to_add_layout", required = false) List<Integer> layoutsToAdd) {
        checkProjectName(project);
        checkProjectNotSemiAuto(project);
        checkRequiredArg(MODEL_ID, modelId);
        val request = new OptRecRequest();
        request.setModelId(modelId);
        request.setProject(project);
        if (layoutsToRemove != null) {
            request.setRecItemsToRemoveLayout(layoutsToRemove);
        }
        if (layoutsToAdd != null) {
            request.setRecItemsToAddLayout(layoutsToAdd);
        }

        optRecService.discard(request.getProject(), request);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "getOptimizeRecommendations", tags = { "AI" }, notes = "Add URL: {model}")
    @GetMapping(value = "/{model:.+}")
    @ResponseBody
    @WaitForSyncBeforeRPC
    public EnvelopeResponse<OptRecLayoutsResponse> getOptimizeRecommendations(
            @PathVariable(value = "model") String modelId, @RequestParam(value = "project") String project,
            @RequestParam(value = "type", required = false, defaultValue = "") List<String> recTypeList,
            @RequestParam(value = "reverse", required = false, defaultValue = "false") Boolean desc,
            @RequestParam(value = "key", required = false, defaultValue = "") String key,
            @RequestParam(value = "sort_by", required = false, defaultValue = "") String sortBy,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer limit) {
        checkProjectName(project);
        checkProjectNotSemiAuto(project);
        checkRequiredArg(MODEL_ID, modelId);
        OptRecLayoutsResponse optRecLayoutsResponse = optRecService.getOptRecLayoutsResponse(project, modelId,
                recTypeList, key, desc, sortBy, offset, limit);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, optRecLayoutsResponse, "");
    }

    @ApiOperation(value = "getOptimizeRecommendationDetail", tags = { "AI" }, notes = "Add URL: {model}")
    @GetMapping(value = "/{model:.+}/{item_id:.+}")
    @ResponseBody
    @WaitForSyncBeforeRPC
    public EnvelopeResponse<OptRecDetailResponse> getOptimizeRecommendations(
            @PathVariable(value = "model") String modelId, //
            @PathVariable(value = "item_id") Integer itemId, //
            @RequestParam(value = "project") String project, //
            @RequestParam(value = "is_add", defaultValue = "true") boolean isAdd) {
        checkProjectName(project);
        checkProjectNotSemiAuto(project);
        checkRequiredArg(MODEL_ID, modelId);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                optRecService.getSingleOptRecDetail(project, modelId, itemId, isAdd), "");
    }

    @ApiOperation(value = "refreshRecommendationCount", tags = { "AI" }, notes = "Add URL: {model}")
    @PutMapping(value = "/count")
    @ResponseBody
    @WaitForSyncBeforeRPC
    public EnvelopeResponse<String> refreshRecommendationCount(@RequestBody RecCountUpdateRequest request) {
        String project = request.getProject();
        String modelId = request.getModelId();
        String action = request.getAction();
        checkProjectName(project);
        checkProjectNotSemiAuto(project);
        checkRequiredArg(MODEL_ID, modelId);
        checkRequiredArg(REC_COUNT_ACTION, action);
        optRecService.updateRecommendationCount(project, modelId);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "accelerate query history and select topn", tags = { "AI" }, notes = "Add URL: {model}")
    @PutMapping(value = "/acceleration")
    @ResponseBody
    @WaitForSyncBeforeRPC
    public EnvelopeResponse<String> accelerate(@RequestParam("project") String project) {
        checkProjectName(project);
        checkProjectNotSemiAuto(project);
        projectService.accelerateImmediately(project);
        rawRecService.updateCostsAndTopNCandidates(project);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }
}

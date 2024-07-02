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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.rec.AbstractContext;
import org.apache.kylin.rec.common.AccelerateInfo;
import org.apache.kylin.rec.query.validator.SQLValidateResult;
import org.apache.kylin.rest.aspect.WaitForSyncBeforeRPC;
import org.apache.kylin.rest.controller.NBasicController;
import org.apache.kylin.rest.request.FavoriteRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.OpenOptRecLayoutsResponse;
import org.apache.kylin.rest.response.OpenValidationResponse;
import org.apache.kylin.rest.response.OptRecLayoutsResponse;
import org.apache.kylin.rest.service.FavoriteRuleService;
import org.apache.kylin.rest.service.ModelService;
import org.apache.kylin.rest.service.ModelSmartService;
import org.apache.kylin.rest.service.OptRecService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping(value = "/api/models", produces = { HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class OpenModelSmartController extends NBasicController {

    @Autowired
    private ModelSmartService modelSmartService;

    @Autowired
    private OptRecService optRecService;

    @Autowired
    private AclEvaluate aclEvaluate;

    @Autowired
    private ModelService modelService;

    @Autowired
    private FavoriteRuleService favoriteRuleService;

    @ApiOperation(value = "couldAnsweredByExistedModel", tags = { "AI" })
    @PostMapping(value = "/validation")
    @ResponseBody
    @WaitForSyncBeforeRPC
    public EnvelopeResponse<List<String>> couldAnsweredByExistedModel(@RequestBody FavoriteRequest request) {
        String projectName = checkProjectName(request.getProject());
        checkSqlIsNotNull(request.getSqls());
        request.setProject(projectName);
        AbstractContext proposeContext = modelSmartService.probeRecommendation(request.getProject(), request.getSqls());
        List<NDataModel> models = proposeContext.getProposedModels();
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                models.stream().map(NDataModel::getAlias).collect(Collectors.toList()), "");
    }

    @VisibleForTesting
    public OpenValidationResponse batchSqlValidate(String project, List<String> sqls) {
        Set<String> normalSqls = Sets.newHashSet();
        Set<String> errorSqls = Sets.newHashSet();
        Set<OpenValidationResponse.ErrorSqlDetail> errorSqlDetailSet = Sets.newHashSet();

        Map<String, SQLValidateResult> validatedSqls = favoriteRuleService.batchSqlValidate(sqls, project);
        validatedSqls.forEach((sql, validateResult) -> {
            if (validateResult.isCapable()) {
                normalSqls.add(sql);
            } else {
                errorSqls.add(sql);
                errorSqlDetailSet.add(new OpenValidationResponse.ErrorSqlDetail(sql, validateResult.getSqlAdvices()));
            }
        });

        AbstractContext proposeContext = modelSmartService.probeRecommendation(project, Lists.newArrayList(normalSqls));
        Map<String, NDataModel> uuidToModelMap = proposeContext.getRelatedModels().stream()
                .collect(Collectors.toMap(NDataModel::getUuid, Function.identity()));
        Map<String, Set<String>> answeredModelAlias = Maps.newHashMap();
        proposeContext.getAccelerateInfoMap().forEach((sql, accelerationInfo) -> {
            answeredModelAlias.putIfAbsent(sql, Sets.newHashSet());
            Set<AccelerateInfo.QueryLayoutRelation> relatedLayouts = accelerationInfo.getRelatedLayouts();
            if (CollectionUtils.isNotEmpty(relatedLayouts)) {
                relatedLayouts.forEach(info -> {
                    String alias = uuidToModelMap.get(info.getModelId()).getAlias();
                    answeredModelAlias.get(sql).add(alias);
                });
            }
        });
        Map<String, List<String>> validSqlToModels = Maps.newHashMap();
        answeredModelAlias.forEach((sql, aliasSet) -> validSqlToModels.put(sql, Lists.newArrayList(aliasSet)));
        return new OpenValidationResponse(validSqlToModels, Lists.newArrayList(errorSqls),
                Lists.newArrayList(errorSqlDetailSet));
    }

    @ApiOperation(value = "answeredByExistedModel", tags = { "AI" })
    @PostMapping(value = "/model_validation")
    @ResponseBody
    @WaitForSyncBeforeRPC
    public EnvelopeResponse<OpenValidationResponse> answeredByExistedModel(@RequestBody FavoriteRequest request) {
        String projectName = checkProjectName(request.getProject());
        request.setProject(projectName);
        aclEvaluate.checkProjectWritePermission(request.getProject());
        checkNotEmpty(request.getSqls());
        OpenValidationResponse response = batchSqlValidate(request.getProject(), request.getSqls());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "getRecommendations", tags = { "AI" })
    @GetMapping(value = "/{model_name:.+}/recommendations")
    @ResponseBody
    @WaitForSyncBeforeRPC
    public EnvelopeResponse<OpenOptRecLayoutsResponse> getRecommendations(
            @PathVariable(value = "model_name") String modelAlias, //
            @RequestParam(value = "project") String project, //
            @RequestParam(value = "recActionType", required = false, defaultValue = "all") String recActionType) {
        String projectName = checkProjectName(project);
        checkProjectNotSemiAuto(projectName);
        aclEvaluate.checkProjectOperationDesignPermission(projectName);
        aclEvaluate.checkProjectReadPermission(projectName);
        String modelId = modelService.getModel(modelAlias, projectName).getId();
        checkRequiredArg("modelId", modelId);
        OptRecLayoutsResponse response = optRecService.getOptRecLayoutsResponse(projectName, modelId, recActionType);
        //open api not add recDetailResponse
        response.getLayouts().forEach(optRecLayoutResponse -> optRecLayoutResponse.setRecDetailResponse(null));
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                new OpenOptRecLayoutsResponse(projectName, modelId, response), "");
    }
}

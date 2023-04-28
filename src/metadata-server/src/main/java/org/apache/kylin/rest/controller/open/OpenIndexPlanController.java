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
import static org.apache.kylin.common.exception.code.ErrorCodeServer.MODEL_NOT_EXIST;

import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.rest.controller.NBasicController;
import org.apache.kylin.rest.controller.NIndexPlanController;
import org.apache.kylin.rest.request.OpenUpdateRuleBasedCuboidRequest;
import org.apache.kylin.rest.request.UpdateRuleBasedCuboidRequest;
import org.apache.kylin.rest.response.DiffRuleBasedIndexResponse;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.OpenAddAggGroupResponse;
import org.apache.kylin.rest.service.FusionIndexService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.annotations.ApiOperation;
import lombok.val;

@RestController
@RequestMapping(value = "/api/index_plans", produces = { HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class OpenIndexPlanController extends NBasicController {

    private static final String MODEL_ALIAS = "model";

    private static final String MODEL_NAME = "model_name";

    private static final String AGGREGATION_GROUPS = "aggregation_groups";

    @Autowired
    @Qualifier("fusionIndexService")
    private FusionIndexService fusionIndexService;

    @Autowired
    NIndexPlanController indexPlanController;

    @PutMapping(value = "/agg_groups")
    public EnvelopeResponse<OpenAddAggGroupResponse> updateRule(@RequestBody OpenUpdateRuleBasedCuboidRequest request) {
        String projectName = checkProjectName(request.getProject());
        checkRequiredArg(MODEL_ALIAS, request.getModelAlias());
        checkListRequiredArg(AGGREGATION_GROUPS, request.getAggregationGroups());
        request.setProject(projectName);
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), projectName);
        NDataModel model = modelManager.getDataModelDescByAlias(request.getModelAlias());
        if (model == null) {
            throw new KylinException(MODEL_NOT_EXIST);
        }
        UpdateRuleBasedCuboidRequest internalRequest = fusionIndexService.convertOpenToInternal(request, model);
        EnvelopeResponse<DiffRuleBasedIndexResponse> response = indexPlanController
                .calculateDiffRuleBasedIndex(internalRequest);
        indexPlanController.updateRule(internalRequest);
        return convertResponse(response);
    }

    @ApiOperation(value = "batch deleteIndex", tags = { "AI" })
    @DeleteMapping(value = "/index")
    public EnvelopeResponse<String> batchDeleteIndex(@RequestParam(value = "index_ids") Set<Long> layoutIds,
            @RequestParam(value = "project") String project, @RequestParam(value = "model_name") String modelName,
            @RequestParam(value = "index_range", required = false) IndexEntity.Range indexRange) {
        checkProjectName(project);
        checkRequiredArg(MODEL_NAME, modelName);
        checkCollectionRequiredArg("index_ids", layoutIds);
        if (null == indexRange) {
            indexRange = IndexEntity.Range.BATCH;
        }
        NDataModel dataModel = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .getDataModelDescByAlias(modelName);
        if (null == dataModel) {
            throw new KylinException(MODEL_NOT_EXIST);
        }
        fusionIndexService.batchRemoveIndex(project, dataModel.getUuid(), layoutIds, indexRange);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    private EnvelopeResponse<OpenAddAggGroupResponse> convertResponse(
            EnvelopeResponse<DiffRuleBasedIndexResponse> internal) {
        if (internal != null && internal.getData() != null) {
            val response = new OpenAddAggGroupResponse(internal.getData().getDecreaseLayouts(),
                    internal.getData().getIncreaseLayouts(), internal.getData().getRollbackLayouts());
            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
        }
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, null, "");
    }
}

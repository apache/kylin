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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.rest.controller.NBasicController;
import org.apache.kylin.rest.controller.NIndexPlanController;
import org.apache.kylin.rest.request.OpenUpdateRuleBasedCuboidRequest;
import org.apache.kylin.rest.request.UpdateRuleBasedCuboidRequest;
import org.apache.kylin.rest.response.DiffRuleBasedIndexResponse;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.service.FusionIndexService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import lombok.val;

@RestController
@RequestMapping(value = "/api/index_plans", produces = { HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class OpenIndexPlanController extends NBasicController {

    private static final String MODEL_ALIAS = "model";

    @Autowired
    @Qualifier("fusionIndexService")
    private FusionIndexService fusionIndexService;

    @Autowired
    NIndexPlanController indexPlanController;

    @PutMapping(value = "/agg_groups")
    public EnvelopeResponse<DiffRuleBasedIndexResponse> updateRule(
            @RequestBody OpenUpdateRuleBasedCuboidRequest request) {
        String projectName = checkProjectName(request.getProject());
        checkRequiredArg(MODEL_ALIAS, request.getModelAlias());
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
        return response;
    }
}

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
package org.apache.kylin.rest.controller.v2;

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V2_JSON;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.MODEL_ID_NOT_EXIST;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.rest.controller.NBasicController;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.service.ModelService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;

import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping(value = "/api/models")
public class NModelControllerV2 extends NBasicController {

    @Autowired
    @Qualifier("modelService")
    private ModelService modelService;

    @ApiOperation(value = "getModels", tags = { "AI" })
    @GetMapping(value = "", produces = { HTTP_VND_APACHE_KYLIN_V2_JSON })
    @ResponseBody
    public EnvelopeResponse<Map<String, Object>> getModels(
            @RequestParam(value = "modelName", required = false) String modelAlias,
            @RequestParam(value = "exactMatch", required = false, defaultValue = "true") boolean exactMatch,
            @RequestParam(value = "projectName") String project,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer limit,
            @RequestParam(value = "sortBy", required = false, defaultValue = "last_modify") String sortBy,
            @RequestParam(value = "reverse", required = false, defaultValue = "true") Boolean reverse) {
        checkProjectName(project);
        List<NDataModel> models = new ArrayList<>(
                modelService.getModels(modelAlias, project, exactMatch, null, Lists.newArrayList(), sortBy, reverse));
        models = modelService.addOldParams(project, models);

        Map<String, Object> modelResponse = getDataResponse("models", models, offset, limit);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, modelResponse, "");
    }

    @ApiOperation(value = "getModelDesc", tags = { "AI" })
    @GetMapping(value = "/{projectName}/{modelName}", produces = { HTTP_VND_APACHE_KYLIN_V2_JSON })
    @ResponseBody
    public EnvelopeResponse<Map<String, Object>> getModelDesc(@PathVariable("projectName") String project,
            @PathVariable("modelName") String modelAlias) {
        checkProjectName(project);
        List<NDataModel> models = new ArrayList<>(
                modelService.getModels(modelAlias, project, true, null, Lists.newArrayList(), "last_modify", true));
        if (models.size() == 0) {
            throw new KylinException(MODEL_ID_NOT_EXIST, modelAlias);
        }
        models = modelService.addOldParams(project, models);

        HashMap<String, Object> modelResponse = new HashMap<>();
        modelResponse.put("model", models.size() == 0 ? Maps.newHashMap() : models.get(0));
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, modelResponse, "");
    }
}

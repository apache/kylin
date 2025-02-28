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

import java.util.Map;

import javax.validation.Valid;

import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.rest.controller.NBasicController;
import org.apache.kylin.rest.request.PrepareSqlRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.response.SQLResponseV2;
import org.apache.kylin.rest.service.QueryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.annotations.ApiOperation;

@RestController
@RequestMapping(value = "/api/query")
public class NQueryControllerV2 extends NBasicController {

    @Autowired
    @Qualifier("queryService")
    private QueryService queryService;

    @Deprecated
    @ApiOperation(value = "query4JDBC", tags = { "QE" })
    @PostMapping(value = "", produces = { "application/json" })
    @ResponseBody
    public SQLResponse query4JDBC(@Valid @RequestBody PrepareSqlRequest sqlRequest) {
        String projectName = checkProjectName(sqlRequest.getProject());
        sqlRequest.setProject(projectName);
        return queryService.queryWithCache(sqlRequest);
    }

    @ApiOperation(value = "query", tags = { "QE" })
    @PostMapping(value = "", produces = { HTTP_VND_APACHE_KYLIN_V2_JSON })
    @ResponseBody
    public EnvelopeResponse<SQLResponseV2> query(@Valid @RequestBody PrepareSqlRequest sqlRequest) {
        String projectName = checkProjectName(sqlRequest.getProject());
        sqlRequest.setProject(projectName);
        SQLResponse sqlResponse = queryService.queryWithCache(sqlRequest);
        SQLResponseV2 sqlResponseV2 = null == sqlResponse ? null : new SQLResponseV2(sqlResponse);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, sqlResponseV2, "");
    }

    @ApiOperation(value = "prepareQuery", tags = { "QE" })
    @PostMapping(value = "/prestate", produces = { HTTP_VND_APACHE_KYLIN_V2_JSON })
    @ResponseBody
    public EnvelopeResponse<SQLResponse> prepareQuery(@Valid @RequestBody PrepareSqlRequest sqlRequest) {
        String projectName = checkProjectName(sqlRequest.getProject());
        sqlRequest.setProject(projectName);
        Map<String, String> newToggles = Maps.newHashMap();
        if (sqlRequest.getBackdoorToggles() != null)
            newToggles.putAll(sqlRequest.getBackdoorToggles());
        newToggles.put(BackdoorToggles.DEBUG_TOGGLE_PREPARE_ONLY, "true");
        sqlRequest.setBackdoorToggles(newToggles);

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, queryService.queryWithCache(sqlRequest), "");
    }
}

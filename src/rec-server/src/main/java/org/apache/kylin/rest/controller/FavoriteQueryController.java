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
import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

import org.apache.kylin.common.constant.LogConstant;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.rest.request.SQLValidateRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.SQLParserResponse;
import org.apache.kylin.rest.response.SQLValidateResponse;
import org.apache.kylin.rest.service.FavoriteRuleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import io.swagger.annotations.ApiOperation;

@RestController
@RequestMapping(value = "/api/query/favorite_queries", produces = { HTTP_VND_APACHE_KYLIN_JSON,
        HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class FavoriteQueryController extends NBasicController {
    private static final Logger logger = LoggerFactory.getLogger(LogConstant.QUERY_CATEGORY);
    @Autowired
    private FavoriteRuleService favoriteRuleService;

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @ApiOperation(value = "importSqls (response)", tags = { "AI" }, notes = "sql_advices")
    @PostMapping(value = "/sql_files")
    @ResponseBody
    public EnvelopeResponse<SQLParserResponse> importSqls(@RequestParam("project") String project,
            @RequestParam("files") MultipartFile[] files) {
        checkProjectName(project);
        checkProjectUnmodifiable(project);
        SQLParserResponse data = favoriteRuleService.importSqls(files, project);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, data, "");
    }

    @ApiOperation(value = "sqlValidate", tags = { "AI" }, notes = "Update Response: incapable_reason, sql_advices")
    @PutMapping(value = "/sql_validation")
    @ResponseBody
    public EnvelopeResponse<SQLValidateResponse> sqlValidate(@RequestBody SQLValidateRequest request) {
        checkProjectName(request.getProject());
        checkProjectUnmodifiable(request.getProject());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                favoriteRuleService.sqlValidate(request.getProject(), request.getSql()), "");
    }
}

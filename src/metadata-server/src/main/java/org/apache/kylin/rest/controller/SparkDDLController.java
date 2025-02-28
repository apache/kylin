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

import java.util.List;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.rest.request.ViewRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.LogicalViewResponse;
import org.apache.kylin.rest.service.SparkDDLService;
import org.apache.spark.sql.LogicalViewLoader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping(value = "/api/spark_source", produces = { HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON,
        HTTP_VND_APACHE_KYLIN_JSON })
@Slf4j
public class SparkDDLController extends NBasicController {

    @Autowired
    private SparkDDLService sparkDDLService;

    @ApiOperation(value = "ddl")
    @PostMapping(value = "/ddl")
    @ResponseBody
    public EnvelopeResponse<String> executeSQL(@RequestBody ViewRequest request) {
        String project = checkProjectName(request.getDdlProject());
        request.setDdlProject(project);
        String result = sparkDDLService.executeSQL(request);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, result, "");
    }

    @ApiOperation(value = "ddl_description")
    @GetMapping(value = "/ddl/description")
    @ResponseBody
    public EnvelopeResponse<List<List<String>>> description(@RequestParam("project") String project,
            @RequestParam("page_type") String pageType) {
        project = checkProjectName(project);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                sparkDDLService.pluginsDescription(project, pageType), "");
    }

    @ApiOperation(value = "ddl_sync")
    @GetMapping(value = "/ddl/sync")
    @ResponseBody
    public EnvelopeResponse<String> sync() {
        LogicalViewLoader.syncViewFromDB();
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "ddl_desc")
    @GetMapping(value = "/ddl/view_list")
    @ResponseBody
    public EnvelopeResponse<List<LogicalViewResponse>> list(@RequestParam("project") String project,
            @RequestParam(value = "table", required = false, defaultValue = "") String tableName) {
        project = checkProjectName(project);
        List<LogicalViewResponse> logicalViews = sparkDDLService.listAll(project, tableName);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, logicalViews, "");
    }
}

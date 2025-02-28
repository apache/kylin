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
import static org.apache.kylin.common.exception.code.ErrorCodeServer.PARAMETER_INVALID_SUPPORT_LIST;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.rest.controller.NBasicController;
import org.apache.kylin.rest.controller.SparkDDLController;
import org.apache.kylin.rest.request.OpenLogicalViewRequest;
import org.apache.kylin.rest.request.ViewRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.spark.ddl.DDLConstant;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.annotations.ApiOperation;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
@RequestMapping(value = "/api/logical_view", produces = { HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class OpenLogicalViewController extends NBasicController {
    @Autowired
    private SparkDDLController sparkDDLController;

    @ApiOperation(value = "ddl", tags = "LogicalView", notes = "create/replace/drop logical view")
    @PostMapping(value = "/ddl")
    @ResponseBody
    public EnvelopeResponse<String> executeSql(@RequestBody OpenLogicalViewRequest request) {
        val project = checkProjectName(request.getProject());
        checkRequiredArg("sql", request.getSql());
        if (!DDLConstant.LOGICAL_VIEW.equals(request.getRestrict())
                && !DDLConstant.REPLACE_LOGICAL_VIEW.equals(request.getRestrict())) {
            throw new KylinException(PARAMETER_INVALID_SUPPORT_LIST, "restrict",
                    DDLConstant.LOGICAL_VIEW + ", " + DDLConstant.REPLACE_LOGICAL_VIEW);
        }
        val viewRequest = new ViewRequest(project, request.getSql(), request.getRestrict());
        return sparkDDLController.executeSQL(viewRequest);
    }
}

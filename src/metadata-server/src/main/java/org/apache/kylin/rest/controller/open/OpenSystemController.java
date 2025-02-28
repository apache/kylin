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

import java.util.Map;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.rest.controller.NBasicController;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.service.SystemService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping(value = "/api", produces = { HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class OpenSystemController extends NBasicController {

    @Autowired
    private SystemService systemService;

    @ApiOperation(value = "getConfig")
    @GetMapping(value = "/config")
    @ResponseBody
    public EnvelopeResponse<Map<String, String>> getConfig(
            @RequestParam(value = "project", required = false) String project,
            @RequestParam(value = "model", required = false) String model) {
        Map<String, String> config = systemService.getReadOnlyConfig(project, model);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, config, "");
    }
}

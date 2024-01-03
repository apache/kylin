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

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.request.EpochRequest;
import org.apache.kylin.rest.service.EpochService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import io.swagger.annotations.ApiOperation;

import java.util.ArrayList;
import java.util.Objects;

@Controller
@RequestMapping(value = "/api/epoch", produces = { HTTP_VND_APACHE_KYLIN_JSON })
public class NEpochController extends NBasicController {

    @Autowired
    @Qualifier("epochService")
    private EpochService epochService;

    @ApiOperation(value = "change epoch", tags = { "DW" })
    @PostMapping(value = "", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<String> updateEpochOwner(@RequestBody EpochRequest epochRequest) {
        if (Objects.isNull(epochRequest.getProjects())) {
            // Avoid following NPEs.
            epochRequest.setProjects(new ArrayList<>(0));
        }
        // Empty projects has specified meanings: all projects do change epoch.
        epochRequest.getProjects().forEach(this::checkProjectName);
        checkRequiredArg("force", epochRequest.getForce());
        epochService.updateEpoch(epochRequest.getProjects(), epochRequest.getForce(), epochRequest.isClient());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "change all epoch", tags = { "DW" })
    @PostMapping(value = "/all", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<String> updateAllEpochOwner(@RequestBody EpochRequest epochRequest) {
        checkRequiredArg("force", epochRequest.getForce());
        epochService.updateAllEpochs(epochRequest.getForce(), epochRequest.isClient());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @GetMapping(value = "/maintenance_mode")
    @ResponseBody
    public EnvelopeResponse<Boolean> isMaintenanceMode() {
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, epochService.isMaintenanceMode(), "");
    }
}

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

package org.apache.kylin.rest.controller2;

import java.io.IOException;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.request.MetricsRequest;
import org.apache.kylin.rest.request.UpdateConfigRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.AdminService;
import org.apache.kylin.rest.service.CubeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * Admin Controller is defined as Restful API entrance for UI.
 * 
 * @author jianliu
 * 
 */
@Controller
@RequestMapping(value = "/admin")
public class AdminControllerV2 extends BasicController {

    @Autowired
    @Qualifier("adminService")
    private AdminService adminService;

    @Autowired
    @Qualifier("cubeMgmtService")
    private CubeService cubeMgmtService;

    @RequestMapping(value = "/env", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getEnvV2(@RequestHeader("Accept-Language") String lang) throws ConfigurationException {
        MsgPicker.setMsg(lang);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, adminService.getEnv(), "");
    }

    @RequestMapping(value = "/config", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getConfigV2(@RequestHeader("Accept-Language") String lang) throws IOException {
        MsgPicker.setMsg(lang);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, adminService.getConfigAsString(), "");
    }

    @RequestMapping(value = "/metrics/cubes", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse cubeMetricsV2(@RequestHeader("Accept-Language") String lang, MetricsRequest request) {
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, cubeMgmtService.calculateMetrics(request), "");
    }

    @RequestMapping(value = "/storage", method = { RequestMethod.DELETE }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public void cleanupStorageV2(@RequestHeader("Accept-Language") String lang) {
        MsgPicker.setMsg(lang);

        adminService.cleanupStorage();
    }

    @RequestMapping(value = "/config", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public void updateKylinConfigV2(@RequestHeader("Accept-Language") String lang,
            @RequestBody UpdateConfigRequest updateConfigRequest) {
        MsgPicker.setMsg(lang);

        KylinConfig.getInstanceFromEnv().setProperty(updateConfigRequest.getKey(), updateConfigRequest.getValue());
    }

    public void setAdminService(AdminService adminService) {
        this.adminService = adminService;
    }

    public void setCubeMgmtService(CubeService cubeMgmtService) {
        this.cubeMgmtService = cubeMgmtService;
    }

}

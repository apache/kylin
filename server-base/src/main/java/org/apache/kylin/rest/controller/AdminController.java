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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.request.MetricsRequest;
import org.apache.kylin.rest.request.UpdateConfigRequest;
import org.apache.kylin.rest.response.GeneralResponse;
import org.apache.kylin.rest.response.MetricsResponse;
import org.apache.kylin.rest.service.AdminService;
import org.apache.kylin.rest.service.CubeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
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
public class AdminController extends BasicController {

    @Autowired
    private AdminService adminService;
    @Autowired
    private CubeService cubeMgmtService;

    @RequestMapping(value = "/env", method = { RequestMethod.GET })
    @ResponseBody
    public GeneralResponse getEnv() {
        String env = adminService.getEnv();

        GeneralResponse envRes = new GeneralResponse();
        envRes.put("env", env);

        return envRes;
    }

    @RequestMapping(value = "/config", method = { RequestMethod.GET })
    @ResponseBody
    public GeneralResponse getConfig() {
        String config = adminService.getConfigAsString();

        GeneralResponse configRes = new GeneralResponse();
        configRes.put("config", config);

        return configRes;
    }

    @RequestMapping(value = "/metrics/cubes", method = { RequestMethod.GET })
    @ResponseBody
    public MetricsResponse cubeMetrics(MetricsRequest request) {
        return cubeMgmtService.calculateMetrics(request);
    }

    @RequestMapping(value = "/storage", method = { RequestMethod.DELETE })
    @ResponseBody
    public void cleanupStorage() {
        adminService.cleanupStorage();
    }

    @RequestMapping(value = "/config", method = { RequestMethod.PUT })
    public void updateKylinConfig(@RequestBody UpdateConfigRequest updateConfigRequest) {
        KylinConfig.getInstanceFromEnv().setProperty(updateConfigRequest.getKey(), updateConfigRequest.getValue());
    }

    public void setAdminService(AdminService adminService) {
        this.adminService = adminService;
    }

    public void setCubeMgmtService(CubeService cubeMgmtService) {
        this.cubeMgmtService = cubeMgmtService;
    }

}

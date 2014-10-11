/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kylinolap.rest.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.rest.request.MetricsRequest;
import com.kylinolap.rest.request.UpdateConfigRequest;
import com.kylinolap.rest.response.GeneralResponse;
import com.kylinolap.rest.response.MetricsResponse;
import com.kylinolap.rest.service.AdminService;
import com.kylinolap.rest.service.CubeService;
import com.kylinolap.rest.service.UserService;

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
    @Autowired
    private UserService userService;

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

    @RequestMapping(value = "/metrics/user", method = { RequestMethod.GET })
    @ResponseBody
    public MetricsResponse userMetrics(MetricsRequest request) {
        return userService.calculateMetrics(request);
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

    public void setUserService(UserService userService) {
        this.userService = userService;
    }

}

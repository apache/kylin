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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinVersion;
import org.apache.kylin.common.util.VersionUtil;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.request.MetricsRequest;
import org.apache.kylin.rest.request.UpdateConfigRequest;
import org.apache.kylin.rest.response.GeneralResponse;
import org.apache.kylin.rest.response.MetricsResponse;
import org.apache.kylin.rest.service.AdminService;
import org.apache.kylin.rest.service.CubeService;
import org.apache.spark.sql.SparderContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * Admin Controller is defined as Restful API entrance for UI.
 * 
 */
@Controller
@RequestMapping(value = "/admin")
public class AdminController extends BasicController {

    private Set<String> propertiesWhiteList = new HashSet<>();

    @Autowired
    @Qualifier("adminService")
    private AdminService adminService;

    @Autowired
    @Qualifier("cubeMgmtService")
    private CubeService cubeMgmtService;

    @RequestMapping(value = "/env", method = { RequestMethod.GET }, produces = { "application/json" })
    @ResponseBody
    public GeneralResponse getEnv() {
        Message msg = MsgPicker.getMsg();
        try {
            String env = adminService.getEnv();

            GeneralResponse envRes = new GeneralResponse();
            envRes.put("env", env);

            return envRes;
        } catch (ConfigurationException | UnsupportedEncodingException e) {
            throw new RuntimeException(msg.getGET_ENV_CONFIG_FAIL(), e);
        }
    }

    @RequestMapping(value = "/version", method = { RequestMethod.GET }, produces = { "application/json" })
    @ResponseBody
    public GeneralResponse getKylinVersions() {
        try {
            GeneralResponse versionRes = new GeneralResponse();
            String commitId = KylinVersion.getGitCommitInfo();
            versionRes.put("kylin.version", VersionUtil.getKylinVersion());
            versionRes.put("kylin.version.commitId", commitId.replace(";", ""));
            return versionRes;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @RequestMapping(value = "/config", method = { RequestMethod.GET }, produces = { "application/json" })
    @ResponseBody
    public GeneralResponse getConfig() throws IOException {
        String config = KylinConfig.getInstanceFromEnv().exportAllToString();
        GeneralResponse configRes = new GeneralResponse();
        configRes.put("config", config);

        return configRes;
    }

    @RequestMapping(value = "/public_config", method = { RequestMethod.GET }, produces = { "application/json" })
    @ResponseBody
    public GeneralResponse getPublicConfig() throws IOException {
        final String config = adminService.getPublicConfig();
        GeneralResponse configRes = new GeneralResponse();
        configRes.put("config", config);
        return configRes;
    }

    @RequestMapping(value = "/sparder_url", method = { RequestMethod.GET }, produces = { "application/json" })
    @ResponseBody
    public GeneralResponse getSparderUrl() throws IOException {
        GeneralResponse configRes = new GeneralResponse();
        configRes.put("url", SparderContext.appMasterTrackURL());
        return configRes;
    }

    @RequestMapping(value = "/metrics/cubes", method = { RequestMethod.GET }, produces = { "application/json" })
    @ResponseBody
    public MetricsResponse cubeMetrics(MetricsRequest request) {
        return cubeMgmtService.calculateMetrics(request);
    }

    @RequestMapping(value = "/storage", method = { RequestMethod.DELETE }, produces = { "application/json" })
    @ResponseBody
    public void cleanupStorage() {
        adminService.cleanupStorage();
    }

    @RequestMapping(value = "/config", method = {RequestMethod.PUT}, produces = {"application/json"})
    public void updateKylinConfig(@RequestBody UpdateConfigRequest updateConfigRequest) {
        if (propertiesWhiteList.isEmpty()) {
            propertiesWhiteList.addAll(Arrays.asList(KylinConfig.getInstanceFromEnv().getPropertiesWhiteListForModification().split(",")));
        }
        if (!adminService.configWritableStatus() && !propertiesWhiteList.contains(updateConfigRequest.getKey())) {
            throw new BadRequestException("Update configuration from API is not allowed.");
        }
        adminService.updateConfig(updateConfigRequest.getKey(), updateConfigRequest.getValue());
    }

    public void setAdminService(AdminService adminService) {
        this.adminService = adminService;
    }

    public void setCubeMgmtService(CubeService cubeMgmtService) {
        this.cubeMgmtService = cubeMgmtService;
    }

}

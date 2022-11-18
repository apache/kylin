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

import java.util.List;

import org.apache.kylin.common.response.MetricsResponse;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.query.exception.UnsupportedQueryException;
import org.apache.kylin.rest.response.NDataModelResponse;
import org.apache.kylin.rest.service.DashboardService;
import org.apache.kylin.rest.service.ModelService;
import org.apache.kylin.rest.service.QueryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping(value = "/api/dashboard")
public class DashboardController extends NBasicController {
    public static final Logger logger = LoggerFactory.getLogger(DashboardController.class);

    @Autowired
    DashboardService dashboardService;

    @Autowired
    ModelService modelService;

    @Autowired
    QueryService queryService;


    @RequestMapping(value = "/metric/model", method = { RequestMethod.GET })
    @ResponseBody
    public MetricsResponse getCubeMetrics(@RequestParam(value = "projectName") String projectName,
            @RequestParam(value = "modelName", required = false) String modelName) {
        checkAuthorization(projectName);
        return dashboardService.getModelMetrics(projectName, modelName);
    }

    @RequestMapping(value = "/metric/query", method = RequestMethod.GET)
    @ResponseBody
    public MetricsResponse getQueryMetrics(@RequestParam(value = "projectName", required = false) String projectName,
            @RequestParam(value = "modelName", required = false) String cubeName,
            @RequestParam(value = "startTime") String startTime, @RequestParam(value = "endTime") String endTime) {
        checkAuthorization(projectName);
        return dashboardService.getQueryMetrics(projectName, startTime, endTime);
    }

    @RequestMapping(value = "/metric/job", method = RequestMethod.GET)
    @ResponseBody
    public MetricsResponse getJobMetrics(@RequestParam(value = "projectName", required = false) String projectName,
            @RequestParam(value = "modelName", required = false) String cubeName,
            @RequestParam(value = "startTime") String startTime, @RequestParam(value = "endTime") String endTime) {
        checkAuthorization(projectName);
        return dashboardService.getJobMetrics(projectName, startTime, endTime);
    }

    @RequestMapping(value = "/chart/{category}/{metric}/{dimension}", method = RequestMethod.GET)
    @ResponseBody
    public MetricsResponse getChartData(@PathVariable String dimension, @PathVariable String metric,
            @PathVariable String category, @RequestParam(value = "projectName", required = false) String projectName,
            @RequestParam(value = "modelName", required = false) String cubeName,
            @RequestParam(value = "startTime") String startTime, @RequestParam(value = "endTime") String endTime) {
        checkAuthorization(projectName);

        try {
            return dashboardService.getChartData(category, projectName, startTime, endTime, dimension, metric);
        } catch (Exception e) {
            throw new UnsupportedQueryException("Category or Metric is not right: { " + e.getMessage() + " }");
        }
    }

    private void checkAuthorization(String projectName) {
        if (projectName != null && !projectName.isEmpty()) {
            ProjectInstance project = dashboardService.getProjectManager().getProject(projectName);
            try {
                dashboardService.checkAuthorization(project);
            } catch (AccessDeniedException e) {
                List<NDataModelResponse> models = modelService.getCubes0(null, projectName);
                if (models.isEmpty()) {
                    throw new AccessDeniedException("Access is denied");
                }
            }
        } else {
            dashboardService.checkAuthorization();
        }
    }
}

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

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.request.PrepareSqlRequest;
import org.apache.kylin.rest.response.MetricsResponse;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.service.CubeService;
import org.apache.kylin.rest.service.DashboardService;
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
@RequestMapping(value = "/dashboard")
public class DashboardController extends BasicController {
    private static final Logger logger = LoggerFactory.getLogger(DashboardController.class);

    @Autowired
    private DashboardService dashboardService;

    @Autowired
    private QueryService queryService;

    @Autowired
    private CubeService cubeService;

    @RequestMapping(value = "/metric/cube", method = { RequestMethod.GET })
    @ResponseBody
    public MetricsResponse getCubeMetrics(@RequestParam(value = "projectName", required = false) String projectName,
            @RequestParam(value = "cubeName", required = false) String cubeName) {
        checkAuthorization(projectName);
        return dashboardService.getCubeMetrics(projectName, cubeName);
    }

    @RequestMapping(value = "/metric/query", method = RequestMethod.GET)
    @ResponseBody
    public MetricsResponse getQueryMetrics(@RequestParam(value = "projectName", required = false) String projectName,
            @RequestParam(value = "cubeName", required = false) String cubeName,
            @RequestParam(value = "startTime") String startTime, @RequestParam(value = "endTime") String endTime) {
        checkAuthorization(projectName);
        MetricsResponse queryMetrics = new MetricsResponse();
        PrepareSqlRequest sqlRequest = dashboardService.getQueryMetricsSQLRequest(startTime, endTime, projectName,
                cubeName);
        SQLResponse sqlResponse = queryService.doQueryWithCache(sqlRequest);
        if (!sqlResponse.getIsException()) {
            queryMetrics.increase("queryCount",
                    dashboardService.getMetricValue(sqlResponse.getResults().get(0).get(0)));
            queryMetrics.increase("avgQueryLatency",
                    dashboardService.getMetricValue(sqlResponse.getResults().get(0).get(1)));
            queryMetrics.increase("maxQueryLatency",
                    dashboardService.getMetricValue(sqlResponse.getResults().get(0).get(2)));
            queryMetrics.increase("minQueryLatency",
                    dashboardService.getMetricValue(sqlResponse.getResults().get(0).get(3)));
        }
        return queryMetrics;
    }

    @RequestMapping(value = "/metric/job", method = RequestMethod.GET)
    @ResponseBody
    public MetricsResponse getJobMetrics(@RequestParam(value = "projectName", required = false) String projectName,
            @RequestParam(value = "cubeName", required = false) String cubeName,
            @RequestParam(value = "startTime") String startTime, @RequestParam(value = "endTime") String endTime) {
        checkAuthorization(projectName);
        MetricsResponse jobMetrics = new MetricsResponse();
        PrepareSqlRequest sqlRequest = dashboardService.getJobMetricsSQLRequest(startTime, endTime, projectName,
                cubeName);
        SQLResponse sqlResponse = queryService.doQueryWithCache(sqlRequest);
        if (!sqlResponse.getIsException()) {
            jobMetrics.increase("jobCount", dashboardService.getMetricValue(sqlResponse.getResults().get(0).get(0)));
            jobMetrics.increase("avgJobBuildTime",
                    dashboardService.getMetricValue(sqlResponse.getResults().get(0).get(1)));
            jobMetrics.increase("maxJobBuildTime",
                    dashboardService.getMetricValue(sqlResponse.getResults().get(0).get(2)));
            jobMetrics.increase("minJobBuildTime",
                    dashboardService.getMetricValue(sqlResponse.getResults().get(0).get(3)));
        }
        return jobMetrics;
    }

    @RequestMapping(value = "/chart/{category}/{metric}/{dimension}", method = RequestMethod.GET)
    @ResponseBody
    public MetricsResponse getChartData(@PathVariable String dimension, @PathVariable String metric,
            @PathVariable String category, @RequestParam(value = "projectName", required = false) String projectName,
            @RequestParam(value = "cubeName", required = false) String cubeName,
            @RequestParam(value = "startTime") String startTime, @RequestParam(value = "endTime") String endTime) {
        checkAuthorization(projectName);
        PrepareSqlRequest sqlRequest = dashboardService.getChartSQLRequest(startTime, endTime, projectName, cubeName,
                dimension, metric, category);
        return dashboardService.transformChartData(queryService.doQueryWithCache(sqlRequest));
    }

    private void checkAuthorization(String projectName) {
        if (projectName != null && !projectName.isEmpty()) {
            ProjectInstance project = dashboardService.getProjectManager().getProject(projectName);
            try {
                dashboardService.checkAuthorization(project);
            } catch (AccessDeniedException e) {
                List<CubeInstance> cubes = cubeService.listAllCubes(null, projectName, null, true);
                if (cubes.isEmpty()) {
                    throw new AccessDeniedException("Access is denied");
                }
            }
        } else {
            dashboardService.checkAuthorization();
        }
    }
}
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
import static org.apache.kylin.common.exception.code.ErrorCodeServer.TIME_INVALID_RANGE_END_LESS_THAN_START;

import java.util.List;

import javax.validation.Valid;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.rest.request.AlertMessageRequest;
import org.apache.kylin.rest.response.ClusterStatisticStatusResponse;
import org.apache.kylin.rest.response.ClusterStatusResponse;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ExecutorMemoryResponse;
import org.apache.kylin.rest.response.ExecutorThreadInfoResponse;
import org.apache.kylin.rest.service.MonitorService;
import org.apache.kylin.shaded.influxdb.org.influxdb.InfluxDBIOException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping(value = "/api/monitor", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class NMonitorController extends NBasicController {
    @Autowired
    @Qualifier("monitorService")
    private MonitorService monitorService;

    @Deprecated
    @ApiOperation(value = "getMemoryMetrics", tags = { "SM" }, notes = "Update URL: memory_info")
    @GetMapping(value = "/memory_info")
    @ResponseBody
    public EnvelopeResponse<List<ExecutorMemoryResponse>> getMemoryMetrics() {
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, Lists.newArrayList(), "");
    }

    @Deprecated
    @ApiOperation(value = "getThreadInfoMetrics", tags = { "SM" }, notes = "Update URL: thread_info")
    @GetMapping(value = "/thread_info")
    @ResponseBody
    public EnvelopeResponse<List<ExecutorThreadInfoResponse>> getThreadInfoMetrics() {
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, Lists.newArrayList(), "");
    }

    @ApiOperation(value = "getStatus", tags = { "SM" })
    @GetMapping(value = "/status")
    @ResponseBody
    public EnvelopeResponse<ClusterStatusResponse> getClusterCurrentStatus() {
        ClusterStatusResponse result;
        try {
            result = monitorService.currentClusterStatus();
        } catch (InfluxDBIOException ie) {
            throw new RuntimeException("Failed to connect InfluxDB service. Please check its status and the network.");
        }

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, result, "");
    }

    @ApiOperation(value = "getStatusStatistics", tags = { "SM" })
    @GetMapping(value = "/status/statistics")
    @ResponseBody
    public EnvelopeResponse<ClusterStatisticStatusResponse> getClusterStatisticStatus(
            @RequestParam(value = "start") long start, @RequestParam(value = "end") long end) {
        long now = System.currentTimeMillis();
        end = end > now ? now : end;
        if (start > end) {
            throw new KylinException(TIME_INVALID_RANGE_END_LESS_THAN_START, String.valueOf(start),
                    String.valueOf(end));
        }

        ClusterStatisticStatusResponse result;
        try {
            result = monitorService.statisticClusterByFloorTime(start, end);
        } catch (InfluxDBIOException ie) {
            throw new RuntimeException("Failed to connect InfluxDB service. Please check its status and the network.");
        }

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, result, "");
    }

    @ApiOperation(value = "alert", tags = { "SM" })
    @PostMapping(value = "/alert")
    @ResponseBody
    public void alert(@RequestBody @Valid AlertMessageRequest alertMessageRequest) {
        monitorService.handleAlertMessage(alertMessageRequest);
    }
}

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

import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.rest.request.SnapshotSourceTableStatsRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.SnapshotSourceTableStatsResponse;
import org.apache.kylin.rest.service.SnapshotSourceTableStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

@Slf4j
@RestController
@RequestMapping(value = "/api/snapshots", produces = { HTTP_VND_APACHE_KYLIN_JSON,
        HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class SnapshotSourceTableStatsController extends NBasicController {
    @Autowired
    @Qualifier("snapshotSourceTableStatsService")
    private SnapshotSourceTableStatsService snapshotSourceTableStatsService;

    @ApiOperation(value = "check source table stats", tags = {"Apollo" })
    @PostMapping(value = "source_table_stats")
    @ResponseBody
    public EnvelopeResponse<SnapshotSourceTableStatsResponse> sourceTableStats(
            @Valid @RequestBody SnapshotSourceTableStatsRequest request) {
        log.debug("sourceTableStats request : {}", request);
        val project = checkProjectName(request.getProject());
        val response = snapshotSourceTableStatsService.checkSourceTableStats(project, request.getDatabase(),
                request.getTable(), request.getSnapshotPartitionCol());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "save snapshot view - sourceTable mapping", tags = { "Apollo" })
    @PostMapping(value = "view_mapping")
    @ResponseBody
    public EnvelopeResponse<Boolean> saveSnapshotViewMapping(@RequestBody SnapshotSourceTableStatsRequest request) {
        log.debug("saveSnapshotViewMapping request : {}", request);
        val project = checkProjectName(request.getProject());
        val result = snapshotSourceTableStatsService.saveSnapshotViewMapping(project);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, result, "");
    }
}

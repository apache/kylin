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
import static org.apache.kylin.common.exception.ServerErrorCode.RELOAD_TABLE_FAILED;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.rest.request.StreamingRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.LoadTableResponse;
import org.apache.kylin.rest.service.StreamingTableService;
import org.apache.kylin.rest.service.TableExtService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping(value = "/api/streaming_tables", produces = { HTTP_VND_APACHE_KYLIN_JSON,
        HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class StreamingTableController extends NBasicController {

    @Autowired
    @Qualifier("streamingTableService")
    private StreamingTableService streamingTableService;

    @Autowired
    @Qualifier("tableExtService")
    private TableExtService tableExtService;

    @ApiOperation(value = "loadTables")
    @PostMapping(value = "/table", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<LoadTableResponse> saveStreamingTable(@RequestBody StreamingRequest streamingRequest) {
        checkStreamingEnabled();
        String project = streamingRequest.getProject();
        checkProjectName(project);
        TableExtDesc tableExt = streamingTableService.getOrCreateTableExt(project, streamingRequest.getTableDesc());
        try {
            // If the Streaming Table does not have a BatchTable, convert decimal to double
            streamingTableService.decimalConvertToDouble(project, streamingRequest);

            streamingTableService.checkColumns(streamingRequest);

            tableExtService.checkAndLoadTable(project, streamingRequest.getTableDesc(), tableExt);
            streamingTableService.createKafkaConfig(project, streamingRequest.getKafkaConfig());
            LoadTableResponse loadTableResponse = new LoadTableResponse();
            loadTableResponse.getLoaded().add(streamingRequest.getTableDesc().getIdentity());
            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, loadTableResponse, "");
        } catch (Exception e) {
            Throwable root = ExceptionUtils.getRootCause(e) == null ? e : ExceptionUtils.getRootCause(e);
            throw new KylinException(RELOAD_TABLE_FAILED, root.getMessage());
        }
    }

    @ApiOperation(value = "updateTables")
    @PutMapping(value = "/table", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<LoadTableResponse> updateStreamingTable(@RequestBody StreamingRequest streamingRequest) {
        checkStreamingEnabled();
        String project = streamingRequest.getProject();
        checkProjectName(project);
        try {
            TableExtDesc tableExt = streamingTableService.getOrCreateTableExt(project, streamingRequest.getTableDesc());
            streamingTableService.reloadTable(project, streamingRequest.getTableDesc(), tableExt);
            streamingTableService.updateKafkaConfig(project, streamingRequest.getKafkaConfig());
            LoadTableResponse loadTableResponse = new LoadTableResponse();
            loadTableResponse.getLoaded().add(streamingRequest.getTableDesc().getIdentity());
            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, loadTableResponse, "");
        } catch (Exception e) {
            Throwable root = ExceptionUtils.getRootCause(e) == null ? e : ExceptionUtils.getRootCause(e);
            throw new KylinException(RELOAD_TABLE_FAILED, root.getMessage());
        }
    }
}

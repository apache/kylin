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

package org.apache.kylin.rest.controller.open;

import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_TABLE_NAME;
import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

import java.io.IOException;
import java.util.Locale;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rest.request.SamplingRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.rest.controller.BaseController;
import org.apache.kylin.rest.controller.SampleController;
import org.apache.kylin.rest.request.RefreshSegmentsRequest;
import org.apache.kylin.rest.response.OpenPartitionColumnFormatResponse;
import org.apache.kylin.rest.service.TableService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.annotations.VisibleForTesting;

import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping(value = "/api/tables", produces = { HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class OpenSampleController extends BaseController {

    private static final String TABLE = "table";

    @Autowired
    private SampleController sampleController;

    @Autowired
    private TableService tableService;

    /**
     * auto mode, refresh data
     *
     * @param request
     * @return
     * @throws IOException
     */
    @ApiOperation(value = "refreshSegments", tags = { "DW" })
    @PutMapping(value = "/data_range")
    @ResponseBody
    public EnvelopeResponse<String> refreshSegments(@RequestBody RefreshSegmentsRequest request) throws IOException {
        String projectName = checkProjectName(request.getProject());
        request.setProject(projectName);
        checkRequiredArg("tableName", request.getTable());

        getTable(request.getProject(), request.getTable());
        return sampleController.refreshSegments(request);
    }

    @ApiOperation(value = "samplingJobs", tags = { "AI" })
    @PostMapping(value = "/sampling_jobs")
    @ResponseBody
    public EnvelopeResponse<String> submitSampling(@RequestBody SamplingRequest request) {
        checkProjectName(request.getProject());
        checkStreamingOperation(request.getProject(), request.getQualifiedTableName());
        return sampleController.submitSampling(request);
    }

    @ApiOperation(value = "getPartitionColumnFormat", tags = { "DW" })
    @GetMapping(value = "/column_format")
    @ResponseBody
    public EnvelopeResponse<OpenPartitionColumnFormatResponse> getPartitionColumnFormat(
            @RequestParam(value = "project") String project, @RequestParam(value = "table") String table,
            @RequestParam(value = "column_name") String columnName) throws Exception {
        String projectName = checkProjectName(project);
        checkRequiredArg(TABLE, table);
        checkRequiredArg("column_name", columnName);

        String columnFormat = tableService.getPartitionColumnFormat(projectName, table.toUpperCase(Locale.ROOT),
                columnName);
        OpenPartitionColumnFormatResponse columnFormatResponse = new OpenPartitionColumnFormatResponse();
        columnFormatResponse.setColumnName(columnName);
        columnFormatResponse.setColumnFormat(columnFormat);

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, columnFormatResponse, "");
    }

    @VisibleForTesting
    protected TableDesc getTable(String project, String tableName) {
        TableDesc table = tableService.getManager(NTableMetadataManager.class, project).getTableDesc(tableName);
        if (null == table) {
            throw new KylinException(INVALID_TABLE_NAME,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getTableNotFound(), tableName));
        }
        return table;
    }

}

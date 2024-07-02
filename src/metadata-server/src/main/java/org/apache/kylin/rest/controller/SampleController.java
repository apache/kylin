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
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_TABLE_NAME;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_SAMPLING_RANGE_INVALID;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.request.SamplingRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.service.DTableService;
import org.apache.kylin.rest.service.ModelBuildSupporter;
import org.apache.kylin.rest.service.TableSampleService;
import org.apache.kylin.rest.service.TableService;
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
@RequestMapping(value = "/api/tables", produces = { HTTP_VND_APACHE_KYLIN_JSON })
public class SampleController extends NBasicController {

    private static final String TABLE = "table";
    private static final int MAX_SAMPLING_ROWS = 20_000_000;
    private static final int MIN_SAMPLING_ROWS = 10_000;

    @Deprecated
    @Autowired
    @Qualifier("tableService")
    private TableService tableService;

    @Autowired
    private DTableService dTableService;

    @Autowired
    @Qualifier("modelBuildService")
    private ModelBuildSupporter modelBuildService;

    @Autowired
    private TableSampleService tableSampleService;

    @ApiOperation(value = "partitionColumnFormat", tags = { "AI" })
    @GetMapping(value = "/partition_column_format")
    @ResponseBody
    public EnvelopeResponse<String> getPartitionColumnFormat(@RequestParam(value = "project") String project,
            @RequestParam(value = "table") String table,
            @RequestParam(value = "partition_column") String partitionColumn,
            @RequestParam(value = "partition_expression", required = false) String partitionExpression)
            throws Exception {
        checkProjectName(project);
        checkRequiredArg(TABLE, table);
        checkRequiredArg("partitionColumn", partitionColumn);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                tableService.getPartitionColumnFormat(project, table, partitionColumn, partitionExpression), "");
    }

    @ApiOperation(value = "samplingJobs", tags = { "AI" })
    @PostMapping(value = "/sampling_jobs", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<String> submitSampling(@RequestBody SamplingRequest request) {
        checkProjectName(request.getProject());
        ProjectInstance prjInstance = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getProject(request.getProject());
        checkParamLength("tag", request.getTag(), prjInstance.getConfig().getJobTagMaxSize());
        checkSamplingRows(request.getRows());
        checkSamplingTable(request.getQualifiedTableName());
        validatePriority(request.getPriority());

        List<String> sampleJobs = tableSampleService.sampling(Sets.newHashSet(request.getQualifiedTableName()),
                request.getProject(), request.getRows(), request.getPriority(), request.getYarnQueue(),
                request.getTag());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, sampleJobs.stream().findFirst().orElse(null), "");
    }

    @ApiOperation(value = "hasSamplingJob", tags = { "AI" }, notes = "Update Param: qualified_table_name")
    @GetMapping(value = "/sampling_check_result")
    @ResponseBody
    public EnvelopeResponse<Boolean> hasSamplingJob(@RequestParam(value = "project") String project,
            @RequestParam(value = "qualified_table_name") String qualifiedTableName) {
        checkProjectName(project);
        checkSamplingTable(qualifiedTableName);
        boolean hasSamplingJob = tableSampleService.hasSamplingJob(project, qualifiedTableName);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, hasSamplingJob, "");
    }

    public static void checkSamplingRows(int rows) {
        if (rows > MAX_SAMPLING_ROWS || rows < MIN_SAMPLING_ROWS) {
            throw new KylinException(JOB_SAMPLING_RANGE_INVALID, MIN_SAMPLING_ROWS, MAX_SAMPLING_ROWS);
        }
    }

    public static void checkSamplingTable(String tableName) {
        Message msg = MsgPicker.getMsg();
        if (tableName == null || StringUtils.isEmpty(tableName.trim())) {
            throw new KylinException(INVALID_TABLE_NAME, msg.getFailedForNoSamplingTable());
        }

        if (tableName.contains(" ") || !tableName.contains(".") || tableName.split("\\.").length != 2) {
            throw new KylinException(INVALID_TABLE_NAME, msg.getSamplingFailedForIllegalTableName());
        }
    }
}

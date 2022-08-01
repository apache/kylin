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

import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_PARAMETER;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;
import static org.apache.kylin.common.exception.ServerErrorCode.SNAPSHOT_RELOAD_PARTITION_FAILED;
import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.constant.SnapshotStatus;
import org.apache.kylin.rest.request.SnapshotRequest;
import org.apache.kylin.rest.request.SnapshotTableConfigRequest;
import org.apache.kylin.rest.request.TablePartitionsRequest;
import org.apache.kylin.rest.request.TableReloadPartitionColRequest;
import org.apache.kylin.rest.response.JobInfoResponse;
import org.apache.kylin.rest.response.NInitTablesResponse;
import org.apache.kylin.rest.response.SnapshotCheckResponse;
import org.apache.kylin.rest.response.SnapshotColResponse;
import org.apache.kylin.rest.response.SnapshotInfoResponse;
import org.apache.kylin.rest.response.SnapshotPartitionsResponse;
import org.apache.kylin.rest.response.TableNameResponse;
import org.apache.kylin.rest.service.SnapshotService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.base.CaseFormat;

import io.swagger.annotations.ApiOperation;
import lombok.val;

@Controller
@RequestMapping(value = "/api/snapshots", produces = { HTTP_VND_APACHE_KYLIN_JSON,
        HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class SnapshotController extends BaseController {

    private static final Logger logger = LoggerFactory.getLogger(SnapshotController.class);

    @Autowired
    @Qualifier("snapshotService")
    private SnapshotService snapshotService;

    @ApiOperation(value = "config partition col for snapshot Tables", tags = { "AI" }, notes = "config partition col")
    @PostMapping(value = "/config")
    @ResponseBody
    public EnvelopeResponse<String> configSnapshotPartitionCol(@RequestBody SnapshotTableConfigRequest configRequest) {
        checkProjectName(configRequest.getProject());
        snapshotService.configSnapshotPartitionCol(configRequest.getProject(), configRequest.getTablePartitionCol());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "get col info of snapshot table", tags = { "AI" }, notes = "get col info")
    @GetMapping(value = "/config")
    @ResponseBody
    public EnvelopeResponse<DataResult<List<SnapshotColResponse>>> getSnapshotCols(
            @RequestParam(value = "project") String project,
            @RequestParam(value = "tables", required = false, defaultValue = "") Set<String> tables,
            @RequestParam(value = "databases", required = false, defaultValue = "") Set<String> databases,
            @RequestParam(value = "table_pattern", required = false, defaultValue = "") String tablePattern,
            @RequestParam(value = "include_exist", required = false, defaultValue = "true") boolean includeExistSnapshot,
            @RequestParam(value = "exclude_broken", required = false, defaultValue = "true") boolean excludeBroken,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer limit) {
        project = checkProjectName(project);
        checkNonNegativeIntegerArg("page_offset", offset);
        checkNonNegativeIntegerArg("page_size", limit);

        List<SnapshotColResponse> responses = snapshotService.getSnapshotCol(project, tables, databases, tablePattern,
                includeExistSnapshot, excludeBroken);

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, DataResult.get(responses, offset, limit), "");
    }

    @ApiOperation(value = "reload partition col of table", tags = { "AI" }, notes = "reload partition col")
    @PostMapping(value = "/reload_partition_col")
    @ResponseBody
    public EnvelopeResponse<SnapshotColResponse> getSnapshotCols(@RequestBody TableReloadPartitionColRequest request)
            throws Exception {
        try {
            checkProjectName(request.getProject());
            SnapshotColResponse responses = snapshotService.reloadPartitionCol(request.getProject(),
                    request.getTable());
            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, responses, "");
        } catch (Exception e) {
            logger.error("reload partition column failed...", e);
            Throwable root = ExceptionUtils.getRootCause(e) == null ? e : ExceptionUtils.getRootCause(e);
            throw new KylinException(SNAPSHOT_RELOAD_PARTITION_FAILED, root.getMessage());
        }
    }

    @ApiOperation(value = "partitions of table", tags = { "AI" }, notes = "get partition value")
    @PostMapping(value = "/partitions")
    @ResponseBody
    public EnvelopeResponse getSnapshotPartitionValues(@RequestBody TablePartitionsRequest tablePartitionsRequest) {
        checkProjectName(tablePartitionsRequest.getProject());
        Map<String, SnapshotPartitionsResponse> response = snapshotService
                .getPartitions(tablePartitionsRequest.getProject(), tablePartitionsRequest.getTableCols());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "buildSnapshotsManually", tags = { "AI" }, notes = "build snapshots")
    @PostMapping(value = "")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponse> buildSnapshotsManually(@RequestBody SnapshotRequest snapshotsRequest) {
        checkProjectName(snapshotsRequest.getProject());
        validatePriority(snapshotsRequest.getPriority());
        checkParamLength("tag", snapshotsRequest.getTag(), 1024);
        if (snapshotsRequest.getTables().isEmpty() && snapshotsRequest.getDatabases().isEmpty()) {
            throw new KylinException(EMPTY_PARAMETER, "You should select at least one table or database to load!!");
        }
        JobInfoResponse response = snapshotService.buildSnapshots(snapshotsRequest.getProject(),
                snapshotsRequest.getDatabases(), snapshotsRequest.getTables(), snapshotsRequest.getOptions(), false,
                snapshotsRequest.getPriority(), snapshotsRequest.getYarnQueue(), snapshotsRequest.getTag());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "refreshSnapshotsManually", tags = { "AI" }, notes = "refresh snapshots")
    @PutMapping(value = "")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponse> refreshSnapshotsManually(@RequestBody SnapshotRequest snapshotsRequest)
            throws Exception {
        checkProjectName(snapshotsRequest.getProject());
        checkParamLength("tag", snapshotsRequest.getTag(), 1024);
        validatePriority(snapshotsRequest.getPriority());
        if (snapshotsRequest.getTables().isEmpty() && snapshotsRequest.getDatabases().isEmpty()) {
            throw new KylinException(EMPTY_PARAMETER, "You should select at least one table or database to load!!");
        }
        JobInfoResponse response = snapshotService.buildSnapshots(snapshotsRequest.getProject(),
                snapshotsRequest.getDatabases(), snapshotsRequest.getTables(), snapshotsRequest.getOptions(), true,
                snapshotsRequest.getPriority(), snapshotsRequest.getYarnQueue(), snapshotsRequest.getTag());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "checkBeforeDelete", tags = { "AI" }, notes = "check before delete snapshots")
    @PostMapping(value = "/check_before_delete")
    @ResponseBody
    public EnvelopeResponse<SnapshotCheckResponse> checkBeforeDelete(@RequestBody SnapshotRequest snapshotsRequest) {
        checkProjectName(snapshotsRequest.getProject());
        SnapshotCheckResponse response = snapshotService.checkBeforeDeleteSnapshots(snapshotsRequest.getProject(),
                snapshotsRequest.getTables());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "deleteSnapshots", tags = { "AI" }, notes = "delete snapshots")
    @DeleteMapping(value = "")
    @ResponseBody
    public EnvelopeResponse<SnapshotCheckResponse> deleteSnapshots(@RequestParam(value = "project") String project,
            @RequestParam(value = "tables") Set<String> tables) {
        project = checkProjectName(project);
        SnapshotCheckResponse response = snapshotService.deleteSnapshots(project, tables);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "getSnapshots", tags = { "AI" }, notes = "get snapshots")
    @GetMapping(value = "")
    @ResponseBody
    public EnvelopeResponse<DataResult<List<SnapshotInfoResponse>>> getSnapshots(
            @RequestParam(value = "project") String project,
            @RequestParam(value = "table", required = false, defaultValue = "") String table,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer limit,
            @RequestParam(value = "status", required = false, defaultValue = "") Set<SnapshotStatus> statusFilter,
            @RequestParam(value = "partition", required = false, defaultValue = "") Set<Boolean> partitionFilter,
            @RequestParam(value = "sort_by", required = false, defaultValue = "last_modified_time") String sortBy,
            @RequestParam(value = "reverse", required = false, defaultValue = "true") boolean isReversed) {
        project = checkProjectName(project);
        checkNonNegativeIntegerArg("page_offset", offset);
        checkNonNegativeIntegerArg("page_size", limit);
        checkBooleanArg("reverse", isReversed);
        try {
            SnapshotInfoResponse.class.getDeclaredField(CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, sortBy));
        } catch (NoSuchFieldException e) {
            logger.error(String.format(Locale.ROOT, "No field called '%s'.", sortBy), e);
            throw new KylinException(INVALID_PARAMETER, String.format(Locale.ROOT, "No field called '%s'.", sortBy));
        }
        List<SnapshotInfoResponse> responses = snapshotService.getProjectSnapshots(project, table, statusFilter,
                partitionFilter, sortBy, isReversed);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, DataResult.get(responses, offset, limit), "");
    }

    @ApiOperation(value = "getTables", tags = { "AI" }, notes = "get all tables with or without snapshot")
    @GetMapping(value = "/tables")
    @ResponseBody
    public EnvelopeResponse<NInitTablesResponse> getTables(@RequestParam(value = "project") String project,
            @RequestParam(value = "table", required = false, defaultValue = "") String table,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer limit) {
        project = checkProjectName(project);
        checkNonNegativeIntegerArg("page_offset", offset);
        checkNonNegativeIntegerArg("page_size", limit);
        val res = snapshotService.getTables(project, table, offset, limit);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, res, "");
    }

    @ApiOperation(value = "loadMoreTables", tags = { "AI" }, notes = "load more table pages")
    @GetMapping(value = "/tables/more")
    @ResponseBody
    public EnvelopeResponse<DataResult<List<TableNameResponse>>> loadMoreTables(
            @RequestParam(value = "project") String project,
            @RequestParam(value = "table", required = false, defaultValue = "") String table,
            @RequestParam(value = "database") String database,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer limit) {
        project = checkProjectName(project);
        checkRequiredArg("database", database);
        checkNonNegativeIntegerArg("page_offset", offset);
        checkNonNegativeIntegerArg("page_size", limit);
        List<TableNameResponse> tables = snapshotService.getTableNameResponses(project, database, table);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, DataResult.get(tables, offset, limit), "");
    }
}

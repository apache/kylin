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
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_PARAMETER;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_TABLE_NAME;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.metadata.table.InternalTablePartitionDetail;
import org.apache.kylin.rest.request.InternalTableBuildRequest;
import org.apache.kylin.rest.request.InternalTableRequest;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.InternalTableDescResponse;
import org.apache.kylin.rest.response.InternalTableLoadingJobResponse;
import org.apache.kylin.rest.service.InternalTableService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import io.swagger.annotations.ApiOperation;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Controller
@RequestMapping(value = "/api/internal_tables", produces = { HTTP_VND_APACHE_KYLIN_JSON })
@Slf4j
public class InternalTableController extends NBasicController {

    @Autowired
    @Qualifier("internalTableService")
    private InternalTableService internalTableService;

    @ApiOperation(value = "create_internal_table", tags = { "AI" })
    @PostMapping(value = "/{database:.+}/{table:.+}", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<String> createInternalTable(@RequestParam(value = "project") String project,
            @PathVariable(value = "database") String database, //
            @PathVariable(value = "table") String table, @RequestBody InternalTableRequest request) throws Exception {
        project = checkProjectName(project);
        if (StringUtils.isBlank(table) || StringUtils.isBlank(database)) {
            throw new KylinException(EMPTY_PARAMETER, "Table or database can not be null, please check again.");
        }
        internalTableService.createInternalTable(project, table, database, request.getPartitionCols(),
                request.getDatePartitionFormat(), request.getTblProperties(), request.getStorageType());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "drop_internal_table", tags = { "AI" })
    @DeleteMapping(value = "/{database:.+}/{table:.+}", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<String> dropInternalTable(@RequestParam(value = "project") String project,
            @PathVariable(value = "database") String database, //
            @PathVariable(value = "table") String table) throws Exception {
        project = checkProjectName(project);
        String dbTblName = database + "." + table;
        internalTableService.dropInternalTable(project, dbTblName);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "truncate_internal_table", tags = { "AI" })
    @DeleteMapping(value = "/truncate_internal_table", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<String> truncateInternalTable(@RequestParam(value = "project") String project,
            @RequestParam(value = "database") String database, @RequestParam(value = "table") String table)
            throws Exception {
        project = checkProjectName(project);
        if (StringUtils.isBlank(table) || StringUtils.isBlank(database)) {
            throw new KylinException(INVALID_TABLE_NAME, MsgPicker.getMsg().getTableOrDatabaseNameCannotEmpty());
        }
        String tableIdentity = database + "." + table;
        internalTableService.truncateInternalTable(project, tableIdentity);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, null, "");
    }

    @ApiOperation(value = "drop_table_partitions", tags = { "AI" })
    @DeleteMapping(value = "/partitions", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<String> dropPartitions(@RequestParam(value = "project") String project,
            @RequestParam(value = "database") String database, @RequestParam(value = "table") String table,
            @RequestParam(value = "partitions") String[] partitionValues) throws Exception {
        project = checkProjectName(project);
        if (StringUtils.isBlank(table) || StringUtils.isBlank(database)) {
            throw new KylinException(INVALID_TABLE_NAME, MsgPicker.getMsg().getTableOrDatabaseNameCannotEmpty());
        }
        // If partitionValues is null, all files will be cleared
        // otherwise only files in the specified partition will be cleared.
        String tableIdentity = database + "." + table;
        internalTableService.dropPartitionsOnDeltaTable(project, tableIdentity, partitionValues, null);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, null, "");
    }

    @ApiOperation(value = "load_into_internal", tags = { "AI" })
    @PostMapping(value = "/{project:.+}/{database:.+}/{table:.+}", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<InternalTableLoadingJobResponse> loadIntoInternalTable(
            @PathVariable(value = "project") String project, @PathVariable(value = "database") String database, //
            @PathVariable(value = "table") String table, @RequestBody InternalTableBuildRequest request)
            throws Exception {
        project = checkProjectName(project);
        if (StringUtils.isBlank(table) || StringUtils.isBlank(database)) {
            throw new KylinException(INVALID_TABLE_NAME, MsgPicker.getMsg().getTableNameCannotEmpty());
        }
        InternalTableLoadingJobResponse response = internalTableService.loadIntoInternalTable(project, table, database,
                request.isIncremental(), request.isRefresh(), request.getStartDate(), request.getEndDate(),
                request.getPartitions(), request.getYarnQueue());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "update_table", tags = { "AI" })
    @PutMapping(value = "/{database:.+}/{table:.+}", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<String> updateTable(@RequestParam(value = "project") String project,
            @PathVariable(value = "database") String database, //
            @PathVariable(value = "table") String table, @RequestBody InternalTableRequest request) throws Exception {
        project = checkProjectName(project);
        if (StringUtils.isBlank(table) || StringUtils.isBlank(database)) {
            throw new KylinException(EMPTY_PARAMETER, "Table or database can not be null, please check again.");
        }
        if (table.isEmpty()) {
            throw new KylinException(INVALID_TABLE_NAME, MsgPicker.getMsg().getTableNameCannotEmpty());
        }
        internalTableService.updateInternalTable(project, table, database, request.getPartitionCols(),
                request.getDatePartitionFormat(), request.getTblProperties(), request.getStorageType());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, null, "");
    }

    @ApiOperation(value = "get_tables", tags = { "AI" })
    @GetMapping(value = "", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<DataResult<List<InternalTableDescResponse>>> getTableList(
            @RequestParam(value = "project") String project,
            @RequestParam(value = "need_details", required = false, defaultValue = "false") boolean needDetails,
            @RequestParam(value = "is_fuzzy", required = false, defaultValue = "false") boolean isFuzzy,
            @RequestParam(value = "database", required = false) String database,
            @RequestParam(value = "table", required = false) String table,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer limit) {
        project = checkProjectName(project);

        val rep = internalTableService.getTableList(project, isFuzzy, needDetails, database, table);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, DataResult.get(rep, offset, limit), "");
    }

    @ApiOperation(value = "get_table_detail", tags = { "AI" })
    @GetMapping(value = "/{database:.+}/{table:.+}", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<DataResult<List<InternalTablePartitionDetail>>> getTableDetail(
            @RequestParam(value = "project") String project, @PathVariable(value = "database") String database,
            @PathVariable(value = "table") String table,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer limit) {
        project = checkProjectName(project);
        val rep = internalTableService.getTableDetail(project, database, table);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, DataResult.get(rep, offset, limit), "");
    }

}

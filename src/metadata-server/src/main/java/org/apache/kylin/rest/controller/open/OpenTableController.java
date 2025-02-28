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

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_TABLE_NAME;
import static org.apache.kylin.common.exception.ServerErrorCode.UNSUPPORTED_DATA_SOURCE_TYPE;
import static org.apache.kylin.common.exception.ServerErrorCode.UNSUPPORTED_STREAMING_OPERATION;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_SAMPLING_RANGE_INVALID;
import static org.apache.kylin.rest.util.TableUtils.calculateTableSize;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rest.controller.NBasicController;
import org.apache.kylin.rest.controller.NTableController;
import org.apache.kylin.rest.request.AWSTableLoadRequest;
import org.apache.kylin.rest.request.OpenReloadTableRequest;
import org.apache.kylin.rest.request.TableLoadRequest;
import org.apache.kylin.rest.request.UpdateAWSTableExtDescRequest;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.LoadTableResponse;
import org.apache.kylin.rest.response.OpenPreReloadTableResponse;
import org.apache.kylin.rest.response.OpenReloadTableResponse;
import org.apache.kylin.rest.response.PreUnloadTableResponse;
import org.apache.kylin.rest.response.UpdateAWSTableExtDescResponse;
import org.apache.kylin.rest.service.ProjectService;
import org.apache.kylin.rest.service.TableService;
import org.springframework.beans.factory.annotation.Autowired;
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

@Controller
@RequestMapping(value = "/api/tables", produces = { HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class OpenTableController extends NBasicController {

    @Autowired
    private NTableController tableController;

    @Autowired
    private TableService tableService;

    @Autowired
    private ProjectService projectService;

    private static final Integer MAX_SAMPLING_ROWS = 20_000_000;
    private static final Integer MIN_SAMPLING_ROWS = 10_000;

    @VisibleForTesting
    public TableDesc getTable(String project, String tableName) {
        TableDesc table = tableService.getManager(NTableMetadataManager.class, project).getTableDesc(tableName);
        if (null == table) {
            throw new KylinException(INVALID_TABLE_NAME,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getTableNotFound(), tableName));
        }
        return table;
    }

    @VisibleForTesting
    public void updateDataSourceType(String project, int dataSourceType) {
        String sourceType = String.valueOf(dataSourceType);
        if (!sourceType.equals(projectService.getDataSourceType(project))) {
            projectService.setDataSourceType(project, sourceType);
        }
    }

    @ApiOperation(value = "getTableDesc", tags = { "AI" })
    @GetMapping(value = "")
    @ResponseBody
    public EnvelopeResponse<DataResult<List<TableDesc>>> getTableDesc(@RequestParam(value = "project") String project,
            @RequestParam(value = "table", required = false) String table,
            @RequestParam(value = "database", required = false) String database,
            @RequestParam(value = "is_fuzzy", required = false, defaultValue = "false") boolean isFuzzy,
            @RequestParam(value = "ext", required = false, defaultValue = "true") boolean withExt,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer limit,
            @RequestParam(value = "source_type", required = false, defaultValue = "9") Integer sourceType)
            throws IOException {
        checkProjectName(project);
        checkNonNegativeIntegerArg("page_offset", offset);
        if (sourceType == ISourceAware.ID_STREAMING) {
            throw new KylinException(UNSUPPORTED_STREAMING_OPERATION,
                    MsgPicker.getMsg().getStreamingOperationNotSupport());
        }
        int returnTableSize = calculateTableSize(offset, limit);
        Pair<List<TableDesc>, Integer> tableDescWithActualSize = tableService.getTableDesc(project, withExt,
                StringUtils.upperCase(table, Locale.ROOT), StringUtils.upperCase(database, Locale.ROOT), isFuzzy,
                Collections.singletonList(sourceType), returnTableSize);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                DataResult.getCustom(tableDescWithActualSize, offset, limit), "");
    }

    @ApiOperation(value = "loadTables", tags = { "AI" })
    @PostMapping(value = "")
    @ResponseBody
    public EnvelopeResponse<LoadTableResponse> loadTables(@RequestBody TableLoadRequest tableLoadRequest)
            throws Exception {
        String projectName = checkProjectName(tableLoadRequest.getProject());
        tableLoadRequest.setProject(projectName);
        checkRequiredArg("need_sampling", tableLoadRequest.getNeedSampling());
        validatePriority(tableLoadRequest.getPriority());
        if (Boolean.TRUE.equals(tableLoadRequest.getNeedSampling())
                && (null == tableLoadRequest.getSamplingRows() || tableLoadRequest.getSamplingRows() > MAX_SAMPLING_ROWS
                        || tableLoadRequest.getSamplingRows() < MIN_SAMPLING_ROWS)) {
            throw new KylinException(JOB_SAMPLING_RANGE_INVALID, MIN_SAMPLING_ROWS, MAX_SAMPLING_ROWS);
        }

        // default set data_source_type = 9 and 8
        if (ISourceAware.ID_SPARK != tableLoadRequest.getDataSourceType()
                && ISourceAware.ID_JDBC != tableLoadRequest.getDataSourceType()) {
            throw new KylinException(UNSUPPORTED_DATA_SOURCE_TYPE,
                    "Only support Hive as the data source. (data_source_type = 9 || 8)");
        }
        updateDataSourceType(tableLoadRequest.getProject(), tableLoadRequest.getDataSourceType());

        return tableController.loadTables(tableLoadRequest);
    }

    @ApiOperation(value = "loadAWSTablesCompatibleCrossAccount", tags = { "N/A" })
    @PostMapping(value = "/compatibility/aws")
    @ResponseBody
    public EnvelopeResponse<LoadTableResponse> loadAWSTablesCompatibleCrossAccount(
            @RequestBody AWSTableLoadRequest tableLoadRequest) throws Exception {
        String projectName = checkProjectName(tableLoadRequest.getProject());
        tableLoadRequest.setProject(projectName);
        checkRequiredArg("need_sampling", tableLoadRequest.getNeedSampling());
        validatePriority(tableLoadRequest.getPriority());
        boolean sampleConditionError = (null == tableLoadRequest.getSamplingRows()
                || tableLoadRequest.getSamplingRows() > MAX_SAMPLING_ROWS
                || tableLoadRequest.getSamplingRows() < MIN_SAMPLING_ROWS);
        if (Boolean.TRUE.equals(tableLoadRequest.getNeedSampling()) && sampleConditionError) {
            throw new KylinException(JOB_SAMPLING_RANGE_INVALID, MIN_SAMPLING_ROWS, MAX_SAMPLING_ROWS);
        }

        // default set data_source_type = 9 and 8
        if (ISourceAware.ID_SPARK != tableLoadRequest.getDataSourceType()
                && ISourceAware.ID_JDBC != tableLoadRequest.getDataSourceType()) {
            throw new KylinException(UNSUPPORTED_DATA_SOURCE_TYPE,
                    "Only support Hive as the data source. (data_source_type = 9 || 8)");
        }
        updateDataSourceType(tableLoadRequest.getProject(), tableLoadRequest.getDataSourceType());
        return tableController.loadAWSTablesCompatibleCrossAccount(tableLoadRequest);
    }

    @ApiOperation(value = "preReloadTable", tags = { "AI" })
    @GetMapping(value = "/pre_reload")
    @ResponseBody
    public EnvelopeResponse<OpenPreReloadTableResponse> preReloadTable(@RequestParam(value = "project") String project,
            @RequestParam(value = "table") String table,
            @RequestParam(value = "need_details", required = false, defaultValue = "false") boolean needDetails)
            throws Exception {
        String projectName = checkProjectName(project);
        table = StringUtils.upperCase(table, Locale.ROOT);
        checkStreamingOperation(project, table);
        OpenPreReloadTableResponse result = tableService.preProcessBeforeReloadWithoutFailFast(projectName, table,
                needDetails);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, result, "");
    }

    @ApiOperation(value = "reloadTable", tags = { "AI" })
    @PostMapping(value = "/reload")
    @ResponseBody
    public EnvelopeResponse<OpenReloadTableResponse> reloadTable(@RequestBody OpenReloadTableRequest request) {
        String projectName = checkProjectName(request.getProject());
        request.setProject(projectName);
        checkRequiredArg("need_sampling", request.getNeedSampling());
        if (StringUtils.isEmpty(request.getTable())) {
            throw new KylinException(INVALID_TABLE_NAME, MsgPicker.getMsg().getTableNameCannotEmpty());
        }
        request.setTable(StringUtils.upperCase(request.getTable(), Locale.ROOT));
        checkStreamingOperation(request.getProject(), request.getTable());
        validatePriority(request.getPriority());

        if (request.getNeedSampling()) {
            checkSamplingRows(request.getSamplingRows());
        }

        Pair<String, List<String>> pair = tableService.reloadTable(request.getProject(), request.getTable(),
                request.getNeedSampling(), request.getSamplingRows(), request.getNeedBuilding(), request.getPriority(),
                request.getYarnQueue());

        OpenReloadTableResponse response = new OpenReloadTableResponse();
        response.setSamplingId(pair.getFirst());
        response.setJobIds(pair.getSecond());

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "reloadAWSTablesCompatibleCrossAccount", tags = { "N/A" })
    @PostMapping(value = "/reload/compatibility/aws")
    @ResponseBody
    public EnvelopeResponse<OpenReloadTableResponse> reloadAWSTablesCompatibleCrossAccount(
            @RequestBody OpenReloadTableRequest request) {
        String projectName = checkProjectName(request.getProject());
        checkStreamingOperation(request.getProject(), request.getS3TableExtInfo().getName());
        request.setProject(projectName);
        checkRequiredArg("need_sampling", request.getNeedSampling());
        validatePriority(request.getPriority());
        if (StringUtils.isEmpty(request.getS3TableExtInfo().getName())) {
            throw new KylinException(INVALID_TABLE_NAME, MsgPicker.getMsg().getTableNameCannotEmpty());
        }

        if (request.getNeedSampling()) {
            checkSamplingRows(request.getSamplingRows());
        }

        Pair<String, List<String>> pair = tableService.reloadAWSTableCompatibleCrossAccount(request.getProject(),
                request.getS3TableExtInfo(), request.getNeedSampling(), request.getSamplingRows(),
                request.getNeedBuilding(), request.getPriority(), request.getYarnQueue());

        OpenReloadTableResponse response = new OpenReloadTableResponse();
        response.setSamplingId(pair.getFirst());
        response.setJobIds(pair.getSecond());

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "prepareUnloadTable", tags = { "AI" })
    @GetMapping(value = "/{database:.+}/{table:.+}/prepare_unload")
    @ResponseBody
    public EnvelopeResponse<PreUnloadTableResponse> prepareUnloadTable(@RequestParam(value = "project") String project,
            @PathVariable(value = "database") String database, @PathVariable(value = "table") String table)
            throws IOException {

        String projectName = checkProjectName(project);
        String dbTblName = String.format(Locale.ROOT, "%s.%s", StringUtils.upperCase(database, Locale.ROOT),
                StringUtils.upperCase(table, Locale.ROOT));
        val response = tableService.preUnloadTable(projectName, dbTblName);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "unloadTable", tags = { "AI" })
    @DeleteMapping(value = "/{database:.+}/{table:.+}")
    @ResponseBody
    public EnvelopeResponse<String> unloadTable(@RequestParam(value = "project") String project,
            @PathVariable(value = "database") String database, @PathVariable(value = "table") String table,
            @RequestParam(value = "cascade", defaultValue = "false") Boolean cascade) {

        String projectName = checkProjectName(project);
        String dbTblName = String.format(Locale.ROOT, "%s.%s", StringUtils.upperCase(database, Locale.ROOT),
                StringUtils.upperCase(table, Locale.ROOT));
        tableService.unloadTable(projectName, dbTblName, cascade);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, dbTblName, "");
    }

    @ApiOperation(value = "updateLoadedAWSTableExtProp", tags = { "N/A" })
    @PutMapping(value = "/ext/prop/aws")
    @ResponseBody
    public EnvelopeResponse<UpdateAWSTableExtDescResponse> updateLoadedAWSTableExtProp(
            @RequestBody UpdateAWSTableExtDescRequest request) {
        String projectName = checkProjectName(request.getProject());
        request.setProject(projectName);
        return tableController.updateLoadedAWSTableExtProp(request);
    }

    public static void checkSamplingRows(int rows) {
        if (rows > MAX_SAMPLING_ROWS || rows < MIN_SAMPLING_ROWS) {
            throw new KylinException(JOB_SAMPLING_RANGE_INVALID, MIN_SAMPLING_ROWS, MAX_SAMPLING_ROWS);
        }
    }
}

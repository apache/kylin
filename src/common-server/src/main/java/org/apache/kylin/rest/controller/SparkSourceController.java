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

import java.io.IOException;
import java.util.List;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.request.DDLRequest;
import org.apache.kylin.rest.request.ExportTableRequest;
import org.apache.kylin.rest.response.DDLResponse;
import org.apache.kylin.rest.response.ExportTablesResponse;
import org.apache.kylin.rest.response.TableNameResponse;
import org.apache.kylin.rest.service.SparkSourceService;
import org.apache.spark.sql.AnalysisException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;

@ConditionalOnProperty(name = "kylin.env.channel", havingValue = "cloud")
@RestController
@RequestMapping(value = "/api/spark_source", produces = { HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON,
        HTTP_VND_APACHE_KYLIN_JSON })
@Slf4j
public class SparkSourceController extends NBasicController {

    @Autowired
    private SparkSourceService sparkSourceService;

    @ApiOperation(value = "execute", tags = { "DW" })
    @PostMapping(value = "/execute")
    @ResponseBody
    public EnvelopeResponse<DDLResponse> executeSQL(@RequestBody DDLRequest request) {
        DDLResponse ddlResponse = sparkSourceService.executeSQL(request);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, ddlResponse, "");
    }

    @ApiOperation(value = "exportTable", tags = { "DW" })
    @PostMapping(value = "/export_table_structure")
    @ResponseBody
    public EnvelopeResponse<ExportTablesResponse> exportTableStructure(@RequestBody ExportTableRequest request) {
        ExportTablesResponse tableResponse = sparkSourceService.exportTables(request.getDatabases(),
                request.getTables());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, tableResponse, "");
    }

    @ApiOperation(value = "dropTable", tags = { "DW" })
    @DeleteMapping(value = "/{database}/tables/{table}")
    public EnvelopeResponse<String> dropTable(@PathVariable("database") String database,
            @PathVariable("table") String table) throws AnalysisException {
        sparkSourceService.dropTable(database, table);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "listDatabase", tags = { "DW" })
    @GetMapping(value = "/databases")
    public EnvelopeResponse<List<String>> listDatabase() {
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, sparkSourceService.listDatabase(), "");
    }

    @ApiOperation(value = "listTables", tags = { "DW" })
    @GetMapping(value = "/{database}/tables")
    public EnvelopeResponse<List<TableNameResponse>> listTables(@PathVariable("database") String database,
            @RequestParam("project") String project) throws Exception {
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, sparkSourceService.listTables(database, project), "");
    }

    @ApiOperation(value = "listColumns", tags = { "DW" })
    @GetMapping(value = "/{database}/{table}/columns")
    public EnvelopeResponse<List<SparkSourceService.ColumnModel>> listColumns(@PathVariable("database") String database,
            @PathVariable("table") String table) {
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, sparkSourceService.listColumns(database, table), "");
    }

    @ApiOperation(value = "getTableDesc", tags = { "DW" })
    @GetMapping(value = "/{database}/{table}/desc")
    public EnvelopeResponse<String> getTableDesc(@PathVariable("database") String database,
            @PathVariable("table") String table) {
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, sparkSourceService.getTableDesc(database, table), "");
    }

    @ApiOperation(value = "hasPartition", tags = { "DW" })
    @GetMapping(value = "{database}/{table}/has_partition")
    public EnvelopeResponse<Boolean> hasPartition(@PathVariable("database") String database,
            @PathVariable("table") String table) {
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, sparkSourceService.hasPartition(database, table), "");
    }

    @ApiOperation(value = "databaseExists", tags = { "DW" })
    @GetMapping(value = "/{database}/exists")
    public EnvelopeResponse<Boolean> databaseExists(@PathVariable("database") String database) {
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, sparkSourceService.databaseExists(database), "");
    }

    @ApiOperation(value = "tableExists", tags = { "DW" })
    @GetMapping(value = "/{database}/{table}/exists")
    public EnvelopeResponse<Boolean> tableExists(@PathVariable("database") String database,
            @PathVariable("table") String table) {
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, sparkSourceService.tableExists(database, table), "");
    }

    @ApiOperation(value = "loadSamples", tags = { "DW" })
    @GetMapping(value = "/load_samples")
    public EnvelopeResponse<List<String>> loadSamples() throws InterruptedException, IOException {
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, sparkSourceService.loadSamples(), "");
    }

    @ApiOperation(value = "msck", tags = { "DW" })
    @GetMapping(value = "/{database}/{table}/msck")
    public EnvelopeResponse<List<String>> msck(@PathVariable("database") String database,
            @PathVariable("table") String table) {
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, sparkSourceService.msck(database, table), "");
    }

}

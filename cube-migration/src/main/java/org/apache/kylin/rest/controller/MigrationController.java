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

import java.io.IOException;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.exception.ConflictException;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.request.MigrationRequest;
import org.apache.kylin.rest.service.MigrationRuleSet;
import org.apache.kylin.rest.service.MigrationService;
import org.apache.kylin.rest.service.ModelService;
import org.apache.kylin.rest.service.QueryService;
import org.apache.kylin.rest.service.StreamingV2Service;
import org.apache.kylin.rest.service.TableService;
import org.apache.kylin.stream.core.source.StreamingSourceConfig;
import org.apache.kylin.tool.migration.CompatibilityCheckRequest;
import org.apache.kylin.tool.migration.StreamTableCompatibilityCheckRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

/**
 * Restful api for cube migration.
 */
@Controller
@RequestMapping(value = "/cubes")
public class MigrationController extends BasicController {
    private static final Logger logger = LoggerFactory.getLogger(MigrationController.class);

    @Autowired
    private MigrationService migrationService;

    @Autowired
    private QueryService queryService;

    @Autowired
    private ModelService modelService;

    @Autowired
    private TableService tableService;

    @Autowired
    private StreamingV2Service streamingV2Service;

    private final String targetHost = KylinConfig.getInstanceFromEnv().getMigrationTargetAddress();

    private CubeInstance getCubeInstance(String cubeName) {
        CubeInstance cube = queryService.getCubeManager().getCube(cubeName);
        if (cube == null) {
            throw new BadRequestException("Cannot find cube " + cubeName);
        }
        return cube;
    }

    @RequestMapping(value = "/{cubeName}/migrateRuleCheck", method = { RequestMethod.GET })
    @ResponseBody
    public String migrationRuleCheck(@PathVariable String cubeName, @RequestParam String projectName,
            @RequestParam(value = "targetHost", required = false) String targetHost) {
        CubeInstance cube = getCubeInstance(cubeName);
        try {
            MigrationRuleSet.Context ctx = new MigrationRuleSet.Context(queryService, cube, getTargetHost(targetHost),
                    projectName);
            return migrationService.checkRule(ctx);
        } catch (Exception e) {
            logger.error("Request migration failed.", e);
            throw new BadRequestException(e.getMessage());
        }
    }
    
    @RequestMapping(value = "/{cubeName}/migrateRequest", method = { RequestMethod.PUT })
    @ResponseBody
    public String requestMigration(@PathVariable String cubeName, @RequestBody MigrationRequest request) {
        CubeInstance cube = getCubeInstance(cubeName);
        try {
            MigrationRuleSet.Context ctx = new MigrationRuleSet.Context(queryService, cube,
                    getTargetHost(request.getTargetHost()), request.getProjectName());
            migrationService.requestMigration(cube, ctx);
        } catch (Exception e) {
            logger.error("Request migration failed.", e);
            throw new BadRequestException(e.getMessage());
        }
        return "ok";
    }

    @RequestMapping(value = "/{cubeName}/migrateReject", method = { RequestMethod.PUT })
    @ResponseBody
    public void reject(@PathVariable String cubeName, @RequestBody MigrationRequest request) {
        boolean reject = migrationService.reject(cubeName, request.getProjectName(), request.getReason());
        if (!reject) {
            throw new InternalErrorException("Email send out failed. See logs.");
        }
    }

    @RequestMapping(value = "/{cubeName}/migrateApprove", method = { RequestMethod.PUT })
    @ResponseBody
    public String approve(@PathVariable String cubeName, @RequestBody MigrationRequest request) {
        CubeInstance cube = getCubeInstance(cubeName);
        try {
            MigrationRuleSet.Context ctx = new MigrationRuleSet.Context(queryService, cube,
                    getTargetHost(request.getTargetHost()), request.getProjectName());
            migrationService.approve(cube, ctx);
        } catch (Exception e) {
            throw new BadRequestException(e.getMessage());
        }
        return "Cube " + cubeName + " migrated.";
    }

    private String getTargetHost(String targetHost) {
        return Strings.isNullOrEmpty(targetHost) ? this.targetHost : targetHost;
    }

    @RequestMapping(value = "/checkStreamTableCompatibility", method = { RequestMethod.POST })
    @ResponseBody
    public void checkStreamTableCompatibility(@RequestBody StreamTableCompatibilityCheckRequest request) {
        TableDesc tableDesc = null;
        try {
            tableDesc = JsonUtil.readValue(request.getTableDesc(), TableDesc.class);
            // check table desc
            logger.info("Stream table compatibility check for table {}, project {}",
                    tableDesc.getName(), tableDesc.getProject());
            tableService.checkStreamTableCompatibility(request.getProjectName(), tableDesc);
            logger.info("Pass stream table compatibility check for table {}, project {}",
                    tableDesc.getName(), tableDesc.getProject());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new ConflictException(e.getMessage(), e);
        }

        // check stream source config
        StreamingSourceConfig config = null;
        try {
            config = JsonUtil.readValue(request.getStreamSource(), StreamingSourceConfig.class);
            logger.info("Stream source config compatibility check for table {}, project {}",
                    tableDesc.getName(), tableDesc.getProject());
            streamingV2Service.checkStreamingSourceCompatibility(request.getProjectName(), config);
            logger.info("Pass stream source config compatibility check for table {}, project {}",
                    tableDesc.getName(), tableDesc.getProject());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new ConflictException(e.getMessage(), e);
        }
    }

    /**
     * Check the schema compatibility for table, model desc
     */
    @RequestMapping(value = "/checkCompatibility", method = { RequestMethod.POST })
    @ResponseBody
    public void checkCompatibility(@RequestBody CompatibilityCheckRequest request) {
        try {
            List<TableDesc> tableDescList = deserializeTableDescList(request);
            for (TableDesc tableDesc : tableDescList) {
                logger.info("Schema compatibility check for table {}", tableDesc.getName());
                tableService.checkTableCompatibility(request.getProjectName(), tableDesc);
                logger.info("Pass schema compatibility check for table {}", tableDesc.getName());
            }
            DataModelDesc dataModelDesc = JsonUtil.readValue(request.getModelDescData(), DataModelDesc.class);
            logger.info("Schema compatibility check for model {}", dataModelDesc.getName());
            modelService.checkModelCompatibility(dataModelDesc, tableDescList);
            logger.info("Pass schema compatibility check for model {}", dataModelDesc.getName());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new ConflictException(e.getMessage(), e);
        }
    }

    /**
     * Check the schema compatibility for table
     */
    @RequestMapping(value = "/checkCompatibility/hiveTable", method = { RequestMethod.POST })
    @ResponseBody
    public void checkHiveTableCompatibility(@RequestBody CompatibilityCheckRequest request) {
        try {
            List<TableDesc> tableDescList = deserializeTableDescList(request);
            for (TableDesc tableDesc : tableDescList) {
                logger.info("Schema compatibility check for table {}", tableDesc.getName());
                tableService.checkHiveTableCompatibility(request.getProjectName(), tableDesc);
                logger.info("Pass schema compatibility check for table {}", tableDesc.getName());
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new ConflictException(e.getMessage(), e);
        }
    }

    private List<TableDesc> deserializeTableDescList(CompatibilityCheckRequest request) {
        List<TableDesc> result = Lists.newArrayList();
        try {
            for (String tableDescData : request.getTableDescDataList()) {
                TableDesc tableDesc = JsonUtil.readValue(tableDescData, TableDesc.class);
                for (ColumnDesc columnDesc : tableDesc.getColumns()) {
                    columnDesc.init(tableDesc);
                }
                result.add(tableDesc);
            }
        } catch (JsonParseException | JsonMappingException e) {
            throw new BadRequestException("Fail to parse table description: " + e);
        } catch (IOException e) {
            throw new InternalErrorException("Failed to deal with the request:" + e.getMessage(), e);
        }
        return result;
    }

    private boolean isStreamingTable(CubeInstance cube) {
        return cube.getDescriptor().getModel().getRootFactTable().getTableDesc().isStreamingTable();
    }
}

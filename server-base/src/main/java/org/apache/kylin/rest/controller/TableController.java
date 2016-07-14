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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.engine.streaming.StreamingConfig;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.request.CardinalityRequest;
import org.apache.kylin.rest.request.HiveTableRequest;
import org.apache.kylin.rest.request.StreamingRequest;
import org.apache.kylin.rest.response.TableDescResponse;
import org.apache.kylin.rest.service.CubeService;
import org.apache.kylin.rest.service.KafkaConfigService;
import org.apache.kylin.rest.service.ModelService;
import org.apache.kylin.rest.service.ProjectService;
import org.apache.kylin.rest.service.StreamingService;
import org.apache.kylin.source.hive.HiveClient;
import org.apache.kylin.source.kafka.config.KafkaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.collect.Sets;

/**
 * @author xduo
 */
@Controller
@RequestMapping(value = "/tables")
public class TableController extends BasicController {
    private static final Logger logger = LoggerFactory.getLogger(TableController.class);

    @Autowired
    private CubeService cubeMgmtService;
    @Autowired
    private ProjectService projectService;
    @Autowired
    private StreamingService streamingService;
    @Autowired
    private KafkaConfigService kafkaConfigService;
    @Autowired
    private ModelService modelService;

    /**
     * Get available table list of the input database
     *
     * @return Table metadata array
     * @throws IOException
     */
    @RequestMapping(value = "", method = { RequestMethod.GET })
    @ResponseBody
    public List<TableDesc> getHiveTables(@RequestParam(value = "ext", required = false) boolean withExt, @RequestParam(value = "project", required = false) String project) {
        long start = System.currentTimeMillis();
        List<TableDesc> tables = null;
        try {
            tables = cubeMgmtService.getProjectManager().listDefinedTables(project);
        } catch (Exception e) {
            logger.error("Failed to deal with the request.", e);
            throw new InternalErrorException(e.getLocalizedMessage());
        }

        if (withExt) {
            tables = cloneTableDesc(tables);
        }
        long end = System.currentTimeMillis();
        logger.info("Return all table metadata in " + (end - start) + " seconds");

        return tables;
    }

    /**
     * Get available table list of the input database
     *
     * @return Table metadata array
     * @throws IOException
     */
    @RequestMapping(value = "/{tableName:.+}", method = { RequestMethod.GET })
    @ResponseBody
    public TableDesc getHiveTable(@PathVariable String tableName) {
        return cubeMgmtService.getMetadataManager().getTableDesc(tableName);
    }

    /**
     * Get available table list of the input database
     *
     * @return Table metadata array
     * @throws IOException
     */
    @RequestMapping(value = "/{tableName}/exd-map", method = { RequestMethod.GET })
    @ResponseBody
    public Map<String, String> getHiveTableExd(@PathVariable String tableName) {
        Map<String, String> tableExd = cubeMgmtService.getMetadataManager().getTableDescExd(tableName);
        return tableExd;
    }

    @RequestMapping(value = "/reload", method = { RequestMethod.PUT })
    @ResponseBody
    public String reloadSourceTable() {
        cubeMgmtService.getMetadataManager().reload();
        return "ok";
    }

    @RequestMapping(value = "/{tables}/{project}", method = { RequestMethod.POST })
    @ResponseBody
    public Map<String, String[]> loadHiveTable(@PathVariable String tables, @PathVariable String project, @RequestBody HiveTableRequest request) throws IOException {
        String submitter = SecurityContextHolder.getContext().getAuthentication().getName();
        String[] loaded = cubeMgmtService.reloadHiveTable(tables);
        if (request.isCalculate()) {
            cubeMgmtService.calculateCardinalityIfNotPresent(loaded, submitter);
        }
        cubeMgmtService.syncTableToProject(loaded, project);
        Map<String, String[]> result = new HashMap<String, String[]>();
        result.put("result.loaded", loaded);
        result.put("result.unloaded", new String[] {});
        return result;
    }

    @RequestMapping(value = "/{tables}/{project}", method = { RequestMethod.DELETE })
    @ResponseBody
    public Map<String, String[]> unLoadHiveTables(@PathVariable String tables, @PathVariable String project) {
        Set<String> unLoadSuccess = Sets.newHashSet();
        Set<String> unLoadFail = Sets.newHashSet();
        Map<String, String[]> result = new HashMap<String, String[]>();
        for (String tableName : tables.split(",")) {
            if (unLoadHiveTable(tableName, project)) {
                unLoadSuccess.add(tableName);
            } else {
                unLoadFail.add(tableName);
            }
        }
        result.put("result.unload.success", (String[]) unLoadSuccess.toArray(new String[unLoadSuccess.size()]));
        result.put("result.unload.fail", (String[]) unLoadFail.toArray(new String[unLoadFail.size()]));
        return result;
    }

    /**
     * table may referenced by several projects, and kylin only keep one copy of meta for each table,
     * that's why we have two if statement here.
     * @param tableName
     * @param project
     * @return
     */
    private boolean unLoadHiveTable(String tableName, String project) {
        boolean rtn = false;
        int tableType = 0;

        //remove streaming info
        String[] dbTableName = HadoopUtil.parseHiveTableName(tableName);
        tableName = dbTableName[0] + "." + dbTableName[1];
        TableDesc desc = cubeMgmtService.getMetadataManager().getTableDesc(tableName);
        if(desc == null)
            return false;
        tableType = desc.getSourceType();

        try {
            if (!modelService.isTableInModel(tableName, project)) {
                cubeMgmtService.removeTableFromProject(tableName, project);
                rtn = true;
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        if (!projectService.isTableInAnyProject(tableName) && !modelService.isTableInAnyModel(tableName)) {
            try {
                cubeMgmtService.unLoadHiveTable(tableName);
                rtn = true;
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
                rtn = false;
            }
        }

        if (tableType == 1 && !projectService.isTableInAnyProject(tableName) && !modelService.isTableInAnyModel(tableName)) {
            StreamingConfig config = null;
            KafkaConfig kafkaConfig = null;
            try {
                config = streamingService.getStreamingManager().getStreamingConfig(tableName);
                kafkaConfig = kafkaConfigService.getKafkaConfig(tableName);
                streamingService.dropStreamingConfig(config);
                kafkaConfigService.dropKafkaConfig(kafkaConfig);
                rtn = true;
            } catch (Exception e) {
                rtn = false;
                logger.error(e.getLocalizedMessage(), e);
            }
        }
        return rtn;
    }

    @RequestMapping(value = "/addStreamingSrc", method = { RequestMethod.POST })
    @ResponseBody
    public Map<String, String> addStreamingTable(@RequestBody StreamingRequest request) throws IOException {
        Map<String, String> result = new HashMap<String, String>();
        String project = request.getProject();
        TableDesc desc = JsonUtil.readValue(request.getTableData(), TableDesc.class);
        desc.setUuid(UUID.randomUUID().toString());
        MetadataManager metaMgr = MetadataManager.getInstance(KylinConfig.getInstanceFromEnv());
        metaMgr.saveSourceTable(desc);
        cubeMgmtService.syncTableToProject(new String[] { desc.getName() }, project);
        result.put("success", "true");
        return result;
    }

    /**
     * Regenerate table cardinality
     *
     * @return Table metadata array
     * @throws IOException
     */
    @RequestMapping(value = "/{tableNames}/cardinality", method = { RequestMethod.PUT })
    @ResponseBody
    public CardinalityRequest generateCardinality(@PathVariable String tableNames, @RequestBody CardinalityRequest request) {
        String submitter = SecurityContextHolder.getContext().getAuthentication().getName();
        String[] tables = tableNames.split(",");
        for (String table : tables) {
            cubeMgmtService.calculateCardinality(table.trim().toUpperCase(), submitter);
        }
        return request;
    }

    /**
     * @param tables
     * @return
     */
    private List<TableDesc> cloneTableDesc(List<TableDesc> tables) {
        if (null == tables) {
            return Collections.emptyList();
        }

        List<TableDesc> descs = new ArrayList<TableDesc>();
        Iterator<TableDesc> it = tables.iterator();
        while (it.hasNext()) {
            TableDesc table = it.next();
            Map<String, String> exd = cubeMgmtService.getMetadataManager().getTableDescExd(table.getIdentity());
            if (exd == null) {
                descs.add(table);
            } else {
                // Clone TableDesc
                TableDescResponse rtableDesc = new TableDescResponse(table);
                rtableDesc.setDescExd(exd);
                if (exd.containsKey(MetadataConstants.TABLE_EXD_CARDINALITY)) {
                    Map<String, Long> cardinality = new HashMap<String, Long>();
                    String scard = exd.get(MetadataConstants.TABLE_EXD_CARDINALITY);
                    if (!StringUtils.isEmpty(scard)) {
                        String[] cards = StringUtils.split(scard, ",");
                        ColumnDesc[] cdescs = rtableDesc.getColumns();
                        for (int i = 0; i < cdescs.length; i++) {
                            ColumnDesc columnDesc = cdescs[i];
                            if (cards.length > i) {
                                cardinality.put(columnDesc.getName(), Long.parseLong(cards[i]));
                            } else {
                                logger.error("The result cardinality is not identical with hive table metadata, cardinaly : " + scard + " column array length: " + cdescs.length);
                                break;
                            }
                        }
                        rtableDesc.setCardinality(cardinality);
                    }
                }
                descs.add(rtableDesc);
            }
        }
        return descs;
    }

    /**
     * Show all databases in Hive
     *
     * @return Hive databases list
     * @throws IOException
     */
    @RequestMapping(value = "/hive", method = { RequestMethod.GET })
    @ResponseBody
    private static List<String> showHiveDatabases() throws IOException {
        HiveClient hiveClient = new HiveClient();
        List<String> results = null;

        try {
            results = hiveClient.getHiveDbNames();
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException(e);
        }
        return results;
    }

    /**
     * Show all tables in a Hive database
     *
     * @return Hive table list
     * @throws IOException
     */
    @RequestMapping(value = "/hive/{database}", method = { RequestMethod.GET })
    @ResponseBody
    private static List<String> showHiveTables(@PathVariable String database) throws IOException {
        HiveClient hiveClient = new HiveClient();
        List<String> results = null;

        try {
            results = hiveClient.getHiveTableNames(database);
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException(e);
        }
        return results;
    }

    public void setCubeService(CubeService cubeService) {
        this.cubeMgmtService = cubeService;
    }

}

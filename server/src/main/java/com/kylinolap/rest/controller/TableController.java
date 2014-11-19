/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.rest.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
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

import com.codahale.metrics.annotation.Metered;
import com.kylinolap.metadata.MetadataConstances;
import com.kylinolap.metadata.model.schema.ColumnDesc;
import com.kylinolap.metadata.model.schema.TableDesc;
import com.kylinolap.rest.exception.InternalErrorException;
import com.kylinolap.rest.request.CardinalityRequest;
import com.kylinolap.rest.response.TableDescResponse;
import com.kylinolap.rest.service.CubeService;

/**
 * @author xduo
 * 
 */
@Controller
@RequestMapping(value = "/tables")
public class TableController extends BasicController {
    private static final Logger logger = LoggerFactory.getLogger(TableController.class);

    @Autowired
    private CubeService cubeMgmtService;

    /**
     * Get available table list of the input database
     * 
     * @return Table metadata array
     * @throws IOException
     */
    @RequestMapping(value = "", method = { RequestMethod.GET })
    @ResponseBody
    @Metered(name = "listSourceTables")
    public List<TableDesc> getHiveTables(@RequestParam(value = "ext", required = false) boolean withExt,@RequestParam(value = "project", required = false) String project ) {
        long start = System.currentTimeMillis();
        List<TableDesc> tables = null;
        try {
                tables = cubeMgmtService.getProjectManager().listDefinedTablesInProject(project);
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
    @RequestMapping(value = "/{tableName}", method = { RequestMethod.GET })
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
    public Map<String, String[]> loadHiveTable(@PathVariable String tables,@PathVariable String project) throws IOException {
        String[] arr = cubeMgmtService.reloadHiveTable(tables);
        cubeMgmtService.syncTableToProject(tables, project);
        Map<String, String[]> result = new HashMap<String, String[]>();
        result.put("result", arr);
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
        String[] tables = tableNames.split(",");
        for (String table : tables) {
            cubeMgmtService.generateCardinality(table.trim(), request.getFormat(), request.getDelimiter());
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
            Map<String, String> exd = cubeMgmtService.getMetadataManager().getTableDescExd(table.getName());
            if (exd == null) {
                descs.add(table);
            } else {
                // Clone TableDesc
                TableDescResponse rtableDesc = new TableDescResponse(table);
                rtableDesc.setDescExd(exd);
                if (exd.containsKey(MetadataConstances.TABLE_EXD_CARDINALITY)) {
                    Map<String, Long> cardinality = new HashMap<String, Long>();
                    String scard = exd.get(MetadataConstances.TABLE_EXD_CARDINALITY);
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
}

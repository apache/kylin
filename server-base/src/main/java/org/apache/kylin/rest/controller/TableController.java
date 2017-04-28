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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.exception.NotFoundException;
import org.apache.kylin.rest.request.CardinalityRequest;
import org.apache.kylin.rest.request.HiveTableRequest;
import org.apache.kylin.rest.service.TableService;
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
    private TableService tableService;

    /**
     * Get available table list of the project
     *
     * @return Table metadata array
     * @throws IOException
     */
    @RequestMapping(value = "", method = { RequestMethod.GET })
    @ResponseBody
    public List<TableDesc> getTableDesc(@RequestParam(value = "ext", required = false) boolean withExt, @RequestParam(value = "project", required = true) String project) throws IOException {
        try {
            return tableService.getTableDescByProject(project, withExt);
        } catch (IOException e) {
            logger.error("Failed to get Hive Tables", e);
            throw new InternalErrorException(e.getLocalizedMessage());
        }
    }

    /**
     * Get available table list of the input database
     *
     * @return Table metadata array
     * @throws IOException
     */
    @RequestMapping(value = "/{tableName:.+}", method = { RequestMethod.GET })
    @ResponseBody
    public TableDesc getTableDesc(@PathVariable String tableName) {
        TableDesc table = tableService.getTableDescByName(tableName, false);
        if (table == null)
            throw new NotFoundException("Could not find Hive table: " + tableName);
        return table;
    }

    @RequestMapping(value = "/{tables}/{project}", method = { RequestMethod.POST })
    @ResponseBody
    public Map<String, String[]> loadHiveTables(@PathVariable String tables, @PathVariable String project, @RequestBody HiveTableRequest request) throws IOException {
        String submitter = SecurityContextHolder.getContext().getAuthentication().getName();
        Map<String, String[]> result = new HashMap<String, String[]>();
        String[] tableNames = StringUtil.splitAndTrim(tables, ",");
        try {
            String[] loaded = tableService.loadHiveTablesToProject(tableNames, project);
            result.put("result.loaded", loaded);
            Set<String> allTables = new HashSet<String>();
            for (String tableName : tableNames) {
                allTables.add(tableService.normalizeHiveTableName(tableName));
            }
            for (String loadedTableName : loaded) {
                allTables.remove(loadedTableName);
            }
            String[] unloaded = new String[allTables.size()];
            allTables.toArray(unloaded);
            result.put("result.unloaded", unloaded);
            if (request.isCalculate()) {
                tableService.calculateCardinalityIfNotPresent(loaded, submitter);
            }
        } catch (Throwable e) {
            logger.error("Failed to load Hive Table", e);
            throw new InternalErrorException(e.getLocalizedMessage());
        }
        return result;
    }

    @RequestMapping(value = "/{tables}/{project}", method = { RequestMethod.DELETE })
    @ResponseBody
    public Map<String, String[]> unLoadHiveTables(@PathVariable String tables, @PathVariable String project) {
        Set<String> unLoadSuccess = Sets.newHashSet();
        Set<String> unLoadFail = Sets.newHashSet();
        Map<String, String[]> result = new HashMap<String, String[]>();
        try {
            for (String tableName : tables.split(",")) {
                if (tableService.unLoadHiveTable(tableName, project)) {
                    unLoadSuccess.add(tableName);
                } else {
                    unLoadFail.add(tableName);
                }
            }
        } catch (Throwable e) {
            logger.error("Failed to unload Hive Table", e);
            throw new InternalErrorException(e.getLocalizedMessage());
        }
        result.put("result.unload.success", (String[]) unLoadSuccess.toArray(new String[unLoadSuccess.size()]));
        result.put("result.unload.fail", (String[]) unLoadFail.toArray(new String[unLoadFail.size()]));
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
    public CardinalityRequest generateCardinality(@PathVariable String tableNames, @RequestBody CardinalityRequest request) throws IOException {
        String submitter = SecurityContextHolder.getContext().getAuthentication().getName();
        String[] tables = tableNames.split(",");
        try {
            for (String table : tables) {
                tableService.calculateCardinality(table.trim().toUpperCase(), submitter);
            }
        } catch (IOException e) {
            logger.error("Failed to calculate cardinality", e);
            throw new InternalErrorException(e.getLocalizedMessage());
        }
        return request;
    }

    /**
     * Show all databases in Hive
     *
     * @return Hive databases list
     * @throws IOException
     */
    @RequestMapping(value = "/hive", method = { RequestMethod.GET })
    @ResponseBody
    private List<String> showHiveDatabases() throws IOException {
        try {
            return tableService.getHiveDbNames();
        } catch (Throwable e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException(e.getLocalizedMessage());
        }
    }

    /**
     * Show all tables in a Hive database
     *
     * @return Hive table list
     * @throws IOException
     */
    @RequestMapping(value = "/hive/{database}", method = { RequestMethod.GET })
    @ResponseBody
    private List<String> showHiveTables(@PathVariable String database) throws IOException {
        try {
            return tableService.getHiveTableNames(database);
        } catch (Throwable e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException(e.getLocalizedMessage());
        }
    }

}

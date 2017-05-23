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

package org.apache.kylin.rest.controller2;

import java.io.IOException;

import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.request.HiveTableRequestV2;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.TableServiceV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * @author xduo
 */
@Controller
@RequestMapping(value = "/tables")
public class TableControllerV2 extends BasicController {

    private static final Logger logger = LoggerFactory.getLogger(TableControllerV2.class);

    @Autowired
    @Qualifier("tableServiceV2")
    private TableServiceV2 tableServiceV2;

    /**
     * Get available table list of the project
     *
     * @return Table metadata array
     * @throws IOException
     */

    @RequestMapping(value = "", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getTableDescV2(@RequestHeader("Accept-Language") String lang, @RequestParam(value = "ext", required = false) boolean withExt, @RequestParam(value = "project", required = true) String project) throws IOException {
        MsgPicker.setMsg(lang);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, tableServiceV2.getTableDescByProject(project, withExt), "");
    }

    /**
     * Get available table list of the input database
     *
     * @return Table metadata array
     * @throws IOException
     */

    @RequestMapping(value = "/{tableName:.+}", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getTableDescV2(@RequestHeader("Accept-Language") String lang, @PathVariable String tableName) {
        MsgPicker.setMsg(lang);
        Message msg = MsgPicker.getMsg();

        TableDesc table = tableServiceV2.getTableDescByName(tableName, false);
        if (table == null)
            throw new BadRequestException(String.format(msg.getHIVE_TABLE_NOT_FOUND(), tableName));
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, table, "");
    }

    @RequestMapping(value = "/load", method = { RequestMethod.POST }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse loadHiveTablesV2(@RequestHeader("Accept-Language") String lang, @RequestBody HiveTableRequestV2 requestV2) throws Exception {
        MsgPicker.setMsg(lang);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, tableServiceV2.loadHiveTables(requestV2.getTables(), requestV2.getProject(), requestV2.isNeedProfile()), "");
    }

    @RequestMapping(value = "/load", method = { RequestMethod.DELETE }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse unLoadHiveTablesV2(@RequestHeader("Accept-Language") String lang, @RequestBody HiveTableRequestV2 requestV2) throws IOException {
        MsgPicker.setMsg(lang);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, tableServiceV2.unloadHiveTables(requestV2.getTables(), requestV2.getProject()), "");
    }

    /**
     * Regenerate table cardinality
     *
     * @return Table metadata array
     * @throws IOException
     */

    @RequestMapping(value = "/cardinality", method = { RequestMethod.POST }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public void generateCardinalityV2(@RequestHeader("Accept-Language") String lang, @RequestBody HiveTableRequestV2 requestV2) throws Exception {
        MsgPicker.setMsg(lang);

        String submitter = SecurityContextHolder.getContext().getAuthentication().getName();
        String[] tables = requestV2.getTables();

        for (String table : tables) {
            tableServiceV2.calculateCardinality(table.toUpperCase(), submitter);
        }
    }

    /**
     * Show all databases in Hive
     *
     * @return Hive databases list
     * @throws IOException
     */

    @RequestMapping(value = "/hive", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    private EnvelopeResponse showHiveDatabasesV2(@RequestHeader("Accept-Language") String lang) throws Exception {
        MsgPicker.setMsg(lang);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, tableServiceV2.getHiveDbNames(), "");
    }

    /**
     * Show all tables in a Hive database
     *
     * @return Hive table list
     * @throws IOException
     */

    @RequestMapping(value = "/hive/{database}", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    private EnvelopeResponse showHiveTablesV2(@RequestHeader("Accept-Language") String lang, @PathVariable String database) throws Exception {
        MsgPicker.setMsg(lang);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, tableServiceV2.getHiveTableNames(database), "");
    }

}

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
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeSet;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.QueryContextFacade;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.metadata.querymeta.TableMeta;
import org.apache.kylin.rest.exception.ForbiddenException;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.model.Query;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.request.MetaRequest;
import org.apache.kylin.rest.request.PrepareSqlRequest;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.request.SaveSqlRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.service.QueryService;
import org.apache.kylin.rest.util.ValidateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.supercsv.io.CsvListWriter;
import org.supercsv.io.ICsvListWriter;
import org.supercsv.prefs.CsvPreference;

import org.apache.kylin.shaded.com.google.common.collect.Maps;

/**
 * Handle query requests.
 * 
 * @author xduo
 */
@Controller
public class QueryController extends BasicController {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(QueryController.class);
    private static String BOM_CHARACTER;
    @Autowired
    @Qualifier("queryService")
    private QueryService queryService;

    {
        BOM_CHARACTER = new String(new byte[] { (byte) 0xEF, (byte) 0xBB, (byte) 0xBF }, StandardCharsets.UTF_8);
    }

    @RequestMapping(value = "/query", method = RequestMethod.POST, produces = { "application/json" })
    @ResponseBody
    public SQLResponse query(@RequestBody PrepareSqlRequest sqlRequest) {
        return queryService.doQueryWithCache(sqlRequest);
    }

    // TODO should be just "prepare" a statement, get back expected ResultSetMetaData
    @RequestMapping(value = "/query/prestate", method = RequestMethod.POST, produces = "application/json")
    @ResponseBody
    public SQLResponse prepareQuery(@RequestBody PrepareSqlRequest sqlRequest) {
        Map<String, String> newToggles = Maps.newHashMap();
        if (sqlRequest.getBackdoorToggles() != null)
            newToggles.putAll(sqlRequest.getBackdoorToggles());
        newToggles.put(BackdoorToggles.DEBUG_TOGGLE_PREPARE_ONLY, "true");
        sqlRequest.setBackdoorToggles(newToggles);

        return queryService.doQueryWithCache(sqlRequest);
    }

    @RequestMapping(value = "/saved_queries", method = RequestMethod.POST, produces = { "application/json" })
    @ResponseBody
    public void saveQuery(@RequestBody SaveSqlRequest sqlRequest) throws IOException {
        String creator = SecurityContextHolder.getContext().getAuthentication().getName();
        Query newQuery = new Query(sqlRequest.getName(), sqlRequest.getProject(), sqlRequest.getSql(),
                sqlRequest.getDescription());

        queryService.saveQuery(creator, newQuery);
    }

    @RequestMapping(value = "/saved_queries/{id}", method = RequestMethod.DELETE, produces = { "application/json" })
    @ResponseBody
    public void removeQuery(@PathVariable String id) throws IOException {
        String creator = SecurityContextHolder.getContext().getAuthentication().getName();
        queryService.removeQuery(creator, id);
    }

    @RequestMapping(value = "/saved_queries", method = RequestMethod.GET, produces = { "application/json" })
    @ResponseBody
    public List<Query> getQueries(@RequestParam(value = "project", required = false) String project)
            throws IOException {
        String creator = SecurityContextHolder.getContext().getAuthentication().getName();
        return queryService.getQueries(creator, project);
    }

    @RequestMapping(value = "/query/format/{format}", method = RequestMethod.GET, produces = { "application/json" })
    @ResponseBody
    public void downloadQueryResult(@PathVariable String format, SQLRequest sqlRequest, HttpServletResponse response) {
        KylinConfig config = queryService.getConfig();
        Message msg = MsgPicker.getMsg();

        if ((isAdmin() && !config.isAdminUserExportAllowed())
                || (!isAdmin() && !config.isNoneAdminUserExportAllowed())) {
            throw new ForbiddenException(msg.getEXPORT_RESULT_NOT_ALLOWED());
        }

        SQLResponse result = queryService.doQueryWithCache(sqlRequest);
        response.setContentType("text/" + format + ";charset=utf-8");

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS", Locale.ROOT);
        Date now = new Date();
        String nowStr = sdf.format(now);
        response.setHeader("Content-Disposition",
                "attachment; filename=\"" + ValidateUtil.convertStringToBeAlphanumericUnderscore(nowStr) + ".result."
                        + ValidateUtil.convertStringToBeAlphanumericUnderscore(format) + "\"");
        ICsvListWriter csvWriter = null;

        try {
            csvWriter = new CsvListWriter(response.getWriter(), CsvPreference.STANDARD_PREFERENCE);

            List<String> headerList = new ArrayList<String>();

            for (SelectedColumnMeta column : result.getColumnMetas()) {
                headerList.add(column.getLabel());
            }

            // KYLIN-3939
            // Add BOM character,slove the bug that it shows Chinese garbled when using
            // excel to open scv file on windows.
            // BOM character should add on head of CSV file.
            // So add it to the head of the first index of headerList.
            if (headerList.size() > 0) {
                String tmpHeaderFirst = headerList.get(0);
                String headerFirst = BOM_CHARACTER.concat(tmpHeaderFirst);
                headerList.set(0, headerFirst);
            }

            String[] headers = new String[headerList.size()];
            csvWriter.writeHeader(headerList.toArray(headers));

            for (List<String> row : result.getResults()) {
                csvWriter.write(row);
            }
        } catch (IOException e) {
            throw new InternalErrorException(e);
        } finally {
            IOUtils.closeQuietly(csvWriter);
        }
    }

    @RequestMapping(value = "/tables_and_columns", method = RequestMethod.GET, produces = { "application/json" })
    @ResponseBody
    public List<TableMeta> getMetadata(MetaRequest metaRequest) throws IOException {
        try {
            return queryService.getMetadataFilterByUser(metaRequest.getProject());
        } catch (SQLException e) {
            throw new InternalErrorException(e.getLocalizedMessage(), e);
        }
    }

    /**
     *
     * @param runTimeMoreThan in seconds
     * @return
     */
    @RequestMapping(value = "/query/runningQueries", method = RequestMethod.GET)
    @ResponseBody
    public TreeSet<QueryContext> getRunningQueries(
            @RequestParam(value = "runTimeMoreThan", required = false, defaultValue = "-1") int runTimeMoreThan) {
        if (runTimeMoreThan == -1) {
            return QueryContextFacade.getAllRunningQueries();
        } else {
            return QueryContextFacade.getLongRunningQueries(runTimeMoreThan * 1000L);
        }
    }

    @RequestMapping(value = "/query/{queryId}/stop", method = RequestMethod.PUT)
    @ResponseBody
    public void stopQuery(@PathVariable String queryId) {
        final String user = SecurityContextHolder.getContext().getAuthentication().getName();
        logger.info("{} tries to stop the query: {}, but not guaranteed to succeed.", user, queryId);
        QueryContextFacade.stopQuery(queryId, "stopped by " + user);
    }

    public void setQueryService(QueryService queryService) {
        this.queryService = queryService;
    }
}

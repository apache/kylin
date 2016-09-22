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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.metrics.QueryMetricsFacade;
import org.apache.kylin.rest.model.Query;
import org.apache.kylin.rest.model.SelectedColumnMeta;
import org.apache.kylin.rest.model.TableMeta;
import org.apache.kylin.rest.request.MetaRequest;
import org.apache.kylin.rest.request.PrepareSqlRequest;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.request.SaveSqlRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.service.QueryService;
import org.apache.kylin.rest.util.QueryUtil;
import org.apache.kylin.storage.exception.ScanOutOfLimitException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.supercsv.io.CsvListWriter;
import org.supercsv.io.ICsvListWriter;
import org.supercsv.prefs.CsvPreference;

import com.google.common.base.Preconditions;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

/**
 * Handle query requests.
 * 
 * @author xduo
 */
@Controller
public class QueryController extends BasicController {

    private static final Logger logger = LoggerFactory.getLogger(QueryController.class);

    public static final String SUCCESS_QUERY_CACHE = "StorageCache";
    public static final String EXCEPTION_QUERY_CACHE = "ExceptionQueryCache";

    @Autowired
    private QueryService queryService;

    @Autowired
    private CacheManager cacheManager;

    @PostConstruct
    public void init() throws IOException {
        Preconditions.checkNotNull(cacheManager, "cacheManager is not injected yet");
    }

    @RequestMapping(value = "/query", method = RequestMethod.POST)
    @ResponseBody
    public SQLResponse query(@RequestBody SQLRequest sqlRequest) {
        return doQueryWithCache(sqlRequest);
    }

    // TODO should be just "prepare" a statement, get back expected ResultSetMetaData
    @RequestMapping(value = "/query/prestate", method = RequestMethod.POST, produces = "application/json")
    @ResponseBody
    public SQLResponse prepareQuery(@RequestBody PrepareSqlRequest sqlRequest) {
        return doQueryWithCache(sqlRequest);
    }

    @RequestMapping(value = "/saved_queries", method = RequestMethod.POST)
    @ResponseBody
    public void saveQuery(@RequestBody SaveSqlRequest sqlRequest) throws IOException {
        String creator = SecurityContextHolder.getContext().getAuthentication().getName();
        Query newQuery = new Query(sqlRequest.getName(), sqlRequest.getProject(), sqlRequest.getSql(), sqlRequest.getDescription());

        queryService.saveQuery(creator, newQuery);
    }

    @RequestMapping(value = "/saved_queries/{id}", method = RequestMethod.DELETE)
    @ResponseBody
    public void removeQuery(@PathVariable String id) throws IOException {
        String creator = SecurityContextHolder.getContext().getAuthentication().getName();
        queryService.removeQuery(creator, id);
    }

    @RequestMapping(value = "/saved_queries", method = RequestMethod.GET)
    @ResponseBody
    public List<Query> getQueries() throws IOException {
        String creator = SecurityContextHolder.getContext().getAuthentication().getName();
        return queryService.getQueries(creator);
    }

    @RequestMapping(value = "/query/format/{format}", method = RequestMethod.GET)
    @ResponseBody
    public void downloadQueryResult(@PathVariable String format, SQLRequest sqlRequest, HttpServletResponse response) {
        SQLResponse result = doQueryWithCache(sqlRequest);
        response.setContentType("text/" + format + ";charset=utf-8");
        response.setHeader("Content-Disposition", "attachment; filename=\"result." + format + "\"");
        ICsvListWriter csvWriter = null;

        try {
            csvWriter = new CsvListWriter(response.getWriter(), CsvPreference.STANDARD_PREFERENCE);

            List<String> headerList = new ArrayList<String>();

            for (SelectedColumnMeta column : result.getColumnMetas()) {
                headerList.add(column.getName());
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

    @RequestMapping(value = "/tables_and_columns", method = RequestMethod.GET)
    @ResponseBody
    public List<TableMeta> getMetadata(MetaRequest metaRequest) {
        try {
            return queryService.getMetadata(metaRequest.getProject());
        } catch (SQLException e) {
            throw new InternalErrorException(e.getLocalizedMessage(), e);
        }
    }

    private SQLResponse doQueryWithCache(SQLRequest sqlRequest) {
        try {
            BackdoorToggles.setToggles(sqlRequest.getBackdoorToggles());

            String sql = sqlRequest.getSql();
            String project = sqlRequest.getProject();
            logger.info("Using project: " + project);
            logger.info("The original query:  " + sql);

            String serverMode = KylinConfig.getInstanceFromEnv().getServerMode();
            if (!(Constant.SERVER_MODE_QUERY.equals(serverMode.toLowerCase()) || Constant.SERVER_MODE_ALL.equals(serverMode.toLowerCase()))) {
                throw new InternalErrorException("Query is not allowed in " + serverMode + " mode.");
            }

            if (!sql.toLowerCase().contains("select")) {
                logger.debug("Directly return exception as not supported");
                throw new InternalErrorException("Not Supported SQL.");
            }

            long startTime = System.currentTimeMillis();

            SQLResponse sqlResponse = searchQueryInCache(sqlRequest);
            try {
                if (null == sqlResponse) {
                    sqlResponse = queryService.query(sqlRequest);

                    long durationThreshold = KylinConfig.getInstanceFromEnv().getQueryDurationCacheThreshold();
                    long scancountThreshold = KylinConfig.getInstanceFromEnv().getQueryScanCountCacheThreshold();
                    sqlResponse.setDuration(System.currentTimeMillis() - startTime);
                    logger.info("Stats of SQL response: isException: {}, duration: {}, total scan count {}", //
                            String.valueOf(sqlResponse.getIsException()), String.valueOf(sqlResponse.getDuration()), String.valueOf(sqlResponse.getTotalScanCount()));
                    if (!sqlResponse.getIsException() && (sqlResponse.getDuration() > durationThreshold || sqlResponse.getTotalScanCount() > scancountThreshold)) {
                        cacheManager.getCache(SUCCESS_QUERY_CACHE).put(new Element(sqlRequest, sqlResponse));
                    }
                } else {
                    sqlResponse.setDuration(System.currentTimeMillis() - startTime);
                }

                checkQueryAuth(sqlResponse);

            } catch (Throwable e) { // calcite may throw AssertError
                logger.error("Exception when execute sql", e);
                String errMsg = QueryUtil.makeErrorMsgUserFriendly(e);

                sqlResponse = new SQLResponse(null, null, 0, true, errMsg);

                // for exception queries, only cache ScanOutOfLimitException
                if (e instanceof ScanOutOfLimitException) {
                    Cache exceptionCache = cacheManager.getCache(EXCEPTION_QUERY_CACHE);
                    exceptionCache.put(new Element(sqlRequest, sqlResponse));
                }
            }

            queryService.logQuery(sqlRequest, sqlResponse);

            QueryMetricsFacade.updateMetrics(sqlRequest, sqlResponse);

            if (sqlResponse.getIsException())
                throw new InternalErrorException(sqlResponse.getExceptionMessage());

            return sqlResponse;

        } finally {
            BackdoorToggles.cleanToggles();
        }
    }

    private SQLResponse searchQueryInCache(SQLRequest sqlRequest) {
        SQLResponse response = null;
        Cache exceptionCache = cacheManager.getCache(EXCEPTION_QUERY_CACHE);
        Cache successCache = cacheManager.getCache(SUCCESS_QUERY_CACHE);

        if (KylinConfig.getInstanceFromEnv().isQueryCacheEnabled() && !BackdoorToggles.getDisableCache()) {
            if (exceptionCache.get(sqlRequest) != null) {
                logger.info("The sqlResponse is found in EXCEPTION_QUERY_CACHE");
                Element element = exceptionCache.get(sqlRequest);
                response = (SQLResponse) element.getObjectValue();
                response.setHitExceptionCache(true);
            } else if (successCache.get(sqlRequest) != null) {
                logger.info("The sqlResponse is found in SUCCESS_QUERY_CACHE");
                Element element = successCache.get(sqlRequest);
                response = (SQLResponse) element.getObjectValue();
                response.setStorageCacheUsed(true);
            }
        }

        return response;
    }

    private void checkQueryAuth(SQLResponse sqlResponse) throws AccessDeniedException {
        if (!sqlResponse.getIsException() && KylinConfig.getInstanceFromEnv().isQuerySecureEnabled()) {
            queryService.checkAuthorization(sqlResponse.getCube());
        }
    }
    
    public void setQueryService(QueryService queryService) {
        this.queryService = queryService;
    }

    public void setCacheManager(CacheManager cacheManager) {
        this.cacheManager = cacheManager;
    }

}

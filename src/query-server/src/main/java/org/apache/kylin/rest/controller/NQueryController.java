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
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_DOWNLOAD_FILE;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_NAME;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_TABLE_REFRESH_PARAMETER;
import static org.apache.kylin.common.exception.ServerErrorCode.PERMISSION_DENIED;
import static org.apache.kylin.common.exception.ServerErrorCode.REDIS_CLEAR_ERROR;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.NativeQueryRealization;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.KylinTimeoutException;
import org.apache.kylin.common.exception.QueryErrorCode;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.transaction.StopQueryBroadcastEventNotifier;
import org.apache.kylin.common.scheduler.EventBusFactory;
import org.apache.kylin.fileseg.FileSegments;
import org.apache.kylin.fileseg.FileSegmentsDetector;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.query.QueryHistoryRequest;
import org.apache.kylin.metadata.query.util.QueryHisTransformStandardUtil;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.metadata.querymeta.TableMetaWithType;
import org.apache.kylin.query.plugin.profiler.AsyncProfiling;
import org.apache.kylin.rest.cluster.ClusterManager;
import org.apache.kylin.rest.exception.ForbiddenException;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.model.Query;
import org.apache.kylin.rest.request.PrepareSqlRequest;
import org.apache.kylin.rest.request.QueryDetectRequest;
import org.apache.kylin.rest.request.SQLFormatRequest;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.request.SaveSqlRequest;
import org.apache.kylin.rest.request.SyncFileSegmentsRequest;
import org.apache.kylin.rest.response.BigQueryResponse;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.QueryDetectResponse;
import org.apache.kylin.rest.response.QueryHistoryFiltersResponse;
import org.apache.kylin.rest.response.QueryStatisticsResponse;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.response.ServerExtInfoResponse;
import org.apache.kylin.rest.response.ServerInfoResponse;
import org.apache.kylin.rest.response.SyncFileSegmentsResponse;
import org.apache.kylin.rest.response.TableRefresh;
import org.apache.kylin.rest.response.TableRefreshAll;
import org.apache.kylin.rest.service.ModelService;
import org.apache.kylin.rest.service.QueryCacheManager;
import org.apache.kylin.rest.service.QueryHistoryService;
import org.apache.kylin.rest.service.QueryService;
import org.apache.kylin.rest.service.TableService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.util.DataRangeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.supercsv.io.CsvListWriter;
import org.supercsv.io.ICsvListWriter;
import org.supercsv.prefs.CsvPreference;

import io.swagger.annotations.ApiOperation;
import lombok.val;
import redis.clients.jedis.exceptions.JedisException;

/**
 * Handle query requests.
 * @author xduo
 */
@RestController
@RequestMapping(value = "/api/query", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class NQueryController extends NBasicController {
    public static final String CN = "zh-cn";
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(NQueryController.class);
    private static final Pattern queryNamePattern = Pattern.compile("^[a-zA-Z0-9_]*$");
    @Autowired
    @Qualifier("queryService")
    private QueryService queryService;

    @Autowired
    @Qualifier("queryHistoryService")
    private QueryHistoryService queryHistoryService;

    @Autowired
    private ClusterManager clusterManager;

    @Autowired
    private QueryCacheManager queryCacheManager;

    @Autowired
    private AclEvaluate aclEvaluate;

    @Autowired
    @Qualifier("tableService")
    private TableService tableService;

    private ModelService modelService;

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @ApiOperation(value = "query", tags = { "QE" }, notes = "")
    @GetMapping(value = "/profile/start")
    @ResponseBody
    public EnvelopeResponse<String> profile(@RequestParam(value = "params", required = false) String params) {
        aclEvaluate.checkIsGlobalAdmin();
        if (!KylinConfig.getInstanceFromEnv().asyncProfilingEnabled()) {
            throw new KylinException(QueryErrorCode.PROFILING_NOT_ENABLED, "async profiling is not enabled");
        }
        AsyncProfiling.start(params);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "query", tags = { "QE" }, notes = "")
    @GetMapping(value = "/profile/dump")
    @ResponseBody
    public EnvelopeResponse<String> stopProfile(@RequestParam(value = "params", required = false) String params,
            HttpServletResponse response) throws IOException {
        aclEvaluate.checkIsGlobalAdmin();
        if (!KylinConfig.getInstanceFromEnv().asyncProfilingEnabled()) {
            throw new KylinException(QueryErrorCode.PROFILING_NOT_ENABLED, "async profiling is not enabled");
        }
        AsyncProfiling.dump(params);
        response.setContentType("application/zip");
        response.setHeader("Content-Disposition",
                "attachment; filename=\"ke-async-prof-result-" + System.currentTimeMillis() + ".zip\"");
        AsyncProfiling.waitForResult(response.getOutputStream());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "query", tags = {
            "QE" }, notes = "Update Param: query_id, accept_partial, backdoor_toggles, cache_key")
    @PostMapping(value = "")
    @ResponseBody
    public EnvelopeResponse<SQLResponse> query(@Valid @RequestBody PrepareSqlRequest sqlRequest,
            @RequestHeader(value = "User-Agent") String userAgent) {
        checkForcedToParams(sqlRequest);
        checkProjectName(sqlRequest.getProject());
        sqlRequest.setUserAgent(userAgent != null ? userAgent : "");
        QueryContext.current().record("end_http_proc");

        // take chance of push-down to detect and apply file segment changes
        boolean detectFileSegments = sqlRequest.isForcedToPushDown();
        if (detectFileSegments)
            FileSegmentsDetector.startLocally(sqlRequest.getProject());

        try {
            SQLResponse sqlResponse = queryService.queryWithCache(sqlRequest);

            if (detectFileSegments) {
                FileSegmentsDetector.report(finding -> {
                    if (FileSegments.isSyncFileSegSql(sqlRequest.getSql())) {
                        // return modelIds to syncFileSegments()
                        sqlResponse.addNativeRealizationIfNotExist(finding.modelId);
                    }
                    modelService.forceFileSegments(finding.project, finding.modelId, finding.storageLocation,
                            Optional.of(finding.fileHashs), SegmentStatusEnum.NEW);
                });
            }

            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, sqlResponse, "");

        } finally {
            if (detectFileSegments)
                FileSegmentsDetector.endLocally();
        }
    }

    @PostMapping(value = "/sync_file_segments")
    @ResponseBody
    public EnvelopeResponse<SyncFileSegmentsResponse> syncFileSegments(@RequestBody SyncFileSegmentsRequest req) {
        String project = req.getProject();
        checkProjectName(project);

        String inputSql = req.getSql();
        List<NDataModel> inputModels = FileSegments.findModelsOfFileSeg(project, req.getModelAliasOrFactTables());

        StringBuilder probeSql = new StringBuilder();
        if (!StringUtils.isBlank(inputSql)) {
            probeSql.append(FileSegments.makeSyncFileSegSql(inputSql));
        }
        if (inputModels.size() > 0) {
            if (probeSql.length() > 0)
                probeSql.append("\n union all \n");

            probeSql.append(FileSegments.makeSyncFileSegSql(inputModels));
        }
        if (probeSql.length() == 0) {
            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, null, "");
        }

        PrepareSqlRequest sqlRequest = new PrepareSqlRequest();
        sqlRequest.setProject(project);
        sqlRequest.setForcedToPushDown(true);
        sqlRequest.setSql(probeSql.toString());

        EnvelopeResponse<SQLResponse> probeResponse = query(sqlRequest, "");

        Set<String> touchedModelIds = new HashSet<>();
        for (NDataModel inputModel : inputModels) {
            touchedModelIds.add(inputModel.getId());
        }
        if (probeResponse.getData() != null && probeResponse.getData().getNativeRealizations() != null) {
            for (NativeQueryRealization real : probeResponse.getData().getNativeRealizations()) {
                touchedModelIds.add(real.getModelId());
            }
        }

        SyncFileSegmentsResponse resp = new SyncFileSegmentsResponse();
        resp.setProject(project);
        resp.setModels(touchedModelIds.stream().map(modelId -> FileSegments.getModelFileSegments(project, modelId))
                .collect(Collectors.toList()));
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, resp, "");
    }

    @ApiOperation(value = "cancelQuery", tags = { "QE" })
    @DeleteMapping(value = "/{id:.+}")
    @ResponseBody
    public EnvelopeResponse<String> stopQuery(@PathVariable("id") String id) {
        queryService.stopQuery(id);
        EventBusFactory.getInstance().postAsync(new StopQueryBroadcastEventNotifier(id));
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "clearCache", tags = { "QE" })
    @DeleteMapping(value = "/cache")
    @ResponseBody
    public EnvelopeResponse<String> clearCache(@RequestParam(value = "project", required = false) String project) {
        if (!isAdmin()) {
            throw new KylinException(PERMISSION_DENIED,
                    "Please make sure you have the admin authority to clear project cache.");
        }
        try {
            queryCacheManager.clearProjectCache(project);
        } catch (JedisException e) {
            throw new KylinException(REDIS_CLEAR_ERROR, "Please make sure your redis service is online.");
        }
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "recoverCache", tags = { "QE" })
    @PostMapping(value = "/cache/recovery")
    @ResponseBody
    public EnvelopeResponse<String> recoverCache() {
        if (!isAdmin()) {
            throw new KylinException(PERMISSION_DENIED,
                    "Please make sure you have the admin authority to recover cache.");
        }
        queryCacheManager.recoverCache();
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    // TODO should be just "prepare" a statement, get back expected ResultSetMetaData

    @ApiOperation(value = "prepareStatement", tags = { "QE" })
    @PostMapping(value = "/prestate")
    @ResponseBody
    public EnvelopeResponse<SQLResponse> prepareQuery(@Valid @RequestBody PrepareSqlRequest sqlRequest) {
        checkProjectName(sqlRequest.getProject());
        Map<String, String> newToggles = Maps.newHashMap();
        if (sqlRequest.getBackdoorToggles() != null)
            newToggles.putAll(sqlRequest.getBackdoorToggles());
        newToggles.put(BackdoorToggles.DEBUG_TOGGLE_PREPARE_ONLY, "true");
        sqlRequest.setBackdoorToggles(newToggles);

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, queryService.queryWithCache(sqlRequest), "");
    }

    @ApiOperation(value = "savedQueries", tags = { "QE" })
    @PostMapping(value = "/saved_queries")
    public EnvelopeResponse<String> saveQuery(@RequestBody SaveSqlRequest sqlRequest) {
        String queryName = sqlRequest.getName();
        checkRequiredArg("name", queryName);
        checkQueryName(queryName);
        String creator = SecurityContextHolder.getContext().getAuthentication().getName();
        Query newQuery = new Query(queryName, sqlRequest.getProject(), sqlRequest.getSql(),
                sqlRequest.getDescription());
        queryService.saveQuery(creator, sqlRequest.getProject(), newQuery);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "removeSavedQueries", tags = { "QE" })
    @DeleteMapping(value = "/saved_queries/{id:.+}")
    @ResponseBody
    public EnvelopeResponse<String> removeSavedQuery(@PathVariable("id") String id,
            @RequestParam("project") String project) {

        String creator = SecurityContextHolder.getContext().getAuthentication().getName();
        queryService.removeSavedQuery(creator, project, id);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "getSavedQueries", tags = { "QE" })
    @GetMapping(value = "/saved_queries")
    @ResponseBody
    public EnvelopeResponse<DataResult<List<Query>>> getSavedQueries(@RequestParam(value = "project") String project,
            @RequestParam(value = "offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "limit", required = false, defaultValue = "10") Integer limit) {
        checkProjectName(project);
        String creator = SecurityContextHolder.getContext().getAuthentication().getName();
        List<Query> savedQueries = queryService.getSavedQueries(creator, project).getQueries();

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, DataResult.get(savedQueries, offset, limit), "");
    }

    @ApiOperation(value = "downloadQueryHistories", tags = { "QE" })
    @GetMapping(value = "/download_query_histories")
    @ResponseBody
    public EnvelopeResponse<String> downloadQueryHistories(@RequestParam(value = "project") String project,
            @RequestParam(value = "timezone_offset_hour") Integer timeZoneOffsetHour,
            @RequestParam(value = "language", required = false) String language,
            @RequestParam(value = "start_time_from", required = false) String startTimeFrom,
            @RequestParam(value = "start_time_to", required = false) String startTimeTo,
            @RequestParam(value = "latency_from", required = false) String latencyFrom,
            @RequestParam(value = "latency_to", required = false) String latencyTo,
            @RequestParam(value = "query_status", required = false) List<String> queryStatus,
            @RequestParam(value = "sql", required = false) String sql,
            @RequestParam(value = "realization", required = false) List<String> realizations,
            @RequestParam(value = "exclude_realization", required = false) List<String> excludeRealization,
            @RequestParam(value = "server", required = false) String server,
            @RequestParam(value = "submitter", required = false) List<String> submitter, HttpServletResponse response) {
        ZoneOffset zoneOffset;
        try {
            zoneOffset = ZoneOffset.ofHours(timeZoneOffsetHour);
        } catch (Exception e) {
            logger.error("Download file error", e);
            throw new KylinException(FAILED_DOWNLOAD_FILE, e.getMessage());
        }
        if (CN.equals(language)) {
            MsgPicker.setMsg("cn");
        }
        checkProjectName(project);
        QueryHistoryRequest request = new QueryHistoryRequest(project, startTimeFrom, startTimeTo, latencyFrom,
                latencyTo, sql, server, submitter, null, null, queryStatus, realizations, excludeRealization, null,
                false, null, true);
        checkGetQueryHistoriesParam(request);
        response.setContentType("text/csv;charset=UTF-8");
        response.setCharacterEncoding("UTF-8");
        String name = "\"query-history-" + System.currentTimeMillis() + ".csv\"";
        response.setHeader("Content-Disposition", "attachment; filename=" + name);
        try {
            queryHistoryService.downloadQueryHistories(request, response, zoneOffset, timeZoneOffsetHour, false);
        } catch (TimeoutException e) {
            throw new KylinTimeoutException(MsgPicker.getMsg().getDownloadQueryHistoryTimeout());
        } catch (Exception e) {
            throw new KylinException(FAILED_DOWNLOAD_FILE, e.getMessage());
        }

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "downloadQueryHistoriesSql", tags = { "QE" })
    @GetMapping(value = "/download_query_histories_sql")
    @ResponseBody
    public EnvelopeResponse<String> downloadQueryHistoriesSql(@RequestParam(value = "project") String project,
            @RequestParam(value = "start_time_from", required = false) String startTimeFrom,
            @RequestParam(value = "start_time_to", required = false) String startTimeTo,
            @RequestParam(value = "latency_from", required = false) String latencyFrom,
            @RequestParam(value = "latency_to", required = false) String latencyTo,
            @RequestParam(value = "query_status", required = false) List<String> queryStatus,
            @RequestParam(value = "sql", required = false) String sql,
            @RequestParam(value = "realization", required = false) List<String> realizations,
            @RequestParam(value = "exclude_realization", required = false) List<String> excludeRealization,
            @RequestParam(value = "server", required = false) String server,
            @RequestParam(value = "submitter", required = false) List<String> submitter, HttpServletResponse response) {
        checkProjectName(project);
        QueryHistoryRequest request = new QueryHistoryRequest(project, startTimeFrom, startTimeTo, latencyFrom,
                latencyTo, sql, server, submitter, null, null, queryStatus, realizations, excludeRealization, null,
                false, null, true);
        checkGetQueryHistoriesParam(request);
        response.setContentType("text/csv;charset=UTF-8");
        response.setCharacterEncoding("UTF-8");
        String name = "\"sql-" + System.currentTimeMillis() + ".txt\"";
        response.setHeader("Content-Disposition", "attachment; filename=" + name);
        try {
            queryHistoryService.downloadQueryHistories(request, response, null, null, true);
        } catch (TimeoutException e) {
            throw new KylinTimeoutException(MsgPicker.getMsg().getDownloadQueryHistoryTimeout());
        } catch (Exception e) {
            throw new KylinException(FAILED_DOWNLOAD_FILE, e.getMessage());
        }

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "getQueryHistories", tags = { "QE" })
    @GetMapping(value = "/history_queries")
    @ResponseBody
    public EnvelopeResponse<Map<String, Object>> getQueryHistories(@RequestParam(value = "project") String project,
            @RequestParam(value = "start_time_from", required = false) String startTimeFrom,
            @RequestParam(value = "start_time_to", required = false) String startTimeTo,
            @RequestParam(value = "latency_from", required = false) String latencyFrom,
            @RequestParam(value = "latency_to", required = false) String latencyTo,
            @RequestParam(value = "query_status", required = false) List<String> queryStatus,
            @RequestParam(value = "sql", required = false) String sql,
            @RequestParam(value = "realization", required = false) List<String> realizations,
            @RequestParam(value = "exclude_realization", required = false) List<String> excludeRealization,
            @RequestParam(value = "server", required = false) String server,
            @RequestParam(value = "offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "limit", required = false, defaultValue = "10") Integer limit,
            @RequestParam(value = "submitter", required = false) List<String> submitter) {
        checkProjectName(project);
        QueryHistoryRequest request = new QueryHistoryRequest(project, startTimeFrom, startTimeTo, latencyFrom,
                latencyTo, sql, server, submitter, null, null, queryStatus, realizations, excludeRealization, null,
                false, null, true);
        checkGetQueryHistoriesParam(request);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, QueryHisTransformStandardUtil
                .transformQueryHistorySqlForDisplay(queryHistoryService.getQueryHistories(request, limit, offset)), "");
    }

    @ApiOperation(value = "getQueryHistories", tags = { "QE" }, notes = "Update Param: start_time_from, start_time_to")
    @GetMapping(value = "/query_histories")
    @ResponseBody
    public EnvelopeResponse<Map<String, Object>> getQueryHistories(@RequestParam(value = "project") String project,
            @RequestParam(value = "start_time_from", required = false) String startTimeFrom,
            @RequestParam(value = "start_time_to", required = false) String startTimeTo,
            @RequestParam(value = "sql", required = false) String sql,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer size) {
        checkProjectName(project);
        QueryHistoryRequest request = new QueryHistoryRequest(project, startTimeFrom, startTimeTo);
        Optional.ofNullable(sql).ifPresent(request::setSql);
        DataRangeUtils.validateDataRange(startTimeFrom, startTimeTo, null);
        Map<String, Object> queryHistories = QueryHisTransformStandardUtil
                .transformQueryHistory(queryHistoryService.getQueryHistories(request, size, offset));
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, queryHistories, "");
    }

    @ApiOperation(value = "getQueryHistoryUsernames", tags = { "QE" }, notes = "Update Param: project, user_name")
    @GetMapping(value = "/query_history_submitters")
    @ResponseBody
    public EnvelopeResponse<List<String>> getQueryHistorySubmitters(@RequestParam(value = "project") String project,
            @RequestParam(value = "submitter", required = false) List<String> submitter,
            @RequestParam(value = "page_size", required = false, defaultValue = "100") Integer size) {
        checkProjectName(project);
        QueryHistoryRequest request = new QueryHistoryRequest();
        request.setProject(project);
        request.setFilterSubmitter(submitter);
        request.setSubmitterExactlyMatch(false);
        checkGetQueryHistoriesParam(request);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                queryHistoryService.getQueryHistoryUsernames(request, size), "");
    }

    @ApiOperation(value = "getQueryHistoryModels", tags = { "QE" }, notes = "Update Param: project, model_name")
    @GetMapping(value = "/query_history_models")
    @ResponseBody
    public EnvelopeResponse<QueryHistoryFiltersResponse> getQueryHistoryModels(
            @RequestParam(value = "project") String project,
            @RequestParam(value = "model_name", required = false) String modelAlias,
            @RequestParam(value = "page_size", required = false, defaultValue = "100") Integer size) {
        checkProjectName(project);
        QueryHistoryRequest request = new QueryHistoryRequest();
        request.setProject(project);
        request.setFilterModelName(modelAlias);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                queryHistoryService.getQueryHistoryModels(request, size), "");
    }

    @ApiOperation(value = "queryHistoryTiredStorageMetrics", tags = { "QE" }, notes = "Update Param: project, query_id")
    @GetMapping(value = "/query_history/tired_storage_metrics")
    @ResponseBody
    public EnvelopeResponse<Map<String, Long>> queryHistoryTiredStorageMetrics(
            @RequestParam(value = "project") String project, @RequestParam(value = "query_id") String queryId) {
        checkProjectName(project);
        checkRequiredArg("query_id", queryId);
        QueryHistoryRequest request = new QueryHistoryRequest();
        request.setProject(project);
        request.setSql(queryId);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, queryHistoryService.queryTiredStorageMetric(request),
                "");
    }

    @ApiOperation(value = "getServers", tags = { "QE" })
    @GetMapping(value = "/servers")
    @ResponseBody
    public EnvelopeResponse<List<?>> getServers(
            @RequestParam(value = "ext", required = false, defaultValue = "false") boolean ext) {
        if (ext) {
            List<ServerExtInfoResponse> serverInfo = clusterManager.getServers().stream().map(
                    server -> new ServerExtInfoResponse().setServer(server).setSecretName(encodeHost(server.getHost())))
                    .collect(Collectors.toList());
            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, serverInfo, "");
        } else {
            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                    clusterManager.getServers().stream().map(ServerInfoResponse::getHost).collect(Collectors.toList()),
                    "");
        }
    }

    private void checkGetQueryHistoriesParam(QueryHistoryRequest request) {
        // check start time and end time
        Preconditions.checkArgument(allEmptyOrNotAllEmpty(request.getStartTimeFrom(), request.getStartTimeTo()),
                "'start time from' and 'start time to' must be used together.");
        Preconditions.checkArgument(allEmptyOrNotAllEmpty(request.getLatencyFrom(), request.getLatencyTo()),
                "'latency from ' and 'latency to' must be used together.");
    }

    private boolean allEmptyOrNotAllEmpty(String param1, String param2) {
        if (StringUtils.isEmpty(param1) && StringUtils.isEmpty(param2))
            return true;

        return StringUtils.isNotEmpty(param1) && StringUtils.isNotEmpty(param2);
    }

    @PostMapping(value = "/format/{format:.+}", consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE)
    @ResponseBody
    public void downloadQueryResult(@PathVariable("format") String format, SQLRequest sqlRequest,
            HttpServletResponse response) {
        checkProjectName(sqlRequest.getProject());
        KylinConfig config = queryService.getConfig();
        val msg = MsgPicker.getMsg();

        if ((isAdmin() && !config.isAdminUserExportAllowed())
                || (!isAdmin() && !config.isNoneAdminUserExportAllowed())) {
            throw new ForbiddenException(msg.getExportResultNotAllowed());
        }

        SQLResponse result = queryService.queryWithCache(sqlRequest);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS", Locale.getDefault(Locale.Category.FORMAT));
        String nowStr = sdf.format(new Date());
        response.setContentType("text/" + format + ";charset=utf-8");
        response.setHeader("Content-Disposition", "attachment; filename=\"" + nowStr + ".result." + format + "\"");
        ICsvListWriter csvWriter = null;

        try {
            //Add a BOM for Excel
            Writer writer = new OutputStreamWriter(response.getOutputStream(), StandardCharsets.UTF_8);
            writer.write('\uFEFF');

            csvWriter = new CsvListWriter(writer, CsvPreference.STANDARD_PREFERENCE);
            List<String> headerList = new ArrayList<>();

            // avoid handle npe in org.apache.kylin.rest.controller.NBasicController.handleError
            // when result.getColumnMetas is null
            if (result.isException()) {
                logger.warn("Download query result failed, exception is {}", result.getExceptionMessage());
                return;
            }

            for (SelectedColumnMeta column : result.getColumnMetas()) {
                headerList.add(column.getLabel());
            }

            String[] headers = new String[headerList.size()];
            csvWriter.writeHeader(headerList.toArray(headers));

            for (List<String> strings : result.getResults()) {
                csvWriter.write(strings);
            }
        } catch (IOException e) {
            logger.error("Download query result failed...", e);
            throw new InternalErrorException(e);
        } finally {
            IOUtils.closeQuietly(csvWriter, null);
        }
    }

    @ApiOperation(value = "getMetadata", notes = "Update Param: project")
    @GetMapping(value = "/tables_and_columns")
    @ResponseBody
    public EnvelopeResponse<List<TableMetaWithType>> getMetadata(@RequestParam("project") String project,
            @RequestParam(value = "cube", required = false) String modelAlias) {
        checkProjectName(project);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, queryService.getMetadataV2(project, modelAlias), "");
    }

    @GetMapping(value = "/statistics")
    public EnvelopeResponse<QueryStatisticsResponse> getQueryStatistics(@RequestParam("project") String project,
            @RequestParam("start_time") long startTime, @RequestParam("end_time") long endTime) {
        checkProjectName(project);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                queryHistoryService.getQueryStatistics(project, startTime, endTime), "");
    }

    @GetMapping(value = "/statistics/count")
    public EnvelopeResponse<Map<String, Object>> getQueryCount(@RequestParam("project") String project,
            @RequestParam("start_time") long startTime, @RequestParam("end_time") long endTime,
            @RequestParam("dimension") String dimension) {
        checkProjectName(project);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                queryHistoryService.getQueryCount(project, startTime, endTime, dimension), "");
    }

    @GetMapping(value = "/statistics/duration")
    public EnvelopeResponse<Map<String, Object>> getAvgDuration(@RequestParam("project") String project,
            @RequestParam("start_time") long startTime, @RequestParam("end_time") long endTime,
            @RequestParam("dimension") String dimension) {
        checkProjectName(project);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                queryHistoryService.getAvgDuration(project, startTime, endTime, dimension), "");
    }

    @Deprecated
    @GetMapping(value = "/history_queries/table_names")
    public EnvelopeResponse<Map<String, String>> getQueryHistoryTableNames(
            @RequestParam(value = "projects", required = false) List<String> projects) {
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                queryHistoryService.getQueryHistoryTableMap(projects), "");
    }

    @PutMapping(value = "/format")
    public EnvelopeResponse<List<String>> formatQuery(@RequestBody SQLFormatRequest request) {
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, queryService.format(request.getSqls()), "");
    }

    @ApiOperation(value = "queryDetect", tags = { "QE" })
    @PostMapping("/detection")
    public EnvelopeResponse<QueryDetectResponse> queryDetect(@RequestBody QueryDetectRequest request) {
        checkProjectName(request.getProject());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, queryService.queryDetect(request), "");
    }

    private void checkQueryName(String queryName) {
        if (!queryNamePattern.matcher(queryName).matches()) {
            throw new KylinException(INVALID_NAME, MsgPicker.getMsg().getInvalidQueryName());
        }
    }

    private void checkForcedToParams(PrepareSqlRequest sqlRequest) {
        if (sqlRequest.isForcedToIndex() && sqlRequest.isForcedToPushDown()) {
            throw new KylinException(QueryErrorCode.INVALID_QUERY_PARAMS,
                    MsgPicker.getMsg().getCannotForceToBothPushdodwnAndIndex());
        }
    }

    @ApiOperation(value = "catalogCache", tags = { "DW" })
    @PutMapping(value = "single_catalog_cache", produces = { HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<TableRefresh> refreshSingleCatalogCache(@RequestBody HashMap refreshRequest) {
        checkRefreshParam(refreshRequest);
        TableRefresh response = tableService.refreshSingleCatalogCache(refreshRequest);
        return new EnvelopeResponse<>(response.getCode(), response, response.getMsg());
    }

    @ApiOperation(value = "catalogCache", tags = { "DW" })
    @PutMapping(value = "catalog_cache", produces = { HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse refreshCatalogCache(final HttpServletRequest refreshRequest) {
        TableRefreshAll response = tableService.refreshAllCatalogCache(refreshRequest);
        return new EnvelopeResponse<>(response.getCode(), response, response.getMsg());
    }

    private void checkRefreshParam(Map refreshRequest) {
        val message = MsgPicker.getMsg();
        Object tables = refreshRequest.get("tables");
        if (tables == null) {
            throw new KylinException(INVALID_TABLE_REFRESH_PARAMETER, message.getTableRefreshParamInvalid(), false);
        } else if (refreshRequest.keySet().size() > 1) {
            throw new KylinException(INVALID_TABLE_REFRESH_PARAMETER, message.getTableRefreshParamMore(), false);
        } else if (!(tables instanceof List)) {
            throw new KylinException(INVALID_TABLE_REFRESH_PARAMETER, message.getTableRefreshParamInvalid(), false);
        }
    }

    @ApiOperation(value = "ifBigQuery", tags = {
            "QE" }, notes = "Update Param: query_id, accept_partial, backdoor_toggles, cache_key")
    @PostMapping(value = "/if_big_query")
    @ResponseBody
    public EnvelopeResponse<BigQueryResponse> ifBigQuery(@Valid @RequestBody PrepareSqlRequest sqlRequest,
            @RequestHeader(value = "User-Agent") String userAgent) {
        sqlRequest.setIfBigQuery(true);
        checkForcedToParams(sqlRequest);
        checkProjectName(sqlRequest.getProject());
        sqlRequest.setUserAgent(userAgent != null ? userAgent : "");
        QueryContext.current().record("end_http_proc");
        BigQueryResponse bigQueryResponse = queryService.ifBigQuery(sqlRequest);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, bigQueryResponse, "");
    }
}

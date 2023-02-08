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
import static org.apache.kylin.common.exception.ServerErrorCode.ACCESS_DENIED;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.ASYNC_QUERY_INCLUDE_HEADER_NOT_EMPTY;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.ASYNC_QUERY_PROJECT_NAME_EMPTY;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.ASYNC_QUERY_TIME_FORMAT_ERROR;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.QueryErrorCode;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.query.util.AsyncQueryUtil;
import org.apache.kylin.rest.exception.ForbiddenException;
import org.apache.kylin.rest.request.AsyncQuerySQLRequest;
import org.apache.kylin.rest.response.AsyncQueryResponse;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.service.AsyncQueryService;
import org.apache.kylin.rest.service.QueryService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AsyncQueryRequestLimits;
import org.apache.spark.sql.SparderEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.alibaba.ttl.TtlRunnable;
import com.google.common.collect.Lists;

import io.swagger.annotations.ApiOperation;

@RestController
@RequestMapping(value = "/api", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class NAsyncQueryController extends NBasicController {

    private static final Logger logger = LoggerFactory.getLogger(NAsyncQueryController.class);

    private static final List<String> FILE_ENCODING = Lists.newArrayList("utf-8", "gbk");
    private static final List<String> FILE_FORMAT = Lists.newArrayList("csv", "json", "xlsx", "parquet");

    @Autowired
    @Qualifier("queryService")
    private QueryService queryService;

    @Autowired
    @Qualifier("asyncQueryService")
    private AsyncQueryService asyncQueryService;

    @Autowired
    protected AclEvaluate aclEvaluate;

    ExecutorService executorService = Executors.newCachedThreadPool();

    @ApiOperation(value = "query", tags = {
            "QE" }, notes = "Update Param: query_id, accept_partial, backdoor_toggles, cache_key; Update Response: query_id")
    @PostMapping(value = "/async_query")
    @ResponseBody
    public EnvelopeResponse<AsyncQueryResponse> query(@Valid @RequestBody final AsyncQuerySQLRequest sqlRequest)
            throws InterruptedException, IOException {
        aclEvaluate.checkProjectQueryPermission(sqlRequest.getProject());
        checkProjectName(sqlRequest.getProject());
        if (!FILE_ENCODING.contains(sqlRequest.getEncode().toLowerCase(Locale.ROOT))) {
            return new EnvelopeResponse<>(QueryErrorCode.ASYNC_QUERY_ILLEGAL_PARAM.toErrorCode().getString(),
                    new AsyncQueryResponse(sqlRequest.getQueryId(), AsyncQueryResponse.Status.FAILED, "Format "
                            + sqlRequest.getFormat() + " unsupported. Only " + FILE_FORMAT + " are supported"),
                    "");
        }
        if (!FILE_FORMAT.contains(sqlRequest.getFormat().toLowerCase(Locale.ROOT))) {
            return new EnvelopeResponse<>(QueryErrorCode.ASYNC_QUERY_ILLEGAL_PARAM.toErrorCode().getString(),
                    new AsyncQueryResponse(sqlRequest.getQueryId(), AsyncQueryResponse.Status.FAILED, "Format "
                            + sqlRequest.getFormat() + " unsupported. Only " + FILE_FORMAT + " are supported"),
                    "");
        }
        final AtomicReference<String> queryIdRef = new AtomicReference<>();
        final AtomicReference<String> exceptionHandle = new AtomicReference<>();
        final SecurityContext context = SecurityContextHolder.getContext();

        if (StringUtils.isEmpty(sqlRequest.getSeparator())) {
            sqlRequest.setSeparator(",");
        }
        if (NProjectManager.getProjectConfig(sqlRequest.getProject()).isUniqueAsyncQueryYarnQueue()) {
            AsyncQueryRequestLimits.checkCount();
        }

        executorService.submit(Objects.requireNonNull(TtlRunnable.get(() -> {
            String format = sqlRequest.getFormat().toLowerCase(Locale.ROOT);
            String encode = sqlRequest.getEncode().toLowerCase(Locale.ROOT);
            SecurityContextHolder.setContext(context);

            SparderEnv.setSeparator(sqlRequest.getSeparator());

            QueryContext queryContext = QueryContext.current();
            sqlRequest.setQueryId(queryContext.getQueryId());
            queryContext.getQueryTagInfo().setAsyncQuery(true);
            queryContext.getQueryTagInfo().setFileFormat(format);
            queryContext.getQueryTagInfo().setFileEncode(encode);
            queryContext.getQueryTagInfo().setFileName(sqlRequest.getFileName());
            queryContext.getQueryTagInfo().setSeparator(sqlRequest.getSeparator());
            queryContext.getQueryTagInfo().setIncludeHeader(sqlRequest.isIncludeHeader());
            queryContext.setProject(sqlRequest.getProject());
            logger.info("Start a new async query with queryId: {}", queryContext.getQueryId());
            String queryId = queryContext.getQueryId();
            queryIdRef.set(queryId);
            try {
                asyncQueryService.saveQueryUsername(sqlRequest.getProject(), queryId);
                SQLResponse response = queryService.queryWithCache(sqlRequest);
                if (response.isException()) {
                    AsyncQueryUtil.createErrorFlag(sqlRequest.getProject(), queryContext.getQueryId(),
                            response.getExceptionMessage());
                    exceptionHandle.set(response.getExceptionMessage());
                }
            } catch (Exception e) {
                try {
                    logger.error("failed to run query {}", queryContext.getQueryId(), e);
                    AsyncQueryUtil.createErrorFlag(sqlRequest.getProject(), queryContext.getQueryId(), e.getMessage());
                    exceptionHandle.set(e.getMessage());
                } catch (Exception e1) {
                    exceptionHandle.set(exceptionHandle.get() + "\n" + e.getMessage());
                    throw new RuntimeException(e1);
                }
            } finally {
                logger.info("Async query with queryId: {} end", queryContext.getQueryId());
                QueryContext.current().close();
            }
        })));

        while (queryIdRef.get() == null) {
            Thread.sleep(200);
        }

        switch (asyncQueryService.queryStatus(sqlRequest.getProject(), sqlRequest.getQueryId())) {
        case SUCCESS:
            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                    new AsyncQueryResponse(queryIdRef.get(), AsyncQueryResponse.Status.SUCCESSFUL, "query success"),
                    "");
        case FAILED:
            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                    new AsyncQueryResponse(queryIdRef.get(), AsyncQueryResponse.Status.FAILED, exceptionHandle.get()),
                    "");
        default:
            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                    new AsyncQueryResponse(queryIdRef.get(), AsyncQueryResponse.Status.RUNNING, "query still running"),
                    "");
        }
    }

    @ApiOperation(value = "cancel async query", tags = { "QE" })
    @DeleteMapping(value = "/async_query")
    @ResponseBody
    public EnvelopeResponse<Boolean> batchDelete(@RequestParam(value = "project", required = false) String project,
            @RequestParam(value = "older_than", required = false) String time) throws Exception {
        if (!isAdmin()) {
            throw new KylinException(ACCESS_DENIED, "Access denied. Only admin users can delete the query results");
        }
        if (project != null) {
            aclEvaluate.checkProjectQueryPermission(project);
            checkProjectName(project);
        }
        try {
            if (asyncQueryService.batchDelete(project, time)) {
                return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, true, "");
            } else {
                return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, false,
                        MsgPicker.getMsg().getCleanFolderFail());
            }
        } catch (ParseException e) {
            logger.error(ASYNC_QUERY_TIME_FORMAT_ERROR.getMsg(), e);
            throw new KylinException(ASYNC_QUERY_TIME_FORMAT_ERROR);
        }
    }

    @ApiOperation(value = "cancel async query", tags = { "QE" })
    @DeleteMapping(value = "/async_query/{query_id:.+}")
    @ResponseBody
    public EnvelopeResponse<Boolean> deleteByQueryId(@PathVariable("query_id") String queryId,
            @Valid @RequestBody(required = false) final AsyncQuerySQLRequest sqlRequest,
            @RequestParam(value = "project", required = false) String project) throws IOException {
        if (project == null) {
            if (sqlRequest == null) {
                throw new KylinException(ASYNC_QUERY_PROJECT_NAME_EMPTY);
            }
            project = sqlRequest.getProject();
        }
        aclEvaluate.checkProjectAdminPermission(project);
        checkProjectName(project);
        if (!asyncQueryService.hasPermission(queryId, project)) {
            return new EnvelopeResponse<>(KylinException.CODE_UNAUTHORIZED, false,
                    "Access denied. Only admin users can delete the query results");
        }
        boolean result = asyncQueryService.deleteByQueryId(project, queryId);
        if (result)
            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, true, "");
        else
            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, false, MsgPicker.getMsg().getCleanFolderFail());
    }

    @ApiOperation(value = "query", tags = { "QE" }, notes = "Update Response: query_id")
    @GetMapping(value = "/async_query/{query_id:.+}/status")
    @ResponseBody
    public EnvelopeResponse<AsyncQueryResponse> inqueryStatus(
            @Valid @RequestBody(required = false) final AsyncQuerySQLRequest sqlRequest,
            @PathVariable("query_id") String queryId, @RequestParam(value = "project", required = false) String project)
            throws IOException {
        if (project == null) {
            if (sqlRequest == null) {
                throw new KylinException(ASYNC_QUERY_PROJECT_NAME_EMPTY);
            }
            project = sqlRequest.getProject();
        }
        aclEvaluate.checkProjectQueryPermission(project);
        checkProjectName(project);
        if (!asyncQueryService.hasPermission(queryId, project)) {
            return new EnvelopeResponse<>(KylinException.CODE_UNAUTHORIZED, null,
                    "Access denied. Only task submitters or admin users can get the query status");
        }
        AsyncQueryService.QueryStatus queryStatus = asyncQueryService.queryStatus(project, queryId);
        AsyncQueryResponse asyncQueryResponse;
        switch (queryStatus) {
        case SUCCESS:
            asyncQueryResponse = new AsyncQueryResponse(queryId, AsyncQueryResponse.Status.SUCCESSFUL,
                    "await fetching results");
            break;
        case RUNNING:
            asyncQueryResponse = new AsyncQueryResponse(queryId, AsyncQueryResponse.Status.RUNNING, "still running");
            break;
        case FAILED:
            asyncQueryResponse = new AsyncQueryResponse(queryId, AsyncQueryResponse.Status.FAILED,
                    asyncQueryService.retrieveSavedQueryException(project, queryId));
            break;
        default:
            asyncQueryResponse = new AsyncQueryResponse(queryId, AsyncQueryResponse.Status.MISSING,
                    "query status is lost"); //
            break;
        }

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, asyncQueryResponse, "");
    }

    @ApiOperation(value = "fileStatus", tags = { "QE" }, notes = "Update URL: file_status")
    @GetMapping(value = "/async_query/{query_id:.+}/file_status")
    @ResponseBody
    public EnvelopeResponse<Long> fileStatus(@PathVariable("query_id") String queryId,
            @Valid @RequestBody(required = false) final AsyncQuerySQLRequest sqlRequest,
            @RequestParam(value = "project", required = false) String project) throws IOException {
        if (project == null) {
            if (sqlRequest == null) {
                throw new KylinException(ASYNC_QUERY_PROJECT_NAME_EMPTY);
            }
            project = sqlRequest.getProject();
        }
        aclEvaluate.checkProjectQueryPermission(project);
        checkProjectName(project);
        if (!asyncQueryService.hasPermission(queryId, project)) {
            return new EnvelopeResponse<>(KylinException.CODE_UNAUTHORIZED, 0L,
                    "Access denied. Only task submitters or admin users can get the file status");
        }
        long length = asyncQueryService.fileStatus(project, queryId);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, length, "");
    }

    @ApiOperation(value = "async query status", tags = { "QE" })
    @GetMapping(value = "/async_query/{query_id:.+}/metadata")
    @ResponseBody
    public EnvelopeResponse<List<List<String>>> metadata(
            @Valid @RequestBody(required = false) final AsyncQuerySQLRequest sqlRequest,
            @PathVariable("query_id") String queryId, @RequestParam(value = "project", required = false) String project)
            throws IOException {
        if (project == null) {
            if (sqlRequest == null) {
                throw new KylinException(ASYNC_QUERY_PROJECT_NAME_EMPTY);
            }
            project = sqlRequest.getProject();
        }
        aclEvaluate.checkProjectQueryPermission(project);
        checkProjectName(project);
        if (!asyncQueryService.hasPermission(queryId, project)) {
            return new EnvelopeResponse<>(KylinException.CODE_UNAUTHORIZED, null,
                    "Access denied. Only task submitters or admin users can get the metadata");
        }
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, asyncQueryService.getMetaData(project, queryId), "");
    }

    @ApiOperation(value = "downloadQueryResult", tags = { "QE" }, notes = "Update URL: result")
    @GetMapping(value = "/async_query/{query_id:.+}/result_download")
    @ResponseBody
    public void downloadQueryResult(@PathVariable("query_id") String queryId,
            @RequestParam(value = "oldIncludeHeader", required = false) Boolean oldIncludeHeader,
            @RequestParam(value = "includeHeader", required = false) Boolean includeHeader,
            @Valid @RequestBody(required = false) final AsyncQuerySQLRequest sqlRequest, HttpServletResponse response,
            @RequestParam(value = "project", required = false) String project) throws IOException {
        if (project == null) {
            if (sqlRequest == null) {
                throw new KylinException(ASYNC_QUERY_PROJECT_NAME_EMPTY);
            }
            project = sqlRequest.getProject();
        }
        if (oldIncludeHeader != null || includeHeader != null) {
            throw new KylinException(ASYNC_QUERY_INCLUDE_HEADER_NOT_EMPTY);
        }
        aclEvaluate.checkProjectQueryPermission(project);
        checkProjectName(project);
        KylinConfig config = queryService.getConfig();
        Message msg = MsgPicker.getMsg();
        if (!asyncQueryService.hasPermission(queryId, project)) {
            throw new KylinException(ACCESS_DENIED, msg.getForbiddenExportAsyncQueryResult());
        }
        asyncQueryService.checkStatus(queryId, AsyncQueryService.QueryStatus.SUCCESS, project,
                MsgPicker.getMsg().getQueryResultNotFound());
        if (((isAdmin() && !config.isAdminUserExportAllowed())
                || (!isAdmin() && !config.isNoneAdminUserExportAllowed()))) {
            throw new ForbiddenException(msg.getExportResultNotAllowed());
        }
        AsyncQueryService.FileInfo fileInfo = asyncQueryService.getFileInfo(project, queryId);
        String format = fileInfo.getFormat();
        String encode = fileInfo.getEncode();
        String fileName = fileInfo.getFileName();
        if (format.equals("xlsx")) {
            response.setContentType("application/octet-stream;charset=" + encode);
        } else {
            response.setContentType("application/" + format + ";charset=" + encode);
        }
        response.setHeader("Content-Disposition", "attachment; filename=\"" + fileName + "." + format + "\"");
        asyncQueryService.retrieveSavedQueryResult(project, queryId, response, format, encode);
    }

    @ApiOperation(value = "async query result path", tags = { "QE" })
    @GetMapping(value = "/async_query/{query_id:.+}/result_path")
    @ResponseBody
    public EnvelopeResponse<String> queryPath(@PathVariable("query_id") String queryId,
            @Valid @RequestBody(required = false) final AsyncQuerySQLRequest sqlRequest, HttpServletResponse response,
            @RequestParam(value = "project", required = false) String project) throws IOException {
        if (project == null) {
            if (sqlRequest == null) {
                throw new KylinException(ASYNC_QUERY_PROJECT_NAME_EMPTY);
            }
            project = sqlRequest.getProject();
        }
        aclEvaluate.checkProjectQueryPermission(project);
        checkProjectName(project);
        if (!asyncQueryService.hasPermission(queryId, project)) {
            return new EnvelopeResponse<>(KylinException.CODE_UNAUTHORIZED, "",
                    "Access denied. Only task submitters or admin users can get the query path");
        }
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                asyncQueryService.asyncQueryResultPath(project, queryId), "");
    }
}

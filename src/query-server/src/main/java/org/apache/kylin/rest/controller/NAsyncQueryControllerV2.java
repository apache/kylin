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

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V2_JSON;

import org.apache.kylin.rest.request.AsyncQuerySQLRequest;
import org.apache.kylin.rest.request.AsyncQuerySQLRequestV2;
import org.apache.kylin.rest.response.AsyncQueryResponse;
import org.apache.kylin.rest.response.AsyncQueryResponseV2;
import org.apache.kylin.rest.service.AsyncQueryService;
import io.swagger.annotations.ApiOperation;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.query.exception.NAsyncQueryIllegalParamException;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;
import java.io.IOException;
import java.util.List;


@RestController
@RequestMapping(value = "/api", produces = { HTTP_VND_APACHE_KYLIN_V2_JSON })
public class NAsyncQueryControllerV2 extends NBasicController {

    @Autowired
    @Qualifier("asyncQueryService")
    private AsyncQueryService asyncQueryService;

    @Autowired
    protected NAsyncQueryController asyncQueryController;

    @ApiOperation(value = "query", tags = {
            "QE" }, notes = "Update Param: query_id, accept_partial, backdoor_toggles, cache_key; Update Response: query_id")
    @PostMapping(value = "/async_query")
    @ResponseBody
    public EnvelopeResponse<AsyncQueryResponseV2> query(@Valid @RequestBody final AsyncQuerySQLRequestV2 asyncQuerySQLRequest)
            throws InterruptedException, IOException {
        AsyncQuerySQLRequest sqlRequest = new AsyncQuerySQLRequest();
        sqlRequest.setProject(asyncQuerySQLRequest.getProject());
        sqlRequest.setSql(asyncQuerySQLRequest.getSql());
        sqlRequest.setSeparator(asyncQuerySQLRequest.getSeparator());
        sqlRequest.setFormat("csv");
        sqlRequest.setEncode("utf-8");
        sqlRequest.setFileName("result");
        sqlRequest.setLimit(asyncQuerySQLRequest.getLimit());
        sqlRequest.setOffset(asyncQuerySQLRequest.getOffset());

        AsyncQueryResponse resp = asyncQueryController.query(sqlRequest).getData();
        if (resp.getStatus() == AsyncQueryResponse.Status.RUNNING) {
            resp.setInfo("still running");
        }

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, AsyncQueryResponseV2.from(resp), "");
    }


    @ApiOperation(value = "async query status", tags = { "QE" })
    @GetMapping(value = "/async_query/{query_id:.+}/metadata")
    @ResponseBody
    public EnvelopeResponse<List<List<String>>> metadata(@PathVariable("query_id") String queryId) throws IOException {
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, asyncQueryService.getMetaData(searchProject(queryId), queryId), "");
    }

    @ApiOperation(value = "fileStatus", tags = { "QE" }, notes = "Update URL: file_status")
    @GetMapping(value = "/async_query/{query_id:.+}/filestatus")
    @ResponseBody
    public EnvelopeResponse<Long> fileStatus(@PathVariable("query_id") String queryId) throws IOException {
        return asyncQueryController.fileStatus(queryId, null, searchProject(queryId));
    }



    @ApiOperation(value = "query", tags = { "QE" }, notes = "Update Response: query_id")
    @GetMapping(value = "/async_query/{query_id:.+}/status")
    @ResponseBody
    public EnvelopeResponse<AsyncQueryResponseV2> inqueryStatus(@PathVariable("query_id") String queryId)
            throws IOException {
        AsyncQueryResponse resp = asyncQueryController.inqueryStatus(null, queryId, searchProject(queryId)).getData();
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, AsyncQueryResponseV2.from(resp), "");
    }

    @ApiOperation(value = "downloadQueryResult", tags = { "QE" }, notes = "Update URL: result")
    @GetMapping(value = "/async_query/{query_id:.+}/result_download")
    @ResponseBody
    public void downloadQueryResult(@PathVariable("query_id") String queryId,
                                    @RequestParam(value = "includeHeader", required = false, defaultValue = "false") boolean includeHeader,
                                    HttpServletResponse response) throws IOException {
        asyncQueryController.downloadQueryResult(queryId, includeHeader, includeHeader, null, response, searchProject(queryId));
    }

    private String searchProject(String queryId) throws IOException {
        String project = asyncQueryService.searchQueryResultProject(queryId);
        if (project == null) {
            throw new NAsyncQueryIllegalParamException(MsgPicker.getMsg().getQueryResultNotFound());
        }
        return project;
    }

}

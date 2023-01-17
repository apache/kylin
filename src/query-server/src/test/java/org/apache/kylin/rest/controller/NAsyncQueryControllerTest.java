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
import static org.apache.kylin.common.exception.QueryErrorCode.TOO_MANY_ASYNC_QUERY;
import static org.apache.kylin.common.exception.ServerErrorCode.ACCESS_DENIED;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.ASYNC_QUERY_PROJECT_NAME_EMPTY;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.ASYNC_QUERY_RESULT_NOT_FOUND;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.ASYNC_QUERY_TIME_FORMAT_ERROR;
import static org.apache.kylin.rest.service.AsyncQueryService.QueryStatus.FAILED;
import static org.apache.kylin.rest.service.AsyncQueryService.QueryStatus.MISS;
import static org.apache.kylin.rest.service.AsyncQueryService.QueryStatus.RUNNING;
import static org.apache.kylin.rest.service.AsyncQueryService.QueryStatus.SUCCESS;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.text.ParseException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.AsyncQuerySQLRequest;
import org.apache.kylin.rest.response.AsyncQueryResponse;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.service.AsyncQueryService;
import org.apache.kylin.rest.service.QueryService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AsyncQueryRequestLimits;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

public class NAsyncQueryControllerTest extends NLocalFileMetadataTestCase {

    private static final String PROJECT = "default";

    private MockMvc mockMvc;

    @Mock
    private QueryService kapQueryService;

    @Mock
    private AsyncQueryService asyncQueryService;

    @Mock
    private AclEvaluate aclEvaluate;

    @InjectMocks
    private NAsyncQueryController nAsyncQueryController = Mockito.spy(new NAsyncQueryController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(nAsyncQueryController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
        createTestMetadata();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
        QueryContext.current().close();
    }

    private AsyncQuerySQLRequest mockAsyncQuerySQLRequest() {
        final AsyncQuerySQLRequest asyncQuerySQLRequest = new AsyncQuerySQLRequest();
        asyncQuerySQLRequest.setQueryId("123");
        asyncQuerySQLRequest.setFormat("csv");
        asyncQuerySQLRequest.setEncode("gbk");
        asyncQuerySQLRequest.setLimit(500);
        asyncQuerySQLRequest.setOffset(0);
        asyncQuerySQLRequest.setProject(PROJECT);
        asyncQuerySQLRequest.setSql("select PART_DT from KYLIN_SALES limit 500");
        asyncQuerySQLRequest.setSeparator(",");
        asyncQuerySQLRequest.setIncludeHeader(false);
        return asyncQuerySQLRequest;
    }

    @Test
    public void testQueryHasNoProjectPermission() throws Exception {
        Mockito.doThrow(new KylinException(ACCESS_DENIED, "Access is denied")).when(aclEvaluate)
                .checkProjectReadPermission(PROJECT);

        mockMvc.perform(MockMvcRequestBuilders.post("/api/async_query").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());

        Mockito.verify(nAsyncQueryController).query(Mockito.any());
    }

    @Test
    public void testBatchDeleteAllNoProjectPermission() throws Exception {
        Authentication otherUser = new TestingAuthenticationToken("OTHER", "OTHER", Constant.IDENTITY_USER);
        SecurityContextHolder.getContext().setAuthentication(otherUser);

        mockMvc.perform(MockMvcRequestBuilders.delete("/api/async_query")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());

        Mockito.verify(nAsyncQueryController).batchDelete(Mockito.any(), Mockito.any());
        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    @Test
    public void testBatchDeleteOldResultNoProjectPermission() throws Exception {
        Mockito.doThrow(new KylinException(ACCESS_DENIED, "Access is denied")).when(aclEvaluate)
                .checkProjectQueryPermission(PROJECT);
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/async_query").param("project", PROJECT)
                .param("older_than", "2011-11-11 11:11:11")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());

        Mockito.verify(nAsyncQueryController).batchDelete(Mockito.any(), Mockito.any());
    }

    @Test
    public void testDeleteByQueryIdNoProjectPermission() throws Exception {
        Mockito.doThrow(new KylinException(ACCESS_DENIED, "Access is denied")).when(aclEvaluate)
                .checkProjectAdminPermission(PROJECT);

        mockMvc.perform(MockMvcRequestBuilders.delete("/api/async_query/{query_id}", "123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());

        Mockito.verify(nAsyncQueryController).deleteByQueryId(Mockito.anyString(), Mockito.any(), Mockito.any());
    }

    @Test
    public void testQueryStatusNoProjectPermission() throws Exception {
        Mockito.doThrow(new KylinException(ACCESS_DENIED, "Access is denied")).when(aclEvaluate)
                .checkProjectQueryPermission(PROJECT);

        mockMvc.perform(MockMvcRequestBuilders.get("/api/async_query/{query_id:.+}/status", "123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());

        Mockito.verify(nAsyncQueryController).inqueryStatus(Mockito.any(), Mockito.anyString(), Mockito.any());
    }

    @Test
    public void testFileStatusNoProjectPermission() throws Exception {
        Mockito.doThrow(new KylinException(ACCESS_DENIED, "Access is denied")).when(aclEvaluate)
                .checkProjectQueryPermission(PROJECT);

        mockMvc.perform(MockMvcRequestBuilders.get("/api/async_query/{query_id:.+}/file_status", "123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());

        Mockito.verify(nAsyncQueryController).fileStatus(Mockito.anyString(), Mockito.any(), Mockito.any());
    }

    @Test
    public void testMetadataNoProjectPermission() throws Exception {
        Mockito.doThrow(new KylinException(ACCESS_DENIED, "Access is denied")).when(aclEvaluate)
                .checkProjectQueryPermission(PROJECT);

        mockMvc.perform(MockMvcRequestBuilders.get("/api/async_query/{query_id:.+}/metadata", "123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());

        Mockito.verify(nAsyncQueryController).metadata(Mockito.any(), Mockito.anyString(), Mockito.any());
    }

    @Test
    public void testDownloadQueryResultNoProjectPermission() throws Exception {
        Mockito.doThrow(new KylinException(ACCESS_DENIED, "Access is denied")).when(aclEvaluate)
                .checkProjectReadPermission(PROJECT);

        mockMvc.perform(MockMvcRequestBuilders.get("/api/async_query/{query_id:.+}/result_download", "123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());

        Mockito.verify(nAsyncQueryController).downloadQueryResult(Mockito.anyString(), Mockito.any(), Mockito.any(),
                Mockito.any(), Mockito.any(), Mockito.any());
    }

    @Test
    public void testDownloadQueryResultWhenQueryNotExist() throws Exception {
        Mockito.doReturn(true).when(asyncQueryService).hasPermission(Mockito.anyString(), Mockito.anyString());
        Mockito.doThrow(new IOException()).when(asyncQueryService).getFileInfo(Mockito.anyString(),
                Mockito.anyString());
        Mockito.doThrow(new KylinException(ASYNC_QUERY_RESULT_NOT_FOUND)).when(asyncQueryService)
                .checkStatus(Mockito.anyString(), Mockito.any(), Mockito.anyString(), Mockito.anyString());

        mockMvc.perform(MockMvcRequestBuilders.get("/api/async_query/{query_id:.+}/result_download", "123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON))).andExpect(result -> {
                    String message = result.getResolvedException().getMessage();
                    Assert.assertTrue(message.contains(
                            "Can’t find the query by this query ID in this project. Please check and try again."));
                });

        Mockito.verify(nAsyncQueryController).downloadQueryResult(Mockito.anyString(), Mockito.any(), Mockito.any(),
                Mockito.any(), Mockito.any(), Mockito.any());
    }

    @Test
    public void testResultPathNoProjectPermission() throws Exception {
        Mockito.doThrow(new KylinException(ACCESS_DENIED, "Access is denied")).when(aclEvaluate)
                .checkProjectQueryPermission(PROJECT);

        mockMvc.perform(MockMvcRequestBuilders.get("/api/async_query/{query_id}/result_path", "123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());

        Mockito.verify(nAsyncQueryController).queryPath(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
    }

    @Test
    public void testQuery() throws Exception {
        Mockito.doReturn(SUCCESS).when(asyncQueryService).queryStatus(Mockito.anyString(), Mockito.anyString());
        SQLResponse response = new SQLResponse();
        response.setException(false);
        Mockito.doReturn(response).when(kapQueryService).queryWithCache(Mockito.any());

        mockMvc.perform(MockMvcRequestBuilders.post("/api/async_query").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).query(Mockito.any());
    }

    @Test
    public void testQueryReachLimit() throws Exception {
        Mockito.doReturn(SUCCESS).when(asyncQueryService).queryStatus(Mockito.anyString(), Mockito.anyString());
        SQLResponse response = new SQLResponse();
        response.setException(false);
        Mockito.doReturn(response).when(kapQueryService).queryWithCache(Mockito.any());
        getTestConfig().setProperty("kylin.query.unique-async-query-yarn-queue-enabled", "true");
        getTestConfig().setProperty("kylin.query.async-query.max-concurrent-jobs", "3");
        reloadAsyncQueryRequestLimits();
        AsyncQueryRequestLimits limit1 = new AsyncQueryRequestLimits();
        AsyncQueryRequestLimits limit2 = new AsyncQueryRequestLimits();
        mockMvc.perform(MockMvcRequestBuilders.post("/api/async_query").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).query(Mockito.any());
        AsyncQueryRequestLimits limits3 = new AsyncQueryRequestLimits();
        mockMvc.perform(MockMvcRequestBuilders.post("/api/async_query").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().is5xxServerError()).andExpect(result -> {
                    Assert.assertTrue(result.getResolvedException() instanceof KylinException);
                    Assert.assertEquals(TOO_MANY_ASYNC_QUERY.toErrorCode().getCodeString(),
                            ((KylinException) result.getResolvedException()).getErrorCode().getCodeString());
                    Assert.assertEquals(MsgPicker.getMsg().getAsyncQueryTooManyRunning(),
                            result.getResolvedException().getMessage());
                });
        limits3.close();
        mockMvc.perform(MockMvcRequestBuilders.post("/api/async_query").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        limit1.close();
        limit2.close();

    }

    @Test
    public void testQueryReachLimitCase2() throws Exception {

        Mockito.doReturn(SUCCESS).when(asyncQueryService).queryStatus(Mockito.anyString(), Mockito.anyString());
        SQLResponse response = new SQLResponse();
        response.setException(false);
        Mockito.doReturn(response).when(kapQueryService).queryWithCache(Mockito.any());
        getTestConfig().setProperty("kylin.query.unique-async-query-yarn-queue-enabled", "true");
        getTestConfig().setProperty("kylin.query.async-query.max-concurrent-jobs", "0");
        reloadAsyncQueryRequestLimits();

        AsyncQueryRequestLimits limit1 = new AsyncQueryRequestLimits();
        AsyncQueryRequestLimits limit2 = new AsyncQueryRequestLimits();
        mockMvc.perform(MockMvcRequestBuilders.post("/api/async_query").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        limit1.close();
        limit2.close();

    }

    private void reloadAsyncQueryRequestLimits() throws Exception {
        Field field = AsyncQueryRequestLimits.class.getDeclaredField("MAX_COUNT");
        field.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        int count = KylinConfig.getInstanceFromEnv().getAsyncQueryMaxConcurrentJobs();
        field.setInt(null, count);
    }

    @Test
    public void testAsyncQueryContextClean() throws Exception {
        AsyncQuerySQLRequest asyncQuerySQLRequest1 = new AsyncQuerySQLRequest();
        asyncQuerySQLRequest1.setProject(PROJECT);
        AsyncQuerySQLRequest asyncQuerySQLRequest2 = new AsyncQuerySQLRequest();
        asyncQuerySQLRequest2.setProject(PROJECT);
        SQLResponse sqlResponse = new SQLResponse();
        sqlResponse.setException(false);

        Mockito.doReturn(AsyncQueryService.QueryStatus.SUCCESS).when(asyncQueryService).queryStatus(Mockito.anyString(),
                Mockito.anyString());
        Mockito.doReturn(sqlResponse).when(kapQueryService).queryWithCache(Mockito.any());

        EnvelopeResponse<AsyncQueryResponse> query1 = nAsyncQueryController.query(asyncQuerySQLRequest1);
        EnvelopeResponse<AsyncQueryResponse> query2 = nAsyncQueryController.query(asyncQuerySQLRequest2);

        Assert.assertNotEquals(query1.getData().getQueryID(), query2.getData().getQueryID());
    }

    @Test
    public void testQueryResponseException() throws Exception {
        Mockito.doReturn(SUCCESS).when(asyncQueryService).queryStatus(Mockito.anyString(), Mockito.anyString());
        SQLResponse response = new SQLResponse();
        response.setException(true);
        Mockito.doReturn(response).when(kapQueryService).queryWithCache(Mockito.any());

        mockMvc.perform(MockMvcRequestBuilders.post("/api/async_query").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).query(Mockito.any());
    }

    @Test
    public void testQueryException() throws Exception {
        Mockito.doReturn(SUCCESS).when(asyncQueryService).queryStatus(Mockito.anyString(), Mockito.anyString());
        mockMvc.perform(MockMvcRequestBuilders.post("/api/async_query").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).query(Mockito.any());
    }

    @Test
    public void testQuerySuccess() throws Exception {
        SQLResponse response = new SQLResponse();
        response.setException(false);
        Mockito.doReturn(response).when(kapQueryService).queryWithCache(Mockito.any());

        Mockito.doReturn(SUCCESS).when(asyncQueryService).queryStatus(Mockito.anyString(), Mockito.anyString());

        mockMvc.perform(MockMvcRequestBuilders.post("/api/async_query").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).query(Mockito.any());
    }

    @Test
    public void testQueryFailed() throws Exception {
        SQLResponse response = new SQLResponse();
        response.setException(false);
        Mockito.doReturn(response).when(kapQueryService).queryWithCache(Mockito.any());

        Mockito.doReturn(FAILED).when(asyncQueryService).queryStatus(Mockito.anyString(), Mockito.anyString());

        mockMvc.perform(MockMvcRequestBuilders.post("/api/async_query").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).query(Mockito.any());
    }

    @Test
    public void testQueryRunning() throws Exception {
        SQLResponse response = new SQLResponse();
        response.setException(false);
        Mockito.doReturn(response).when(kapQueryService).queryWithCache(Mockito.any());

        Mockito.doReturn(RUNNING).when(asyncQueryService).queryStatus(Mockito.anyString(), Mockito.anyString());

        mockMvc.perform(MockMvcRequestBuilders.post("/api/async_query").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).query(Mockito.any());
    }

    @Test
    public void testQueryMiss() throws Exception {
        SQLResponse response = new SQLResponse();
        response.setException(false);
        Mockito.doReturn(response).when(kapQueryService).queryWithCache(Mockito.any());

        Mockito.doReturn(MISS).when(asyncQueryService).queryStatus(Mockito.anyString(), Mockito.anyString());

        mockMvc.perform(MockMvcRequestBuilders.post("/api/async_query").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).query(Mockito.any());
    }

    @Test
    public void testBatchDeleteAll() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/async_query")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).batchDelete(null, null);
    }

    @Test
    public void testBatchDeleteOldResult() throws Exception {
        Mockito.doReturn(true).when(asyncQueryService).batchDelete(Mockito.anyString(), Mockito.anyString());

        mockMvc.perform(MockMvcRequestBuilders.delete("/api/async_query")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        nAsyncQueryController.batchDelete(PROJECT, "2011-11-11 11:11:11");
    }

    @Test
    public void testBatchDeleteOldResultWhenTimeFormatError() throws Exception {
        Mockito.doThrow(new ParseException("", 0)).when(asyncQueryService).batchDelete(PROJECT, "2011-11/11 11:11:11");

        mockMvc.perform(MockMvcRequestBuilders.delete("/api/async_query").param("project", PROJECT)
                .param("older_than", "2011-11/11 11:11:11")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON))).andExpect(result -> {
                    Assert.assertTrue(result.getResolvedException() instanceof KylinException);
                    Assert.assertEquals("KE-010031303",
                            ((KylinException) result.getResolvedException()).getErrorCode().getCodeString());
                    Assert.assertEquals(ASYNC_QUERY_TIME_FORMAT_ERROR.getMsg(),
                            result.getResolvedException().getMessage());
                });
    }

    @Test
    public void testDeleteByQueryIdNoPermission() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/async_query/{query_id}", "123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).deleteByQueryId(Mockito.anyString(), Mockito.any(), Mockito.any());
    }

    @Test
    public void testDeleteByQueryIdSuccess() throws Exception {
        Mockito.doReturn(true).when(asyncQueryService).hasPermission(Mockito.anyString(), Mockito.anyString());
        Mockito.doReturn(true).when(asyncQueryService).deleteByQueryId(Mockito.anyString(), Mockito.anyString());

        mockMvc.perform(MockMvcRequestBuilders.delete("/api/async_query/{query_id}", "123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).deleteByQueryId(Mockito.anyString(), Mockito.any(), Mockito.any());
    }

    @Test
    public void testDeleteByQueryIdFailed() throws Exception {
        Mockito.doReturn(true).when(asyncQueryService).hasPermission(Mockito.anyString(), Mockito.anyString());
        Mockito.doReturn(false).when(asyncQueryService).deleteByQueryId(Mockito.anyString(), Mockito.anyString());

        mockMvc.perform(MockMvcRequestBuilders.delete("/api/async_query/{query_id}", "123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).deleteByQueryId(Mockito.anyString(), Mockito.any(), Mockito.any());
    }

    @Test
    public void testInqueryStatusNoPermission() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/async_query/{query_id}/status", "123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).inqueryStatus(Mockito.any(), Mockito.anyString(), Mockito.any());
    }

    @Test
    public void testInqueryStatusSuccess() throws Exception {
        Mockito.doReturn(true).when(asyncQueryService).hasPermission(Mockito.anyString(), Mockito.anyString());
        Mockito.doReturn(SUCCESS).when(asyncQueryService).queryStatus(Mockito.anyString(), Mockito.anyString());

        mockMvc.perform(MockMvcRequestBuilders.get("/api/async_query/{query_id}/status", "123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).inqueryStatus(Mockito.any(), Mockito.anyString(), Mockito.any());
    }

    @Test
    public void testInqueryStatusFailed() throws Exception {
        Mockito.doReturn(true).when(asyncQueryService).hasPermission(Mockito.anyString(), Mockito.anyString());
        Mockito.doReturn(FAILED).when(asyncQueryService).queryStatus(Mockito.anyString(), Mockito.anyString());

        mockMvc.perform(MockMvcRequestBuilders.get("/api/async_query/{query_id}/status", "123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).inqueryStatus(Mockito.any(), Mockito.anyString(), Mockito.any());
    }

    @Test
    public void testInqueryStatusRunning() throws Exception {
        Mockito.doReturn(true).when(asyncQueryService).hasPermission(Mockito.anyString(), Mockito.anyString());
        Mockito.doReturn(RUNNING).when(asyncQueryService).queryStatus(Mockito.anyString(), Mockito.anyString());

        mockMvc.perform(MockMvcRequestBuilders.get("/api/async_query/{query_id}/status", "123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).inqueryStatus(Mockito.any(), Mockito.anyString(), Mockito.any());
    }

    @Test
    public void testInqueryStatusMiss() throws Exception {
        Mockito.doReturn(true).when(asyncQueryService).hasPermission(Mockito.anyString(), Mockito.anyString());
        Mockito.doReturn(MISS).when(asyncQueryService).queryStatus(Mockito.anyString(), Mockito.anyString());

        mockMvc.perform(MockMvcRequestBuilders.get("/api/async_query/{query_id}/status", "123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).inqueryStatus(Mockito.any(), Mockito.anyString(), Mockito.any());
    }

    @Test
    public void testFileStatusNoPermission() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/async_query/{query_id}/file_status", "123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).fileStatus(Mockito.anyString(), Mockito.any(), Mockito.any());
    }

    @Test
    public void testFileStatus() throws Exception {
        Mockito.doReturn(true).when(asyncQueryService).hasPermission(Mockito.anyString(), Mockito.anyString());

        mockMvc.perform(MockMvcRequestBuilders.get("/api/async_query/{query_id}/file_status", "123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).fileStatus(Mockito.anyString(), Mockito.any(), Mockito.any());
    }

    @Test
    public void testMetadataNoPermission() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/async_query/{query_id:.+}/metadata", "123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).metadata(Mockito.any(), Mockito.anyString(), Mockito.any());
    }

    @Test
    public void testMetadata() throws Exception {
        Mockito.doReturn(true).when(asyncQueryService).hasPermission(Mockito.anyString(), Mockito.anyString());

        mockMvc.perform(MockMvcRequestBuilders.get("/api/async_query/{query_id:.+}/metadata", "123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).metadata(Mockito.any(), Mockito.anyString(), Mockito.any());
    }

    @Test
    public void testDownloadQueryResultNoPermission() throws Exception {
        Mockito.doReturn(KylinConfig.getInstanceFromEnv()).when(kapQueryService).getConfig();
        AsyncQueryService.FileInfo fileInfo = new AsyncQueryService.FileInfo("csv", "gbk", "result");
        Mockito.doReturn(fileInfo).when(asyncQueryService).getFileInfo(Mockito.anyString(), Mockito.anyString());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/async_query/{query_id:.+}/result_download", "123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON))).andExpect(result -> {
                    Assert.assertTrue(result.getResolvedException() instanceof KylinException);
                    KylinException resolvedException = (KylinException) result.getResolvedException();
                    Assert.assertEquals("KE-010024001", resolvedException.getErrorCode().getCodeString());
                    Assert.assertEquals(MsgPicker.getMsg().getForbiddenExportAsyncQueryResult(),
                            resolvedException.getMessage());
                });

        Mockito.verify(nAsyncQueryController).downloadQueryResult(Mockito.anyString(), Mockito.any(), Mockito.any(),
                Mockito.any(), Mockito.any(), Mockito.any());
    }

    @Test
    public void testDownloadQueryResult() throws Exception {
        Mockito.doReturn(true).when(asyncQueryService).hasPermission(Mockito.anyString(), Mockito.anyString());
        AsyncQueryService.FileInfo fileInfo = new AsyncQueryService.FileInfo("csv", "gbk", "result");
        Mockito.doReturn(fileInfo).when(asyncQueryService).getFileInfo(Mockito.anyString(), Mockito.anyString());
        Mockito.doReturn(KylinConfig.getInstanceFromEnv()).when(kapQueryService).getConfig();

        mockMvc.perform(MockMvcRequestBuilders.get("/api/async_query/{query_id:.+}/result_download", "123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).downloadQueryResult(Mockito.anyString(), Mockito.any(), Mockito.any(),
                Mockito.any(), Mockito.any(), Mockito.any());
    }

    @Test
    public void testQueryPathNoPermission() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/async_query/{query_id}/result_path", "123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).queryPath(Mockito.anyString(), Mockito.any(), Mockito.any(),
                Mockito.any());
    }

    @Test
    public void testQueryPath() throws Exception {
        Mockito.doReturn(true).when(asyncQueryService).hasPermission(Mockito.anyString(), Mockito.anyString());

        mockMvc.perform(MockMvcRequestBuilders.get("/api/async_query/{query_id}/result_path", "123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).queryPath(Mockito.anyString(), Mockito.any(), Mockito.any(),
                Mockito.any());
    }

    @Test
    public void testDeleteByQueryIdWhenProjectIsNull() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/async_query/{query_id}", "123")
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(result -> {
                    Assert.assertTrue(result.getResolvedException() instanceof KylinException);
                    Assert.assertEquals("KE-010031302",
                            ((KylinException) result.getResolvedException()).getErrorCode().getCodeString());
                    Assert.assertEquals(ASYNC_QUERY_PROJECT_NAME_EMPTY.getMsg(),
                            result.getResolvedException().getMessage());
                });
    }

    @Test
    public void testQueryStatusWhenProjectIsNull() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/async_query/{query_id:.+}/status", "123")
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(result -> {
                    Assert.assertTrue(result.getResolvedException() instanceof KylinException);
                    Assert.assertEquals("KE-010031302",
                            ((KylinException) result.getResolvedException()).getErrorCode().getCodeString());
                    Assert.assertEquals(ASYNC_QUERY_PROJECT_NAME_EMPTY.getMsg(),
                            result.getResolvedException().getMessage());
                });
    }

    @Test
    public void testFileStatusWhenProjectIsNull() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/async_query/{query_id:.+}/file_status", "123")
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(result -> {
                    Assert.assertTrue(result.getResolvedException() instanceof KylinException);
                    Assert.assertEquals("KE-010031302",
                            ((KylinException) result.getResolvedException()).getErrorCode().getCodeString());
                    Assert.assertEquals(ASYNC_QUERY_PROJECT_NAME_EMPTY.getMsg(),
                            result.getResolvedException().getMessage());
                });
    }

    @Test
    public void testMetadataWhenProjectIsNull() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/async_query/{query_id:.+}/metadata", "123")
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(result -> {
                    Assert.assertTrue(result.getResolvedException() instanceof KylinException);
                    Assert.assertEquals("KE-010031302",
                            ((KylinException) result.getResolvedException()).getErrorCode().getCodeString());
                    Assert.assertEquals(ASYNC_QUERY_PROJECT_NAME_EMPTY.getMsg(),
                            result.getResolvedException().getMessage());
                });
    }

    @Test
    public void testDownloadQueryResultWhenProjectIsNull() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/async_query/{query_id:.+}/result_download", "123")
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(result -> {
                    Assert.assertTrue(result.getResolvedException() instanceof KylinException);
                    Assert.assertEquals("KE-010031302",
                            ((KylinException) result.getResolvedException()).getErrorCode().getCodeString());
                    Assert.assertEquals(ASYNC_QUERY_PROJECT_NAME_EMPTY.getMsg(),
                            result.getResolvedException().getMessage());
                });
    }

    @Test
    public void testQueryPathWhenProjectIsNull() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/async_query/{query_id}/result_path", "123")
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(result -> {
                    Assert.assertTrue(result.getResolvedException() instanceof KylinException);
                    Assert.assertEquals("KE-010031302",
                            ((KylinException) result.getResolvedException()).getErrorCode().getCodeString());
                    Assert.assertEquals(ASYNC_QUERY_PROJECT_NAME_EMPTY.getMsg(),
                            result.getResolvedException().getMessage());
                });
    }

    @Test
    public void testCheckUserPermissionBeforeQueryTaskComplete() throws Exception {
        Authentication otherUser = new TestingAuthenticationToken("OTHER", "OTHER", Constant.IDENTITY_USER);
        SecurityContextHolder.getContext().setAuthentication(otherUser);

        AsyncQueryService service = new AsyncQueryService();
        Mockito.doAnswer(invocation -> {
            service.saveQueryUsername(PROJECT, "123");
            return null;
        }).when(asyncQueryService).saveQueryUsername(Mockito.anyString(), Mockito.anyString());
        Mockito.doReturn(SUCCESS).when(asyncQueryService).queryStatus(Mockito.anyString(), Mockito.anyString());
        nAsyncQueryController.query(mockAsyncQuerySQLRequest());
        Thread.sleep(5000);

        Assert.assertTrue(service.hasPermission("123", PROJECT));
        SecurityContextHolder.getContext().setAuthentication(authentication);
    }
}

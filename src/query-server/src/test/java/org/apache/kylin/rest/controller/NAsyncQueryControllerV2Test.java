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
import static org.apache.kylin.common.exception.code.ErrorCodeServer.ASYNC_QUERY_INCLUDE_HEADER_NOT_EMPTY;
import static org.apache.kylin.rest.service.AsyncQueryService.QueryStatus.FAILED;
import static org.apache.kylin.rest.service.AsyncQueryService.QueryStatus.MISS;
import static org.apache.kylin.rest.service.AsyncQueryService.QueryStatus.RUNNING;
import static org.apache.kylin.rest.service.AsyncQueryService.QueryStatus.SUCCESS;

import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.AsyncQuerySQLRequestV2;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.service.AsyncQueryService;
import org.apache.kylin.rest.service.QueryService;
import org.apache.kylin.rest.util.AclEvaluate;
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

public class NAsyncQueryControllerV2Test extends NLocalFileMetadataTestCase {

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

    @InjectMocks
    private NAsyncQueryControllerV2 nAsyncQueryControllerV2 = Mockito.spy(new NAsyncQueryControllerV2());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() throws IOException {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(nAsyncQueryControllerV2)
                .defaultRequest(MockMvcRequestBuilders.get("/")).build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
        createTestMetadata();

        Mockito.doReturn("default").when(asyncQueryService).searchQueryResultProject(Mockito.anyString());
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
        QueryContext.current().close();
    }

    private AsyncQuerySQLRequestV2 mockAsyncQuerySQLRequest() {
        final AsyncQuerySQLRequestV2 asyncQuerySQLRequest = new AsyncQuerySQLRequestV2();
        asyncQuerySQLRequest.setLimit(500);
        asyncQuerySQLRequest.setOffset(0);
        asyncQuerySQLRequest.setProject(PROJECT);
        asyncQuerySQLRequest.setSql("select PART_DT from KYLIN_SALES limit 500");
        asyncQuerySQLRequest.setSeparator(",");
        return asyncQuerySQLRequest;
    }

    @Test
    public void testQuery() throws Exception {
        Mockito.doReturn(SUCCESS).when(asyncQueryService).queryStatus(Mockito.anyString(), Mockito.anyString());
        SQLResponse response = new SQLResponse();
        response.setException(false);
        Mockito.doReturn(response).when(kapQueryService).queryWithCache(Mockito.any());

        mockMvc.perform(MockMvcRequestBuilders.post("/api/async_query").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryControllerV2).query(Mockito.any());
    }

    @Test
    public void testQuerySuccess() throws Exception {
        SQLResponse response = new SQLResponse();
        response.setException(false);
        Mockito.doReturn(response).when(kapQueryService).queryWithCache(Mockito.any());

        Mockito.doReturn(SUCCESS).when(asyncQueryService).queryStatus(Mockito.anyString(), Mockito.anyString());

        mockMvc.perform(MockMvcRequestBuilders.post("/api/async_query").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryControllerV2).query(Mockito.any());
    }

    @Test
    public void testQueryFailed() throws Exception {
        SQLResponse response = new SQLResponse();
        response.setException(false);
        Mockito.doReturn(response).when(kapQueryService).queryWithCache(Mockito.any());

        Mockito.doReturn(FAILED).when(asyncQueryService).queryStatus(Mockito.anyString(), Mockito.anyString());

        mockMvc.perform(MockMvcRequestBuilders.post("/api/async_query").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryControllerV2).query(Mockito.any());
    }

    @Test
    public void testQueryRunning() throws Exception {
        SQLResponse response = new SQLResponse();
        response.setException(false);
        Mockito.doReturn(response).when(kapQueryService).queryWithCache(Mockito.any());

        Mockito.doReturn(RUNNING).when(asyncQueryService).queryStatus(Mockito.anyString(), Mockito.anyString());

        mockMvc.perform(MockMvcRequestBuilders.post("/api/async_query").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryControllerV2).query(Mockito.any());
    }

    @Test
    public void testQueryMiss() throws Exception {
        SQLResponse response = new SQLResponse();
        response.setException(false);
        Mockito.doReturn(response).when(kapQueryService).queryWithCache(Mockito.any());

        Mockito.doReturn(MISS).when(asyncQueryService).queryStatus(Mockito.anyString(), Mockito.anyString());

        mockMvc.perform(MockMvcRequestBuilders.post("/api/async_query").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryControllerV2).query(Mockito.any());
    }

    @Test
    public void testInqueryStatusSuccess() throws Exception {
        Mockito.doReturn(true).when(asyncQueryService).hasPermission(Mockito.anyString(), Mockito.anyString());
        Mockito.doReturn(SUCCESS).when(asyncQueryService).queryStatus(Mockito.anyString(), Mockito.anyString());

        mockMvc.perform(MockMvcRequestBuilders.get("/api/async_query/{query_id}/status", "123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryControllerV2).inqueryStatus(Mockito.anyString());
    }

    @Test
    public void testFileStatus() throws Exception {
        Mockito.doReturn(true).when(asyncQueryService).hasPermission(Mockito.anyString(), Mockito.anyString());

        mockMvc.perform(MockMvcRequestBuilders.get("/api/async_query/{query_id}/filestatus", "123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryControllerV2).fileStatus(Mockito.anyString());
    }

    @Test
    public void testMetadata() throws Exception {
        Mockito.doReturn(true).when(asyncQueryService).hasPermission(Mockito.anyString(), Mockito.anyString());

        mockMvc.perform(MockMvcRequestBuilders.get("/api/async_query/{query_id:.+}/metadata", "123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryControllerV2).metadata(Mockito.anyString());
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
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryControllerV2).downloadQueryResult(Mockito.anyString(), Mockito.any(), Mockito.any());
    }

    @Test
    public void testDownloadQueryResultNotIncludeHeader() throws Exception {
        Mockito.doReturn(true).when(asyncQueryService).hasPermission(Mockito.anyString(), Mockito.anyString());
        AsyncQueryService.FileInfo fileInfo = new AsyncQueryService.FileInfo("csv", "gbk", "result");
        Mockito.doReturn(fileInfo).when(asyncQueryService).getFileInfo(Mockito.anyString(), Mockito.anyString());
        Mockito.doReturn(KylinConfig.getInstanceFromEnv()).when(kapQueryService).getConfig();

        mockMvc.perform(MockMvcRequestBuilders.get("/api/async_query/{query_id:.+}/result_download", "123")
                .param("includeHeader", "false").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andExpect(result -> {
                    Assert.assertTrue(result.getResolvedException() instanceof KylinException);
                    Assert.assertEquals(ASYNC_QUERY_INCLUDE_HEADER_NOT_EMPTY.getMsg(),
                            result.getResolvedException().getMessage());
                });

        Mockito.verify(nAsyncQueryControllerV2).downloadQueryResult(Mockito.anyString(), Mockito.any(), Mockito.any());
    }

}

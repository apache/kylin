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

import io.kyligence.kap.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.PrepareSqlRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.service.QueryService;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.query.NativeQueryRealization;
import org.apache.kylin.metadata.query.QueryMetrics;
import org.apache.kylin.rest.controller.v2.NQueryControllerV2;
import org.apache.kylin.rest.response.SQLResponseV2;
import org.apache.kylin.rest.service.QueryHistoryService;
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

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V2_JSON;

public class NQueryControllerV2Test extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;

    @Mock
    private QueryService kapQueryService;

    @Mock
    private QueryHistoryService queryHistoryService;

    @InjectMocks
    private NQueryControllerV2 nQueryControllerV2 = Mockito.spy(new NQueryControllerV2());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(nQueryControllerV2).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
        createTestMetadata();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    private PrepareSqlRequest mockPrepareSqlRequest() {
        final PrepareSqlRequest sqlRequest = new PrepareSqlRequest();
        sqlRequest.setSql("SELECT * FROM empty_table");
        sqlRequest.setProject("default");
        return sqlRequest;
    }

    @Test
    public void testPrepareQuery() throws Exception {
        final PrepareSqlRequest sqlRequestV2 = mockPrepareSqlRequest();
        mockMvc.perform(MockMvcRequestBuilders.post("/api/query/prestate").contentType(MediaType.APPLICATION_JSON)
                        .content(JsonUtil.writeValueAsString(sqlRequestV2))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nQueryControllerV2).prepareQuery(Mockito.any());
    }

    @Test
    public void testQuery() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.post("/api/query").contentType(MediaType.APPLICATION_JSON)
                        .content(JsonUtil.writeValueAsString(mockPrepareSqlRequest()))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nQueryControllerV2).query(Mockito.any());
    }

    private PrepareSqlRequest mockInvalidateTagSqlRequest() {
        final PrepareSqlRequest sqlRequest = new PrepareSqlRequest();
        sqlRequest.setSql("SELECT * FROM empty_table");
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i <= 256; i++) {
            builder.append('a');
        }
        sqlRequest.setUser_defined_tag(builder.toString());
        return sqlRequest;
    }

    @Test
    public void testQueryUserTagExceedLimitation() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.post("/api/query").contentType(MediaType.APPLICATION_JSON)
                        .content(JsonUtil.writeValueAsString(mockInvalidateTagSqlRequest()))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isBadRequest())
                .andExpect(MockMvcResultMatchers.jsonPath("$.code").value("999")).andExpect(MockMvcResultMatchers
                        .jsonPath("$.msg").value("Can’t add the tag, as the length exceeds the maximum 256 characters. Please modify it."));

        Mockito.verify(nQueryControllerV2, Mockito.times(0)).query(Mockito.any());
    }

    @Test
    public void testPrepareQueryUserTagExceedLimitation() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.post("/api/query/prestate").contentType(MediaType.APPLICATION_JSON)
                        .content(JsonUtil.writeValueAsString(mockInvalidateTagSqlRequest()))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isBadRequest());

        Mockito.verify(nQueryControllerV2, Mockito.times(0)).prepareQuery(Mockito.any());
    }

    @Test
    public void testSqlResponseV2() {
        SQLResponse sqlResponse = new SQLResponse();
        sqlResponse.setScanRows(Lists.newArrayList(50L, 10L));
        NativeQueryRealization nativeQueryRealization1 = new NativeQueryRealization();
        nativeQueryRealization1.setIndexType(QueryMetrics.AGG_INDEX);
        nativeQueryRealization1.setModelAlias("modelA");
        NativeQueryRealization nativeQueryRealization2 = new NativeQueryRealization();
        nativeQueryRealization2.setIndexType(QueryMetrics.TABLE_INDEX);
        nativeQueryRealization2.setModelAlias("modelB");
        NativeQueryRealization nativeQueryRealization3 = new NativeQueryRealization();
        nativeQueryRealization3.setIndexType(QueryMetrics.TABLE_INDEX);
        nativeQueryRealization3.setModelAlias("modelA");
        NativeQueryRealization nativeQueryRealization4 = new NativeQueryRealization();
        nativeQueryRealization4.setIndexType(QueryMetrics.AGG_INDEX);
        nativeQueryRealization4.setModelAlias("modelB");
        NativeQueryRealization nativeQueryRealization5 = new NativeQueryRealization();
        nativeQueryRealization5.setIndexType(QueryMetrics.TABLE_SNAPSHOT);
        nativeQueryRealization5.setModelAlias("modelB");
        sqlResponse.setNativeRealizations(Lists.newArrayList(nativeQueryRealization1, nativeQueryRealization2));
        SQLResponseV2 sqlResponseV2 = new SQLResponseV2(sqlResponse);
        Assert.assertEquals(sqlResponseV2.getThrowable(), sqlResponse.getThrowable());
        Assert.assertEquals(sqlResponseV2.getTotalScanRows(), sqlResponse.getTotalScanRows());
        Assert.assertEquals(sqlResponseV2.getTotalScanBytes(), sqlResponse.getTotalScanBytes());
        Assert.assertEquals(sqlResponseV2.getTotalScanCount(), sqlResponse.getTotalScanRows());
        Assert.assertTrue(sqlResponseV2.isSparderUsed());
        Assert.assertEquals("CUBE[name=modelA],INVERTED_INDEX[name=modelB]", sqlResponseV2.getCube());

        SQLResponseV2 sqlResponseV22 = new SQLResponseV2();
        Assert.assertEquals(0, sqlResponseV22.getTotalScanCount());
        Assert.assertFalse(sqlResponseV22.isSparderUsed());
        Assert.assertTrue(StringUtils.isEmpty(sqlResponseV22.adapterCubeField(sqlResponseV22.getNativeRealizations())));
        Assert.assertEquals("CUBE[name=modelA]",
                sqlResponseV22.adapterCubeField(Lists.newArrayList(nativeQueryRealization1)));
        Assert.assertEquals("CUBE[name=modelB]",
                sqlResponseV22.adapterCubeField(Lists.newArrayList(nativeQueryRealization5)));
        Assert.assertEquals("INVERTED_INDEX[name=modelA]",
                sqlResponseV22.adapterCubeField(Lists.newArrayList(nativeQueryRealization3)));
        Assert.assertEquals("CUBE[name=modelA],CUBE[name=modelB],INVERTED_INDEX[name=modelB]",
                sqlResponseV22.adapterCubeField(
                        Lists.newArrayList(nativeQueryRealization1, nativeQueryRealization2, nativeQueryRealization4)));
        Assert.assertThrows(NullPointerException.class, () -> new SQLResponseV2(null));
    }
}

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

import java.nio.charset.StandardCharsets;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.PartitionKeyRequest;
import org.apache.kylin.rest.request.RefreshSegmentsRequest;
import org.apache.kylin.rest.request.SamplingRequest;
import org.apache.kylin.rest.request.TableLoadRequest;
import org.apache.kylin.rest.service.ModelBuildService;
import org.apache.kylin.rest.service.TableSamplingService;
import org.apache.kylin.rest.service.TableService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class SampleControllerTest extends NLocalFileMetadataTestCase {

    private static final String APPLICATION_JSON = HTTP_VND_APACHE_KYLIN_JSON;

    private MockMvc mockMvc;

    @Mock
    private ModelBuildService modelBuildService;

    @Mock
    private TableSamplingService tableSamplingService;

    @Mock
    private TableService tableService;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @InjectMocks
    private final SampleController sampleController = Mockito.spy(new SampleController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(sampleController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .defaultResponseCharacterEncoding(StandardCharsets.UTF_8).build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    private PartitionKeyRequest mockFactTableRequest() {
        final PartitionKeyRequest partitionKeyRequest = new PartitionKeyRequest();
        partitionKeyRequest.setProject("default");
        partitionKeyRequest.setTable("table1");
        partitionKeyRequest.setColumn("CAL_DT");
        return partitionKeyRequest;
    }

    private TableLoadRequest mockLoadTableRequest() {
        final TableLoadRequest tableLoadRequest = new TableLoadRequest();
        tableLoadRequest.setProject("default");
        tableLoadRequest.setDataSourceType(11);
        String[] tables = { "table1", "DEFAULT.TEST_ACCOUNT" };
        String[] dbs = { "db1", "default" };
        tableLoadRequest.setTables(tables);
        tableLoadRequest.setDatabases(dbs);
        return tableLoadRequest;
    }

    @Test
    public void testRefreshSegments() throws Exception {
        Mockito.doNothing().when(modelBuildService).refreshSegments("default", "TEST_KYLIN_FACT", "0", "100", "0",
                "100");
        RefreshSegmentsRequest refreshSegmentsRequest = new RefreshSegmentsRequest();
        refreshSegmentsRequest.setProject("default");
        refreshSegmentsRequest.setRefreshStart("0");
        refreshSegmentsRequest.setRefreshEnd("100");
        refreshSegmentsRequest.setAffectedStart("0");
        refreshSegmentsRequest.setAffectedEnd("100");
        refreshSegmentsRequest.setTable("TEST_KYLIN_FACT");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/tables/data_range") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(refreshSegmentsRequest)) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(sampleController).refreshSegments(Mockito.any(RefreshSegmentsRequest.class));
    }

    @Test
    public void testSubmitSampling() throws Exception {
        final SamplingRequest request = new SamplingRequest();
        request.setProject("default");
        request.setRows(20000);
        request.setQualifiedTableName("default.test_kylin_fact");
        Mockito.doReturn(Lists.newArrayList()).when(tableSamplingService) //
                .sampling(Sets.newHashSet(request.getQualifiedTableName()), request.getProject(), request.getRows(),
                        ExecutablePO.DEFAULT_PRIORITY, null, null);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/sampling_jobs") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(request)) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(sampleController).submitSampling(Mockito.any(SamplingRequest.class));
    }

    @Test
    public void testSubmitSamplingFailedForNoTable() throws Exception {
        final SamplingRequest request = new SamplingRequest();
        request.setProject("default");
        request.setRows(20000);

        String errorMsg = "Can’t perform table sampling. Please select at least one table.";
        Mockito.doReturn(Lists.newArrayList()).when(tableSamplingService) //
                .sampling(Sets.newHashSet(request.getQualifiedTableName()), request.getProject(), request.getRows(),
                        ExecutablePO.DEFAULT_PRIORITY, null, null);
        final MvcResult mvcResult = mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/sampling_jobs") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(request)) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();
        Mockito.verify(sampleController).submitSampling(Mockito.any(SamplingRequest.class));
        final JsonNode jsonNode = JsonUtil.readValueAsTree(mvcResult.getResponse().getContentAsString());
        Assert.assertTrue(StringUtils.contains(jsonNode.get("exception").textValue(), errorMsg));
    }

    @Test
    public void testSubmitSamplingFailedForIllegalTableName() throws Exception {
        final SamplingRequest request = new SamplingRequest();
        request.setProject("default");
        request.setRows(20000);
        request.setQualifiedTableName("test_kylin_fact");

        String errorMsg = "The name of table for sampling is invalid. Please enter a table name like “database.table”.";
        Mockito.doReturn(Lists.newArrayList()).when(tableSamplingService) //
                .sampling(Sets.newHashSet(request.getQualifiedTableName()), request.getProject(), request.getRows(),
                        ExecutablePO.DEFAULT_PRIORITY, null, null);
        final MvcResult mvcResult = mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/sampling_jobs") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(request)) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();
        Mockito.verify(sampleController).submitSampling(Mockito.any(SamplingRequest.class));
        final JsonNode jsonNode = JsonUtil.readValueAsTree(mvcResult.getResponse().getContentAsString());
        Assert.assertTrue(StringUtils.contains(jsonNode.get("exception").textValue(), errorMsg));
    }

}

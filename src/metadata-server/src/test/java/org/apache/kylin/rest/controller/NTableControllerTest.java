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
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_SAMPLING_RANGE_INVALID;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.AWSTableLoadRequest;
import org.apache.kylin.rest.request.AutoMergeRequest;
import org.apache.kylin.rest.request.DateRangeRequest;
import org.apache.kylin.rest.request.PartitionKeyRequest;
import org.apache.kylin.rest.request.PushDownModeRequest;
import org.apache.kylin.rest.request.ReloadTableRequest;
import org.apache.kylin.rest.request.S3TableExtInfo;
import org.apache.kylin.rest.request.TableLoadRequest;
import org.apache.kylin.rest.request.TopTableRequest;
import org.apache.kylin.rest.request.UpdateAWSTableExtDescRequest;
import org.apache.kylin.rest.response.LoadTableResponse;
import org.apache.kylin.rest.response.TableNameResponse;
import org.apache.kylin.rest.response.TableRefresh;
import org.apache.kylin.rest.response.TablesAndColumnsResponse;
import org.apache.kylin.rest.response.UpdateAWSTableExtDescResponse;
import org.apache.kylin.rest.service.ModelService;
import org.apache.kylin.rest.service.TableExtService;
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

import lombok.val;

public class NTableControllerTest extends NLocalFileMetadataTestCase {

    private static final String APPLICATION_JSON = HTTP_VND_APACHE_KYLIN_JSON;

    private static final String APPLICATION_PUBLIC_JSON = HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

    private MockMvc mockMvc;

    @Mock
    private TableService tableService;

    @Mock
    private ModelService modelService;

    @Mock
    private TableExtService tableExtService;

    @Mock
    private TableSamplingService tableSamplingService;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @InjectMocks
    private NTableController nTableController = Mockito.spy(new NTableController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    private static final Integer MAX_SAMPLING_ROWS = 20_000_000;
    private static final Integer MIN_SAMPLING_ROWS = 10_000;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(nTableController).defaultRequest(MockMvcRequestBuilders.get("/"))
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
    public void testGetTableDesc() throws Exception {
        Mockito.when(tableService.getTableDesc("default", false, "", "DEFAULT", true)) //
                .thenReturn(mockTables());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/tables") //
                .contentType(MediaType.APPLICATION_JSON) //
                .param("ext", "false") //
                .param("project", "default") //
                .param("table", "") //
                .param("database", "DEFAULT") //
                .param("page_offset", "0") //
                .param("page_size", "10") //
                .param("is_fuzzy", "true") //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nTableController).getTableDesc(false, "default", "", "DEFAULT", true, 0, 10, 9);
    }

    @Test
    public void testGetProjectTables() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/project_tables") //
                .contentType(MediaType.APPLICATION_JSON) //
                .param("ext", "false") //
                .param("project", "default") //
                .param("table", "") //
                .param("database", "DEFAULT") //
                .param("source_type", "9") //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
    }

    @Test
    public void testGetTableDescWithName() throws Exception {
        Mockito.when(tableService.getTableDesc("default", true, "TEST_KYLIN_FACT", "DEFAULT", false))
                .thenReturn(mockTables());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/tables") //
                .contentType(MediaType.APPLICATION_JSON) //
                .param("withExt", "false") //
                .param("project", "default") //
                .param("table", "TEST_KYLIN_FACT") //
                .param("database", "DEFAULT") //
                .param("pageOffset", "0") //
                .param("pageSize", "10") //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nTableController).getTableDesc(false, "default", "TEST_KYLIN_FACT", "DEFAULT", false, 0, 10, 9);
    }

    @Test
    public void testShowDatabases() throws Exception {
        List<String> list = new ArrayList<>();
        list.add("ddd");
        list.add("fff");
        Mockito.when(tableService.getSourceDbNames("default")).thenReturn(list);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/databases") //
                .contentType(MediaType.APPLICATION_JSON) //
                .param("project", "default") //
                .param("datasourceType", "11") //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).showDatabases("default");
    }

    @Test
    public void testShowTables() throws Exception {
        List<TableNameResponse> list = new ArrayList<>();
        list.add(new TableNameResponse());
        list.add(new TableNameResponse());
        Mockito.when(tableService.getTableNameResponses("default", "db1", "")).thenReturn(list);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/names") //
                .contentType(MediaType.APPLICATION_JSON) //
                .param("project", "default") //
                .param("data_source_type", "11") //
                .param("database", "db1") //
                .param("page_offset", "0") //
                .param("page_size", "10") //
                .param("table", "") //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).showTables("default", 11, "", 0, 10, "db1");
    }

    @Test
    public void testSetPartitionKey() throws Exception {
        final PartitionKeyRequest partitionKeyRequest = mockFactTableRequest();
        Mockito.doNothing().when(tableService).setPartitionKey(partitionKeyRequest.getProject(),
                partitionKeyRequest.getTable(), partitionKeyRequest.getColumn(),
                partitionKeyRequest.getPartitionColumnFormat());

        mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/partition_key") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(partitionKeyRequest)) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).setPartitionKey(Mockito.any(PartitionKeyRequest.class));
    }

    @Test
    public void testSetNoPartitionKey() throws Exception {
        final PartitionKeyRequest partitionKeyRequest = mockFactTableRequest();
        partitionKeyRequest.setColumn("");
        Mockito.doNothing().when(tableService).setPartitionKey(partitionKeyRequest.getProject(),
                partitionKeyRequest.getTable(), partitionKeyRequest.getColumn(),
                partitionKeyRequest.getPartitionColumnFormat());

        mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/partition_key") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(partitionKeyRequest)) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).setPartitionKey(Mockito.any(PartitionKeyRequest.class));
    }

    @Test
    public void testSetTop() throws Exception {
        final TopTableRequest topTableRequest = mockTopTableRequest();
        Mockito.doNothing().when(tableService).setTop(topTableRequest.getTable(), topTableRequest.getProject(),
                topTableRequest.isTop());
        mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/top") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(topTableRequest)) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).setTableTop(Mockito.any(TopTableRequest.class));

    }

    private TopTableRequest mockTopTableRequest() {
        TopTableRequest topTableRequest = new TopTableRequest();
        topTableRequest.setTop(true);
        topTableRequest.setTable("table1");
        topTableRequest.setProject("default");
        return topTableRequest;
    }

    private DateRangeRequest mockDateRangeRequest() {
        DateRangeRequest request = new DateRangeRequest();
        request.setStart("0");
        request.setEnd("" + Long.MAX_VALUE);
        request.setProject("default");
        request.setTable("TEST_KYLIN_FACT");
        return request;
    }

    private void initMockito(LoadTableResponse loadTableResponse, TableLoadRequest tableLoadRequest) throws Exception {
        Mockito.when(tableExtService.loadDbTables(tableLoadRequest.getTables(), "default", false))
                .thenReturn(loadTableResponse);
        Mockito.when(tableExtService.loadDbTables(tableLoadRequest.getDatabases(), "default", true))
                .thenReturn(loadTableResponse);
    }

    @Test
    public void testLoadTables() throws Exception {
        Set<String> loaded = Sets.newHashSet("table1");
        Set<String> failed = Sets.newHashSet("table2");
        Set<String> loading = Sets.newHashSet("table3");
        LoadTableResponse loadTableResponse = new LoadTableResponse();
        loadTableResponse.setLoaded(loaded);
        loadTableResponse.setFailed(failed);
        final TableLoadRequest tableLoadRequest = mockLoadTableRequest();
        initMockito(loadTableResponse, tableLoadRequest);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/tables") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(tableLoadRequest)) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))).andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).loadTables(Mockito.any(TableLoadRequest.class));
    }

    @Test
    public void testLoadTablesException() throws Exception {
        String errorMsg = "You should select at least one table or database to load!!";
        Set<String> loaded = Sets.newHashSet("table1");
        Set<String> failed = Sets.newHashSet("table2");
        Set<String> loading = Sets.newHashSet("table3");
        LoadTableResponse loadTableResponse = new LoadTableResponse();
        loadTableResponse.setLoaded(loaded);
        loadTableResponse.setFailed(failed);
        final TableLoadRequest tableLoadRequest = mockLoadTableRequest();
        tableLoadRequest.setTables(null);
        tableLoadRequest.setDatabases(null);
        Mockito.when(tableExtService.loadDbTables(tableLoadRequest.getTables(), "default", false))
                .thenReturn(loadTableResponse);
        Mockito.when(tableExtService.loadDbTables(tableLoadRequest.getDatabases(), "default", true))
                .thenReturn(loadTableResponse);
        final MvcResult mvcResult = mockMvc.perform(MockMvcRequestBuilders.post("/api/tables") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(tableLoadRequest)) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();
        Mockito.verify(nTableController).loadTables(Mockito.any(TableLoadRequest.class));

        final JsonNode jsonNode = JsonUtil.readValueAsTree(mvcResult.getResponse().getContentAsString());
        Assert.assertTrue(StringUtils.contains(jsonNode.get("exception").textValue(), errorMsg));
    }

    @Test
    public void testLoadAWSTablesCompatibleCrossAccount() throws Exception {
        List<S3TableExtInfo> s3TableExtInfoList = new ArrayList<>();
        S3TableExtInfo s3TableExtInfo = new S3TableExtInfo();
        s3TableExtInfo.setName("DEFAULT.TABLE0");
        s3TableExtInfo.setLocation("s3://bucket1/test1/");
        S3TableExtInfo s3TableExtInfo2 = new S3TableExtInfo();
        s3TableExtInfo2.setName("DEFAULT.TABLE1");
        s3TableExtInfo2.setLocation("s3://bucket2/test2/");
        s3TableExtInfo2.setEndpoint("us-west-2.amazonaws.com");
        s3TableExtInfo2.setRoleArn("test:role1");
        s3TableExtInfoList.add(s3TableExtInfo);
        s3TableExtInfoList.add(s3TableExtInfo2);

        AWSTableLoadRequest tableLoadRequest = new AWSTableLoadRequest();
        tableLoadRequest.setProject("default");
        tableLoadRequest.setDataSourceType(9);
        tableLoadRequest.setTables(s3TableExtInfoList);

        Set<String> loaded = Sets.newHashSet("TABLE0");
        Set<String> failed = Sets.newHashSet("TABLE1");
        LoadTableResponse loadTableResponse = new LoadTableResponse();
        loadTableResponse.setLoaded(loaded);
        loadTableResponse.setFailed(failed);

        Mockito.when(tableExtService.loadAWSTablesCompatibleCrossAccount(tableLoadRequest.getTables(),
                        "default")).thenReturn(loadTableResponse);

        mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/compatibility/aws") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(tableLoadRequest)) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))).andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).loadAWSTablesCompatibleCrossAccount(Mockito.any(AWSTableLoadRequest.class));
    }

    @Test
    public void testUpdateLoadedTableExtProp() throws Exception {
        List<S3TableExtInfo> s3TableExtInfoList = new ArrayList<>();
        S3TableExtInfo s3TableExtInfo = new S3TableExtInfo();
        s3TableExtInfo.setName("DEFAULT.TABLE0");
        s3TableExtInfo.setLocation("s3://bucket11/test11/");
        S3TableExtInfo s3TableExtInfo2 = new S3TableExtInfo();
        s3TableExtInfo2.setName("DEFAULT.TABLE1");
        s3TableExtInfo2.setLocation("s3://bucket22/test22/");
        s3TableExtInfo2.setEndpoint("us-west-2.amazonaws.com");
        s3TableExtInfo2.setRoleArn("test:role2");
        s3TableExtInfoList.add(s3TableExtInfo);
        s3TableExtInfoList.add(s3TableExtInfo2);

        UpdateAWSTableExtDescRequest request = new UpdateAWSTableExtDescRequest();
        request.setProject("default");
        request.setTables(s3TableExtInfoList);

        Set<String> succeed = Sets.newHashSet("TABLE0");
        Set<String> failed = Sets.newHashSet("TABLE1");
        UpdateAWSTableExtDescResponse updateTableExeDescResponse = new UpdateAWSTableExtDescResponse();
        updateTableExeDescResponse.setSucceed(succeed);
        updateTableExeDescResponse.setFailed(failed);

        Mockito.when(tableExtService.updateAWSLoadedTableExtProp(request))
                .thenReturn(updateTableExeDescResponse);

        mockMvc.perform(MockMvcRequestBuilders.put("/api/tables/ext/prop/aws") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(request)) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))).andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).updateLoadedAWSTableExtProp(Mockito.any(UpdateAWSTableExtDescRequest.class));
    }

    @Test
    public void testUnloadTable() throws Exception {
        Mockito.doReturn(false).when(modelService).isModelsUsingTable("DEFAULT.TABLE", "default");
        Mockito.doReturn("DEFAULT.TABLE").when(tableService).unloadTable("default", "DEFAULT.TABLE", false);
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/tables/{database}/{table}", "DEFAULT", "TABLE")
                .param("project", "default").accept(MediaType.parseMediaType(APPLICATION_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).unloadTable("default", "DEFAULT", "TABLE", false);
    }

    @Test
    public void testUnloadTableException() throws Exception {
        Mockito.doReturn(true).when(modelService).isModelsUsingTable("DEFAULT.TABLE", "default");
        Mockito.doReturn("DEFAULT.TABLE").when(tableService).unloadTable("default", "DEFAULT.TABLE", false);
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/tables/{database}/{table}", "DEFAULT", "TABLE")
                .param("project", "default").accept(MediaType.parseMediaType(APPLICATION_JSON)));
        Mockito.verify(nTableController).unloadTable("default", "DEFAULT", "TABLE", false);
    }

    @Test
    public void testGetTablesAndColumns() throws Exception {
        Mockito.doReturn(mockTableAndColumns()).when(tableService).getTableAndColumns("default");
        mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/simple_table") //
                .contentType(MediaType.APPLICATION_JSON) //
                .param("project", "default") //
                .param("pageSize", "10") //
                .param("pageOffset", "0") //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).getTablesAndColomns("default", 0, 10);
    }

    @Test
    public void testGetAutoMergeConfig() throws Exception {
        Mockito.doReturn(null).when(tableService).getAutoMergeConfigByTable("default", "DEFAULT.TEST_KYLIN_FACT");
        mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/auto_merge_config") //
                .contentType(MediaType.APPLICATION_JSON) //
                .param("project", "default") //
                .param("model", "") //
                .param("table", "DEFAULT.TEST_KYLIN_FACT") //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).getAutoMergeConfig("", "DEFAULT.TEST_KYLIN_FACT", "default");
    }

    @Test
    public void testGetAutoMergeConfigException() throws Exception {
        String errorMsg = "model name or table name must be specified!";
        Mockito.doReturn(null).when(tableService).getAutoMergeConfigByModel("default", "");
        final MvcResult mvcResult = mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/auto_merge_config") //
                .contentType(MediaType.APPLICATION_JSON) //
                .param("project", "default") //
                .param("model", "") //
                .param("table", "") //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();
        Mockito.verify(nTableController).getAutoMergeConfig("", "", "default");
        final JsonNode jsonNode = JsonUtil.readValueAsTree(mvcResult.getResponse().getContentAsString());
        Assert.assertTrue(StringUtils.contains(jsonNode.get("exception").textValue(), errorMsg));
    }

    @Test
    public void testGetRefreshDateRange() throws Exception {
        Mockito.doNothing().when(tableService).checkRefreshDataRangeReadiness("default", "DEFAULT.TEST_KYLIN_FACT", "0",
                "100");
        Mockito.doReturn(null).when(modelService).getRefreshAffectedSegmentsResponse("default",
                "DEFAULT.TEST_KYLIN_FACT", "0", "100");
        mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/affected_data_range") //
                .contentType(MediaType.APPLICATION_JSON) //
                .param("project", "default") //
                .param("start", "0") //
                .param("table", "DEFAULT.TEST_KYLIN_FACT") //
                .param("end", "100") //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).getRefreshAffectedDateRange("default", "DEFAULT.TEST_KYLIN_FACT", "0", "100");
    }

    @Test
    public void testGetPushdownMode() throws Exception {
        Mockito.doReturn(true).when(tableService).getPushDownMode("default", "DEFAULT.TEST_KYLIN_FACT");
        mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/pushdown_mode") //
                .contentType(MediaType.APPLICATION_JSON) //
                .param("project", "default") //
                .param("table", "DEFAULT.TEST_KYLIN_FACT") //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).getPushdownMode("default", "DEFAULT.TEST_KYLIN_FACT");
    }

    @Test
    public void testUpdateAutoMergeConfigException() throws Exception {
        String errorMsg = "You should specify at least one autoMerge range!";
        AutoMergeRequest autoMergeRequest = mockAutoMergeRequest();
        autoMergeRequest.setAutoMergeTimeRanges(null);
        final MvcResult mvcResult = mockMvc.perform(MockMvcRequestBuilders.put("/api/tables/auto_merge_config") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(autoMergeRequest)) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();
        Mockito.verify(nTableController).updateAutoMergeConfig(Mockito.any(AutoMergeRequest.class));
        final JsonNode jsonNode = JsonUtil.readValueAsTree(mvcResult.getResponse().getContentAsString());
        Assert.assertTrue(StringUtils.contains(jsonNode.get("exception").textValue(), errorMsg));
    }

    @Test
    public void testUpdateAutoMergeConfigException2() throws Exception {
        String errorMsg = "model name or table name must be specified!";
        AutoMergeRequest autoMergeRequest = mockAutoMergeRequest();
        autoMergeRequest.setModel("");
        autoMergeRequest.setTable("");
        val mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.put("/api/tables/auto_merge_config")
                        .contentType(MediaType.APPLICATION_JSON) //
                        .content(JsonUtil.writeValueAsString(autoMergeRequest)) //
                        .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();
        Mockito.verify(nTableController).updateAutoMergeConfig(Mockito.any(AutoMergeRequest.class));
        final JsonNode jsonNode = JsonUtil.readValueAsTree(mvcResult.getResponse().getContentAsString());
        Assert.assertTrue(StringUtils.contains(jsonNode.get("exception").textValue(), errorMsg));
    }

    @Test
    public void testUpdateAutoMergeConfig() throws Exception {
        AutoMergeRequest autoMergeRequest = mockAutoMergeRequest();
        Mockito.doNothing().when(tableService).setAutoMergeConfigByTable("default", autoMergeRequest);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/tables/auto_merge_config") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(autoMergeRequest)) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).updateAutoMergeConfig(Mockito.any(AutoMergeRequest.class));
    }

    @Test
    public void testUpdatePushdownMode() throws Exception {
        PushDownModeRequest config = new PushDownModeRequest();
        config.setProject("default");
        config.setPushdownRangeLimited(true);
        config.setTable("DEFAULT.TEST_KYLIN_FACT");
        Mockito.doNothing().when(tableService).setPushDownMode("default", "DEFAULT.TEST_KYLIN_FACT", true);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/tables/pushdown_mode") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(config)) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).setPushdownMode(Mockito.any(PushDownModeRequest.class));
    }

    @Test
    public void testGetLoadedDatabases() throws Exception {
        Mockito.doReturn(null).when(tableService).getLoadedDatabases("default");
        mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/loaded_databases") //
                .contentType(MediaType.APPLICATION_JSON) //
                .param("project", "default") //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).getLoadedDatabases("default");
    }

    @Test
    public void testLoadTablesWithSampling() throws Exception {
        Set<String> loaded = Sets.newHashSet("default.test_kylin_fact", "default.test_account");
        Set<String> failed = Sets.newHashSet("default.test_country");
        LoadTableResponse loadTableResponse = new LoadTableResponse();
        loadTableResponse.setLoaded(loaded);
        loadTableResponse.setFailed(failed);
        final TableLoadRequest tableLoadRequest = mockLoadTableRequest();
        tableLoadRequest.setNeedSampling(true);
        tableLoadRequest.setSamplingRows(20000);
        initMockito(loadTableResponse, tableLoadRequest);
        Assert.assertNotNull(tableSamplingService);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/tables") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(tableLoadRequest)) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nTableController).loadTables(Mockito.any(TableLoadRequest.class));
    }

    @Test
    public void testLoadTablesExceptionForSamplingRowsTooSmall() throws Exception {
        Set<String> loaded = Sets.newHashSet("default.test_kylin_fact");
        LoadTableResponse loadTableResponse = new LoadTableResponse();
        loadTableResponse.setLoaded(loaded);
        final TableLoadRequest tableLoadRequest = mockLoadTableRequest();
        tableLoadRequest.setNeedSampling(true);
        tableLoadRequest.setSamplingRows(200);

        initMockito(loadTableResponse, tableLoadRequest);
        final MvcResult mvcResult = mockMvc.perform(MockMvcRequestBuilders.post("/api/tables") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(tableLoadRequest)) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();
        Mockito.verify(nTableController).loadTables(Mockito.any(TableLoadRequest.class));
        final JsonNode jsonNode = JsonUtil.readValueAsTree(mvcResult.getResponse().getContentAsString());
        Assert.assertTrue(StringUtils.contains(jsonNode.get("exception").textValue(),
                JOB_SAMPLING_RANGE_INVALID.getMsg(MIN_SAMPLING_ROWS, MAX_SAMPLING_ROWS)));
    }

    @Test
    public void testLoadTablesExceptionForSamplingRowsTooLarge() throws Exception {
        Set<String> loaded = Sets.newHashSet("default.test_kylin_fact");
        LoadTableResponse loadTableResponse = new LoadTableResponse();
        loadTableResponse.setLoaded(loaded);
        final TableLoadRequest tableLoadRequest = mockLoadTableRequest();
        tableLoadRequest.setNeedSampling(true);
        tableLoadRequest.setSamplingRows(30_000_000);

        initMockito(loadTableResponse, tableLoadRequest);
        final MvcResult mvcResult = mockMvc.perform(MockMvcRequestBuilders.post("/api/tables") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(tableLoadRequest)) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();

        Mockito.verify(nTableController).loadTables(Mockito.any(TableLoadRequest.class));
        final JsonNode jsonNode = JsonUtil.readValueAsTree(mvcResult.getResponse().getContentAsString());
        Assert.assertTrue(StringUtils.contains(jsonNode.get("exception").textValue(),
                JOB_SAMPLING_RANGE_INVALID.getMsg(MIN_SAMPLING_ROWS, MAX_SAMPLING_ROWS)));
    }

    private List<TablesAndColumnsResponse> mockTableAndColumns() {
        List<TablesAndColumnsResponse> result = new ArrayList<>();
        result.add(new TablesAndColumnsResponse());
        return result;
    }

    private AutoMergeRequest mockAutoMergeRequest() {
        AutoMergeRequest autoMergeRequest = new AutoMergeRequest();
        autoMergeRequest.setProject("default");
        autoMergeRequest.setTable("DEFAULT.TEST_KYLIN_FACT");
        autoMergeRequest.setAutoMergeEnabled(true);
        autoMergeRequest.setAutoMergeTimeRanges(new String[] { "MINUTE" });
        autoMergeRequest.setVolatileRangeEnabled(true);
        autoMergeRequest.setVolatileRangeNumber(7);
        autoMergeRequest.setVolatileRangeType("MINUTE");
        return autoMergeRequest;
    }

    private List<TableDesc> mockTables() {
        final List<TableDesc> tableDescs = new ArrayList<>();
        TableDesc tableDesc = new TableDesc();
        tableDesc.setName("table1");
        tableDescs.add(tableDesc);
        TableDesc tableDesc2 = new TableDesc();
        tableDesc2.setName("table2");
        tableDescs.add(tableDesc2);
        return tableDescs;
    }

    @Test
    public void testReloadHiveTablename() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/reload_hive_table_name") //
                .contentType(MediaType.APPLICATION_JSON) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).reloadHiveTablename("", false);
    }

    @Test
    public void testRefreshSingleCatalogCache() throws Exception {
        List<String> tables = Lists.newArrayList();
        tables.add("DEFAULT.TEST_KYLIN_FACT");
        HashMap request = new HashMap();
        request.put("tables", tables);
        TableRefresh tableRefresh = new TableRefresh();
        tableRefresh.setCode(KylinException.CODE_SUCCESS);
        Mockito.doReturn(tableRefresh).when(tableService).refreshSingleCatalogCache(Mockito.any());

        mockMvc.perform(MockMvcRequestBuilders.put("/api/tables/single_catalog_cache")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(APPLICATION_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }

    @Test
    public void testRefreshSingleCatalogCacheError() throws Exception {
        HashMap request = new HashMap();
        request.put("tables", "DEFAULT.TEST_KYLIN_FACT");
        String errorMsg = "The “table“ parameter is invalid. Please check and try again.";
        MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.put("/api/tables/single_catalog_cache")
                        .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType(APPLICATION_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();
        JsonNode jsonNode = JsonUtil.readValueAsTree(mvcResult.getResponse().getContentAsString());
        Assert.assertTrue(StringUtils.contains(jsonNode.get("exception").textValue(), errorMsg));
    }

    @Test
    public void testGetModelTables() throws Exception {
        String project = "default";
        String modelName = "model_name";

        List<TableDesc> tableDescs = Lists.newArrayList();
        tableDescs.add(Mockito.mock(TableDesc.class));

        Mockito.doReturn(tableDescs).when(tableService).getTablesOfModel(project, modelName);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/model_tables").param("project", project)
                .param("model_name", modelName).contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nTableController).getModelTables(project, modelName);
    }

    @Test
    public void testReloadTable() throws Exception {
        Mockito.doAnswer(x -> null).when(tableService).reloadTable("default", "a", false, 100000, false);
        ReloadTableRequest request = new ReloadTableRequest();
        request.setProject("default");
        request.setMaxRows(100000);
        request.setTable("a");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/reload").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nTableController).reloadTable(Mockito.any());
    }
}

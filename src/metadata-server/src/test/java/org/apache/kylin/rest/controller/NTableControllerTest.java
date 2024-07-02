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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.StringHelper;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.AWSTableLoadRequest;
import org.apache.kylin.rest.request.AutoMergeRequest;
import org.apache.kylin.rest.request.PartitionKeyRequest;
import org.apache.kylin.rest.request.ReloadTableRequest;
import org.apache.kylin.rest.request.S3TableExtInfo;
import org.apache.kylin.rest.request.TableDescRequest;
import org.apache.kylin.rest.request.TableExclusionRequest;
import org.apache.kylin.rest.request.TableLoadRequest;
import org.apache.kylin.rest.request.TopTableRequest;
import org.apache.kylin.rest.request.UpdateAWSTableExtDescRequest;
import org.apache.kylin.rest.response.ExcludedTableDetailResponse;
import org.apache.kylin.rest.response.LoadTableResponse;
import org.apache.kylin.rest.response.TableNameResponse;
import org.apache.kylin.rest.response.TableRefreshAll;
import org.apache.kylin.rest.response.TablesAndColumnsResponse;
import org.apache.kylin.rest.response.UpdateAWSTableExtDescResponse;
import org.apache.kylin.rest.service.ModelService;
import org.apache.kylin.rest.service.TableExtService;
import org.apache.kylin.rest.service.TableSampleService;
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
    private TableSampleService tableSampleService;

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
        TableDescRequest mockTableDescRequest = new TableDescRequest("default", "", "DEFAULT", false, true,
                Pair.newPair(0, 10), Collections.singletonList(9));

        Mockito.when(tableService.getTableDesc(mockTableDescRequest, 10)).thenReturn(Pair.newPair(mockTables(), 10));

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
        TableDescRequest mockTableDescRequest = new TableDescRequest("default", "TEST_KYLIN_FACT", "DEFAULT", false,
                false, Pair.newPair(0, 10), Collections.singletonList(9));

        Mockito.when(tableService.getTableDesc(mockTableDescRequest, 10)).thenReturn(Pair.newPair(mockTables(), 10));

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

    private void initMockito(LoadTableResponse loadTableResponse, TableLoadRequest tableLoadRequest) throws Exception {
        StringHelper.toUpperCaseArray(tableLoadRequest.getTables(), tableLoadRequest.getTables());
        StringHelper.toUpperCaseArray(tableLoadRequest.getDatabases(), tableLoadRequest.getDatabases());
        Mockito.when(tableExtService.loadDbTables(tableLoadRequest.getTables(), "default", false))
                .thenReturn(loadTableResponse);
        Mockito.when(tableExtService.loadDbTables(tableLoadRequest.getDatabases(), "default", true))
                .thenReturn(loadTableResponse);
        Mockito.when(tableExtService.loadTablesWithShortCircuit(tableLoadRequest)).thenReturn(loadTableResponse);
    }

    @Test
    public void testLoadTables() throws Exception {
        Set<String> loaded = Sets.newHashSet("table1");
        Set<String> failed = Sets.newHashSet("table2");
        LoadTableResponse loadTableResponse = new LoadTableResponse();
        loadTableResponse.setLoaded(loaded);
        loadTableResponse.setFailed(failed);

        {
            final TableLoadRequest tableLoadRequest = mockLoadTableRequest();
            initMockito(loadTableResponse, tableLoadRequest);
            tableLoadRequest.setNeedSampling(false);
            tableLoadRequest.setSamplingRows(0);
            mockMvc.perform(MockMvcRequestBuilders.post("/api/tables") //
                    .contentType(MediaType.APPLICATION_JSON) //
                    .content(JsonUtil.writeValueAsString(tableLoadRequest)) //
                    .accept(MediaType.parseMediaType(APPLICATION_JSON)))
                    .andExpect(MockMvcResultMatchers.status().isOk());
            Mockito.verify(nTableController).loadTables(Mockito.any(TableLoadRequest.class));
        }

        {
            // test case-insensitive
            String[] databasesMixTure = new String[] { "SSb", "DeFauLT" };
            String[] databasesLowercase = new String[] { "ssb", "default" };
            String[] databasesUppercase = new String[] { "SSB", "DEFAULT" };
            String[] tablesMixTure = new String[] { "PERson", "Order" };
            String[] tablesLowercase = new String[] { "person", "order" };
            String[] tablesUppercase = new String[] { "PERSON", "ORDER" };
            String project = "default";
            TableLoadRequest request = new TableLoadRequest();
            request.setDatabases(databasesUppercase);
            request.setTables(tablesUppercase);
            request.setProject(project);
            initMockito(loadTableResponse, request);
            request.setNeedSampling(false);
            request.setSamplingRows(0);

            request.setDatabases(databasesMixTure);
            request.setTables(tablesMixTure);
            mockMvc.perform(MockMvcRequestBuilders.post("/api/tables") //
                    .contentType(MediaType.APPLICATION_JSON) //
                    .content(JsonUtil.writeValueAsString(request)) //
                    .accept(MediaType.parseMediaType(APPLICATION_JSON)))
                    .andExpect(MockMvcResultMatchers.status().isOk());
            Mockito.verify(tableExtService, Mockito.times(1)).loadTablesWithShortCircuit(request);

            request.setDatabases(databasesLowercase);
            request.setTables(tablesLowercase);
            mockMvc.perform(MockMvcRequestBuilders.post("/api/tables") //
                    .contentType(MediaType.APPLICATION_JSON) //
                    .content(JsonUtil.writeValueAsString(request)) //
                    .accept(MediaType.parseMediaType(APPLICATION_JSON)))
                    .andExpect(MockMvcResultMatchers.status().isOk());
            Mockito.verify(tableExtService, Mockito.times(1)).loadTablesWithShortCircuit(request);

            request.setDatabases(databasesUppercase);
            request.setTables(tablesUppercase);
            mockMvc.perform(MockMvcRequestBuilders.post("/api/tables") //
                    .contentType(MediaType.APPLICATION_JSON) //
                    .content(JsonUtil.writeValueAsString(request)) //
                    .accept(MediaType.parseMediaType(APPLICATION_JSON)))
                    .andExpect(MockMvcResultMatchers.status().isOk());
            Mockito.verify(tableExtService, Mockito.times(1)).loadTablesWithShortCircuit(request);
        }
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

        Mockito.when(tableExtService.loadAWSTablesCompatibleCrossAccount(tableLoadRequest.getTables(), "default"))
                .thenReturn(loadTableResponse);

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

        Mockito.when(tableExtService.updateAWSLoadedTableExtProp(request)).thenReturn(updateTableExeDescResponse);

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
        Mockito.doReturn(null).when(tableService).getAutoMergeConfigByModel("default", "model_uuid");
        mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/auto_merge_config") //
                .contentType(MediaType.APPLICATION_JSON) //
                .param("project", "default") //
                .param("model", "model_uuid") //
                .param("table", "") //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).getAutoMergeConfig("model_uuid", "", "default");
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
        Mockito.doNothing().when(tableService).setAutoMergeConfigByModel("default", autoMergeRequest);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/tables/auto_merge_config") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(autoMergeRequest)) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).updateAutoMergeConfig(Mockito.any(AutoMergeRequest.class));
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
        loadTableResponse.setNeedRealSampling(loaded);
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
        loadTableResponse.setNeedRealSampling(loaded);
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
    public void testRefreshCatalogCache() throws Exception {
        List<String> tables = Lists.newArrayList();
        tables.add("DEFAULT.TEST_KYLIN_FACT");
        HashMap request = new HashMap();
        request.put("tables", tables);
        TableRefreshAll tableRefreshAll = new TableRefreshAll();
        tableRefreshAll.setCode(KylinException.CODE_SUCCESS);
        Mockito.doReturn(tableRefreshAll).when(tableService).refreshAllCatalogCache(Mockito.any());

        mockMvc.perform(MockMvcRequestBuilders.put("/api/tables/catalog_cache")
                        .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType(APPLICATION_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
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

    @Test
    public void updateExcludedTables() throws Exception {
        TableExclusionRequest request = new TableExclusionRequest();
        request.setProject("default");
        TableExclusionRequest.ExcludedTable excludedTable = new TableExclusionRequest.ExcludedTable();
        excludedTable.setExcluded(true);
        excludedTable.setTable("default.test_order");
        request.setExcludedTables(Lists.newArrayList(excludedTable));
        List<String> cancelTables = Lists.newArrayList("default.test_account");
        request.setCanceledTables(cancelTables);

        Mockito.doNothing().when(tableExtService).updateExcludedTables("default", request);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/tables/excluded_tables")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nTableController).updateExcludedTables(request);
    }

    @Test
    public void testGetExcludedTable() throws Exception {
        Mockito.when(tableExtService.getExcludedTable("default", "test_kylin_fact", 0, 10, "", true)) //
                .thenReturn(Mockito.mock(ExcludedTableDetailResponse.class));
        mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/excluded_table") //
                .contentType(MediaType.APPLICATION_JSON) //
                .param("project", "default") //
                .param("table", "test_kylin_fact") //
                .param("page_offset", "0") //
                .param("page_size", "10") //
                .param("key", "") //
                .param("col_type", "0") //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nTableController).getExcludedTable("default", "test_kylin_fact", 0, 10, "", 0);
    }

    @Test
    public void testGetExcludedTables() throws Exception {
        Mockito.when(tableExtService.getExcludedTables("default", true, "")) //
                .thenReturn(Lists.newArrayList());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/excluded_tables") //
                .contentType(MediaType.APPLICATION_JSON) //
                .param("project", "default") //
                .param("page_offset", "0") //
                .param("page_size", "10") //
                .param("view_partial_cols", "true") //
                .param("search_key", "") //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nTableController).getExcludedTables("default", 0, 10, true, "");
    }
}

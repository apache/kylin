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
package org.apache.kylin.rest.controller.open;

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.controller.NTableController;
import org.apache.kylin.rest.request.AWSTableLoadRequest;
import org.apache.kylin.rest.request.OpenReloadTableRequest;
import org.apache.kylin.rest.request.S3TableExtInfo;
import org.apache.kylin.rest.request.TableLoadRequest;
import org.apache.kylin.rest.request.UpdateAWSTableExtDescRequest;
import org.apache.kylin.rest.response.PreUnloadTableResponse;
import org.apache.kylin.rest.service.ModelService;
import org.apache.kylin.rest.service.ProjectService;
import org.apache.kylin.rest.service.TableService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.junit.After;
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

import lombok.val;

public class OpenTableControllerTest extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;

    @Mock
    private NTableController nTableController;

    @Mock
    private AclEvaluate aclEvaluate;

    @Mock
    private ProjectService projectService;

    @Mock
    private TableService tableService;

    @Mock
    private ModelService modelService;

    @InjectMocks
    private OpenTableController openTableController = Mockito.spy(new OpenTableController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(openTableController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .defaultResponseCharacterEncoding(StandardCharsets.UTF_8).build();

        SecurityContextHolder.getContext().setAuthentication(authentication);

        ProjectInstance projectInstance = new ProjectInstance();
        projectInstance.setName("default");
        Mockito.doReturn(Lists.newArrayList(projectInstance)).when(projectService)
                .getReadableProjects(projectInstance.getName(), true);
        Mockito.doReturn(true).when(aclEvaluate).hasProjectWritePermission(Mockito.any());

        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    private void mockGetTable(String project, String tableName) {
        TableDesc tableDesc = new TableDesc();
        Mockito.doReturn(tableDesc).when(openTableController).getTable(project, tableName);
    }

    @Test
    public void testGetTable() throws Exception {
        {
            String project = "default";
            String tableName = "TEST_KYLIN_FACT";
            String database = "DEFAULT";

            Mockito.when(tableService.getTableDesc(project, true, tableName, database, false,
                    Collections.singletonList(9), 10))
                    .thenReturn(Pair.newPair(Collections.singletonList(new TableDesc()), 10));

            mockMvc.perform(MockMvcRequestBuilders.get("/api/tables") //
                    .contentType(MediaType.APPLICATION_JSON) //
                    .param("project", project).param("table", tableName).param("database", database)
                    .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                    .andExpect(MockMvcResultMatchers.status().isOk());
            Mockito.verify(openTableController).getTableDesc(project, tableName, database, false, true, 0, 10, 9);
        }

        {
            // call failed  when table is kafka table
            String project = "streaming_test";
            String tableName = "P_LINEORDER_STR";
            String database = "SSB";

            Mockito.when(tableService.getTableDesc(project, true, tableName, database, false,
                    Collections.singletonList(1), 10))
                    .thenReturn(Pair.newPair(Collections.singletonList(new TableDesc()), 10));

            mockMvc.perform(MockMvcRequestBuilders.get("/api/tables") //
                    .contentType(MediaType.APPLICATION_JSON) //
                    .param("project", project).param("table", tableName).param("database", database)
                    .param("source_type", "1").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                    .andExpect(MockMvcResultMatchers.status().isInternalServerError());
            Mockito.verify(openTableController).getTableDesc(project, tableName, database, false, true, 0, 10, 1);
        }

        {
            // test case-insensitive
            String project = "default";
            String tableNameMixture = "TEsT_KYliN";
            String tableNameLowerCase = "test_kylin";
            String tableNameUppercase = "TEST_KYLIN";
            String databaseMixture = "Ssb";
            String databaseLowercase = "ssb";
            String databaseUppercase = "SSB";

            Mockito.when(tableService.getTableDesc(project, true, tableNameUppercase, databaseUppercase, false,
                    Collections.singletonList(9), 10))
                    .thenReturn(Pair.newPair(Collections.singletonList(new TableDesc()), 10));

            mockMvc.perform(MockMvcRequestBuilders.get("/api/tables") //
                    .contentType(MediaType.APPLICATION_JSON) //
                    .param("project", project).param("table", tableNameMixture).param("database", databaseMixture)
                    .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                    .andExpect(MockMvcResultMatchers.status().isOk());
            Mockito.verify(tableService, Mockito.times(1)).getTableDesc(project, true, tableNameUppercase,
                    databaseUppercase, false, Collections.singletonList(9), 10);

            mockMvc.perform(MockMvcRequestBuilders.get("/api/tables") //
                    .contentType(MediaType.APPLICATION_JSON) //
                    .param("project", project).param("table", tableNameLowerCase).param("database", databaseLowercase)
                    .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                    .andExpect(MockMvcResultMatchers.status().isOk());
            Mockito.verify(tableService, Mockito.times(2)).getTableDesc(project, true, tableNameUppercase,
                    databaseUppercase, false, Collections.singletonList(9), 10);

            mockMvc.perform(MockMvcRequestBuilders.get("/api/tables") //
                    .contentType(MediaType.APPLICATION_JSON) //
                    .param("project", project).param("table", tableNameUppercase).param("database", databaseUppercase)
                    .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                    .andExpect(MockMvcResultMatchers.status().isOk());
            Mockito.verify(tableService, Mockito.times(3)).getTableDesc(project, true, tableNameUppercase,
                    databaseUppercase, false, Collections.singletonList(9), 10);
        }
    }

    @Test
    public void testLoadTables() throws Exception {
        TableLoadRequest tableLoadRequest = new TableLoadRequest();
        tableLoadRequest.setDatabases(new String[] { "kk" });
        tableLoadRequest.setTables(new String[] { "hh.kk" });
        tableLoadRequest.setNeedSampling(false);
        tableLoadRequest.setProject("default");
        tableLoadRequest.setSamplingRows(0);
        Mockito.doNothing().when(openTableController).updateDataSourceType("default", 9);
        Mockito.doAnswer(x -> null).when(nTableController).loadTables(tableLoadRequest);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/tables") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(tableLoadRequest)) //
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openTableController).loadTables(tableLoadRequest);

        tableLoadRequest.setNeedSampling(true);
        tableLoadRequest.setSamplingRows(10_000);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/tables") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(tableLoadRequest)) //
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openTableController).loadTables(tableLoadRequest);

        tableLoadRequest.setNeedSampling(true);
        tableLoadRequest.setSamplingRows(1_000);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/tables") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(tableLoadRequest)) //
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());
        Mockito.verify(openTableController).loadTables(tableLoadRequest);

        val mapRequest = mockStreamingTablesRequestMap();
        mockMvc.perform(MockMvcRequestBuilders.post("/api/tables") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(mapRequest)) //
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                .andExpect(MockMvcResultMatchers.status().is5xxServerError());
    }

    @Test
    public void testLoadAWSTablesCompatibleCrossAccount() throws Exception {
        AWSTableLoadRequest tableLoadRequest = new AWSTableLoadRequest();
        List<S3TableExtInfo> tableExtInfoList = new ArrayList<>();
        S3TableExtInfo s3TableExtInfo1 = new S3TableExtInfo();
        s3TableExtInfo1.setName("DEFAULT.TABLE0");
        s3TableExtInfo1.setLocation("s3://bucket1/test1/");
        S3TableExtInfo s3TableExtInfo2 = new S3TableExtInfo();
        s3TableExtInfo2.setName("DEFAULT.TABLE1");
        s3TableExtInfo2.setLocation("s3://bucket2/test2/");
        s3TableExtInfo2.setEndpoint("us-west-2.amazonaws.com");
        s3TableExtInfo2.setRoleArn("test:role");
        tableExtInfoList.add(s3TableExtInfo1);
        tableExtInfoList.add(s3TableExtInfo2);
        tableLoadRequest.setTables(tableExtInfoList);
        tableLoadRequest.setNeedSampling(false);
        tableLoadRequest.setProject("default");
        Mockito.doNothing().when(openTableController).updateDataSourceType("default", 9);
        Mockito.doAnswer(x -> null).when(nTableController).loadAWSTablesCompatibleCrossAccount(tableLoadRequest);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/compatibility/aws") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(tableLoadRequest)) //
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openTableController).loadAWSTablesCompatibleCrossAccount(tableLoadRequest);

        tableLoadRequest.setNeedSampling(true);
        tableLoadRequest.setSamplingRows(10000);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/compatibility/aws") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(tableLoadRequest)) //
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openTableController).loadAWSTablesCompatibleCrossAccount(tableLoadRequest);

        tableLoadRequest.setNeedSampling(true);
        tableLoadRequest.setSamplingRows(1000);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/compatibility/aws") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(tableLoadRequest)) //
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());
        Mockito.verify(openTableController).loadAWSTablesCompatibleCrossAccount(tableLoadRequest);

    }

    @Test
    public void testUpdateLoadedAWSTableExtProp() throws Exception {
        List<S3TableExtInfo> tableExtInfoList = new ArrayList<>();
        S3TableExtInfo s3TableExtInfo1 = new S3TableExtInfo();
        s3TableExtInfo1.setName("DEFAULT.TABLE0");
        s3TableExtInfo1.setLocation("s3://bucket1/test1/");
        S3TableExtInfo s3TableExtInfo2 = new S3TableExtInfo();
        s3TableExtInfo2.setName("DEFAULT.TABLE1");
        s3TableExtInfo2.setLocation("s3://bucket2/test1/");
        s3TableExtInfo2.setEndpoint("us-west-2.amazonaws.com");
        s3TableExtInfo2.setRoleArn("testrole");
        tableExtInfoList.add(s3TableExtInfo1);
        tableExtInfoList.add(s3TableExtInfo2);

        UpdateAWSTableExtDescRequest request = new UpdateAWSTableExtDescRequest();
        request.setProject("default");
        request.setTables(tableExtInfoList);

        mockMvc.perform(MockMvcRequestBuilders.put("/api/tables/ext/prop/aws") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(request)) //
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openTableController)
                .updateLoadedAWSTableExtProp(Mockito.any(UpdateAWSTableExtDescRequest.class));
    }

    @Test
    public void testPreReloadTable() throws Exception {
        {
            String project = "default";
            String tableName = "TEST_KYLIN_FACT";

            mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/pre_reload") //
                    .contentType(MediaType.APPLICATION_JSON) //
                    .param("project", project).param("table", tableName)
                    .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                    .andExpect(MockMvcResultMatchers.status().isOk());
            Mockito.verify(openTableController).preReloadTable(project, tableName, false);
        }

        {
            // call failed  when table is kafka table
            String project = "streaming_test";
            String tableName = "SSB.P_LINEORDER";

            mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/pre_reload") //
                    .contentType(MediaType.APPLICATION_JSON) //
                    .param("project", project).param("table", tableName)
                    .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                    .andExpect(MockMvcResultMatchers.status().isInternalServerError());
            Mockito.verify(openTableController).preReloadTable(project, tableName, false);
        }

        {
            // test case-insensitive
            String project = "default";
            String tableMixture = "SsB.P_LINEorDER";
            String tableLowercase = "ssb.p_lineorder";
            String tableUppercase = "SSB.P_LINEORDER";

            mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/pre_reload") //
                    .contentType(MediaType.APPLICATION_JSON) //
                    .param("project", project).param("table", tableMixture)
                    .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                    .andExpect(MockMvcResultMatchers.status().isOk());
            Mockito.verify(tableService, Mockito.times(1)).preProcessBeforeReloadWithoutFailFast(project,
                    tableUppercase, false);

            mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/pre_reload") //
                    .contentType(MediaType.APPLICATION_JSON) //
                    .param("project", project).param("table", tableLowercase)
                    .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                    .andExpect(MockMvcResultMatchers.status().isOk());
            Mockito.verify(tableService, Mockito.times(2)).preProcessBeforeReloadWithoutFailFast(project,
                    tableUppercase, false);

            mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/pre_reload") //
                    .contentType(MediaType.APPLICATION_JSON) //
                    .param("project", project).param("table", tableUppercase)
                    .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                    .andExpect(MockMvcResultMatchers.status().isOk());
            Mockito.verify(tableService, Mockito.times(3)).preProcessBeforeReloadWithoutFailFast(project,
                    tableUppercase, false);
        }
    }

    @Test
    public void testPreReloadTableNeedDetail() throws Exception {
        String project = "default";
        String tableName = "TEST_KYLIN_FACT";

        mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/pre_reload") //
                .contentType(MediaType.APPLICATION_JSON) //
                .param("project", project).param("table", tableName).param("need_details", "true")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openTableController).preReloadTable(project, tableName, true);
    }

    @Test
    public void testReloadTable() throws Exception {
        {
            String project = "default";
            String tableName = "TEST_KYLIN_FACT";

            OpenReloadTableRequest request = new OpenReloadTableRequest();
            request.setProject(project);
            request.setTable(tableName);
            request.setNeedSampling(false);

            Mockito.doReturn(new Pair<String, List<String>>()).when(tableService).reloadTable(request.getProject(),
                    request.getTable(), request.getNeedSampling(), 0, false, ExecutablePO.DEFAULT_PRIORITY, null);
            mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/reload") //
                    .contentType(MediaType.APPLICATION_JSON) //
                    .content(JsonUtil.writeValueAsString(request)) //
                    .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                    .andExpect(MockMvcResultMatchers.status().isOk());
            Mockito.verify(openTableController).reloadTable(request);
            Mockito.reset(tableService);
        }

        {
            // test request without need_sampling
            String project = "default";
            String tableName = "TEST_KYLIN_FACT";

            OpenReloadTableRequest request = new OpenReloadTableRequest();
            request.setProject(project);
            request.setTable(tableName);

            mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/reload") //
                    .contentType(MediaType.APPLICATION_JSON) //
                    .content(JsonUtil.writeValueAsString(request)) //
                    .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                    .andExpect(MockMvcResultMatchers.status().isInternalServerError());
            Mockito.verify(openTableController).reloadTable(request);
        }

        {
            // test request without need_sampling
            OpenReloadTableRequest request = new OpenReloadTableRequest();
            request.setProject("streaming_test");
            request.setTable("SSB.P_LINEORDER");

            mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/reload") //
                    .contentType(MediaType.APPLICATION_JSON) //
                    .content(JsonUtil.writeValueAsString(request)) //
                    .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                    .andExpect(MockMvcResultMatchers.status().isInternalServerError());
            Mockito.verify(openTableController).reloadTable(request);
        }

        {
            // test case-insensitive
            String project = "default";
            String tableNameMixture = "TEst_KYliN_FacT";
            String tableNameLowercase = "test_kylin_fact";
            String tableNameUppercase = "TEST_KYLIN_FACT";

            OpenReloadTableRequest request = new OpenReloadTableRequest();
            request.setProject(project);
            request.setNeedSampling(false);

            request.setTable(tableNameMixture);
            mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/reload") //
                    .contentType(MediaType.APPLICATION_JSON) //
                    .content(JsonUtil.writeValueAsString(request)) //
                    .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                    .andExpect(MockMvcResultMatchers.status().is5xxServerError());
            Mockito.verify(tableService, Mockito.times(1)).reloadTable(request.getProject(), tableNameUppercase,
                    request.getNeedSampling(), request.getSamplingRows(), request.getNeedBuilding(),
                    request.getPriority(), request.getYarnQueue());

            request.setTable(tableNameLowercase);
            mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/reload") //
                    .contentType(MediaType.APPLICATION_JSON) //
                    .content(JsonUtil.writeValueAsString(request)) //
                    .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                    .andExpect(MockMvcResultMatchers.status().is5xxServerError());
            Mockito.verify(tableService, Mockito.times(2)).reloadTable(request.getProject(), tableNameUppercase,
                    request.getNeedSampling(), request.getSamplingRows(), request.getNeedBuilding(),
                    request.getPriority(), request.getYarnQueue());

            request.setTable(tableNameUppercase);
            mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/reload") //
                    .contentType(MediaType.APPLICATION_JSON) //
                    .content(JsonUtil.writeValueAsString(request)) //
                    .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                    .andExpect(MockMvcResultMatchers.status().is5xxServerError());
            Mockito.verify(tableService, Mockito.times(3)).reloadTable(request.getProject(), tableNameUppercase,
                    request.getNeedSampling(), request.getSamplingRows(), request.getNeedBuilding(),
                    request.getPriority(), request.getYarnQueue());
        }
    }

    @Test
    public void testReloadAWSTablesCompatibleCrossAccount() throws Exception {
        String project = "default";
        S3TableExtInfo s3TableExtInfo = new S3TableExtInfo();
        s3TableExtInfo.setName("DEFAULT.TABLE1");
        s3TableExtInfo.setLocation("s3://bucket1/test1/");
        s3TableExtInfo.setEndpoint("us-west-2.amazonaws.com");
        s3TableExtInfo.setRoleArn("test:role");

        OpenReloadTableRequest request = new OpenReloadTableRequest();
        request.setProject(project);
        request.setNeedSampling(false);
        request.setS3TableExtInfo(s3TableExtInfo);

        Mockito.doReturn(new Pair<String, List<String>>()).when(tableService).reloadAWSTableCompatibleCrossAccount(
                request.getProject(), request.getS3TableExtInfo(), request.getNeedSampling(), 0, false,
                ExecutablePO.DEFAULT_PRIORITY, null);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/reload/compatibility/aws") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(request)) //
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openTableController).reloadAWSTablesCompatibleCrossAccount(request);

        // test request with need_sampling
        OpenReloadTableRequest request2 = new OpenReloadTableRequest();
        request2.setProject(project);
        request2.setS3TableExtInfo(s3TableExtInfo);
        request2.setNeedSampling(true);
        request2.setSamplingRows(10000);
        Mockito.doReturn(new Pair<String, List<String>>()).when(tableService).reloadAWSTableCompatibleCrossAccount(
                request2.getProject(), request2.getS3TableExtInfo(), request2.getNeedSampling(),
                request2.getSamplingRows(), false, ExecutablePO.DEFAULT_PRIORITY, null);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/reload/compatibility/aws") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(request2)) //
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openTableController).reloadAWSTablesCompatibleCrossAccount(request2);
    }

    @Test
    public void testPrepareUnloadTable() throws Exception {
        {
            Mockito.doReturn(new PreUnloadTableResponse()).when(tableService).preUnloadTable("default",
                    "DEFAULT.TABLE");
            mockMvc.perform(MockMvcRequestBuilders
                    .get("/api/tables/{database}/{table}/prepare_unload", "DEFAULT", "TABLE")
                    .param("project", "default").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                    .andExpect(MockMvcResultMatchers.status().isOk());
            Mockito.verify(openTableController).prepareUnloadTable("default", "DEFAULT", "TABLE");
            Mockito.reset(tableService);
        }

        {
            // test case-insensitive
            String tableNameMixture = "TEsT_KYliNFAct";
            String tableNameLowerCase = "test_kylinfact";
            String tableNameUppercase = "TEST_KYLINFACT";
            String databaseMixture = "Ssb";
            String databaseLowercase = "ssb";
            String databaseUppercase = "SSB";

            mockMvc.perform(MockMvcRequestBuilders
                    .get("/api/tables/{database}/{table}/prepare_unload", databaseMixture, tableNameMixture)
                    .param("project", "default").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                    .andExpect(MockMvcResultMatchers.status().isOk());
            Mockito.verify(tableService, Mockito.times(1)).preUnloadTable("default",
                    databaseUppercase + "." + tableNameUppercase);

            mockMvc.perform(MockMvcRequestBuilders
                    .get("/api/tables/{database}/{table}/prepare_unload", databaseLowercase, tableNameLowerCase)
                    .param("project", "default").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                    .andExpect(MockMvcResultMatchers.status().isOk());
            Mockito.verify(tableService, Mockito.times(2)).preUnloadTable("default",
                    databaseUppercase + "." + tableNameUppercase);

            mockMvc.perform(MockMvcRequestBuilders
                    .get("/api/tables/{database}/{table}/prepare_unload", databaseUppercase, tableNameUppercase)
                    .param("project", "default").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                    .andExpect(MockMvcResultMatchers.status().isOk());
            Mockito.verify(tableService, Mockito.times(3)).preUnloadTable("default",
                    databaseUppercase + "." + tableNameUppercase);
        }
    }

    @Test
    public void testUnloadTable() throws Exception {
        {
            Mockito.doReturn(false).when(modelService).isModelsUsingTable("DEFAULT.TABLE", "default");
            Mockito.doReturn("DEFAULT.TABLE").when(tableService).unloadTable("default", "DEFAULT.TABLE", false);
            mockMvc.perform(MockMvcRequestBuilders.delete("/api/tables/{database}/{table}", "DEFAULT", "TABLE")
                    .param("project", "default").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                    .andExpect(MockMvcResultMatchers.status().isOk());
            Mockito.verify(openTableController).unloadTable("default", "DEFAULT", "TABLE", false);
        }

        {
            // test case-insensitive
            String tableNameMixture = "TEsT_KYliN";
            String tableNameLowerCase = "test_kylin";
            String tableNameUppercase = "TEST_KYLIN";
            String databaseMixture = "Ssb";
            String databaseLowercase = "ssb";
            String databaseUppercase = "SSB";

            mockMvc.perform(MockMvcRequestBuilders
                    .delete("/api/tables/{database}/{table}", databaseMixture, tableNameMixture)
                    .param("project", "default").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                    .andExpect(MockMvcResultMatchers.status().isOk());
            Mockito.verify(tableService, Mockito.times(1)).unloadTable("default",
                    databaseUppercase + "." + tableNameUppercase, false);

            mockMvc.perform(MockMvcRequestBuilders
                    .delete("/api/tables/{database}/{table}", databaseLowercase, tableNameLowerCase)
                    .param("project", "default").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                    .andExpect(MockMvcResultMatchers.status().isOk());
            Mockito.verify(tableService, Mockito.times(2)).unloadTable("default",
                    databaseUppercase + "." + tableNameUppercase, false);
        }
    }

    @Test
    public void testUnloadTableException() throws Exception {
        Mockito.doReturn(true).when(modelService).isModelsUsingTable("DEFAULT.TABLE", "default");
        Mockito.doReturn("DEFAULT.TABLE").when(tableService).unloadTable("default", "DEFAULT.TABLE", false);
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/tables/{database}/{table}", "DEFAULT", "TABLE")
                .param("project", "default").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)));
        Mockito.verify(openTableController).unloadTable("default", "DEFAULT", "TABLE", false);
    }

    private Map<String, Object> mockStreamingTablesRequestMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("sampling_rows", "1");
        return map;
    }

}

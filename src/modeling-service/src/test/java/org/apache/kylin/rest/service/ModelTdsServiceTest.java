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

package org.apache.kylin.rest.service;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.scheduler.EventBusFactory;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.guava30.shaded.common.base.Charsets;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.guava30.shaded.common.io.CharStreams;
import org.apache.kylin.junit.rule.TransactionExceptedException;
import org.apache.kylin.metadata.acl.AclTCR;
import org.apache.kylin.metadata.acl.AclTCRManager;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.recommendation.candidate.JdbcRawRecStore;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.ModelRequest;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.apache.kylin.tool.bisync.BISyncTool;
import org.apache.kylin.tool.bisync.SyncContext;
import org.apache.kylin.tool.bisync.model.SyncModel;
import org.apache.kylin.tool.bisync.tableau.TableauDatasourceModel;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ModelTdsServiceTest extends SourceTestCase {

    @InjectMocks
    private final ModelService modelService = Mockito.spy(new ModelService());

    @InjectMocks
    private final ModelTdsService tdsService = Mockito.spy(new ModelTdsService());

    @InjectMocks
    private final ModelSemanticHelper semanticService = Mockito.spy(new ModelSemanticHelper());

    @InjectMocks
    private final IndexPlanService indexPlanService = Mockito.spy(new IndexPlanService());

    @Mock
    private final AclUtil aclUtil = Mockito.spy(AclUtil.class);

    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    protected IUserGroupService userGroupService = Mockito.spy(NUserGroupService.class);

    @Mock
    private final AccessService accessService = Mockito.spy(AccessService.class);

    @Rule
    public TransactionExceptedException thrown = TransactionExceptedException.none();

    protected String getProject() {
        return "default";
    }

    @Before
    public void setUp() {
        super.setUp();
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(modelService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(modelService, "accessService", accessService);
        ReflectionTestUtils.setField(modelService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(semanticService, "userGroupService", userGroupService);

        ReflectionTestUtils.setField(tdsService, "accessService", accessService);
        ReflectionTestUtils.setField(tdsService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(tdsService, "aclEvaluate", aclEvaluate);

        modelService.setSemanticUpdater(semanticService);
        modelService.setIndexPlanService(indexPlanService);

        try {
            new JdbcRawRecStore(getTestConfig());
        } catch (Exception e) {
            //
        }
    }

    @After
    public void tearDown() {
        getTestConfig().setProperty("kylin.metadata.semi-automatic-mode", "false");
        EventBusFactory.getInstance().restart();
        cleanupTestMetadata();
    }

    @Test
    public void testExportTDSWithDupMeasureDimColumnNames() throws IOException {
        String projectName = "default";
        String modelId = "199ee99e-8419-3e7a-7cad-97059999ec0a";
        val modelRequest = JsonUtil.readValue(
                new File("src/test/resources/ut_meta/dup_name_test/model_desc/model_dup_mea_dimcol.json"),
                ModelRequest.class);
        modelRequest.setProject(projectName);
        List<NDataModel.NamedColumn> simplifiedDims = modelRequest.getAllNamedColumns().stream()
                .filter(NDataModel.NamedColumn::isDimension).collect(Collectors.toList());
        modelRequest.setSimplifiedDimensions(simplifiedDims);
        modelService.createModel(projectName, modelRequest);
        NDataModelManager projectInstance = NDataModelManager.getInstance(getTestConfig(), projectName);
        Assert.assertNotNull(projectInstance.getDataModelDescByAlias("model_dup_mea_dimcol"));
        SyncContext syncContext = new SyncContext();
        syncContext.setProjectName(projectName);
        syncContext.setModelId(modelId);
        syncContext.setModelElement(SyncContext.ModelElement.ALL_COLS);
        syncContext.setAdmin(true);
        syncContext.setDataflow(NDataflowManager.getInstance(getTestConfig(), projectName).getDataflow(modelId));
        syncContext.setKylinConfig(getTestConfig());
        SyncModel syncModel = tdsService.exportModel(syncContext);
        overwriteSystemProp("kylin.model.skip-check-tds", "false");
        Assert.assertThrows(
                "There are duplicated names among dimension column LO_LINENUMBER and measure name LO_LINENUMBER. Cannot export a valid TDS file. Please correct the duplicated names and try again.",
                KylinException.class, () -> tdsService.preCheckNameConflict(syncModel));

        syncContext.setAdmin(false);
        prepareBasicPermissionByModel(projectName, syncContext.getDataflow().getModel());
        Mockito.when(accessService.getGroupsOfExecuteUser(Mockito.any(String.class)))
                .thenReturn(Sets.newHashSet("ROLE_ANALYST"));
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("u1", "ANALYST", Constant.ROLE_ANALYST));
        SyncModel syncModel2 = tdsService.exportModel(syncContext);
        Assert.assertThrows(
                "There are duplicated names among dimension column LO_LINENUMBER and measure name LO_LINENUMBER. Cannot export a valid TDS file. Please correct the duplicated names and try again.",
                KylinException.class, () -> tdsService.preCheckNameConflict(syncModel2));

        overwriteSystemProp("kylin.model.skip-check-tds", "true");
        Assert.assertTrue(tdsService.preCheckNameConflict(syncModel));

        syncContext.setAdmin(false);
        prepareBasicPermissionByModel(projectName, syncContext.getDataflow().getModel());
        Mockito.when(accessService.getGroupsOfExecuteUser(Mockito.any(String.class)))
                .thenReturn(Sets.newHashSet("ROLE_ANALYST"));
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("u1", "ANALYST", Constant.ROLE_ANALYST));
        SyncModel syncModel3 = tdsService.exportModel(syncContext);
        Assert.assertTrue(tdsService.preCheckNameConflict(syncModel3));
    }

    @Test
    public void testExportTdsWithDupMeasureDimensionNamesNoConflict() throws IOException {
        String projectName = "default";
        String modelId = "6f8cd656-9beb-47f6-87f5-89a8c548d17c";
        val modelRequest = JsonUtil.readValue(
                new File("src/test/resources/ut_meta/dup_name_test/model_desc/model_dup_mea_dim.json"),
                ModelRequest.class);
        modelRequest.setProject(projectName);
        List<NDataModel.NamedColumn> simplifiedDims = modelRequest.getAllNamedColumns().stream()
                .filter(NDataModel.NamedColumn::isDimension).collect(Collectors.toList());
        modelRequest.setSimplifiedDimensions(simplifiedDims);
        modelService.createModel(projectName, modelRequest);
        NDataModelManager projectInstance = NDataModelManager.getInstance(getTestConfig(), projectName);
        Assert.assertNotNull(projectInstance.getDataModelDescByAlias("model_dup_mea_dim"));
        SyncContext syncContext = new SyncContext();
        syncContext.setProjectName(projectName);
        syncContext.setModelId(modelId);
        syncContext.setModelElement(SyncContext.ModelElement.ALL_COLS);
        syncContext.setAdmin(true);
        syncContext.setDataflow(NDataflowManager.getInstance(getTestConfig(), projectName).getDataflow(modelId));
        syncContext.setKylinConfig(getTestConfig());
        SyncModel syncModel = tdsService.exportModel(syncContext);
        Assert.assertTrue(tdsService.preCheckNameConflict(syncModel));

        syncContext.setAdmin(false);
        prepareBasicPermissionByModel(projectName, syncContext.getDataflow().getModel());
        Mockito.when(accessService.getGroupsOfExecuteUser(Mockito.any(String.class)))
                .thenReturn(Sets.newHashSet("ROLE_ANALYST"));
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("u1", "ANALYST", Constant.ROLE_ANALYST));
        syncModel = tdsService.exportModel(syncContext);
        Assert.assertTrue(tdsService.preCheckNameConflict(syncModel));
    }

    @Test
    public void testExportTDSWithDupMeasureColumnNamesOutOfScope() throws IOException {
        String projectName = "default";
        String modelId = "2ed3bf12-ad40-e8a0-73da-8dc3b4c798bb";
        val modelRequest = JsonUtil.readValue(
                new File("src/test/resources/ut_meta/dup_name_test/model_desc/model_dup_mea_col.json"),
                ModelRequest.class);
        modelRequest.setProject(projectName);
        List<NDataModel.NamedColumn> simplifiedDims = modelRequest.getAllNamedColumns().stream()
                .filter(NDataModel.NamedColumn::isDimension).collect(Collectors.toList());
        modelRequest.setSimplifiedDimensions(simplifiedDims);
        modelService.createModel(projectName, modelRequest);
        NDataModelManager projectInstance = NDataModelManager.getInstance(getTestConfig(), projectName);
        Assert.assertNotNull(projectInstance.getDataModelDescByAlias("model_dup_mea_col"));
        SyncContext syncContext = new SyncContext();
        syncContext.setProjectName(projectName);
        syncContext.setModelId(modelId);
        syncContext.setModelElement(SyncContext.ModelElement.ALL_COLS);
        syncContext.setDataflow(NDataflowManager.getInstance(getTestConfig(), projectName).getDataflow(modelId));
        syncContext.setKylinConfig(getTestConfig());
        syncContext.setAdmin(true);
        overwriteSystemProp("kylin.model.skip-check-tds", "false");
        SyncModel syncModel = tdsService.exportModel(syncContext);
        Assert.assertThrows(
                "There are duplicated names among model column LO_LINENUMBER and measure name LO_LINENUMBER. Cannot export a valid TDS file. Please correct the duplicated names and try again.",
                KylinException.class, () -> tdsService.preCheckNameConflict(syncModel));

        syncContext.setAdmin(false);
        prepareBasicPermissionByModel(projectName, syncContext.getDataflow().getModel());
        Mockito.when(accessService.getGroupsOfExecuteUser(Mockito.any(String.class)))
                .thenReturn(Sets.newHashSet("ROLE_ANALYST"));
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("u1", "ANALYST", Constant.ROLE_ANALYST));
        SyncModel syncModel2 = tdsService.exportModel(syncContext);
        Assert.assertThrows(
                "There are duplicated names among model column LO_LINENUMBER and measure name LO_LINENUMBER. Cannot export a valid TDS file. Please correct the duplicated names and try again.",
                KylinException.class, () -> tdsService.preCheckNameConflict(syncModel2));
    }

    @Test
    public void testExportTDSByAdmin() throws Exception {
        val project = "default";
        val modelId = "cb596712-3a09-46f8-aea1-988b43fe9b6c";
        prepareBasic(project);
        List<String> dimensions = Lists.newArrayList();
        dimensions.add("DEFAULT.TEST_MEASURE.FLAG");
        dimensions.add("DEFAULT.TEST_MEASURE.PRICE1");
        dimensions.add("DEFAULT.TEST_MEASURE.ID1");
        List<String> measurs = Lists.newArrayList();
        measurs.add("COUNT_STAR");
        measurs.add("SUM_1");
        SyncContext syncContext = tdsService.prepareSyncContext(project, modelId, SyncContext.BI.TABLEAU_CONNECTOR_TDS,
                SyncContext.ModelElement.CUSTOM_COLS, "localhost", 8080);
        SyncModel syncModel = tdsService.exportTDSDimensionsAndMeasuresByAdmin(syncContext, dimensions, measurs);
        TableauDatasourceModel datasource1 = (TableauDatasourceModel) BISyncTool.getBISyncModel(syncContext, syncModel);
        ByteArrayOutputStream outStream4 = new ByteArrayOutputStream();
        datasource1.dump(outStream4);
        Assert.assertEquals(getExpectedTds("/bisync_tableau/nmodel_full_measure_test.connector_admin.tds"),
                outStream4.toString(Charset.defaultCharset().name()));
    }

    @Test
    public void testExportTDSByUser() throws Exception {
        val project = "default";
        val modelId = "cb596712-3a09-46f8-aea1-988b43fe9b6c";
        prepareBasic(project);
        List<String> dimensions = Lists.newArrayList();
        dimensions.add("TEST_MEASURE.ID1");
        dimensions.add("TEST_MEASURE.ID2");
        dimensions.add("TEST_MEASURE.ID3");
        dimensions.add("TEST_MEASURE1.ID1");
        dimensions.add("TEST_MEASURE1.NAME1");
        dimensions.add("TEST_MEASURE1.NAME2");
        dimensions.add("TEST_MEASURE1.NAME3");
        List<String> measurs = Lists.newArrayList();
        measurs.add("COUNT_STAR");
        measurs.add("SUM_1");
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("u1", "ANALYST", Constant.ROLE_ANALYST));
        SyncContext syncContext = tdsService.prepareSyncContext(project, modelId, SyncContext.BI.TABLEAU_CONNECTOR_TDS,
                SyncContext.ModelElement.CUSTOM_COLS, "localhost", 8080);
        syncContext.setAdmin(false);
        SyncModel syncModel = tdsService.exportTDSDimensionsAndMeasuresByNormalUser(syncContext, dimensions, measurs);
        TableauDatasourceModel datasource1 = (TableauDatasourceModel) BISyncTool.getBISyncModel(syncContext, syncModel);
        ByteArrayOutputStream outStream4 = new ByteArrayOutputStream();
        datasource1.dump(outStream4);
        Assert.assertEquals(getExpectedTds("/bisync_tableau/nmodel_full_measure_test.connector_user.tds"),
                outStream4.toString(Charset.defaultCharset().name()));
    }

    @Test
    public void testExportTDSByUserAndElement() throws Exception {
        val project = "default";
        val modelId = "cb596712-3a09-46f8-aea1-988b43fe9b6c";
        prepareBasic(project);
        List<String> dimensions = Lists.newArrayList();
        dimensions.add("TEST_MEASURE.ID1");
        try {
            SecurityContextHolder.getContext()
                    .setAuthentication(new TestingAuthenticationToken("u1", "ANALYST", Constant.ROLE_ANALYST));
            SyncContext syncContext = tdsService.prepareSyncContext(project, modelId,
                    SyncContext.BI.TABLEAU_CONNECTOR_TDS, SyncContext.ModelElement.AGG_INDEX_COL, "localhost", 8080);
            SyncModel syncModel = tdsService.exportTDSDimensionsAndMeasuresByNormalUser(syncContext, dimensions,
                    ImmutableList.of());
            TableauDatasourceModel datasource1 = (TableauDatasourceModel) BISyncTool.getBISyncModel(syncContext,
                    syncModel);
            ByteArrayOutputStream outStream4 = new ByteArrayOutputStream();
            datasource1.dump(outStream4);
            Assert.assertEquals(
                    getExpectedTds("/bisync_tableau/nmodel_full_measure_test.connector_user_agg_index_col.tds"),
                    outStream4.toString(Charset.defaultCharset().name()));
        } finally {
            SecurityContextHolder.getContext()
                    .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        }
    }

    @Test
    public void testCheckModelExportPermissionException() {
        val project = "default";
        val modelId = "cb596712-3a09-46f8-aea1-988b43fe9b6c";
        prepareBasic(project);
        tdsService.prepareSyncContext(project, modelId, SyncContext.BI.TABLEAU_CONNECTOR_TDS,
                SyncContext.ModelElement.AGG_INDEX_COL, "localhost", 8080);
        try {
            Mockito.when(accessService.getGroupsOfExecuteUser(Mockito.any(String.class)))
                    .thenReturn(Sets.newHashSet("ROLE_ANALYST"));
            SecurityContextHolder.getContext()
                    .setAuthentication(new TestingAuthenticationToken("u1", "ANALYST", Constant.ROLE_ANALYST));
            thrown.expect(KylinException.class);
            thrown.expectMessage("current user does not have full permission on requesting model");
            SyncContext syncContext = tdsService.prepareSyncContext(project, modelId,
                    SyncContext.BI.TABLEAU_CONNECTOR_TDS, SyncContext.ModelElement.AGG_INDEX_COL, "localhost", 8080);
            tdsService.exportModel(syncContext);
        } finally {
            SecurityContextHolder.getContext()
                    .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        }
    }

    @Test
    public void testCheckModelExportPermission() {
        val project = "default";
        val modelId = "cb596712-3a09-46f8-aea1-988b43fe9b6c";
        prepareBasic(project);
        tdsService.prepareSyncContext(project, modelId, SyncContext.BI.TABLEAU_CONNECTOR_TDS,
                SyncContext.ModelElement.AGG_INDEX_COL, "localhost", 8080);
        tdsService.prepareSyncContext(project, modelId, SyncContext.BI.TABLEAU_CONNECTOR_TDS,
                SyncContext.ModelElement.AGG_INDEX_COL, "localhost", 8080);
    }

    @Test
    public void testCheckModelExportPermissionWithCC() {
        val project = "cc_test";
        val modelId = "0d146f1a-bdd3-4548-87ac-21c2c6f9a0da";
        AclTCRManager manager = AclTCRManager.getInstance(getTestConfig(), project);
        {
            AclTCR u1a1 = new AclTCR();
            manager.updateAclTCR(u1a1, "u1", true);
            SecurityContextHolder.getContext()
                    .setAuthentication(new TestingAuthenticationToken("u1", "ANALYST", Constant.ROLE_ANALYST));
            Mockito.when(accessService.getGroupsOfExecuteUser(Mockito.any(String.class)))
                    .thenReturn(Sets.newHashSet("ROLE_ANALYST"));
            tdsService.prepareSyncContext(project, modelId, SyncContext.BI.TABLEAU_CONNECTOR_TDS,
                    SyncContext.ModelElement.AGG_INDEX_COL, "localhost", 8080);
        }
        {
            try {
                AclTCR u1a1 = new AclTCR();
                AclTCR.Table u1t1 = new AclTCR.Table();
                AclTCR.ColumnRow u1cr1 = new AclTCR.ColumnRow();
                AclTCR.Column u1c1 = new AclTCR.Column();
                u1c1.add("ORDER_ID");
                u1cr1.setColumn(u1c1);
                u1t1.put("SSB.LINEORDER", u1cr1);
                u1a1.setTable(u1t1);
                manager.updateAclTCR(u1a1, "u1", true);
                thrown.expect(KylinException.class);
                thrown.expectMessage("current user does not have full permission on requesting model");
                SyncContext syncContext = tdsService.prepareSyncContext(project, modelId,
                        SyncContext.BI.TABLEAU_CONNECTOR_TDS, SyncContext.ModelElement.AGG_INDEX_COL, "localhost",
                        8080);
                tdsService.exportModel(syncContext);
            } finally {
                SecurityContextHolder.getContext()
                        .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
            }
        }

    }

    @Test
    public void testExportTDSByBroken() {
        val project = "test_broken_project";
        val modelId = "4b93b131-824e-6966-c4dd-5a4268d27095";
        List<String> dimensions = Lists.newArrayList();
        List<String> measures = Lists.newArrayList();
        Assert.assertThrows(KylinException.class, () -> tdsService.prepareSyncContext(project, modelId,
                SyncContext.BI.TABLEAU_CONNECTOR_TDS, SyncContext.ModelElement.CUSTOM_COLS, "localhost", 8080));
    }

    @Test
    public void testExportTDSMeasurePermission() {
        val project = "default";
        val modelId = "82fa7671-a935-45f5-8779-85703601f49a";
        prepareBasicByMeasure(project);
        List<String> dimensions = Lists.newArrayList();
        //"ORDER_ID", "PRICE", "CAL_DT", "PRICE", "ITEM_COUNT", "LEAF_CATEG_ID"
        dimensions.add("TEST_KYLIN_FACT.ORDER_ID");
        dimensions.add("TEST_KYLIN_FACT.PRICE");
        dimensions.add("TEST_KYLIN_FACT.CAL_DT");
        dimensions.add("TEST_KYLIN_FACT.PRICE");
        dimensions.add("TEST_KYLIN_FACT.ITEM_COUNT");
        dimensions.add("TEST_KYLIN_FACT.LEAF_CATEG_ID");
        //"ORDER_ID", "TEST_TIME_ENC", "TEST_DATE_ENC"
        dimensions.add("TEST_ORDER.ORDER_ID");
        dimensions.add("TEST_ORDER.TEST_TIME_ENC");
        dimensions.add("TEST_ORDER.TEST_DATE_ENC");
        //"ORDER_ID", "PRICE", "CAL_DT", "TRANS_ID"
        dimensions.add("TEST_MEASURE.ORDER_ID");
        dimensions.add("TEST_MEASURE.PRICE");
        dimensions.add("TEST_MEASURE.CAL_DT");
        dimensions.add("TEST_MEASURE.TRANS_ID");

        List<String> measures = Lists.newArrayList();
        measures.add("TRANS_CNT");
        measures.add("GMV_SUM");
        measures.add("GMV_MIN");
        measures.add("GMV_MAX");
        measures.add("ITEM_COUNT_SUM");
        measures.add("ITEM_COUNT_MAX");
        measures.add("ITEM_COUNT_MIN");
        measures.add("SELLER_HLL");
        measures.add("COUNT_DISTINCT");
        measures.add("TOP_SELLER");
        measures.add("TEST_COUNT_DISTINCT_BITMAP");
        measures.add("GVM_PERCENTILE");
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("u1", "ANALYST", Constant.ROLE_ANALYST));
        SyncContext syncContext = tdsService.prepareSyncContext(project, modelId, SyncContext.BI.TABLEAU_CONNECTOR_TDS,
                SyncContext.ModelElement.CUSTOM_COLS, "localhost", 8080);
        Assert.assertThrows(KylinException.class,
                () -> tdsService.exportTDSDimensionsAndMeasuresByNormalUser(syncContext, dimensions, measures));
    }

    private void prepareBasicByMeasure(String project) {
        AclTCRManager manager = AclTCRManager.getInstance(getTestConfig(), project);

        AclTCR u1a1 = new AclTCR();
        AclTCR.Table u1t1 = new AclTCR.Table();
        AclTCR.ColumnRow u1cr1 = new AclTCR.ColumnRow();
        AclTCR.Column u1c1 = new AclTCR.Column();
        u1c1.addAll(Arrays.asList("ORDER_ID", "PRICE", "CAL_DT", "PRICE", "ITEM_COUNT", "LEAF_CATEG_ID"));
        u1cr1.setColumn(u1c1);

        AclTCR.ColumnRow u1cr2 = new AclTCR.ColumnRow();
        AclTCR.Column u1c2 = new AclTCR.Column();
        u1c2.addAll(Arrays.asList("ORDER_ID", "TEST_TIME_ENC", "TEST_DATE_ENC"));
        u1cr2.setColumn(u1c2);
        u1t1.put("DEFAULT.TEST_KYLIN_FACT", u1cr1);
        u1t1.put("DEFAULT.TEST_ORDER", u1cr2);
        u1a1.setTable(u1t1);
        manager.updateAclTCR(u1a1, "u1", true);
    }

    @Test
    public void testExportModel() throws Exception {
        val project = "default";
        val modelId = "cb596712-3a09-46f8-aea1-988b43fe9b6c";
        prepareBasic(project);
        SyncContext syncContext = tdsService.prepareSyncContext(project, modelId, SyncContext.BI.TABLEAU_CONNECTOR_TDS,
                SyncContext.ModelElement.AGG_INDEX_AND_TABLE_INDEX_COL, "localhost", 7070);
        SyncModel syncModel = tdsService.exportModel(syncContext);
        TableauDatasourceModel datasource1 = (TableauDatasourceModel) BISyncTool.getBISyncModel(syncContext, syncModel);
        ByteArrayOutputStream outStream4 = new ByteArrayOutputStream();
        datasource1.dump(outStream4);
        Assert.assertEquals(getExpectedTds("/bisync_tableau/nmodel_full_measure_test.connector.tds"),
                outStream4.toString(Charset.defaultCharset().name()));
    }

    private String getExpectedTds(String path) throws IOException {
        return CharStreams.toString(
                new InputStreamReader(Objects.requireNonNull(getClass().getResourceAsStream(path)), Charsets.UTF_8));
    }

    private void prepareBasic(String project) {
        AclTCRManager manager = AclTCRManager.getInstance(getTestConfig(), project);

        AclTCR u1a1 = new AclTCR();
        AclTCR.Table u1t1 = new AclTCR.Table();
        AclTCR.ColumnRow u1cr1 = new AclTCR.ColumnRow();
        AclTCR.Column u1c1 = new AclTCR.Column();
        u1c1.addAll(Arrays.asList("ID1", "ID2", "ID3"));
        u1cr1.setColumn(u1c1);

        AclTCR.ColumnRow u1cr2 = new AclTCR.ColumnRow();
        AclTCR.Column u1c2 = new AclTCR.Column();
        u1c2.addAll(Arrays.asList("ID1", "NAME1", "NAME2", "NAME3"));
        u1cr2.setColumn(u1c2);
        u1t1.put("DEFAULT.TEST_MEASURE", u1cr1);
        u1t1.put("DEFAULT.TEST_MEASURE1", u1cr2);
        u1a1.setTable(u1t1);
        manager.updateAclTCR(u1a1, "u1", true);

        AclTCR g1a1 = new AclTCR();
        AclTCR.Table g1t1 = new AclTCR.Table();
        AclTCR.ColumnRow g1cr1 = new AclTCR.ColumnRow();
        AclTCR.Column g1c1 = new AclTCR.Column();
        g1c1.addAll(Arrays.asList("ID1", "ID2", "ID3", "ID4"));
        g1cr1.setColumn(g1c1);
        g1t1.put("DEFAULT.TEST_MEASURE", g1cr1);
        g1a1.setTable(g1t1);
        manager.updateAclTCR(g1a1, "g1", false);
    }

    private void prepareBasicPermissionByModel(String project, NDataModel model) {
        AclTCRManager manager = AclTCRManager.getInstance(getTestConfig(), project);
        AclTCR u1a1 = new AclTCR();
        AclTCR.Table u1t1 = new AclTCR.Table();
        for (TableRef table : model.getAllTables()) {
            AclTCR.ColumnRow u1cr1 = new AclTCR.ColumnRow();
            AclTCR.Column u1c1 = new AclTCR.Column();
            List<String> colNames = Lists.newArrayList();
            for (TblColRef col : table.getColumns()) {
                colNames.add(col.getName());
            }
            u1c1.addAll(colNames);
            u1cr1.setColumn(u1c1);
            u1t1.put(table.getTableIdentity(), u1cr1);
        }
        u1a1.setTable(u1t1);
        manager.updateAclTCR(u1a1, "u1", true);
    }

    @Test
    public void testCheckTablePermission() {
        val project = "default";
        val modelId = "cb596712-3a09-46f8-aea1-988b43fe9b6c";
        thrown.expect(KylinException.class);
        thrown.expectMessage(MsgPicker.getMsg().getTableNoColumnsPermission());

        AclTCRManager manager = AclTCRManager.getInstance(getTestConfig(), project);
        Set<String> columns = new HashSet<>();
        columns.add("DEFAULT.TEST_MEASURE1.NAME1");
        columns.add("DEFAULT.TEST_MEASURE1.NAME2");
        columns.add("DEFAULT.TEST_MEASURE1.NAME3");

        AclTCR u1a1 = new AclTCR();
        AclTCR.Table u1t1 = new AclTCR.Table();
        AclTCR.ColumnRow u1cr1 = new AclTCR.ColumnRow();
        AclTCR.Column u1c1 = new AclTCR.Column();
        u1cr1.setColumn(u1c1);

        AclTCR.ColumnRow u1cr2 = new AclTCR.ColumnRow();
        AclTCR.Column u1c2 = new AclTCR.Column();
        u1c2.addAll(Arrays.asList("NAME1", "NAME2", "NAME3"));
        u1cr2.setColumn(u1c2);
        u1t1.put("DEFAULT.TEST_MEASURE", u1cr1);
        u1t1.put("DEFAULT.TEST_MEASURE1", u1cr2);
        u1a1.setTable(u1t1);
        manager.updateAclTCR(u1a1, "u1", true);
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("u1", "ANALYST", Constant.ROLE_ANALYST));
        List<String> dimensions = Lists.newArrayList();
        dimensions.add("TEST_MEASURE.FLAG");
        dimensions.add("TEST_MEASURE.PRICE1");
        dimensions.add("TEST_MEASURE.ID1");
        List<String> measurs = Lists.newArrayList();
        measurs.add("COUNT_STAR");
        measurs.add("SUM_1");
        tdsService.checkTableHasColumnPermission(SyncContext.ModelElement.CUSTOM_COLS, project, modelId, columns,
                dimensions, measurs);

        dimensions.add("TEST_MEASURE.ID4");
        Assert.assertThrows(KylinException.class,
                () -> tdsService.checkTableHasColumnPermission(SyncContext.ModelElement.CUSTOM_COLS, project, modelId,
                        columns, dimensions, measurs));
    }

    @Test
    public void testExportTDSCheckColumnPermission() {
        val project = "default";
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";

        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        NDataModel dataModel = modelManager.getDataModelDesc(modelId);

        Set<String> authColumns = Sets.newHashSet();
        List<String> dimensions = Lists.newArrayList();
        List<String> measurs = Lists.newArrayList();

        Assert.assertTrue(tdsService.checkColumnPermission(dataModel, authColumns, null, measurs));
        Assert.assertTrue(tdsService.checkColumnPermission(dataModel, authColumns, null, null));
        Assert.assertTrue(tdsService.checkColumnPermission(dataModel, authColumns, dimensions, null));
        Assert.assertTrue(tdsService.checkColumnPermission(dataModel, authColumns, dimensions, measurs));

        authColumns.add("DEFAULT.TEST_KYLIN_FACT.PRICE");
        authColumns.add("DEFAULT.TEST_KYLIN_FACT.ITEM_COUNT");
        authColumns.add("EDW.TEST_CAL_DT.CAL_DT");
        authColumns.add("DEFAULT.TEST_ACCOUNT.ACCOUNT_ID");

        Set<String> newAuthColumns = Sets.newHashSet();
        dataModel.getAllTables().forEach(tableRef -> {
            List<TblColRef> collect = tableRef.getColumns().stream()
                    .filter(column -> authColumns.contains(column.getCanonicalName())).collect(Collectors.toList());
            collect.forEach(x -> newAuthColumns.add(x.getAliasDotName()));
        });

        dimensions.add("TEST_KYLIN_FACT.DEAL_AMOUNT");
        dimensions.add("TEST_KYLIN_FACT.TRANS_ID");

        Assert.assertFalse(tdsService.checkColumnPermission(dataModel, newAuthColumns, dimensions, measurs));

        newAuthColumns.add("TEST_KYLIN_FACT.TRANS_ID");

        measurs.add("SUM_NEST4");
        measurs.add("COUNT_CAL_DT");
        Assert.assertTrue(tdsService.checkColumnPermission(dataModel, newAuthColumns, dimensions, measurs));

        Assert.assertTrue(tdsService.checkColumnPermission(dataModel, newAuthColumns, dimensions, measurs));
    }

    @Test
    public void testConvertCCToNormalCols() {
        val project = "default";
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        NDataModel dataModel = modelManager.getDataModelDesc(modelId);
        NDataModel.Measure measure = dataModel.getEffectiveMeasures().values().stream()
                .filter(x -> x.getName().equals("SUM_NEST4")).findFirst().get();
        Set<String> measureColumns = measure.getFunction().getParameters().stream()
                .filter(parameterDesc -> parameterDesc.getColRef() != null)
                .map(parameterDesc -> parameterDesc.getColRef().getCanonicalName()).collect(Collectors.toSet());
        ComputedColumnDesc sumNest4 = dataModel.getComputedColumnDescs().stream()
                .filter(x -> measureColumns.contains(x.getIdentName())).findFirst().get();
        Set<String> strings = tdsService.convertCCToNormalCols(dataModel, sumNest4);
        Assert.assertEquals("TEST_KYLIN_FACT.PRICE, TEST_KYLIN_FACT.ITEM_COUNT", String.join(", ", strings));

        sumNest4.setInnerExpression("1 + 2");
        Set<String> set = tdsService.convertCCToNormalCols(dataModel, sumNest4);
        Assert.assertEquals(Collections.emptySet(), set);

        HashSet<Object> authColumns = Sets.newHashSet();
        authColumns.add("DEFAULT.TEST_KYLIN_FACT.PRICE");
        Assert.assertTrue(authColumns.containsAll(set));
    }
}

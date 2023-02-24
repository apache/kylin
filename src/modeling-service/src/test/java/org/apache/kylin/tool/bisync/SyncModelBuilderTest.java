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

package org.apache.kylin.tool.bisync;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.acl.AclTCR;
import org.apache.kylin.metadata.acl.AclTCRDigest;
import org.apache.kylin.metadata.acl.AclTCRManager;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.ModelJoinRelationTypeEnum;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.tool.bisync.tableau.TableauDatasourceModel;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import lombok.val;

public class SyncModelBuilderTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }

    private String getProject() {
        return "default";
    }

    @Test
    public void testBuildSyncModel() {
        val project = "default";
        val modelId = "cb596712-3a09-46f8-aea1-988b43fe9b6c";
        val syncContext = SyncModelTestUtil.createSyncContext(project, modelId, KylinConfig.getInstanceFromEnv());
        syncContext.setModelElement(SyncContext.ModelElement.ALL_COLS);
        syncContext.setAdmin(true);
        val syncModel = new SyncModelBuilder(syncContext).buildSourceSyncModel(ImmutableList.of(), ImmutableList.of());
        val df = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project).getDataflow(modelId);
        val model = df.getModel();

        Assert.assertEquals(project, syncModel.getProject());
        Assert.assertEquals(model.getAlias(), syncModel.getModelName());
        Assert.assertEquals("localhost", syncModel.getHost());
        Assert.assertEquals("7070", syncModel.getPort());
        val factTable = syncModel.getJoinTree().getValue();
        Assert.assertEquals(1, syncModel.getJoinTree().getChildNodes().size());

        val lookupTable = syncModel.getJoinTree().getChildNodes().get(0).getValue();
        Assert.assertEquals("TEST_MEASURE", factTable.getAlias());
        Assert.assertEquals(NDataModel.TableKind.FACT, factTable.getKind());
        Assert.assertEquals(ModelJoinRelationTypeEnum.MANY_TO_ONE, factTable.getJoinRelationTypeEnum());
        Assert.assertEquals("TEST_MEASURE1", lookupTable.getAlias());
        Assert.assertEquals(NDataModel.TableKind.LOOKUP, lookupTable.getKind());
        Assert.assertEquals(ModelJoinRelationTypeEnum.MANY_TO_ONE, lookupTable.getJoinRelationTypeEnum());

        val modelCol = model.getEffectiveCols().values().iterator().next();
        Assert.assertEquals(model.getEffectiveCols().size(), syncModel.getColumnDefMap().size());
        val syncCol = syncModel.getColumnDefMap().get(modelCol.getIdentity());
        Assert.assertEquals(modelCol.getTableAlias(), syncCol.getTableAlias());
        Assert.assertEquals(modelCol.getName(), syncCol.getColumnName());
        Assert.assertEquals(modelCol.getColumnDesc().getName(), syncCol.getColumnAlias());
        Assert.assertEquals("dimension", syncCol.getRole());

        Assert.assertEquals(model.getAllMeasures().size(), syncModel.getMetrics().size());
        val syncMeasure = syncModel.getMetrics().get(0).getMeasure();
        val modelMeasure = model.getAllMeasures().stream().filter(m -> m.getId() == syncMeasure.getId()).findFirst();
        Assert.assertTrue(modelMeasure.isPresent());
        Assert.assertEquals(modelMeasure.get(), syncMeasure);
    }

    @Test
    public void testBuildHasPermissionSourceSyncModel() throws Exception {
        Set<String> groups = new HashSet<>();
        groups.add("g1");
        val project = "default";
        val modelId = "82fa7671-a935-45f5-8779-85703601f49a";
        val syncContext = SyncModelTestUtil.createSyncContext(project, modelId, KylinConfig.getInstanceFromEnv());
        prepareBasic();

        Set<String> allAuthTables = Sets.newHashSet();
        Set<String> allAuthColumns = Sets.newHashSet();
        AclTCRManager aclTCRManager = AclTCRManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        AclTCRDigest auths = aclTCRManager.getAuthTablesAndColumns(project, "u1", true);
        allAuthTables.addAll(auths.getTables());
        allAuthColumns.addAll(auths.getColumns());
        for (String group : groups) {
            auths = aclTCRManager.getAuthTablesAndColumns(project, group, false);
            allAuthTables.addAll(auths.getTables());
            allAuthColumns.addAll(auths.getColumns());
        }

        Set<String> newAuthColumns = convertColumns(syncContext.getDataflow().getModel(), allAuthColumns);
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
        dimensions.add("DEFAULT.TEST_ORDER.TEST_DATE_ENC");
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

        syncContext.setAdmin(false);
        syncContext.setModelElement(SyncContext.ModelElement.CUSTOM_COLS);
        TableauDatasourceModel datasource = (TableauDatasourceModel) BISyncTool
                .dumpHasPermissionToBISyncModel(syncContext, allAuthTables, newAuthColumns, dimensions, measures);
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        datasource.dump(outStream);
        Assert.assertEquals(getExpectedTds("/bisync_tableau/nmodel_full_measure_test.connector_permission.tds"),
                outStream.toString(Charset.defaultCharset().name()));

        syncContext.setAdmin(true);
        syncContext.setModelElement(SyncContext.ModelElement.CUSTOM_COLS);
        TableauDatasourceModel datasource1 = (TableauDatasourceModel) BISyncTool.dumpBISyncModel(syncContext,
                dimensions, ImmutableList.of());
        ByteArrayOutputStream outStream1 = new ByteArrayOutputStream();
        datasource1.dump(outStream1);
        Assert.assertEquals(
                getExpectedTds("/bisync_tableau/nmodel_full_measure_test.connector_permission_no_measure.tds"),
                outStream1.toString(Charset.defaultCharset().name()));

        syncContext.setAdmin(true);
        syncContext.setModelElement(SyncContext.ModelElement.AGG_INDEX_COL);
        TableauDatasourceModel datasource2 = (TableauDatasourceModel) BISyncTool.dumpBISyncModel(syncContext,
                ImmutableList.of(), ImmutableList.of());
        ByteArrayOutputStream outStream2 = new ByteArrayOutputStream();
        datasource2.dump(outStream2);
        Assert.assertEquals(
                getExpectedTds("/bisync_tableau/nmodel_full_measure_test.connector_permission_agg_index_col.tds"),
                outStream2.toString(Charset.defaultCharset().name()));

        syncContext.setAdmin(true);
        syncContext.setModelElement(SyncContext.ModelElement.AGG_INDEX_AND_TABLE_INDEX_COL);
        TableauDatasourceModel datasource3 = (TableauDatasourceModel) BISyncTool.dumpBISyncModel(syncContext,
                ImmutableList.of(), ImmutableList.of());
        ByteArrayOutputStream outStream3 = new ByteArrayOutputStream();
        datasource3.dump(outStream3);
        Assert.assertEquals(
                getExpectedTds("/bisync_tableau/nmodel_full_measure_test.connector_permission_agg_index_col.tds"),
                outStream3.toString(Charset.defaultCharset().name()));

        syncContext.setAdmin(true);
        syncContext.setModelElement(SyncContext.ModelElement.ALL_COLS);
        TableauDatasourceModel datasource4 = (TableauDatasourceModel) BISyncTool.dumpBISyncModel(syncContext,
                ImmutableList.of(), ImmutableList.of());
        ByteArrayOutputStream outStream4 = new ByteArrayOutputStream();
        datasource4.dump(outStream4);
        Assert.assertEquals(getExpectedTds("/bisync_tableau/nmodel_full_measure_test.connector_permission_all_col.tds"),
                outStream4.toString(Charset.defaultCharset().name()));
    }

    @Test
    public void testBuildSyncModelType() throws Exception {
        val project = "default";
        val modelId = "cb596712-3a09-46f8-aea1-988b43fe9b6c";
        val syncContext = SyncModelTestUtil.createSyncContext(project, modelId, KylinConfig.getInstanceFromEnv());
        syncContext.setModelElement(SyncContext.ModelElement.ALL_COLS);
        syncContext.setAdmin(true);
        prepareBasic();

        TableauDatasourceModel datasource = (TableauDatasourceModel) BISyncTool.dumpToBISyncModel(syncContext);
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        datasource.dump(outStream);
        Assert.assertEquals(getExpectedTds("/bisync_tableau/nmodel_full_measure_test.connector.tds"),
                outStream.toString(Charset.defaultCharset().name()));

        syncContext.setModelElement(SyncContext.ModelElement.AGG_INDEX_AND_TABLE_INDEX_COL);
        TableauDatasourceModel datasource1 = (TableauDatasourceModel) BISyncTool.dumpToBISyncModel(syncContext);
        ByteArrayOutputStream outStream1 = new ByteArrayOutputStream();
        datasource1.dump(outStream1);
        Assert.assertEquals(getExpectedTds("/bisync_tableau/nmodel_full_measure_test.connector.tds"),
                outStream1.toString(Charset.defaultCharset().name()));

        syncContext.setModelElement(SyncContext.ModelElement.AGG_INDEX_COL);
        TableauDatasourceModel datasource2 = (TableauDatasourceModel) BISyncTool.dumpToBISyncModel(syncContext);
        ByteArrayOutputStream outStream2 = new ByteArrayOutputStream();
        datasource2.dump(outStream2);
        Assert.assertEquals(getExpectedTds("/bisync_tableau/nmodel_full_measure_test.connector.tds"),
                outStream2.toString(Charset.defaultCharset().name()));

        val syncContext1 = SyncModelTestUtil.createSyncContext(project, "89af4ee2-2cdb-4b07-b39e-4c29856309aa",
                KylinConfig.getInstanceFromEnv());
        syncContext1.setAdmin(true);
        syncContext1.setModelElement(SyncContext.ModelElement.AGG_INDEX_COL);
        TableauDatasourceModel datasource3 = (TableauDatasourceModel) BISyncTool.dumpToBISyncModel(syncContext1);
        ByteArrayOutputStream outStream3 = new ByteArrayOutputStream();
        datasource3.dump(outStream3);
        Assert.assertEquals(getExpectedTds("/bisync_tableau/nmodel_full_measure_test.table_index_connector.tds"),
                outStream3.toString(Charset.defaultCharset().name()));
    }

    private Set<String> convertColumns(NDataModel model, Set<String> authColumns) {
        Set<String> newAuthColumns = Sets.newHashSet();
        model.getAllTables().forEach(tableRef -> {
            List<TblColRef> collect = tableRef.getColumns().stream()
                    .filter(column -> authColumns.contains(column.getCanonicalName())).collect(Collectors.toList());
            collect.forEach(x -> newAuthColumns.add(x.getAliasDotName()));
        });
        return newAuthColumns;
    }

    @Test
    public void testCheckCC() throws Exception {
        Set<String> groups = new HashSet<>();
        groups.add("g1");
        val project = "default";
        val modelId = "741ca86a-1f13-46da-a59f-95fb68615e3a";
        prepareBasic();
        Set<String> allAuthTables = Sets.newHashSet();
        Set<String> allAuthColumns = Sets.newHashSet();
        AclTCRManager aclTCRManager = AclTCRManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        AclTCRDigest auths = aclTCRManager.getAuthTablesAndColumns(project, "u1", true);
        allAuthTables.addAll(auths.getTables());
        allAuthColumns.addAll(auths.getColumns());
        for (String group : groups) {
            auths = aclTCRManager.getAuthTablesAndColumns(project, group, false);
            allAuthTables.addAll(auths.getTables());
            allAuthColumns.addAll(auths.getColumns());
        }
        val cc_syncContext = SyncModelTestUtil.createSyncContext(project, modelId, KylinConfig.getInstanceFromEnv());
        Set<String> newAuthColumns = convertColumns(cc_syncContext.getDataflow().getModel(), allAuthColumns);
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

        cc_syncContext.setModelElement(SyncContext.ModelElement.CUSTOM_COLS);
        cc_syncContext.setAdmin(false);
        TableauDatasourceModel datasource3 = (TableauDatasourceModel) BISyncTool
                .dumpHasPermissionToBISyncModel(cc_syncContext, allAuthTables, newAuthColumns, dimensions, measures);
        ByteArrayOutputStream outStream3 = new ByteArrayOutputStream();
        datasource3.dump(outStream3);
        Assert.assertEquals(getExpectedTds("/bisync_tableau/nmodel_full_measure_test.connector_cc.tds"),
                outStream3.toString(Charset.defaultCharset().name()));

        cc_syncContext.setModelElement(SyncContext.ModelElement.CUSTOM_COLS);
        cc_syncContext.setAdmin(true);
        TableauDatasourceModel datasource1 = (TableauDatasourceModel) BISyncTool.dumpBISyncModel(cc_syncContext,
                dimensions, measures);
        ByteArrayOutputStream outStream1 = new ByteArrayOutputStream();
        datasource1.dump(outStream1);
        Assert.assertEquals(getExpectedTds("/bisync_tableau/nmodel_full_measure_test.connector_cc_admin.tds"),
                outStream1.toString(Charset.defaultCharset().name()));
    }

    @Test
    public void testHierarchies() throws Exception {
        Set<String> groups = new HashSet<>();
        groups.add("g1");
        val project = "default";
        val modelId = "82fa7671-a935-45f5-8779-85703601f49a";
        prepareBasic();
        Set<String> allAuthTables = Sets.newHashSet();
        Set<String> allAuthColumns = Sets.newHashSet();
        AclTCRManager aclTCRManager = AclTCRManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        AclTCRDigest auths = aclTCRManager.getAuthTablesAndColumns(project, "u1", true);
        allAuthTables.addAll(auths.getTables());
        allAuthColumns.addAll(auths.getColumns());
        for (String group : groups) {
            auths = aclTCRManager.getAuthTablesAndColumns(project, group, false);
            allAuthTables.addAll(auths.getTables());
            allAuthColumns.addAll(auths.getColumns());
        }
        val cc_syncContext = SyncModelTestUtil.createSyncContext(project, modelId, KylinConfig.getInstanceFromEnv());
        cc_syncContext.setModelElement(SyncContext.ModelElement.ALL_COLS);
        cc_syncContext.setAdmin(true);

        Set<String> newAuthColumns = convertColumns(cc_syncContext.getDataflow().getModel(), allAuthColumns);
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

        cc_syncContext.setModelElement(SyncContext.ModelElement.CUSTOM_COLS);
        cc_syncContext.setAdmin(false);
        TableauDatasourceModel datasource3 = (TableauDatasourceModel) BISyncTool
                .dumpHasPermissionToBISyncModel(cc_syncContext, allAuthTables, newAuthColumns, dimensions, measures);
        ByteArrayOutputStream outStream3 = new ByteArrayOutputStream();
        datasource3.dump(outStream3);
        Assert.assertEquals(getExpectedTds("/bisync_tableau/nmodel_full_measure_test.connector_hierarchies.tds"),
                outStream3.toString(Charset.defaultCharset().name()));

        cc_syncContext.setModelElement(SyncContext.ModelElement.AGG_INDEX_COL);
        cc_syncContext.setAdmin(false);
        TableauDatasourceModel datasource4 = (TableauDatasourceModel) BISyncTool.dumpHasPermissionToBISyncModel(
                cc_syncContext, allAuthTables, newAuthColumns, new ArrayList<>(), new ArrayList<>());
        ByteArrayOutputStream outStream4 = new ByteArrayOutputStream();
        datasource4.dump(outStream4);
        Assert.assertEquals(getExpectedTds("/bisync_tableau/nmodel_full_measure_test.connector_hierarchies.tds"),
                outStream4.toString(Charset.defaultCharset().name()));

        cc_syncContext.setModelElement(SyncContext.ModelElement.AGG_INDEX_AND_TABLE_INDEX_COL);
        cc_syncContext.setAdmin(false);
        TableauDatasourceModel datasource5 = (TableauDatasourceModel) BISyncTool.dumpHasPermissionToBISyncModel(
                cc_syncContext, allAuthTables, newAuthColumns, new ArrayList<>(), new ArrayList<>());
        ByteArrayOutputStream outStream5 = new ByteArrayOutputStream();
        datasource5.dump(outStream5);
        Assert.assertEquals(getExpectedTds("/bisync_tableau/nmodel_full_measure_test.connector_hierarchies.tds"),
                outStream5.toString(Charset.defaultCharset().name()));

        cc_syncContext.setModelElement(SyncContext.ModelElement.ALL_COLS);
        cc_syncContext.setAdmin(false);
        TableauDatasourceModel datasource6 = (TableauDatasourceModel) BISyncTool.dumpHasPermissionToBISyncModel(
                cc_syncContext, allAuthTables, newAuthColumns, new ArrayList<>(), new ArrayList<>());
        ByteArrayOutputStream outStream6 = new ByteArrayOutputStream();
        datasource6.dump(outStream6);
        Assert.assertEquals(getExpectedTds("/bisync_tableau/nmodel_full_measure_test.connector_hierarchies.tds"),
                outStream6.toString(Charset.defaultCharset().name()));
    }

    @Test
    public void testNoHierarchies() throws Exception {
        Set<String> groups = new HashSet<>();
        groups.add("g1");
        val project = "default";
        val modelId = "82fa7671-a935-45f5-8779-85703601f49a";
        prepareBasicNoHierarchies();
        Set<String> allAuthTables = Sets.newHashSet();
        Set<String> allAuthColumns = Sets.newHashSet();
        AclTCRManager aclTCRManager = AclTCRManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        AclTCRDigest auths = aclTCRManager.getAuthTablesAndColumns(project, "u1", true);
        allAuthTables.addAll(auths.getTables());
        allAuthColumns.addAll(auths.getColumns());
        for (String group : groups) {
            auths = aclTCRManager.getAuthTablesAndColumns(project, group, false);
            allAuthTables.addAll(auths.getTables());
            allAuthColumns.addAll(auths.getColumns());
        }
        val cc_syncContext = SyncModelTestUtil.createSyncContext(project, modelId, KylinConfig.getInstanceFromEnv());
        Set<String> newAuthColumns = convertColumns(cc_syncContext.getDataflow().getModel(), allAuthColumns);
        List<String> dimensions = Lists.newArrayList();
        //"ORDER_ID", "PRICE", "CAL_DT", "PRICE", "ITEM_COUNT"
        dimensions.add("TEST_KYLIN_FACT.ORDER_ID");
        dimensions.add("TEST_KYLIN_FACT.PRICE");
        dimensions.add("TEST_KYLIN_FACT.CAL_DT");
        dimensions.add("TEST_KYLIN_FACT.PRICE");
        dimensions.add("TEST_KYLIN_FACT.ITEM_COUNT");

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
        cc_syncContext.setModelElement(SyncContext.ModelElement.CUSTOM_COLS);
        cc_syncContext.setAdmin(false);
        TableauDatasourceModel datasource3 = (TableauDatasourceModel) BISyncTool
                .dumpHasPermissionToBISyncModel(cc_syncContext, allAuthTables, newAuthColumns, dimensions, measures);
        ByteArrayOutputStream outStream3 = new ByteArrayOutputStream();
        datasource3.dump(outStream3);
        Assert.assertEquals(getExpectedTds("/bisync_tableau/nmodel_full_measure_test.connector_no_hierarchies.tds"),
                outStream3.toString(Charset.defaultCharset().name()));
    }

    private String getExpectedTds(String path) throws IOException {
        URL resource = getClass().getResource(path);
        String fullPath = Objects.requireNonNull(resource).getPath();
        return FileUtils.readFileToString(new File(fullPath), Charset.defaultCharset());
    }

    private void prepareBasic() {
        AclTCRManager manager = AclTCRManager.getInstance(getTestConfig(), getProject());

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

        AclTCR g1a1 = new AclTCR();
        AclTCR.Table g1t1 = new AclTCR.Table();
        AclTCR.ColumnRow g1cr1 = new AclTCR.ColumnRow();
        AclTCR.Column g1c1 = new AclTCR.Column();
        g1c1.addAll(Arrays.asList("ORDER_ID", "PRICE", "CAL_DT", "TRANS_ID"));
        g1cr1.setColumn(g1c1);
        g1t1.put("DEFAULT.TEST_MEASURE", g1cr1);
        g1a1.setTable(g1t1);
        manager.updateAclTCR(g1a1, "g1", false);
    }

    private void prepareBasicNoHierarchies() {
        AclTCRManager manager = AclTCRManager.getInstance(getTestConfig(), getProject());

        AclTCR u1a1 = new AclTCR();
        AclTCR.Table u1t1 = new AclTCR.Table();
        AclTCR.ColumnRow u1cr1 = new AclTCR.ColumnRow();
        AclTCR.Column u1c1 = new AclTCR.Column();
        u1c1.addAll(Arrays.asList("ORDER_ID", "PRICE", "CAL_DT", "PRICE", "ITEM_COUNT"));
        u1cr1.setColumn(u1c1);

        AclTCR.ColumnRow u1cr2 = new AclTCR.ColumnRow();
        AclTCR.Column u1c2 = new AclTCR.Column();
        u1c2.addAll(Arrays.asList("ORDER_ID", "TEST_TIME_ENC", "TEST_DATE_ENC"));
        u1cr2.setColumn(u1c2);
        u1t1.put("DEFAULT.TEST_KYLIN_FACT", u1cr1);
        u1t1.put("DEFAULT.TEST_ORDER", u1cr2);
        u1a1.setTable(u1t1);
        manager.updateAclTCR(u1a1, "u1", true);

        AclTCR g1a1 = new AclTCR();
        AclTCR.Table g1t1 = new AclTCR.Table();
        AclTCR.ColumnRow g1cr1 = new AclTCR.ColumnRow();
        AclTCR.Column g1c1 = new AclTCR.Column();
        g1c1.addAll(Arrays.asList("ORDER_ID", "PRICE", "CAL_DT", "TRANS_ID"));
        g1cr1.setColumn(g1c1);
        g1t1.put("DEFAULT.TEST_MEASURE", g1cr1);
        g1a1.setTable(g1t1);
        manager.updateAclTCR(g1a1, "g1", false);
    }

    @Test
    public void testRenameColumnName() {
        val project = "default";
        val modelId = "741ca86a-1f13-46da-a59f-95fb68615e3a";
        Set<String> columns = Sets.newHashSet();
        columns.add("DEFAULT.TEST_KYLIN_FACT.ORDER_ID");
        columns.add("TEST_KYLIN_FACT.TEST_TIME_ENC");
        val cc_syncContext = SyncModelTestUtil.createSyncContext(project, modelId, KylinConfig.getInstanceFromEnv());
        Set<String> set = new SyncModelBuilder(cc_syncContext).renameColumnName(columns);
        Set<String> expectColumns = Sets.newHashSet();
        expectColumns.add("TEST_KYLIN_FACT.ORDER_ID");
        expectColumns.add("TEST_KYLIN_FACT.TEST_TIME_ENC");
        Assert.assertEquals(set, expectColumns);
    }

    @Test
    public void testBuildSyncModelHavingCrossModelCC() throws Exception {
        overwriteSystemProp("kylin.query.metadata.expose-computed-column", "true");

        val project = "default";
        val model1Id = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val model2Id = "741ca86a-1f13-46da-a59f-95fb68615e3a";
        val syncContext1 = SyncModelTestUtil.createSyncContext(project, model1Id, KylinConfig.getInstanceFromEnv());
        val syncContext2 = SyncModelTestUtil.createSyncContext(project, model2Id, KylinConfig.getInstanceFromEnv());
        syncContext1.setModelElement(SyncContext.ModelElement.AGG_INDEX_AND_TABLE_INDEX_COL);
        syncContext1.setAdmin(true);
        syncContext2.setModelElement(SyncContext.ModelElement.AGG_INDEX_AND_TABLE_INDEX_COL);
        syncContext2.setAdmin(true);
        prepareBasic();

        TableauDatasourceModel datasource1 = (TableauDatasourceModel) BISyncTool.dumpToBISyncModel(syncContext1);
        ByteArrayOutputStream outStream1 = new ByteArrayOutputStream();
        datasource1.dump(outStream1);
        Assert.assertEquals(getExpectedTds("/bisync_tableau/nmodel_basic_all_cols.tds"),
                outStream1.toString(Charset.defaultCharset().name()));

        TableauDatasourceModel datasource2 = (TableauDatasourceModel) BISyncTool.dumpToBISyncModel(syncContext2);
        ByteArrayOutputStream outStream2 = new ByteArrayOutputStream();
        datasource2.dump(outStream2);
        Assert.assertEquals(getExpectedTds("/bisync_tableau/nmodel_basic_inner_all_cols.tds"),
                outStream2.toString(Charset.defaultCharset().name()));
    }
}

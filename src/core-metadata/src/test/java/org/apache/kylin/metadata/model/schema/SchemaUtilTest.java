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

package org.apache.kylin.metadata.model.schema;

import static org.apache.kylin.common.exception.ServerErrorCode.MODEL_METADATA_FILE_ERROR;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.model.ModelJoinRelationTypeEnum;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.guava20.shaded.common.collect.Sets;
import io.kyligence.kap.guava20.shaded.common.graph.Graph;
import io.kyligence.kap.guava20.shaded.common.io.ByteSource;
import lombok.val;

public class SchemaUtilTest extends NLocalFileMetadataTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() {
        this.createTestMetadata("src/test/resources/ut_meta/schema_utils/original_project");
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    public String getTargetProject() {
        return "original_project";
    }

    public String getTargetModel() {
        return "ssb_model";
    }

    @Test
    public void testDependencyGraph() {
        Graph<SchemaNode> graph = SchemaUtil.dependencyGraph("default", "DEFAULT.TEST_ORDER");
        Set<String> set = Sets.newHashSet();
        graph.nodes().stream().map(SchemaNode::getKey).forEach(set::add);
        Assert.assertTrue(set.contains("DEFAULT.TEST_ORDER"));
    }

    @Test
    public void testConflictDifferentFactTable() throws IOException {
        val file = new File(
                "src/test/resources/ut_meta/schema_utils/conflict_fact_table_project/conflict_fact_table_project_model_metadata_2020_11_17_07_38_50_ECFF797F0B088D53A2FA781ABA3BC111.zip");
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(file);
        String srcProject = getModelMetadataProjectName(rawResourceMap.keySet());
        val importModelContext = new ImportModelContext(getTargetProject(), srcProject, rawResourceMap);
        val difference = SchemaUtil.diff(getTargetProject(), KylinConfig.getInstanceFromEnv(),
                importModelContext.getTargetKylinConfig());

        val schemaChangeResponse = ModelImportChecker.check(difference, importModelContext);
        Assert.assertFalse(schemaChangeResponse.getModels().isEmpty());

        val modelSchemaChange = schemaChangeResponse.getModels().get(getTargetModel());
        Assert.assertEquals(64, modelSchemaChange.getDifferences());

        Assert.assertEquals(18, modelSchemaChange.getMissingItems().size());
        Assert.assertEquals(13, modelSchemaChange.getNewItems().size());
        Assert.assertEquals(20, modelSchemaChange.getUpdateItems().size());
        Assert.assertEquals(13, modelSchemaChange.getReduceItems().size());

        Assert.assertTrue(modelSchemaChange.getNewItems().stream()
                .anyMatch(schemaChange -> schemaChange.getType() == SchemaNodeType.MODEL_FACT
                        && schemaChange.isCreatable() && schemaChange.getDetail().equals("SSB.LINEORDER")));
        Assert.assertTrue(modelSchemaChange.getNewItems().stream()
                .anyMatch(schemaChange -> schemaChange.getType() == SchemaNodeType.MODEL_JOIN
                        && schemaChange.getDetail().equals("LINEORDER-CUSTOMER")));

        Assert.assertTrue(modelSchemaChange.getReduceItems().stream()
                .anyMatch(schemaChange -> schemaChange.getType() == SchemaNodeType.MODEL_FACT
                        && schemaChange.isCreatable() && schemaChange.getDetail().equals("SSB.P_LINEORDER")));

        Assert.assertTrue(modelSchemaChange.getReduceItems().stream()
                .anyMatch(schemaChange -> schemaChange.getType() == SchemaNodeType.MODEL_JOIN
                        && schemaChange.getDetail().equals("P_LINEORDER-CUSTOMER")));

    }

    @Test
    public void testConflictWithDifferentDimTable() throws IOException {
        val file = new File(
                "src/test/resources/ut_meta/schema_utils/conflict_dim_table_project/conflict_dim_table_project_model_metadata_2020_11_14_16_20_06_5BCDB43E43D8C8D9E94A90C396CDA23F.zip");
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(file);
        String srcProject = getModelMetadataProjectName(rawResourceMap.keySet());
        val importModelContext = new ImportModelContext(getTargetProject(), srcProject, rawResourceMap);
        val difference = SchemaUtil.diff(getTargetProject(), KylinConfig.getInstanceFromEnv(),
                importModelContext.getTargetKylinConfig());

        val schemaChangeResponse = ModelImportChecker.check(difference, importModelContext);
        Assert.assertFalse(schemaChangeResponse.getModels().isEmpty());

        val modelSchemaChange = schemaChangeResponse.getModels().get(getTargetModel());

        Assert.assertTrue(modelSchemaChange.getNewItems().stream().anyMatch(
                schemaChange -> schemaChange.getType() == SchemaNodeType.MODEL_DIM && !schemaChange.isOverwritable()));

        Assert.assertTrue(modelSchemaChange.getReduceItems().stream().anyMatch(
                schemaChange -> schemaChange.getType() == SchemaNodeType.MODEL_DIM && !schemaChange.isOverwritable()));

        Assert.assertTrue(modelSchemaChange.getNewItems().stream()
                .anyMatch(schemaChange -> schemaChange.getType() == SchemaNodeType.MODEL_JOIN
                        && schemaChange.getDetail().equals("P_LINEORDER-SUPPLIER")));

        Assert.assertTrue(modelSchemaChange.getReduceItems().stream()
                .anyMatch(schemaChange -> schemaChange.getType() == SchemaNodeType.MODEL_JOIN
                        && schemaChange.getDetail().equals("P_LINEORDER-CUSTOMER")));
    }

    @Test
    public void testConflictWithDifferentFilterCondition() throws IOException {
        val file = new File(
                "src/test/resources/ut_meta/schema_utils/conflict_filter_condition_project/conflict_filter_condition_project_model_metadata_2020_11_13_14_07_15_2AE3E159782BD88DF0445DE9F8B5101C.zip");
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(file);
        String srcProject = getModelMetadataProjectName(rawResourceMap.keySet());
        val importModelContext = new ImportModelContext(getTargetProject(), srcProject, rawResourceMap);
        val difference = SchemaUtil.diff(getTargetProject(), KylinConfig.getInstanceFromEnv(),
                importModelContext.getTargetKylinConfig());

        val schemaChangeResponse = ModelImportChecker.check(difference, importModelContext);
        Assert.assertFalse(schemaChangeResponse.getModels().isEmpty());

        val modelSchemaChange = schemaChangeResponse.getModels().get(getTargetModel());

        Assert.assertTrue(modelSchemaChange.getUpdateItems().stream()
                .anyMatch(pair -> pair.getType() == SchemaNodeType.MODEL_FILTER && !pair.isOverwritable()));
    }

    @Test
    public void testConflictWithDifferentJoinCondition() throws IOException {
        val file = new File(
                "src/test/resources/ut_meta/schema_utils/conflict_join_condition_project/conflict_join_condition_project_model_metadata_2020_11_16_02_17_56_FB6573FA0363D0850D6807ABF0BCE060.zip");
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(file);
        String srcProject = getModelMetadataProjectName(rawResourceMap.keySet());
        val importModelContext = new ImportModelContext(getTargetProject(), srcProject, rawResourceMap);
        val difference = SchemaUtil.diff(getTargetProject(), KylinConfig.getInstanceFromEnv(),
                importModelContext.getTargetKylinConfig());

        val schemaChangeResponse = ModelImportChecker.check(difference, importModelContext);
        Assert.assertFalse(schemaChangeResponse.getModels().isEmpty());

        val modelSchemaChange = schemaChangeResponse.getModels().get(getTargetModel());

        Assert.assertEquals(1, modelSchemaChange.getUpdateItems().size());
        Assert.assertEquals(0, modelSchemaChange.getMissingItems().size());
        Assert.assertEquals(0, modelSchemaChange.getNewItems().size());
        Assert.assertEquals(0, modelSchemaChange.getReduceItems().size());

        Assert.assertTrue(modelSchemaChange.getUpdateItems().stream()
                .anyMatch(pair -> pair.getType() == SchemaNodeType.MODEL_JOIN
                        && pair.getFirstSchemaNode().getDetail().equals("P_LINEORDER-CUSTOMER")
                        && pair.getFirstSchemaNode().getAttributes().get("join_type").equals("INNER")
                        && pair.getFirstSchemaNode().getAttributes().get("primary_keys")
                                .equals(Collections.singletonList("CUSTOMER.C_CUSTKEY"))
                        && pair.getFirstSchemaNode().getAttributes().get("foreign_keys")
                                .equals(Collections.singletonList("P_LINEORDER.LO_CUSTKEY"))
                        && pair.getFirstSchemaNode().getAttributes().get("non_equal_join_condition").equals("")
                        && pair.getSecondSchemaNode().getDetail().equals("P_LINEORDER-CUSTOMER")
                        && pair.getSecondSchemaNode().getAttributes().get("join_type").equals("LEFT")
                        && pair.getSecondSchemaNode().getAttributes().get("primary_keys")
                                .equals(Collections.singletonList("CUSTOMER.C_CUSTKEY"))
                        && pair.getSecondSchemaNode().getAttributes().get("foreign_keys")
                                .equals(Collections.singletonList("P_LINEORDER.LO_CUSTKEY"))
                        && pair.getSecondSchemaNode().getAttributes().get("non_equal_join_condition").equals("")
                        && !pair.isOverwritable()));
    }

    @Test
    public void testConflictWithDifferentJoinRelationType() throws IOException {
        val file = new File(
                "src/test/resources/ut_meta/schema_utils/conflict_join_relation_type_project/target_project_model_metadata_2020_12_07_14_26_55_89173666F1E93F7988011666659D95AD.zip");
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(file);
        String srcProject = getModelMetadataProjectName(rawResourceMap.keySet());
        val importModelContext = new ImportModelContext(getTargetProject(), srcProject, rawResourceMap);
        val difference = SchemaUtil.diff(getTargetProject(), KylinConfig.getInstanceFromEnv(),
                importModelContext.getTargetKylinConfig());

        val schemaChangeResponse = ModelImportChecker.check(difference, importModelContext);
        Assert.assertFalse(schemaChangeResponse.getModels().isEmpty());

        val modelSchemaChange = schemaChangeResponse.getModels().get(getTargetModel());

        Assert.assertEquals(1, modelSchemaChange.getDifferences());
        Assert.assertEquals(1, modelSchemaChange.getUpdateItems().size());

        Assert.assertTrue(modelSchemaChange.getUpdateItems().stream().anyMatch(updatedItem -> {
            Assert.assertEquals(ModelJoinRelationTypeEnum.MANY_TO_ONE,
                    updatedItem.getFirstAttributes().get("join_relation_type"));
            Assert.assertEquals(ModelJoinRelationTypeEnum.MANY_TO_MANY,
                    updatedItem.getSecondAttributes().get("join_relation_type"));
            return !updatedItem.isOverwritable() && updatedItem.isCreatable();
        }));
    }

    @Test
    public void testConflictWithDifferentPartition() throws IOException {
        val file = new File(
                "src/test/resources/ut_meta/schema_utils/conflict_partition_col_project/conflict_partition_col_project_model_metadata_2020_11_14_17_09_51_98DA15B726CE71B8FACA563708B8F4E5.zip");
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(file);
        String srcProject = getModelMetadataProjectName(rawResourceMap.keySet());
        val importModelContext = new ImportModelContext(getTargetProject(), srcProject, rawResourceMap);
        val difference = SchemaUtil.diff(getTargetProject(), KylinConfig.getInstanceFromEnv(),
                importModelContext.getTargetKylinConfig());

        val schemaChangeResponse = ModelImportChecker.check(difference, importModelContext);
        Assert.assertFalse(schemaChangeResponse.getModels().isEmpty());

        val modelSchemaChange = schemaChangeResponse.getModels().get(getTargetModel());

        Assert.assertTrue(modelSchemaChange.getUpdateItems().stream()
                .anyMatch(pair -> pair.getType() == SchemaNodeType.MODEL_PARTITION && !pair.isOverwritable()));
    }

    @Test
    public void testConflictWithModelColumnUpdate() throws IOException {
        val file = new File(
                "src/test/resources/ut_meta/schema_utils/model_column_update/model_column_update_model_metadata_2020_11_14_17_11_19_77E61B40A0A3C2D1E1DE54E6982C98F5.zip");
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(file);
        String srcProject = getModelMetadataProjectName(rawResourceMap.keySet());
        val importModelContext = new ImportModelContext(getTargetProject(), srcProject, rawResourceMap);
        val difference = SchemaUtil.diff(getTargetProject(), KylinConfig.getInstanceFromEnv(),
                importModelContext.getTargetKylinConfig());

        val schemaChangeResponse = ModelImportChecker.check(difference, importModelContext);
        Assert.assertFalse(schemaChangeResponse.getModels().isEmpty());

        val modelSchemaChange = schemaChangeResponse.getModels().get(getTargetModel());

        Assert.assertTrue(modelSchemaChange.getReduceItems().stream()
                .anyMatch(schemaChange -> schemaChange.getType() == SchemaNodeType.MODEL_DIMENSION
                        && schemaChange.isOverwritable()));

        Assert.assertTrue(modelSchemaChange.getReduceItems().stream()
                .anyMatch(schemaChange -> schemaChange.getType() == SchemaNodeType.RULE_BASED_INDEX
                        && schemaChange.isOverwritable()));

        Assert.assertTrue(modelSchemaChange.getNewItems().stream()
                .anyMatch(schemaChange -> schemaChange.getType() == SchemaNodeType.MODEL_DIMENSION
                        && schemaChange.isOverwritable()));

        Assert.assertTrue(modelSchemaChange.getNewItems().stream()
                .anyMatch(schemaChange -> schemaChange.getType() == SchemaNodeType.RULE_BASED_INDEX
                        && schemaChange.isOverwritable()));

    }

    @Test
    public void testConflictWithModelCCUpdate() throws IOException {
        val file = new File(
                "src/test/resources/ut_meta/schema_utils/model_cc_update/model_cc_update_model_metadata_2020_11_14_17_15_29_FAAAF2F8FA213F2888D3380720C28B4D.zip");
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(file);
        String srcProject = getModelMetadataProjectName(rawResourceMap.keySet());
        val importModelContext = new ImportModelContext(getTargetProject(), srcProject, rawResourceMap);
        val difference = SchemaUtil.diff(getTargetProject(), KylinConfig.getInstanceFromEnv(),
                importModelContext.getTargetKylinConfig());

        val schemaChangeResponse = ModelImportChecker.check(difference, importModelContext);
        Assert.assertFalse(schemaChangeResponse.getModels().isEmpty());

        val modelSchemaChange = schemaChangeResponse.getModels().get(getTargetModel());

        Assert.assertEquals(2, modelSchemaChange.getDifferences());

        Assert.assertTrue(modelSchemaChange.getNewItems().stream()
                .anyMatch(schemaChange -> schemaChange.getType() == SchemaNodeType.MODEL_CC
                        && schemaChange.isOverwritable() && schemaChange.getDetail().equals("CC2")
                        && schemaChange.getAttributes().get("expression").equals("P_LINEORDER.LO_SUPPKEY + 1")));

        Assert.assertTrue(modelSchemaChange.getUpdateItems().stream()
                .anyMatch(pair -> pair.getType() == SchemaNodeType.MODEL_CC
                        && pair.getFirstSchemaNode().getDetail().equals("CC1")
                        && pair.getFirstSchemaNode().getAttributes().get("expression")
                                .equals("P_LINEORDER.LO_CUSTKEY + 1")
                        && !pair.isImportable() && pair.getSecondSchemaNode().getDetail().equals("CC1")
                        && pair.getSecondSchemaNode().getAttributes().get("expression")
                                .equals("P_LINEORDER.LO_CUSTKEY + 2")));

    }

    @Test
    public void testConflictWithSameExpressionAndDifferentNameUpdate() throws IOException {
        val file = new File(
                "src/test/resources/ut_meta/schema_utils/model_cc_update/model_cc_same_expression_different_name_update_model_metadata_2020_11_14_17_15_29_850F781B4B5B5C7A480F5834C6644FCB.zip");
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(file);
        String srcProject = getModelMetadataProjectName(rawResourceMap.keySet());
        val importModelContext = new ImportModelContext(getTargetProject(), srcProject, rawResourceMap);
        val difference = SchemaUtil.diff(getTargetProject(), KylinConfig.getInstanceFromEnv(),
                importModelContext.getTargetKylinConfig());

        val schemaChangeResponse = ModelImportChecker.check(difference, importModelContext);
        Assert.assertFalse(schemaChangeResponse.getModels().isEmpty());

        val modelSchemaChange = schemaChangeResponse.getModels().get(getTargetModel());

        Assert.assertEquals(2, modelSchemaChange.getDifferences());

        Assert.assertTrue(modelSchemaChange.getNewItems().stream()
                .anyMatch(pair -> pair.getType() == SchemaNodeType.MODEL_CC && pair.getDetail().equals("CC2")
                        && pair.getAttributes().get("expression").equals("P_LINEORDER.LO_CUSTKEY + 1")
                        && !pair.isImportable()
                        && pair.getConflictReason()
                                .getReason() == SchemaChangeCheckResult.UN_IMPORT_REASON.DIFFERENT_CC_NAME_HAS_SAME_EXPR
                        && pair.getConflictReason().getConflictItem().equals("CC1")));
    }

    @Test
    public void testModelCCInDifferentFactTableUpdate() throws IOException {
        val file = new File(
                "src/test/resources/ut_meta/schema_utils/model_cc_in_different_fact_table/target_project_model_metadata_2020_12_07_15_37_19_1C3C399B52A4E4E4F07C017C9692A7CB.zip");
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(file);
        String srcProject = getModelMetadataProjectName(rawResourceMap.keySet());
        val importModelContext = new ImportModelContext(getTargetProject(), srcProject, rawResourceMap);
        val difference = SchemaUtil.diff(getTargetProject(), KylinConfig.getInstanceFromEnv(),
                importModelContext.getTargetKylinConfig());

        val schemaChangeResponse = ModelImportChecker.check(difference, importModelContext);
        Assert.assertFalse(schemaChangeResponse.getModels().isEmpty());

        val modelSchemaChange = schemaChangeResponse.getModels().get(getTargetModel());

        Assert.assertTrue(modelSchemaChange.getUpdateItems().stream()
                .anyMatch(updatedItem -> updatedItem.getType() == SchemaNodeType.MODEL_CC
                        && updatedItem.getFirstAttributes().get("fact_table").equals("SSB.P_LINEORDER")
                        && updatedItem.getSecondAttributes().get("fact_table").equals("SSB.LINEORDER")
                        && updatedItem.getFirstAttributes().get("expression").equals("P_LINEORDER.LO_CUSTKEY + 1")
                        && updatedItem.getSecondAttributes().get("expression").equals("LINEORDER.LO_CUSTKEY + 1")
                        && updatedItem.isOverwritable()));
    }

    @Test
    public void testConflictWithModelAggUpdate() throws IOException {
        val file = new File(
                "src/test/resources/ut_meta/schema_utils/model_agg_update/model_agg_update_model_metadata_2020_11_14_17_16_54_CF515DB4597CBE5DAE244A6EBF5FF5F3.zip");
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(file);
        String srcProject = getModelMetadataProjectName(rawResourceMap.keySet());
        val importModelContext = new ImportModelContext(getTargetProject(), srcProject, rawResourceMap);
        val difference = SchemaUtil.diff(getTargetProject(), KylinConfig.getInstanceFromEnv(),
                importModelContext.getTargetKylinConfig());

        val schemaChangeResponse = ModelImportChecker.check(difference, importModelContext);
        Assert.assertFalse(schemaChangeResponse.getModels().isEmpty());

        val modelSchemaChange = schemaChangeResponse.getModels().get(getTargetModel());

        Assert.assertEquals(4, modelSchemaChange.getDifferences());

        Assert.assertEquals(4, modelSchemaChange.getReduceItems().size());
        Assert.assertEquals("10001,40001,60001,70001", modelSchemaChange.getReduceItems().stream()
                .map(SchemaChangeCheckResult.ChangedItem::getDetail).sorted().collect(Collectors.joining(",")));
    }

    @Test
    public void testConflictWithModelTableIndexUpdate() throws IOException {
        val file = new File(
                "src/test/resources/ut_meta/schema_utils/model_index_update/model_index_project_model_metadata_2020_11_16_02_37_33_1B8D602879F14297E132978D784C46EA.zip");
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(file);
        String srcProject = getModelMetadataProjectName(rawResourceMap.keySet());
        val importModelContext = new ImportModelContext(getTargetProject(), srcProject, rawResourceMap);
        val difference = SchemaUtil.diff(getTargetProject(), KylinConfig.getInstanceFromEnv(),
                importModelContext.getTargetKylinConfig());

        val schemaChangeResponse = ModelImportChecker.check(difference, importModelContext);
        Assert.assertFalse(schemaChangeResponse.getModels().isEmpty());

        val modelSchemaChange = schemaChangeResponse.getModels().get(getTargetModel());

        Assert.assertEquals(3, modelSchemaChange.getDifferences());

        Assert.assertEquals(0, modelSchemaChange.getMissingItems().size());
        Assert.assertEquals(1, modelSchemaChange.getReduceItems().size());

        Assert.assertTrue(modelSchemaChange.getReduceItems().stream()
                .filter(schemaChange -> schemaChange.getType() == SchemaNodeType.WHITE_LIST_INDEX)
                .filter(schemaChange -> schemaChange.getDetail().equals("20000000001"))
                .filter(SchemaChangeCheckResult.BaseItem::isOverwritable).anyMatch(schemaChange -> {
                    String col_orders = String.join(",",
                            ((ArrayList<String>) schemaChange.getAttributes().get("col_orders")));
                    return col_orders.equals(
                            "P_LINEORDER.LO_CUSTKEY,P_LINEORDER.LO_SUPPKEY,P_LINEORDER.LO_ORDERDATE,P_LINEORDER.LO_QUANTITY,P_LINEORDER.LO_DISCOUNT,P_LINEORDER.LO_LINENUMBER,P_LINEORDER.LO_PARTKEY,P_LINEORDER.LO_ORDERKEY");
                }));

        Assert.assertEquals(2, modelSchemaChange.getNewItems().size());
        Assert.assertEquals(0, modelSchemaChange.getUpdateItems().size());

        Assert.assertTrue(modelSchemaChange.getNewItems().stream()
                .filter(schemaChange -> schemaChange.getType() == SchemaNodeType.WHITE_LIST_INDEX)
                .filter(schemaChange -> schemaChange.getDetail().equals("20000000001"))
                .filter(SchemaChangeCheckResult.BaseItem::isOverwritable).anyMatch(schemaChange -> {
                    String col_orders = String.join(",",
                            ((ArrayList<String>) schemaChange.getAttributes().get("col_orders")));
                    return col_orders.equals(
                            "P_LINEORDER.LO_LINENUMBER,P_LINEORDER.LO_SUPPKEY,P_LINEORDER.LO_QUANTITY,P_LINEORDER.LO_PARTKEY,P_LINEORDER.LO_ORDERKEY,P_LINEORDER.LO_CUSTKEY,P_LINEORDER.LO_DISCOUNT,P_LINEORDER.LO_ORDERDATE");
                }));

        Assert.assertTrue(modelSchemaChange.getNewItems().stream()
                .filter(schemaChange -> schemaChange.getType() == SchemaNodeType.WHITE_LIST_INDEX)
                .filter(schemaChange -> schemaChange.getDetail().equals("20000010001"))
                .filter(SchemaChangeCheckResult.BaseItem::isOverwritable).anyMatch(schemaChange -> {
                    String col_orders = String.join(",",
                            ((ArrayList<String>) schemaChange.getAttributes().get("col_orders")));
                    return col_orders.equals("P_LINEORDER.LO_SUPPKEY,P_LINEORDER.LO_QUANTITY");
                }));
    }

    @Test
    public void testIndexWithShardByDiff() throws IOException {
        val file = new File(
                "src/test/resources/ut_meta/schema_utils/model_index_update/model_index_project_model_metadata_2020_11_16_02_37_33_1B8D602879F14297E132978D784C46EA.zip");
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(file);
        String key = "/model_index_project/index_plan/10d5eb7c-d854-4f72-9e4b-9b1f3c65bcda.json";
        String value = "{\"uuid\":\"10d5eb7c-d854-4f72-9e4b-9b1f3c65bcda\",\"last_modified\":1605494236502,\"create_time\":1605494168935,\"version\":\"4.0.0.0\","
                + "\"description\":null,\"rule_based_index\":{\"dimensions\":[1,4,5,6,7,8,10,12],\"measures\":[100000,100001,100002,100003,100004,100005,100006,100007,"
                + "100008,100009,100010,100011],\"global_dim_cap\":null,\"aggregation_groups\":[{\"includes\":[1,4,5,6,7,8,10,12],\"measures\":[100011,100000,100001,100002,"
                + "100003,100004,100005,100006,100007,100008,100009,100010],\"select_rule\":{\"hierarchy_dims\":[],\"mandatory_dims\":[12],\"joint_dims\":[[4,5,6,7,8]]}}],"
                + "\"layout_id_mapping\":[1,10001,20001,30001,40001,50001,60001,70001],\"parent_forward\":3,\"index_start_id\":0,\"last_modify_time\":1605494223951,"
                + "\"layout_black_list\":[],\"scheduler_version\":2},\"indexes\":[{\"id\":20000000000,\"dimensions\":[1,4,5,6,7,8,10,12],\"measures\":[],\"layouts\":"
                + "[{\"id\":20000000001,\"name\":null,\"owner\":\"ADMIN\",\"col_order\":[1,4,5,6,7,8,10,12],\"shard_by_columns\":[4],\"partition_by_columns\":[],"
                + "\"sort_by_columns\":[],\"storage_type\":20,\"update_time\":1605494229727,\"manual\":true,\"auto\":false,\"draft_version\":null},{\"id\":20000000002,"
                + "\"name\":null,\"owner\":\"ADMIN\",\"col_order\":[1,4,5,6,7,8,10,12],\"shard_by_columns\":[],\"partition_by_columns\":[],\"sort_by_columns\":[],"
                + "\"storage_type\":20,\"update_time\":1605494229727,\"manual\":true,\"auto\":false,\"draft_version\":null}],\"next_layout_offset\":3},{\"id\":20000010000,"
                + "\"dimensions\":[4,5],\"measures\":[],\"layouts\":[{\"id\":20000010001,\"name\":null,\"owner\":\"ADMIN\",\"col_order\":[4,5],\"shard_by_columns\":[],"
                + "\"partition_by_columns\":[],\"sort_by_columns\":[4,5],\"storage_type\":20,\"update_time\":1605494236499,\"manual\":true,\"auto\":false,\"draft_version\":null}"
                + "],\"next_layout_offset\":2}],\"override_properties\":{},\"to_be_deleted_indexes\":[],\"auto_merge_time_ranges\":null,\"retention_range\":0,\"engine_type\":80,"
                + "\"next_aggregation_index_id\":80000,\"next_table_index_id\":20000020000,\"agg_shard_by_columns\":[],\"extend_partition_columns\":[],\"layout_bucket_num\":{}}";

        val rawResource = rawResourceMap.get(key);
        val update = new RawResource(rawResource.getResPath(), ByteSource.wrap(value.getBytes()),
                rawResource.getTimestamp(), rawResource.getMvcc());
        rawResourceMap.put(key, update);
        String srcProject = getModelMetadataProjectName(rawResourceMap.keySet());
        val importModelContext = new ImportModelContext(getTargetProject(), srcProject, rawResourceMap);
        importModelContext.getTargetKylinConfig();
        boolean result = true;
        try {
            SchemaUtil.diff(getTargetProject(), KylinConfig.getInstanceFromEnv(),
                    importModelContext.getTargetKylinConfig());
        } catch (Exception e) {
            e.printStackTrace();
            result = false;
        }
        Assert.assertTrue(result);
    }

    @Test
    public void testModelWithDifferentColumnMeasureIdUpdate() throws IOException {
        val file = new File(
                "src/test/resources/ut_meta/schema_utils/model_different_column_measure_id_update/original_project_model_metadata_2020_11_14_15_24_56_4B2101A84E908397A8E711864FC8ADF2.zip");
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(file);
        String srcProject = getModelMetadataProjectName(rawResourceMap.keySet());
        val importModelContext = new ImportModelContext(getTargetProject(), srcProject, rawResourceMap);
        val difference = SchemaUtil.diff(getTargetProject(), KylinConfig.getInstanceFromEnv(),
                importModelContext.getTargetKylinConfig());

        val schemaChangeResponse = ModelImportChecker.check(difference, importModelContext);
        Assert.assertFalse(schemaChangeResponse.getModels().isEmpty());

        val modelSchemaChange = schemaChangeResponse.getModels().get(getTargetModel());

        Assert.assertEquals(0, modelSchemaChange.getDifferences());
    }

    @Test
    public void testConflictModelWithDifferentColumnDataTypeUpdate() throws IOException {
        val file = new File(
                "src/test/resources/ut_meta/schema_utils/model_different_column_datatype_update/original_project_datatype_model_metadata_2020_11_14_15_24_56_9BCCCB5D08F218EC86DD9D850017F5F5.zip");
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(file);
        String srcProject = getModelMetadataProjectName(rawResourceMap.keySet());
        val importModelContext = new ImportModelContext(getTargetProject(), srcProject, rawResourceMap);
        val difference = SchemaUtil.diff(getTargetProject(), KylinConfig.getInstanceFromEnv(),
                importModelContext.getTargetKylinConfig());

        val schemaChangeResponse = ModelImportChecker.check(difference, importModelContext);
        Assert.assertFalse(schemaChangeResponse.getModels().isEmpty());

        val modelSchemaChange = schemaChangeResponse.getModels().get(getTargetModel());

        Assert.assertEquals(1, modelSchemaChange.getDifferences());
        Assert.assertTrue(modelSchemaChange.getUpdateItems().stream()
                .anyMatch(pair -> pair.getType() == SchemaNodeType.TABLE_COLUMN && !pair.isImportable()
                        && pair.getFirstDetail().equals("SSB.CUSTOMER.C_CUSTKEY")
                        && pair.getFirstSchemaNode().getAttributes().get("datatype").equals("integer")
                        && pair.getSecondSchemaNode().getAttributes().get("datatype").equals("varchar(4096)")
                        && pair.getConflictReason()
                                .getReason() == SchemaChangeCheckResult.UN_IMPORT_REASON.TABLE_COLUMN_DATATYPE_CHANGED
                        && pair.getConflictReason().getConflictItem().equals("C_CUSTKEY")));
    }

    @Test
    public void testConflictModelWithMissingTable() throws IOException {
        val file = new File(
                "src/test/resources/ut_meta/schema_utils/model_missing_table_update/model_table_missing_update_model_metadata_2020_11_16_02_37_33_3182D4A7694DA64E3D725C140CF80A47.zip");
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(file);
        String srcProject = getModelMetadataProjectName(rawResourceMap.keySet());
        val importModelContext = new ImportModelContext(getTargetProject(), srcProject, rawResourceMap);
        val difference = SchemaUtil.diff(getTargetProject(), KylinConfig.getInstanceFromEnv(),
                importModelContext.getTargetKylinConfig());

        val schemaChangeResponse = ModelImportChecker.check(difference, importModelContext);
        Assert.assertFalse(schemaChangeResponse.getModels().isEmpty());

        val modelSchemaChange = schemaChangeResponse.getModels().get(getTargetModel());

        Assert.assertEquals(11, modelSchemaChange.getDifferences());
        Assert.assertTrue(modelSchemaChange.getMissingItems().stream()
                .anyMatch(schemaChange -> schemaChange.getType() == SchemaNodeType.MODEL_TABLE
                        && !schemaChange.isImportable() && schemaChange.getDetail().equals("SSB.CUSTOMER_NEW")
                        && schemaChange.getConflictReason()
                                .getReason() == SchemaChangeCheckResult.UN_IMPORT_REASON.MISSING_TABLE
                        && schemaChange.getConflictReason().getConflictItem().equals("SSB.CUSTOMER_NEW")));

        NTableMetadataManager manager = NTableMetadataManager.getInstance(getTestConfig(), getTargetProject());
        TableDesc tableDesc = manager.getTableDesc("SSB.CUSTOMER");
        tableDesc.setName("CUSTOMER_NEW");
        tableDesc.init(getTargetProject());
        val diff = SchemaUtil.diff(getTargetProject(), KylinConfig.getInstanceFromEnv(),
                importModelContext.getTargetKylinConfig(), Lists.newArrayList(tableDesc));
        val checkResult = ModelImportChecker.check(diff, importModelContext);
        Assert.assertFalse(checkResult.getModels().isEmpty());
        val change = checkResult.getModels().get(getTargetModel());
        Assert.assertTrue(change.getMissingItems().isEmpty());
    }

    @Test
    public void testConflictMultiplePartitionModel() throws IOException {
        val file = new File(
                "src/test/resources/ut_meta/schema_utils/conflict_multiple_partition_project/target_project_model_metadata_2020_12_02_17_27_25_F5A5FC2CC8452A2D55384F97D90C8CCE.zip");
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(file);
        String srcProject = getModelMetadataProjectName(rawResourceMap.keySet());
        val importModelContext = new ImportModelContext(getTargetProject(), srcProject, rawResourceMap);
        val difference = SchemaUtil.diff(getTargetProject(), KylinConfig.getInstanceFromEnv(),
                importModelContext.getTargetKylinConfig());

        val schemaChangeResponse = ModelImportChecker.check(difference, importModelContext);
        Assert.assertFalse(schemaChangeResponse.getModels().isEmpty());

        val modelSchemaChange = schemaChangeResponse.getModels().get("conflict_multiple_partition_col_model");

        Assert.assertEquals(1, modelSchemaChange.getDifferences());
        Assert.assertTrue(modelSchemaChange.getUpdateItems().stream()
                .anyMatch(updatedItem -> !updatedItem.isOverwritable()
                        && updatedItem.getFirstDetail().equals("P_LINEORDER.LO_CUSTKEY")
                        && updatedItem.getSecondDetail().equals("P_LINEORDER.LO_PARTKEY")
                        && String.join(",", (List<String>) updatedItem.getFirstAttributes().get("columns"))
                                .equals("P_LINEORDER.LO_CUSTKEY")
                        && String.join(",", (List<String>) updatedItem.getSecondAttributes().get("columns"))
                                .equals("P_LINEORDER.LO_PARTKEY")
                        && ((List<String>) updatedItem.getFirstAttributes().get("partitions")).size() == 2
                        && ((List<String>) updatedItem.getSecondAttributes().get("partitions")).size() == 3));
    }

    @Test
    public void testMultiplePartitionModel() throws IOException {
        val file = new File(
                "src/test/resources/ut_meta/schema_utils/model_different_multiple_partition_project/target_project_model_metadata_2020_12_02_20_50_10_F85294019F1CE7DB159D6C264B672472.zip");
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(file);
        String srcProject = getModelMetadataProjectName(rawResourceMap.keySet());
        val importModelContext = new ImportModelContext(getTargetProject(), srcProject, rawResourceMap);
        val difference = SchemaUtil.diff(getTargetProject(), KylinConfig.getInstanceFromEnv(),
                importModelContext.getTargetKylinConfig());

        val schemaChangeResponse = ModelImportChecker.check(difference, importModelContext);
        Assert.assertFalse(schemaChangeResponse.getModels().isEmpty());

        val modelSchemaChange = schemaChangeResponse.getModels().get("conflict_multiple_partition_col_model");

        Assert.assertEquals(1, modelSchemaChange.getDifferences());
        Assert.assertTrue(modelSchemaChange.getUpdateItems().stream()
                .anyMatch(updatedItem -> updatedItem.isOverwritable()
                        && updatedItem.getFirstDetail().equals("P_LINEORDER.LO_CUSTKEY")
                        && updatedItem.getSecondDetail().equals("P_LINEORDER.LO_CUSTKEY")
                        && String.join(",", (List<String>) updatedItem.getFirstAttributes().get("columns"))
                                .equals("P_LINEORDER.LO_CUSTKEY")
                        && String.join(",", (List<String>) updatedItem.getSecondAttributes().get("columns"))
                                .equals("P_LINEORDER.LO_CUSTKEY")
                        && ((List<String>) updatedItem.getFirstAttributes().get("partitions")).size() == 2
                        && ((List<String>) updatedItem.getSecondAttributes().get("partitions")).size() == 3));
    }

    @Test
    public void testNewModel() throws IOException {
        val file = new File(
                "src/test/resources/ut_meta/schema_utils/model_create/model_create_model_metadata_2020_11_14_17_11_19_B6A82E50A2B4A7EE5CD606F01045CA84.zip");
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(file);
        String srcProject = getModelMetadataProjectName(rawResourceMap.keySet());
        val importModelContext = new ImportModelContext(getTargetProject(), srcProject, rawResourceMap);
        val difference = SchemaUtil.diff(getTargetProject(), KylinConfig.getInstanceFromEnv(),
                importModelContext.getTargetKylinConfig());

        val schemaChangeResponse = ModelImportChecker.check(difference, importModelContext);
        Assert.assertFalse(schemaChangeResponse.getModels().isEmpty());

        val modelSchemaChange = schemaChangeResponse.getModels().get("ssb_model_new");

        Assert.assertEquals(35, modelSchemaChange.getDifferences());
        Assert.assertEquals(35, modelSchemaChange.getNewItems().size());
        Assert.assertTrue(modelSchemaChange.creatable());
        Assert.assertTrue(modelSchemaChange.importable());
        Assert.assertFalse(modelSchemaChange.overwritable());

        Assert.assertTrue(modelSchemaChange.getNewItems().stream()
                .anyMatch(schemaChange -> schemaChange.getType() == SchemaNodeType.MODEL_FACT
                        && schemaChange.isCreatable() && schemaChange.getDetail().equals("SSB.P_LINEORDER")));

        Assert.assertTrue(modelSchemaChange.getNewItems().stream()
                .anyMatch(schemaChange -> schemaChange.getType() == SchemaNodeType.MODEL_DIM
                        && schemaChange.isCreatable() && schemaChange.getDetail().equals("SSB.CUSTOMER")));

        Assert.assertTrue(modelSchemaChange.getNewItems().stream()
                .anyMatch(schemaChange -> schemaChange.getType() == SchemaNodeType.MODEL_JOIN
                        && schemaChange.isCreatable() && schemaChange.getDetail().equals("P_LINEORDER-CUSTOMER")));

        Assert.assertTrue(modelSchemaChange.getNewItems().stream()
                .anyMatch(schemaChange -> schemaChange.getType() == SchemaNodeType.MODEL_FILTER
                        && schemaChange.isCreatable()
                        && schemaChange.getDetail().equals("(P_LINEORDER.LO_CUSTKEY <> 1)")));

        Assert.assertTrue(modelSchemaChange.getNewItems().stream()
                .anyMatch(schemaChange -> schemaChange.getType() == SchemaNodeType.MODEL_PARTITION
                        && schemaChange.isCreatable() && schemaChange.getDetail().equals("P_LINEORDER.LO_ORDERDATE")));

        Assert.assertEquals(1, modelSchemaChange.getNewItems().stream()
                .filter(schemaChange -> schemaChange.getType() == SchemaNodeType.MODEL_CC).count());

        Assert.assertEquals(8, modelSchemaChange.getNewItems().stream()
                .filter(schemaChange -> schemaChange.getType() == SchemaNodeType.MODEL_DIMENSION).count());

        Assert.assertEquals(12, modelSchemaChange.getNewItems().stream()
                .filter(schemaChange -> schemaChange.getType() == SchemaNodeType.MODEL_MEASURE).count());

        Assert.assertEquals(8, modelSchemaChange.getNewItems().stream()
                .filter(schemaChange -> schemaChange.getType() == SchemaNodeType.RULE_BASED_INDEX).count());

        Assert.assertEquals(1, modelSchemaChange.getNewItems().stream()
                .filter(schemaChange -> schemaChange.getType() == SchemaNodeType.WHITE_LIST_INDEX).count());

        Assert.assertTrue(modelSchemaChange.getNewItems().stream()
                .anyMatch(schemaChange -> schemaChange.getType() == SchemaNodeType.MODEL_MEASURE
                        && Objects.equals(schemaChange.getAttributes().get("expression"), "COUNT")
                        && ((List<SchemaNode.FunctionParameter>) schemaChange.getAttributes().get("parameters"))
                                .stream().anyMatch(
                                        functionParameter -> Objects.equals(functionParameter.getType(), "constant"))));
    }

    @Test
    public void testExceptions() throws IOException {
        val file = new File(
                "src/test/resources/ut_meta/schema_utils/model_exception/model_exception_model_metadata_2020_11_19_15_13_11_4B823934CF76C0A124244456539C9296.zip");
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(file);
        String srcProject = getModelMetadataProjectName(rawResourceMap.keySet());
        thrown.expect(KylinException.class);
        thrown.expectMessage("Can’t import the model.\n" + "Model [none_exists_column] is broken, can not export.\n"
                + "Model [illegal_index] is broken, can not export.\n"
                + "Model [none_exists_table] is broken, can not export.");
        new ImportModelContext(getTargetProject(), srcProject, rawResourceMap);
    }

    static Map<String, RawResource> getRawResourceFromUploadFile(File uploadFile) throws IOException {
        Map<String, RawResource> rawResourceMap = Maps.newHashMap();
        try (ZipInputStream zipInputStream = new ZipInputStream(new FileInputStream(uploadFile))) {
            ZipEntry zipEntry;
            while ((zipEntry = zipInputStream.getNextEntry()) != null) {
                val bs = ByteSource.wrap(IOUtils.toByteArray(zipInputStream));
                long t = zipEntry.getTime();
                String resPath = StringUtils.prependIfMissing(zipEntry.getName(), "/");
                if (!resPath.startsWith(ResourceStore.METASTORE_UUID_TAG) && !resPath.endsWith(".json")) {
                    continue;
                }
                rawResourceMap.put(resPath, new RawResource(resPath, bs, t, 0));
            }
            return rawResourceMap;
        }
    }

    static String getModelMetadataProjectName(Set<String> rawResourceList) {
        String anyPath = rawResourceList.stream().filter(
                resourcePath -> resourcePath.indexOf(File.separator) != resourcePath.lastIndexOf(File.separator))
                .findAny().orElse(null);
        if (StringUtils.isBlank(anyPath)) {
            throw new KylinException(MODEL_METADATA_FILE_ERROR, MsgPicker.getMsg().getModelMetadataPackageInvalid());
        }
        return anyPath.split(File.separator)[1];
    }
}

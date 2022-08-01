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

package org.apache.kylin.metadata.model;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.hystrix.NCircuitBreaker;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.NDataModel.Measure;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.google.common.collect.Lists;

import lombok.val;

public class NDataModelManagerTest extends NLocalFileMetadataTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private NDataModelManager mgrDefault;
    private String projectDefault = "default";
    private String modelBasic = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
    private String modelTest = "model_test";
    private String ownerTest = "owner_test";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        mgrDefault = NDataModelManager.getInstance(getTestConfig(), projectDefault);
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGetInstance() {
        NDataModelManager mgrSsb = NDataModelManager.getInstance(getTestConfig(), "ssb");
        Assert.assertNotEquals(mgrDefault, mgrSsb);

        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), "not_exist_prj");
        Assert.assertEquals(0, dataflowManager.listUnderliningDataModels().size());
    }

    @Test
    public void testBasicModel() {
        NDataModel bm = mgrDefault.getDataModelDesc(modelBasic);
        List<String> alias = Lists.newArrayList("TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.LEAF_CATEG_ID",
                "TEST_KYLIN_FACT.LSTG_FORMAT_NAME");
        List<String> partitions = Lists.newArrayList("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.ORDER_ID",
                "TEST_KYLIN_FACT.CAL_DT");
        List<Pair<List<String>, List<String>>> valueMapping = Lists.newArrayList();
        List<String> pValue1 = Lists.newArrayList("1", "2", "3");
        List<String> aValue1 = Lists.newArrayList("a", "b", "c");
        List<String> aValue2 = Lists.newArrayList("a'", "b'", "c'");

        List<String> pValue2 = Lists.newArrayList("4", "5", "6");
        List<String> aValue3 = Lists.newArrayList("e", "f", "g");
        List<String> aValue4 = Lists.newArrayList("e'", "f'", "g'");

        valueMapping.add(Pair.newPair(pValue1, aValue1));
        valueMapping.add(Pair.newPair(pValue1, aValue2));
        valueMapping.add(Pair.newPair(pValue2, aValue3));
        valueMapping.add(Pair.newPair(pValue2, aValue4));

        MultiPartitionKeyMappingImpl mapping = new MultiPartitionKeyMappingImpl(alias, partitions, valueMapping);
        //        bm.setMultiPartitionKeyMapping(mapping);
        mgrDefault.updateDataModelDesc(bm);

        Assert.assertEquals(9, bm.getJoinTables().size());
    }

    @Test
    public void testGetDataModelDesc() {
        NDataModel dataModel = mgrDefault.getDataModelDesc(modelBasic);
        Assert.assertEquals(modelBasic, dataModel.getUuid());
        Assert.assertEquals(projectDefault, dataModel.getProject());
    }

    @Test
    public void testDropModel() throws IOException {
        NDataModel toDrop = mgrDefault.getDataModelDesc(modelBasic);
        NDataModel dropped = mgrDefault.dropModel(toDrop);
        Assert.assertEquals(toDrop, dropped);
    }

    @Test
    public void testCreateDataModelDesc() throws IOException {
        NDataModel model = mockModel();
        NDataModel result = mgrDefault.createDataModelDesc(model, ownerTest);

        Assert.assertEquals(projectDefault, result.getProject());
        Assert.assertEquals(ownerTest, result.getOwner());
        Assert.assertEquals(result, mgrDefault.getDataModelDesc(modelTest));
    }

    @Test
    public void testUpdateDataModelDesc() throws IOException {
        NDataModel model = mockModel();
        mgrDefault.createDataModelDesc(model, ownerTest);

        String newVersion = "v8.8.8";
        model.setVersion(newVersion);
        NDataModel updated = mgrDefault.updateDataModelDesc(model);

        Assert.assertEquals(newVersion, updated.getVersion());
        Assert.assertEquals(mgrDefault.getDataModelDesc(modelTest), updated);
    }

    private NDataModel mockModel() {
        NDataModel model = new NDataModel();
        model.setAlias(modelTest);
        model.setUuid(modelTest);
        model.setOwner(ownerTest);
        model.setProject(projectDefault);
        model.setRootFactTableName("DEFAULT.TEST_KYLIN_FACT");
        Measure measure = new Measure();
        measure.setName("test_measure");
        measure.setFunction(FunctionDesc.newInstance(FunctionDesc.FUNC_COUNT,
                Lists.newArrayList(ParameterDesc.newInstance("1")), "bigint"));
        model.setAllMeasures(Lists.newArrayList(measure));

        return model;
    }

    @Test
    public void createDataModelDesc_duplicateModelName_fail() throws IOException {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Model name 'nmodel_basic' is duplicated, could not be created.");
        NDataModel nDataModel = JsonUtil.deepCopy(mgrDefault.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa"),
                NDataModel.class);
        nDataModel.setProject(projectDefault);

        mgrDefault.createDataModelDesc(nDataModel, "root");
    }

    @Test
    public void createDataModelDesc_simpleModel_succeed() throws IOException {
        NDataModel nDataModel = JsonUtil.deepCopy(
                (NDataModel) mgrDefault.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa"), NDataModel.class);

        nDataModel.setAlias("nmodel_basic2");
        nDataModel.setUuid(RandomUtil.randomUUIDStr());
        nDataModel.setLastModified(0L);
        nDataModel.setMvcc(-1);
        nDataModel.setProject(projectDefault);
        mgrDefault.createDataModelDesc(nDataModel, "root");

        NDataModel model = mgrDefault.getDataModelDesc(nDataModel.getId());
        Assert.assertNotNull(model);
        Assert.assertEquals(nDataModel, model);
    }

    @Test
    public void createDataModelDesc_duplicateNamedColumn_fail() throws IOException {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Multiple entries with same value");

        NDataModel nDataModel = JsonUtil.deepCopy(
                (NDataModel) mgrDefault.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa"), NDataModel.class);

        nDataModel.setAlias("nmodel_basic2");
        nDataModel.setUuid(RandomUtil.randomUUIDStr());
        nDataModel.setLastModified(0L);
        nDataModel.setProject(projectDefault);

        //add a duplicate
        List<NDataModel.NamedColumn> allNamedColumns = nDataModel.getAllNamedColumns();
        NDataModel.NamedColumn e = JsonUtil.deepCopy(allNamedColumns.get(0), NDataModel.NamedColumn.class);
        e.setId(allNamedColumns.size());
        allNamedColumns.add(e);

        mgrDefault.createDataModelDesc(nDataModel, "root");
    }

    @Test
    public void createDataModelDesc_duplicateNameColumnName_succeed() throws IOException {

        NDataModel nDataModel = JsonUtil.deepCopy(
                (NDataModel) mgrDefault.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa"), NDataModel.class);

        nDataModel.setAlias("nmodel_basic2");
        nDataModel.setUuid(RandomUtil.randomUUIDStr());
        nDataModel.setLastModified(0L);
        nDataModel.setMvcc(-1);
        nDataModel.setProject(projectDefault);

        //make conflict on NamedColumn.name
        List<NDataModel.NamedColumn> allNamedColumns = nDataModel.getAllNamedColumns();
        allNamedColumns.get(1).setName(allNamedColumns.get(0).getName());

        mgrDefault.createDataModelDesc(nDataModel, "root");
    }

    @Test
    public void saveDataModelDesc_MultipleDataLoadingRange_exception() throws IOException {

        NDataModel nDataModel = mgrDefault
                .copyForWrite(mgrDefault.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa"));
        val tableManager = NTableMetadataManager.getInstance(getTestConfig(), "default");
        val table = tableManager.getTableDesc("DEFAULT.TEST_ACCOUNT");
        table.setIncrementLoading(true);
        tableManager.updateTableDesc(table);
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("Only one incremental loading table can be set in model!");
        mgrDefault.updateDataModelDesc(nDataModel);
    }

    @Test
    public void testGetRelatedTables() {
        String project = "default";
        NDataModel nDataModel = mgrDefault.getDataModelDesc("b780e4e4-69af-449e-b09f-05c90dfa04b6");
        val tables = NDataModelManager.getRelatedTables(nDataModel, project);
        Assert.assertEquals(2, tables.size());
        Assert.assertTrue(tables.keySet().contains("DEFAULT.TEST_BANK_LOCATION"));
        Assert.assertTrue(tables.keySet().contains("DEFAULT.TEST_BANK_INCOME"));
    }

    @Test
    public void getModel_WithSelfBroken() {
        val project = "broken_test";
        val modelId = "3f8941de-d01c-42b8-91b5-44646390864b";
        val modelManager = NDataModelManager.getInstance(getTestConfig(), project);
        val model = modelManager.getDataModelDesc(modelId);
        Assert.assertEquals(true, model.isBroken());

        val model2 = modelManager.getDataModelDescByAlias("AUTO_MODEL_TEST_COUNTRY_1");
        Assert.assertNull(model2);
    }

    @Test
    public void testCreateModelWithBreaker() {
        final String owner = "test_ck_owner";
        Arrays.asList("test_ck_1").forEach(name -> mockModel());
        NDataModelManager manager = Mockito.spy(NDataModelManager.getInstance(getTestConfig(), projectDefault));

        getTestConfig().setProperty("kylin.circuit-breaker.threshold.model", "1");
        NCircuitBreaker.start(KapConfig.wrap(getTestConfig()));
        try {
            thrown.expect(KylinException.class);
            manager.createDataModelDesc(mockModel(), owner);
        } finally {
            NCircuitBreaker.stop();
        }
    }
}

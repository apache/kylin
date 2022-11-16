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

import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.TempMetadataBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.Lists;

import lombok.val;

public class NDataModelTest {

    private static final String DEFAULT_PROJECT = "default";
    KylinConfig config;
    NDataModelManager mgr;

    @Before
    public void setUp() throws Exception {
        String tempMetadataDir = TempMetadataBuilder.prepareLocalTempMetadata();
        KylinConfig.setKylinConfigForLocalTest(tempMetadataDir);
        config = KylinConfig.getInstanceFromEnv();
        mgr = NDataModelManager.getInstance(config, DEFAULT_PROJECT);
    }

    @Test
    public void testBasics() {
        try {
            mgr.init(config, DEFAULT_PROJECT);
        } catch (Exception e) {
            Assert.fail();
        }

        NDataModel model = mgr.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertNotNull(model);
        Assert.assertNotEquals(model, model.getRootFactTable());

        Assert.assertTrue(model.isLookupTable("DEFAULT.TEST_ORDER"));
        Set<TableRef> lookupTables = model.getLookupTables();
        TableRef lookupTable = null;
        for (TableRef table : lookupTables) {
            if (table.getTableIdentity().equals("DEFAULT.TEST_ORDER")) {
                lookupTable = table;
                break;
            }
        }
        Assert.assertNotNull(lookupTable);
        Assert.assertTrue(model.isLookupTable(lookupTable));

        Assert.assertTrue(model.isFactTable("DEFAULT.TEST_KYLIN_FACT"));
        Set<TableRef> factTables = model.getFactTables();
        TableRef factTable = null;
        for (TableRef table : factTables) {
            if (table.getTableIdentity().equals("DEFAULT.TEST_KYLIN_FACT")) {
                factTable = table;
                break;
            }
        }
        Assert.assertNotNull(factTable);
        Assert.assertTrue(model.isFactTable(factTable));

        ImmutableBiMap<Integer, TblColRef> dimMap = model.getEffectiveCols();
        Assert.assertEquals(model.findColumn("TRANS_ID"), dimMap.get(1));
        Assert.assertEquals(model.findColumn("TEST_KYLIN_FACT.CAL_DT"), dimMap.get(2));
        Assert.assertEquals(model.findColumn("LSTG_FORMAT_NAME"), dimMap.get(3));
        Assert.assertThrows(RuntimeException.class, () -> model.findColumn("not_exits"));
        Assert.assertEquals(model.getAllNamedColumns().size() - 1, dimMap.size());

        Assert.assertNotNull(model.findFirstTable("DEFAULT.TEST_KYLIN_FACT"));

        NDataModel copyModel = mgr.copyForWrite(model);
        Assert.assertEquals(model.getProject(), copyModel.getProject());
        Assert.assertEquals(model.getAllNamedColumns(), copyModel.getAllNamedColumns());
        Assert.assertEquals(model.getAllMeasures(), copyModel.getAllMeasures());
        Assert.assertEquals(model.getAllNamedColumns(), copyModel.getAllNamedColumns());

        ImmutableBiMap<Integer, NDataModel.Measure> measureMap = model.getEffectiveMeasures();
        Assert.assertEquals(model.getAllMeasures().size() - 1, measureMap.size());

        NDataModel.Measure m = measureMap.get(100001);
        Assert.assertEquals(100001, m.getId());
        Assert.assertEquals("GMV_SUM", m.getName());
        Assert.assertEquals("SUM", m.getFunction().getExpression());
        Assert.assertEquals(model.findColumn("PRICE"), m.getFunction().getParameters().get(0).getColRef());
        Assert.assertEquals("default", model.getProject());
    }

    @Test
    public void getAllNamedColumns_changeToTomb_lessEffectiveCols() {
        NDataModel model = mgr.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        int size = model.getEffectiveCols().size();

        model.getAllNamedColumns().get(0).setStatus(NDataModel.ColumnStatus.TOMB);
        mgr.updateDataModelDesc(model);
        model = mgr.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        int size2 = model.getEffectiveCols().size();

        Assert.assertEquals(size - 1, size2);
    }

    @Test
    public void testGetCopyOf() {
        NDataModel model = mgr.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        NDataModel copyModel = mgr.copyForWrite(model);
        Assert.assertEquals(model, copyModel);
        Assert.assertEquals(model.getAllMeasures(), copyModel.getAllMeasures());
        copyModel.getAllMeasures().get(0).setTomb(true);
        Assert.assertFalse(model.getAllMeasures().get(0).isTomb());
        Assert.assertNotEquals(model, copyModel);

        NDataModel copyModel2 = mgr.copyForWrite(model);
        Assert.assertEquals(model, copyModel2);
        copyModel2.getAllNamedColumns().remove(copyModel2.getAllNamedColumns().size() - 1);
        Assert.assertNotEquals(model, copyModel2);

        NDataModel copyModel3 = mgr.copyForWrite(model);
        Assert.assertEquals(model, copyModel3);
    }

    @Test
    public void testGetNameById() {
        NDataModel model = mgr.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals("CAL_DT", model.getNameByColumnId(2));
        Assert.assertNull(model.getNameByColumnId(300));
    }

    @Test
    public void testBrokenModel() {
        try {
            mgr.init(config, "broken_test");
        } catch (Exception e) {
            Assert.fail();
        }

        NDataModel model = mgr.getDataModelDesc("a9e4c7d2-60c8-4a16-9949-2c8ed0199efd");
        Assert.assertEquals(NDataModel.ModelType.BATCH, model.getModelType());
        Assert.assertEquals(true, model.isBroken());
    }

    @Test
    public void testModelRenameEvent() {
        NDataModel.ModelRenameEvent renameEvent = new NDataModel.ModelRenameEvent(DEFAULT_PROJECT,
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa", "new_model_name");
        Assert.assertEquals(DEFAULT_PROJECT, renameEvent.getProject());
        Assert.assertEquals("89af4ee2-2cdb-4b07-b39e-4c29856309aa", renameEvent.getSubject());
        Assert.assertEquals("new_model_name", renameEvent.getNewName());

        renameEvent.setProject("streaming_test");
        renameEvent.setSubject("e78a89dd-847f-4574-8afa-8768b4228b73");
        renameEvent.setNewName("new_model_name2");
        Assert.assertEquals("streaming_test", renameEvent.getProject());
        Assert.assertEquals("e78a89dd-847f-4574-8afa-8768b4228b73", renameEvent.getSubject());
        Assert.assertEquals("new_model_name2", renameEvent.getNewName());

        NDataModel.ModelRenameEvent renameEvent1 = new NDataModel.ModelRenameEvent("name");
        Assert.assertEquals("name", renameEvent1.getNewName());
    }

    @Test
    public void testCheckCCFailAtEnd() {
        String modelId = "4a45dc4d-937e-43cc-8faa-34d59d4e11d3";
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "cc_test");
        NDataModel originModelDesc = modelManager.getDataModelDesc(modelId);
        System.setProperty("needCheckCC", "true");
        originModelDesc.setProject("cc_test");

        {
            val ccList = Lists.newArrayList(getComputedColumnDesc("CC_1", "CUSTOMER.C_NAME +'USA'", "DOUBLE"),
                    getComputedColumnDesc("CC_2", "LINEORDER.LO_TAX +1 ", "DOUBLE"),
                    getComputedColumnDesc("CC_3", "1+2", "INTEGER"));
            originModelDesc.setComputedColumnDescs(ccList);
            val ccRelatedModels = modelManager.getCCRelatedModels(originModelDesc);
            originModelDesc.checkCCFailAtEnd(KylinConfig.getInstanceFromEnv(), originModelDesc.getProject(),
                    ccRelatedModels, true);
        }

        {
            val ccList = Lists.newArrayList(getComputedColumnDesc("CC_1", "CUSTOMER.C_NAME +'USA'", "DOUBLE"),
                    getComputedColumnDesc("CC_LTAX", "LINEORDER.LO_TAX *1 ", "DOUBLE"),
                    getComputedColumnDesc("CC_3", "1+2", "INTEGER"));
            originModelDesc.setComputedColumnDescs(ccList);
            val ccRelatedModels = modelManager.getCCRelatedModels(originModelDesc);
            originModelDesc.checkCCFailAtEnd(KylinConfig.getInstanceFromEnv(), originModelDesc.getProject(),
                    ccRelatedModels, true);
        }

        {
            val ccList = Lists.newArrayList(getComputedColumnDesc("CC_1", "CUSTOMER.C_NAME +'USA'", "DOUBLE"),
                    getComputedColumnDesc("CC_LTAX", "LINEORDER.LO_TAX *1 ", "DOUBLE"),
                    getComputedColumnDesc("CC_3", "1+2", "INTEGER"));
            originModelDesc.setComputedColumnDescs(ccList);
            val ccRelatedModels = modelManager.getCCRelatedModels(originModelDesc);
            originModelDesc.checkCCFailAtEnd(KylinConfig.getInstanceFromEnv(), originModelDesc.getProject(),
                    ccRelatedModels, true);
        }

    }

    public ComputedColumnDesc getComputedColumnDesc(String ccName, String ccExpression, String dataType) {
        ComputedColumnDesc ccDesc = new ComputedColumnDesc();
        ccDesc.setColumnName(ccName);
        ccDesc.setExpression(ccExpression);
        ccDesc.setDatatype(dataType);
        ccDesc.setTableAlias("LINEORDER");
        ccDesc.setTableIdentity("SSB.LINEORDER");
        return ccDesc;
    }

}

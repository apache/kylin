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

package org.apache.kylin.metadata.model.util;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.TableRef;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;

import io.kyligence.kap.guava20.shaded.common.collect.Lists;
import lombok.val;

public class ComputedColumnUtilTest extends NLocalFileMetadataTestCase {

    NDataModelManager modelManager;

    @Before
    public void setUp() {
        this.createTestMetadata();
        modelManager = NDataModelManager.getInstance(getTestConfig(), "default");
    }

    @After
    public void clean() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGetCCUsedColsInProject() {
        NDataModel model = modelManager.getDataModelDescByAlias("nmodel_basic_inner");
        TableRef firstTable = model.findFirstTable("DEFAULT.TEST_KYLIN_FACT");
        ColumnDesc ccColDesc = firstTable.getColumn("DEAL_YEAR").getColumnDesc();
        Set<String> ccUsedColsInProject = ComputedColumnUtil.getCCUsedColsWithProject("default", ccColDesc);
        Assert.assertEquals(1, ccUsedColsInProject.size());
        Assert.assertTrue(ccUsedColsInProject.contains("DEFAULT.TEST_KYLIN_FACT.CAL_DT"));
    }

    @Test
    public void testGetCCUsedColsInModel() {
        NDataModel model = modelManager.getDataModelDescByAlias("nmodel_basic_inner");
        //test param type (model,ColumnDesc)
        TableRef firstTable = model.findFirstTable("DEFAULT.TEST_KYLIN_FACT");
        ColumnDesc ccColDesc = firstTable.getColumn("DEAL_YEAR").getColumnDesc();
        Set<String> ccUsedColsInModel = ComputedColumnUtil.getCCUsedColsWithModel(model, ccColDesc);
        Assert.assertEquals(1, ccUsedColsInModel.size());
        Assert.assertTrue(ccUsedColsInModel.contains("DEFAULT.TEST_KYLIN_FACT.CAL_DT"));

        //test param (model, ComputedColumnDesc)
        List<ComputedColumnDesc> computedColumnDescs = model.getComputedColumnDescs();
        for (ComputedColumnDesc ccCol : computedColumnDescs) {
            if (ccCol.getColumnName().equals("DEAL_AMOUNT")) {
                ccUsedColsInModel = ComputedColumnUtil.getCCUsedColsWithModel(model, ccCol);
                Assert.assertEquals(2, ccUsedColsInModel.size());
                Assert.assertTrue(ccUsedColsInModel.contains("DEFAULT.TEST_KYLIN_FACT.PRICE"));
                Assert.assertTrue(ccUsedColsInModel.contains("DEFAULT.TEST_KYLIN_FACT.ITEM_COUNT"));
            }
            if (ccCol.getColumnName().equals("DEAL_YEAR")) {
                ccUsedColsInModel = ComputedColumnUtil.getCCUsedColsWithModel(model, ccCol);
                Assert.assertEquals(1, ccUsedColsInModel.size());
                Assert.assertTrue(ccUsedColsInModel.contains("DEFAULT.TEST_KYLIN_FACT.CAL_DT"));
            }
        }
    }

    @Test
    public void testGetCCUsedColWithDoubleQuoteInModel() {
        NDataModel model = modelManager.getDataModelDescByAlias("nmodel_basic_inner");
        modelManager.updateDataModel(model.getUuid(), copyForWrite -> {
            final List<ComputedColumnDesc> ccList = copyForWrite.getComputedColumnDescs();
            final ComputedColumnDesc cc0 = ccList.get(0);
            cc0.setExpression("\"TEST_KYLIN_FACT\".\"PRICE\" * \"TEST_KYLIN_FACT\".\"ITEM_COUNT\"");
            copyForWrite.setComputedColumnDescs(ccList);
        });

        NDataModel modelNew = modelManager.getDataModelDesc(model.getUuid());
        ColumnDesc column = new ColumnDesc();
        column.setName("DEAL_AMOUNT");
        column.setComputedColumnExpr("`TEST_KYLIN_FACT`.`PRICE` * `TEST_KYLIN_FACT`.`ITEM_COUNT`");
        Map<String, Set<String>> colsMapWithModel = ComputedColumnUtil.getCCUsedColsMapWithModel(modelNew, column);
        Assert.assertEquals(1, colsMapWithModel.size());
        Assert.assertTrue(colsMapWithModel.containsKey("DEFAULT.TEST_KYLIN_FACT"));
        Set<String> columns = colsMapWithModel.get("DEFAULT.TEST_KYLIN_FACT");
        Assert.assertEquals(Sets.newHashSet("PRICE", "ITEM_COUNT"), columns);

        ColumnDesc notExistColumn = new ColumnDesc();
        notExistColumn.setName("CC_NOT_EXIST");
        notExistColumn.setComputedColumnExpr("`TEST_KYLIN_FACT`.`PRICE` * 0.95");
        try {
            ComputedColumnUtil.getCCUsedColsMapWithModel(modelNew, notExistColumn);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals(
                    "ComputedColumn(name: CC_NOT_EXIST) is not on model: 741ca86a-1f13-46da-a59f-95fb68615e3a",
                    e.getMessage());
        }
    }

    @Test
    public void testGetAllCCUsedColsInModel() {
        NDataModel model = modelManager.getDataModelDescByAlias("nmodel_basic_inner");
        Set<String> allCCUsedColsInModel = ComputedColumnUtil.getAllCCUsedColsInModel(model);
        Assert.assertEquals(6, allCCUsedColsInModel.size());
        Assert.assertTrue(allCCUsedColsInModel.contains("DEFAULT.TEST_KYLIN_FACT.PRICE")); //belong to cc "DEAL_AMOUNT"
        Assert.assertTrue(allCCUsedColsInModel.contains("DEFAULT.TEST_KYLIN_FACT.ITEM_COUNT")); //belong to cc "DEAL_AMOUNT"
        Assert.assertTrue(allCCUsedColsInModel.contains("DEFAULT.TEST_KYLIN_FACT.CAL_DT")); //belong to cc "DEAL_YEAR"
        Assert.assertTrue(allCCUsedColsInModel.contains("DEFAULT.TEST_KYLIN_FACT.NEST1"));
        Assert.assertTrue(allCCUsedColsInModel.contains("DEFAULT.TEST_KYLIN_FACT.NEST2"));
        Assert.assertTrue(allCCUsedColsInModel.contains("DEFAULT.TEST_KYLIN_FACT.NEST3"));
    }

    @Test
    public void testCCConflict() {
        val conflictInfo = new ComputedColumnUtil.CCConflictInfo();
        val handler = new ComputedColumnUtil.AdjustCCConflictHandler(conflictInfo);
        val originModelDesc = NDataModelManager.getInstance(getTestConfig(), "cc_test")
                .getDataModelDesc("4a45dc4d-937e-43cc-8faa-34d59d4e11d3");
        val ccList = Lists.newArrayList(//
                getComputedColumnDesc("CC_1", "CUSTOMER.C_NAME +'USA'", "DOUBLE"),
                getComputedColumnDesc("CC_LTAX", "LINEORDER.LO_TAX *1 ", "DOUBLE"),
                getComputedColumnDesc("CC_3", "1+2", "INTEGER"));
        for (ComputedColumnDesc cc : ccList) {
            handler.handleOnSameNameDiffExpr(originModelDesc, originModelDesc, cc, cc);
            handler.handleOnSameExprDiffName(originModelDesc, cc, cc);
        }

        Assert.assertTrue(conflictInfo.hasSameNameConflict());
        Assert.assertTrue(conflictInfo.hasSameExprConflict());

        val sameNameConflictException = conflictInfo.getSameNameConflictException();
        val sameExprConflictException = conflictInfo.getSameExprConflictException();
        val allConflictException = conflictInfo.getAllConflictException();
        Assert.assertEquals(3, sameExprConflictException.size());
        Assert.assertEquals(3, sameNameConflictException.size());
        Assert.assertEquals(6, allConflictException.size());

        val pair = conflictInfo.getAdjustedCCList(ccList);
        Assert.assertEquals(3, pair.getFirst().size());
        Assert.assertEquals(3, pair.getSecond().size());
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

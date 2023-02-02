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

import java.util.List;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.kylin.util.MetadataTestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

@MetadataInfo
class ColExcludedCheckerTest {
    String modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";

    public String getProject() {
        return "default";
    }

    private NDataModel prepareModel(KylinConfig kylinConfig) {
        NDataModelManager modelManager = NDataModelManager.getInstance(kylinConfig, getProject());
        NDataModel model = modelManager.getDataModelDesc(modelId);
        model.getComputedColumnDescs().forEach(cc -> {
            String innerExp = QueryUtil.massageComputedColumn(model, getProject(), cc, null);
            cc.setInnerExpression(innerExp);
        });
        model.init(kylinConfig, getProject(), Lists.newArrayList());
        return model;
    }

    @Test
    void testWithoutTurnOnExcludedTableSettings() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        NDataModel model = prepareModel(kylinConfig);
        ColExcludedChecker checker = new ColExcludedChecker(kylinConfig, getProject(), model);
        Set<ColumnDesc> excludedCols = checker.getExcludedCols();
        Assertions.assertTrue(excludedCols.isEmpty());
    }

    @Test
    void testIsExcludedComputedColumn() {

        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        NDataModel model = prepareModel(kylinConfig);

        String factTable = "DEFAULT.TEST_KYLIN_FACT";
        String lookupTable = "DEFAULT.TEST_ACCOUNT";
        MetadataTestUtils.mockExcludedTable(getProject(), factTable);
        MetadataTestUtils.mockExcludedCols(getProject(), lookupTable, Sets.newHashSet("ACCOUNT_ID", "ACCOUNT_COUNTRY"));
        ColExcludedChecker checker = new ColExcludedChecker(kylinConfig, getProject(), model);
        Assertions.assertEquals(6, checker.getExcludedCols().size());

        List<ComputedColumnDesc> ccList = model.getComputedColumnDescs();
        Assertions.assertFalse(checker.isExcludedCC(ccList.get(0)));
        Assertions.assertFalse(checker.isExcludedCC(ccList.get(1)));
        Assertions.assertTrue(checker.isExcludedCC(ccList.get(2)));
        Assertions.assertTrue(checker.isExcludedCC(ccList.get(3)));
        Assertions.assertTrue(checker.isExcludedCC(ccList.get(4)));
        Assertions.assertTrue(checker.isExcludedCC(ccList.get(5)));
        Assertions.assertFalse(checker.isExcludedCC(ccList.get(6)));
        Assertions.assertFalse(checker.isExcludedCC(ccList.get(7)));
        Assertions.assertFalse(checker.isExcludedCC(ccList.get(8)));
        Assertions.assertFalse(checker.isExcludedCC(ccList.get(9)));

        Assertions.assertFalse(checker.isExcludedCC(""));
    }

    @Test
    void testIsExcludedMeasure() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        NDataModel model = prepareModel(kylinConfig);

        String factTable = "DEFAULT.TEST_KYLIN_FACT";
        String lookupTable = "DEFAULT.TEST_ACCOUNT";
        MetadataTestUtils.mockExcludedTable(getProject(), factTable);
        MetadataTestUtils.mockExcludedCols(getProject(), lookupTable, Sets.newHashSet("ACCOUNT_ID", "ACCOUNT_COUNTRY"));
        ColExcludedChecker checker = new ColExcludedChecker(kylinConfig, getProject(), model);
        Assertions.assertEquals(6, checker.getExcludedCols().size());

        // assert measure
        {
            ParameterDesc param = new ParameterDesc();
            param.setColRef(model.getEffectiveCols().get(29));
            param.setValue("TEST_KYLIN_FACT.LEFTJOIN_BUYER_ID_AND_COUNTRY_NAME");
            param.setType("column");
            FunctionDesc function = FunctionDesc.newInstance("sum", ImmutableList.of(param), "int");
            Assertions.assertTrue(checker.isExcludedMeasure(function));
        }

        {
            ParameterDesc param = new ParameterDesc();
            param.setColRef(model.getEffectiveCols().get(28));
            param.setValue("");
            param.setType("column");
            FunctionDesc function = FunctionDesc.newInstance("sum", ImmutableList.of(param), "int");
            Assertions.assertFalse(checker.isExcludedMeasure(function));
        }

        {
            FunctionDesc function = FunctionDesc.newInstance("sum", ImmutableList.of(), "int");
            Assertions.assertFalse(checker.isExcludedMeasure(function));
        }
    }

    @Test
    void testFilterRelatedExcludedColumn() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        NDataModel model = prepareModel(kylinConfig);

        String factTable = "DEFAULT.TEST_KYLIN_FACT";
        String lookupTable = "DEFAULT.TEST_ACCOUNT";
        MetadataTestUtils.mockExcludedTable(getProject(), factTable);
        MetadataTestUtils.mockExcludedCols(getProject(), lookupTable, Sets.newHashSet("ACCOUNT_ID", "ACCOUNT_COUNTRY"));

        // init without model, usually used by QueryUtil
        ColExcludedChecker checker = new ColExcludedChecker(kylinConfig, getProject(), null);
        Assertions.assertEquals(14, checker.getExcludedCols().size());

        // return the given model's excluded columns without considering columns from cc
        Set<String> excludedCols = checker.filterRelatedExcludedColumn(model);
        Assertions.assertEquals(2, excludedCols.size());
        Assertions.assertEquals(Sets.newHashSet("TEST_ACCOUNT.ACCOUNT_ID", "TEST_ACCOUNT.ACCOUNT_COUNTRY"),
                excludedCols);

        // if model is null, return all project excluded columns
        Assertions.assertEquals(14, checker.filterRelatedExcludedColumn(null).size());
    }

    @Test
    void testNewColExcludedCheckerWithBrokenModel() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        NDataModel model = prepareModel(kylinConfig);
        model.setBroken(true);
        model.setBrokenReason(NDataModel.BrokenReason.NULL);

        String factTable = "DEFAULT.TEST_KYLIN_FACT";
        String lookupTable = "DEFAULT.TEST_ACCOUNT";
        MetadataTestUtils.mockExcludedTable(getProject(), factTable);
        MetadataTestUtils.mockExcludedCols(getProject(), lookupTable, Sets.newHashSet("ACCOUNT_ID", "ACCOUNT_COUNTRY"));

        ColExcludedChecker checker = new ColExcludedChecker(kylinConfig, getProject(), model);
        Assertions.assertTrue(checker.getExcludedCols().isEmpty());
    }

    @Test
    void testNewColExcludedCheckerWithModelMissingExcludedTable() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        NDataModel model = prepareModel(kylinConfig);
        model.setBroken(true);
        model.setBrokenReason(NDataModel.BrokenReason.NULL);

        String factTable = "DEFAULT.TEST_KYLIN_FACT";
        String lookupTable = "DEFAULT.TEST_ACCOUNT";
        MetadataTestUtils.mockExcludedTable(getProject(), factTable);
        MetadataTestUtils.mockExcludedCols(getProject(), lookupTable, Sets.newHashSet("ACCOUNT_ID", "ACCOUNT_COUNTRY"));

        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(kylinConfig, getProject());
        tableMgr.removeSourceTable("DEFAULT.TEST_ACCOUNT");

        ColExcludedChecker checker = new ColExcludedChecker(kylinConfig, getProject(), model);
        Assertions.assertTrue(checker.getExcludedCols().isEmpty());
    }

    @Test
    void testNewColExcludedCheckerWithModelMissingNonExcludedTable() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        NDataModel model = prepareModel(kylinConfig);
        model.setBroken(true);
        model.setBrokenReason(NDataModel.BrokenReason.NULL);

        String factTable = "DEFAULT.TEST_KYLIN_FACT";
        String lookupTable = "DEFAULT.TEST_ACCOUNT";
        MetadataTestUtils.mockExcludedTable(getProject(), factTable);
        MetadataTestUtils.mockExcludedCols(getProject(), lookupTable, Sets.newHashSet("ACCOUNT_ID", "ACCOUNT_COUNTRY"));

        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(kylinConfig, getProject());
        tableMgr.removeSourceTable("DEFAULT.TEST_ORDER");

        ColExcludedChecker checker = new ColExcludedChecker(kylinConfig, getProject(), model);
        Assertions.assertTrue(checker.getExcludedCols().isEmpty());
    }
}

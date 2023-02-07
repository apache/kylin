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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.query.util.PushDownUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;

@MetadataInfo
class AntiFlatCheckerTest {

    String modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";

    String getProject() {
        return "default";
    }

    private NDataModel prepareModel(KylinConfig kylinConfig) {
        NDataModelManager modelManager = NDataModelManager.getInstance(kylinConfig, getProject());
        NDataModel model = modelManager.getDataModelDesc(modelId);
        model.getComputedColumnDescs().forEach(cc -> {
            String innerExp = PushDownUtil.massageComputedColumn(model, getProject(), cc, null);
            cc.setInnerExpression(innerExp);
        });
        model.init(kylinConfig, getProject(), Lists.newArrayList());
        return model;
    }

    @Test
    void testJoinTablesWithNullValue() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        NDataModel model = prepareModel(kylinConfig);
        AntiFlatChecker checker = new AntiFlatChecker(null, model);
        Assertions.assertTrue(checker.getAntiFlattenLookups().isEmpty());
        Assertions.assertTrue(checker.getInvalidComputedColumns(model).isEmpty());
        Assertions.assertTrue(checker.getInvalidDimensions(model).isEmpty());
        Assertions.assertTrue(checker.getInvalidMeasures(model).isEmpty());
    }

    @Test
    void testWithBrokenModel() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        NDataModel model = prepareModel(kylinConfig);

        NDataModel copyModel = JsonUtil.deepCopyQuietly(model, NDataModel.class);
        copyModel.init(kylinConfig, getProject(), Lists.newArrayList());
        List<JoinTableDesc> joinTables = copyModel.getJoinTables();
        joinTables.forEach(joinTableDesc -> {
            if ("DEFAULT.TEST_ORDER".equals(joinTableDesc.getTable())
                    || "EDW.TEST_CAL_DT".equals(joinTableDesc.getTable())) {
                joinTableDesc.setFlattenable(JoinTableDesc.NORMALIZED);
            }
        });

        {
            AntiFlatChecker checker = new AntiFlatChecker(joinTables, null);
            Assertions.assertTrue(checker.getAntiFlattenLookups().isEmpty());
        }

        {
            model.setBroken(true);
            AntiFlatChecker checker = new AntiFlatChecker(null, model);
            Assertions.assertTrue(checker.getAntiFlattenLookups().isEmpty());
        }
    }

    @Test
    void testNormal() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        NDataModel model = prepareModel(kylinConfig);

        NDataModel copyModel = JsonUtil.deepCopyQuietly(model, NDataModel.class);
        copyModel.init(kylinConfig, getProject(), Lists.newArrayList());
        List<JoinTableDesc> joinTables = copyModel.getJoinTables();
        joinTables.forEach(joinTableDesc -> {
            if ("DEFAULT.TEST_ORDER".equals(joinTableDesc.getTable())
                    || "EDW.TEST_CAL_DT".equals(joinTableDesc.getTable())) {
                joinTableDesc.setFlattenable(JoinTableDesc.NORMALIZED);
            }
        });

        AntiFlatChecker checker = new AntiFlatChecker(joinTables, model);

        // =========== column ==================
        /* 2 -> TblColRef: "DEFAULT.TEST_KYLIN_FACT.CAL_DT" */
        Assertions.assertFalse(checker.isColOfAntiLookup(model.getEffectiveCols().get(2)));
        /* 14 -> TblColRef: "DEFAULT.TEST_ORDER.TEST_DATE_ENC" */
        Assertions.assertTrue(checker.isColOfAntiLookup(model.getEffectiveCols().get(14)));
        /* 58 -> TblColRef: "EDW.TEST_CAL_DT.RETAIL_WEEK" */
        Assertions.assertTrue(checker.isColOfAntiLookup(model.getEffectiveCols().get(58)));

        // ============ measure =================

        /* 100000 FunctionDesc [expression=COUNT, parameter=1, returnType=bigint] */
        NDataModel.Measure measure1 = model.getEffectiveMeasures().get(100000);
        Assertions.assertFalse(checker.isMeasureOfAntiLookup(measure1.getFunction()));

        /* 100014 FunctionDesc [expression=COUNT, parameter=EDW.TEST_CAL_DT.CAL_DT, returnType=bigint] */
        NDataModel.Measure measure2 = model.getEffectiveMeasures().get(100014);
        Assertions.assertTrue(checker.isMeasureOfAntiLookup(measure2.getFunction()));

        // ============ cc =================
        /* CC {tableIdentity=DEFAULT.TEST_KYLIN_FACT, columnName=DEAL_AMOUNT,
                    expression=TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT}
           27 -> TblColRef: "DEFAULT.TEST_KYLIN_FACT.DEAL_AMOUNT" , from CC */
        Assertions.assertFalse(checker.isColOfAntiLookup(model.getEffectiveCols().get(27)));

        /* CC {tableIdentity=DEFAULT.TEST_KYLIN_FACT, columnName=DEAL_YEAR,
                    expression=year(TEST_KYLIN_FACT.CAL_DT)}
           28 -> TblColRef: "DEFAULT.TEST_KYLIN_FACT.DEAL_YEAR" */
        Assertions.assertFalse(checker.isColOfAntiLookup(model.getEffectiveCols().get(28)));

        /* CC {tableIdentity=DEFAULT.TEST_KYLIN_FACT, columnName=LEFTJOIN_BUYER_COUNTRY_ABBR,
                    expression=SUBSTR(BUYER_ACCOUNT.ACCOUNT_COUNTRY,0,1)}
           31 -> TblColRef: "DEFAULT.TEST_KYLIN_FACT.LEFTJOIN_BUYER_COUNTRY_ABBR" */
        Assertions.assertTrue(checker.isColOfAntiLookup(model.getEffectiveCols().get(31)));
    }

    /**
     * This case shows a wrong result of using a non-initialized joinTables to check.
     */
    @Test
    void exception() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        NDataModel model = prepareModel(kylinConfig);

        NDataModel copyModel = JsonUtil.deepCopyQuietly(model, NDataModel.class);
        List<JoinTableDesc> joinTables = copyModel.getJoinTables();
        joinTables.forEach(joinTableDesc -> {
            if ("DEFAULT.TEST_ORDER".equals(joinTableDesc.getTable())
                    || "EDW.TEST_CAL_DT".equals(joinTableDesc.getTable())) {
                joinTableDesc.setFlattenable(JoinTableDesc.NORMALIZED);
            }
        });

        AntiFlatChecker checker = new AntiFlatChecker(joinTables, model);

        //============ The result should be true, but it is false. ======================
        /* CC {tableIdentity=DEFAULT.TEST_KYLIN_FACT, columnName=LEFTJOIN_BUYER_COUNTRY_ABBR,
                    expression=SUBSTR(BUYER_ACCOUNT.ACCOUNT_COUNTRY,0,1)}
           31 -> TblColRef: "DEFAULT.TEST_KYLIN_FACT.LEFTJOIN_BUYER_COUNTRY_ABBR" */
        Assertions.assertFalse(checker.isColOfAntiLookup(model.getEffectiveCols().get(31)));
    }
}

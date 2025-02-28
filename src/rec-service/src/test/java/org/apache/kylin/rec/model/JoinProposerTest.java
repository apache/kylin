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

package org.apache.kylin.rec.model;

import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.rec.AbstractContext;
import org.apache.kylin.rec.util.AccelerationUtil;
import org.junit.Assert;
import org.junit.Test;

import lombok.val;

public class JoinProposerTest extends NLocalWithSparkSessionTest {

    @Override
    public String getProject() {
        return "newten";
    }

    @Test
    public void testMergeReuseExistedModelAndAliasIsNotExactlyMatched() {
        // 1. create existed model
        final String sql = "select sum(ITEM_COUNT) as ITEM_CNT, count(SELLER_ACCOUNT.ACCOUNT_ID + 1), count(BUYER_ACCOUNT.ACCOUNT_ID + 1)\n"
                + "FROM TEST_KYLIN_FACT as TEST_KYLIN_FACT\n" + "LEFT JOIN TEST_ACCOUNT as SELLER_ACCOUNT\n"
                + "ON TEST_KYLIN_FACT.SELLER_ID = SELLER_ACCOUNT.ACCOUNT_ID\n" + "LEFT JOIN TEST_ORDER as TEST_ORDER\n"
                + "ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID\n" + "LEFT JOIN TEST_ACCOUNT as BUYER_ACCOUNT\n"
                + "ON TEST_ORDER.BUYER_ID = BUYER_ACCOUNT.ACCOUNT_ID\n";
        AbstractContext context = AccelerationUtil.runWithSmartContext(getTestConfig(), getProject(),
                new String[] { sql }, true);
        Assert.assertFalse(context.getAccelerateInfoMap().get(sql).isNotSucceed());
        Assert.assertEquals(1, context.getModelContexts().size());
        NDataModel originModel = context.getModelContexts().get(0).getTargetModel();
        Assert.assertEquals(2, originModel.getComputedColumnDescs().size());

        final String sql1 = "select sum(ITEM_COUNT) as ITEM_CNT\n" + "FROM TEST_KYLIN_FACT as TEST_KYLIN_FACT\n"
                + "LEFT JOIN TEST_ORDER as ORDERS\n" + "ON TEST_KYLIN_FACT.ORDER_ID = ORDERS.ORDER_ID\n"
                + "LEFT JOIN TEST_ACCOUNT as BUYER_ACCOUNT\n" + "ON ORDERS.BUYER_ID = BUYER_ACCOUNT.ACCOUNT_ID";
        AbstractContext context1 = AccelerationUtil.runWithSmartContext(getTestConfig(), getProject(),
                new String[] { sql1 }, true);
        Assert.assertFalse(context1.getAccelerateInfoMap().get(sql1).isNotSucceed());
        Assert.assertEquals(1, context1.getModelContexts().size());
        NDataModel modelFromPartialJoin = context1.getModelContexts().get(0).getTargetModel();
        Assert.assertTrue(originModel.getJoinsGraph().match(modelFromPartialJoin.getJoinsGraph(), Maps.newHashMap()));
    }

    @Test
    public void testProposeModel_wontChangeOriginModelJoins_whenExistsSameTable() {
        final String proj = "newten";
        // create new Model for this test.
        String sql = "select item_count, lstg_format_name, sum(price)\n" //
                + "from TEST_KYLIN_FACT inner join TEST_ACCOUNT as buyer_account on TEST_KYLIN_FACT.ORDER_ID = buyer_account.account_id\n"
                + "inner join TEST_ACCOUNT as seller_account on TEST_KYLIN_FACT.seller_id = seller_account.account_id\n "
                + "group by item_count, lstg_format_name\n" //
                + "order by item_count, lstg_format_name\n" //
                + "limit 10";
        AccelerationUtil.runWithSmartContext(getTestConfig(), getProject(), new String[] { sql }, true);
        val newModels = NDataModelManager.getInstance(getTestConfig(), proj).listAllModels();
        Assert.assertEquals(1, newModels.size());
        Assert.assertEquals(2, newModels.get(0).getJoinTables().size());
        val originModelGragh = newModels.get(0).getJoinsGraph();

        // secondly propose, still work in SMART-Mode
        AccelerationUtil.runWithSmartContext(getTestConfig(), getProject(), new String[] { sql }, true);
        val modelManager = NDataModelManager.getInstance(getTestConfig(), proj);
        val secondModels = modelManager.listAllModels();
        Assert.assertEquals(1, secondModels.size());
        Assert.assertEquals(2, secondModels.get(0).getJoinTables().size());
        val secondModelGragh = newModels.get(0).getJoinsGraph();
        Assert.assertEquals(originModelGragh, secondModelGragh);

        // set this project to semi-auto-Mode, change the join alias.
        // it will reuse this origin model and will not change this.
        val prjInstance = NProjectManager.getInstance(getTestConfig()).getProject(proj);
        getTestConfig().setProperty("kylin.metadata.semi-automatic-mode", "true");
        Assert.assertTrue(prjInstance.isSemiAutoMode());

        val originModels = NDataModelManager.getInstance(getTestConfig(), proj).listAllModels();
        val originModel = originModels.get(0);
        AccelerationUtil.runModelReuseContext(getTestConfig(), getProject(), new String[] { sql });
        val semiAutoModels = modelManager.listAllModels();
        Assert.assertEquals(1, semiAutoModels.size());
        Assert.assertEquals(2, semiAutoModels.get(0).getJoinTables().size());
        Assert.assertTrue(originModel.getJoinsGraph().match(semiAutoModels.get(0).getJoinsGraph(), Maps.newHashMap())
                && semiAutoModels.get(0).getJoinsGraph().match(originModel.getJoinsGraph(), Maps.newHashMap()));

    }
}

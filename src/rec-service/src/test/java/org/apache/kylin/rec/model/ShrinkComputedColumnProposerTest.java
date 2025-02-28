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
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.rec.util.AccelerationUtil;
import org.junit.Assert;
import org.junit.Test;

import lombok.val;

public class ShrinkComputedColumnProposerTest extends NLocalWithSparkSessionTest {
    @Override
    public String getProject() {
        return "newten";
    }

    @Test
    public void testFailComputedColumnShouldBeRemoved() {
        // 1. create a sql that cannot be proposed, for MOD FUNCTION
        // cannot be generated to ComputedColumn
        final String sql = "select sum(ITEM_COUNT) as ITEM_CNT, count(mod(SELLER_ACCOUNT.ACCOUNT_ID,ITEM_COUNT), count(BUYER_ACCOUNT.ACCOUNT_ID + 1)\n"
                + "FROM TEST_KYLIN_FACT as TEST_KYLIN_FACT\n" + "LEFT JOIN TEST_ACCOUNT as SELLER_ACCOUNT\n"
                + "ON TEST_KYLIN_FACT.SELLER_ID = SELLER_ACCOUNT.ACCOUNT_ID\n" + "LEFT JOIN TEST_ORDER as TEST_ORDER\n"
                + "ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID\n" + "LEFT JOIN TEST_ACCOUNT as BUYER_ACCOUNT\n"
                + "ON TEST_ORDER.BUYER_ID = BUYER_ACCOUNT.ACCOUNT_ID\n";

        final String sql1 = "select sum(ITEM_COUNT) as ITEM_CNT \n" + "FROM TEST_KYLIN_FACT as TEST_KYLIN_FACT\n"
                + "LEFT JOIN TEST_ACCOUNT as SELLER_ACCOUNT\n"
                + "ON TEST_KYLIN_FACT.SELLER_ID = SELLER_ACCOUNT.ACCOUNT_ID\n" + "LEFT JOIN TEST_ORDER as TEST_ORDER\n"
                + "ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID\n" + "LEFT JOIN TEST_ACCOUNT as BUYER_ACCOUNT\n"
                + "ON TEST_ORDER.BUYER_ID = BUYER_ACCOUNT.ACCOUNT_ID\n";
        val context = AccelerationUtil.runWithSmartContext(getTestConfig(), getProject(), new String[] { sql, sql1 },
                true);
        Assert.assertTrue(context.getAccelerateInfoMap().get(sql).isNotSucceed());
        Assert.assertFalse(context.getAccelerateInfoMap().get(sql1).isNotSucceed());
        Assert.assertEquals(1, context.getModelContexts().size());
        NDataModel originModel = context.getModelContexts().get(0).getTargetModel();
        Assert.assertEquals(0, originModel.getComputedColumnDescs().size());
    }
}

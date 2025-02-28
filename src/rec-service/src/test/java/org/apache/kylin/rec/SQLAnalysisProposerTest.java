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

package org.apache.kylin.rec;

import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rec.common.AccelerateInfo;
import org.apache.kylin.rec.common.AutoTestOnLearnKylinData;
import org.apache.kylin.rec.util.AccelerationUtil;
import org.junit.Assert;
import org.junit.Test;

public class SQLAnalysisProposerTest extends AutoTestOnLearnKylinData {

    @Test
    public void testNormalJion() {
        String[] sqls = new String[] {
                // normal query will be accelerated
                "SELECT \"TEST_KYLIN_FACT\".\"LSTG_FORMAT_NAME\" AS \"LSTG_FORMAT_NAME\",\n"
                        + "  SUM(\"TEST_KYLIN_FACT\".\"PRICE\") AS \"sum_price\"\n"
                        + "FROM \"DEFAULT\".\"TEST_KYLIN_FACT\" \"TEST_KYLIN_FACT\"\n"
                        + "GROUP BY \"TEST_KYLIN_FACT\".\"LSTG_FORMAT_NAME\"" };

        AbstractContext context = AccelerationUtil.runWithSmartContext(getTestConfig(), "newten", sqls, true);
        final List<AbstractContext.ModelContext> modelContexts = context.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        final Map<String, AccelerateInfo> accelerateInfoMap = context.getAccelerateInfoMap();
        Assert.assertTrue(!accelerateInfoMap.get(sqls[0]).isFailed() && !accelerateInfoMap.get(sqls[0]).isPending());
    }

    @Test
    public void testCircledJoinSQL() {

        String[] sqls = new String[] { //

                "SELECT \"TEST_KYLIN_FACT\".\"LSTG_FORMAT_NAME\" AS \"LSTG_FORMAT_NAME\",\n"
                        + "  SUM(\"TEST_KYLIN_FACT\".\"PRICE\") AS \"sum_price\"\n"
                        + "FROM \"DEFAULT\".\"TEST_KYLIN_FACT\" \"TEST_KYLIN_FACT\"\n"
                        + "INNER JOIN TEST_ORDER as TEST_ORDER\n"
                        + "ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID\n"
                        + "INNER JOIN TEST_ACCOUNT as BUYER_ACCOUNT\n"
                        + "ON TEST_ORDER.BUYER_ID = BUYER_ACCOUNT.ACCOUNT_ID\n"
                        + "INNER JOIN TEST_ACCOUNT as SELLER_ACCOUNT\n"
                        + "ON TEST_KYLIN_FACT.SELLER_ID = SELLER_ACCOUNT.ACCOUNT_ID "
                        + "AND SELLER_ACCOUNT.ACCOUNT_ID = BUYER_ACCOUNT.ACCOUNT_ID\n"
                        + "GROUP BY \"TEST_KYLIN_FACT\".\"LSTG_FORMAT_NAME\"",

                "SELECT *\n" + "FROM\n" + "TEST_KYLIN_FACT INNER JOIN TEST_ACCOUNT ON SELLER_ID = ACCOUNT_ID\n"
                        + "INNER JOIN EDW.TEST_CAL_DT ON TEST_KYLIN_FACT.CAL_DT=TEST_CAL_DT.CAL_DT\n"
                        + "INNER JOIN TEST_ORDER ON TEST_ACCOUNT.ACCOUNT_ID = TEST_ORDER.BUYER_ID AND TEST_CAL_DT.CAL_DT = TEST_ORDER.TEST_DATE_ENC\n" };
        AbstractContext context = AccelerationUtil.runWithSmartContext(getTestConfig(), "newten", sqls, true);
        final List<AbstractContext.ModelContext> modelContexts = context.getModelContexts();
        Assert.assertEquals(4, modelContexts.size());
        final Map<String, AccelerateInfo> accelerateInfoMap = context.getAccelerateInfoMap();
        Assert.assertTrue(!accelerateInfoMap.get(sqls[0]).isFailed() && !accelerateInfoMap.get(sqls[0]).isPending());
        Assert.assertTrue(!accelerateInfoMap.get(sqls[1]).isFailed() && !accelerateInfoMap.get(sqls[1]).isPending());
    }

    @Test
    public void testCircledNonEquiJoinSQL() {

        String[] sqls = new String[] { //
                "SELECT *\n" + "FROM\n" + "TEST_KYLIN_FACT INNER JOIN TEST_ACCOUNT ON SELLER_ID = ACCOUNT_ID\n"
                        + "INNER JOIN EDW.TEST_CAL_DT ON TEST_KYLIN_FACT.CAL_DT=TEST_CAL_DT.CAL_DT\n"
                        + "LEFT JOIN TEST_ORDER ON TEST_ACCOUNT.ACCOUNT_ID = TEST_ORDER.BUYER_ID AND\n"
                        + "TEST_CAL_DT.CAL_DT = TEST_ORDER.TEST_DATE_ENC AND\n" + "TEST_ORDER.BUYER_ID <> 10000000\n" };
        KylinConfig conf = getTestConfig();
        conf.setProperty("kylin.query.non-equi-join-model-enabled", "TRUE");
        conf.setProperty("kylin.model.non-equi-join-recommendation-enabled", "TRUE");
        AbstractContext context = AccelerationUtil.runWithSmartContext(conf, "newten", sqls, true);
        final List<AbstractContext.ModelContext> modelContexts = context.getModelContexts();
        Assert.assertEquals(2, modelContexts.size());
        final Map<String, AccelerateInfo> accelerateInfoMap = context.getAccelerateInfoMap();
        Assert.assertTrue(!accelerateInfoMap.get(sqls[0]).isFailed() && !accelerateInfoMap.get(sqls[0]).isPending());
    }
}

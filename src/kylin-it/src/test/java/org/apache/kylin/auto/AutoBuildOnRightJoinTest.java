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

package org.apache.kylin.auto;

import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.query.engine.QueryExec;
import org.apache.kylin.query.engine.data.QueryResult;
import org.apache.kylin.rec.AbstractContext;
import org.apache.kylin.rec.util.AccelerationUtil;
import org.apache.kylin.util.SuggestTestBase;
import org.junit.Assert;
import org.junit.Test;

public class AutoBuildOnRightJoinTest extends SuggestTestBase {

    @Test
    public void testRightJoin() throws Exception {
        final int TEST_SQL_CNT = 3;
        for (int i = 0; i < TEST_SQL_CNT; i++) {
            List<Pair<String, String>> queries = fetchQueries("query/sql_join/sql_right_join", i, i + 1);
            AbstractContext context = proposeWithSmartMaster(queries);
            List<AbstractContext.ModelContext> modelContexts = context.getModelContexts();
            // ensure only one model is created for right join
            Assert.assertEquals(1, modelContexts.size());
            AbstractContext.ModelContext modelContext = modelContexts.get(0);
            NDataModel dataModel = modelContext.getTargetModel();
            Assert.assertNotNull(dataModel);
            Assert.assertFalse(dataModel.getJoinTables().isEmpty());
            Assert.assertEquals("LEFT", dataModel.getJoinTables().get(0).getJoin().getType());
            IndexPlan indexPlan = modelContext.getTargetIndexPlan();
            Assert.assertNotNull(indexPlan);
        }
    }

    // https://olapio.atlassian.net/browse/KE-19473
    @Test
    public void testRightJoinWhenTwoModelHaveSameMeasure() throws Exception {
        // prepare two model have different fact table and same measure
        String query1 = "select sum(test_kylin_fact.price) from TEST_ORDER  "
                + "left JOIN  test_kylin_fact ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID  "
                + "left join test_measure on test_kylin_fact.ITEM_COUNT = test_measure.ID3 "
                + "left join test_account on TEST_KYLIN_FACT.seller_id = test_account.account_id "
                + "where test_kylin_fact.LSTG_FORMAT_NAME = 'FP-GTC' group by test_kylin_fact.CAL_DT order by test_kylin_fact.CAL_DT";
        String query2 = "select sum(test_kylin_fact.price) from test_kylin_fact";
        AccelerationUtil.runWithSmartContext(getTestConfig(), getProject(), new String[] { query1, query2 }, true);
        buildAllModels(kylinConfig, getProject());

        // query
        QueryResult queryResult = new QueryExec(getProject(), KylinConfig.getInstanceFromEnv())
                .executeQuery("select sum(test_kylin_fact.price) from test_kylin_fact "
                        + "right JOIN TEST_ORDER  ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID "
                        + "left  join test_measure on test_kylin_fact.ITEM_COUNT = test_measure.ID3 "
                        + "left join test_account on TEST_KYLIN_FACT.seller_id = test_account.account_id "
                        + "where test_kylin_fact.LSTG_FORMAT_NAME = 'FP-GTC' group by test_kylin_fact.CAL_DT order by test_kylin_fact.CAL_DT");
        QueryResult queryResult2 = new QueryExec(getProject(), KylinConfig.getInstanceFromEnv()).executeQuery(query1);
        Assert.assertEquals(862.69, Double.parseDouble(queryResult.getRows().get(0).get(0)), 0.01);
        Assert.assertEquals(862.69, Double.parseDouble(queryResult2.getRows().get(0).get(0)), 0.01);
    }

    private AbstractContext proposeWithSmartMaster(List<Pair<String, String>> queries) {
        String[] sqls = queries.stream().map(Pair::getSecond).toArray(String[]::new);
        return AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), sqls, true);
    }
}

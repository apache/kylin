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

package org.apache.kylin.newten;

import java.sql.SQLException;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.query.engine.QueryExec;
import org.apache.kylin.query.relnode.ContextUtil;
import org.apache.kylin.query.relnode.OlapContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class EnhancedAggPushDownTest extends NLocalWithSparkSessionTest {

    @Override
    @Before
    public void setUp() throws Exception {
        JobContextUtil.cleanUp();
        super.setUp();
        overwriteSystemProp("kylin.query.enhanced-agg-pushdown-enabled", "true");
        this.createTestMetadata("src/test/resources/ut_meta/enhanced_agg_pushdown");

        JobContextUtil.getJobContext(getTestConfig());
    }

    public String[] getOverlay() {
        return new String[] { "src/test/resources/ut_meta/enhanced_agg_pushdown" };
    }

    @Override
    @After
    public void tearDown() throws Exception {
        JobContextUtil.cleanUp();
        cleanupTestMetadata();
    }

    @Test
    public void testBasic() {
        String sql = "select SELLER_ID,kcg.CATEG_LVL3_ID,sum(price),count(distinct TEST_KYLIN_FACT.ORDER_ID) from TEST_KYLIN_FACT\n"
                + "inner join TEST_ORDER on TEST_ORDER.ORDER_ID = TEST_KYLIN_FACT.ORDER_ID\n"
                + "inner join TEST_CATEGORY_GROUPINGS as tcg on TEST_KYLIN_FACT.LEAF_CATEG_ID = tcg.LEAF_CATEG_ID\n"
                + "inner join KYLIN_CATEGORY_GROUPINGS as kcg on TEST_KYLIN_FACT.LEAF_CATEG_ID = kcg.LEAF_CATEG_ID \n"
                + "group by SELLER_ID,kcg.CATEG_LVL3_ID    \n" + "order by SELLER_ID,kcg.CATEG_LVL3_ID desc limit 9";
        List<OlapContext> contexts = executeSql(sql);
        Assert.assertEquals(1L, contexts.get(0).getStorageContext().getBatchCandidate().getLayoutId(), 0);
        Assert.assertEquals("DEFAULT.KYLIN_CATEGORY_GROUPINGS",
                contexts.get(1).getStorageContext().getLookupCandidate().getTable());
    }

    @Test
    public void testCountDistinct() {
        String sql = "select SELLER_ID,kcg.CATEG_LVL3_ID,count(distinct TEST_KYLIN_FACT.ORDER_ID) from TEST_KYLIN_FACT\n"
                + "inner join TEST_ORDER on TEST_ORDER.ORDER_ID = TEST_KYLIN_FACT.ORDER_ID\n"
                + "inner join TEST_CATEGORY_GROUPINGS as tcg on TEST_KYLIN_FACT.LEAF_CATEG_ID = tcg.LEAF_CATEG_ID\n"
                + "inner join KYLIN_CATEGORY_GROUPINGS as kcg on TEST_KYLIN_FACT.LEAF_CATEG_ID = kcg.LEAF_CATEG_ID \n"
                + "group by SELLER_ID,kcg.CATEG_LVL3_ID    \n" + "order by SELLER_ID,kcg.CATEG_LVL3_ID desc limit 9";
        List<OlapContext> contexts = executeSql(sql);
        Assert.assertEquals(1L, contexts.get(0).getStorageContext().getBatchCandidate().getLayoutId(), 0);
        Assert.assertEquals("DEFAULT.KYLIN_CATEGORY_GROUPINGS",
                contexts.get(1).getStorageContext().getLookupCandidate().getTable());
    }

    @Test
    public void testMultiJoinSnapshot() {
        String sql = "select SELLER_ID,kcg.CATEG_LVL3_ID,kcg2.CATEG_LVL3_ID,sum(price),count(distinct TEST_KYLIN_FACT.ORDER_ID) from TEST_KYLIN_FACT\n"
                + "inner join TEST_ORDER on TEST_ORDER.ORDER_ID = TEST_KYLIN_FACT.ORDER_ID\n"
                + "inner join TEST_CATEGORY_GROUPINGS as tcg on TEST_KYLIN_FACT.LEAF_CATEG_ID = tcg.LEAF_CATEG_ID\n"
                + "inner join KYLIN_CATEGORY_GROUPINGS as kcg on TEST_KYLIN_FACT.LEAF_CATEG_ID = kcg.LEAF_CATEG_ID \n"
                + "inner join KYLIN_CATEGORY_GROUPINGS as kcg2 on TEST_KYLIN_FACT.LEAF_CATEG_ID = kcg2.LEAF_CATEG_ID \n"
                + "group by SELLER_ID,kcg.CATEG_LVL3_ID,kcg2.CATEG_LVL3_ID  \n"
                + "order by SELLER_ID,kcg.CATEG_LVL3_ID desc limit 9";
        List<OlapContext> contexts = executeSql(sql);
        Assert.assertEquals(1L, contexts.get(0).getStorageContext().getBatchCandidate().getLayoutId(), 0);
        Assert.assertEquals("DEFAULT.KYLIN_CATEGORY_GROUPINGS",
                contexts.get(1).getStorageContext().getLookupCandidate().getTable());
        Assert.assertEquals("DEFAULT.KYLIN_CATEGORY_GROUPINGS",
                contexts.get(2).getStorageContext().getLookupCandidate().getTable());
    }

    @Test
    public void testSumExpression() {
        overwriteSystemProp("kylin.query.convert-sum-expression-enabled", "true");
        String sql = "select SELLER_ID,kcg.CATEG_LVL3_ID,sum(case when TRANS_ID > 10 then price else 1 end),count(distinct TEST_KYLIN_FACT.ORDER_ID) from TEST_KYLIN_FACT\n"
                + "inner join TEST_ORDER on TEST_ORDER.ORDER_ID = TEST_KYLIN_FACT.ORDER_ID\n"
                + "inner join TEST_CATEGORY_GROUPINGS as tcg on TEST_KYLIN_FACT.LEAF_CATEG_ID = tcg.LEAF_CATEG_ID\n"
                + "inner join KYLIN_CATEGORY_GROUPINGS as kcg on TEST_KYLIN_FACT.LEAF_CATEG_ID = kcg.LEAF_CATEG_ID \n"
                + "group by SELLER_ID,kcg.CATEG_LVL3_ID    \n" + "order by SELLER_ID,kcg.CATEG_LVL3_ID desc limit 9";
        List<OlapContext> contexts = executeSql(sql);
        Assert.assertEquals(1L, contexts.get(0).getStorageContext().getBatchCandidate().getLayoutId(), 0);
        Assert.assertEquals("DEFAULT.KYLIN_CATEGORY_GROUPINGS",
                contexts.get(1).getStorageContext().getLookupCandidate().getTable());
        Assert.assertEquals(1L, contexts.get(2).getStorageContext().getBatchCandidate().getLayoutId(), 0);
        Assert.assertEquals("DEFAULT.KYLIN_CATEGORY_GROUPINGS",
                contexts.get(3).getStorageContext().getLookupCandidate().getTable());
    }

    @Test
    public void testSumExpressionAndCountDistinct() {
        overwriteSystemProp("kylin.query.convert-count-distinct-expression-enabled", "true");
        overwriteSystemProp("kylin.query.convert-sum-expression-enabled", "true");
        String sql = "select SELLER_ID,kcg.CATEG_LVL3_ID,sum(price),sum(case when TRANS_ID > 10 then price else 1 end)\n"
                + ",count(distinct TEST_KYLIN_FACT.ORDER_ID),count(distinct case when TRANS_ID > 10 then price else 1 end) from TEST_KYLIN_FACT\n"
                + "inner join TEST_ORDER on TEST_ORDER.ORDER_ID = TEST_KYLIN_FACT.ORDER_ID  \n"
                + "inner join TEST_CATEGORY_GROUPINGS as tcg on TEST_KYLIN_FACT.LEAF_CATEG_ID = tcg.LEAF_CATEG_ID\n"
                + "inner join KYLIN_CATEGORY_GROUPINGS as kcg on TEST_KYLIN_FACT.LEAF_CATEG_ID = kcg.LEAF_CATEG_ID \n"
                + "group by SELLER_ID,kcg.CATEG_LVL3_ID      \n" + "order by SELLER_ID,kcg.CATEG_LVL3_ID desc limit 9";
        List<OlapContext> contexts = executeSql(sql);
        Assert.assertEquals(1L, contexts.get(0).getStorageContext().getBatchCandidate().getLayoutId(), 0);
        Assert.assertEquals("DEFAULT.KYLIN_CATEGORY_GROUPINGS",
                contexts.get(1).getStorageContext().getLookupCandidate().getTable());
        Assert.assertEquals(1L, contexts.get(2).getStorageContext().getBatchCandidate().getLayoutId(), 0);
        Assert.assertEquals("DEFAULT.KYLIN_CATEGORY_GROUPINGS",
                contexts.get(3).getStorageContext().getLookupCandidate().getTable());
    }

    private List<OlapContext> executeSql(String sql) {
        QueryExec queryExec = new QueryExec(getProject(), KylinConfig.getInstanceFromEnv());
        try {
            QueryContext.current().setProject(getProject());
            queryExec.executeQuery(sql);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof SQLException);
            Assert.assertTrue(queryExec.getSparderQueryOptimizedExceptionMsg().contains("does not exist"));
        }
        return ContextUtil.listContexts();
    }

    @Override
    public String getProject() {
        return "test_agg_pushdown";
    }

}

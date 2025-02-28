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

package org.apache.kylin.query;

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.query.engine.QueryExec;
import org.apache.kylin.query.relnode.ContextUtil;
import org.apache.kylin.rec.AbstractContext;
import org.apache.kylin.rec.util.AccelerationUtil;
import org.apache.kylin.util.ExecAndCompExt;
import org.apache.kylin.util.SuggestTestBase;
import org.apache.spark.sql.SparderEnv;
import org.junit.Test;

import lombok.val;

public class QueryLayoutChooserTest extends SuggestTestBase {

    @Test
    public void testDimensionAsMeasureCountDistinctDerivedFkFromPk() throws Exception {
        val sql1 = new String[] {
                "select cal_dt, account_id, count(*) " + "from test_kylin_fact inner join test_account "
                        + "on test_kylin_fact.seller_id = test_account.account_id "
                        + "group by test_kylin_fact.cal_dt, test_account.account_id" };

        val context = AccelerationUtil.runWithSmartContext(getTestConfig(), getProject(), sql1, true);
        AbstractContext.ModelContext modelContext = context.getModelContexts().get(0);
        String modelId = modelContext.getTargetModel().getId();
        UnitOfWork.doInTransactionWithRetry(() -> NDataModelManager.getInstance(getTestConfig(), getProject())
                .updateDataModel(modelId, copyForWrite -> {
                    copyForWrite.getJoinTables().get(0).setKind(NDataModel.TableKind.LOOKUP);
                }), getProject());

        buildAllModels(getTestConfig(), getProject());

        val sql2 = "select cal_dt, count(distinct seller_id) " + "from test_kylin_fact inner join test_account "
                + "on test_kylin_fact.seller_id = test_account.account_id " + "group by cal_dt";

        List<Pair<String, String>> query = new ArrayList<>();
        query.add(new Pair<>("count_distinct_derived_fk_from_pk", sql2));
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        ExecAndCompExt.execAndCompare(query, getProject(), ExecAndCompExt.CompareLevel.SAME, "inner");
    }

    @Test
    public void testDimensionAsMeasureCountDistinctDerivedOnLookup() throws Exception {
        val sql1 = new String[] { "select cal_dt, seller_id, count(*) from test_kylin_fact left join test_account "
                + "on test_kylin_fact.seller_id = test_account.account_id "
                + "group by test_kylin_fact.cal_dt, test_kylin_fact.seller_id" };
        val context = AccelerationUtil.runWithSmartContext(getTestConfig(), getProject(), sql1, true);
        AbstractContext.ModelContext modelContext = context.getModelContexts().get(0);
        String modelId = modelContext.getTargetModel().getId();
        UnitOfWork.doInTransactionWithRetry(() -> NDataModelManager.getInstance(getTestConfig(), getProject())
                .updateDataModel(modelId, copyForWrite -> {
                    copyForWrite.getJoinTables().get(0).setKind(NDataModel.TableKind.LOOKUP);
                }), getProject());

        buildAllModels(getTestConfig(), getProject());

        val sql2 = "select cal_dt, count(distinct ACCOUNT_COUNTRY) " + "from test_kylin_fact left join test_account "
                + "on test_kylin_fact.seller_id = test_account.account_id " + "group by cal_dt";

        List<Pair<String, String>> query = new ArrayList<>();
        query.add(new Pair<>("count_distinct_derived_fk_from_pk", sql2));
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        ExecAndCompExt.execAndCompare(query, getProject(), ExecAndCompExt.CompareLevel.SAME, "left");
    }

    @Test
    public void testDimensionAsMeasureCountDistinctComplexExpr() throws Exception {
        val sql1 = new String[] {
                "select cal_dt, price, item_count from test_kylin_fact group by cal_dt, price, item_count" };
        AccelerationUtil.runWithSmartContext(getTestConfig(), getProject(), sql1, true);
        buildAllModels(getTestConfig(), getProject());

        val sql2 = "select count(distinct (case when cal_dt > date'2013-01-01' then price else item_count end)) from test_kylin_fact";

        List<Pair<String, String>> query = new ArrayList<>();
        query.add(new Pair<>("count_distinct_complex_expr", sql2));
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        ExecAndCompExt.execAndCompare(query, getProject(), ExecAndCompExt.CompareLevel.SAME, "left");
    }

    @Test
    public void testCountDistinctExprFallback() throws Exception {
        overwriteSystemProp("kylin.query.convert-count-distinct-expression-enabled", "true");
        val sql1 = new String[] { "select cal_dt, price from test_kylin_fact group by cal_dt, price" };
        AccelerationUtil.runWithSmartContext(getTestConfig(), getProject(), sql1, true);

        buildAllModels(getTestConfig(), getProject());

        val sql2 = "select count(distinct (case when cal_dt > date'2013-01-01' then price else null end)) from test_kylin_fact";

        List<Pair<String, String>> query = new ArrayList<>();
        query.add(new Pair<>("count_distinct_expr_fallback", sql2));
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        ExecAndCompExt.execAndCompare(query, getProject(), ExecAndCompExt.CompareLevel.SAME, "left");
    }

    @Test
    public void testAggpushdownWithSemiJoin() throws Exception {
        val sql1 = new String[] {
                // agg index on test_acc
                "select max(account_id), account_buyer_level from test_account group by account_buyer_level",
                // table index on test_acc
                "select * from test_account",
                // table index on test_kylin_fact
                "select * from test_kylin_fact" };
        AccelerationUtil.runWithSmartContext(getTestConfig(), getProject(), sql1, true);
        buildAllModels(getTestConfig(), getProject());

        val sql2 = "select sum(price)\n" + "from test_kylin_fact inner join test_account\n"
                + "on seller_id = account_id\n" + "where cal_dt <> date'2012-01-01'\n"
                + "and account_id in (select max(account_id) - 1000 from test_account)";

        List<Pair<String, String>> query = new ArrayList<>();
        query.add(new Pair<>("aggPushdownOnSemiJoin", sql2));
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        ExecAndCompExt.execAndCompare(query, getProject(), ExecAndCompExt.CompareLevel.SAME, "inner");
    }

    @Test
    public void testAggPushDownWithMultipleAgg() throws Exception {
        val sql1 = new String[] { "select * from TEST_KYLIN_FACT", "select * from TEST_ACCOUNT",
                "select * from TEST_COUNTRY" };
        AccelerationUtil.runWithSmartContext(getTestConfig(), getProject(), sql1, true);
        buildAllModels(getTestConfig(), getProject());

        val sql2 = "select d.ACCOUNT_CONTACT from\n" + "TEST_KYLIN_FACT c\n" + "inner join (\n"
                + "select a.ACCOUNT_ID, a.ACCOUNT_COUNTRY, e.COUNTRY, a.ACCOUNT_CONTACT\n" + "from TEST_ACCOUNT a\n"
                + "inner join TEST_COUNTRY e\n" + "on a.ACCOUNT_COUNTRY = e.COUNTRY\n"
                + "inner join TEST_KYLIN_FACT b\n" + "on a.ACCOUNT_ID = b.SELLER_ID\n"
                + "and b.LSTG_FORMAT_NAME = e.NAME\n"
                + "group by a.ACCOUNT_ID, a.ACCOUNT_COUNTRY, e.COUNTRY, a.ACCOUNT_CONTACT\n"
                + ") d on c.SELLER_ID = d.ACCOUNT_ID\n" + "group by d.ACCOUNT_CONTACT";

        List<Pair<String, String>> query = new ArrayList<>();
        query.add(new Pair<>("aggPushdownWithMultipleAgg", sql2));
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        ExecAndCompExt.execAndCompare(query, getProject(), ExecAndCompExt.CompareLevel.SAME, "default");
    }

    @Test
    public void testGetNativeRealizationsWhenThruDerivedDimsFromFactTable() throws Exception {
        val sql1 = new String[] {
                "select TEST_ORDER.ORDER_ID from TEST_KYLIN_FACT inner join TEST_ORDER on TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID" };
        AccelerationUtil.runWithSmartContext(getTestConfig(), getProject(), sql1, true);
        buildAllModels(getTestConfig(), getProject());

        val sql2 = "select TEST_KYLIN_FACT.ORDER_ID from TEST_KYLIN_FACT inner join TEST_ORDER on TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID";
        QueryExec queryExec = new QueryExec(getProject(), NProjectManager.getProjectConfig(getProject()), true);
        queryExec.executeQuery(sql2);
        ContextUtil.getNativeRealizations();
        ContextUtil.clearThreadLocalContexts();
    }
}

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
package org.apache.kylin.query.util;

import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class PushDownUtilTest {
    @Test
    public void testSchemaCompletion() {
        String sql1 = "SELECT a \n"+
                "FROM a.KYLIN_SALES as KYLIN_SALES\n" +
                "INNER JOIN \"A\".KYLIN_ACCOUNT as BUYER_ACCOUNT\n" +
                "ON KYLIN_SALES.BUYER_ID = BUYER_ACCOUNT.ACCOUNT_ID\n" +
                "INNER JOIN \"KYLIN_COUNTRY\" as BUYER_COUNTRY\n" +
                "ON BUYER_ACCOUNT.ACCOUNT_COUNTRY = BUYER_COUNTRY.COUNTRY";
        String sql2 = "select * from DB2.t,DB2.tt,ttt";

        String sql3 = "SELECT t1.week_beg_dt, t1.sum_price, t2.cnt\n" +
                "FROM (\n" +
                "  select test_cal_dt.week_beg_dt, sum(price) as sum_price\n" +
                "  from DB1.\"test_kylin_fact\"\n" +
                "  inner JOIN test_cal_dt as test_cal_dt\n" +
                "  ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt\n" +
                "  inner JOIN test_category_groupings\n" +
                "  ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id\n" +
                "  inner JOIN test_sites as test_sites\n" +
                "  ON test_kylin_fact.lstg_site_id = test_sites.site_id\n" +
                "  group by test_cal_dt.week_beg_dt\n" +
                ") t1\n" +
                "inner join  (\n" +
                "  select test_cal_dt.week_beg_dt, count(*) as cnt\n" +
                "  from DB1.test_kylin_fact\n" +
                "  inner JOIN test_cal_dt as test_cal_dt\n" +
                "  ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt\n" +
                "  inner JOIN test_category_groupings\n" +
                "  ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id\n" +
                "  inner JOIN test_sites as test_sites\n" +
                "  ON test_kylin_fact.lstg_site_id = test_sites.site_id\n" +
                "  group by test_cal_dt.week_beg_dt\n" +
                ") t2\n" +
                "on t1.week_beg_dt=t2.week_beg_dt";

        String exceptSQL1 = "SELECT a \n" +
                "FROM a.KYLIN_SALES as KYLIN_SALES\n" +
                "INNER JOIN \"A\".KYLIN_ACCOUNT as BUYER_ACCOUNT\n" +
                "ON KYLIN_SALES.BUYER_ID = BUYER_ACCOUNT.ACCOUNT_ID\n" +
                "INNER JOIN EDW.\"KYLIN_COUNTRY\" as BUYER_COUNTRY\n" +
                "ON BUYER_ACCOUNT.ACCOUNT_COUNTRY = BUYER_COUNTRY.COUNTRY";

        String exceptSQL2 = "select * from DB2.t,DB2.tt,EDW.ttt";

        String exceptSQL3 = "SELECT t1.week_beg_dt, t1.sum_price, t2.cnt\n" +
                "FROM (\n" +
                "  select test_cal_dt.week_beg_dt, sum(price) as sum_price\n" +
                "  from DB1.\"test_kylin_fact\"\n" +
                "  inner JOIN EDW.test_cal_dt as test_cal_dt\n" +
                "  ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt\n" +
                "  inner JOIN EDW.test_category_groupings\n" +
                "  ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id\n" +
                "  inner JOIN EDW.test_sites as test_sites\n" +
                "  ON test_kylin_fact.lstg_site_id = test_sites.site_id\n" +
                "  group by test_cal_dt.week_beg_dt\n" +
                ") t1\n" +
                "inner join  (\n" +
                "  select test_cal_dt.week_beg_dt, count(*) as cnt\n" +
                "  from DB1.test_kylin_fact\n" +
                "  inner JOIN EDW.test_cal_dt as test_cal_dt\n" +
                "  ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt\n" +
                "  inner JOIN EDW.test_category_groupings\n" +
                "  ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id\n" +
                "  inner JOIN EDW.test_sites as test_sites\n" +
                "  ON test_kylin_fact.lstg_site_id = test_sites.site_id\n" +
                "  group by test_cal_dt.week_beg_dt\n" +
                ") t2\n" +
                "on t1.week_beg_dt=t2.week_beg_dt";
        Assert.assertEquals(exceptSQL1, PushDownUtil.schemaCompletion(sql1, "EDW"));
        Assert.assertEquals(exceptSQL2, PushDownUtil.schemaCompletion(sql2, "EDW"));
        Assert.assertEquals(exceptSQL3, PushDownUtil.schemaCompletion(sql3, "EDW"));
    }

    @Test
    public void testReplaceIdentifierInExpr() {
        {
            String ret = PushDownUtil.replaceIdentifierInExpr("x * y", null, false);
            Assert.assertEquals("x * y", ret);
        }
        {
            String ret = PushDownUtil.replaceIdentifierInExpr("x_3 * y_3", "b_2", false);
            Assert.assertEquals("b_2.x_3 * b_2.y_3", ret);
        }
        {
            String ret = PushDownUtil.replaceIdentifierInExpr("substr(x,1,3)>y", "c", true);
            Assert.assertEquals("substr(c.x,1,3)>c.y", ret);
        }
        {
            String ret = PushDownUtil.replaceIdentifierInExpr("strcmp(substr(x,1,3),y)", "c", true);
            Assert.assertEquals("strcmp(substr(c.x,1,3),c.y)", ret);
        }
        {
            String ret = PushDownUtil.replaceIdentifierInExpr("strcmp(substr(x,1,3),y)", null, true);
            Assert.assertEquals("strcmp(substr(x,1,3),y)", ret);
        }
        {
            String ret = PushDownUtil.replaceIdentifierInExpr("strcmp(substr(x,1,3),y)", null, false);
            Assert.assertEquals("strcmp(substr(x,1,3),y)", ret);
        }
    }

    @Test
    public void testRestoreComputedColumnToExpr() {

        ComputedColumnDesc computedColumnDesc = Mockito.mock(ComputedColumnDesc.class);
        Mockito.when(computedColumnDesc.getColumnName()).thenReturn("DEAL_AMOUNT");
        Mockito.when(computedColumnDesc.getExpression()).thenReturn("price * number");
        {
            String ret = PushDownUtil.restoreComputedColumnToExpr(
                    "select DEAL_AMOUNT from DB.TABLE group by date order by DEAL_AMOUNT", computedColumnDesc);
            Assert.assertEquals("select (price * number) from DB.TABLE group by date order by (price * number)", ret);
        }
        {
            String ret = PushDownUtil.restoreComputedColumnToExpr(
                    "select DEAL_AMOUNT as DEAL_AMOUNT from DB.TABLE group by date order by DEAL_AMOUNT",
                    computedColumnDesc);
            Assert.assertEquals(
                    "select (price * number) as DEAL_AMOUNT from DB.TABLE group by date order by (price * number)",
                    ret);
        }
        {
            String ret = PushDownUtil.restoreComputedColumnToExpr(
                    "select \"DEAL_AMOUNT\" AS deal_amount from DB.TABLE group by date order by DEAL_AMOUNT",
                    computedColumnDesc);
            Assert.assertEquals(
                    "select (price * number) AS deal_amount from DB.TABLE group by date order by (price * number)",
                    ret);
        }
        {
            String ret = PushDownUtil.restoreComputedColumnToExpr(
                    "select x.DEAL_AMOUNT AS deal_amount from DB.TABLE x group by date order by x.DEAL_AMOUNT",
                    computedColumnDesc);
            Assert.assertEquals(
                    "select (x.price * x.number) AS deal_amount from DB.TABLE x group by date order by (x.price * x.number)",
                    ret);
        }
    }
}

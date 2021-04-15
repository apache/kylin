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

import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Assert;
import org.junit.Test;

public class PushDownUtilTest {
    @Test
    public void testSchemaCompletion() throws SqlParseException {
        String sql1 = "SELECT a \n" + "FROM a.KYLIN_SALES as KYLIN_SALES\n"
                + "INNER JOIN \"A\".KYLIN_ACCOUNT as BUYER_ACCOUNT\n"
                + "ON KYLIN_SALES.BUYER_ID = BUYER_ACCOUNT.ACCOUNT_ID\n"
                + "INNER JOIN \"KYLIN_COUNTRY\" as BUYER_COUNTRY\n"
                + "ON BUYER_ACCOUNT.ACCOUNT_COUNTRY = BUYER_COUNTRY.COUNTRY LIMIT 5";
        String sql2 = "select * from DB2.t,DB2.tt,ttt";

        String sql3 = "SELECT t1.week_beg_dt, t1.sum_price, t2.cnt\n" + "FROM (\n"
                + "  select test_cal_dt.week_beg_dt, sum(price) as sum_price\n" + "  from DB1.\"test_kylin_fact\"\n"
                + "  inner JOIN test_cal_dt as test_cal_dt\n" + "  ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt\n"
                + "  inner JOIN test_category_groupings\n"
                + "  ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id\n"
                + "  inner JOIN test_sites as test_sites\n" + "  ON test_kylin_fact.lstg_site_id = test_sites.site_id\n"
                + "  where price > 100\n" + "  group by test_cal_dt.week_beg_dt\n" + "  having sum(price) > 1000\n"
                + "  order by sum(price)\n" + ") t1\n" + "inner join  (\n"
                + "  select test_cal_dt.week_beg_dt, count(*) as cnt\n" + "  from DB1.test_kylin_fact\n"
                + "  inner JOIN test_cal_dt as test_cal_dt\n" + "  ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt\n"
                + "  inner JOIN test_category_groupings\n"
                + "  ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id\n"
                + "  inner JOIN test_sites as test_sites\n" + "  ON test_kylin_fact.lstg_site_id = test_sites.site_id\n"
                + "  group by test_cal_dt.week_beg_dt\n" + ") t2\n" + "on t1.week_beg_dt=t2.week_beg_dt limit 5";

        String exceptSQL1 = "SELECT a \n" + "FROM a.KYLIN_SALES as KYLIN_SALES\n"
                + "INNER JOIN \"A\".KYLIN_ACCOUNT as BUYER_ACCOUNT\n"
                + "ON KYLIN_SALES.BUYER_ID = BUYER_ACCOUNT.ACCOUNT_ID\n"
                + "INNER JOIN EDW.\"KYLIN_COUNTRY\" as BUYER_COUNTRY\n"
                + "ON BUYER_ACCOUNT.ACCOUNT_COUNTRY = BUYER_COUNTRY.COUNTRY LIMIT 5";

        String exceptSQL2 = "select * from DB2.t,DB2.tt,EDW.ttt";

        String exceptSQL3 = "SELECT t1.week_beg_dt, t1.sum_price, t2.cnt\n" + "FROM (\n"
                + "  select test_cal_dt.week_beg_dt, sum(price) as sum_price\n" + "  from DB1.\"test_kylin_fact\"\n"
                + "  inner JOIN EDW.test_cal_dt as test_cal_dt\n" + "  ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt\n"
                + "  inner JOIN EDW.test_category_groupings\n"
                + "  ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id\n"
                + "  inner JOIN EDW.test_sites as test_sites\n"
                + "  ON test_kylin_fact.lstg_site_id = test_sites.site_id\n" + "  where price > 100\n"
                + "  group by test_cal_dt.week_beg_dt\n" + "  having sum(price) > 1000\n" + "  order by sum(price)\n"
                + ") t1\n" + "inner join  (\n" + "  select test_cal_dt.week_beg_dt, count(*) as cnt\n"
                + "  from DB1.test_kylin_fact\n" + "  inner JOIN EDW.test_cal_dt as test_cal_dt\n"
                + "  ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt\n" + "  inner JOIN EDW.test_category_groupings\n"
                + "  ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id\n"
                + "  inner JOIN EDW.test_sites as test_sites\n"
                + "  ON test_kylin_fact.lstg_site_id = test_sites.site_id\n" + "  group by test_cal_dt.week_beg_dt\n"
                + ") t2\n" + "on t1.week_beg_dt=t2.week_beg_dt limit 5";
        Assert.assertEquals(exceptSQL1, PushDownUtil.schemaCompletion(sql1, "EDW"));
        Assert.assertEquals(exceptSQL2, PushDownUtil.schemaCompletion(sql2, "EDW"));
        Assert.assertEquals(exceptSQL3, PushDownUtil.schemaCompletion(sql3, "EDW"));
    }

    @Test
    public void testSchemaCompletionWithComplexSubquery() throws SqlParseException {
        String sql = "SELECT a, b " + "FROM (" + "   SELECT c, d, sum(p) " + "   FROM table1 t1, DB.table2 t2 "
                + "   WHERE t1.c > t2.d " + "   GROUP BY t.e" + "   HAVING sum(p) > 100" + "   ORDER BY t2.f" + ") at1 "
                + "INNER JOIN table3 t3 " + "ON at1.c = t3.c " + "WHERE t3.d > 0 " + "ORDER BY t3.e";

        String exceptSQL = "SELECT a, b " + "FROM (" + "   SELECT c, d, sum(p) "
                + "   FROM EDW.table1 t1, DB.table2 t2 " + "   WHERE t1.c > t2.d " + "   GROUP BY t.e"
                + "   HAVING sum(p) > 100" + "   ORDER BY t2.f" + ") at1 " + "INNER JOIN EDW.table3 t3 "
                + "ON at1.c = t3.c " + "WHERE t3.d > 0 " + "ORDER BY t3.e";
        Assert.assertEquals(exceptSQL, PushDownUtil.schemaCompletion(sql, "EDW"));
    }

    @Test
    public void testSchemaCompletionWithJoin() throws SqlParseException {
        String sql = "select * from t1 join (select * from t2 join (select * from t3))";
        String exceptSQL = "select * from EDW.t1 join (select * from EDW.t2 join (select * from EDW.t3))";
        Assert.assertEquals(exceptSQL, PushDownUtil.schemaCompletion(sql, "EDW"));
    }

    @Test
    public void testWithSyntax() throws SqlParseException {
        String ori = "WITH tmp AS\n" + //
                "  (SELECT *\n" + //
                "   FROM t) \n" + //
                "SELECT a1\n" + //
                "FROM (\n" + //
                "  WITH tmp2 AS\n" + //
                "    (SELECT *\n" + //
                "     FROM t) \n" + //
                "  SELECT a1\n" + //
                "  FROM t2\n" + //
                "  ORDER BY c_customer_id\n" + //
                ")\n" + //
                "ORDER BY c_customer_id limit 5"; //
        String expected = "WITH tmp AS\n" + //
                "  (SELECT *\n" + //
                "   FROM EDW.t) \n" + //
                "SELECT a1\n" + //
                "FROM (\n" + //
                "  WITH tmp2 AS\n" + //
                "    (SELECT *\n" + //
                "     FROM EDW.t) \n" + //
                "  SELECT a1\n" + //
                "  FROM EDW.t2\n" + //
                "  ORDER BY c_customer_id\n" + //
                ")\n" + //
                "ORDER BY c_customer_id limit 5"; //
        Assert.assertEquals(expected, PushDownUtil.schemaCompletion(ori, "EDW"));
    }

    @Test
    public void testWithSyntax2() throws SqlParseException {
        String origin = "with tmp as (select id from a),tmp2 as (select * from b) select * from tmp,tmp2 where tmp.id = tmp2.id";
        String expected = "with tmp as (select id from ssb.a),tmp2 as (select * from ssb.b) select * from tmp,tmp2 where tmp.id = tmp2.id";
        Assert.assertEquals(expected, PushDownUtil.schemaCompletion(origin, "ssb"));
    }
}

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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FilterPushDownUtilTest extends NLocalFileMetadataTestCase {

    private static final String DEFAULT_CONDITION = "kylin_sales.part_dt < '2015-08-01' and kylin_sales.part_dt > '2013-07-09'";
    private static final String DEFAULT_TABLE = "default.kylin_sales";
    private final boolean DEV_MODE = false;

    @Before
    public void setUp() {
        this.createTestMetadata();
    }

    @After
    public void after() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testApplyFilterCondition() throws SqlParseException {
        testWithTableAlias();
        testWithoutTableAlias();

        testWithWhere();
        testWithoutWhere();

        testSubquery();
        testUnion();
        testWithSelect();

        testJoin();
        testCrossJoin();
        testLeftJoin();
        testRightJoin();

        testTableNameCamouflage();
        testComplexCase();
    }

    private void testWithoutTableAlias() throws SqlParseException {
        String sql = "select kylin_sales.price, kylin_sales.part_dt from kylin_sales "
                + "where kylin_sales.price > 50 and kylin_sales.part_dt > '2012-02-05'";
        String expected = "SELECT KYLIN_SALES.PRICE, KYLIN_SALES.PART_DT\n" + "FROM KYLIN_SALES\n"
                + "WHERE KYLIN_SALES.PRICE > 50 AND KYLIN_SALES.PART_DT > '2012-02-05' "
                + "AND (KYLIN_SALES.PART_DT < '2015-08-01' AND KYLIN_SALES.PART_DT > '2013-07-09')";
        checkExp(sql, expected);
    }

    private void testWithTableAlias() throws SqlParseException {
        String sql = "select ks.price as price, ks.part_dt as dt from kylin_sales as ks "
                + "where ks.price > 50 and ks.part_dt > '2012-02-05'";
        String expected = "SELECT KS.PRICE PRICE, KS.PART_DT DT\n" //
                + "FROM KYLIN_SALES KS\n" //
                + "WHERE KS.PRICE > 50 AND KS.PART_DT > '2012-02-05' "//
                + "AND (KS.PART_DT < '2015-08-01' AND KS.PART_DT > '2013-07-09')";
        checkExp(sql, expected);
    }

    private void testWithWhere() throws SqlParseException {
        checkExp("select a from db.t1 as t1 where t1.b < 2 or t1.b >10",
                "SELECT A\nFROM DB.T1 T1\nWHERE (T1.B < 2 OR T1.B > 10) AND (T1.C > 3 AND T1.C < 6)",
                "t1.c > 3 and t1.c < 6", "db.t1");
    }

    private void testWithoutWhere() throws SqlParseException {

        // case 1
        String sql = "select ks.price as price, ks.part_dt as dt from kylin_sales ks";
        String expected = "SELECT KS.PRICE PRICE, KS.PART_DT DT\n" //
                + "FROM KYLIN_SALES KS\n" //
                + "WHERE KS.PART_DT < '2015-08-01' AND KS.PART_DT > '2013-07-09'";
        checkExp(sql, expected);

        // case 2
        checkExp("select * from db.t1", "SELECT *\n" + "FROM DB.T1\n" + "WHERE T1.C > 3", "t1.c > 3", "db.t1");
    }

    private void testSubquery() throws SqlParseException {
        String sql = "select a.price, a.dt from ( " //
                + "select ks.price as price, ks.part_dt as dt "//
                + "from kylin_sales ks "//
                + "where ks.price > 50 and ks.part_dt > '2012-07-08' " //
                + ") a "//
                + "where a.dt < '2015-08-09'";
        String expectd = "SELECT A.PRICE, A.DT\n" //
                + "FROM (SELECT KS.PRICE PRICE, KS.PART_DT DT\n" //
                + "FROM KYLIN_SALES KS\n" //
                + "WHERE KS.PRICE > 50 AND KS.PART_DT > '2012-07-08' " //
                + "AND (KS.PART_DT < '2015-08-01' AND KS.PART_DT > '2013-07-09')) A\n" //
                + "WHERE A.DT < '2015-08-09'";
        checkExp(sql, expectd);
    }

    private void testUnion() throws SqlParseException {
        // case 1, with one fact table
        String unionSql = "select ks.price as price, ks.part_dt as dt " //
                + "from kylin_sales ks "//
                + "where ks.price > 50 and ks.part_dt > '2012-08-23' " //
                + "union "//
                + "select ky.price as price, ky.part_dt as dt " //
                + "from kylin_sales ky "//
                + "where ky.price < 40 and ky.part_dt < '2015-08-22'";
        String expected = "SELECT KS.PRICE PRICE, KS.PART_DT DT\n" //
                + "FROM KYLIN_SALES KS\n" //
                + "WHERE KS.PRICE > 50 AND KS.PART_DT > '2012-08-23' " //
                + "AND (KS.PART_DT < '2015-08-01' AND KS.PART_DT > '2013-07-09')\n" //
                + "UNION\n" //
                + "SELECT KY.PRICE PRICE, KY.PART_DT DT\n" //
                + "FROM KYLIN_SALES KY\n" //
                + "WHERE KY.PRICE < 40 AND KY.PART_DT < '2015-08-22' " //
                + "AND (KY.PART_DT < '2015-08-01' AND KY.PART_DT > '2013-07-09')";
        checkExp(unionSql, expected);

        // case 2, with two fact table
        String sql2 = "select 'customer' as type, firstname + ' ' + lastname as contactname, city, country, phone \n" //
                + "from sales.customer\n" //
                + "union\n" //
                + "select 'supplier' as type, contactname, city, country, phone\n" //
                + "from sales.supplier";
        String expected2 = "SELECT 'customer' TYPE, FIRSTNAME + ' ' + LASTNAME CONTACTNAME, CITY, COUNTRY, PHONE\n"
                + "FROM SALES.CUSTOMER\n" //
                + "UNION\n" //
                + "SELECT 'supplier' TYPE, CONTACTNAME, CITY, COUNTRY, PHONE\n" //
                + "FROM SALES.SUPPLIER\n" //
                + "WHERE CITY = 'HK'";
        String appendCondition = "city = 'HK'";
        String targetTable = "sales.supplier";
        checkExp(sql2, expected2, appendCondition, targetTable);
    }

    private void testWithSelect() throws SqlParseException {
        String withSelect = "with a as (" //
                + "select ks.price as price, ks.part_dt as dt " //
                + "from kylin_sales ks "//
                + "where ks.price > 50 and ks.part_dt > '2012-07-08') "//
                + "select a.price, a.dt from a where a.dt < '2015-08-09'";
        String expected = "WITH A AS (SELECT KS.PRICE PRICE, KS.PART_DT DT\n" //
                + "FROM KYLIN_SALES KS\n" //
                + "WHERE KS.PRICE > 50 AND KS.PART_DT > '2012-07-08' " //
                + "AND (KS.PART_DT < '2015-08-01' AND KS.PART_DT > '2013-07-09'))" + " (SELECT A.PRICE, A.DT\n" //
                + "FROM A\n" + "WHERE A.DT < '2015-08-09')";
        checkExp(withSelect, expected);
    }

    private void testJoin() throws SqlParseException {
        /*
         * case 1, test simple join
         */
        String joinSql = "select ks.price, ks.trans_id, ks.part_dt " //
                + "from kylin_sales ks " //
                + "join kylin_cal_dt kcal " //
                + "on ks.part_dt = kcal.cal_dt " //
                + "where kcal.cal_dt > '2012-07-23'";
        String expected = "SELECT KS.PRICE, KS.TRANS_ID, KS.PART_DT\n" //
                + "FROM KYLIN_SALES KS\n" //
                + "INNER JOIN KYLIN_CAL_DT KCAL ON KS.PART_DT = KCAL.CAL_DT\n" //
                + "WHERE KCAL.CAL_DT > '2012-07-23' AND (KS.PART_DT < '2015-08-01' AND KS.PART_DT > '2013-07-09')";
        checkExp(joinSql, expected);

        /*
         * case 2, change the table order
         */
        String changeTableOrder = "select ks.price, ks.trans_id, ks.part_dt " //
                + "from kylin_cal_dt kcal  " //
                + "join kylin_sales ks " //
                + "on ks.part_dt = kcal.cal_dt " //
                + "where kcal.cal_dt > '2012-07-23'";
        String changeTableOrderExpected = "SELECT KS.PRICE, KS.TRANS_ID, KS.PART_DT\n" //
                + "FROM KYLIN_CAL_DT KCAL\n" //
                + "INNER JOIN KYLIN_SALES KS ON KS.PART_DT = KCAL.CAL_DT\n" //
                + "WHERE KCAL.CAL_DT > '2012-07-23' AND (KS.PART_DT < '2015-08-01' AND KS.PART_DT > '2013-07-09')";
        checkExp(changeTableOrder, changeTableOrderExpected);

        /*
         * case 3, test two layer join
         */
        String twoLayerJoin = "select ks.price, ks.trans_id, ks.part_dt dt " //
                + "from kylin_sales ks " //
                + "join (" //
                + "select ksin.part_dt as dt " //
                + "from kylin_sales ksin " //
                + "where ksin.price > 50) a " //
                + "on a.dt = ks.part_dt " //
                + "where ks.part_dt > '2012-09-23'";
        String twoLayerExpected = "SELECT KS.PRICE, KS.TRANS_ID, KS.PART_DT DT\n" //
                + "FROM KYLIN_SALES KS\n" //
                + "INNER JOIN (SELECT KSIN.PART_DT DT\n" //
                + "FROM KYLIN_SALES KSIN\n" //
                + "WHERE KSIN.PRICE > 50 AND (KSIN.PART_DT < '2015-08-01' AND KSIN.PART_DT > '2013-07-09')) A " //
                + "ON A.DT = KS.PART_DT\n" //
                + "WHERE KS.PART_DT > '2012-09-23' AND (KS.PART_DT < '2015-08-01' AND KS.PART_DT > '2013-07-09')";
        checkExp(twoLayerJoin, twoLayerExpected);

        /*
         * case 4, three layer join
         */
        String threeLayerJoin = "select ks.price, ks.trans_id, ks.part_dt as dt\n" //
                + "from kylin_sales ks\n" //
                + "  join (\n" //
                + "    select ksin.part_dt as dt\n" //
                + "    from kylin_sales ksin\n" //
                + "      join (\n" //
                + "        select ksin2.part_dt as dt\n" //
                + "        from kylin_sales ksin2\n" //
                + "        where ksin2.price > 20\n" //
                + "        order by ksin.part_dt desc\n" //
                + "      ) b\n" //
                + "      on b.dt = ksin.part_dt\n" //
                + "  where ksin.price > 50\n" //
                + "  ) a\n" //
                + "  on a.dt = ks.part_dt\n" //
                + "where ks.part_dt > '2012-09-23'";
        String threeLayerJoinExpected = "SELECT KS.PRICE, KS.TRANS_ID, KS.PART_DT DT\n" //
                + "FROM KYLIN_SALES KS\n" //
                + "INNER JOIN (SELECT KSIN.PART_DT DT\n" //
                + "FROM KYLIN_SALES KSIN\n" //
                + "INNER JOIN (SELECT KSIN2.PART_DT DT\n" //
                + "FROM KYLIN_SALES KSIN2\n" //
                + "WHERE KSIN2.PRICE > 20 AND (KSIN2.PART_DT < '2015-08-01' AND KSIN2.PART_DT > '2013-07-09')\n"
                + "ORDER BY KSIN.PART_DT DESC) B ON B.DT = KSIN.PART_DT\n"
                + "WHERE KSIN.PRICE > 50 AND (KSIN.PART_DT < '2015-08-01' AND KSIN.PART_DT > '2013-07-09')) A ON A.DT = KS.PART_DT\n"
                + "WHERE KS.PART_DT > '2012-09-23' AND (KS.PART_DT < '2015-08-01' AND KS.PART_DT > '2013-07-09')";
        checkExp(threeLayerJoin, threeLayerJoinExpected);

        /*
         * case 5, three layer join but one inner layer doesn't contains target table
         */
        String sql5 = "SELECT ks.price, ks.trans_id, ks.part_dt AS dt\n" //
                + "FROM kylin_sales ks\n" //
                + "  JOIN (\n" //
                + "    SELECT kcal.cal_dt AS dt\n" //
                + "    FROM kylin_cal_dt kcal\n" //
                + "      JOIN (\n" //
                + "        SELECT ksin2.part_dt AS dt\n" //
                + "        FROM kylin_sales ksin2\n" //
                + "        WHERE ksin2.price > 20\n" //
                + "        ORDER BY ksin.part_dt DESC\n" //
                + "      ) b\n" //
                + "      ON b.dt = kcal.cal_dt\n" //
                + "    WHERE kcal.cal_dt > '2011-06-12'\n" //
                + "  ) a\n" //
                + "  ON a.dt = ks.part_dt\n" //
                + "WHERE ks.part_dt > '2012-09-23'";
        String sql5Expected = "SELECT KS.PRICE, KS.TRANS_ID, KS.PART_DT DT\n" //
                + "FROM KYLIN_SALES KS\n" //
                + "INNER JOIN (SELECT KCAL.CAL_DT DT\n" //
                + "FROM KYLIN_CAL_DT KCAL\n" //
                + "INNER JOIN (SELECT KSIN2.PART_DT DT\n" //
                + "FROM KYLIN_SALES KSIN2\n" //
                + "WHERE KSIN2.PRICE > 20 AND (KSIN2.PART_DT < '2015-08-01' AND KSIN2.PART_DT > '2013-07-09')\n" //
                + "ORDER BY KSIN.PART_DT DESC) B ON B.DT = KCAL.CAL_DT\n" //
                + "WHERE KCAL.CAL_DT > '2011-06-12') A ON A.DT = KS.PART_DT\n" //
                + "WHERE KS.PART_DT > '2012-09-23' AND (KS.PART_DT < '2015-08-01' AND KS.PART_DT > '2013-07-09')";
        checkExp(sql5, sql5Expected);

        /*
         * case 6, another form of self join
         */
        String selfJoin = "select b.firstname as firstname1, b.lastname as lastname1, \n"
                + "a.firstname as firstname2, a.lastname as lastname2, b.city, b.country\n" //
                + "from sales.customer a, sales.customer b\n" //
                + "where (a.id <> b.id and a.city = b.city and a.country = b.country)\n" //
                + "order by a.country";
        String selfJoinExpected = "SELECT B.FIRSTNAME FIRSTNAME1, B.LASTNAME LASTNAME1, " //
                + "A.FIRSTNAME FIRSTNAME2, A.LASTNAME LASTNAME2, B.CITY, B.COUNTRY\n" + "FROM SALES.CUSTOMER A,\n" //
                + "SALES.CUSTOMER B\n" //
                + "WHERE A.ID <> B.ID AND A.CITY = B.CITY AND A.COUNTRY = B.COUNTRY " //
                + "AND B.CITY = 'HK' AND A.CITY = 'HK'\n" //
                + "ORDER BY A.COUNTRY";
        String appendCondition = "customer.city = 'HK'";
        String targetTable = "sales.customer";
        checkExp(selfJoin, selfJoinExpected, appendCondition, targetTable);
    }

    private void testCrossJoin() throws SqlParseException {

        // case 0, use comma, condition on the first table
        checkExp("select t1.a, t2.d from db.t1 as t1, ddd.t2 as t2", //
                "SELECT T1.A, T2.D\n" + "FROM DB.T1 T1,\n" + "DDD.T2 T2\n" + "WHERE T1.M < 5", //
                "t1.m < 5", //
                "db.t1");

        // case 1, use comma, condition not only affect the second table but also affect the sub-query
        String crossJoin = "select t1.a, t2.k, t2.d from db.t1 as t1, db.t2 as t2, "
                + "(select t1.k as k, t2.d as d from db.t2 as t2 where t2.m > 3) tbl";
        String expected = "SELECT T1.A, T2.K, T2.D\n" //
                + "FROM DB.T1 T1,\n" //
                + "DB.T2 T2,\n" //
                + "(SELECT T1.K K, T2.D D\n" //
                + "FROM DB.T2 T2\n" //
                + "WHERE T2.M > 3 AND T2.M < 5) TBL\n" //
                + "WHERE T2.M < 5";
        checkExp(crossJoin, expected, "t2.m < 5", "db.t2");

        // case 2, use cross without on
        String crossJoin2 = "select * from db.t1 cross join db.t2";
        String expected2 = "SELECT *\n" + "FROM DB.T1\n" + "CROSS JOIN DB.T2\n" + "WHERE T2.A > 13";
        checkExp(crossJoin2, expected2, "t2.a > 13", "db.t2");

        // case 3, use cross with on
        String crossJoin3 = "select * from db.t1 cross join db.t2 on t1.a = t2.a";
        String expected3 = "SELECT *\n" + "FROM DB.T1\n" + "CROSS JOIN DB.T2 ON T1.A = T2.A\n" + "WHERE T2.A > 13";
        checkExp(crossJoin3, expected3, "t2.a > 13", "db.t2");
    }

    private void testLeftJoin() throws SqlParseException {
        String sql = "select * from db.t1 left join db.t2 on t1.a = t2.a";
        String expected = "SELECT *\n" + "FROM DB.T1\n" + "LEFT JOIN DB.T2 ON T1.A = T2.A\n" + "WHERE T2.A > 13";
        checkExp(sql, expected, "t2.a > 13", "db.t2");
    }

    private void testRightJoin() throws SqlParseException {
        String sql = "select * from db.t1 right join db.t2 on t1.a = t2.a";
        String expected = "SELECT *\n" + "FROM DB.T1\n" + "RIGHT JOIN DB.T2 ON T1.A = T2.A\n" + "WHERE T2.A > 13";
        checkExp(sql, expected, "t2.a > 13", "db.t2");
    }

    private void testTableNameCamouflage() throws SqlParseException {
        String sql = "select kylin_sales.leaf_categ_id, kylin_sales.meta_categ_name, kylin_sales.site_id "//
                + "from kylin_category_groupings as kylin_sales " //
                + "where kylin_sales.site_id=15";
        String expected = "SELECT KYLIN_SALES.LEAF_CATEG_ID, KYLIN_SALES.META_CATEG_NAME, KYLIN_SALES.SITE_ID\n" //
                + "FROM KYLIN_CATEGORY_GROUPINGS KYLIN_SALES\n" //
                + "WHERE KYLIN_SALES.SITE_ID = 15";
        checkExp(sql, expected);
    }

    private void testComplexCase() throws SqlParseException {

        // case 1
        String sql = "select t1.week_beg_dt, t1.sum_price, t2.cnt\n" //
                + "from (\n"//
                + "    select test_cal_dt.week_beg_dt, sum(test_kylin_fact.price) as sum_price\n"//
                + "    from db1.test_kylin_fact test_kylin_fact\n"//
                + "        inner join edw.test_cal_dt test_cal_dt on test_kylin_fact.cal_dt = test_cal_dt.cal_dt\n"//
                + "        inner join edw.test_category_groupings test_category_groupings\n"//
                + "        on test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id\n"//
                + "            and test_kylin_fact.lstg_site_id = test_category_groupings.site_id\n"//
                + "        inner join edw.test_sites test_sites on test_kylin_fact.lstg_site_id = test_sites.site_id\n"//
                + "    where test_kylin_fact.price > 100\n" //
                + "    group by test_cal_dt.week_beg_dt\n" //
                + "    having sum(test_kylin_fact.price) > 1000\n" //
                + "    order by sum(test_kylin_fact.price)\n" //
                + ") t1\n" //
                + "    inner join (\n" //
                + "        select test_cal_dt.week_beg_dt, count(*) as cnt\n" //
                + "        from db1.test_kylin_fact test_kylin_fact\n" //
                + "            inner join edw.test_cal_dt test_cal_dt on test_kylin_fact.cal_dt = test_cal_dt.cal_dt\n" //
                + "            inner join edw.test_category_groupings test_category_groupings\n" //
                + "            on test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id\n" //
                + "                and test_kylin_fact.lstg_site_id = test_category_groupings.site_id\n" //
                + "            inner join edw.test_sites test_sites on test_kylin_fact.lstg_site_id = test_sites.site_id\n" //
                + "        group by test_cal_dt.week_beg_dt\n" //
                + "    ) t2\n" //
                + "    on t1.week_beg_dt = t2.week_beg_dt\n" //
                + "where t1.week_beg_dt > '2012-08-23'";
        String expected = "SELECT T1.WEEK_BEG_DT, T1.SUM_PRICE, T2.CNT\n"
                + "FROM (SELECT TEST_CAL_DT.WEEK_BEG_DT, SUM(TEST_KYLIN_FACT.PRICE) SUM_PRICE\n"
                + "FROM DB1.TEST_KYLIN_FACT TEST_KYLIN_FACT\n"
                + "INNER JOIN EDW.TEST_CAL_DT TEST_CAL_DT ON TEST_KYLIN_FACT.CAL_DT = TEST_CAL_DT.CAL_DT\n"
                + "INNER JOIN EDW.TEST_CATEGORY_GROUPINGS TEST_CATEGORY_GROUPINGS "
                + "ON TEST_KYLIN_FACT.LEAF_CATEG_ID = TEST_CATEGORY_GROUPINGS.LEAF_CATEG_ID "
                + "AND TEST_KYLIN_FACT.LSTG_SITE_ID = TEST_CATEGORY_GROUPINGS.SITE_ID\n"
                + "INNER JOIN EDW.TEST_SITES TEST_SITES ON TEST_KYLIN_FACT.LSTG_SITE_ID = TEST_SITES.SITE_ID\n"
                + "WHERE TEST_KYLIN_FACT.PRICE > 100 AND TEST_KYLIN_FACT.PRICE < 200\n"
                + "GROUP BY TEST_CAL_DT.WEEK_BEG_DT\n" //
                + "HAVING SUM(TEST_KYLIN_FACT.PRICE) > 1000\n" //
                + "ORDER BY SUM(TEST_KYLIN_FACT.PRICE)) T1\n" //
                + "INNER JOIN (SELECT TEST_CAL_DT.WEEK_BEG_DT, COUNT(*) CNT\n" //
                + "FROM DB1.TEST_KYLIN_FACT TEST_KYLIN_FACT\n" //
                + "INNER JOIN EDW.TEST_CAL_DT TEST_CAL_DT ON TEST_KYLIN_FACT.CAL_DT = TEST_CAL_DT.CAL_DT\n"
                + "INNER JOIN EDW.TEST_CATEGORY_GROUPINGS TEST_CATEGORY_GROUPINGS "
                + "ON TEST_KYLIN_FACT.LEAF_CATEG_ID = TEST_CATEGORY_GROUPINGS.LEAF_CATEG_ID "
                + "AND TEST_KYLIN_FACT.LSTG_SITE_ID = TEST_CATEGORY_GROUPINGS.SITE_ID\n"
                + "INNER JOIN EDW.TEST_SITES TEST_SITES ON TEST_KYLIN_FACT.LSTG_SITE_ID = TEST_SITES.SITE_ID\n"
                + "WHERE TEST_KYLIN_FACT.PRICE < 200\n"
                + "GROUP BY TEST_CAL_DT.WEEK_BEG_DT) T2 ON T1.WEEK_BEG_DT = T2.WEEK_BEG_DT\n"
                + "WHERE T1.WEEK_BEG_DT > '2012-08-23'";
        checkExp(sql, expected, "test_kylin_fact.price < 200", "db1.test_kylin_fact");
    }

    @Test
    public void testApplyFilterError() {
        String sql = "select t1.a, t1.b from db.t1 where t1.a > 10";

        // case 1, Malformed target table
        Pattern malTablePtn = Pattern.compile("Malformed target table\\s+\\p{all}*, missing schema or table name.");
        checkFail(sql, ".t1", null, malTablePtn);
        checkFail(sql, "db.", null, malTablePtn);

        // case 2, Target table cannot be found in the primitive query
        Pattern notFoundPtn = Pattern.compile("The target table\\s+\\p{all}*\\s+cannot be found in query\\p{all}*");
        checkFail(sql, "db.t2", null, notFoundPtn);

        // case 3, Filter condition is null or empty string
        Pattern emptyFilterPtn = Pattern.compile("Filter condition can not be NULL or empty string.");
        checkFail(sql, "db.t1", null, emptyFilterPtn);
        checkFail(sql, "db.t1", "", emptyFilterPtn);

        // case 4, Filter condition parse error
        Pattern parseErrPtn = Pattern
                .compile("While parsing filter condition\\s+\\p{all}*\\s+throws an \\p{all}*ParseException");
        checkFail(sql, "db.t1", "t1.a = ", parseErrPtn);
        checkFail(sql, "db.t1", "=?", parseErrPtn);
        checkFail(sql, "db.t1", "*", parseErrPtn);
        checkFail(sql, "db.t1", "select t1.a from db.t1", parseErrPtn);

        // case 5, Malformed query
    }

    private void checkExp(String sql, String expected) throws SqlParseException {
        checkExp(sql, expected, DEFAULT_CONDITION, DEFAULT_TABLE);
    }

    private void checkExp(String sql, String expected, String appendCondition, String targetTable)
            throws SqlParseException {
        final String actual = FilterPushDownUtil.applyFilterCondition(sql, appendCondition, targetTable);
        if (DEV_MODE) {
            System.out.println(actual + "\n\n");
        }
        Assert.assertEquals(expected, actual);
    }

    private void checkFail(String sql, String targetTable, String filterCondition, Pattern pattern) {
        try {
            FilterPushDownUtil.applyFilterCondition(sql, filterCondition, targetTable);
        } catch (Exception e) {
            final Matcher matcher = pattern.matcher(e.getMessage());
            Assert.assertTrue(matcher.find());
        }
    }
}

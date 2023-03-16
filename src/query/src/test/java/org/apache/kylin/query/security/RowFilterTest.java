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

package org.apache.kylin.query.security;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.acl.AclTCR;
import org.apache.kylin.metadata.acl.AclTCRManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.kylin.guava30.shaded.common.collect.Sets;

public class RowFilterTest extends NLocalFileMetadataTestCase {

    private final static String PROJECT = "default";
    private final static String SCHEMA = "DEFAULT";

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        getTestConfig().setProperty("kylin.query.security.acl-tcr-enabled", "true");
    }

    @After
    public void after() {
        cleanupTestMetadata();
    }

    @Test
    public void testConvert() {
        AclTCRManager manager = AclTCRManager.getInstance(getTestConfig(), PROJECT);

        AclTCR u1a1 = new AclTCR();
        AclTCR.Table u1t1 = new AclTCR.Table();
        AclTCR.ColumnRow u1cr1 = new AclTCR.ColumnRow();
        AclTCR.Column u1c1 = new AclTCR.Column();
        u1c1.addAll(Arrays.asList("PRICE", "ITEM_COUNT"));
        u1cr1.setColumn(u1c1);
        AclTCR.Row u1r1 = new AclTCR.Row();
        AclTCR.RealRow u1rr1 = new AclTCR.RealRow();
        u1rr1.addAll(Arrays.asList("11", "22", "33"));
        u1r1.put("ITEM_COUNT", u1rr1);
        u1cr1.setRow(u1r1);

        AclTCR.ColumnRow u1cr2 = new AclTCR.ColumnRow();
        AclTCR.Column u1c2 = new AclTCR.Column();
        u1c2.addAll(Arrays.asList("ORDER_ID", "BUYER_ID", "TEST_DATE_ENC"));
        u1cr2.setColumn(u1c2);
        AclTCR.Row u1r2 = new AclTCR.Row();
        AclTCR.RealRow u1rr2 = new AclTCR.RealRow();
        u1rr2.addAll(Arrays.asList("900900", "900901", "900902"));
        u1r2.put("BUYER_ID", u1rr2);
        u1cr2.setRow(u1r2);

        u1t1.put("DEFAULT.TEST_KYLIN_FACT", u1cr1);
        u1t1.put("DEFAULT.TEST_ORDER", u1cr2);
        u1a1.setTable(u1t1);
        manager.updateAclTCR(u1a1, "u1", true);

        AclTCR g1a1 = new AclTCR();
        AclTCR.Table g1t1 = new AclTCR.Table();
        AclTCR.ColumnRow g1cr1 = new AclTCR.ColumnRow();
        AclTCR.Column g1c1 = new AclTCR.Column();
        g1c1.addAll(Arrays.asList("ORDER_ID"));
        g1cr1.setColumn(g1c1);
        AclTCR.Row g1r1 = new AclTCR.Row();
        AclTCR.RealRow g1rr1 = new AclTCR.RealRow();
        g1rr1.addAll(Arrays.asList("44", "55", "66"));
        g1r1.put("ITEM_COUNT", g1rr1);
        g1cr1.setRow(g1r1);
        g1t1.put("DEFAULT.TEST_KYLIN_FACT", g1cr1);
        g1a1.setTable(g1t1);
        manager.updateAclTCR(g1a1, "g1", false);

        final RowFilter rowFilter = new RowFilter();
        QueryContext.current().setAclInfo(new QueryContext.AclInfo("u1", Sets.newHashSet("g1"), false));
        final String originSql = "select * from TEST_KYLIN_FACT t1 join TEST_ORDER t2 on t1.ORDER_ID = t2.ORDER_ID";
        final String convertedSql = rowFilter.convert(originSql, PROJECT, SCHEMA);
        final String expectedSql = originSql
                + " WHERE ((T1.ITEM_COUNT in (11,22,33)) OR (T1.ITEM_COUNT in (44,55,66))) AND (T2.BUYER_ID in (900902,900901,900900))";
        assertRoughlyEquals(convertedSql, expectedSql);
    }

    @Test
    public void testBrackets() {
        String sql = "select * from (select * from t inner join tt on t.a = tt.a inner join ttt on t.a = ttt.a where t.a = 0 and tt.a=1 ) t1 where t1.a = 2";
        String sql1 = "select * from t where t.a in (select * from tt where tt.b=1)";
        HashSet<String> tbls = Sets.newHashSet("DB.T", "DB.TT", "DB.TTT");
        String expectSQL = "select * from (select * from t inner join tt on t.a = tt.a inner join ttt on t.a = ttt.a where (t.a = 0 and tt.a=1) ) t1 where t1.a = 2";
        String expectSQL1 = "select * from t where (t.a in (select * from tt where (tt.b=1)))";
        Assert.assertEquals(expectSQL, RowFilter.whereClauseBracketsCompletion("DB", sql, tbls));
        Assert.assertEquals(expectSQL1, RowFilter.whereClauseBracketsCompletion("DB", sql1, tbls));
    }

    @Test
    public void testRowFilterWithMultiWhereConds() {
        List<Map<String, String>> list = new ArrayList<>();
        Map<String, String> whereCond = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Map<String, String> whereCond1 = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Map<String, String> whereCond2 = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        whereCond.put("DB.T", "( a > 0 OR b < 0 )");
        whereCond1.put("DB.T", "( a > 0 OR b < 0 )");
        whereCond2.put("DB.T", "( a > 0 OR b < 0 )");
        list.add(whereCond);
        list.add(whereCond1);
        list.add(whereCond2);
        String sql = "select * from (select * from t)";
        for (Map<String, String> whereCondWithTbls : list) {
            sql = RowFilter.rowFilter("DB", sql, whereCondWithTbls);
        }
        String expectedSQL = "select * from (select * from t WHERE ( T.a > 0 OR T.b < 0 ) AND ( T.a > 0 OR T.b < 0 ) AND ( T.a > 0 OR T.b < 0 ))";
        Assert.assertEquals(expectedSQL, sql);
    }

    @Test
    public void testExplainSyntax() {
        String sql = "explain plan for select * from t";
        Map<String, String> whereCond = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        whereCond.put("DB.T", "(a > 0 OR b < 0)");
        Assert.assertEquals("explain plan for select * from t WHERE (T.a > 0 OR T.b < 0)",
                RowFilter.rowFilter("DB", sql, whereCond));
    }

    @Test
    public void testSimpleRowFilter() {
        String sql = "select count(*) from t join tt on t.a = tt.a group by c";
        String sql2 = "select count(*) from t join tt on t.a = tt.a where (b1 = 'v1') group by c";
        String sql3 = "select count(*) from t where t.c > 10";
        Map<String, String> whereCond = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        whereCond.put("DB.T", "(a > 0 OR b < 0)");
        whereCond.put("DB.TT", "(aa > 0 OR bb < 0)");

        String expectedSQL = "select count(*) from t join tt on t.a = tt.a WHERE (T.a > 0 OR T.b < 0) AND (TT.aa > 0 OR TT.bb < 0) group by c";
        String expectedSQL2 = "select count(*) from t join tt on t.a = tt.a where (b1 = 'v1') AND (T.a > 0 OR T.b < 0) AND (TT.aa > 0 OR TT.bb < 0) group by c";
        String expectedSQL3 = "select count(*) from t where t.c > 10 AND (T.a > 0 OR T.b < 0)";

        Assert.assertEquals(expectedSQL, RowFilter.rowFilter("DB", sql, whereCond));
        Assert.assertEquals(expectedSQL2, RowFilter.rowFilter("DB", sql2, whereCond));
        Assert.assertEquals(expectedSQL3, RowFilter.rowFilter("DB", sql3, whereCond));
    }

    @Test
    public void testRowFilter() throws SqlParseException {
        String sql = "select a, (select count(*) from DB3.aa a1 order by a)\n"
                + "from ttt join (select a,b from (select * from DB.t t1), (select * from DB.bb)), tt\n"
                + "where c in (select * from tt) and d > 10 order by abc";
        Map<String, String> whereCond = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        whereCond.put("DB.T", "(a > 0 OR b < 0)");
        whereCond.put("DB2.TT", "(aa > 0 OR bb < 0)");
        whereCond.put("DB2.TTT", "(aaa > 0 OR bbb < 0)");
        String expectedSQL = "select a, (select count(*) from DB3.aa a1 order by a)\n"
                + "from ttt join (select a,b from (select * from DB.t t1 WHERE (T1.a > 0 OR T1.b < 0)), (select * from DB.bb)), tt\n"
                + "where c in (select * from tt WHERE (TT.aa > 0 OR TT.bb < 0)) and d > 10 AND (TTT.aaa > 0 OR TTT.bbb < 0) AND (TT.aa > 0 OR TT.bb < 0) order by abc";
        Assert.assertEquals(expectedSQL, RowFilter.rowFilter("DB2", sql, whereCond));
    }

    @Test
    public void testWithFunc() throws SqlParseException {
        String sql = "with avg_tmp as (\n" + "    select\n" + "        avg(c_acctbal) as avg_acctbal\n" + "    from\n"
                + "        customer\n" + "    where\n"
                + "        c_acctbal > 0.00 and substring(c_phone, 1, 2) in ('13','31','23','29','30','18','17')\n"
                + "),\n" + "cus_tmp as (\n" + "    select c_custkey as noordercus\n" + "    from\n"
                + "        customer left join v_orders on c_custkey = o_custkey\n" + "    where o_orderkey is null\n"
                + ")\n" + "\n" + "select\n" + "    cntrycode,\n" + "    count(1) as numcust,\n"
                + "    sum(c_acctbal) as totacctbal\n" + "from (\n" + "    select\n"
                + "        substring(c_phone, 1, 2) as cntrycode,\n" + "        c_acctbal\n" + "    from \n"
                + "        customer inner join cus_tmp on c_custkey = noordercus, avg_tmp\n" + "    where \n"
                + "        substring(c_phone, 1, 2) in ('13','31','23','29','30','18','17')\n"
                + "        and c_acctbal > avg_acctbal\n" + ") t\n" + "group by\n" + "    cntrycode\n" + "order by\n"
                + "    cntrycode";
        Map<String, String> whereCond = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        String expectedSQL = "with avg_tmp as (\n" + "    select\n" + "        avg(c_acctbal) as avg_acctbal\n"
                + "    from\n" + "        customer\n" + "    where\n"
                + "        c_acctbal > 0.00 and substring(c_phone, 1, 2) in ('13','31','23','29','30','18','17') AND (CUSTOMER.a > 0 OR CUSTOMER.b < 0)\n"
                + "),\n" + "cus_tmp as (\n" + "    select c_custkey as noordercus\n" + "    from\n"
                + "        customer left join v_orders on c_custkey = o_custkey\n"
                + "    where o_orderkey is null AND (CUSTOMER.a > 0 OR CUSTOMER.b < 0)\n" + ")\n" + "\n" + "select\n"
                + "    cntrycode,\n" + "    count(1) as numcust,\n" + "    sum(c_acctbal) as totacctbal\n" + "from (\n"
                + "    select\n" + "        substring(c_phone, 1, 2) as cntrycode,\n" + "        c_acctbal\n"
                + "    from \n" + "        customer inner join cus_tmp on c_custkey = noordercus, avg_tmp\n"
                + "    where \n" + "        substring(c_phone, 1, 2) in ('13','31','23','29','30','18','17')\n"
                + "        and c_acctbal > avg_acctbal AND (CUSTOMER.a > 0 OR CUSTOMER.b < 0) AND (CUS_TMP.aa > 0 OR CUS_TMP.bb < 0)\n"
                + ") t\n" + "group by\n" + "    cntrycode\n" + "order by\n" + "    cntrycode";
        whereCond.put("DB.CUSTOMER", "(a > 0 OR b < 0)");
        whereCond.put("DB.CUS_TMP", "(aa > 0 OR bb < 0)");
        Assert.assertEquals(expectedSQL, RowFilter.rowFilter("DB", sql, whereCond));
    }

    @Test
    public void testNeedEscape() throws SqlParseException {
        String sql = "select count(*) as \"m0\"";
        Assert.assertEquals(true, RowFilter.needEscape(sql, "DB2", new HashMap<>()));
    }

    @Test
    public void testRowFilterWithUnoin() {
        String sql = "select * from TEST_KYLIN_FACT where CAL_DT < DATE '2012-06-01'\n" + "union\n"
                + "select * from TEST_KYLIN_FACT where CAL_DT > DATE '2013-06-01'";
        Map<String, String> whereCond = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        whereCond.put("DB.TEST_KYLIN_FACT", "(a > 0 OR b < 0)");
        String expectedSQL = "select * from TEST_KYLIN_FACT where CAL_DT < DATE '2012-06-01' AND (TEST_KYLIN_FACT.a > 0 OR TEST_KYLIN_FACT.b < 0)\n"
                + "union\n"
                + "select * from TEST_KYLIN_FACT where CAL_DT > DATE '2013-06-01' AND (TEST_KYLIN_FACT.a > 0 OR TEST_KYLIN_FACT.b < 0)";
        Assert.assertEquals(expectedSQL, RowFilter.rowFilter("DB", sql, whereCond));
    }

    @Test
    public void testJoinWithOutWhere() {
        String sql = "select * from T1 join (select * from T2) ta on T1.c=ta.c GROUP BY c";
        String exceptedSQL = "select * from T1 join (select * from T2) ta on T1.c=ta.c WHERE T1.OPS_REGION='Shanghai' GROUP BY c";
        Map<String, String> whereCond = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        whereCond.put("DB.T1", "OPS_REGION='Shanghai'");
        whereCond.put("DB.T3", "OPS_REGION='Beijing'");
        Assert.assertEquals(exceptedSQL, RowFilter.rowFilter("DB", sql, whereCond));
    }

    @Test
    public void testInsertAliasInExpr() {
        String expr = "a > b and c = 10 and city ='上海'";
        String expr1 = "a > 0 OR b < 0";
        String expr2 = "aa > 0 OR bb < 0";
        String expr3 = "aaa > 0 OR bbb < 0";
        String excepted = "t1.a > t1.b and t1.c = 10 and t1.city ='上海'";
        String excepted1 = "t1.a > 0 OR t1.b < 0";
        String excepted2 = "t1.aa > 0 OR t1.bb < 0";
        String excepted3 = "t1.aaa > 0 OR t1.bbb < 0";
        Assert.assertEquals(excepted, CalciteParser.insertAliasInExpr(expr, "t1"));
        Assert.assertEquals(excepted1, CalciteParser.insertAliasInExpr(expr1, "t1"));
        Assert.assertEquals(excepted2, CalciteParser.insertAliasInExpr(expr2, "t1"));
        Assert.assertEquals(excepted3, CalciteParser.insertAliasInExpr(expr3, "t1"));
    }

    private void assertRoughlyEquals(String expect, String actual) {
        String[] expectSplit = expect.split("\\s+");
        String[] actualSplit = actual.split("\\s+");
        Arrays.sort(expectSplit);
        Arrays.sort(actualSplit);

        Assert.assertArrayEquals(expectSplit, actualSplit);
    }
}

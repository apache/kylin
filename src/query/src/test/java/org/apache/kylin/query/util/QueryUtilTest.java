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

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.collect.BiMap;
import org.apache.kylin.guava30.shaded.common.collect.HashBiMap;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.util.ComputedColumnUtil;
import org.apache.kylin.query.IQueryTransformer;
import org.apache.kylin.query.security.AccessDeniedException;
import org.apache.kylin.util.MetadataTestUtils;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@MetadataInfo
public class QueryUtilTest {

    @BeforeAll
    public static void setUpForClass() {
        ComputedColumnUtil.setEXTRACTOR(ComputedColumnRewriter::extractCcRexNode);
    }

    @Test
    void testMaxResultRowsEnabled() {
        Map<String, String> map = Maps.newHashMap();
        map.put("kylin.query.max-result-rows", "15");
        map.put("kylin.query.force-limit", "14");
        MetadataTestUtils.updateProjectConfig("default", map);
        String result = QueryUtil.appendLimitOffset("default", "select * from table1", 16, 0);
        Assertions.assertEquals("select * from table1" + "\n" + "LIMIT 15", result);
    }

    @Test
    void testCompareMaxResultRowsAndLimit() {
        Map<String, String> map = Maps.newHashMap();
        map.put("kylin.query.max-result-rows", "15");
        map.put("kylin.query.force-limit", "14");
        MetadataTestUtils.updateProjectConfig("default", map);
        String result = QueryUtil.appendLimitOffset("default", "select * from table1", 13, 0);
        Assertions.assertEquals("select * from table1" + "\n" + "LIMIT 13", result);
    }

    @Test
    void testMassageSql() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.query.transformers", DefaultQueryTransformer.class.getCanonicalName());

        String sql = "SELECT * FROM TABLE1";
        QueryParams queryParams1 = new QueryParams(config, sql, "default", 100, 20, "", true);
        String newSql = QueryUtil.massageSql(queryParams1);
        Assertions.assertEquals("SELECT * FROM TABLE1\nLIMIT 100\nOFFSET 20", newSql);

        String sql2 = "SELECT SUM({fn convert(0, INT)}) from TABLE1";
        QueryParams queryParams2 = new QueryParams(config, sql2, "default", 0, 0, "", true);
        String newSql2 = QueryUtil.massageSql(queryParams2);
        Assertions.assertEquals("SELECT SUM({fn convert(0, INT)}) from TABLE1", newSql2);
    }

    @Test
    void testAdaptCubePriority() {
        {
            String sql = "--CubePriority(m)\nselect price from test_kylin_fact";
            QueryParams queryParams1 = new QueryParams(KylinConfig.getInstanceFromEnv(), sql, "default", 0, 0,
                    "DEFAULT", true);
            String transformed = QueryUtil.massageSql(queryParams1);
            Assertions.assertEquals(sql, transformed);
        }

        {
            String sql = "-- CubePriority(m)\nselect price from test_kylin_fact";
            QueryParams queryParams1 = new QueryParams(KylinConfig.getInstanceFromEnv(), sql, "default", 0, 0,
                    "DEFAULT", true);
            String transformed = QueryUtil.massageSql(queryParams1);
            Assertions.assertEquals(sql, transformed);
        }
    }

    @Test
    void testInit() {
        KylinConfig config = KylinConfig.createKylinConfig(new Properties());
        config.setProperty("kylin.query.transformers", DefaultQueryTransformer.class.getCanonicalName());
        List<IQueryTransformer> transformers = QueryUtil.fetchTransformers(true, config.getQueryTransformers());
        Assertions.assertEquals(1, transformers.size());
        Assertions.assertInstanceOf(DefaultQueryTransformer.class, transformers.get(0));

        config.setProperty("kylin.query.transformers", KeywordDefaultDirtyHack.class.getCanonicalName());
        transformers = QueryUtil.fetchTransformers(true, config.getQueryTransformers());
        Assertions.assertEquals(1, transformers.size());
        Assertions.assertInstanceOf(KeywordDefaultDirtyHack.class, transformers.get(0));

        transformers = QueryUtil.fetchTransformers(false, config.getQueryTransformers());
        Assertions.assertEquals(1, transformers.size());
        Assertions.assertInstanceOf(KeywordDefaultDirtyHack.class, transformers.get(0));

        config.setProperty("kylin.query.transformers", DefaultQueryTransformer.class.getCanonicalName() + ","
                + ConvertToComputedColumn.class.getCanonicalName());
        transformers = QueryUtil.fetchTransformers(true, config.getQueryTransformers());
        Assertions.assertEquals(2, transformers.size());

        transformers = QueryUtil.fetchTransformers(false, config.getQueryTransformers());
        Assertions.assertEquals(1, transformers.size());
        Assertions.assertInstanceOf(DefaultQueryTransformer.class, transformers.get(0));
    }

    @Test
    void testMakeErrorMsgUserFriendly() {
        Assertions.assertTrue(
                QueryUtil.makeErrorMsgUserFriendly(new SQLException(new NoSuchTableException("default", "test_ab")))
                        .contains("default"));

        final Exception exception = new IllegalStateException(
                "\tThere is no column\t'age' in table 'test_kylin_fact'.\n"
                        + "Please contact Kylin 5.0 technical support for more details.\n");
        final String errorMsg = QueryUtil.makeErrorMsgUserFriendly(exception);
        Assertions.assertEquals("There is no column\t'age' in table 'test_kylin_fact'.\n"
                + "Please contact Kylin 5.0 technical support for more details.", errorMsg);
    }

    @Test
    void testMakeErrorMsgUserFriendlyForAccessDeniedException() {
        String accessDeniedMsg = "Query failed, access DEFAULT.TEST_KYLIN_FACT denied";
        Exception sqlException = new SQLException("exception while executing query",
                new AccessDeniedException("DEFAULT.TEST_KYLIN_FACT"));
        String errorMessage = QueryUtil.makeErrorMsgUserFriendly(sqlException);
        Assertions.assertEquals(accessDeniedMsg, errorMessage);
    }

    @Test
    void testErrorMsg() {
        String errorMsg = "Error while executing SQL \"select lkp.clsfd_ga_prfl_id, ga.sum_dt, sum(ga.bounces) as bounces, sum(ga.exits) as exits, sum(ga.entrances) as entrances, sum(ga.pageviews) as pageviews, count(distinct ga.GA_VSTR_ID, ga.GA_VST_ID) as visits, count(distinct ga.GA_VSTR_ID) as uniqVistors from CLSFD_GA_PGTYPE_CATEG_LOC ga left join clsfd_ga_prfl_lkp lkp on ga.SRC_GA_PRFL_ID = lkp.SRC_GA_PRFL_ID group by lkp.clsfd_ga_prfl_id,ga.sum_dt order by lkp.clsfd_ga_prfl_id,ga.sum_dt LIMIT 50000\": From line 14, column 14 to line 14, column 29: Column 'CLSFD_GA_PRFL_ID' not found in table 'LKP'";
        Assertions.assertEquals(
                "From line 14, column 14 to line 14, column 29: Column 'CLSFD_GA_PRFL_ID' not found in table 'LKP'\n"
                        + "while executing SQL: \"select lkp.clsfd_ga_prfl_id, ga.sum_dt, sum(ga.bounces) as bounces, sum(ga.exits) as exits, sum(ga.entrances) as entrances, sum(ga.pageviews) as pageviews, count(distinct ga.GA_VSTR_ID, ga.GA_VST_ID) as visits, count(distinct ga.GA_VSTR_ID) as uniqVistors from CLSFD_GA_PGTYPE_CATEG_LOC ga left join clsfd_ga_prfl_lkp lkp on ga.SRC_GA_PRFL_ID = lkp.SRC_GA_PRFL_ID group by lkp.clsfd_ga_prfl_id,ga.sum_dt order by lkp.clsfd_ga_prfl_id,ga.sum_dt LIMIT 50000\"",
                QueryUtil.makeErrorMsgUserFriendly(errorMsg));
    }

    @Test
    void testJudgeSelectStatementStartsWithParentheses() {
        String sql = "(((SELECT COUNT(DISTINCT \"LO_SUPPKEY\"), \"LO_SUPPKEY\", \"LO_ORDERKEY\", \"LO_ORDERDATE\", \"LO_PARTKEY\", \"LO_REVENUE\" "
                + "FROM \"SSB\".\"LINEORDER\" INNER JOIN \"SSB\".\"CUSTOMER\" ON (\"LO_CUSTKEY\" = \"C_CUSTKEY\") "
                + "GROUP BY \"LO_SUPPKEY\", \"LO_ORDERKEY\", \"LO_ORDERDATE\", \"LO_PARTKEY\", \"LO_REVENUE\" "
                + "UNION ALL "
                + "SELECT COUNT(DISTINCT \"LO_SUPPKEY\"), \"LO_SUPPKEY\", \"LO_ORDERKEY\", \"LO_ORDERDATE\", \"LO_PARTKEY\", \"LO_REVENUE\" "
                + "FROM \"SSB\".\"LINEORDER\" INNER JOIN \"SSB\".\"CUSTOMER\" ON (\"LO_CUSTKEY\" = \"C_CUSTKEY\") "
                + "GROUP BY \"LO_SUPPKEY\", \"LO_ORDERKEY\", \"LO_ORDERDATE\", \"LO_PARTKEY\", \"LO_REVENUE\")\n) \n)";
        Assertions.assertTrue(QueryUtil.isSelectStatement(sql));
    }

    @Test
    void testIsSelectStatement() {
        Assertions.assertFalse(QueryUtil.isSelectStatement("insert into person values ('li si', 'beijing');\n;\n"));
        Assertions.assertFalse(QueryUtil.isSelectStatement("update t set name = 'fred' where name = 'lisi' "));
        Assertions.assertFalse(QueryUtil.isSelectStatement("delete from t where name = 'wilson'"));
        Assertions.assertFalse(QueryUtil.isSelectStatement("drop table person"));
        Assertions.assertTrue(QueryUtil.isSelectStatement("with tempName as (select * from t) select * from tempName"));
        Assertions.assertFalse(QueryUtil.isSelectStatement("with tempName "));
        Assertions.assertTrue(QueryUtil.isSelectStatement("explain\n select * from tempName"));
        Assertions
                .assertFalse(QueryUtil.isSelectStatement("explain\n update t set name = 'fred' where name = 'lisi' "));
    }

    @Test
    void testIsSelectStarStatement() {
        Assertions.assertTrue(QueryUtil.isSelectStarStatement("select * from t"));
        Assertions.assertFalse(QueryUtil.isSelectStarStatement("select a from t"));
        Assertions.assertFalse(QueryUtil.isSelectStarStatement("update t set a = 1"));

        // not support yet
        String withClause = "with tempName as (select * from t) select * from tempName";
        Assertions.assertFalse(QueryUtil.isSelectStarStatement(withClause));
    }

    @Test
    void testRemoveCommentWithException() {
        Assertions.assertEquals("", QueryUtil.removeCommentInSql(""));
    }

    @Test
    void testRemoveCommentInSql() {
        //test remove comment when last comment is --
        Assertions.assertEquals(
                "select sum(ITEM_COUNT)\nfrom TEST_KYLIN_FACT\ngroup by CAL_DT\n" + "order by SELLER_ID",
                QueryUtil.removeCommentInSql(
                        "select sum(ITEM_COUNT) --1 /* 7 */\nfrom TEST_KYLIN_FACT  --2 /* 7 */\ngroup by CAL_DT  --3 /* 7 */\n"
                                + "order by SELLER_ID;  --4 /* 7 */\n--5\n/* 7 */\n--6"));
        Assertions.assertEquals(
                "select sum(ITEM_COUNT)\nfrom TEST_KYLIN_FACT\ngroup by CAL_DT\n" + "order by SELLER_ID",
                QueryUtil.removeCommentInSql(
                        "select sum(ITEM_COUNT) --1 /* 7 */\nfrom TEST_KYLIN_FACT  --2 /* 7 */\ngroup by CAL_DT  --3 /* 7 */\n"
                                + "order by SELLER_ID  --4 /* 7 */\n--5\n/* 7 */\n--6"));

        //test remove comment when last comment is /* */
        Assertions.assertEquals(
                "select sum(ITEM_COUNT)\nfrom TEST_KYLIN_FACT\ngroup by CAL_DT\n" + "order by SELLER_ID",
                QueryUtil.removeCommentInSql(
                        "select sum(ITEM_COUNT) --1 /* 7 */\nfrom TEST_KYLIN_FACT  --2 /* 7 */\ngroup by CAL_DT  --3 /* 7 */\n"
                                + "order by SELLER_ID;  --4 /* 7 */\n--5\n/* 7 */"));
        Assertions.assertEquals(
                "select sum(ITEM_COUNT)\nfrom TEST_KYLIN_FACT\ngroup by CAL_DT\n" + "order by SELLER_ID",
                QueryUtil.removeCommentInSql(
                        "select sum(ITEM_COUNT) --1 /* 7 */\nfrom TEST_KYLIN_FACT  --2 /* 7 */\ngroup by CAL_DT  --3 /* 7 */\n"
                                + "order by SELLER_ID  --4 /* 7 */\n--5\n/* 7 */"));

        //test remove comment when comment contain ''
        Assertions.assertEquals("select sum(ITEM_COUNT) 'sum_count'\nfrom TEST_KYLIN_FACT 'table'",
                QueryUtil.removeCommentInSql(
                        "select sum(ITEM_COUNT) 'sum_count' -- 'comment' \nfrom TEST_KYLIN_FACT 'table' --comment"));
        Assertions.assertEquals("select sum(ITEM_COUNT)",
                QueryUtil.removeCommentInSql("select sum(ITEM_COUNT) -- 'comment' --"));

        //test remove comment when comment contain , \t /
        Assertions.assertEquals("select sum(ITEM_COUNT)",
                QueryUtil.removeCommentInSql("select sum(ITEM_COUNT) -- , --\t --/ --"));

        Assertions.assertEquals("select 1", QueryUtil.removeCommentInSql("select 1 --注释"));
        Assertions.assertEquals("select 1", QueryUtil.removeCommentInSql("select 1 /* 注释 */"));
        Assertions.assertEquals("select 1", QueryUtil.removeCommentInSql("select 1\t--注释"));
        Assertions.assertEquals("select 1", QueryUtil.removeCommentInSql("select 1\t/* 注释 */"));
        Assertions.assertEquals("select 1", QueryUtil.removeCommentInSql("select 1\n--注释"));
        Assertions.assertEquals("select 1", QueryUtil.removeCommentInSql("select 1\t/* 注释 */"));
        Assertions.assertEquals("select 1,\n2", QueryUtil.removeCommentInSql("select 1,--注释\n2"));
        Assertions.assertEquals("select 1,\n2", QueryUtil.removeCommentInSql("select 1,/* 注释 */\n2"));
        Assertions.assertEquals("select 4/\n2", QueryUtil.removeCommentInSql("select 4/-- 注释\n2"));
        Assertions.assertEquals("select 4/\n2", QueryUtil.removeCommentInSql("select 4//* 注释 */\n2"));

        Assertions.assertEquals("select 1 'constant_1'", QueryUtil.removeCommentInSql("select 1 'constant_1'--注释''"));
        Assertions.assertEquals("select 1 'constant_1'", QueryUtil.removeCommentInSql("select 1 'constant_1'/* 注释 */"));

        Assertions.assertEquals("select 1", QueryUtil.removeCommentInSql("select 1;--注释"));
        Assertions.assertEquals("select 1", QueryUtil.removeCommentInSql("select 1--注释"));

        Assertions.assertEquals("select 'abc-1'", QueryUtil.removeCommentInSql("select 'abc-1'"));
        Assertions.assertEquals("select ' \t\n,\r/-'", QueryUtil.removeCommentInSql("select ' \t\n,\r/-'"));
        Assertions.assertEquals("select 'abc-1'", QueryUtil.removeCommentInSql("select 'abc-1'--注释"));
        Assertions.assertEquals("select ' \t\n,\r/-'", QueryUtil.removeCommentInSql("select ' \t\n,\r/-'--注释"));
        Assertions.assertEquals("select 'abc-1'", QueryUtil.removeCommentInSql("select 'abc-1'/*注释*/"));
        Assertions.assertEquals("select ' \t\n,\r/-'", QueryUtil.removeCommentInSql("select ' \t\n,\r/-'/*注释*/"));

        Assertions.assertEquals("select 1 \"--注释\"", QueryUtil.removeCommentInSql("select 1 \"--注释\""));
        Assertions.assertEquals("select 1 \"apache's kylin\"",
                QueryUtil.removeCommentInSql("select 1 \"apache's kylin\""));
    }

    @Test
    void testMassageAndExpandComputedColumn() {
        String modelUuid = "abe3bf1a-c4bc-458d-8278-7ea8b00f5e96";
        final KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.query.optimized-sum-cast-double-rule-enabled", "false");
        NDataModelManager modelManager = NDataModelManager.getInstance(config, "default");
        modelManager.updateDataModel(modelUuid, copyForWrite -> {
            ComputedColumnDesc cc = new ComputedColumnDesc();
            cc.setTableAlias("TEST_KYLIN_FACT");
            cc.setTableIdentity("DEFAULT.TEST_KYLIN_FACT");
            cc.setComment("");
            cc.setColumnName("CC1");
            cc.setDatatype("DECIMAL(38,4)");
            cc.setExpression("TEST_KYLIN_FACT.PRICE + 1");
            cc.setInnerExpression("TEST_KYLIN_FACT.PRICE + 1");
            copyForWrite.getComputedColumnDescs().add(cc);
        });

        config.setProperty("kylin.query.transformers", DefaultQueryTransformer.class.getCanonicalName());
        String sql = "select sum(cast(CC1 as double)) from test_kylin_fact";
        String expected = "select SUM(\"TEST_KYLIN_FACT\".\"PRICE\" + 1) from test_kylin_fact";
        QueryParams queryParams = new QueryParams(config, sql, "default", 0, 0, "DEFAULT", true);
        Assertions.assertEquals(expected, QueryUtil.massageSqlAndExpandCC(queryParams));
    }

    @Test
    void testAddLimit() {
        String originString = "select t.TRANS_ID from (\n"
                + "    select * from test_kylin_fact s inner join TEST_ACCOUNT a \n"
                + "        on s.BUYER_ID = a.ACCOUNT_ID inner join TEST_COUNTRY c on c.COUNTRY = a.ACCOUNT_COUNTRY\n"
                + "     limit 10000)t\n";
        String replacedString = QueryUtil.addLimit(originString);
        Assertions.assertEquals(originString.trim().concat(" limit 1"), replacedString);
    }

    @Test
    void testAddLimitWithSemicolon() {
        String origin = "select a from t;;;;\n\t;;;\n;";
        Assertions.assertEquals("select a from t limit 1", QueryUtil.addLimit(origin));

        origin = "select a from t limit 10;";
        Assertions.assertEquals(origin, QueryUtil.addLimit(origin));

        origin = "select a from t limit 10; ;\t;\n;";
        Assertions.assertEquals(origin, QueryUtil.addLimit(origin));
    }

    @Test
    void testAddLimitWithEmpty() {
        String origin = "     ";
        Assertions.assertEquals(origin, QueryUtil.addLimit(origin));

        Assertions.assertNull(QueryUtil.addLimit(null));
    }

    @Test
    void testAddLimitNonSelect() {
        String origin = "aaa";
        Assertions.assertEquals(origin, QueryUtil.addLimit(origin));
    }

    @Test
    void testAdaptCalciteSyntax() {
        Assertions.assertEquals("'a\"b('", QueryUtil.adaptCalciteSyntax("'a\"b('"));
        Assertions.assertEquals("  ", QueryUtil.adaptCalciteSyntax("  "));
        Assertions.assertEquals("", QueryUtil.adaptCalciteSyntax(""));
        Assertions.assertEquals("CEIL(col to year)", QueryUtil.adaptCalciteSyntax("ceil_datetime(col, 'year')"));
        Assertions.assertEquals("CEIL(\"t\".\"col\" to year)",
                QueryUtil.adaptCalciteSyntax("ceil_datetime(`t`.`col`, 'year')"));
        Assertions.assertEquals("TIMESTAMPDIFF(day, t1, t2)",
                QueryUtil.adaptCalciteSyntax("timestampdiff('day', t1, t2)"));

        Assertions.assertEquals("concat(test_kylin_fact.lstg_format_name, '''''')",
                QueryUtil.adaptCalciteSyntax("concat(test_kylin_fact.lstg_format_name, '\\\'\\\'')"));
        Assertions.assertEquals("concat(test_kylin_fact.lstg_format_name, '''''')",
                QueryUtil.adaptCalciteSyntax("concat(test_kylin_fact.lstg_format_name, '\\'\\'')"));
        Assertions.assertEquals("concat(test_kylin_fact.lstg_format_name, '''''')",
                QueryUtil.adaptCalciteSyntax("concat(test_kylin_fact.lstg_format_name, '''''')"));

    }

    @Test
    void testLimitOffsetMatch() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        String sql1 = "select TRANS_ID as test_limit, ORDER_ID as test_offset from TEST_KYLIN_FACT group by TRANS_ID, ORDER_ID";
        QueryParams queryParams1 = new QueryParams(config, sql1, "default", 5, 2, "DEFAULT", true);
        String newSql1 = QueryUtil.massageSql(queryParams1);
        Assertions.assertEquals(
                "select TRANS_ID as test_limit, ORDER_ID as test_offset from TEST_KYLIN_FACT group by TRANS_ID, ORDER_ID\n"
                        + "LIMIT 5\n" + "OFFSET 2",
                newSql1);

        String sql2 = "select TRANS_ID as test_limit, ORDER_ID as test_offset from TEST_KYLIN_FACT group by TRANS_ID, ORDER_ID limit 10 offset 3";
        QueryParams queryParams2 = new QueryParams(config, sql2, "cc_test", 5, 2, "ssb", true);
        String newSql2 = QueryUtil.massageSql(queryParams2);
        Assertions.assertEquals(
                "select TRANS_ID as test_limit, ORDER_ID as test_offset from TEST_KYLIN_FACT group by TRANS_ID, ORDER_ID "
                        + "limit 10 offset 3",
                newSql2);

        String sql3 = "(select TRANS_ID as test_limit, ORDER_ID as test_offset from TEST_KYLIN_FACT group by TRANS_ID, ORDER_ID)limit 10 offset 3";
        QueryParams queryParams3 = new QueryParams(config, sql3, "cc_test", 5, 2, "ssb", true);
        String newSql3 = QueryUtil.massageSql(queryParams3);
        Assertions.assertEquals(
                "(select TRANS_ID as test_limit, ORDER_ID as test_offset from TEST_KYLIN_FACT group by TRANS_ID, ORDER_ID)"
                        + "limit 10 offset 3",
                newSql3);

        String sql4 = "select TRANS_ID as test_limit, ORDER_ID as \"limit\" from TEST_KYLIN_FACT group by TRANS_ID, ORDER_ID";
        QueryParams queryParams4 = new QueryParams(config, sql4, "cc_test", 5, 2, "ssb", true);
        String newSql4 = QueryUtil.massageSql(queryParams4);
        Assertions.assertEquals(
                "select TRANS_ID as test_limit, ORDER_ID as \"limit\" from TEST_KYLIN_FACT group by TRANS_ID, ORDER_ID\n"
                        + "LIMIT 5\n" + "OFFSET 2",
                newSql4);

        String sql5 = "select '\"`OFFSET`\"'";
        QueryParams queryParams5 = new QueryParams(config, sql5, "cc_test", 1, 4, "ssb", true);
        String newSql5 = QueryUtil.massageSql(queryParams5);
        Assertions.assertEquals("select '\"`OFFSET`\"'\n" + "LIMIT 1\n" + "OFFSET 4", newSql5);

        String sql6 = "select TRANS_ID as \"offset\", \"limit\" as \"offset limit\" from TEST_KYLIN_FACT group by TRANS_ID, \"limit\"";
        QueryParams queryParams6 = new QueryParams(config, sql6, "cc_test", 10, 5, "ssb", true);
        String newSql6 = QueryUtil.massageSql(queryParams6);
        Assertions.assertEquals(
                "select TRANS_ID as \"offset\", \"limit\" as \"offset limit\" from TEST_KYLIN_FACT group by TRANS_ID, \"limit\"\n"
                        + "LIMIT 10\n" + "OFFSET 5",
                newSql6);
    }

    @Test
    void testBigQueryPushDown() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.query.share-state-switch-implement", "jdbc");
        config.setProperty("kylin.query.big-query-source-scan-rows-threshold", "10");
        config.setProperty("kylin.query.big-query-pushdown", "true");
        String sql1 = "select TRANS_ID as test_limit, ORDER_ID as test_offset from TEST_KYLIN_FACT group by TRANS_ID, ORDER_ID";
        QueryParams queryParams1 = new QueryParams(config, sql1, "default", 0, 0, "DEFAULT", true);
        String newSql1 = QueryUtil.massageSql(queryParams1);
        Assertions.assertEquals(
                "select TRANS_ID as test_limit, ORDER_ID as test_offset from TEST_KYLIN_FACT group by TRANS_ID, ORDER_ID\n"
                        + "LIMIT 10",
                newSql1);
    }

    @Test
    void testBigQueryPushDownByParams() {
        // no limit offset from backend and front-end
        KylinConfig config = KylinConfig.createKylinConfig(new Properties());
        String sql1 = "select TRANS_ID as test_limit, ORDER_ID as test_offset from TEST_KYLIN_FACT group by TRANS_ID, ORDER_ID";
        QueryParams queryParams1 = new QueryParams(config, sql1, "default", 0, 0, "DEFAULT", true);
        String newSql1 = QueryUtil.massageSql(queryParams1);
        Assertions.assertEquals(
                "select TRANS_ID as test_limit, ORDER_ID as test_offset from TEST_KYLIN_FACT group by TRANS_ID, ORDER_ID",
                newSql1);

        // both limit and offset are 0
        String sql = "select TRANS_ID as test_limit, ORDER_ID as test_offset from TEST_KYLIN_FACT group by TRANS_ID, ORDER_ID";
        QueryParams queryParams = new QueryParams(config, sql, "default", 0, 0, "DEFAULT", true);
        String targetSQL = QueryUtil.massageSql(queryParams);
        Assertions.assertEquals(
                "select TRANS_ID as test_limit, ORDER_ID as test_offset from TEST_KYLIN_FACT group by TRANS_ID, ORDER_ID",
                targetSQL);

        // limit 1 from front-end
        queryParams = new QueryParams(config, sql, "default", 1, 0, "DEFAULT", true);
        targetSQL = QueryUtil.massageSql(queryParams);
        Assertions.assertEquals(
                "select TRANS_ID as test_limit, ORDER_ID as test_offset from TEST_KYLIN_FACT group by TRANS_ID, ORDER_ID\n"
                        + "LIMIT 1",
                targetSQL);
    }

    @Test
    void testAddLimitOffsetBetweenBigQueryPushDownByParamsAndMaxResultRows() {
        KylinConfig config = KylinConfig.createKylinConfig(new Properties());
        // read project config of `kylin.query.max-result-rows`
        String sql = "select TRANS_ID as test_limit, ORDER_ID as test_offset from TEST_KYLIN_FACT group by TRANS_ID, ORDER_ID";
        MetadataTestUtils.updateProjectConfig("default", "kylin.query.max-result-rows", "2");
        QueryParams queryParams = new QueryParams(config, sql, "default", 0, 0, "DEFAULT", true);
        String targetSQL = QueryUtil.massageSql(queryParams);
        Assertions.assertEquals(
                "select TRANS_ID as test_limit, ORDER_ID as test_offset from TEST_KYLIN_FACT group by TRANS_ID, ORDER_ID\n"
                        + "LIMIT 2",
                targetSQL);

        // read project config of `kylin.query.max-result-rows=2` but front-end limit has a high priority
        queryParams = new QueryParams(config, sql, "default", 1, 0, "DEFAULT", true);
        targetSQL = QueryUtil.massageSql(queryParams);
        Assertions.assertEquals(
                "select TRANS_ID as test_limit, ORDER_ID as test_offset from TEST_KYLIN_FACT group by TRANS_ID, ORDER_ID\n"
                        + "LIMIT 1",
                targetSQL);
    }

    @Test
    void testAddLimitOffsetBetweenBigQueryPushDownWithForceLimit() {
        KylinConfig config = KylinConfig.createKylinConfig(new Properties());
        String sql = "select TRANS_ID as test_limit, ORDER_ID as test_offset from TEST_KYLIN_FACT group by TRANS_ID, ORDER_ID";
        // compare the priority of two properties, the `kylin.query.max-result-rows`
        // has higher priority if it is bigger than 0
        {
            Map<String, String> map = Maps.newHashMap();
            map.put("kylin.query.max-result-rows", "-1");
            map.put("kylin.query.force-limit", "3");
            MetadataTestUtils.updateProjectConfig("default", map);
            QueryParams queryParams = new QueryParams(config, sql, "default", 0, 0, "DEFAULT", true);
            String targetSQL = QueryUtil.massageSql(queryParams);
            Assertions.assertEquals(
                    "select TRANS_ID as test_limit, ORDER_ID as test_offset from TEST_KYLIN_FACT group by TRANS_ID, ORDER_ID",
                    targetSQL);

            // the front-end param has a higher priority
            queryParams = new QueryParams(config, sql, "default", 1, 0, "DEFAULT", true);
            targetSQL = QueryUtil.massageSql(queryParams);
            Assertions.assertEquals(
                    "select TRANS_ID as test_limit, ORDER_ID as test_offset from TEST_KYLIN_FACT group by TRANS_ID, ORDER_ID\n"
                            + "LIMIT 1",
                    targetSQL);

            String sql1 = "select * from table1";
            queryParams = new QueryParams(config, sql1, "default", 0, 0, "DEFAULT", true);
            targetSQL = QueryUtil.massageSql(queryParams);
            Assertions.assertEquals("select * from table1" + "\n" + "LIMIT 3", targetSQL);
            queryParams = new QueryParams(config, sql1, "default", 2, 0, "DEFAULT", true);
            targetSQL = QueryUtil.massageSql(queryParams);
            Assertions.assertEquals("select * from table1" + "\n" + "LIMIT 2", targetSQL);
            sql1 = "select * from table1 limit 4";
            queryParams = new QueryParams(config, sql1, "default", 0, 0, "DEFAULT", true);
            targetSQL = QueryUtil.massageSql(queryParams);
            Assertions.assertEquals("select * from table1 limit 4", targetSQL);
            queryParams = new QueryParams(config, sql1, "default", 2, 0, "DEFAULT", true);
            targetSQL = QueryUtil.massageSql(queryParams);
            Assertions.assertEquals("select * from table1 limit 4", targetSQL);
        }

        {
            Map<String, String> map = Maps.newHashMap();
            map.put("kylin.query.force-limit", "-1");
            map.put("kylin.query.share-state-switch-implement", "jdbc");
            MetadataTestUtils.updateProjectConfig("default", map);
            QueryParams queryParams = new QueryParams(config, sql, "default", 0, 0, "DEFAULT", true);
            String targetSQL = QueryUtil.massageSql(queryParams);
            Assertions.assertEquals(
                    "select TRANS_ID as test_limit, ORDER_ID as test_offset from TEST_KYLIN_FACT group by TRANS_ID, ORDER_ID",
                    targetSQL);

            // add one more property `kylin.query.big-query-pushdown`
            MetadataTestUtils.updateProjectConfig("default", "kylin.query.big-query-pushdown", "true");
            queryParams = new QueryParams(config, sql, "default", 0, 0, "DEFAULT", true);
            targetSQL = QueryUtil.massageSql(queryParams);
            Assertions.assertEquals(
                    "select TRANS_ID as test_limit, ORDER_ID as test_offset from TEST_KYLIN_FACT group by TRANS_ID, ORDER_ID",
                    targetSQL);
        }
    }

    @Test
    void testGetSubQueries() throws SqlParseException {
        String s1 = "WITH customer_total_return AS\n" //
                + "  (SELECT sr_customer_sk AS ctr_customer_sk, sr_store_sk AS ctr_store_sk, sum(sr_return_amt) AS ctr_total_return\n"
                + "   FROM store_returns JOIN date_dim ON sr_returned_date_sk = d_date_sk\n" //
                + "   WHERE d_year = 1998 GROUP BY sr_customer_sk, sr_store_sk),\n" //
                + "  tmp AS (  SELECT avg(ctr_total_return)*1.2 tmp_avg, ctr_store_sk\n"
                + "   FROM customer_total_return GROUP BY ctr_store_sk)\n" //
                + "SELECT c_customer_id FROM customer_total_return ctr1\n" //
                + "JOIN tmp ON tmp.ctr_store_sk = ctr1.ctr_store_sk\n" //
                + "JOIN store ON s_store_sk = ctr1.ctr_store_sk\n" //
                + "JOIN customer ON ctr1.ctr_customer_sk = c_customer_sk\n" //
                + "WHERE ctr1.ctr_total_return > tmp_avg AND s_state = 'TN'\n" //
                + "ORDER BY c_customer_id LIMIT 100";
        String s2 = "WITH a1 AS  (WITH a1 AS (SELECT * FROM t) SELECT a1 FROM t2 ORDER BY c_customer_id)\n"
                + "SELECT a1 FROM t2 ORDER BY c_customer_id";
        String s3 = "WITH a1 AS  (SELECT * FROM t)\n" //
                + "SELECT a1 FROM (WITH a2 AS (SELECT * FROM t)  SELECT a2 FROM t2) ORDER BY c_customer_id";

        Assertions.assertEquals(1, SqlSubqueryFinder.getSubqueries(s1).size());
        Assertions.assertEquals(3, SqlSubqueryFinder.getSubqueries(s2).size());
        Assertions.assertEquals(3, SqlSubqueryFinder.getSubqueries(s3).size());
    }

    @Test
    void testErrorCase() throws SqlParseException {
        BiMap<String, String> mockMapping = HashBiMap.create();
        mockMapping.put("t", "t");
        QueryAliasMatchInfo queryAliasMatchInfo = new QueryAliasMatchInfo(mockMapping, null);

        //computed column is null or empty
        String sql = "select a from t";
        List<ComputedColumnDesc> list = Lists.newArrayList();
        List<SqlCall> sqlSelects = SqlSubqueryFinder.getSubqueries(sql);
        ConvertToComputedColumn converter = new ConvertToComputedColumn();
        Assertions.assertEquals("select a from t", converter.replaceComputedColumns(sql,
                converter.collectLatentCcExpList(sqlSelects.get(0)), null, queryAliasMatchInfo).getFirst());
        Assertions.assertEquals("select a from t", converter.replaceComputedColumns(sql,
                converter.collectLatentCcExpList(sqlSelects.get(0)), list, queryAliasMatchInfo).getFirst());

    }
}

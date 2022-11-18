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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Properties;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfig.SetAndUnsetThreadLocalConfig;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.query.security.AccessDeniedException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;

public class QueryUtilTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        QueryUtil.queryTransformers = Collections.emptyList();
        QueryUtil.pushDownConverters = Collections.emptyList();
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    public static final String SQL = "select * from table1";

    @Test
    public void testMaxResultRowsEnabled() {
        try (MockedStatic<KylinConfig> kylinConfigMockedStatic = mockStatic(KylinConfig.class)) {
            KylinConfig kylinConfig = mock(KylinConfig.class);
            kylinConfigMockedStatic.when(KylinConfig::getInstanceFromEnv).thenReturn(kylinConfig);
            when(kylinConfig.getMaxResultRows()).thenReturn(15);
            when(kylinConfig.getForceLimit()).thenReturn(14);
            String result = QueryUtil.normalMassageSql(kylinConfig, SQL, 16, 0);
            assertEquals("select * from table1" + "\n" + "LIMIT 15", result);
        }
    }

    @Test
    public void testCompareMaxResultRowsAndLimit() {
        try (MockedStatic<KylinConfig> kylinConfigMockedStatic = mockStatic(KylinConfig.class)) {
            KylinConfig kylinConfig = mock(KylinConfig.class);
            kylinConfigMockedStatic.when(KylinConfig::getInstanceFromEnv).thenReturn(kylinConfig);
            when(kylinConfig.getMaxResultRows()).thenReturn(15);
            when(kylinConfig.getForceLimit()).thenReturn(14);
            String result = QueryUtil.normalMassageSql(kylinConfig, SQL, 13, 0);
            assertEquals("select * from table1" + "\n" + "LIMIT 13", result);
        }
    }

    @Test
    public void testMassageSql() {
        KylinConfig config = KylinConfig.createKylinConfig(new Properties());
        try (SetAndUnsetThreadLocalConfig autoUnset = KylinConfig.setAndUnsetThreadLocalConfig(config)) {
            config.setProperty("kylin.query.transformers", DefaultQueryTransformer.class.getCanonicalName());

            String sql = "SELECT * FROM TABLE";
            QueryParams queryParams1 = new QueryParams(config, sql, "", 100, 20, "", true);
            String newSql = QueryUtil.massageSql(queryParams1);
            Assert.assertEquals("SELECT * FROM TABLE\nLIMIT 100\nOFFSET 20", newSql);

            String sql2 = "SELECT SUM({fn convert(0, INT)}) from TABLE";
            QueryParams queryParams2 = new QueryParams(config, sql2, "", 0, 0, "", true);
            String newSql2 = QueryUtil.massageSql(queryParams2);
            Assert.assertEquals("SELECT SUM({fn convert(0, INT)}) from TABLE", newSql2);
        }
    }

    @Test
    public void testAdaptCubePriority() {
        {
            String sql = "--CubePriority(m)\nselect price from test_kylin_fact";
            QueryParams queryParams1 = new QueryParams(KylinConfig.getInstanceFromEnv(), sql, "default", 0, 0,
                    "DEFAULT", true);
            String transformed = QueryUtil.massageSql(queryParams1);
            Assert.assertEquals(sql, transformed);
        }

        {
            String sql = "-- CubePriority(m)\nselect price from test_kylin_fact";
            QueryParams queryParams1 = new QueryParams(KylinConfig.getInstanceFromEnv(), sql, "default", 0, 0,
                    "DEFAULT", true);
            String transformed = QueryUtil.massageSql(queryParams1);
            Assert.assertEquals(sql, transformed);
        }
    }

    @Test
    public void testMassageWithoutConvertToComputedColumn() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();

        try (SetAndUnsetThreadLocalConfig autoUnset = KylinConfig.setAndUnsetThreadLocalConfig(config)) {
            // enable ConvertToComputedColumn
            config.setProperty("kylin.query.transformers", "org.apache.kylin.query.util.ConvertToComputedColumn");
            QueryParams queryParams1 = new QueryParams(config, "SELECT price * item_count FROM test_kylin_fact",
                    "default", 0, 0, "DEFAULT", true);
            String newSql1 = QueryUtil.massageSql(queryParams1);
            Assert.assertEquals("SELECT TEST_KYLIN_FACT.DEAL_AMOUNT FROM test_kylin_fact", newSql1);
            QueryParams queryParams2 = new QueryParams(config,
                    "SELECT price * item_count,DEAL_AMOUNT FROM test_kylin_fact", "default", 0, 0, "DEFAULT", true);
            newSql1 = QueryUtil.massageSql(queryParams2);
            Assert.assertEquals("SELECT TEST_KYLIN_FACT.DEAL_AMOUNT,DEAL_AMOUNT FROM test_kylin_fact", newSql1);

            // disable ConvertToComputedColumn
            config.setProperty("kylin.query.transformers", "");
            QueryParams queryParams3 = new QueryParams(config, "SELECT price * item_count FROM test_kylin_fact",
                    "default", 0, 0, "DEFAULT", true);
            String newSql2 = QueryUtil.massageSql(queryParams3);
            Assert.assertEquals("SELECT price * item_count FROM test_kylin_fact", newSql2);
            QueryParams queryParams4 = new QueryParams(config,
                    "SELECT price * item_count,DEAL_AMOUNT FROM test_kylin_fact", "default", 0, 0, "DEFAULT", false);
            newSql2 = QueryUtil.massageSql(queryParams4);
            Assert.assertEquals("SELECT price * item_count,DEAL_AMOUNT FROM test_kylin_fact", newSql2);
        }
    }

    @Test
    public void testConvertedToComputedColumn() {
        String modelUuid = "abe3bf1a-c4bc-458d-8278-7ea8b00f5e96";
        final KylinConfig config = KylinConfig.getInstanceFromEnv();
        NDataModelManager modelManager = NDataModelManager.getInstance(config, "default");
        modelManager.updateDataModel(modelUuid, copyForWrite -> {
            ComputedColumnDesc cc = new ComputedColumnDesc();
            cc.setTableAlias("TEST_KYLIN_FACT");
            cc.setTableIdentity("DEFAULT.TEST_KYLIN_FACT");
            cc.setComment("");
            cc.setColumnName("TMP_CC");
            cc.setDatatype("DECIMAL(38,4)");
            cc.setExpression("TEST_KYLIN_FACT.PRICE + 1");
            cc.setInnerExpression("TEST_KYLIN_FACT.PRICE + 1");
            copyForWrite.getComputedColumnDescs().add(cc);
        });

        config.setProperty("kylin.query.transformers", "org.apache.kylin.query.util.ConvertToComputedColumn");

        // join condition is tableAlias.colName = tableAlias.colName
        String sql = "select price + 1 from test_kylin_fact left join TEST_CATEGORY_GROUPINGS "
                + "on TEST_KYLIN_FACT.LEAF_CATEG_ID = TEST_CATEGORY_GROUPINGS.LEAF_CATEG_ID and  TEST_KYLIN_FACT.LSTG_SITE_ID = TEST_CATEGORY_GROUPINGS.SITE_ID";
        String expected = "select TEST_KYLIN_FACT.TMP_CC from test_kylin_fact left join TEST_CATEGORY_GROUPINGS "
                + "on TEST_KYLIN_FACT.LEAF_CATEG_ID = TEST_CATEGORY_GROUPINGS.LEAF_CATEG_ID and  TEST_KYLIN_FACT.LSTG_SITE_ID = TEST_CATEGORY_GROUPINGS.SITE_ID";
        QueryParams queryParams = new QueryParams(config, sql, "default", 0, 0, "DEFAULT", true);
        String result = QueryUtil.massageSql(queryParams);
        Assert.assertEquals(expected, result);

        // join condition is colName = colName
        String sql2 = "select price + 1 from test_kylin_fact left join TEST_CATEGORY_GROUPINGS "
                + "on TEST_KYLIN_FACT.LEAF_CATEG_ID = TEST_CATEGORY_GROUPINGS.LEAF_CATEG_ID and LSTG_SITE_ID = SITE_ID";
        String expected2 = "select TEST_KYLIN_FACT.TMP_CC from test_kylin_fact left join TEST_CATEGORY_GROUPINGS "
                + "on TEST_KYLIN_FACT.LEAF_CATEG_ID = TEST_CATEGORY_GROUPINGS.LEAF_CATEG_ID and LSTG_SITE_ID = SITE_ID";
        QueryParams queryParams2 = new QueryParams(config, sql2, "default", 0, 0, "DEFAULT", true);
        String result2 = QueryUtil.massageSql(queryParams2);
        Assert.assertEquals(expected2, result2);

        // join condition is colName = colName
        String sql3 = "SELECT PRICE + 1 FROM TEST_KYLIN_FACT LEFT JOIN TEST_CATEGORY_GROUPINGS "
                + "ON \"DEFAULT\".TEST_KYLIN_FACT.LEAF_CATEG_ID = \"DEFAULT\".TEST_CATEGORY_GROUPINGS.LEAF_CATEG_ID"
                + " AND  \"DEFAULT\".TEST_KYLIN_FACT.LSTG_SITE_ID = \"DEFAULT\".TEST_CATEGORY_GROUPINGS.SITE_ID";
        String expected3 = "SELECT TEST_KYLIN_FACT.TMP_CC FROM TEST_KYLIN_FACT LEFT JOIN TEST_CATEGORY_GROUPINGS "
                + "ON \"DEFAULT\".TEST_KYLIN_FACT.LEAF_CATEG_ID = \"DEFAULT\".TEST_CATEGORY_GROUPINGS.LEAF_CATEG_ID "
                + "AND  \"DEFAULT\".TEST_KYLIN_FACT.LSTG_SITE_ID = \"DEFAULT\".TEST_CATEGORY_GROUPINGS.SITE_ID";
        QueryParams queryParams3 = new QueryParams(config, sql3, "default", 0, 0, "DEFAULT", true);
        String result3 = QueryUtil.massageSql(queryParams3);
        Assert.assertEquals(expected3, result3);
    }

    @Test
    public void testMassagePushDownSql() {
        KylinConfig config = KylinConfig.createKylinConfig(new Properties());
        try (SetAndUnsetThreadLocalConfig autoUnset = KylinConfig.setAndUnsetThreadLocalConfig(config)) {
            config.setProperty("kylin.query.pushdown.converter-class-names",
                    SparkSQLFunctionConverter.class.getCanonicalName());
            String sql = "SELECT \"Z_PROVDASH_UM_ED\".\"GENDER\" AS \"GENDER\",\n"
                    + "SUM({fn CONVERT(0, SQL_BIGINT)}) AS \"sum_Calculation_336925569152049156_ok\"\n"
                    + "FROM \"POPHEALTH_ANALYTICS\".\"Z_PROVDASH_UM_ED\" \"Z_PROVDASH_UM_ED\"";

            QueryParams queryParams = new QueryParams("", sql, "default", false);
            queryParams.setKylinConfig(config);
            String massagedSql = QueryUtil.massagePushDownSql(queryParams);
            String expectedSql = "SELECT `Z_PROVDASH_UM_ED`.`GENDER` AS `GENDER`,\n"
                    + "SUM(CAST(0 AS BIGINT)) AS `sum_Calculation_336925569152049156_ok`\n"
                    + "FROM `POPHEALTH_ANALYTICS`.`Z_PROVDASH_UM_ED` `Z_PROVDASH_UM_ED`";
            Assert.assertEquals(expectedSql, massagedSql);
        }
    }

    @Test
    public void testMassagePushDownSqlWithDoubleQuote() {
        KylinConfig config = KylinConfig.createKylinConfig(new Properties());
        String sql = "select '''',trans_id from test_kylin_fact where LSTG_FORMAT_NAME like '%''%' group by trans_id limit 2;";
        QueryParams queryParams = new QueryParams("", sql, "default", false);
        queryParams.setKylinConfig(config);
        String massagedSql = QueryUtil.massagePushDownSql(queryParams);
        String expectedSql = "select '\\'', `TRANS_ID` from `TEST_KYLIN_FACT` where `LSTG_FORMAT_NAME` like '%\\'%' group by `TRANS_ID` limit 2";
        Assert.assertEquals(expectedSql, massagedSql);
    }

    @Test
    public void testInit() {
        KylinConfig config = KylinConfig.createKylinConfig(new Properties());
        try (SetAndUnsetThreadLocalConfig autoUnset = KylinConfig.setAndUnsetThreadLocalConfig(config)) {

            config.setProperty("kylin.query.transformers", DefaultQueryTransformer.class.getCanonicalName());
            Assert.assertEquals(0, QueryUtil.queryTransformers.size());
            QueryUtil.initQueryTransformersIfNeeded(config, true);
            Assert.assertEquals(1, QueryUtil.queryTransformers.size());
            Assert.assertTrue(QueryUtil.queryTransformers.get(0) instanceof DefaultQueryTransformer);

            config.setProperty("kylin.query.transformers", KeywordDefaultDirtyHack.class.getCanonicalName());
            QueryUtil.initQueryTransformersIfNeeded(config, true);
            Assert.assertEquals(1, QueryUtil.queryTransformers.size());
            Assert.assertTrue(QueryUtil.queryTransformers.get(0) instanceof KeywordDefaultDirtyHack);

            QueryUtil.initQueryTransformersIfNeeded(config, false);
            Assert.assertEquals(1, QueryUtil.queryTransformers.size());
            Assert.assertTrue(QueryUtil.queryTransformers.get(0) instanceof KeywordDefaultDirtyHack);

            config.setProperty("kylin.query.transformers", DefaultQueryTransformer.class.getCanonicalName() + ","
                    + ConvertToComputedColumn.class.getCanonicalName());
            QueryUtil.initQueryTransformersIfNeeded(config, true);
            Assert.assertEquals(2, QueryUtil.queryTransformers.size());
            QueryUtil.initQueryTransformersIfNeeded(config, false);
            Assert.assertEquals(1, QueryUtil.queryTransformers.size());
            Assert.assertTrue(QueryUtil.queryTransformers.get(0) instanceof DefaultQueryTransformer);
        }
    }

    @Test
    public void testMakeErrorMsgUserFriendly() {
        Assert.assertTrue(
                QueryUtil.makeErrorMsgUserFriendly(new SQLException(new NoSuchTableException("default", "test_ab")))
                        .contains("default"));

        final Exception exception = new IllegalStateException(
                "\tThere is no column\t'age' in table 'test_kylin_fact'.\n"
                        + "Please contact Kyligence Enterprise technical support for more details.\n");
        final String errorMsg = QueryUtil.makeErrorMsgUserFriendly(exception);
        Assert.assertEquals("There is no column\t'age' in table 'test_kylin_fact'.\n"
                + "Please contact Kyligence Enterprise technical support for more details.", errorMsg);
    }

    @Test
    public void testMakeErrorMsgUserFriendlyForAccessDeniedException() {
        String accessDeniedMsg = "Query failed, access DEFAULT.TEST_KYLIN_FACT denied";
        Exception sqlException = new SQLException("exception while executing query",
                new AccessDeniedException("DEFAULT.TEST_KYLIN_FACT"));
        String errorMessage = QueryUtil.makeErrorMsgUserFriendly(sqlException);
        Assert.assertEquals(accessDeniedMsg, errorMessage);
    }

    @Test
    public void testErrorMsg() {
        String errorMsg = "Error while executing SQL \"select lkp.clsfd_ga_prfl_id, ga.sum_dt, sum(ga.bounces) as bounces, sum(ga.exits) as exits, sum(ga.entrances) as entrances, sum(ga.pageviews) as pageviews, count(distinct ga.GA_VSTR_ID, ga.GA_VST_ID) as visits, count(distinct ga.GA_VSTR_ID) as uniqVistors from CLSFD_GA_PGTYPE_CATEG_LOC ga left join clsfd_ga_prfl_lkp lkp on ga.SRC_GA_PRFL_ID = lkp.SRC_GA_PRFL_ID group by lkp.clsfd_ga_prfl_id,ga.sum_dt order by lkp.clsfd_ga_prfl_id,ga.sum_dt LIMIT 50000\": From line 14, column 14 to line 14, column 29: Column 'CLSFD_GA_PRFL_ID' not found in table 'LKP'";
        Assert.assertEquals(
                "From line 14, column 14 to line 14, column 29: Column 'CLSFD_GA_PRFL_ID' not found in table 'LKP'\n"
                        + "while executing SQL: \"select lkp.clsfd_ga_prfl_id, ga.sum_dt, sum(ga.bounces) as bounces, sum(ga.exits) as exits, sum(ga.entrances) as entrances, sum(ga.pageviews) as pageviews, count(distinct ga.GA_VSTR_ID, ga.GA_VST_ID) as visits, count(distinct ga.GA_VSTR_ID) as uniqVistors from CLSFD_GA_PGTYPE_CATEG_LOC ga left join clsfd_ga_prfl_lkp lkp on ga.SRC_GA_PRFL_ID = lkp.SRC_GA_PRFL_ID group by lkp.clsfd_ga_prfl_id,ga.sum_dt order by lkp.clsfd_ga_prfl_id,ga.sum_dt LIMIT 50000\"",
                QueryUtil.makeErrorMsgUserFriendly(errorMsg));
    }

    @Test
    public void testJudgeSelectStatementStartsWithParentheses() {
        String sql = "(((SELECT COUNT(DISTINCT \"LO_SUPPKEY\"), \"LO_SUPPKEY\", \"LO_ORDERKEY\", \"LO_ORDERDATE\", \"LO_PARTKEY\", \"LO_REVENUE\" "
                + "FROM \"SSB\".\"LINEORDER\" INNER JOIN \"SSB\".\"CUSTOMER\" ON (\"LO_CUSTKEY\" = \"C_CUSTKEY\") "
                + "GROUP BY \"LO_SUPPKEY\", \"LO_ORDERKEY\", \"LO_ORDERDATE\", \"LO_PARTKEY\", \"LO_REVENUE\" "
                + "UNION ALL "
                + "SELECT COUNT(DISTINCT \"LO_SUPPKEY\"), \"LO_SUPPKEY\", \"LO_ORDERKEY\", \"LO_ORDERDATE\", \"LO_PARTKEY\", \"LO_REVENUE\" "
                + "FROM \"SSB\".\"LINEORDER\" INNER JOIN \"SSB\".\"CUSTOMER\" ON (\"LO_CUSTKEY\" = \"C_CUSTKEY\") "
                + "GROUP BY \"LO_SUPPKEY\", \"LO_ORDERKEY\", \"LO_ORDERDATE\", \"LO_PARTKEY\", \"LO_REVENUE\")\n) \n)";
        Assert.assertTrue(QueryUtil.isSelectStatement(sql));
    }

    @Test
    public void testIsSelectStatement() {
        Assert.assertFalse(QueryUtil.isSelectStatement("insert into person values ('li si', 'beijing');\n;\n"));
        Assert.assertFalse(QueryUtil.isSelectStatement("update t set name = 'fred' where name = 'lisi' "));
        Assert.assertFalse(QueryUtil.isSelectStatement("delete from t where name = 'wilson'"));
        Assert.assertFalse(QueryUtil.isSelectStatement("drop table person"));
        Assert.assertTrue(QueryUtil.isSelectStatement("with tempName as (select * from t) select * from tempName"));
        Assert.assertFalse(QueryUtil.isSelectStatement("with tempName "));
        Assert.assertTrue(QueryUtil.isSelectStatement("explain\n select * from tempName"));
        Assert.assertFalse(QueryUtil.isSelectStatement("explain\n update t set name = 'fred' where name = 'lisi' "));
    }

    @Test
    public void testIsSelectStarStatement() {
        Assert.assertTrue(QueryUtil.isSelectStarStatement("select * from t"));
        Assert.assertFalse(QueryUtil.isSelectStarStatement("select a from t"));
        Assert.assertFalse(QueryUtil.isSelectStarStatement("update t set a = 1"));

        // not support yet
        String withClause = "with tempName as (select * from t) select * from tempName";
        Assert.assertFalse(QueryUtil.isSelectStarStatement(withClause));
    }

    @Test
    public void testRemoveCommentWithException() {
        Assert.assertEquals("", QueryUtil.removeCommentInSql(""));
    }

    @Test
    public void testRemoveCommentInSql() {
        //test remove comment when last comment is --
        Assert.assertEquals("select sum(ITEM_COUNT)\nfrom TEST_KYLIN_FACT\ngroup by CAL_DT\n" + "order by SELLER_ID",
                QueryUtil.removeCommentInSql(
                        "select sum(ITEM_COUNT) --1 /* 7 */\nfrom TEST_KYLIN_FACT  --2 /* 7 */\ngroup by CAL_DT  --3 /* 7 */\n"
                                + "order by SELLER_ID;  --4 /* 7 */\n--5\n/* 7 */\n--6"));
        Assert.assertEquals("select sum(ITEM_COUNT)\nfrom TEST_KYLIN_FACT\ngroup by CAL_DT\n" + "order by SELLER_ID",
                QueryUtil.removeCommentInSql(
                        "select sum(ITEM_COUNT) --1 /* 7 */\nfrom TEST_KYLIN_FACT  --2 /* 7 */\ngroup by CAL_DT  --3 /* 7 */\n"
                                + "order by SELLER_ID  --4 /* 7 */\n--5\n/* 7 */\n--6"));

        //test remove comment when last comment is /* */
        Assert.assertEquals("select sum(ITEM_COUNT)\nfrom TEST_KYLIN_FACT\ngroup by CAL_DT\n" + "order by SELLER_ID",
                QueryUtil.removeCommentInSql(
                        "select sum(ITEM_COUNT) --1 /* 7 */\nfrom TEST_KYLIN_FACT  --2 /* 7 */\ngroup by CAL_DT  --3 /* 7 */\n"
                                + "order by SELLER_ID;  --4 /* 7 */\n--5\n/* 7 */"));
        Assert.assertEquals("select sum(ITEM_COUNT)\nfrom TEST_KYLIN_FACT\ngroup by CAL_DT\n" + "order by SELLER_ID",
                QueryUtil.removeCommentInSql(
                        "select sum(ITEM_COUNT) --1 /* 7 */\nfrom TEST_KYLIN_FACT  --2 /* 7 */\ngroup by CAL_DT  --3 /* 7 */\n"
                                + "order by SELLER_ID  --4 /* 7 */\n--5\n/* 7 */"));

        //test remove comment when comment contain ''
        Assert.assertEquals("select sum(ITEM_COUNT) 'sum_count'\nfrom TEST_KYLIN_FACT 'table'",
                QueryUtil.removeCommentInSql(
                        "select sum(ITEM_COUNT) 'sum_count' -- 'comment' \nfrom TEST_KYLIN_FACT 'table' --comment"));
        Assert.assertEquals("select sum(ITEM_COUNT)",
                QueryUtil.removeCommentInSql("select sum(ITEM_COUNT) -- 'comment' --"));

        //test remove comment when comment contain , \t /
        Assert.assertEquals("select sum(ITEM_COUNT)",
                QueryUtil.removeCommentInSql("select sum(ITEM_COUNT) -- , --\t --/ --"));

        Assert.assertEquals("select 1", QueryUtil.removeCommentInSql("select 1 --注释"));
        Assert.assertEquals("select 1", QueryUtil.removeCommentInSql("select 1 /* 注释 */"));
        Assert.assertEquals("select 1", QueryUtil.removeCommentInSql("select 1\t--注释"));
        Assert.assertEquals("select 1", QueryUtil.removeCommentInSql("select 1\t/* 注释 */"));
        Assert.assertEquals("select 1", QueryUtil.removeCommentInSql("select 1\n--注释"));
        Assert.assertEquals("select 1", QueryUtil.removeCommentInSql("select 1\t/* 注释 */"));
        Assert.assertEquals("select 1,\n2", QueryUtil.removeCommentInSql("select 1,--注释\n2"));
        Assert.assertEquals("select 1,\n2", QueryUtil.removeCommentInSql("select 1,/* 注释 */\n2"));
        Assert.assertEquals("select 4/\n2", QueryUtil.removeCommentInSql("select 4/-- 注释\n2"));
        Assert.assertEquals("select 4/\n2", QueryUtil.removeCommentInSql("select 4//* 注释 */\n2"));

        Assert.assertEquals("select 1 'constant_1'", QueryUtil.removeCommentInSql("select 1 'constant_1'--注释''"));
        Assert.assertEquals("select 1 'constant_1'", QueryUtil.removeCommentInSql("select 1 'constant_1'/* 注释 */"));

        Assert.assertEquals("select 1", QueryUtil.removeCommentInSql("select 1;--注释"));
        Assert.assertEquals("select 1", QueryUtil.removeCommentInSql("select 1--注释"));

        Assert.assertEquals("select 'abc-1'", QueryUtil.removeCommentInSql("select 'abc-1'"));
        Assert.assertEquals("select ' \t\n,\r/-'", QueryUtil.removeCommentInSql("select ' \t\n,\r/-'"));
        Assert.assertEquals("select 'abc-1'", QueryUtil.removeCommentInSql("select 'abc-1'--注释"));
        Assert.assertEquals("select ' \t\n,\r/-'", QueryUtil.removeCommentInSql("select ' \t\n,\r/-'--注释"));
        Assert.assertEquals("select 'abc-1'", QueryUtil.removeCommentInSql("select 'abc-1'/*注释*/"));
        Assert.assertEquals("select ' \t\n,\r/-'", QueryUtil.removeCommentInSql("select ' \t\n,\r/-'/*注释*/"));

        Assert.assertEquals("select 1 \"--注释\"", QueryUtil.removeCommentInSql("select 1 \"--注释\""));
        Assert.assertEquals("select 1 \"apache's kylin\"", QueryUtil.removeCommentInSql("select 1 \"apache's kylin\""));
    }

    @Test
    public void testMassageAndExpandComputedColumn() {
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
        Assert.assertEquals(expected, QueryUtil.massageSqlAndExpandCC(queryParams));
    }

    @Test
    public void testAddLimit() {
        String originString = "select t.TRANS_ID from (\n"
                + "    select * from test_kylin_fact s inner join TEST_ACCOUNT a \n"
                + "        on s.BUYER_ID = a.ACCOUNT_ID inner join TEST_COUNTRY c on c.COUNTRY = a.ACCOUNT_COUNTRY\n"
                + "     limit 10000)t\n";
        String replacedString = QueryUtil.addLimit(originString);
        Assert.assertEquals(originString.trim().concat(" limit 1"), replacedString);
    }

    @Test
    public void testAddLimitWithSemicolon() {
        String origin = "select a from t;;;;\n\t;;;\n;";
        Assert.assertEquals("select a from t limit 1", QueryUtil.addLimit(origin));

        origin = "select a from t limit 10;";
        Assert.assertEquals(origin, QueryUtil.addLimit(origin));

        origin = "select a from t limit 10; ;\t;\n;";
        Assert.assertEquals(origin, QueryUtil.addLimit(origin));
    }

    @Test
    public void testAddLimitWithEmpty() {
        String origin = "     ";
        Assert.assertEquals(origin, QueryUtil.addLimit(origin));

        Assert.assertNull(QueryUtil.addLimit(null));
    }

    @Test
    public void testAddLimitNonSelect() {
        String origin = "aaa";
        Assert.assertEquals(origin, QueryUtil.addLimit(origin));
    }

    @Test
    public void testReplaceCC() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        String sql1 = "select EXTRACT(minute FROM lineorder.lo_orderdate) from lineorder inner join customer on lineorder.lo_custkey = customer.c_custkey";
        QueryParams queryParams1 = new QueryParams(config, sql1, "cc_test", 0, 0, "ssb", true);
        String newSql1 = QueryUtil.massageSql(queryParams1);
        Assert.assertEquals(
                "select LINEORDER.CC_EXTRACT from lineorder inner join customer on lineorder.lo_custkey = customer.c_custkey",
                newSql1);

        String sql2 = "select {fn convert(lineorder.lo_orderkey, double)} from lineorder inner join customer on lineorder.lo_custkey = customer.c_custkey";
        QueryParams queryParams2 = new QueryParams(config, sql2, "cc_test", 0, 0, "ssb", true);
        String newSql2 = QueryUtil.massageSql(queryParams2);
        Assert.assertEquals(
                "select LINEORDER.CC_CAST_LO_ORDERKEY from lineorder inner join customer on lineorder.lo_custkey = customer.c_custkey",
                newSql2);

    }

    @Test
    public void testLimitOffsetMatch() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        String sql1 = "select TRANS_ID as test_limit, ORDER_ID as test_offset from TEST_KYLIN_FACT group by TRANS_ID, ORDER_ID";
        QueryParams queryParams1 = new QueryParams(config, sql1, "default", 5, 2, "DEFAULT", true);
        String newSql1 = QueryUtil.massageSql(queryParams1);
        Assert.assertEquals(
                "select TRANS_ID as test_limit, ORDER_ID as test_offset from TEST_KYLIN_FACT group by TRANS_ID, ORDER_ID\n"
                        + "LIMIT 5\n" + "OFFSET 2",
                newSql1);

        String sql2 = "select TRANS_ID as test_limit, ORDER_ID as test_offset from TEST_KYLIN_FACT group by TRANS_ID, ORDER_ID limit 10 offset 3";
        QueryParams queryParams2 = new QueryParams(config, sql2, "cc_test", 5, 2, "ssb", true);
        String newSql2 = QueryUtil.massageSql(queryParams2);
        Assert.assertEquals(
                "select TRANS_ID as test_limit, ORDER_ID as test_offset from TEST_KYLIN_FACT group by TRANS_ID, ORDER_ID "
                        + "limit 10 offset 3",
                newSql2);

        String sql3 = "(select TRANS_ID as test_limit, ORDER_ID as test_offset from TEST_KYLIN_FACT group by TRANS_ID, ORDER_ID)limit 10 offset 3";
        QueryParams queryParams3 = new QueryParams(config, sql3, "cc_test", 5, 2, "ssb", true);
        String newSql3 = QueryUtil.massageSql(queryParams3);
        Assert.assertEquals(
                "(select TRANS_ID as test_limit, ORDER_ID as test_offset from TEST_KYLIN_FACT group by TRANS_ID, ORDER_ID)"
                        + "limit 10 offset 3",
                newSql3);

        String sql4 = "select TRANS_ID as test_limit, ORDER_ID as \"limit\" from TEST_KYLIN_FACT group by TRANS_ID, ORDER_ID";
        QueryParams queryParams4 = new QueryParams(config, sql4, "cc_test", 5, 2, "ssb", true);
        String newSql4 = QueryUtil.massageSql(queryParams4);
        Assert.assertEquals(
                "select TRANS_ID as test_limit, ORDER_ID as \"limit\" from TEST_KYLIN_FACT group by TRANS_ID, ORDER_ID\n"
                        + "LIMIT 5\n" + "OFFSET 2",
                newSql4);

        String sql5 = "select '\"`OFFSET`\"'";
        QueryParams queryParams5 = new QueryParams(config, sql5, "cc_test", 1, 4, "ssb", true);
        String newSql5 = QueryUtil.massageSql(queryParams5);
        Assert.assertEquals("select '\"`OFFSET`\"'\n" + "LIMIT 1\n" + "OFFSET 4", newSql5);

        String sql6 = "select TRANS_ID as \"offset\", \"limit\" as \"offset limit\" from TEST_KYLIN_FACT group by TRANS_ID, \"limit\"";
        QueryParams queryParams6 = new QueryParams(config, sql6, "cc_test", 10, 5, "ssb", true);
        String newSql6 = QueryUtil.massageSql(queryParams6);
        Assert.assertEquals(
                "select TRANS_ID as \"offset\", \"limit\" as \"offset limit\" from TEST_KYLIN_FACT group by TRANS_ID, \"limit\"\n"
                        + "LIMIT 10\n" + "OFFSET 5",
                newSql6);
    }

    @Test
    public void testMassagePushDownSqlWithDialectConverter() {
        KylinConfig config = KylinConfig.createKylinConfig(new Properties());
        try (SetAndUnsetThreadLocalConfig autoUnset = KylinConfig.setAndUnsetThreadLocalConfig(config)) {
            config.setProperty("kylin.query.pushdown.converter-class-names",
                    "org.apache.kylin.query.util.DialectConverter,org.apache.kylin.source.adhocquery.DoubleQuotePushDownConverter,"
                            + SparkSQLFunctionConverter.class.getCanonicalName());
            String sql = "SELECT \"Z_PROVDASH_UM_ED\".\"GENDER\" AS \"GENDER\",\n"
                    + "SUM({fn CONVERT(0, SQL_BIGINT)}) AS \"sum_Calculation_336925569152049156_ok\"\n"
                    + "FROM \"POPHEALTH_ANALYTICS\".\"Z_PROVDASH_UM_ED\" \"Z_PROVDASH_UM_ED\""
                    + " fetch first 1 rows only";

            QueryParams queryParams = new QueryParams("", sql, "default", false);
            queryParams.setKylinConfig(config);
            String massagedSql = QueryUtil.massagePushDownSql(queryParams);
            String expectedSql = "SELECT `Z_PROVDASH_UM_ED`.`GENDER` AS `GENDER`, "
                    + "SUM(CAST(0 AS BIGINT)) AS `sum_Calculation_336925569152049156_ok`\n"
                    + "FROM `POPHEALTH_ANALYTICS`.`Z_PROVDASH_UM_ED` AS `Z_PROVDASH_UM_ED`\n" + "LIMIT 1";
            Assert.assertEquals(expectedSql, massagedSql);
        }
    }

    @Test
    public void testBigQueryPushDown() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.query.share-state-switch-implement", "jdbc");
        config.setProperty("kylin.query.big-query-source-scan-rows-threshold", "10");
        config.setProperty("kylin.query.big-query-pushdown", "true");
        String sql1 = "select TRANS_ID as test_limit, ORDER_ID as test_offset from TEST_KYLIN_FACT group by TRANS_ID, ORDER_ID";
        QueryParams queryParams1 = new QueryParams(config, sql1, "default", 0, 0, "DEFAULT", true);
        String newSql1 = QueryUtil.massageSql(queryParams1);
        Assert.assertEquals(
                "select TRANS_ID as test_limit, ORDER_ID as test_offset from TEST_KYLIN_FACT group by TRANS_ID, ORDER_ID\n"
                        + "LIMIT 10",
                newSql1);
    }

    @Test
    public void testBigQueryPushDownByParams() {
        KylinConfig config = KylinConfig.createKylinConfig(new Properties());
        String sql1 = "select TRANS_ID as test_limit, ORDER_ID as test_offset from TEST_KYLIN_FACT group by TRANS_ID, ORDER_ID";
        QueryParams queryParams1 = new QueryParams(config, sql1, "default", 0, 0, "DEFAULT", true);
        String newSql1 = QueryUtil.massageSql(queryParams1);
        Assert.assertEquals(
                "select TRANS_ID as test_limit, ORDER_ID as test_offset from TEST_KYLIN_FACT group by TRANS_ID, ORDER_ID",
                newSql1);
        String sql = "select TRANS_ID as test_limit, ORDER_ID as test_offset from TEST_KYLIN_FACT group by TRANS_ID, ORDER_ID";
        QueryParams queryParams = new QueryParams(config, sql, "default", 0, 0, "DEFAULT", true);
        String targetSQL = QueryUtil.massageSql(queryParams);
        Assert.assertEquals(
                "select TRANS_ID as test_limit, ORDER_ID as test_offset from TEST_KYLIN_FACT group by TRANS_ID, ORDER_ID",
                targetSQL);
        queryParams = new QueryParams(config, sql, "default", 1, 0, "DEFAULT", true);
        targetSQL = QueryUtil.massageSql(queryParams);
        Assert.assertEquals(
                "select TRANS_ID as test_limit, ORDER_ID as test_offset from TEST_KYLIN_FACT group by TRANS_ID, ORDER_ID\n"
                        + "LIMIT 1",
                targetSQL);
        config.setProperty("kylin.query.max-result-rows", "2");
        queryParams = new QueryParams(config, sql, "default", 0, 0, "DEFAULT", true);
        targetSQL = QueryUtil.massageSql(queryParams);
        Assert.assertEquals(
                "select TRANS_ID as test_limit, ORDER_ID as test_offset from TEST_KYLIN_FACT group by TRANS_ID, ORDER_ID\n"
                        + "LIMIT 2",
                targetSQL);
        queryParams = new QueryParams(config, sql, "default", 1, 0, "DEFAULT", true);
        targetSQL = QueryUtil.massageSql(queryParams);
        Assert.assertEquals(
                "select TRANS_ID as test_limit, ORDER_ID as test_offset from TEST_KYLIN_FACT group by TRANS_ID, ORDER_ID\n"
                        + "LIMIT 1",
                targetSQL);
        config.setProperty("kylin.query.max-result-rows", "-1");
        config.setProperty("kylin.query.force-limit", "3");
        queryParams = new QueryParams(config, sql, "default", 0, 0, "DEFAULT", true);
        targetSQL = QueryUtil.massageSql(queryParams);
        Assert.assertEquals(
                "select TRANS_ID as test_limit, ORDER_ID as test_offset from TEST_KYLIN_FACT group by TRANS_ID, ORDER_ID",
                targetSQL);
        queryParams = new QueryParams(config, sql, "default", 1, 0, "DEFAULT", true);
        targetSQL = QueryUtil.massageSql(queryParams);
        Assert.assertEquals(
                "select TRANS_ID as test_limit, ORDER_ID as test_offset from TEST_KYLIN_FACT group by TRANS_ID, ORDER_ID\n"
                        + "LIMIT 1",
                targetSQL);
        sql1 = "select * from table1";
        queryParams = new QueryParams(config, sql1, "default", 0, 0, "DEFAULT", true);
        targetSQL = QueryUtil.massageSql(queryParams);
        Assert.assertEquals("select * from table1" + "\n" + "LIMIT 3", targetSQL);
        queryParams = new QueryParams(config, sql1, "default", 2, 0, "DEFAULT", true);
        targetSQL = QueryUtil.massageSql(queryParams);
        Assert.assertEquals("select * from table1" + "\n" + "LIMIT 2", targetSQL);
        sql1 = "select * from table1 limit 4";
        queryParams = new QueryParams(config, sql1, "default", 0, 0, "DEFAULT", true);
        targetSQL = QueryUtil.massageSql(queryParams);
        Assert.assertEquals("select * from table1 limit 4", targetSQL);
        queryParams = new QueryParams(config, sql1, "default", 2, 0, "DEFAULT", true);
        targetSQL = QueryUtil.massageSql(queryParams);
        Assert.assertEquals("select * from table1 limit 4", targetSQL);
        config.setProperty("kylin.query.force-limit", "-1");
        config.setProperty("kylin.query.share-state-switch-implement", "jdbc");
        queryParams = new QueryParams(config, sql, "default", 0, 0, "DEFAULT", true);
        targetSQL = QueryUtil.massageSql(queryParams);
        Assert.assertEquals(
                "select TRANS_ID as test_limit, ORDER_ID as test_offset from TEST_KYLIN_FACT group by TRANS_ID, ORDER_ID",
                targetSQL);
        config.setProperty("kylin.query.big-query-pushdown", "true");
        queryParams = new QueryParams(config, sql, "default", 0, 0, "DEFAULT", true);
        targetSQL = QueryUtil.massageSql(queryParams);
        Assert.assertEquals(
                "select TRANS_ID as test_limit, ORDER_ID as test_offset from TEST_KYLIN_FACT group by TRANS_ID, ORDER_ID",
                targetSQL);
    }

    @Test
    public void testReplaceDoubleQuoteToSingle() {
        String sql = "select ab from table where aa = '' and bb = '''as''n'''";
        String resSql = "select ab from table where aa = '' and bb = '\\'as\\'n\\''";
        Assert.assertEquals(resSql, QueryUtil.replaceDoubleQuoteToSingle(sql));
    }
}

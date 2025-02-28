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

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.OlapContext;
import org.apache.kylin.util.OlapContextTestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * test against real models
 */
@MetadataInfo(overlay = "../core-metadata/src/test/resources/ut_meta/ccjointest")
class RewriteCcOnRealModelTest {
    private final ConvertToComputedColumn converter = new ConvertToComputedColumn();

    @BeforeEach
    public void setUp() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NDataModelManager modelManager = NDataModelManager.getInstance(config, "default");
        for (String modelId : modelManager.listAllModelIds()) {
            modelManager.updateDataModel(modelId, copyForWrite -> copyForWrite.setPartitionDesc(null));
        }
    }

    @Test
    void testConvertSingleTableCC() throws SqlParseException {
        {
            String originSql = "select count(*), sum (price * item_count) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID"
                    + " left join test_account a on o.buyer_id = a.account_id group by ACCOUNT_COUNTRY";
            List<OlapContext> olapContexts = OlapContextTestUtil.getOlapContexts("default", originSql, false);
            OlapContext olapContext = olapContexts.get(0);
            List<FunctionDesc> aggList = olapContext.getAggregations().stream()
                    .filter(agg -> agg.getExpression().equals("SUM")).collect(Collectors.toList());
            Assertions.assertEquals(1, aggList.size());
            Assertions.assertEquals("[DEFAULT.TEST_KYLIN_FACT.DEAL_AMOUNT]", aggList.get(0).getParameters().toString());
        }

        {
            String originSql = "select count(*), sum (price * item_count) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID"
                    + " left join test_account a on o.buyer_id = a.account_id group by ACCOUNT_COUNTRY order by sum(price * item_count)";

            List<OlapContext> olapContexts = OlapContextTestUtil.getOlapContexts("default", originSql, false);
            OlapContext olapContext = olapContexts.get(0);
            List<FunctionDesc> aggList = olapContext.getAggregations().stream()
                    .filter(agg -> agg.getExpression().equals("SUM")).collect(Collectors.toList());
            Assertions.assertEquals(1, aggList.size());
            Assertions.assertEquals("[DEFAULT.TEST_KYLIN_FACT.DEAL_AMOUNT]", aggList.get(0).getParameters().toString());
        }

        {
            String originSql = "select count(*), sum (price * item_count) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID"
                    + " left join test_account a on o.buyer_id = a.account_id group by ACCOUNT_COUNTRY" + " union"
                    + " select count(*), sum (price * item_count) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID"
                    + " left join test_account a on o.buyer_id = a.account_id group by ACCOUNT_COUNTRY";
            List<OlapContext> olapContexts = OlapContextTestUtil.getOlapContexts("default", originSql, false);
            Assertions.assertEquals(2, olapContexts.size());
            for (OlapContext olapContext : olapContexts) {
                List<FunctionDesc> aggList = olapContext.getAggregations().stream()
                        .filter(agg -> agg.getExpression().equals("SUM")).collect(Collectors.toList());
                Assertions.assertEquals(1, aggList.size());
                Assertions.assertEquals("[DEFAULT.TEST_KYLIN_FACT.DEAL_AMOUNT]",
                        aggList.get(0).getParameters().toString());
            }
        }

        {
            String originSql = "select count(*), sum (price * item_count) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID"
                    + " left join test_account a on o.buyer_id = a.account_id group by substring(ACCOUNT_COUNTRY,0,1)";
            List<OlapContext> olapContexts = OlapContextTestUtil.getOlapContexts("default", originSql, false);
            OlapContext olapContext = olapContexts.get(0);
            List<FunctionDesc> aggList = olapContext.getAggregations().stream()
                    .filter(agg -> agg.getExpression().equals("SUM")).collect(Collectors.toList());
            Assertions.assertEquals(1, aggList.size());
            Assertions.assertEquals("[DEFAULT.TEST_KYLIN_FACT.DEAL_AMOUNT]", aggList.get(0).getParameters().toString());
            Assertions.assertEquals("LEFTJOIN_BUYER_COUNTRY_ABBR",
                    olapContext.getGroupByColumns().iterator().next().getName());
        }
    }

    @Test
    void testConvertCrossTableCC() throws SqlParseException {
        {
            //buyer
            String originSql = "select count(*), sum (price * item_count) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID"
                    + " left join test_account a on o.buyer_id = a.account_id  left join test_country c on a.account_country = c.country group by concat(a.ACCOUNT_ID, c.NAME)";
            List<OlapContext> olapContexts = OlapContextTestUtil.getOlapContexts("default", originSql, false);
            OlapContext olapContext = olapContexts.get(0);
            List<FunctionDesc> aggList = olapContext.getAggregations().stream()
                    .filter(agg -> agg.getExpression().equals("SUM")).collect(Collectors.toList());
            Assertions.assertEquals(1, aggList.size());
            Assertions.assertEquals("[DEFAULT.TEST_KYLIN_FACT.DEAL_AMOUNT]", aggList.get(0).getParameters().toString());
            Assertions.assertEquals("LEFTJOIN_BUYER_ID_AND_COUNTRY_NAME",
                    olapContext.getGroupByColumns().iterator().next().getName());
        }

        {
            //seller
            String originSql = "select count(*), sum (price * item_count) from test_kylin_fact f"
                    + " left join test_account a on f.seller_id = a.account_id  left join test_country c on a.account_country = c.country group by concat(a.ACCOUNT_ID, c.NAME)";
            List<OlapContext> olapContexts = OlapContextTestUtil.getOlapContexts("default", originSql, false);
            OlapContext olapContext = olapContexts.get(0);
            List<FunctionDesc> aggList = olapContext.getAggregations().stream()
                    .filter(agg -> agg.getExpression().equals("SUM")).collect(Collectors.toList());
            Assertions.assertEquals(1, aggList.size());
            Assertions.assertEquals("[DEFAULT.TEST_KYLIN_FACT.DEAL_AMOUNT]", aggList.get(0).getParameters().toString());
            Assertions.assertEquals("LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME",
                    olapContext.getGroupByColumns().iterator().next().getName());
        }

        {
            //seller, but swap join condition
            String originSql = "select count(*), sum (price * item_count) from test_kylin_fact f"
                    + " left join test_account a on f.seller_id = a.account_id  left join test_country c on country = account_country group by concat(a.ACCOUNT_ID, c.NAME)";
            List<OlapContext> olapContexts = OlapContextTestUtil.getOlapContexts("default", originSql, false);
            OlapContext olapContext = olapContexts.get(0);
            List<FunctionDesc> aggList = olapContext.getAggregations().stream()
                    .filter(agg -> agg.getExpression().equals("SUM")).collect(Collectors.toList());
            Assertions.assertEquals(1, aggList.size());
            Assertions.assertEquals("[DEFAULT.TEST_KYLIN_FACT.DEAL_AMOUNT]", aggList.get(0).getParameters().toString());
            Assertions.assertEquals("LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME",
                    olapContext.getGroupByColumns().iterator().next().getName());
        }
    }

    @Test
    void testSubQuery() throws SqlParseException {
        {
            String sql = "select count(*), DEAL_AMOUNT from (select count(*), sum (price * item_count) as DEAL_AMOUNT from test_kylin_fact) group by DEAL_AMOUNT";
            List<OlapContext> olapContexts = OlapContextTestUtil.getOlapContexts("default", sql, true, true);
            OlapContext olapContext = olapContexts.get(0);
            List<FunctionDesc> aggList = olapContext.getAggregations().stream()
                    .filter(agg -> agg.getExpression().equals("SUM")).collect(Collectors.toList());
            Assertions.assertEquals(1, aggList.size());
            Assertions.assertEquals("[DEFAULT.TEST_KYLIN_FACT.DEAL_AMOUNT]", aggList.get(0).getParameters().toString());
        }

        {
            // pruned by query optimization rules, therefore, only the count star aggregation is left
            String sql = "select count(*) from (select count(*), sum (price * item_count) from test_kylin_fact) f";
            List<OlapContext> olapContexts = OlapContextTestUtil.getOlapContexts("default", sql, true, true);
            OlapContext olapContext = olapContexts.get(0);
            List<FunctionDesc> aggList = olapContext.getAggregations().stream()
                    .filter(agg -> agg.getExpression().equals("SUM")).collect(Collectors.toList());
            Assertions.assertEquals(0, aggList.size());
        }

        {
            String sql = "select sum (price * item_count) from (select * from test_kylin_fact)";
            List<OlapContext> olapContexts = OlapContextTestUtil.getOlapContexts("default", sql, true, true);
            OlapContext olapContext = olapContexts.get(0);
            List<FunctionDesc> aggList = olapContext.getAggregations().stream()
                    .filter(agg -> agg.getExpression().equals("SUM")).collect(Collectors.toList());
            Assertions.assertEquals(1, aggList.size());
            Assertions.assertEquals("[DEFAULT.TEST_KYLIN_FACT.DEAL_AMOUNT]", aggList.get(0).getParameters().toString());
        }

        {
            // can we make a rule to push agg into each subQuery of union?
            String sql = "select sum (price * item_count) from (select * from TEST_KYLIN_FACT where CAL_DT < DATE '2012-06-01' union select * from TEST_KYLIN_FACT where CAL_DT > DATE '2013-06-01') ff";
            List<OlapContext> olapContexts = OlapContextTestUtil.getOlapContexts("default", sql, true, true);
            for (OlapContext olapContext : olapContexts) {
                Assertions.assertTrue(olapContext.getSQLDigest().isRawQuery);
            }
        }

        {
            // can we make a rule to push agg into each subQuery of union?
            String sql = "select sum (DEAL_AMOUNT) from (select price * item_count as DEAL_AMOUNT  from (select * from TEST_KYLIN_FACT where CAL_DT < DATE '2012-06-01' union select * from TEST_KYLIN_FACT where CAL_DT > DATE '2013-06-01') group by price * item_count) ff";
            List<OlapContext> olapContexts = OlapContextTestUtil.getOlapContexts("default", sql, true, true);
            for (OlapContext olapContext : olapContexts) {
                Assertions.assertTrue(olapContext.getSQLDigest().isRawQuery);
            }
        }
    }

    // need to fix OlapAggProjectTransposeRule to use ComputedColumnRewriter to replace agg expression
    @Test
    void testMixModel() {
        {
            String sql = "select count(*), sum (price * item_count) as DEAL_AMOUNT from test_kylin_fact f \n"
                    + " left join test_order o on f.ORDER_ID = o.ORDER_ID\n"
                    + " left join test_account a on o.buyer_id = a.account_id\n"
                    + " left join test_country c on a.account_country = c.country\n"
                    + " left join edw.test_cal_dt dt on f.cal_dt = dt.cal_dt\n"//
                    + " left join TEST_CATEGORY_GROUPINGS x on x.LEAF_CATEG_ID = f.LEAF_CATEG_ID and x.SITE_ID = f.LSTG_SITE_ID\n"
                    + " left join ( \n"//
                    + "     select count(*), sum (price * item_count) ,country from test_kylin_fact f2\n"
                    + "     left join test_account a2 on f2.seller_id = a2.account_id\n"
                    + "     left join test_country c2 on account_country = country\n"
                    + "     group by concat(ACCOUNT_ID, NAME), country"
                    + " ) s on s.country = c.country  group by concat(a.ACCOUNT_ID, c.NAME)";
            String expected = "select count(*), sum (\"F\".\"DEAL_AMOUNT\") as DEAL_AMOUNT from test_kylin_fact f \n"
                    + " left join test_order o on f.ORDER_ID = o.ORDER_ID\n"
                    + " left join test_account a on o.buyer_id = a.account_id\n"
                    + " left join test_country c on a.account_country = c.country\n"
                    + " left join edw.test_cal_dt dt on f.cal_dt = dt.cal_dt\n"
                    + " left join TEST_CATEGORY_GROUPINGS x on x.LEAF_CATEG_ID = f.LEAF_CATEG_ID and x.SITE_ID = f.LSTG_SITE_ID\n"
                    + " left join ( \n"
                    + "     select count(*), sum (\"F2\".\"DEAL_AMOUNT\") ,country from test_kylin_fact f2\n"
                    + "     left join test_account a2 on f2.seller_id = a2.account_id\n"
                    + "     left join test_country c2 on account_country = country\n"
                    + "     group by \"F2\".\"LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME\", country"
                    + " ) s on s.country = c.country  group by \"F\".\"LEFTJOIN_BUYER_ID_AND_COUNTRY_NAME\"";
            String transformed = converter.transform(sql, "default", "DEFAULT");
            Assertions.assertEquals(expected, transformed);
        }

        {
            String sql = "select count(*), sum (price * item_count) from test_kylin_fact f"
                    + " left join test_order o on f.ORDER_ID = o.ORDER_ID\n"
                    + " left join test_account a on o.buyer_id = a.account_id\n"
                    + " left join test_country c on a.account_country = c.country\n"
                    + " left join edw.test_cal_dt dt on f.cal_dt = dt.cal_dt\n"//
                    + " left join TEST_CATEGORY_GROUPINGS x on x.LEAF_CATEG_ID = f.LEAF_CATEG_ID and x.SITE_ID = f.LSTG_SITE_ID\n"
                    + " inner join ( "//
                    + "     select count(*), sum (price * item_count), country from test_kylin_fact f2\n"
                    + "     left join test_account a2 on f2.seller_id = a2.account_id\n"
                    + "     left join test_country c2 on account_country = country\n"
                    + "     group by concat(ACCOUNT_ID, NAME), country\n" //
                    + " ) s on s.country = c.country group by a.ACCOUNT_ID";
            String expected = "select count(*), sum (\"F\".\"DEAL_AMOUNT\") from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID\n"
                    + " left join test_account a on o.buyer_id = a.account_id\n"
                    + " left join test_country c on a.account_country = c.country\n"
                    + " left join edw.test_cal_dt dt on f.cal_dt = dt.cal_dt\n"
                    + " left join TEST_CATEGORY_GROUPINGS x on x.LEAF_CATEG_ID = f.LEAF_CATEG_ID and x.SITE_ID = f.LSTG_SITE_ID\n"
                    + " inner join (      select count(*), sum (\"F2\".\"DEAL_AMOUNT\"), country from test_kylin_fact f2\n"
                    + "     left join test_account a2 on f2.seller_id = a2.account_id\n"
                    + "     left join test_country c2 on account_country = country\n"
                    + "     group by \"F2\".\"LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME\", country\n"
                    + " ) s on s.country = c.country group by a.ACCOUNT_ID";
            String transformed = converter.transform(sql, "default", "DEFAULT");
            Assertions.assertEquals(expected, transformed);
        }
    }

    // at present, we replace cc join key on SqlNode, can we replace this with RexNode?
    @Test
    void testJoinOnCC() {
        {
            String originSql = "select count(*) from TEST_KYLIN_FACT\n"
                    + "left join TEST_ORDER on TEST_KYLIN_FACT.ORDER_ID + 1 = TEST_ORDER.ORDER_ID + 1";
            String ccSql = "select count(*) from TEST_KYLIN_FACT\n"
                    + "left join TEST_ORDER on \"TEST_KYLIN_FACT\".\"ORDER_ID_PLUS_1\" = \"TEST_ORDER\".\"ID_PLUS_1\"";
            checkReplaceCcJoinKeys(converter, originSql, ccSql);
        }

        {
            String originSql = "select LSTG_FORMAT_NAME, LEAF_CATEG_ID from TEST_KYLIN_FACT\n"
                    + "left join TEST_ORDER on TEST_KYLIN_FACT.ORDER_ID + 1 = TEST_ORDER.ORDER_ID + 1\n"
                    + "group by LSTG_FORMAT_NAME, LEAF_CATEG_ID";
            String ccSql = "select LSTG_FORMAT_NAME, LEAF_CATEG_ID from TEST_KYLIN_FACT\n"
                    + "left join TEST_ORDER on \"TEST_KYLIN_FACT\".\"ORDER_ID_PLUS_1\" = \"TEST_ORDER\".\"ID_PLUS_1\"\n"
                    + "group by LSTG_FORMAT_NAME, LEAF_CATEG_ID";
            checkReplaceCcJoinKeys(converter, originSql, ccSql);
        }

        {
            String originSql = "select count(*) from TEST_KYLIN_FACT\n"
                    + "left join TEST_ORDER on TEST_KYLIN_FACT.ORDER_ID + 1 = TEST_ORDER.ORDER_ID + 1\n"
                    + "left join TEST_ACCOUNT on (CASE WHEN TRUE THEN TEST_ACCOUNT.ACCOUNT_ID ELSE 0 END) = (CASE WHEN TRUE THEN TEST_ORDER.BUYER_ID ELSE 0 END)\n";
            String ccSql = "select count(*) from TEST_KYLIN_FACT\n"
                    + "left join TEST_ORDER on \"TEST_KYLIN_FACT\".\"ORDER_ID_PLUS_1\" = \"TEST_ORDER\".\"ID_PLUS_1\"\n"
                    + "left join TEST_ACCOUNT on (\"TEST_ACCOUNT\".\"BUYER_ACCOUNT_CASE_WHEN\") = (\"TEST_ORDER\".\"ACCOUNT_CASE_WHEN\")\n";
            checkReplaceCcJoinKeys(converter, originSql, ccSql);
        }

        {
            String originSql = "select count(*) from TEST_KYLIN_FACT\n"
                    + "left join TEST_ORDER on TEST_KYLIN_FACT.ORDER_ID + 1 = TEST_ORDER.ORDER_ID + 1\n"
                    + "left join TEST_ACCOUNT on (CASE WHEN TRUE THEN TEST_ACCOUNT.ACCOUNT_ID ELSE 0 END) = (CASE WHEN TRUE THEN TEST_ORDER.BUYER_ID ELSE 0 END)\n"
                    + "left join TEST_COUNTRY on UPPER(TEST_ACCOUNT.ACCOUNT_COUNTRY) = TEST_COUNTRY.COUNTRY";
            String ccSql = "select count(*) from TEST_KYLIN_FACT\n"
                    + "left join TEST_ORDER on \"TEST_KYLIN_FACT\".\"ORDER_ID_PLUS_1\" = \"TEST_ORDER\".\"ID_PLUS_1\"\n"
                    + "left join TEST_ACCOUNT on (\"TEST_ACCOUNT\".\"BUYER_ACCOUNT_CASE_WHEN\") = (\"TEST_ORDER\".\"ACCOUNT_CASE_WHEN\")\n"
                    + "left join TEST_COUNTRY on \"TEST_ACCOUNT\".\"COUNTRY_UPPER\" = TEST_COUNTRY.COUNTRY";
            checkReplaceCcJoinKeys(converter, originSql, ccSql);
        }
    }

    private void checkReplaceCcJoinKeys(ConvertToComputedColumn converter, String originSql, String ccSql) {
        String transform = converter.transform(originSql, "default", "DEFAULT");
        Assertions.assertEquals(ccSql, transform);
    }

    @Test
    void testNoFrom() throws SqlParseException {
        String sql = "select sum(price * item_count),(SELECT 1 as VERSION) from test_kylin_fact";
        List<OlapContext> olapContexts = OlapContextTestUtil.getOlapContexts("default", sql, true, true);
        OlapContext olapContext = olapContexts.get(0);
        List<FunctionDesc> aggList = olapContext.getAggregations().stream()
                .filter(agg -> agg.getExpression().equals("SUM")).collect(Collectors.toList());
        Assertions.assertEquals(1, aggList.size());
        Assertions.assertEquals("[DEFAULT.TEST_KYLIN_FACT.DEAL_AMOUNT]", aggList.get(0).getParameters().toString());
    }

    @Test
    void testFromValues() throws SqlParseException {
        String sql = "select sum(price * item_count),(SELECT 1 FROM (VALUES(1))) from test_kylin_fact";
        List<OlapContext> olapContexts = OlapContextTestUtil.getOlapContexts("default", sql, true, true);
        OlapContext olapContext = olapContexts.get(0);
        List<FunctionDesc> aggList = olapContext.getAggregations().stream()
                .filter(agg -> agg.getExpression().equals("SUM")).collect(Collectors.toList());
        Assertions.assertEquals(1, aggList.size());
        Assertions.assertEquals("[DEFAULT.TEST_KYLIN_FACT.DEAL_AMOUNT]", aggList.get(0).getParameters().toString());
    }

    @Test
    void testNestedCC() throws SqlParseException {

        {
            String sql = "select count(*), sum ((round((F.PRICE + 11) * 12, 0)) * F.ITEM_COUNT) from test_kylin_fact F";
            List<OlapContext> olapContexts = OlapContextTestUtil.getOlapContexts("default", sql, true, true);
            OlapContext olapContext = olapContexts.get(0);
            List<FunctionDesc> aggList = olapContext.getAggregations().stream()
                    .filter(agg -> agg.getExpression().equals("SUM")).collect(Collectors.toList());
            Assertions.assertEquals(1, aggList.size());
            Assertions.assertEquals("[DEFAULT.TEST_KYLIN_FACT.NEST4]", aggList.get(0).getParameters().toString());
        }

        {
            String sql = "select count(*), sum ((round(F.NEST1 * 12, 0)) * F.ITEM_COUNT) from test_kylin_fact F";
            List<OlapContext> olapContexts = OlapContextTestUtil.getOlapContexts("default", sql, true, true);
            OlapContext olapContext = olapContexts.get(0);
            List<FunctionDesc> aggList = olapContext.getAggregations().stream()
                    .filter(agg -> agg.getExpression().equals("SUM")).collect(Collectors.toList());
            Assertions.assertEquals(1, aggList.size());
            Assertions.assertEquals("[DEFAULT.TEST_KYLIN_FACT.NEST4]", aggList.get(0).getParameters().toString());
        }

        {
            String sql = "select count(*), sum ((round(F.NEST2, 0)) * F.ITEM_COUNT) from test_kylin_fact F";
            List<OlapContext> olapContexts = OlapContextTestUtil.getOlapContexts("default", sql, true, true);
            OlapContext olapContext = olapContexts.get(0);
            List<FunctionDesc> aggList = olapContext.getAggregations().stream()
                    .filter(agg -> agg.getExpression().equals("SUM")).collect(Collectors.toList());
            Assertions.assertEquals(1, aggList.size());
            Assertions.assertEquals("[DEFAULT.TEST_KYLIN_FACT.NEST4]", aggList.get(0).getParameters().toString());
        }

        {
            String sql = "select count(*), sum (F.NEST3 * F.ITEM_COUNT) from test_kylin_fact F";
            List<OlapContext> olapContexts = OlapContextTestUtil.getOlapContexts("default", sql, true, true);
            OlapContext olapContext = olapContexts.get(0);
            List<FunctionDesc> aggList = olapContext.getAggregations().stream()
                    .filter(agg -> agg.getExpression().equals("SUM")).collect(Collectors.toList());
            Assertions.assertEquals(1, aggList.size());
            Assertions.assertEquals("[DEFAULT.TEST_KYLIN_FACT.NEST4]", aggList.get(0).getParameters().toString());
        }
    }

    @Test
    void testCcConvertedOnMultiModel() throws SqlParseException {
        String sql = "select count(*), sum (price * item_count) from test_kylin_fact f";
        List<OlapContext> olapContexts = OlapContextTestUtil.getOlapContexts("default", sql, true, true);
        OlapContext olapContext = olapContexts.get(0);
        List<FunctionDesc> aggList = olapContext.getAggregations().stream()
                .filter(agg -> agg.getExpression().equals("SUM")).collect(Collectors.toList());
        Assertions.assertEquals(1, aggList.size());
        List<ParameterDesc> parameters = aggList.get(0).getParameters();
        String str = parameters.stream().map(param -> param.getColRef().getCanonicalName())
                .collect(Collectors.joining(",", "[", "]"));
        Assertions.assertEquals("[DEFAULT.TEST_KYLIN_FACT.DEAL_AMOUNT]", str);
    }

    @Test
    void testDateFamily() throws SqlParseException {
        NDataModel model = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "tdvt")
                .getDataModelDesc("e6a4c3bb-6391-4995-9e33-cc24ac5a155b");
        String sql = "select count( year(date0)), max(extract(year from date1)),\n"
                + "       count( month(date0)), max(extract(month from date1)),\n"
                + "       count( quarter(date0)), max(extract(quarter from date1)),\n"
                + "       count( hour(date0)), max(extract(hour from date1)),\n"
                + "       count( minute(date0)), max(extract(minute from date1)),\n"
                + "       count( second(date0)), max(extract(second from date1)),\n"
                + "       count(dayofmonth(date0)), max(extract(day from date1)),\n"
                + "        count(dayofyear(date0)), max(extract(doy from date0)),\n"
                + "        count(dayofmonth(date0)), max(extract(day from date1)),\n"
                + "        count(dayofweek(date0)), max(extract(dow from date0))\n" //
                + "from tdvt.calcs as calcs";
        checkReplaceAggDateFunctionsCc(model, sql);
    }

    @Test
    void testBasicTimestampAddAndDiff() throws SqlParseException {
        NDataModel model = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "tdvt")
                .getDataModelDesc("e6a4c3bb-6391-4995-9e33-cc24ac5a155b");
        String sql = "select sum(timestampdiff(second, time0, time1) ) as c1,\n" //
                + "count(distinct timestampadd(minute, 1, time1)) as c2,\n" //
                + "max(timestampdiff(hour, time1, time0)) as c3,\n" //
                + "min(timestampadd(second, 1, time1)) as c4,\n" //
                + "avg(timestampdiff(hour, time0, time1)) as c5,\n" //
                + "count(timestampadd(second, 1+2, time0)),\n" //
                + "max(timestampadd(second, 1, timestamp '1970-01-01 10:01:01')),\n" //
                + "count(timestampadd(minute, int0+1, time1)),\n" //
                + "sum(timestampdiff(second, time0, cast(datetime1 as timestamp)))\n" //
                + "from tdvt.calcs";
        checkReplaceAggDateFunctionsCc(model, sql);
    }

    private void checkReplaceAggDateFunctionsCc(NDataModel model, String sql) throws SqlParseException {
        List<OlapContext> olapContexts = OlapContextTestUtil.getOlapContexts("tdvt", sql, true, true);
        OlapContext olapContext = olapContexts.get(0);
        OlapContextTestUtil.rewriteComputedColumns(model, olapContext);
        for (FunctionDesc aggregation : olapContext.getAggregations()) {
            String paramList = aggregation.getParameters().stream().map(param -> param.getColRef().getCanonicalName())
                    .collect(Collectors.joining(","));
            Assertions.assertTrue(paramList.startsWith("TDVT.CALCS.CC_AUTO_")); // assert the cc has been replaced
        }
    }

    private void checkReplaceGroupByDateFunctionsCc(NDataModel model, String sql) throws SqlParseException {
        List<OlapContext> olapContexts = OlapContextTestUtil.getOlapContexts("tdvt", sql, true, true);
        OlapContext olapContext = olapContexts.get(0);
        OlapContextTestUtil.rewriteComputedColumns(model, olapContext);
        for (TblColRef groupByColumn : olapContext.getGroupByColumns()) {
            Assertions.assertTrue(groupByColumn.getColumnDesc().getName().startsWith("CC_AUTO_"));
        }
    }

    @Test
    void testMoreTimestampAddAndDiff() throws SqlParseException {
        NDataModel model = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "tdvt")
                .getDataModelDesc("e6a4c3bb-6391-4995-9e33-cc24ac5a155b");
        {
            String sql = "select sum((int1-int2)/(int1+int2)) as c1,\n"
                    + "sum((int1-int2)/timestampdiff(second, time0, time1) ) as c2,\n"
                    + "sum(timestampdiff(second, time0, time1)/timestampdiff(second, timestampadd(year,1, time1), time1)) as c3\n"
                    + "from tdvt.calcs";
            checkReplaceAggDateFunctionsCc(model, sql);
        }

        // case 2
        {
            String sql = "select sum(case when int0 > 0 then timestampdiff(day, time0, time1) end) as ab\n"
                    + "  from tdvt.calcs as calcs";
            checkReplaceAggDateFunctionsCc(model, sql);
        }

        // case 3
        {
            String sql = "select sum(case when time0 <> time1 then (int2-int1)/timestampdiff(second, time0, time1) * 60\n"
                    + "    else (int2 - int1)/ timestampdiff(second, time1, datetime0)*60 end)\n" //
                    + "from tdvt.calcs";
            checkReplaceAggDateFunctionsCc(model, sql);
        }

        // case 4
        {
            String sql = "select case when int0 > 0 then timestampdiff(second, time0, time1) else 0 end\n"
                    + "from tdvt.calcs\n"
                    + "group by case when int0 > 0 then timestampdiff(second, time0, time1) else 0 end\n"
                    + "order by case when int0 > 0 then timestampdiff(second, time0, time1) else 0 end";
            checkReplaceGroupByDateFunctionsCc(model, sql);
        }

        // case 5
        {
            String sql = "select case when int0 > 100 then timestampdiff(second, time0, time1)\n"
                    + "                when int0 > 50 then timestampdiff(minute, time0, time1)\n"
                    + "                when int0 > 0 then timestampdiff(hour, time0, time1) else null end\n"
                    + "from tdvt.calcs group by case when int0 > 100 then timestampdiff(second, time0, time1)\n"
                    + "                when int0 > 50 then timestampdiff(minute, time0, time1)\n"
                    + "                when int0 > 0 then timestampdiff(hour, time0, time1) else null end";
            checkReplaceGroupByDateFunctionsCc(model, sql);
        }

        // case 6
        {
            String sql = "select case when int0 > 10 then sum(timestampdiff(second, time0, time1)) else sum(timestampdiff(minute, time0, time1)) end\n"
                    + "from tdvt.calcs group by int0";
            checkReplaceAggDateFunctionsCc(model, sql);
        }

        // case 7
        {
            String sql = "with ca as(select time0 as t0, time1, datetime0 from tdvt.calcs)\n"
                    + "select sum(timestampdiff(minute, ca.time1, ca.t0)), ca.datetime0\n"
                    + "from ca group by ca.datetime0";
            checkReplaceAggDateFunctionsCc(model, sql);
        }

        // case 8
        {
            String sql = "select sum(tmp.ab) from (\n"
                    + "  select sum(timestampdiff(second, time0, time1)/timestampdiff(second, timestampadd(year,1, time1), time1)) as ab\n"
                    + "  from tdvt.calcs as calcs) tmp";
            checkReplaceAggDateFunctionsCc(model, sql);
        }

        // case 9
        {
            String sql = "select sum(timestampdiff(minute, time1, time0)), datetime0, time1, time0\n"
                    + "from tdvt.calcs group by datetime0, time1, time0\n" //
                    + "union\n" //
                    + "select max(timestampdiff(minute, time1, time0)), datetime0, time1, time0\n"
                    + "from tdvt.calcs group by datetime0, time1, time0";
            List<OlapContext> olapContexts = OlapContextTestUtil.getOlapContexts("tdvt", sql, true, true);
            for (OlapContext olapContext : olapContexts) {
                OlapContextTestUtil.rewriteComputedColumns(model, olapContext);
                for (FunctionDesc aggregation : olapContext.getAggregations()) {
                    String paramList = aggregation.getParameters().stream()
                            .map(param -> param.getColRef().getCanonicalName()).collect(Collectors.joining(","));
                    // assert the cc has been replaced
                    Assertions.assertTrue(paramList.startsWith("TDVT.CALCS.CC_AUTO_"));
                }
            }
        }

        // case 10
        {
            String sql = "select max(timestampdiff(second, time0, cast(datetime1 as timestamp)))\n"
                    + " - min(timestampdiff(second, time0, cast(datetime1 as timestamp)))\n" //
                    + "from tdvt.calcs";
            checkReplaceAggDateFunctionsCc(model, sql);
        }

        // case 11: window function
        {
            String sql = "select num1, max(TIMESTAMPADD(SQL_TSI_DAY, 1, TIMESTAMP '1970-01-01 10:01:01')) MAXTIME,\n"
                    + "      max(TIMESTAMPADD(SQL_TSI_DAY, 1, TIMESTAMP '1970-01-01 10:01:01')) over() MAXTIME1\n"
                    + "from tdvt.calcs where num1 > 0\n" //
                    + "group by num1\n" //
                    + "order by TIMESTAMPADD(SQL_TSI_DAY,1, TIMESTAMP '1970-01-01 10:01:01')";
            checkReplaceAggDateFunctionsCc(model, sql);
        }

        // case 11: window function
        {
            String sql = "select num1, max(TIMESTAMPADD(SQL_TSI_DAY, 1, time0)) over() MAXTIME1\n" //
                    + "from tdvt.calcs\n" //
                    + "where num1 > 0\n" //
                    + "group by num1, time0\n" //
                    + "order by TIMESTAMPADD(SQL_TSI_DAY,1, TIMESTAMP'1970-01-01 10:01:01')";
            checkReplaceAggDateFunctionsCc(model, sql);
        }
    }

    @Test
    void testExplicitCcNameToInnerName() throws SqlParseException {
        {
            // case 1. explicit query name in innermost sub-query
            String sql = "select max(CC_AUTO_17) - min(CC_AUTO_17), max(CC_AUTO_17) - min(\"CC_AUTO_17\")\n"
                    + "from (select CC_AUTO_17 from tdvt.calcs group by CC_AUTO_17)";
            List<OlapContext> olapContexts = OlapContextTestUtil.getOlapContexts("tdvt", sql);
            Set<TblColRef> groupByColumns = olapContexts.get(0).getGroupByColumns();
            Assertions.assertEquals(1, groupByColumns.size());
            for (TblColRef groupByColumn : groupByColumns) {
                Assertions.assertEquals("CC_AUTO_17", groupByColumn.getColumnDesc().getName());
            }
        }

        // case 2. explicit query name with AS ALIAS in innermost sub-query
        {
            String sql = "select max(CALCS.CC_AUTO_17) - min(CALCS.CC_AUTO_17)\n"
                    + "from (select CC_AUTO_17 as CC_AUTO_17  from tdvt.calcs group by CC_AUTO_17) as CALCS";
            List<OlapContext> olapContexts = OlapContextTestUtil.getOlapContexts("tdvt", sql);
            Set<TblColRef> groupByColumns = olapContexts.get(0).getGroupByColumns();
            Assertions.assertEquals(1, groupByColumns.size());
            for (TblColRef groupByColumn : groupByColumns) {
                Assertions.assertEquals("CC_AUTO_17", groupByColumn.getColumnDesc().getName());
            }
        }

        // case 3. explicit query CC with double quote & lower case table alias
        {
            String sql = "select max(CC_AUTO_17) - min(\"CC_AUTO_17\")\n"
                    + "from (select calcs.\"CC_AUTO_17\" as CC_AUTO_17  from tdvt.calcs group by CC_AUTO_17)";
            List<OlapContext> olapContexts = OlapContextTestUtil.getOlapContexts("tdvt", sql);
            Set<TblColRef> groupByColumns = olapContexts.get(0).getGroupByColumns();
            Assertions.assertEquals(1, groupByColumns.size());
            for (TblColRef groupByColumn : groupByColumns) {
                Assertions.assertEquals("CC_AUTO_17", groupByColumn.getColumnDesc().getName());
            }
        }
    }

    @Test
    void testReplaceTableIndexCc() {
        /*NDataModel model = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "cc_test")
                .getDataModelDesc("0d146f1a-bdd3-4548-87ac-21c2c6f9a0da")*/
        {
            String sql = "select EXTRACT(minute FROM lineorder.lo_orderdate) from ssb.lineorder inner join ssb.customer on lineorder.lo_custkey = customer.c_custkey";
            String expected = "select \"LINEORDER\".\"CC_EXTRACT\" from ssb.lineorder inner join ssb.customer on lineorder.lo_custkey = customer.c_custkey";
            String transformed = converter.transform(sql, "cc_test", "DEFAULT");
            Assertions.assertEquals(expected, transformed);
        }

        {
            String sql = "select {fn convert(lineorder.lo_orderkey, double)} from ssb.lineorder inner join ssb.customer on lineorder.lo_custkey = customer.c_custkey";
            String expected = "select \"LINEORDER\".\"CC_CAST_LO_ORDERKEY\" from ssb.lineorder inner join ssb.customer on lineorder.lo_custkey = customer.c_custkey";
            String transformed = converter.transform(sql, "cc_test", "DEFAULT");
            Assertions.assertEquals(expected, transformed);
        }
    }
}

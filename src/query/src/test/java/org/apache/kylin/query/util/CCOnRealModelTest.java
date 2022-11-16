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

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * test against real models
 */
public class CCOnRealModelTest extends NLocalFileMetadataTestCase {
    private ConvertToComputedColumn converter;

    @Before
    public void setup() throws Exception {
        this.createTestMetadata("../core-metadata/src/test/resources/ut_meta/ccjointest");
        converter = new ConvertToComputedColumn();
    }

    @After
    public void after() throws Exception {
        super.cleanupTestMetadata();
    }

    @Test
    public void testConvertSingleTableCC() {
        {
            String originSql = "select count(*), sum (price * item_count) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID"
                    + " left join test_account a on o.buyer_id = a.account_id group by ACCOUNT_COUNTRY";
            String ccSql = "select count(*), sum (F.DEAL_AMOUNT) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID left join test_account a on o.buyer_id = a.account_id group by ACCOUNT_COUNTRY";

            check(converter, originSql, ccSql);

        }

        {
            String originSql = "select count(*), sum (price * item_count) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID"
                    + " left join test_account a on o.buyer_id = a.account_id group by ACCOUNT_COUNTRY order by sum(price * item_count)";
            String ccSql = "select count(*), sum (F.DEAL_AMOUNT) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID left join test_account a on o.buyer_id = a.account_id group by ACCOUNT_COUNTRY order by sum(F.DEAL_AMOUNT)";

            check(converter, originSql, ccSql);

        }

        {
            String originSql = "select count(*), sum (price * item_count) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID"
                    + " left join test_account a on o.buyer_id = a.account_id group by ACCOUNT_COUNTRY" + " union"
                    + " select count(*), sum (price * item_count) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID"
                    + " left join test_account a on o.buyer_id = a.account_id group by ACCOUNT_COUNTRY";
            String ccSql = "select count(*), sum (F.DEAL_AMOUNT) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID left join test_account a on o.buyer_id = a.account_id group by ACCOUNT_COUNTRY union select count(*), sum (F.DEAL_AMOUNT) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID left join test_account a on o.buyer_id = a.account_id group by ACCOUNT_COUNTRY";
            check(converter, originSql, ccSql);

        }

        {
            String originSql = "select count(*), sum (price * item_count) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID"
                    + " left join test_account a on o.buyer_id = a.account_id group by substr(ACCOUNT_COUNTRY,0,1)";
            String ccSql = "select count(*), sum (F.DEAL_AMOUNT) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID"
                    + " left join test_account a on o.buyer_id = a.account_id group by F.LEFTJOIN_BUYER_COUNTRY_ABBR";
            check(converter, originSql, ccSql);

        }

    }

    @Test
    public void testConvertCrossTableCC() {
        {
            //buyer
            String originSql = "select count(*), sum (price * item_count) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID"
                    + " left join test_account a on o.buyer_id = a.account_id  left join test_country c on a.account_country = c.country group by concat(a.ACCOUNT_ID, c.NAME)";
            String ccSql = "select count(*), sum (F.DEAL_AMOUNT) from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID left join test_account a on o.buyer_id = a.account_id  left join test_country c on a.account_country = c.country group by F.LEFTJOIN_BUYER_ID_AND_COUNTRY_NAME";
            check(converter, originSql, ccSql);

        }

        {
            //seller
            String originSql = "select count(*), sum (price * item_count) from test_kylin_fact f"
                    + " left join test_account a on f.seller_id = a.account_id  left join test_country c on a.account_country = c.country group by concat(a.ACCOUNT_ID, c.NAME)";
            String ccSql = "select count(*), sum (F.DEAL_AMOUNT) from test_kylin_fact f left join test_account a on f.seller_id = a.account_id  left join test_country c on a.account_country = c.country group by F.LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME";
            check(converter, originSql, ccSql);

        }

        {
            //seller, but swap join condition
            String originSql = "select count(*), sum (price * item_count) from test_kylin_fact f"
                    + " left join test_account a on f.seller_id = a.account_id  left join test_country c on country = account_country group by concat(a.ACCOUNT_ID, c.NAME)";
            String ccSql = "select count(*), sum (F.DEAL_AMOUNT) from test_kylin_fact f left join test_account a on f.seller_id = a.account_id  left join test_country c on country = account_country group by F.LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME";
            check(converter, originSql, ccSql);

        }
    }

    // ignored for KAP#16258
    @Ignore
    @Test
    public void testSubquery() {
        {
            String originSql = "select count(*), sum (price * item_count) as DEAL_AMOUNT from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID"
                    + " left join test_account a on o.buyer_id = a.account_id  left join test_country c on a.account_country = c.country"
                    + " left join edw.test_cal_dt dt on f.cal_dt = dt.cal_dt"//
                    + " left join TEST_CATEGORY_GROUPINGS x on x.LEAF_CATEG_ID = f.LEAF_CATEG_ID and x.SITE_ID = f.LSTG_SITE_ID"
                    + " left join " //
                    + "( "//
                    + "     select count(*), sum (price * item_count) ,country from test_kylin_fact f2"
                    + "     left join test_account a2 on f2.seller_id = a2.account_id  left join test_country c2 on account_country = country group by concat(ACCOUNT_ID, NAME), country"
                    + ") s on s.country = c.country  group by concat(a.ACCOUNT_ID, c.NAME)";
            String ccSql = "select count(*), sum (F.DEAL_AMOUNT) as DEAL_AMOUNT from test_kylin_fact f left join test_order o on f.ORDER_ID = o.ORDER_ID left join test_account a on o.buyer_id = a.account_id  left join test_country c on a.account_country = c.country left join edw.test_cal_dt dt on f.cal_dt = dt.cal_dt left join TEST_CATEGORY_GROUPINGS x on x.LEAF_CATEG_ID = f.LEAF_CATEG_ID and x.SITE_ID = f.LSTG_SITE_ID left join (      select count(*), sum (F2.DEAL_AMOUNT) ,country from test_kylin_fact f2     left join test_account a2 on f2.seller_id = a2.account_id  left join test_country c2 on account_country = country group by F2.LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME, country) s on s.country = c.country  group by F.LEFTJOIN_BUYER_ID_AND_COUNTRY_NAME";

            check(converter, originSql, ccSql);

        }

        {
            String originSql = "select count(*), DEAL_AMOUNT from (select count(*), sum (price * item_count) as DEAL_AMOUNT from test_kylin_fact)";
            String ccSql = "select count(*), DEAL_AMOUNT from (select count(*), sum (TEST_KYLIN_FACT.DEAL_AMOUNT) as DEAL_AMOUNT from test_kylin_fact)";

            check(converter, originSql, ccSql);
        }

        {
            String originSql = "select count(*) from (select count(*), sum (price * item_count) from test_kylin_fact) f";
            String ccSql = "select count(*) from (select count(*), sum (TEST_KYLIN_FACT.DEAL_AMOUNT) from test_kylin_fact) f";

            check(converter, originSql, ccSql);
        }

        {
            String originSql = "select sum (price * item_count) from (select * from test_kylin_fact)";
            String ccSql = "select sum (TEST_KYLIN_FACT.DEAL_AMOUNT) from (select * from test_kylin_fact)";

            check(converter, originSql, ccSql);
        }

        {
            String originSql = "select sum (price * item_count) from (select * from TEST_KYLIN_FACT where CAL_DT < DATE '2012-06-01' union select * from TEST_KYLIN_FACT where CAL_DT > DATE '2013-06-01') ff";
            String ccSql = "select sum (TEST_KYLIN_FACT.DEAL_AMOUNT) from (select * from TEST_KYLIN_FACT where CAL_DT < DATE '2012-06-01' union select * from TEST_KYLIN_FACT where CAL_DT > DATE '2013-06-01') ff";

            check(converter, originSql, ccSql);
        }

        {
            String originSql = "select sum (DEAL_AMOUNT) from (select price * item_count as DEAL_AMOUNT  from (select * from TEST_KYLIN_FACT where CAL_DT < DATE '2012-06-01' union select * from TEST_KYLIN_FACT where CAL_DT > DATE '2013-06-01') group by price * item_count) ff";
            String ccSql = "select sum (DEAL_AMOUNT) from (select TEST_KYLIN_FACT.DEAL_AMOUNT as DEAL_AMOUNT  from (select * from TEST_KYLIN_FACT where CAL_DT < DATE '2012-06-01' union select * from TEST_KYLIN_FACT where CAL_DT > DATE '2013-06-01') group by TEST_KYLIN_FACT.DEAL_AMOUNT) ff";
            check(converter, originSql, ccSql);
        }

    }

    @Test
    public void testMixModel() {
        String originSql = "select count(*), sum (price * item_count) from test_kylin_fact f"
                + " left join test_order o on f.ORDER_ID = o.ORDER_ID"
                + " left join test_account a on o.buyer_id = a.account_id "
                + " left join test_country c on a.account_country = c.country"
                + " left join edw.test_cal_dt dt on f.cal_dt = dt.cal_dt"//
                + " left join TEST_CATEGORY_GROUPINGS x on x.LEAF_CATEG_ID = f.LEAF_CATEG_ID and x.SITE_ID = f.LSTG_SITE_ID"
                + " inner join ( "//
                + "     select count(*), sum (price * item_count), country from test_kylin_fact f2"
                + "     left join test_account a2 on f2.seller_id = a2.account_id"
                + "     left join test_country c2 on account_country = country"
                + "     group by concat(ACCOUNT_ID, NAME), country" + " ) s on s.country = c.country"
                + " group by a.ACCOUNT_ID";
        String ccSql = "select count(*), sum (F.DEAL_AMOUNT) from test_kylin_fact f"
                + " left join test_order o on f.ORDER_ID = o.ORDER_ID"
                + " left join test_account a on o.buyer_id = a.account_id "
                + " left join test_country c on a.account_country = c.country"
                + " left join edw.test_cal_dt dt on f.cal_dt = dt.cal_dt"
                + " left join TEST_CATEGORY_GROUPINGS x on x.LEAF_CATEG_ID = f.LEAF_CATEG_ID and x.SITE_ID = f.LSTG_SITE_ID"
                + " inner join ( " + "     select count(*), sum (F2.DEAL_AMOUNT), country from test_kylin_fact f2"
                + "     left join test_account a2 on f2.seller_id = a2.account_id"
                + "     left join test_country c2 on account_country = country"
                + "     group by F2.LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME, country" + " ) s on s.country = c.country"
                + " group by a.ACCOUNT_ID";
        check(converter, originSql, ccSql);

    }

    @Test
    @Ignore("Not support CC on Join condition")
    public void testJoinOnCC() {
        {
            String originSql = "select count(*) from TEST_KYLIN_FACT\n"
                    + "left join TEST_ORDER on TEST_KYLIN_FACT.ORDER_ID + 1 = TEST_ORDER.ORDER_ID + 1";
            String ccSql = "select count(*) from TEST_KYLIN_FACT\n"
                    + "left join TEST_ORDER on TEST_KYLIN_FACT.ORDER_ID_PLUS_1 = TEST_ORDER.ID_PLUS_1";
            check(converter, originSql, ccSql);
        }

        {
            String originSql = "select count(*) from TEST_KYLIN_FACT\n"
                    + "left join TEST_ORDER on TEST_KYLIN_FACT.ORDER_ID + 1 = TEST_ORDER.ORDER_ID + 1\n"
                    + "left join TEST_ACCOUNT on (CASE WHEN TRUE THEN TEST_ACCOUNT.ACCOUNT_ID ELSE 0 END) = (CASE WHEN TRUE THEN TEST_ORDER.BUYER_ID ELSE 0 END)\n";
            String ccSql = "select count(*) from TEST_KYLIN_FACT\n"
                    + "left join TEST_ORDER on TEST_KYLIN_FACT.ORDER_ID_PLUS_1 = TEST_ORDER.ID_PLUS_1\n"
                    + "left join TEST_ACCOUNT on (TEST_ACCOUNT.BUYER_ACCOUNT_CASE_WHEN) = (TEST_ORDER.ACCOUNT_CASE_WHEN)\n";
            check(converter, originSql, ccSql);
        }

        {
            String originSql = "select count(*) from TEST_KYLIN_FACT\n"
                    + "left join TEST_ORDER on TEST_KYLIN_FACT.ORDER_ID + 1 = TEST_ORDER.ORDER_ID + 1\n"
                    + "left join TEST_ACCOUNT on (CASE WHEN TRUE THEN TEST_ACCOUNT.ACCOUNT_ID ELSE 0 END) = (CASE WHEN TRUE THEN TEST_ORDER.BUYER_ID ELSE 0 END)\n"
                    + "left join TEST_COUNTRY on UPPER(TEST_ACCOUNT.ACCOUNT_COUNTRY) = TEST_COUNTRY.COUNTRY";
            String ccSql = "select count(*) from TEST_KYLIN_FACT\n"
                    + "left join TEST_ORDER on TEST_KYLIN_FACT.ORDER_ID_PLUS_1 = TEST_ORDER.ID_PLUS_1\n"
                    + "left join TEST_ACCOUNT on (TEST_ACCOUNT.BUYER_ACCOUNT_CASE_WHEN) = (TEST_ORDER.ACCOUNT_CASE_WHEN)\n"
                    + "left join TEST_COUNTRY on TEST_ACCOUNT.COUNTRY_UPPER = TEST_COUNTRY.COUNTRY";
            check(converter, originSql, ccSql);
        }
    }

    @Test
    public void testNoFrom() throws Exception {
        String originSql = "select sum(price * item_count),(SELECT 1 as VERSION) from test_kylin_fact";
        String ccSql = "select sum(TEST_KYLIN_FACT.DEAL_AMOUNT),(SELECT 1 as VERSION) from test_kylin_fact";

        check(converter, originSql, ccSql);
    }

    @Test
    public void testFromValues() {
        String originSql = "select sum(price * item_count),(SELECT 1 FROM (VALUES(1))) from test_kylin_fact";
        String ccSql = "select sum(TEST_KYLIN_FACT.DEAL_AMOUNT),(SELECT 1 FROM (VALUES(1))) from test_kylin_fact";

        check(converter, originSql, ccSql);
    }

    @Test
    public void testNestedCC() {
        String ccSql = "select count(*), sum (F.NEST4) from test_kylin_fact F";

        {
            String originSql = "select count(*), sum ((round((F.PRICE + 11) * 12, 0)) * F.ITEM_COUNT) from test_kylin_fact F";
            check(converter, originSql, ccSql);
        }

        {
            String originSql = "select count(*), sum ((round(F.NEST1 * 12, 0)) * F.ITEM_COUNT) from test_kylin_fact F";
            check(converter, originSql, ccSql);
        }

        {
            String originSql = "select count(*), sum ((round(F.NEST2, 0)) * F.ITEM_COUNT) from test_kylin_fact F";
            check(converter, originSql, ccSql);
        }

        {
            String originSql = "select count(*), sum (F.NEST3 * F.ITEM_COUNT) from test_kylin_fact F";
            check(converter, originSql, ccSql);
        }
    }

    @Test
    public void testCcConvertedOnMultiModel() {
        String originSql = "select count(*), sum (price * item_count) from test_kylin_fact f";
        String ccSql = "select count(*), sum (F.DEAL_AMOUNT) from test_kylin_fact f";
        check(converter, originSql, ccSql);
    }

    @Test
    public void testDateFamily() {
        String originSql = "select count( year(date0)), max(extract(year from date1)),\n"
                + "       count( month(date0)), max(extract(month from date1)),\n"
                + "       count( quarter(date0)), max(extract(quarter from date1)),\n"
                + "       count( hour(date0)), max(extract(hour from date1)),\n"
                + "       count( minute(date0)), max(extract(minute from date1)),\n"
                + "       count( second(date0)), max(extract(second from date1)),\n"
                + "       count(dayofmonth(date0)), max(extract(day from date1)),\n"
                + "        count(dayofyear(date0)), max(extract(doy from date1)),\n"
                + "        count(dayofmonth(date0)), max(extract(day from date1)),\n"
                + "        count(dayofweek(date0)), max(extract(dow from date1))\n" //
                + "from tdvt.calcs as calcs";
        String ccSql = "select count( CALCS.CC_AUTO_2), max(extract(year from date1)),\n"
                + "       count( CALCS.CC_AUTO_37), max(extract(month from date1)),\n"
                + "       count( CALCS.CC_AUTO_33), max(extract(quarter from date1)),\n"
                + "       count( CALCS.CC_AUTO_27), max(extract(hour from date1)),\n"
                + "       count( CALCS.CC_AUTO_32), max(extract(minute from date1)),\n"
                + "       count( CALCS.CC_AUTO_20), max(extract(second from date1)),\n"
                + "       count(CALCS.CC_AUTO_8), max(extract(day from date1)),\n"
                + "        count(CALCS.CC_AUTO_14), max(extract(doy from date1)),\n"
                + "        count(CALCS.CC_AUTO_8), max(extract(day from date1)),\n"
                + "        count(CALCS.CC_AUTO_21), max(extract(dow from date1))\n" //
                + "from tdvt.calcs as calcs";
        check(converter, originSql, ccSql, "tdvt");
    }

    @Test
    public void testBasicTimestampAddAndDiff() {
        String originSql = "select sum(timestampdiff(second, time0, time1) ) as c1,\n" //
                + "count(distinct timestampadd(minute, 1, time1)) as c2,\n" //
                + "max(timestampdiff(hour, time1, time0)) as c3,\n" //
                + "min(timestampadd(second, 1, time1)) as c4,\n" //
                + "avg(timestampdiff(hour, time0, time1)) as c5,\n" //
                + "count(timestampadd(second, 1+2, time0)),\n" //
                + "max(timestampadd(second, 1, timestamp '1970-01-01 10:01:01')),\n" //
                + "count(timestampadd(minute, int0+1, time1)),\n" //
                + "sum(timestampdiff(second, time0, cast(datetime1 as timestamp)))\n" //
                + "from tdvt.calcs";
        String ccSql = "select sum(CALCS.CC_AUTO_38 ) as c1,\n" //
                + "count(distinct CALCS.CC_AUTO_34) as c2,\n" //
                + "max(CALCS.CC_AUTO_30) as c3,\n" //
                + "min(CALCS.CC_AUTO_25) as c4,\n" //
                + "avg(CALCS.CC_AUTO_24) as c5,\n" //
                + "count(timestampadd(second, 1+2, time0)),\n" //
                + "max(timestampadd(second, 1, timestamp '1970-01-01 10:01:01')),\n" //
                + "count(CALCS.CC_AUTO_39),\n" //
                + "sum(CALCS.CC_AUTO_17)\n" //
                + "from tdvt.calcs";
        check(converter, originSql, ccSql, "tdvt");
    }

    @Test
    public void testMoreTimestampAddAndDiff() {
        String originSql, ccSql;
        originSql = "select sum((int1-int2)/(int1+int2)) as c1,\n"
                + "sum((int1-int2)/timestampdiff(second, time0, time1) ) as c2,\n"
                + "sum(timestampdiff(second, time0, time1)/timestampdiff(second, timestampadd(year,1, time1), time1)) as c3\n"
                + "from tdvt.calcs";
        ccSql = "select sum(CALCS.CC_AUTO_4) as c1,\n" //
                + "sum(CALCS.CC_AUTO_5 ) as c2,\n" //
                + "sum(CALCS.CC_AUTO_18) as c3\n" //
                + "from tdvt.calcs";
        check(converter, originSql, ccSql, "tdvt");

        // case 2
        originSql = "select sum(case when int0 > 0 then timestampdiff(day, time0, time1) end) as ab\n"
                + "  from tdvt.calcs as calcs";
        ccSql = "select sum(CALCS.CC_AUTO_11) as ab\n  from tdvt.calcs as calcs";
        check(converter, originSql, ccSql, "tdvt");

        // case 3
        originSql = "select sum(case when time0 <> time1 then (int2-int1)/timestampdiff(second, time0, time1) * 60\n"
                + "    else (int2 - int1)/ timestampdiff(second, time1, datetime0)*60 end)\n" //
                + "from tdvt.calcs";
        ccSql = "select sum(CALCS.CC_AUTO_6)\nfrom tdvt.calcs";
        check(converter, originSql, ccSql, "tdvt");

        // case 4
        originSql = "select case when int0 > 0 then timestampdiff(second, time0, time1) else 0 end\n"
                + "from tdvt.calcs\n"
                + "group by case when int0 > 0 then timestampdiff(second, time0, time1) else 0 end\n"
                + "order by case when int0 > 0 then timestampdiff(second, time0, time1) else 0 end";
        ccSql = "select CALCS.CC_AUTO_29\n" //
                + "from tdvt.calcs\n" //
                + "group by CALCS.CC_AUTO_29\n" //
                + "order by CALCS.CC_AUTO_29";
        check(converter, originSql, ccSql, "tdvt");

        // case 5
        originSql = "select case when int0 > 100 then timestampdiff(second, time0, time1)\n"
                + "                when int0 > 50 then timestampdiff(minute, time0, time1)\n"
                + "                when int0 > 0 then timestampdiff(hour, time0, time1) else null end\n"
                + "from tdvt.calcs group by case when int0 > 100 then timestampdiff(second, time0, time1)\n"
                + "                when int0 > 50 then timestampdiff(minute, time0, time1)\n"
                + "                when int0 > 0 then timestampdiff(hour, time0, time1) else null end";
        ccSql = "select CALCS.CC_AUTO_31\nfrom tdvt.calcs group by CALCS.CC_AUTO_31";
        check(converter, originSql, ccSql, "tdvt");

        // case 6
        originSql = "select case when int0 > 10 then sum(timestampdiff(second, time0, time1)) else sum(timestampdiff(minute, time0, time1)) end\n"
                + "from tdvt.calcs group by int0";
        ccSql = "select case when int0 > 10 then sum(CALCS.CC_AUTO_38) else sum(CALCS.CC_AUTO_1) end\n"
                + "from tdvt.calcs group by int0";
        check(converter, originSql, ccSql, "tdvt");

        // case 7: will not replace
        originSql = "with ca as(select time0 as t0, time1, datetime0 from tdvt.calcs)\n"
                + "select sum(timestampdiff(minute, ca.time1, ca.t0)), ca.datetime0\n"
                + "from ca group by ca.datetime0";
        ccSql = "with ca as(select time0 as t0, time1, datetime0 from tdvt.calcs)\n"
                + "select sum(timestampdiff(minute, ca.time1, ca.t0)), ca.datetime0\n"
                + "from ca group by ca.datetime0";
        check(converter, originSql, ccSql, "tdvt");

        // case 8
        originSql = "select sum(tmp.ab) from (\n"
                + "  select sum(timestampdiff(second, time0, time1)/timestampdiff(second, timestampadd(year,1, time1), time1)) as ab\n"
                + "  from tdvt.calcs as calcs group by ab order by ab ) tmp";
        ccSql = "select sum(tmp.ab) from (\n" //
                + "  select sum(CALCS.CC_AUTO_18) as ab\n" //
                + "  from tdvt.calcs as calcs group by ab order by ab ) tmp";
        check(converter, originSql, ccSql, "tdvt");

        // case 9
        originSql = "select sum(timestampdiff(minute, time1, time0)), datetime0, time1, time0\n"
                + "from tdvt.calcs group by datetime0, time1, time0\n" //
                + "union\n" //
                + "select max(timestampdiff(minute, time1, time0)), datetime0, time1, time0\n"
                + "from tdvt.calcs group by datetime0, time1, time0";
        ccSql = "select sum(CALCS.CC_AUTO_9), datetime0, time1, time0\n"
                + "from tdvt.calcs group by datetime0, time1, time0\n" //
                + "union\n" //
                + "select max(CALCS.CC_AUTO_9), datetime0, time1, time0\n"
                + "from tdvt.calcs group by datetime0, time1, time0";
        check(converter, originSql, ccSql, "tdvt");

        // case 10
        originSql = "select max(timestampdiff(second, time0, cast(datetime1 as timestamp)))\n"
                + " - min(timestampdiff(second, time0, cast(datetime1 as timestamp)))\n" //
                + "from tdvt.calcs";
        ccSql = "select max(CALCS.CC_AUTO_17)\n" //
                + " - min(CALCS.CC_AUTO_17)\n" //
                + "from tdvt.calcs";
        check(converter, originSql, ccSql, "tdvt");

        // case 11
        originSql = "select num1, max(TIMESTAMPADD(SQL_TSI_DAY, 1, TIMESTAMP '1970-01-01 10:01:01')) MAXTIME,\n"
                + "      max(TIMESTAMPADD(SQL_TSI_DAY, 1, TIMESTAMP '1970-01-01 10:01:01')) over() MAXTIME1\n"
                + "from tdvt.calcs where num1 > 0\n" //
                + "group by num1\n" //
                + "order by TIMESTAMPADD(SQL_TSI_DAY,1, TIMESTAMP '1970-01-01 10:01:01')";
        ccSql = "select num1, max(TIMESTAMPADD(SQL_TSI_DAY, 1, TIMESTAMP '1970-01-01 10:01:01')) MAXTIME,\n"
                + "      max(TIMESTAMPADD(SQL_TSI_DAY, 1, TIMESTAMP '1970-01-01 10:01:01')) over() MAXTIME1\n"
                + "from tdvt.calcs where num1 > 0\n" //
                + "group by num1\n" //
                + "order by TIMESTAMPADD(SQL_TSI_DAY,1, TIMESTAMP '1970-01-01 10:01:01')";
        check(converter, originSql, ccSql, "tdvt");

        // case 12
        originSql = "select num1, max(TIMESTAMPADD(SQL_TSI_DAY, 1, time0)) over() MAXTIME1\n" //
                + "from tdvt.calcs\n" //
                + "where num1 > 0\n" //
                + "group by num1, time0\n" //
                + "order by TIMESTAMPADD(SQL_TSI_DAY,1, TIMESTAMP'1970-01-01 10:01:01')";
        ccSql = "select num1, max(TIMESTAMPADD(SQL_TSI_DAY, 1, time0)) over() MAXTIME1\n" //
                + "from tdvt.calcs\n" //
                + "where num1 > 0\n" //
                + "group by num1, time0\n" //
                + "order by TIMESTAMPADD(SQL_TSI_DAY,1, TIMESTAMP'1970-01-01 10:01:01')";
        check(converter, originSql, ccSql, "tdvt");
    }

    @Test
    public void testExplicitCcNameToInnerName() {

        // case 1. explicit query name in inner-most sub-query
        String originSql = "select max(CALCS.CC_AUTO_17)\n" + " - min(CALCS.CC_AUTO_17)\n"
                + "from (select CC_AUTO_17 from tdvt.calcs group by CC_AUTO_17)";
        String ccSql = "select max(CALCS.CC_AUTO_17)\n" + " - min(CALCS.CC_AUTO_17)\n"
                + "from (select CC_AUTO_17 from tdvt.calcs group by CC_AUTO_17)";
        check(converter, originSql, ccSql, "tdvt");

        // case 2. explicit query name with AS ALIAS in inner-most sub-query
        originSql = "select max(CALCS.CC_AUTO_17)\n" + " - min(CALCS.CC_AUTO_17)\n"
                + "from (select CC_AUTO_17 as CC_AUTO_17  from tdvt.calcs group by CC_AUTO_17)";
        ccSql = "select max(CALCS.CC_AUTO_17)\n" + " - min(CALCS.CC_AUTO_17)\n"
                + "from (select CC_AUTO_17 as CC_AUTO_17  from tdvt.calcs group by CC_AUTO_17)";
        check(converter, originSql, ccSql, "tdvt");

        // case 3. explicit query CC with 'quotation marks'
        originSql = "select max(CALCS.CC_AUTO_17)\n" + " - min(\"CALCS\".\"CC_AUTO_17\")\n"
                + "from (select CC_AUTO_17 as CC_AUTO_17  from tdvt.calcs group by CC_AUTO_17)";
        ccSql = "select max(CALCS.CC_AUTO_17)\n" + " - min(\"CALCS\".\"CC_AUTO_17\")\n"
                + "from (select CC_AUTO_17 as CC_AUTO_17  from tdvt.calcs group by CC_AUTO_17)";
        check(converter, originSql, ccSql, "tdvt");

        originSql = "select max(CALCS.CC_AUTO_17)\n" + " - min(CALCS.CC_AUTO_17)\n"
                + "from (select \"CC_AUTO_17\" as CC_AUTO_17  from tdvt.calcs group by CC_AUTO_17)";
        ccSql = "select max(CALCS.CC_AUTO_17)\n" + " - min(CALCS.CC_AUTO_17)\n"
                + "from (select \"CC_AUTO_17\" as CC_AUTO_17  from tdvt.calcs group by CC_AUTO_17)";
        check(converter, originSql, ccSql, "tdvt");

        // case 4. explicit query CC with lower-case
        originSql = "select max(CALCS.CC_AUTO_17)\n" + " - min(calcs.\"CC_AUTO_17\")\n"
                + "from (select \"CC_AUTO_17\" as CC_AUTO_17  from tdvt.calcs group by CC_AUTO_17)";
        ccSql = "select max(CALCS.CC_AUTO_17)\n" + " - min(calcs.\"CC_AUTO_17\")\n"
                + "from (select \"CC_AUTO_17\" as CC_AUTO_17  from tdvt.calcs group by CC_AUTO_17)";
        check(converter, originSql, ccSql, "tdvt");

    }

    private void check(ConvertToComputedColumn converter, String originSql, String ccSql) {
        check(converter, originSql, ccSql, "default");
    }

    private void check(ConvertToComputedColumn converter, String originSql, String ccSql, String project) {
        String transform = converter.transform(originSql, project, "DEFAULT");
        Assert.assertEquals(ccSql, transform);
    }

}

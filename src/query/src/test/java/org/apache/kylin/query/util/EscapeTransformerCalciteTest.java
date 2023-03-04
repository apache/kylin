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

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class EscapeTransformerCalciteTest {

    private static final EscapeTransformer transformer = new EscapeTransformer();

    @BeforeClass
    public static void prepare() {
        transformer.setFunctionDialect(EscapeDialect.CALCITE);
    }

    @Test
    public void scalarFNTest() {
        String originalSQL = "select { fn count(*) }, avg(sales) from tbl";
        String expectedSQL = "select {fn count(*)}, avg(sales) from tbl";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void leftFNTest() {
        String originalSQL = "select { fn LEFT(LSTG_FORMAT_NAME, 2) } from KYLIN_SALES";
        String expectedSQL = "select SUBSTRING(LSTG_FORMAT_NAME, 1, 2) from KYLIN_SALES";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void rightFNTest() {
        String originalSQL = "select { fn RIGHT(LSTG_FORMAT_NAME, 2) } from KYLIN_SALES";
        String expectedSQL = "select SUBSTRING(LSTG_FORMAT_NAME, CHAR_LENGTH(LSTG_FORMAT_NAME) + 1 - 2, 2) from KYLIN_SALES";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void lengthFNTest() {
        String originalSQL = "select {fn LENGTH('Happy')}";
        String expectedSQL = "select {fn LENGTH('Happy')}";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void convertFNTest() {
        String expectedSQL = "select CAST(PART_DT AS DATE), {fn LTRIM(CAST(PRICE AS VARCHAR))} from KYLIN_SALES";

        String originalSQL_1 = "select {fn CONVERT(PART_DT, SQL_DATE)}, {fn LTRIM({fn CONVERT(PRICE, SQL_VARCHAR)})} from KYLIN_SALES";
        String transformedSQL_1 = transformer.transform(originalSQL_1);
        Assert.assertEquals(expectedSQL, transformedSQL_1);

        String originalSQL_2 = "select {fn CONVERT(PART_DT, SQL_DATE)}, {fn LTRIM({fn CONVERT(PRICE, SQL_WVARCHAR)})} from KYLIN_SALES";
        String transformedSQL_2 = transformer.transform(originalSQL_2);
        Assert.assertEquals(expectedSQL, transformedSQL_2);

        String originalSQL_3 = "select {fn CONVERT(PART_DT, SQL_DATE)}, {fn LTRIM({fn CONVERT(PRICE, SQL_CHAR)})} from KYLIN_SALES";
        String transformedSQL_3 = transformer.transform(originalSQL_3);
        Assert.assertEquals(expectedSQL, transformedSQL_3);

        String originalSQL_4 = "select {fn CONVERT(PART_DT, SQL_DATE)}, {fn LTRIM({fn CONVERT(PRICE, SQL_WCHAR)})} from KYLIN_SALES";
        String transformedSQL_4 = transformer.transform(originalSQL_4);
        Assert.assertEquals(expectedSQL, transformedSQL_4);
    }

    @Test
    public void lcaseFNTest() {
        String originalSQL = "select { fn LCASE(LSTG_FORMAT_NAME) } from KYLIN_SALES";
        String expectedSQL = "select {fn LCASE(LSTG_FORMAT_NAME)} from KYLIN_SALES";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void ucaseFNTest() {
        String originalSQL = "select { fn UCASE(LSTG_FORMAT_NAME) } from KYLIN_SALES";
        String expectedSQL = "select {fn UCASE(LSTG_FORMAT_NAME)} from KYLIN_SALES";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void ifnullFNTest() {
        String originalSQL = "select { fn IFNULL(LSTG_FORMAT_NAME, 'Bad name') } from KYLIN_SALES";
        String expectedSQL = "select {fn IFNULL(LSTG_FORMAT_NAME, 'Bad name')} from KYLIN_SALES";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void logFNTest() {
        String originalSQL = "select { fn LOG(PRICE) } from KYLIN_SALES";
        String expectedSQL = "select {fn LOG(PRICE)} from KYLIN_SALES";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void currentDateFNTest() {
        String originalSQL = "select { fn CURRENT_DATE() }";
        String expectedSQL = "select CURRENT_DATE";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void currentTimeFNTest() {
        String originalSQL = "select { fn CURRENT_TIME() }";
        String expectedSQL = "select CURRENT_TIME";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void currentTimestampFNTest() {
        String originalSQL = "select { fn CURRENT_TIMESTAMP() }";
        String expectedSQL = "select CURRENT_TIMESTAMP";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void quotedStringTest() {
        String originalSQL = "select 'Hello World!', {fn LENGTH('12345 67890')}";
        String expectedSQL = "select 'Hello World!', {fn LENGTH('12345 67890')}";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void spaceDelimitersTest() {
        String originalSQL = "select 'Hello World!',\r\n\t {fn\tLENGTH('12345 \r\n\t 67890')}\nlimit 1";
        String expectedSQL = "select 'Hello World!',\r\n\t {fn LENGTH('12345 \r\n\t 67890')}\nlimit 1";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void ceilFloorTest() {
        String originSQL = "select ceil('2012-02-02 00:23:23' to year), ceil(floor(col to hour) to day)";
        String expectedSQL = "select CEIL('2012-02-02 00:23:23' to year), CEIL(FLOOR(col to hour) to day)";

        String transformedSQL = transformer.transform(originSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void ceilFloorDtTest() {
        String originSQL = "select ceil_datetime('2012-02-02 00:23:23', 'year'), ceil(floor_datetime(col, 'hour') to day)";
        String expectedSQL = "select CEIL('2012-02-02 00:23:23' to year), CEIL(FLOOR(col to hour) to day)";

        String transformedSQL = transformer.transform(originSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void testFailToTransformCeilFloorDt() {
        {
            String origin = "select ceil_datetime('2012-02-02 00:23:23')";
            Assert.assertEquals(origin, transformer.transform(origin));
        }

        {
            String origin = "select floor_datetime('2012-02-02 00:23:23')";
            Assert.assertEquals(origin, transformer.transform(origin));
        }
    }

    @Test
    public void testCeilFloorQuery() {
        String originSql = "SELECT {FN WEEK(CEIL( FLOOR(\t  TIME2 TO HOUR  ) TO DAY    )) }, FLOOR(SELLER_ID), CEIL(SELLER_ID) FROM TEST_MEASURE";
        String expectedSql = "SELECT WEEK(CEIL(FLOOR(TIME2 to HOUR) to DAY)), FLOOR(SELLER_ID), CEIL(SELLER_ID) FROM TEST_MEASURE";
        String transformedSQL = transformer.transform(originSql);
        Assert.assertEquals(expectedSql, transformedSQL);
    }

    @Test
    public void testSubstring() {
        String originString = "select substring( lstg_format_name   from   1  for   4 ) from test_kylin_fact limit 10;";
        String expectedSql = "select SUBSTRING(lstg_format_name, 1, 4) from test_kylin_fact limit 10";
        String transformedSQL = transformer.transform(originString);
        Assert.assertEquals(expectedSql, transformedSQL);

        originString = "select substring( lstg_format_name   from   1  ) from test_kylin_fact limit 10;";
        expectedSql = "select SUBSTRING(lstg_format_name, 1) from test_kylin_fact limit 10";
        transformedSQL = transformer.transform(originString);
        Assert.assertEquals(expectedSql, transformedSQL);

        originString = "select distinct " //
                + "substring (\"ZB_POLICY_T_VIEW\".\"DIMENSION1\" " //
                + "\nfrom position ('|1|' in \"ZB_POLICY_T_VIEW\".\"DIMENSION1\") + 3 " //
                + "\nfor (position ('|2|' in \"ZB_POLICY_T_VIEW\".\"DIMENSION1\") - position ('|1|' in \"ZB_POLICY_T_VIEW\".\"DIMENSION1\")) - 3"
                + ") as \"memberUniqueName\"  " //
                + "from \"FRPDB0322\".\"ZB_POLICY_T_VIEW\" \"ZB_POLICY_T_VIEW\" limit10;";
        expectedSql = "select distinct SUBSTRING(\"ZB_POLICY_T_VIEW\".\"DIMENSION1\", "
                + "position ('|1|' in \"ZB_POLICY_T_VIEW\".\"DIMENSION1\") + 3, "
                + "(position ('|2|' in \"ZB_POLICY_T_VIEW\".\"DIMENSION1\") - position ('|1|' in \"ZB_POLICY_T_VIEW\".\"DIMENSION1\")) - 3) as \"memberUniqueName\" "
                + "from \"FRPDB0322\".\"ZB_POLICY_T_VIEW\" \"ZB_POLICY_T_VIEW\" limit10";
        transformedSQL = transformer.transform(originString);
        Assert.assertEquals(expectedSql, transformedSQL);
    }

    @Test
    public void testTimestampdiffOrTimestampaddReplace() {
        String originString = "select timestampdiff(second,   time0,   time1) as c1 from tdvt.calcs;";
        String expectedSql = "select TIMESTAMPDIFF(second, time0, time1) as c1 from tdvt.calcs";
        String transformedSQL = transformer.transform(originString);
        Assert.assertEquals(expectedSql, transformedSQL);

        originString = "select timestampdiff(year, cast(time0  as timestamp), cast(datetime0 as timestamp)) from tdvt.calcs;";
        expectedSql = "select TIMESTAMPDIFF(year, cast(time0 as timestamp), cast(datetime0 as timestamp)) from tdvt.calcs";
        transformedSQL = transformer.transform(originString);
        Assert.assertEquals(expectedSql, transformedSQL);
    }

    @Test
    public void testExtractFromExpression() {
        String originalSQL = "select count(distinct {fn year(date0)}), max(extract(year from date1)),\n"
                + "       count(distinct month(date0)), max(extract(month from date1)),\n"
                + "       count(distinct quarter(date0)), max(extract(quarter from date1)),\n"
                + "       count(distinct hour(date0)), max(extract(hour from date1)),\n"
                + "       count(distinct minute(date0)), max(extract(minute from date1)),\n"
                + "       count(distinct second(date0)), max(extract(second from date1)),\n"
                + "       count(week(date0)), max(extract(week from date1)),\n"
                + "       count(dayofyear(date0)), max(extract(doy from date1)),\n"
                + "       count(dayofweek(date0)), max(extract(dow from date1)),\n"
                + "       count(dayofmonth(date0)), max(extract(day from date1)) from tdvt.calcs as calcs";
        String expectedSQL = "select count(distinct YEAR(date0)), max(YEAR(date1)),\n"
                + " count(distinct month(date0)), max(MONTH(date1)),\n"
                + " count(distinct quarter(date0)), max(QUARTER(date1)),\n"
                + " count(distinct hour(date0)), max(HOUR(date1)),\n"
                + " count(distinct minute(date0)), max(MINUTE(date1)),\n"
                + " count(distinct second(date0)), max(SECOND(date1)),\n" + " count(week(date0)), max(WEEK(date1)),\n"
                + " count(dayofyear(date0)), max(DAYOFYEAR(date1)),\n"
                + " count(dayofweek(date0)), max(DAYOFWEEK(date1)),\n"
                + " count(dayofmonth(date0)), max(DAYOFMONTH(date1)) from tdvt.calcs as calcs";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void testReplaceDoubleQuote() {
        String originalSQL = "select kylin_sales.\"DIMENSION2\", \"KYLIN_SALES\".DIM3, "
                + "\"DEFAULT\".KYLIN_SALES.\"DIM4\", \"KYLIN_SALES\".\"DIMENSION1\", '\"abc\"' as \"ABC\" from KYLIN_SALES";
        String expectedSQL = "select kylin_sales.\"DIMENSION2\", \"KYLIN_SALES\".DIM3, \"DEFAULT\".KYLIN_SALES.\"DIM4\", "
                + "\"KYLIN_SALES\".\"DIMENSION1\", '\"abc\"' as \"ABC\" from KYLIN_SALES";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void testOverlayReplace() {
        String originSQL = "select overlay(myStr1   PLACING myStr2   FROM  myInteger) from tableA";
        String expectedSQL = "select OVERLAY(myStr1 PLACING myStr2 FROM myInteger) from tableA";
        String replacedString = transformer.transform(originSQL);
        Assert.assertEquals(expectedSQL, replacedString);

        originSQL = "select overlay(myStr1 PLACING myStr2 FROM myInteger FOR myInteger2) from tableA";
        expectedSQL = "select OVERLAY(myStr1 PLACING myStr2 FROM myInteger for myInteger2) from tableA";
        replacedString = transformer.transform(originSQL);
        Assert.assertEquals(expectedSQL, replacedString);
    }

    @Test
    public void testGroupingSets() {
        String originSQL = "select sum(price) as GMV group by grouping sets((lstg_format_name, cal_dt, slr_segment_cd), (cal_dt, slr_segment_cd), (lstg_format_name, slr_segment_cd));";
        String expectedSQL = "select sum(price) as GMV group by grouping sets((lstg_format_name, cal_dt, slr_segment_cd),(cal_dt, slr_segment_cd),(lstg_format_name, slr_segment_cd))";
        String transformedSQL = transformer.transform(originSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void testGroupingFunction() {
        String originSQL = "select\n"
                + "(case grouping(cal_dt) when 1 then 'ALL' else cast(cal_dt as varchar(256)) end) as dt,\n"
                + "(case grouping(slr_segment_cd) when 1 then 'ALL' else cast(slr_segment_cd as varchar(256)) end) as cd,\n"
                + "(case grouping(lstg_format_name) when 1 then 'ALL' else lstg_format_name end) as name,\n"
                + "sum(price) as GMV, count(*) as TRANS_CNT from test_kylin_fact\n"
                + "where cal_dt between '2012-01-01' and '2012-02-01'\n"
                + "group by grouping sets((lstg_format_name, cal_dt, slr_segment_cd), (cal_dt, slr_segment_cd), (lstg_format_name, slr_segment_cd))";
        String expectedSQL = "select\n"
                + "(case GROUPING(cal_dt) when 1 then 'ALL' else cast(cal_dt as varchar(256)) end) as dt,\n"
                + "(case GROUPING(slr_segment_cd) when 1 then 'ALL' else cast(slr_segment_cd as varchar(256)) end) as cd,\n"
                + "(case GROUPING(lstg_format_name) when 1 then 'ALL' else lstg_format_name end) as name,\n"
                + "sum(price) as GMV, count(*) as TRANS_CNT from test_kylin_fact\n"
                + "where cal_dt between '2012-01-01' and '2012-02-01'\n"
                + "group by grouping sets((lstg_format_name, cal_dt, slr_segment_cd),(cal_dt, slr_segment_cd),(lstg_format_name, slr_segment_cd))";
        String transformedSQL = transformer.transform(originSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void testPI() {
        String originalSQL = "select sum({fn pi()}), count(pi() + price), lstg_format_name  from test_kylin_fact";
        String expectedSQL = "select sum({fn pi()}), count(PI + price), lstg_format_name from test_kylin_fact";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Ignore
    @Test
    public void test() {
        String originalSQL = "select {fn EXTRACT(YEAR from PART_DT)} from KYLIN_SALES";
        String expectedSQL = "select YEAR(PART_DT) from KYLIN_SALES";
        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void escapeTSTest() {

        String originalSQL = "select {ts '2013-01-01 00:00:00'}, {d '2013-01-01'}, {t '00:00:00'}";
        String expectedSQL = "select TIMESTAMP '2013-01-01 00:00:00', DATE '2013-01-01', TIME '00:00:00'";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void escapeBangEqualTest() {

        String originalSQL = "select a from table where a != 'b!=c'";
        String expectedSQL = "select a from table where a <> 'b!=c'";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void testReplaceStringWithVarchar() {
        // test cases ref: https://dev.mysql.com/doc/refman/8.0/en/select.html
        String[] actuals = new String[] {
                // select, from, where, like, group by, having, order by, subclause
                "Select 0 as STRING, cast(D1.c1 as STRING) as c2 from (select distinct substring(cast(T33458.CAL_DT as STRING), 1, 30) as c1 from TEST_KYLIN_FACT T33458) D1 where cast(D1.c1 as STRING) like cast('2012-01%' as STRING) group by cast(D1.c1 as STRING) having cast(D1.c1 as STRING)>'2012-01-01' order by cast(D1.c1 as STRING)",
                // over(window)
                "select TRANS_ID, CAL_DT, LSTG_FORMAT_NAME, MAX(CAL_DT) over (PARTITION BY CAST(LSTG_FORMAT_NAME AS STRING)) from TEST_KYLIN_FACT",
                // case when else
                "select case cast(CAL_DT as STRING) when cast('2012-01-13' as STRING) THEN cast('1' as STRING) ELSE cast('null' as STRING) END AS STRING, CAL_DT from TEST_KYLIN_FACT order by cast(CAL_DT as STRING)",
                // join
                "select * from TEST_KYLIN_FACT a left join EDW.TEST_CAL_DT b on cast(a.CAL_DT as STRING)=cast(b.CAL_DT as STRING) limit 100",
                // union
                "select * from TEST_KYLIN_FACT where cast(LSTG_FORMAT_NAME as STRING)='FP-GTC' union select * from TEST_KYLIN_FACT where cast(LSTG_FORMAT_NAME as STRING)='Auction' limit 100",
                // with
                "WITH t1 AS (Select 0 as STRING, cast(D1.c1 as STRING) as c2 from (select distinct substring(cast(T33458.CAL_DT as STRING), 1, 30) as c1 from TEST_KYLIN_FACT T33458) D1 where cast(D1.c1 as STRING) like cast('2012-01%' as STRING) group by cast(D1.c1 as STRING) having cast(D1.c1 as STRING)> '2012-01-01' order by cast(D1.c1 as STRING)) \n"
                        + "SELECT * from t1\n" + "UNION ALL\n" + "SELECT * from t1" };
        String[] expecteds = new String[] {
                "Select 0 as STRING, cast(D1.c1 as VARCHAR) as c2 from (select distinct SUBSTRING(cast(T33458.CAL_DT as VARCHAR), 1, 30) as c1 from TEST_KYLIN_FACT T33458) D1 where cast(D1.c1 as VARCHAR) like cast('2012-01%' as VARCHAR) group by cast(D1.c1 as VARCHAR) having cast(D1.c1 as VARCHAR)> '2012-01-01' order by cast(D1.c1 as VARCHAR)",
                "select TRANS_ID, CAL_DT, LSTG_FORMAT_NAME, MAX(CAL_DT) over (PARTITION BY CAST(LSTG_FORMAT_NAME AS VARCHAR)) from TEST_KYLIN_FACT",
                "select case cast(CAL_DT as VARCHAR) when cast('2012-01-13' as VARCHAR) THEN cast('1' as VARCHAR) ELSE cast('null' as VARCHAR) END AS STRING, CAL_DT from TEST_KYLIN_FACT order by cast(CAL_DT as VARCHAR)",
                "select * from TEST_KYLIN_FACT a left join EDW.TEST_CAL_DT b on cast(a.CAL_DT as VARCHAR) = cast(b.CAL_DT as VARCHAR) limit 100",
                "select * from TEST_KYLIN_FACT where cast(LSTG_FORMAT_NAME as VARCHAR) = 'FP-GTC' union select * from TEST_KYLIN_FACT where cast(LSTG_FORMAT_NAME as VARCHAR) = 'Auction' limit 100",
                "WITH t1 AS (Select 0 as STRING, cast(D1.c1 as VARCHAR) as c2 from (select distinct SUBSTRING(cast(T33458.CAL_DT as VARCHAR), 1, 30) as c1 from TEST_KYLIN_FACT T33458) D1 where cast(D1.c1 as VARCHAR) like cast('2012-01%' as VARCHAR) group by cast(D1.c1 as VARCHAR) having cast(D1.c1 as VARCHAR)> '2012-01-01' order by cast(D1.c1 as VARCHAR))\n"
                        + "SELECT * from t1\n" + "UNION ALL\n" + "SELECT * from t1" };
        for (int i = 0; i < actuals.length; ++i) {
            String transformedActual = transformer.transform(actuals[i]);
            Assert.assertEquals(expecteds[i], transformedActual);
            System.out.println("\nTRANSFORM SUCCEED\nBEFORE:\n" + actuals[i] + "\nAFTER:\n" + expecteds[i]);
        }
    }
}

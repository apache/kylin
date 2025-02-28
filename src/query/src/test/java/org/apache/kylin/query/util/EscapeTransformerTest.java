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

import java.io.File;
import java.nio.charset.Charset;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.query.util.EscapeFunction.FnConversion;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class EscapeTransformerTest {

    private static final EscapeTransformer transformer = new EscapeTransformer();

    @BeforeClass
    public static void prepare() {

        /*
         * Use all existing function conversions
         */
        EscapeDialect ALL_FUNC = new EscapeDialect() {

            @Override
            public void init() {
                for (FnConversion func : FnConversion.values()) {
                    register(func);
                }
            }

            @Override
            public String defaultConversion(String functionName, String[] args) {
                return EscapeFunction.normalFN(functionName, args);
            }
        };

        transformer.setFunctionDialect(ALL_FUNC);
    }

    @Test
    public void testStrings() {
        String originalSQL = "select N'foo', x'foo', X'foo', _a'foo', U&'foo' from tbl where bar = N'a' and bar='a'";
        String expectedSQL = "select N'foo', x'foo', X'foo', _a'foo', U&'foo' from tbl where bar = N'a' and bar = 'a'";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void testSqlwithComment() {
        String originalSQL = "select --test comment will remove\n \"--wont remove in quote\", /* will remove multi line comment*/ { fn count(*) } from tbl";
        String expectedSQL = "select\n\"--wont remove in quote\", count(*) from tbl";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void notEscapeQuoteTest() {

        String originalSQL = "select 'do not escape {fn CURRENT_TIME()}' name from table_2 "
                + "where address='qwerty(1123)' or address='qwerty(1123' or address='qwerty1123)'";
        String expectedSQL = "select 'do not escape {fn CURRENT_TIME()}' name from table_2 "
                + "where address = 'qwerty(1123)' or address = 'qwerty(1123' or address = 'qwerty1123)'";
        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);

        String originSQL2 = "SELECT ' FLOOR(ABC TO HOUR)' FROM T";
        String transformedSQL2 = transformer.transform(originSQL2);
        Assert.assertEquals(originSQL2, transformedSQL2);
    }

    @Test
    public void normalFNTest() {
        String originalSQL = "select { fn count(*) }, avg(sales) from tbl";
        String expectedSQL = "select count(*), avg(sales) from tbl";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void leftFNTest() {
        String originalSQL = "select { fn \n LEFT(LSTG_FORMAT_NAME,\n 2) } from KYLIN_SALES";
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
    public void rightFNTest2() {
        String originalSQL = "SELECT CASE WHEN \"CALCS\".\"NUM4\" >= 0 THEN {fn RIGHT(\"CALCS\".\"STR0\","
                + " {fn CONVERT({fn TRUNCATE(\"CALCS\".\"NUM4\",0)}, SQL_BIGINT)})} ELSE NULL END AS \"TEMP_Test__3364126490__0_\""
                + " FROM \"TDVT\".\"CALCS\" \"CALCS\""
                + " GROUP BY CASE WHEN \"CALCS\".\"NUM4\" >= 0 THEN {fn RIGHT(\"CALCS\".\"STR0\","
                + " {fn CONVERT({fn TRUNCATE(\"CALCS\".\"NUM4\",0)}, SQL_BIGINT)})} ELSE NULL END";
        String expectedSQL = "SELECT CASE WHEN \"CALCS\".\"NUM4\" >= 0 THEN SUBSTRING(\"CALCS\".\"STR0\","
                + " CHAR_LENGTH(\"CALCS\".\"STR0\") + 1 - CAST(TRUNCATE(\"CALCS\".\"NUM4\", 0) AS BIGINT),"
                + " CAST(TRUNCATE(\"CALCS\".\"NUM4\", 0) AS BIGINT)) ELSE NULL END AS \"TEMP_Test__3364126490__0_\""
                + " FROM \"TDVT\".\"CALCS\" \"CALCS\""
                + " GROUP BY CASE WHEN \"CALCS\".\"NUM4\" >= 0 THEN SUBSTRING(\"CALCS\".\"STR0\","
                + " CHAR_LENGTH(\"CALCS\".\"STR0\") + 1 - CAST(TRUNCATE(\"CALCS\".\"NUM4\", 0) AS BIGINT),"
                + " CAST(TRUNCATE(\"CALCS\".\"NUM4\", 0) AS BIGINT)) ELSE NULL END";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void lengthFNTest() {
        String originalSQL = "select {fn LENGTH('Happy')}";
        String expectedSQL = "select LENGTH('Happy')";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void convertFNTest() {
        String originalSQL = "select {fn CONVERT(PART_DT, SQL_DATE)}, {fn LTRIM({fn CONVERT(PRICE, SQL_VARCHAR)})} from KYLIN_SALES";
        String expectedSQL = "select CAST(PART_DT AS DATE), TRIM(leading CAST(PRICE AS VARCHAR)) from KYLIN_SALES";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void lcaseFNTest() {
        String originalSQL = "select { fn LCASE(LSTG_FORMAT_NAME) } from KYLIN_SALES";
        String expectedSQL = "select LOWER(LSTG_FORMAT_NAME) from KYLIN_SALES";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void ucaseFNTest() {
        String originalSQL = "select { fn UCASE(LSTG_FORMAT_NAME) } from KYLIN_SALES";
        String expectedSQL = "select UPPER(LSTG_FORMAT_NAME) from KYLIN_SALES";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void logFNTest() {
        String originalSQL = "select { fn LOG(PRICE) } from KYLIN_SALES";
        String expectedSQL = "select LN(PRICE) from KYLIN_SALES";

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

        String originalSQL1 = "select { fn CURRENT_TIMESTAMP(0) }";

        String transformedSQL1 = transformer.transform(originalSQL1);
        Assert.assertEquals(expectedSQL, transformedSQL1);
    }

    @Test
    public void quotedStringTest() {
        String originalSQL = "select 'Hello World!', {fn LENGTH('12345 67890')}";
        String expectedSQL = "select 'Hello World!', LENGTH('12345 67890')";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void spaceDelimitersTest() {
        String originalSQL = "select 'Hello World!',\r\n\t {fn\tLENGTH('12345 \r\n\t 67890')}\nlimit 1";
        String expectedSQL = "select 'Hello World!',\r\n\t LENGTH('12345 \r\n\t 67890')\nlimit 1";

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
    public void testAnyWithJavaCCSignature() {

        String originSql = "select {ts '2013-01-01 00:00:00'} from test_kylin_fact where '1'='1' and'{{KYLIN_ACCOUNT.ACCOUNT_ID}}' ='2'";
        String expectedSql = "select TIMESTAMP '2013-01-01 00:00:00' from test_kylin_fact where '1' = '1' and '{{KYLIN_ACCOUNT.ACCOUNT_ID}}' = '2'";

        String transformedSQL = transformer.transform(originSql);
        Assert.assertEquals(expectedSql, transformedSQL);
    }

    @Test
    public void trimTest() {
        String originalSQL = "SELECT {FN TRIM( '     test    ')}";
        String expectedSQL = "SELECT TRIM(both '     test    ')";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void weekTest() {
        String originalSQL = "SELECT {FN WEEK('2002-06-18')}";
        String expectedSQL = "SELECT WEEK('2002-06-18')";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void timestampAddTest() {
        String originalSQL = "SELECT {FN TIMESTAMPADD(MONTH, 2 ,'2014-02-18')}";
        String expectedSQL = "SELECT TIMESTAMPADD(MONTH, 2, '2014-02-18')";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void timestampDiffTest() {
        String originalSQL = "SELECT {FN TIMESTAMPDIFF(MONTH,'2015-03-18','2015-07-29')}";
        String expectedSQL = "SELECT TIMESTAMPDIFF(MONTH, '2015-03-18', '2015-07-29')";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void testBigQuery() throws Exception {
        //cpic query was caused StackOverFlow Error
        String originSql = FileUtils.readFileToString(new File("src/test/resources/query/cpic/big_query1.sql"),
                Charset.defaultCharset());
        String expectedSql = FileUtils.readFileToString(
                new File("src/test/resources/query/cpic/big_query1.sql.expected"), Charset.defaultCharset());

        String transformedSQL = transformer.transform(originSql).replaceAll("\n+", "");
        expectedSql = expectedSql.replaceAll("\n+", "");
        Assert.assertEquals(expectedSql, transformedSQL);

    }

    @Test
    public void testRemoveCommentQuery() throws Exception {
        String originalSQL = "--a single line comment\r" + "select 'a---b'+\"a--b\", 'ab''cd', \"abc\"\"cd\" "
                + ", 'abc\"def', \"abc'def\""
                + " --test comment will remove\n \"--won't remove in quote, /*test*/\", /* will remove multi line comment*/ { fn count(*) }"
                + " from tbl where a in ('a--b') and c in (\"a---b\", 'a--b', '')";
        String transformedSQL = (new RawSqlParser(originalSQL)).parse().getStatementString();

        String expectedSQL = "select 'a---b'+\"a--b\", 'ab''cd', \"abc\"\"cd\" , 'abc\"def', \"abc'def\"\n"
                + "\"--won't remove in quote, /*test*/\", { fn count(*) } from tbl where a in ('a--b') and c in (\"a---b\", 'a--b', '')";
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void testReserveHintQuery() throws Exception {
        String originalSQL = "select /**/ /*+*//*+ some hint */ --test comment will remove\n"
                + " \"--won't remove in quote, /*test*/\", /* will remove multi line comment*/ { fn count(*) } from tbl";
        String transformedSQL = (new RawSqlParser(originalSQL)).parse().getStatementString();

        String expectedSQL = "select /*+*/ /*+ some hint */\n\"--won't remove in quote, /*test*/\", { fn count(*) } from tbl";
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void testReservedHintQueryForEscapeParser() {
        String originalSQL = "select /**/ /*+*//*+ some hint */ \n"
                + " \"--won't remove in quote, /*test*/\",  { fn count(*) } from tbl";
        String transformedSQL = transformer.transform(originalSQL);

        String expectedSQL = "select /*+*/ /*+ some hint */\n"
                + "\"--won't remove in quote, /*test*/\", count(*) from tbl";
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void testApproxNumericLiteral() {
        String originSql = "select LO_DISCOUNT*0.10e+000 from ssb.lineorder";
        String transformedSQL = transformer.transform(originSql);
        System.out.println(transformedSQL);
        String expectedSQL = "select LO_DISCOUNT * 0.10e+000 from ssb.lineorder";
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void testTransformJDBCFuncQuery() {
        String originSql = "SELECT \"CALCS\".\"DATETIME0\", {DATE '2004-07-01'}, {fn TIMESTAMPDIFF(SQL_TSI_DAY,{ts '2004-07-01 00:00:00'},{fn CONVERT(\"CALCS\".\"DATETIME0\", DATE)})} AS \"TEMP_Test__2422160351__0_\"\n"
                + "FROM \"TDVT\".\"CALCS\" \"CALCS\"\n" + "GROUP BY \"CALCS\".\"DATETIME0\"";
        String expectedSql = "SELECT \"CALCS\".\"DATETIME0\", DATE '2004-07-01', TIMESTAMPDIFF(DAY, TIMESTAMP '2004-07-01 00:00:00', CAST(\"CALCS\".\"DATETIME0\" AS DATE)) AS \"TEMP_Test__2422160351__0_\"\n"
                + "FROM \"TDVT\".\"CALCS\" \"CALCS\"\n" + "GROUP BY \"CALCS\".\"DATETIME0\"";
        String transformedSQL = transformer.transform(originSql);
        Assert.assertEquals(expectedSql, transformedSQL);
    }

    @Test
    public void testCeilFloorQueryNotConvert() {
        String originSQL = "SELECT FLOOR(ABC   TO   OTHER) FROM T";
        String transformedSQL = transformer.transform(originSQL);
        Assert.assertEquals(originSQL, transformedSQL);
    }

    @Test
    public void testSubstring() {
        String originString = "select substring( lstg_format_name   from   1  for   4 ) from test_kylin_fact limit 10;";
        String expectedSql = "select substring(lstg_format_name, 1, 4) from test_kylin_fact limit 10";
        String transformedSQL = transformer.transform(originString);
        Assert.assertEquals(expectedSql, transformedSQL);

        originString = "select substring( lstg_format_name   from   1  ) from test_kylin_fact limit 10;";
        expectedSql = "select substring(lstg_format_name, 1) from test_kylin_fact limit 10";
        transformedSQL = transformer.transform(originString);
        Assert.assertEquals(expectedSql, transformedSQL);

        originString = "select distinct " //
                + "substring (\"ZB_POLICY_T_VIEW\".\"DIMENSION1\" " //
                + "\nfrom position ('|1|' in \"ZB_POLICY_T_VIEW\".\"DIMENSION1\") + 3 " //
                + "\nfor (position ('|2|' in \"ZB_POLICY_T_VIEW\".\"DIMENSION1\") - position ('|1|' in \"ZB_POLICY_T_VIEW\".\"DIMENSION1\")) - 3"
                + ") as \"memberUniqueName\"  " //
                + "from \"FRPDB0322\".\"ZB_POLICY_T_VIEW\" \"ZB_POLICY_T_VIEW\" limit10;";
        expectedSql = "select distinct substring(\"ZB_POLICY_T_VIEW\".\"DIMENSION1\", "
                + "position ('|1|' in \"ZB_POLICY_T_VIEW\".\"DIMENSION1\") + 3, "
                + "(position ('|2|' in \"ZB_POLICY_T_VIEW\".\"DIMENSION1\") - position ('|1|' in \"ZB_POLICY_T_VIEW\".\"DIMENSION1\")) - 3) as \"memberUniqueName\" "
                + "from \"FRPDB0322\".\"ZB_POLICY_T_VIEW\" \"ZB_POLICY_T_VIEW\" limit10";
        transformedSQL = transformer.transform(originString);
        Assert.assertEquals(expectedSql, transformedSQL);
    }

    @Test
    public void timestampdiffOrTimestampaddReplace() {
        String originString = "select timestampdiff(second,   \"calcs\".time0,   calcs.time1) as c1 from tdvt.calcs;";
        String expectedSql = "select TIMESTAMPDIFF(second, \"calcs\".time0, calcs.time1) as c1 from tdvt.calcs";
        String transformedSQL = transformer.transform(originString);
        Assert.assertEquals(expectedSql, transformedSQL);

        originString = "select timestampdiff(year, cast(time0  as timestamp), cast(datetime0 as timestamp)) from tdvt.calcs;";
        expectedSql = "select TIMESTAMPDIFF(year, cast(time0 as timestamp), cast(datetime0 as timestamp)) from tdvt.calcs";
        transformedSQL = transformer.transform(originString);
        Assert.assertEquals(expectedSql, transformedSQL);
    }

    @Test
    public void testExtractFromExpression() {
        String originalSQL = "select count(distinct year(date0)), max(extract(year from date1)),\n"
                + "       count(distinct month(date0)), max(extract(month from date1)),\n"
                + "       count(distinct quarter(date0)), max(extract(quarter from date1)),\n"
                + "       count(distinct hour(date0)), max(extract(hour from date1)),\n"
                + "       count(distinct minute(date0)), max(extract(minute from date1)),\n"
                + "       count(distinct second(date0)), max(extract(second from date1)),\n"
                + "       count(week(date0)), max(extract(week from date1)),\n"
                + "       count(dayofyear(date0)), max(extract(doy from date1)),\n"
                + "       count(dayofweek(date0)), max(extract(dow from date1)),\n"
                + "       count(dayofmonth(date0)), max(extract(day from date1)) from tdvt.calcs as calcs";
        String expectedSQL = "select count(distinct year(date0)), max(YEAR(date1)),\n"
                + " count(distinct month(date0)), max(MONTH(date1)),\n"
                + " count(distinct quarter(date0)), max(QUARTER(date1)),\n"
                + " count(distinct hour(date0)), max(HOUR(date1)),\n"
                + " count(distinct minute(date0)), max(MINUTE(date1)),\n"
                + " count(distinct second(date0)), max(SECOND(date1)),\n" //
                + " count(week(date0)), max(week(date1)),\n" + " count(dayofyear(date0)), max(DAYOFYEAR(date1)),\n"
                + " count(dayofweek(date0)), max(DAYOFWEEK(date1)),\n"
                + " count(dayofmonth(date0)), max(DAYOFMONTH(date1)) from tdvt.calcs as calcs";

        String transformedSQL = transformer.transform(originalSQL);
        Assert.assertEquals(expectedSQL, transformedSQL);
    }

    @Test
    public void testReplaceDoubleQuote() {
        String originalSQL = "select kylin_sales.\"DIMENSION2\", \"KYLIN_SALES\".DIM3, "
                + "\"DEFAULT\".KYLIN_SALES.\"DIM4\", \"KYLIN_SALES\".\"DIMENSION1\", '\"abc\"' as \"ABC\" from KYLIN_SALES";
        String expectedSQL = "select kylin_sales.\"DIMENSION2\", \"KYLIN_SALES\".DIM3, "
                + "\"DEFAULT\".KYLIN_SALES.\"DIM4\", \"KYLIN_SALES\".\"DIMENSION1\", '\"abc\"' as \"ABC\" from KYLIN_SALES";

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
        String originString = "select sum(price) as GMV group by grouping sets((lstg_format_name, cal_dt, slr_segment_cd), (cal_dt, slr_segment_cd), (lstg_format_name, slr_segment_cd));";
        String expectedSQL = "select sum(price) as GMV group by grouping sets((lstg_format_name, cal_dt, slr_segment_cd),(cal_dt, slr_segment_cd),(lstg_format_name, slr_segment_cd))";
        String replacedString = transformer.transform(originString);
        Assert.assertEquals(expectedSQL, replacedString);
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
        String expectedSQL = "select sum(pi()), count(PI + price), lstg_format_name from test_kylin_fact";

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
}

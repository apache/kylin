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
package org.apache.kylin.sdk.datasource.framework.conv;

import java.sql.SQLException;
import java.util.Locale;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.sdk.datasource.adaptor.SQLDWAdaptor;
import org.apache.kylin.sdk.datasource.framework.def.DataSourceDef;
import org.apache.kylin.sdk.datasource.framework.def.DataSourceDefProvider;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class SqlConverterTest extends NLocalFileMetadataTestCase {
    private final static String TEST_TARGET = "testing";

    @Before
    public void setUp() {
        createTestMetadata();
    }

    @After
    public void after() {
        cleanupTestMetadata();
    }

    @Test
    public void testConvertSqlWithoutEscape() throws SQLException {
        DataSourceDefProvider provider = DataSourceDefProvider.getInstance();
        ConvMaster master = new ConvMaster(provider.getDefault(), provider.getById(TEST_TARGET));
        SqlConverter converter = new SqlConverter(new SqlConverter.IConfigurer() {
            @Override
            public boolean skipHandleDefault() {
                return true;
            }

            @Override
            public boolean useUppercaseDefault() {
                return true;
            }

            @Override
            public boolean allowNoOffset() {
                return true;
            }

            @Override
            public boolean allowFetchNoRows() {
                return true;
            }

            @Override
            public boolean allowNoOrderByWithFetch() {
                return true;
            }

            @Override
            public boolean enableCache() {
                return true;
            }

            @Override
            public boolean enableQuote() {
                return true;
            }

            @Override
            public String getTransformDatePattern() {
                return null;
            }
        }, master);

        // escape default keywords
        Assert.assertEquals("SELECT *\nFROM \"DEFAULT\".\"FACT\"",
                converter.convertSql("select * from \"DEFAULT\".FACT"));
        Assert.assertEquals("SELECT *\nFROM \"Default\".\"FACT\"",
                converter.convertSql("select * from \"Default\".FACT"));
        Assert.assertEquals("SELECT *\nFROM \"default\".\"FACT\"",
                converter.convertSql("select * from \"default\".FACT"));
    }

    @Test
    @SuppressWarnings("checkstyle:methodlength")
    public void testConvertSqlWithEscape() throws SQLException, SqlParseException {
        DataSourceDefProvider provider = DataSourceDefProvider.getInstance();
        ConvMaster master = new ConvMaster(provider.getDefault(), provider.getById(TEST_TARGET));
        SqlConverter converter = new SqlConverter(new SqlConverter.IConfigurer() {
            @Override
            public boolean useUppercaseDefault() {
                return true;
            }

            @Override
            public boolean allowNoOffset() {
                return true;
            }

            @Override
            public boolean allowFetchNoRows() {
                return true;
            }

            @Override
            public boolean allowNoOrderByWithFetch() {
                return true;
            }

            @Override
            public boolean enableCache() {
                return true;
            }

            @Override
            public boolean enableQuote() {
                return true;
            }

            @Override
            public String getTransformDatePattern() {
                return null;
            }
        }, master);

        // normal cases
        Assert.assertEquals("SELECT 1", converter.convertSql("select     1"));
        Assert.assertEquals("SELECT *\nFROM \"FACT\"", converter.convertSql("select * from FACT"));

        // limit and offset
        Assert.assertEquals("SELECT 1\nFETCH NEXT 1 ROWS ONLY", converter.convertSql("SELECT 1 LIMIT 1"));
        Assert.assertEquals("SELECT 1\nFETCH NEXT 0 ROWS ONLY", converter.convertSql("SELECT 1 LIMIT 0"));
        Assert.assertEquals("SELECT 1\nOFFSET 1 ROWS\nFETCH NEXT 1 ROWS ONLY",
                converter.convertSql("SELECT 1 LIMIT 1 OFFSET 1"));

        // escape default keywords
        Assert.assertEquals("SELECT *\nFROM \"DEFAULT\".\"FACT\"", converter.convertSql("select * from DEFAULT.FACT"));
        Assert.assertEquals("SELECT *\nFROM \"DEFAULT\".\"FACT\"",
                converter.convertSql("select * from \"DEFAULT\".FACT"));
        Assert.assertEquals("SELECT *\nFROM \"DEFAULT\".\"FACT\"",
                converter.convertSql("select * from \"Default\".FACT"));
        Assert.assertEquals("SELECT *\nFROM \"DEFAULT\".\"FACT\"",
                converter.convertSql("select * from \"default\".FACT"));

        // function mapping
        Assert.assertEquals("SELECT EXTRACT(DOY FROM \"PART_DT\")\nFROM \"DEFAULT\".\"FACT\"",
                converter.convertSql("select DAYOFYEAR(PART_DT) from \"DEFAULT\".FACT"));
        Assert.assertEquals(
                "SELECT 12 * (EXTRACT(YEAR FROM \"DT1\") - EXTRACT(YEAR FROM \"DT2\")) + EXTRACT(MONTH FROM \"DT1\") - EXTRACT(MONTH FROM \"DT2\") - "
                        + "CASE WHEN EXTRACT(DAY FROM \"DT2\") > EXTRACT(DAY FROM \"DT1\") THEN 1 ELSE 0 END\n"
                        + "FROM \"DEFAULT\".\"FACT\"",
                converter.convertSql("select TIMESTAMPDIFF(month,DT2,      DT1) from \"DEFAULT\".FACT"));
        Assert.assertEquals("SELECT TRUNC(\"ID\")\nFROM \"DEFAULT\".\"FACT\"",
                converter.convertSql("select cast(ID as INT) from \"DEFAULT\".FACT"));
        Assert.assertEquals("SELECT 1\nFROM \"A\"\nWHERE 1 BETWEEN 0 AND 2",
                converter.convertSql("select 1 from a where 1 BETWEEN 0 and 2"));
        Assert.assertEquals("SELECT \"CURRENT_DATE\", TEST_CURR_TIME()",
                converter.convertSql("select CURRENT_DATE, CURRENT_TIME"));
        Assert.assertEquals(
                "SELECT EXP(AVG(LN(EXTRACT(DOY FROM CAST('2018-03-20' AS DATE)))))\nFROM \"DEFAULT\".\"FACT\"",
                converter.convertSql(
                        "select exp(avg(ln(dayofyear(cast('2018-03-20' as date))))) from \"DEFAULT\".FACT"));

        // over function
        Assert.assertEquals(
                "SELECT STDDEVP(\"C1\") OVER (ORDER BY \"C1\")\nFROM \"TEST_SUITE\"\nFETCH NEXT 1 ROWS ONLY",
                converter.convertSql("select stddev_pop(c1) over(order by c1) from test_suite limit 1"));

        // type mapping
        Assert.assertEquals("SELECT CAST(\"PRICE\" AS DOUBLE PRECISION)\nFROM \"DEFAULT\".\"FACT\"",
                converter.convertSql("select cast(PRICE as DOUBLE) from \"DEFAULT\".FACT"));
        Assert.assertEquals("SELECT CAST(\"PRICE\" AS DECIMAL(19, 4))\nFROM \"DEFAULT\".\"FACT\"",
                converter.convertSql("select cast(PRICE as DECIMAL(19,4)) from \"DEFAULT\".FACT"));
        Assert.assertEquals("SELECT CAST(\"PRICE\" AS DECIMAL(19))\nFROM \"DEFAULT\".\"FACT\"",
                converter.convertSql("select cast(PRICE as DECIMAL(19)) from \"DEFAULT\".FACT"));
        Assert.assertEquals("SELECT CAST(\"BYTE\" AS BIT(8))\nFROM \"DEFAULT\".\"FACT\"",
                converter.convertSql("select cast(BYTE as BYTE) from \"DEFAULT\".FACT"));
        Assert.assertEquals("SELECT CAST(\"BYTE\" AS VARCHAR(1024))\nFROM \"DEFAULT\".\"FACT\"",
                converter.convertSql("select cast(BYTE as VARCHAR(1024)) from \"DEFAULT\".FACT"));

        // cannot find mapping
        Assert.assertEquals("SELECT \"CURRENT_DATE_1\", \"CURRENT_TIME_1\"",
                converter.convertSql("select CURRENT_DATE_1, CURRENT_TIME_1"));
        Assert.assertEquals("SELECT \"CURRENT_DATE_1\", TEST_CURR_TIME(), \"CURRENT_DATE\"",
                converter.convertSql("select CURRENT_DATE_1, CURRENT_TIME, CURRENT_DATE"));
        Assert.assertEquals("SELECT CAST(\"BYTE\" AS VAR(1024))\nFROM \"DEFAULT\".\"FACT\"",
                converter.convertSql("select cast(BYTE as VAR(1024)) from \"DEFAULT\".FACT"));
        Assert.assertEquals("SELECT CAST(\"PRICE\" AS DDD)\nFROM \"DEFAULT\".\"FACT\"",
                converter.convertSql("select cast(PRICE as DDD) from \"DEFAULT\".FACT"));
        Assert.assertEquals("SELECT A(), B(\"A\"), CAST(\"PRICE\" AS DDD)\nFROM \"DEFAULT\".\"FACT\"",
                converter.convertSql("select A(), B(A), cast(PRICE as DDD) from \"DEFAULT\".\"FACT\""));
        Assert.assertEquals("SELECT ONLY_DEFAULT(1)", converter.convertSql("SELECT ONLY_DEFAULT(1)"));

        // invalid case
        Assert.assertEquals("create table test(id int, price double, name string, value byte)",
                converter.convertSql("create table test(id int, price double, name string, value byte)"));
        Assert.assertEquals("select cast(BYTE as VARCHAR(1000000000000)) from \"DEFAULT\".FACT",
                converter.convertSql("select cast(BYTE as VARCHAR(1000000000000)) from \"DEFAULT\".FACT"));
        Assert.assertEquals("I am not a SQL", converter.convertSql("I am not a SQL"));
    }

    @Test
    public void testConvertSqlWithStrictLimitOffset() throws SQLException, SqlParseException {
        DataSourceDefProvider provider = DataSourceDefProvider.getInstance();
        ConvMaster master = new ConvMaster(provider.getDefault(), provider.getById(TEST_TARGET));
        SqlConverter converter = new SqlConverter(new SqlConverter.IConfigurer() {
            @Override
            public boolean useUppercaseDefault() {
                return true;
            }

            @Override
            public boolean enableCache() {
                return true;
            }

            @Override
            public boolean enableQuote() {
                return true;
            }

            @Override
            public String fixIdentifierCaseSensitive(String orig) {
                return orig;
            }

            @Override
            public String getTransformDatePattern() {
                return null;
            }
        }, master);

        Assert.assertEquals("SELECT 1\nORDER BY 2\nOFFSET 0 ROWS\nFETCH NEXT 1 ROWS ONLY",
                converter.convertSql("SELECT 1 ORDER BY 2 LIMIT 1"));
        Assert.assertEquals("SELECT 1\nORDER BY 1\nOFFSET 0 ROWS\nFETCH NEXT 1 ROWS ONLY",
                converter.convertSql("SELECT 1 ORDER BY 1 LIMIT 1"));
        Assert.assertEquals("SELECT 1\nORDER BY \"COL\"\nOFFSET 0 ROWS\nFETCH NEXT 1 ROWS ONLY",
                converter.convertSql("SELECT 1 ORDER BY COL LIMIT 1"));
        Assert.assertEquals("SELECT 1\nORDER BY 1\nOFFSET 0 ROWS\nFETCH NEXT 1 ROWS ONLY",
                converter.convertSql("SELECT 1 ORDER BY 1 LIMIT 0"));
        Assert.assertEquals("SELECT 1\nORDER BY 1\nOFFSET 1 ROWS\nFETCH NEXT 1 ROWS ONLY",
                converter.convertSql("SELECT 1 ORDER BY 1 LIMIT 1 OFFSET 1"));
        Assert.assertEquals("SELECT 1\nORDER BY 1\nOFFSET 0 ROWS\nFETCH NEXT 1 ROWS ONLY",
                converter.convertSql("SELECT 1 LIMIT 1"));
    }

    @Test
    public void testConvertQuotedSqlWithEscape() throws SQLException {
        DataSourceDefProvider provider = DataSourceDefProvider.getInstance();
        ConvMaster master = new ConvMaster(provider.getDefault(), provider.getById(TEST_TARGET));
        SqlConverter converter = new SqlConverter(new SqlConverter.IConfigurer() {
            @Override
            public boolean useUppercaseDefault() {
                return true;
            }

            @Override
            public boolean allowNoOffset() {
                return true;
            }

            @Override
            public boolean allowFetchNoRows() {
                return true;
            }

            @Override
            public boolean allowNoOrderByWithFetch() {
                return true;
            }

            @Override
            public boolean enableCache() {
                return true;
            }

            @Override
            public boolean enableQuote() {
                return true;
            }

            @Override
            public String getTransformDatePattern() {
                return null;
            }
        }, master);

        Assert.assertEquals("SELECT SUM(\"A\"), COUNT(\"A\") AS \"AB\"\nFROM \"DEFAULT\".\"CUBE\"",
                converter.convertSql("select sum(A), count(`A`) as AB from DEFAULT.`CUBE`"));
        Assert.assertEquals("SELECT A(), B(\"A\"), CAST(\"PRICE@@\" AS DDD)\nFROM \"DEFAULT\".\"CUBE\"",
                converter.convertSql("select A(), B(`A`), cast(`PRICE@@` as `DDD`) from DEFAULT.`CUBE`"));
        Assert.assertEquals("SELECT A(), B(\"A\"), CAST(\"PRICE@@\" AS DDD)\nFROM \"DEFAULT\".\"CUBE\"",
                converter.convertSql("select A(), B(\"A\"), cast(\"PRICE@@\" as \"DDD\") from \"DEFAULT\".\"CUBE\""));
        Assert.assertEquals(
                "SELECT \"kylin_sales\".\"price_@@\", \"kylin_sales\".\"count\"\nFROM \"cube\".\"kylin_sales\"\nWHERE \"kylin_sales\".\"price_@@\" > 1 AND \"kylin_sales\".\"count\" < 50",
                converter.convertSql(
                        "select `kylin_sales`.`price_@@`, `kylin_sales`.`count` from `cube`.`kylin_sales` where `kylin_sales`.`price_@@` > 1 and `kylin_sales`.`count` < 50"));
        Assert.assertEquals("SELECT COUNT(DISTINCT \"price_#@\")\nFROM \"cube\".\"kylin_sales\"",
                converter.convertSql("select count(distinct `price_#@`) from `cube`.`kylin_sales`"));
        Assert.assertEquals("SELECT COUNT(*)\n" + "FROM \"DEFAULT\".\"KYLIN_SALES\"",
                converter.convertSql("SELECT COUNT(*) FROM `DEFAULT`.`KYLIN_SALES`"));

    }

    @Test
    public void testConvertColumn() throws SQLException {
        DataSourceDefProvider provider = DataSourceDefProvider.getInstance();
        ConvMaster master = new ConvMaster(provider.getDefault(), provider.getById(TEST_TARGET));
        SqlConverter converter = new SqlConverter(new SqlConverter.IConfigurer() {

            @Override
            public boolean useUppercaseDefault() {
                return true;
            }

            @Override
            public boolean allowNoOffset() {
                return true;
            }

            @Override
            public boolean allowFetchNoRows() {
                return true;
            }

            @Override
            public boolean allowNoOrderByWithFetch() {
                return true;
            }

            @Override
            public boolean isCaseSensitive() {
                return true;
            }

            @Override
            public boolean enableCache() {
                return true;
            }

            @Override
            public boolean enableQuote() {
                return true;
            }

            @Override
            public String fixIdentifierCaseSensitive(String orig) {
                return orig.toUpperCase(Locale.ROOT);
            }

            @Override
            public String getTransformDateToStringExpression() {
                return null;
            }

            @Override
            public String getTransformDatePattern() {
                return null;
            }
        }, master);

        Assert.assertEquals("\"TEST\".\"AA\"", converter.convertColumn("`test`.`aa`", "`"));
        Assert.assertEquals("\"TEST\".\"AA\"", converter.convertColumn("`test`.aa", "`"));
        Assert.assertEquals("\"TEST\".\"AA\"", converter.convertColumn("test.aa", "`"));
    }

    @Test
    public void testConvertDate() throws SQLException {
        DataSourceDefProvider provider = DataSourceDefProvider.getInstance();
        ConvMaster master = new ConvMaster(provider.getDefault(), provider.getById(TEST_TARGET));
        SqlConverter converter = new SqlConverter(new SqlConverter.IConfigurer() {

            @Override
            public boolean useUppercaseDefault() {
                return true;
            }

            @Override
            public boolean allowNoOffset() {
                return true;
            }

            @Override
            public boolean allowFetchNoRows() {
                return true;
            }

            @Override
            public boolean allowNoOrderByWithFetch() {
                return true;
            }

            @Override
            public boolean isCaseSensitive() {
                return true;
            }

            @Override
            public boolean enableCache() {
                return true;
            }

            @Override
            public boolean enableQuote() {
                return true;
            }

            @Override
            public String fixIdentifierCaseSensitive(String orig) {
                return orig.toUpperCase(Locale.ROOT);
            }

            @Override
            public boolean enableTransformDateToString() {
                return true;
            }

            @Override
            public String getTransformDateToStringExpression() {
                return "to_char(%s,'%s')";
            }

            @Override
            public String getTransformDatePattern() {
                return "YYYY-MM-DD HH:mm:ss";
            }
        }, master);

        // date type column need to convert but without pattern, use format defined
        Assert.assertEquals("TO_CHAR(\"TEST\".\"AA\", 'YYYY-MM-DD HH:mm:ss')",
                converter.formatDateColumn("test.aa", new DataType("timestamp", 0, 0), ""));
        // date type column need to convert with defined Date pattern
        Assert.assertEquals("TO_CHAR(\"TEST\".\"AA\", 'xxxxx')",
                converter.formatDateColumn("test.aa", new DataType("timestamp", 0, 0), "xxxxx"));
        // non date type column
        Assert.assertEquals("\"TEST\".\"AA\"",
                converter.formatDateColumn("test.aa", new DataType("interger", 0, 0), ""));

        // date type column need to convert but without pattern, use format defined
        Assert.assertEquals(
                "SELECT \"AA\"\nFROM \"TABLEAA\" WHERE TO_CHAR(\"AA\", 'YYYY-MM-DD HH:mm:ss') < '2019-01-01'",
                converter.convertDateCondition("select aa from tableaa where aa < '2019-01-01'", "AA",
                        new DataType("timestamp", 0, 0), ""));

        // date type column need to convert with defined Date pattern
        Assert.assertEquals("SELECT \"AA\"\nFROM \"TABLEAA\" WHERE TO_CHAR(\"AA\", 'xxxxx') < '2019-01-01'",
                converter.convertDateCondition("select aa from tableaa where aa < '2019-01-01'", "AA",
                        new DataType("timestamp", 0, 0), "xxxxx"));

        // non date type column return origin sql
        Assert.assertEquals("select aa from tableaa where aa < '2019-01-01'",
                converter.convertDateCondition("select aa from tableaa where aa < '2019-01-01'", "AA",
                        new DataType("integer", 0, 0), "YYYY-MM-DD HH:mm:ss"));

        // complex where
        Assert.assertEquals(
                "SELECT \"AA\"\nFROM \"TABLEAA\" WHERE \"ASDF\" = 'aa' AND 1 = 1 AND (TO_CHAR(\"AA\", 'YYYY-MM-DD HH:mm:ss') < '2019-01-01' AND TO_CHAR(\"AA\", 'YYYY-MM-DD HH:mm:ss') > '2018-01-01')",
                converter.convertDateCondition(
                        "select aa from tableaa where asdf = 'aa' and 1 = 1 and (`AA` < '2019-01-01' and `AA` > '2018-01-01')",
                        "AA", new DataType("timestamp", 0, 0), ""));
    }

    @Test
    @SuppressWarnings("checkstyle:methodlength")
    public void testConvRownumSqlWriter() throws SQLException {
        DataSourceDefProvider provider = DataSourceDefProvider.getInstance();
        ConvMaster master = new ConvMaster(provider.getDefault(), provider.getById(TEST_TARGET));
        SqlConverter converter = new SqlConverter(new SqlConverter.IConfigurer() {

            @Override
            public boolean useUppercaseDefault() {
                return true;
            }

            @Override
            public SqlDialect getSqlDialect() {
                return SqlDialect.DatabaseProduct.ORACLE.getDialect();
            }

            @Override
            public boolean allowNoOffset() {
                return true;
            }

            @Override
            public boolean allowFetchNoRows() {
                return true;
            }

            @Override
            public boolean allowNoOrderByWithFetch() {
                return true;
            }

            @Override
            public String getPagingType() {
                return "ROWNUM";
            }

            @Override
            public boolean isCaseSensitive() {
                return true;
            }

            @Override
            public boolean enableCache() {
                return true;
            }

            @Override
            public boolean enableQuote() {
                return true;
            }

            @Override
            public String fixIdentifierCaseSensitive(String orig) {
                return orig.toUpperCase(Locale.ROOT);
            }

            @Override
            public boolean enableTransformDateToString() {
                return true;
            }

            @Override
            public String getTransformDateToStringExpression() {
                return "to_char(%s,'%s')";
            }

            @Override
            public String getTransformDatePattern() {
                return "YYYY-MM-DD HH:mm:ss";
            }
        }, master);

        // No limit test
        Assert.assertEquals("SELECT COUNT(\"A\") \"AB\"\n" + "FROM \"DEFAULT\".\"CUBE\"",
                converter.convertSql("select count(`A`) as AB from DEFAULT.`CUBE`"));

        // limit + offset test
        Assert.assertEquals(
                "SELECT * \n" + "FROM(\n" + "\tSELECT T.*, ROWNUM ROWNUM__1\n" + "\tFROM ( \n"
                        + "\t SELECT COUNT(\"A\") \"AB\"\n" + "FROM \"DEFAULT\".\"CUBE\"\n" + "\n"
                        + "\t) T WHERE ROWNUM <=  10  +  20  \n" + ") \n"
                        + "WHERE 1 = 1 AND  ROWNUM__1  BETWEEN  20  + 1 AND  20  +  10",
                converter.convertSql("select count(`A`) as AB from DEFAULT.`CUBE` LIMIT 10 OFFSET 20"));

        // limit test
        Assert.assertEquals(
                "SELECT * \n" + "FROM(\n" + "\tSELECT T.*, ROWNUM ROWNUM__1\n" + "\tFROM ( \n"
                        + "\t SELECT COUNT(\"A\") \"AB\"\n" + "FROM \"DEFAULT\".\"CUBE\"\n" + "\n"
                        + "\t) T WHERE ROWNUM <=  10  \n" + ") \n" + "WHERE 1 = 1 AND  ROWNUM__1  <=  10",
                converter.convertSql("select count(`A`) as AB from DEFAULT.`CUBE` LIMIT 10"));

        // No limit in with clause
        Assert.assertEquals(
                "WITH \"TMP\" AS (SELECT COUNT(\"A\") \"AB\"\n"
                        + "        FROM \"DEFAULT\".\"CUBE\") SELECT \"TMP\".\"A\"\n" + "    FROM \"TMP\"",
                converter.convertSql(
                        "with tmp as (select count(`A`) as AB from DEFAULT.`CUBE` ) select tmp.A from tmp "));

        // limit in with clause
        Assert.assertEquals(
                "WITH \"TMP\" AS ((SELECT * \n" + "FROM(\n" + "\tSELECT T.*, ROWNUM ROWNUM__1\n" + "\tFROM ( \n"
                        + "\t SELECT COUNT(\"A\") \"AB\"\n" + "            FROM \"DEFAULT\".\"CUBE\"\n"
                        + "            \n" + "\t) T WHERE ROWNUM <=  10  \n" + ") \n"
                        + "WHERE 1 = 1 AND  ROWNUM__1  <=  10)) SELECT \"TMP\".\"A\"\n" + "    FROM \"TMP\"",
                converter.convertSql(
                        "with tmp as (select count(`A`) as AB from DEFAULT.`CUBE` limit 10 ) select tmp.A from tmp "));

        // limit + offset in with clause
        Assert.assertEquals(
                "WITH \"TMP\" AS ((SELECT * \n" + "FROM(\n" + "\tSELECT T.*, ROWNUM ROWNUM__1\n" + "\tFROM ( \n"
                        + "\t SELECT COUNT(\"A\") \"AB\"\n" + "            FROM \"DEFAULT\".\"CUBE\"\n"
                        + "            \n" + "\t) T WHERE ROWNUM <=  10  +  20  \n" + ") \n"
                        + "WHERE 1 = 1 AND  ROWNUM__1  BETWEEN  20  + 1 AND  20  +  10)) SELECT \"TMP\".\"A\"\n"
                        + "    FROM \"TMP\"",
                converter.convertSql(
                        "with tmp as (select count(`A`) as AB from DEFAULT.`CUBE` limit 10 offset 20) select tmp.A from tmp "));

        // Both limit in with and select clause
        Assert.assertEquals("WITH \"TMP\" AS ((SELECT * \n" + "FROM(\n" + "\tSELECT T.*, ROWNUM ROWNUM__1\n"
                + "\tFROM ( \n" + "\t SELECT COUNT(\"A\") \"AB\"\n" + "            FROM \"DEFAULT\".\"CUBE\"\n"
                + "            \n" + "\t) T WHERE ROWNUM <=  10  \n" + ") \n"
                + "WHERE 1 = 1 AND  ROWNUM__1  <=  10)) SELECT * \n" + "FROM(\n" + "\tSELECT T.*, ROWNUM ROWNUM__2\n"
                + "\tFROM ( \n" + "\t SELECT \"TMP\".\"A\"\n" + "    FROM \"TMP\"\n" + "\n"
                + "\t) T WHERE ROWNUM <=  10  \n" + ") \n" + "WHERE 1 = 1 AND  ROWNUM__2  <=  10",
                converter.convertSql(
                        "with tmp as (select count(`A`) as AB from DEFAULT.`CUBE` limit 10 ) select tmp.A from tmp LIMIT 10  "));

        // nest-select both with order by
        Assert.assertEquals(
                "SELECT * \n" + "FROM(\n" + "\tSELECT T.*, ROWNUM ROWNUM__1\n" + "\tFROM ( \n" + "\t SELECT *\n"
                        + "FROM (SELECT * \n" + "FROM(\n" + "\tSELECT T.*, ROWNUM ROWNUM__2\n" + "\tFROM ( \n"
                        + "\t SELECT *\n" + "            FROM \"KYLIN\".\"KYLIN_ACCOUNT\"\n"
                        + "            ORDER BY \"ACCOUNT_ID\" DESC\n" + "            \n" + "\t) T\n" + ") )\n"
                        + "ORDER BY \"ACCOUNT_ID\"\n" + "\n" + "\t) T WHERE ROWNUM <=  10  \n" + ") \n"
                        + "WHERE 1 = 1 AND  ROWNUM__1  <=  10",
                converter.convertSql(
                        "select * from (select * from KYLIN.KYLIN_ACCOUNT order by ACCOUNT_ID desc) order by ACCOUNT_ID limit 10"));

        // nest-select outer with order by
        Assert.assertEquals(
                "SELECT * \n" + "FROM(\n" + "\tSELECT T.*, ROWNUM ROWNUM__1\n" + "\tFROM ( \n" + "\t SELECT *\n"
                        + "FROM (SELECT *\n" + "        FROM \"KYLIN\".\"KYLIN_ACCOUNT\")\n"
                        + "ORDER BY \"ACCOUNT_ID\"\n" + "\n" + "\t) T WHERE ROWNUM <=  10  \n" + ") \n"
                        + "WHERE 1 = 1 AND  ROWNUM__1  <=  10",
                converter.convertSql("select * from (select * from KYLIN.KYLIN_ACCOUNT) order by ACCOUNT_ID limit 10"));

        // nest-select inner with order by
        Assert.assertEquals(
                "SELECT * \n" + "FROM(\n" + "\tSELECT T.*, ROWNUM ROWNUM__1\n" + "\tFROM ( \n" + "\t SELECT *\n"
                        + "FROM (SELECT * \n" + "FROM(\n" + "\tSELECT T.*, ROWNUM ROWNUM__2\n" + "\tFROM ( \n"
                        + "\t SELECT *\n" + "            FROM \"KYLIN\".\"KYLIN_ACCOUNT\"\n"
                        + "            ORDER BY \"ACCOUNT_ID\" DESC\n" + "            \n" + "\t) T\n" + ") )\n" + "\n"
                        + "\t) T WHERE ROWNUM <=  10  \n" + ") \n" + "WHERE 1 = 1 AND  ROWNUM__1  <=  10",
                converter.convertSql(
                        "select * from (select * from KYLIN.KYLIN_ACCOUNT order by ACCOUNT_ID desc) limit 10"));

        // nest-select inner with limit
        Assert.assertEquals(
                "SELECT * \n" + "FROM(\n" + "\tSELECT T.*, ROWNUM ROWNUM__1\n" + "\tFROM ( \n" + "\t SELECT *\n"
                        + "FROM (SELECT * \n" + "FROM(\n" + "\tSELECT T.*, ROWNUM ROWNUM__2\n" + "\tFROM ( \n"
                        + "\t SELECT *\n" + "            FROM \"KYLIN\".\"KYLIN_ACCOUNT\"\n"
                        + "            ORDER BY \"ACCOUNT_ID\" DESC\n" + "            \n"
                        + "\t) T WHERE ROWNUM <=  10  \n" + ") \n" + "WHERE 1 = 1 AND  ROWNUM__2  <=  10)\n"
                        + "ORDER BY \"ACCOUNT_ID\"\n" + "\n" + "\t) T\n" + ") ",
                converter.convertSql(
                        "select * from (select * from KYLIN.KYLIN_ACCOUNT order by ACCOUNT_ID desc limit 10) order by ACCOUNT_ID "));
    }

    @Test
    public void testConvertSql_Mssql_OrderByFetchOffset() throws Exception {
        DataSourceDefProvider provider = DataSourceDefProvider.getInstance();
        ConvMaster master = new ConvMaster(provider.getDefault(), provider.getById(TEST_TARGET));
        SQLDWAdaptor sqlDWAdaptor = Mockito.mock(SQLDWAdaptor.class);
        Mockito.doCallRealMethod().when(sqlDWAdaptor).fixSql(Mockito.anyString());
        SqlConverter converter = new SqlConverter(new DefaultConfigurer(sqlDWAdaptor, new DataSourceDef()) {
            @Override
            public boolean skipHandleDefault() {
                return false;
            }

            @Override
            public boolean skipDefaultConvert() {
                return false;
            }

            @Override
            public boolean useUppercaseDefault() {
                return true;
            }

            @Override
            public String fixAfterDefaultConvert(String orig) {
                return super.fixAfterDefaultConvert(orig);
            }

            @Override
            public String getPagingType() {
                return "AUTO";
            }

            @Override
            public SqlDialect getSqlDialect() {
                return SqlDialect.DatabaseProduct.MSSQL.getDialect();
            }

            @Override
            public boolean isCaseSensitive() {
                return false;
            }

            @Override
            public boolean enableQuote() {
                return false;
            }

            @Override
            public boolean allowNoOffset() {
                return true;
            }

            @Override
            public boolean allowNoOrderByWithFetch() {
                return false;
            }

            @Override
            public boolean allowFetchNoRows() {
                return true;
            }
        }, master);

        Assert.assertEquals("SELECT *\n" + "FROM DBO.KYLIN_SALES\n" + "ORDER BY 1\n" + "OFFSET 0 ROWS \n"
                + "FETCH NEXT 500 ROWS ONLY", converter.convertSql("select * from DBO.KYLIN_SALES limit 500"));
    }
}

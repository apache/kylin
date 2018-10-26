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

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.sdk.datasource.framework.def.DataSourceDefProvider;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class SqlConverterTest extends LocalFileMetadataTestCase {
    private final static String TEST_TARGET = "testing";

    @BeforeClass
    public static void setUp() throws Exception {
        staticCreateTestMetadata();
    }

    @AfterClass
    public static void after() throws Exception {
        staticCleanupTestMetadata();
    }

    @Test
    public void testConvertSqlWithoutEscape() throws SQLException, SqlParseException {
        DataSourceDefProvider provider = DataSourceDefProvider.getInstance();
        ConvMaster master = new ConvMaster(provider.getDefault(), provider.getById(TEST_TARGET));
        SqlConverter converter = new SqlConverter(new SqlConverter.IConfigurer() {
            @Override
            public boolean skipDefaultConvert() {
                return false;
            }

            @Override
            public boolean skipHandleDefault() {
                return true;
            }

            @Override
            public boolean useUppercaseDefault() {
                return true;
            }

            @Override
            public String fixAfterDefaultConvert(String orig) {
                return orig;
            }

            @Override
            public SqlDialect getSqlDialect() throws SQLException {
                return SqlDialect.CALCITE;
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
                return "AUTO";
            }

            @Override
            public boolean isCaseSensitive() {
                return false;
            }

            @Override
            public boolean enableCache() {
                return true;
            }
        }, master);

        // escape default keywords
        Assert.assertEquals("SELECT *\nFROM DEFAULT.FACT", converter.convertSql("select * from \"DEFAULT\".FACT"));
        Assert.assertEquals("SELECT *\nFROM \"Default\".FACT", converter.convertSql("select * from \"Default\".FACT"));
        Assert.assertEquals("SELECT *\nFROM \"default\".FACT", converter.convertSql("select * from \"default\".FACT"));
    }

    @Test
    public void testConvertSqlWithEscape() throws SQLException, SqlParseException {
        DataSourceDefProvider provider = DataSourceDefProvider.getInstance();
        ConvMaster master = new ConvMaster(provider.getDefault(), provider.getById(TEST_TARGET));
        SqlConverter converter = new SqlConverter(new SqlConverter.IConfigurer() {

            @Override
            public boolean skipDefaultConvert() {
                return false;
            }

            @Override
            public boolean skipHandleDefault() {
                return false;
            }

            @Override
            public boolean useUppercaseDefault() {
                return true;
            }

            @Override
            public String fixAfterDefaultConvert(String orig) {
                return orig;
            }

            @Override
            public SqlDialect getSqlDialect() throws SQLException {
                return SqlDialect.CALCITE;
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
                return "AUTO";
            }

            @Override
            public boolean isCaseSensitive() {
                return false;
            }

            @Override
            public boolean enableCache() {
                return true;
            }
        }, master);

        // normal cases
        Assert.assertEquals("SELECT 1", converter.convertSql("select     1"));
        Assert.assertEquals("SELECT *\nFROM FACT", converter.convertSql("select * from FACT"));

        // limit and offset
        Assert.assertEquals("SELECT 1\nFETCH NEXT 1 ROWS ONLY", converter.convertSql("SELECT 1 LIMIT 1"));
        Assert.assertEquals("SELECT 1\nFETCH NEXT 0 ROWS ONLY", converter.convertSql("SELECT 1 LIMIT 0"));
        Assert.assertEquals("SELECT 1\nOFFSET 1 ROWS\nFETCH NEXT 1 ROWS ONLY",
                converter.convertSql("SELECT 1 LIMIT 1 OFFSET 1"));

        // escape default keywords
        Assert.assertEquals("SELECT *\nFROM \"DEFAULT\".FACT", converter.convertSql("select * from DEFAULT.FACT"));
        Assert.assertEquals("SELECT *\nFROM \"DEFAULT\".FACT", converter.convertSql("select * from \"DEFAULT\".FACT"));
        Assert.assertEquals("SELECT *\nFROM \"DEFAULT\".FACT", converter.convertSql("select * from \"Default\".FACT"));
        Assert.assertEquals("SELECT *\nFROM \"DEFAULT\".FACT", converter.convertSql("select * from \"default\".FACT"));

        // function mapping
        Assert.assertEquals("SELECT EXTRACT(DOY FROM PART_DT)\nFROM \"DEFAULT\".FACT",
                converter.convertSql("select DAYOFYEAR(PART_DT) from \"DEFAULT\".FACT"));
        Assert.assertEquals(
                "SELECT 12 * (EXTRACT(YEAR FROM DT1) - EXTRACT(YEAR FROM DT2)) + EXTRACT(MONTH FROM DT1) - EXTRACT(MONTH FROM DT2) "
                        + "- CASE WHEN EXTRACT(DAY FROM DT2) > EXTRACT(DAY FROM DT1) THEN 1 ELSE 0 END\n"
                        + "FROM \"DEFAULT\".FACT",
                converter.convertSql("select TIMESTAMPDIFF(month,DT2,      DT1) from \"DEFAULT\".FACT"));
        Assert.assertEquals("SELECT TRUNC(ID)\nFROM \"DEFAULT\".FACT",
                converter.convertSql("select cast(ID as INT) from \"DEFAULT\".FACT"));
        Assert.assertEquals("SELECT 1\nFROM A\nWHERE 1 BETWEEN ASYMMETRIC 0 AND 2",
                converter.convertSql("select 1 from a where 1 BETWEEN 0 and 2"));
        Assert.assertEquals("SELECT CURRENT_DATE, TEST_CURR_TIME()",
                converter.convertSql("select CURRENT_DATE, CURRENT_TIME"));
        Assert.assertEquals("SELECT EXP(AVG(LN(EXTRACT(DOY FROM CAST('2018-03-20' AS DATE)))))\nFROM \"DEFAULT\".FACT",
                converter.convertSql(
                        "select exp(avg(ln(dayofyear(cast('2018-03-20' as date))))) from \"DEFAULT\".FACT"));

        // over function
        Assert.assertEquals("SELECT STDDEVP(C1) OVER (ORDER BY C1)\nFROM TEST_SUITE\nFETCH NEXT 1 ROWS ONLY",
                converter.convertSql("select stddev_pop(c1) over(order by c1) from test_suite limit 1"));

        // type mapping
        Assert.assertEquals("SELECT CAST(PRICE AS DOUBLE PRECISION)\nFROM \"DEFAULT\".FACT",
                converter.convertSql("select cast(PRICE as DOUBLE) from \"DEFAULT\".FACT"));
        Assert.assertEquals("SELECT CAST(PRICE AS DECIMAL(19, 4))\nFROM \"DEFAULT\".FACT",
                converter.convertSql("select cast(PRICE as DECIMAL(19,4)) from \"DEFAULT\".FACT"));
        Assert.assertEquals("SELECT CAST(PRICE AS DECIMAL(19))\nFROM \"DEFAULT\".FACT",
                converter.convertSql("select cast(PRICE as DECIMAL(19)) from \"DEFAULT\".FACT"));
        Assert.assertEquals("SELECT CAST(BYTE AS BIT(8))\nFROM \"DEFAULT\".FACT",
                converter.convertSql("select cast(BYTE as BYTE) from \"DEFAULT\".FACT"));
        Assert.assertEquals("SELECT CAST(BYTE AS VARCHAR(1024))\nFROM \"DEFAULT\".FACT",
                converter.convertSql("select cast(BYTE as VARCHAR(1024)) from \"DEFAULT\".FACT"));

        // cannot find mapping
        Assert.assertEquals("SELECT CURRENT_DATE_1, CURRENT_TIME_1",
                converter.convertSql("select CURRENT_DATE_1, CURRENT_TIME_1"));
        Assert.assertEquals("SELECT CURRENT_DATE_1, TEST_CURR_TIME(), CURRENT_DATE",
                converter.convertSql("select CURRENT_DATE_1, CURRENT_TIME, CURRENT_DATE"));
        Assert.assertEquals("SELECT CAST(BYTE AS VAR(1024))\nFROM \"DEFAULT\".FACT",
                converter.convertSql("select cast(BYTE as VAR(1024)) from \"DEFAULT\".FACT"));
        Assert.assertEquals("SELECT CAST(PRICE AS DDD)\nFROM \"DEFAULT\".FACT",
                converter.convertSql("select cast(PRICE as DDD) from \"DEFAULT\".FACT"));
        Assert.assertEquals("SELECT A(), B(A), CAST(PRICE AS DDD)\nFROM \"DEFAULT\".FACT",
                converter.convertSql("select A(), B(A), cast(PRICE as DDD) from \"DEFAULT\".FACT"));
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
            public boolean skipDefaultConvert() {
                return false;
            }

            @Override
            public boolean skipHandleDefault() {
                return false;
            }

            @Override
            public boolean useUppercaseDefault() {
                return true;
            }

            @Override
            public String fixAfterDefaultConvert(String orig) {
                return orig;
            }

            @Override
            public SqlDialect getSqlDialect() throws SQLException {
                return SqlDialect.CALCITE;
            }

            @Override
            public boolean allowNoOffset() {
                return false;
            }

            @Override
            public boolean allowFetchNoRows() {
                return false;
            }

            @Override
            public boolean allowNoOrderByWithFetch() {
                return false;
            }

            @Override
            public String getPagingType() {
                return "AUTO";
            }

            @Override
            public boolean isCaseSensitive() {
                return false;
            }

            @Override
            public boolean enableCache() {
                return true;
            }
        }, master);

        Assert.assertEquals("SELECT 1\nORDER BY 2\nOFFSET 0 ROWS\nFETCH NEXT 1 ROWS ONLY",
                converter.convertSql("SELECT 1 ORDER BY 2 LIMIT 1"));
        Assert.assertEquals("SELECT 1\nORDER BY 1\nOFFSET 0 ROWS\nFETCH NEXT 1 ROWS ONLY",
                converter.convertSql("SELECT 1 ORDER BY 1 LIMIT 1"));
        Assert.assertEquals("SELECT 1\nORDER BY COL\nOFFSET 0 ROWS\nFETCH NEXT 1 ROWS ONLY",
                converter.convertSql("SELECT 1 ORDER BY COL LIMIT 1"));
        Assert.assertEquals("SELECT 1\nORDER BY 1\nOFFSET 0 ROWS\nFETCH NEXT 1 ROWS ONLY",
                converter.convertSql("SELECT 1 ORDER BY 1 LIMIT 0"));
        Assert.assertEquals("SELECT 1\nORDER BY 1\nOFFSET 1 ROWS\nFETCH NEXT 1 ROWS ONLY",
                converter.convertSql("SELECT 1 ORDER BY 1 LIMIT 1 OFFSET 1"));
        Assert.assertEquals("SELECT 1\nORDER BY 1\nOFFSET 0 ROWS\nFETCH NEXT 1 ROWS ONLY",
                converter.convertSql("SELECT 1 LIMIT 1"));
    }
}

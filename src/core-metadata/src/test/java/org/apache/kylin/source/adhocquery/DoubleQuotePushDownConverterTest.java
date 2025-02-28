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
package org.apache.kylin.source.adhocquery;

import java.util.Arrays;
import java.util.List;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DoubleQuotePushDownConverterTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() {
        this.createTestMetadata();
    }

    @After
    public void after() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testConvertDoubleQuoteSuccess() {
        List<List<String>> convertSuccessUTs = Arrays.asList(
                //[0] is input,[1] is expect result

                //test for blank space with Identifier
                Arrays.asList("select atbale .a as \"ACOL\",atbale. b as Bcol,\"c\" as \"acol\" from atbale",
                        "select \"ATBALE\".\"A\" as \"ACOL\",\"ATBALE\".\"B\" as \"BCOL\",\"c\" as \"acol\" from \"ATBALE\""),
                Arrays.asList("select a as \"ACOL\",b as Bcol,\"c\" as \"acol\" from atbale",
                        "select \"A\" as \"ACOL\",\"B\" as \"BCOL\",\"c\" as \"acol\" from \"ATBALE\""),
                //test for with as...(select ....)
                Arrays.asList(
                        "WITH 中文tb AS (\n" + "\t\tSELECT *\n" + "\t\tFROM abc\n" + "\t)\n" + "SELECT *\n" + "FROM t1",
                        "WITH \"中文TB\" AS (\n" + "\t\tSELECT *\n" + "\t\tFROM \"ABC\"\n" + "\t)\n" + "SELECT *\n"
                                + "FROM \"T1\""),
                //test for with as...(union all)
                Arrays.asList(
                        "WITH t1 AS (\n" + "\t\tSELECT *\n" + "\t\tFROM abc\n" + "\t\tUNION ALL\n" + "\t\tSELECT *\n"
                                + "\t\tFROM dfs\n" + "\t)\n" + "SELECT *\n" + "FROM t1\n" + "LIMIT 500",
                        "WITH \"T1\" AS (\n" + "\t\tSELECT *\n" + "\t\tFROM \"ABC\"\n" + "\t\tUNION ALL\n"
                                + "\t\tSELECT *\n" + "\t\tFROM \"DFS\"\n" + "\t)\n" + "SELECT *\n" + "FROM \"T1\"\n"
                                + "LIMIT 500"),

                //test for select * ...
                Arrays.asList("select a.* from a", "select \"A\".* from \"A\""),
                Arrays.asList("select 中文表.* from 中文表", "select \"中文表\".* from \"中文表\""),
                Arrays.asList("select 中文表 . * from 中文表", "select \"中文表\".* from \"中文表\""),

                //test chinese column
                Arrays.asList("select a.中文列 from a", "select \"A\".\"中文列\" from \"A\""),
                //test for unquoted and limit
                Arrays.asList(
                        "SELECT ACCOUNT_ID AS 中文id, ACCOUNT_COUNTRY AS country\n" + "FROM \"DEFAULT\".TEST_ACCOUNT\n"
                                + "WHERE ACCOUNT_COUNTRY = 'RU'\n" + "LIMIT 500",
                        "SELECT \"ACCOUNT_ID\" AS \"中文ID\", \"ACCOUNT_COUNTRY\" AS \"COUNTRY\"\n"
                                + "FROM \"DEFAULT\".\"TEST_ACCOUNT\"\n" + "WHERE \"ACCOUNT_COUNTRY\" = 'RU'\n"
                                + "LIMIT 500"),
                //test for quoted
                Arrays.asList(
                        "select ACCOUNT_ID as \"中文id\", ACCOUNT_COUNTRY as \"country\" from  \"DEFAULT\".TEST_ACCOUNT where ACCOUNT_COUNTRY='RU'",
                        "select \"ACCOUNT_ID\" as \"中文id\", \"ACCOUNT_COUNTRY\" as \"country\" from  \"DEFAULT\".\"TEST_ACCOUNT\" where \"ACCOUNT_COUNTRY\"='RU'"),
                //test for unchanged for  all expressions
                Arrays.asList("select (a + b) * c", "select (\"A\" + \"B\") * \"C\""),
                Arrays.asList("select a + b * c", "select \"A\" + \"B\" * \"C\""),
                Arrays.asList("select 1 + b * c", "select 1 + \"B\" * \"C\""),

                //for filter the function without parentheses
                Arrays.asList(
                        "select CURRENT_CATALOG, CURRENT_DATE, CURRENT_PATH,CURRENT_ROLE, CURRENT_SCHEMA, CURRENT_TIME, CURRENT_TIMESTAMP, CURRENT_USER,LOCALTIME, LOCALTIMESTAMP, SESSION_USER, SYSTEM_USER, USER",
                        "select CURRENT_CATALOG, CURRENT_DATE, CURRENT_PATH,CURRENT_ROLE, CURRENT_SCHEMA, CURRENT_TIME, CURRENT_TIMESTAMP, CURRENT_USER,LOCALTIME, LOCALTIMESTAMP, SESSION_USER, SYSTEM_USER, USER"),
                //filter the function without parentheses, test for lowercase or uppercase
                Arrays.asList("select CUrrENT_DATE     ,     current_role     , a,1+1",
                        "select CUrrENT_DATE     ,     current_role     , \"A\",1+1"));
        convertSuccessUTs.forEach(this::testConvertSuccess);

    }

    @Test
    public void convertBackTickQuoteNormal() {
        String sql = "select ACCOUNT_ID as 中文id, ACCOUNT_COUNTRY as `country` from  `DEFAULT`.TEST_ACCOUNT where ACCOUNT_COUNTRY='RU'";
        String expected = "select \"ACCOUNT_ID\" as \"中文ID\", \"ACCOUNT_COUNTRY\" as \"country\" from  \"DEFAULT\".\"TEST_ACCOUNT\" where \"ACCOUNT_COUNTRY\"='RU'";
        final String real = DoubleQuotePushDownConverter.convertDoubleQuote(sql);
        Assert.assertEquals(expected, real);

    }

    @Test
    public void testConvertDoubleQuoteFailure() {
        // wrong grammar sql
        String sql = "select ACCOUNT_ID as 中文id, ACCOUNT_COUNTRY as country from1  \"DEFAULT\".TEST_ACCOUNT where ACCOUNT_COUNTRY='RU'";
        testConvertFailure(sql);

    }

    @Test
    public void testConvertWithSqlHint() {
        // Root node after calcite parsed is SqlOrderBy
        {
            String sql = "select /*+ REPARTITION(2), AAABBB(1,2,3) */ ACCOUNT_ID, REPARTITION from `DEFAULT`.TEST_ACCOUNT limit 1";
            String convertSql = DoubleQuotePushDownConverter.convertDoubleQuote(sql);
            String expectedSql = "select /*+ REPARTITION(2), AAABBB(1,2,3) */ \"ACCOUNT_ID\", \"REPARTITION\" from \"DEFAULT\".\"TEST_ACCOUNT\" limit 1";
            Assert.assertEquals(expectedSql, convertSql);
        }

        // Root node after calcite parsed is SqlSelect
        {
            String sql = "select /*+ REPARTITION(2), AAABBB(1,2,3) */ ACCOUNT_ID, REPARTITION from `DEFAULT`.TEST_ACCOUNT";
            String convertSql = DoubleQuotePushDownConverter.convertDoubleQuote(sql);
            String expectedSql = "select /*+ REPARTITION(2), AAABBB(1,2,3) */ \"ACCOUNT_ID\", \"REPARTITION\" from \"DEFAULT\".\"TEST_ACCOUNT\"";
            Assert.assertEquals(expectedSql, convertSql);
        }
    }

    private void testConvertSuccess(List<String> inputArrayList) {
        String convertSql = DoubleQuotePushDownConverter.convertDoubleQuote(inputArrayList.get(0));
        String expectedSql = inputArrayList.get(1);
        Assert.assertEquals(convertSql, expectedSql);
    }

    private void testConvertFailure(String inputSql) {
        String convertSql = DoubleQuotePushDownConverter.convertDoubleQuote(inputSql);
        Assert.assertEquals(convertSql, inputSql);
    }
}

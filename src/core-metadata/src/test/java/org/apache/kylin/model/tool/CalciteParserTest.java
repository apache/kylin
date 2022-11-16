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

package org.apache.kylin.model.tool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.metadata.project.NProjectManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.base.Preconditions;

public class CalciteParserTest extends NLocalFileMetadataTestCase {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Before
    public void setUp() {
        this.createTestMetadata();
    }

    @After
    public void after() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testNoTableNameExists() throws SqlParseException {
        String expr1 = "a + b";
        assertEquals("x.a + x.b", CalciteParser.insertAliasInExpr(expr1, "x"));

        String expr2 = "a + year(b)";
        assertEquals("x.a + year(x.b)", CalciteParser.insertAliasInExpr(expr2, "x"));

        String expr3 = "a + hiveudf(b)";
        assertEquals("x.a + hiveudf(x.b)", CalciteParser.insertAliasInExpr(expr3, "x"));
    }

    @Test
    public void testTableNameExists1() throws SqlParseException {
        String expr1 = "a + x.b";

        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("SqlIdentifier X.B contains DB/Table name");
        CalciteParser.insertAliasInExpr(expr1, "x");
    }

    @Test
    public void testTableNameExists2() throws SqlParseException {
        String expr1 = "a + year(x.b)";

        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("SqlIdentifier X.B contains DB/Table name");
        CalciteParser.insertAliasInExpr(expr1, "x");
    }

    @Test
    public void testTableNameExists3() throws SqlParseException {
        String expr1 = "a + hiveudf(x.b)";

        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("SqlIdentifier X.B contains DB/Table name");
        CalciteParser.insertAliasInExpr(expr1, "x");
    }

    @Test
    public void testCaseWhen() {
        String expr = "(CASE LSTG_FORMAT_NAME  WHEN 'Auction' THEN 'x'  WHEN 'y' THEN '222' ELSE 'z' END)";
        String alias = "TEST_KYLIN_FACT";
        String s = CalciteParser.insertAliasInExpr(expr, alias);
        System.out.println(s);
        assertEquals(
                "(CASE TEST_KYLIN_FACT.LSTG_FORMAT_NAME  WHEN 'Auction' THEN 'x'  WHEN 'y' THEN '222' ELSE 'z' END)",
                s);
    }

    @Test
    public void testPos() throws SqlParseException {
        String[] sqls = new String[] { "select \n a \n + \n b \n from t", //
                "select\na\n+\nb\nfrom t", //
                "select \r\n a \r\n + \r\n b \r\n from t", //
                "select\r\na\r\n+\r\nb\r\nfrom t" };

        for (String sql : sqls) {
            SqlNode parse = ((SqlSelect) CalciteParser.parse(sql)).getSelectList().get(0);
            Pair<Integer, Integer> replacePos = CalciteParser.getReplacePos(parse, sql);
            String substring = sql.substring(replacePos.getFirst(), replacePos.getSecond());
            Preconditions.checkArgument(substring.startsWith("a"));
            Preconditions.checkArgument(substring.endsWith("b"));
        }

    }

    @Test
    public void testLikeClausePos() throws SqlParseException {
        String sql = "select gender from employee where name like '%berg'";

        SqlNode parse = ((SqlSelect) CalciteParser.parse(sql)).getWhere();
        Assert.assertTrue(parse instanceof SqlBasicCall);

        SqlOperator operator = ((SqlBasicCall) parse).getOperator();
        Assert.assertEquals(SqlKind.LIKE, operator.getKind());

        SqlParserPos pos = parse.getParserPosition();
        Assert.assertNotEquals(SqlParserPos.ZERO, pos);

        Pair<Integer, Integer> replacePos = CalciteParser.getReplacePos(parse, sql);
        String substring = sql.substring(replacePos.getFirst(), replacePos.getSecond());
        Preconditions.checkArgument(substring.startsWith("name"));
        Preconditions.checkArgument(substring.endsWith("'%berg'"));
    }

    @Test
    public void testPosWithBrackets() throws SqlParseException {
        String[] sqls = new String[] { "select (   a + b) * (c+ d     ) from t", "select (a+b) * (c+d) from t",
                "select (a + b) * (c+ d) from t", "select (a+b) * (c+d) from t", };

        for (String sql : sqls) {
            SqlNode parse = ((SqlSelect) CalciteParser.parse(sql)).getSelectList().get(0);
            Pair<Integer, Integer> replacePos = CalciteParser.getReplacePos(parse, sql);
            String substring = sql.substring(replacePos.getFirst(), replacePos.getSecond());
            Preconditions.checkArgument(substring.startsWith("("));
            Preconditions.checkArgument(substring.endsWith(")"));
        }
    }

    @Test
    public void testPosWithBracketsInConstant() throws SqlParseException {
        String[] sqls = new String[] { "select '(   a + b) * (c+ d     ' from t", };

        for (String sql : sqls) {
            SqlNode parse = ((SqlSelect) CalciteParser.parse(sql)).getSelectList().get(0);
            Pair<Integer, Integer> replacePos = CalciteParser.getReplacePos(parse, sql);
            String substring = sql.substring(replacePos.getFirst(), replacePos.getSecond());
            Preconditions.checkArgument(substring.startsWith("'"));
        }
    }

    @Test
    public void testPosWithBracketsInAlias() throws SqlParseException {
        String sql = "select a as \"(   a + b) * (c+ d）     \" from (select a,b,c from t)";

        SqlNode parse = ((SqlSelect) CalciteParser.parse(sql)).getSelectList().get(0);
        Pair<Integer, Integer> replacePos = CalciteParser.getReplacePos(parse, sql);
        String substring = sql.substring(replacePos.getFirst(), replacePos.getSecond());
        Assert.assertEquals("a as \"(   a + b) * (c+ d）     \"", substring);
    }

    @Test
    public void testTransformDoubleQuote() throws SqlParseException {
        String expr = "`ABC`.`CBA` + 1";
        Assert.assertEquals(expr.replace("`", "\""), CalciteParser.transformDoubleQuote(expr));
    }

    @Test
    public void testRowExpression() {
        String sql = "SELECT 'LO_LINENUMBER', 'LO_SUPPKEY' FROM \"SSB\".\"P_LINEORDER\" WHERE ROW('LO_ORDERKEY', 'LO_CUSTKEY') IN (ROW(123, 234), ROW(321, 432)) GROUP BY 'LO_LINENUMBER', 'LO_SUPPKEY'";
        try {
            CalciteParser.parse(sql);
        } catch (SqlParseException e) {
            fail("can't parse row construction");
        }
    }

    @Test
    public void testQueryParseCaseSensitive() throws Throwable {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.source.name-case-sensitive-enabled", "true");
        final String[] select_sqls = {
                "select count(1), TEST_ACCOUNT.account_buyer_level from TEST_KYLIN_FACT inner join TEST_ACCOUNT on TEST_KYLIN_FACT.SELLER_ID = TEST_ACCOUNT.ACCOUNT_ID group by TEST_ACCOUNT.account_buyer_level",
                "select count(1), TEST_ACCOUNT.ACCOUNT_BUYER_LEVEL from TEST_KYLIN_FACT inner join TEST_ACCOUNT on TEST_KYLIN_FACT.SELLER_ID = TEST_ACCOUNT.ACCOUNT_ID group by TEST_ACCOUNT.ACCOUNT_BUYER_LEVEL" };
        final String[] select_columns = { "account_buyer_level", "ACCOUNT_BUYER_LEVEL" };
        for (int i = 0; i < select_sqls.length; i++) {
            SqlNode sqlNode = CalciteParser.parse(select_sqls[i]);
            Assert.assertTrue(sqlNode.toString().contains(select_columns[i]));
        }
    }

    @Test
    public void testQueryParseProjectCaseSensitive() throws Throwable {
        final String project = "default";
        KylinConfig config = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(project)
                .getConfig();
        config.setProperty("kylin.source.name-case-sensitive-enabled", "true");
        final String[] select_sqls = {
                "select count(1), TEST_ACCOUNT.account_buyer_level from TEST_KYLIN_FACT inner join TEST_ACCOUNT on TEST_KYLIN_FACT.SELLER_ID = TEST_ACCOUNT.ACCOUNT_ID group by TEST_ACCOUNT.account_buyer_level",
                "select count(1), TEST_ACCOUNT.ACCOUNT_BUYER_LEVEL from TEST_KYLIN_FACT inner join TEST_ACCOUNT on TEST_KYLIN_FACT.SELLER_ID = TEST_ACCOUNT.ACCOUNT_ID group by TEST_ACCOUNT.ACCOUNT_BUYER_LEVEL" };
        final String[] select_columns = { "account_buyer_level", "ACCOUNT_BUYER_LEVEL" };
        for (int i = 0; i < select_sqls.length; i++) {
            SqlNode sqlNode = CalciteParser.parse(select_sqls[i]);
            Assert.assertTrue(sqlNode.toString().contains(select_columns[i]));
        }
    }

    @Test
    public void testQueryParseCaseNotSensitive() throws Throwable {
        final String[] select_sqls = {
                "select count(1), TEST_ACCOUNT.account_buyer_level from TEST_KYLIN_FACT inner join TEST_ACCOUNT on TEST_KYLIN_FACT.SELLER_ID = TEST_ACCOUNT.ACCOUNT_ID group by TEST_ACCOUNT.account_buyer_level",
                "select count(1), TEST_ACCOUNT.ACCOUNT_BUYER_LEVEL from TEST_KYLIN_FACT inner join TEST_ACCOUNT on TEST_KYLIN_FACT.SELLER_ID = TEST_ACCOUNT.ACCOUNT_ID group by TEST_ACCOUNT.ACCOUNT_BUYER_LEVEL" };
        final String[] select_columns = { "ACCOUNT_BUYER_LEVEL", "ACCOUNT_BUYER_LEVEL" };
        for (int i = 0; i < select_sqls.length; i++) {
            SqlNode sqlNode = CalciteParser.parse(select_sqls[i]);
            Assert.assertTrue(sqlNode.toString().contains(select_columns[i]));
        }
    }

}

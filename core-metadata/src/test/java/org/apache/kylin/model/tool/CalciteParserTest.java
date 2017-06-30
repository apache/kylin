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

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.base.Preconditions;

public class CalciteParserTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

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
    public void testCasewhen() {
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
            String[] lines = sql.split("\n");
            Pair<Integer, Integer> replacePos = CalciteParser.getReplacePos(parse, lines);
            String substring = sql.substring(replacePos.getLeft(), replacePos.getRight());
            Preconditions.checkArgument(substring.startsWith("a"));
            Preconditions.checkArgument(substring.endsWith("b"));
        }

    }

    @Test
    public void testEqual() throws SqlParseException {
        String sql0 = "select a.a + a.b + a.c from t as a";
        String sql1 = "select (((a . a +    a.b +    a.c))) from t as a";
        String sql2 = "select (a + b) + c  from t";
        String sql3 = "select a.a + (a.b + a.c) from t as a";

        SqlNode sn0 = CalciteParser.getOnlySelectNode(sql0);
        SqlNode sn1 = CalciteParser.getOnlySelectNode(sql1);
        SqlNode sn2 = CalciteParser.getOnlySelectNode(sql2);
        SqlNode sn3 = CalciteParser.getOnlySelectNode(sql3);

        assertEquals(true, CalciteParser.isNodeEqual(sn0, sn1));
        assertEquals(true, CalciteParser.isNodeEqual(sn0, sn2));
        assertEquals(false, CalciteParser.isNodeEqual(sn0, sn3));
    }
}

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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.LinkedHashMap;
import java.util.List;

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.collect.BiMap;
import org.apache.kylin.guava30.shaded.common.collect.HashBiMap;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.query.relnode.ColumnRowType;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@MetadataInfo
public class ImplicitCCTest {

    private final ConvertToComputedColumn converter = new ConvertToComputedColumn();

    private ComputedColumnDesc mockComputedColumnDesc(String name, String expr, String tableAlias) {
        ComputedColumnDesc mockedCC = Mockito.mock(ComputedColumnDesc.class, invocation -> {
            throw new RuntimeException(invocation.getMethod().getName() + " is not stubbed");
        });
        Mockito.doReturn(name).when(mockedCC).getColumnName();
        Mockito.doReturn(expr).when(mockedCC).getExpression();
        Mockito.doReturn(null).when(mockedCC).getInnerExpression();
        Mockito.doReturn(tableAlias).when(mockedCC).getTableAlias();

        return mockedCC;
    }

    @Test
    public void testReplaceComputedColumn() throws SqlParseException {

        String sql0 = "select (t1 . a + t1.b + t1.c) as c, substring(substring(d,1,3),1,3) as z from table1 as t1 "
                + "group by t1.a+   t1.b +     t1.c, d having t1.a+t1.b+t1.c > 100 order by t1.a +t1.b +t1.c";
        //String sql0 = "select (\"t1\" . \"a\" + t1.b + t1.c) as c, substring(substring(d,1,3),1,3) as z " +
        // "from table1 as t1 group by t1.a+   t1.b +     t1.c having t1.a+t1.b+t1.c > 100 order by t1.a +t1.b +t1.c";
        String sql1 = "select sum(cast(a as bigint)) from table1 as t1";
        String sql2 = "select substring(substring(t1.d,1,3),1,3) from table1 as t1";
        String sql3 = "select a + b + (c+d   \t\n) from table1";
        String sql4 = "select sum(\"0910_a\" * \"0910_b\"), c from \"0910_table3\" group by c";

        List<ComputedColumnDesc> mockCCs = Lists.newArrayList(
                mockComputedColumnDesc("cc0", "table1.a + table1.b + table1.c", "TABLE1"),
                mockComputedColumnDesc("cc1", "cast(table1.a as bigint)", "TABLE1"), //
                mockComputedColumnDesc("cc2", "table1.a + table1.b", "TABLE1"),
                mockComputedColumnDesc("cc3", "table2.c + table2.d", "TABLE2"),
                mockComputedColumnDesc("cc", "substring(substring(table1.d,1,3),1,3)", "TABLE1"),
                mockComputedColumnDesc("cc4", "(table1.a + table1.b) + (table1.c + table1.d)", "TABLE1"),
                mockComputedColumnDesc("cc5", "CAST(table1.a AS double)", "TABLE1"), mockComputedColumnDesc("cc6",
                        "\"0910_TABLE3\".\"0910_A\" * \"0910_TABLE3\".\"0910_B\"", "0910_TABLE3"));
        mockCCs = ConvertToComputedColumn.getCCListSortByLength(mockCCs);

        BiMap<String, String> aliasMapping = HashBiMap.create();
        aliasMapping.put("T1", "TABLE1");
        aliasMapping.put("T2", "TABLE2");
        aliasMapping.put("0910_TABLE3", "0910_TABLE3");

        ColumnRowType columnRowType1 = ColumnRowTypeMockUtil.mock("TABLE1", "T1",
                ImmutableList.of(Pair.newPair("A", "integer"), //
                        Pair.newPair("B", "integer"), //
                        Pair.newPair("C", "integer"), //
                        Pair.newPair("D", "integer")));

        LinkedHashMap<String, ColumnRowType> mockQueryAlias = Maps.newLinkedHashMap();
        mockQueryAlias.put("TABLE1", columnRowType1);

        QueryAliasMatchInfo queryAliasMatchInfo = new QueryAliasMatchInfo(aliasMapping, mockQueryAlias);

        assertEquals(
                "select (\"T1\".\"cc0\") as c, substring(substring(d,1,3),1,3) as z from table1 "
                        + "as t1 group by \"T1\".\"cc0\", d having \"T1\".\"cc0\" > 100 order by \"T1\".\"cc0\"",
                converter.replaceComputedColumns(sql0,
                        converter.collectLatentCcExpList(SqlSubqueryFinder.getSubqueries(sql0).get(0)), mockCCs,
                        queryAliasMatchInfo).getFirst());

        assertEquals("select sum(\"T1\".\"cc1\") from table1 as t1",
                converter.replaceComputedColumns(sql1,
                        converter.collectLatentCcExpList(SqlSubqueryFinder.getSubqueries(sql1).get(0)), mockCCs,
                        queryAliasMatchInfo).getFirst());

        assertEquals("select \"T1\".\"cc\" from table1 as t1",
                converter.replaceComputedColumns(sql2,
                        converter.collectLatentCcExpList(SqlSubqueryFinder.getSubqueries(sql2).get(0)), mockCCs,
                        queryAliasMatchInfo).getFirst());

        assertEquals("select \"T1\".\"cc4\" from table1",
                converter.replaceComputedColumns(sql3,
                        converter.collectLatentCcExpList(SqlSubqueryFinder.getSubqueries(sql3).get(0)), mockCCs,
                        queryAliasMatchInfo).getFirst());

        //Case SUM(CAST(...)) and sum({fn convert(...)})
        String sqlWithSum = "select sum(CAST(T1.a AS double)) from table1";
        assertEquals("select sum(\"T1\".\"cc5\") from table1",
                converter.replaceComputedColumns(sqlWithSum,
                        converter.collectLatentCcExpList(SqlSubqueryFinder.getSubqueries(sqlWithSum).get(0)), mockCCs,
                        queryAliasMatchInfo).getFirst());

        //more tables
        String sql2tables = "select t1.a + t1.b as aa, t2.c + t2.d as bb from table1 t1 inner join "
                + "table2 t2 on t1.x = t2.y where t1.a + t1.b > t2.c + t2.d order by t1.a + t1.b";

        ColumnRowType columnRowType2 = ColumnRowTypeMockUtil.mock("TABLE2", "T2",
                ImmutableList.of(Pair.newPair("A", "integer"), //
                        Pair.newPair("B", "integer"), //
                        Pair.newPair("C", "integer"), //
                        Pair.newPair("D", "integer")));

        mockQueryAlias.put("TABLE2", columnRowType2);
        queryAliasMatchInfo = new QueryAliasMatchInfo(aliasMapping, mockQueryAlias);

        assertEquals(
                "select \"T1\".\"cc2\" as aa, \"T2\".\"cc3\" as bb from table1 t1 inner join table2 t2 on t1.x = t2.y where \"T1\".\"cc2\" > \"T2\".\"cc3\" order by \"T1\".\"cc2\"",
                converter.replaceComputedColumns(sql2tables,
                        converter.collectLatentCcExpList(SqlSubqueryFinder.getSubqueries(sql2tables).get(0)), mockCCs,
                        queryAliasMatchInfo).getFirst());

        String sql2tableswithquote = "\r\n select \"T1\".\"A\" + \"T1\".\"B\" as aa, \"T2\".\"C\" + \"T2\".\"D\" as bb "
                + "from \r\n table1 \"T1\" inner join table2 \"T2\" on \"T1\".\"X\" = \"T2\".\"Y\" "
                + "where \"T1\".\"A\" + \"T1\".\"B\" > \"T2\".\"C\" + \"T2\".\"D\" "
                + "order by \"T1\".\"A\" + \"T1\".\"B\"";
        assertEquals("\r\n select \"T1\".\"cc2\" as aa, \"T2\".\"cc3\" as bb from \r\n"
                + " table1 \"T1\" inner join table2 \"T2\" on \"T1\".\"X\" = \"T2\".\"Y\" where \"T1\".\"cc2\" > \"T2\".\"cc3\" order by \"T1\".\"cc2\"",
                converter.replaceComputedColumns(sql2tableswithquote,
                        converter.collectLatentCcExpList(SqlSubqueryFinder.getSubqueries(sql2tableswithquote).get(0)),
                        mockCCs, queryAliasMatchInfo).getFirst());

        ColumnRowType columnRowType3 = ColumnRowTypeMockUtil.mock("0910_TABLE3", "0910_TABLE3",
                ImmutableList.of(Pair.newPair("0910_A", "integer"), //
                        Pair.newPair("0910_B", "integer"), //
                        Pair.newPair("C", "integer")));

        mockQueryAlias.put("0910_TABLE3", columnRowType3);
        queryAliasMatchInfo = new QueryAliasMatchInfo(aliasMapping, mockQueryAlias);

        assertEquals("select sum(\"0910_TABLE3\".\"cc6\"), c from \"0910_table3\" group by c",
                converter.replaceComputedColumns(sql4,
                        converter.collectLatentCcExpList(SqlSubqueryFinder.getSubqueries(sql4).get(0)), mockCCs,
                        queryAliasMatchInfo).getFirst());
    }

    @Test
    public void testReplaceComputedColumnWithGroupKeys() throws SqlParseException {
        List<ComputedColumnDesc> mockCCs = Lists.newArrayList(
                mockComputedColumnDesc("cc0", "table1.a + table1.b + table1.c", "TABLE1"),
                mockComputedColumnDesc("cc1", "sum(table1.a)", "TABLE1"), //
                mockComputedColumnDesc("cc2", "table1.a + table1.b", "TABLE1"),
                mockComputedColumnDesc("cc3", "table2.c + table2.d", "TABLE2"),
                mockComputedColumnDesc("cc", "substring(substring(table1.d,1,3),1,3)", "TABLE1"),
                mockComputedColumnDesc("cc4", "(table1.a + table1.b) + (table1.c + table1.d)", "TABLE1"),
                mockComputedColumnDesc("cc5", "CAST(table1.a AS double)", "TABLE1"),
                mockComputedColumnDesc("cc6", "{fn convert(table1.a, double)}", "TABLE1"));
        mockCCs = ConvertToComputedColumn.getCCListSortByLength(mockCCs);
        BiMap<String, String> aliasMapping = HashBiMap.create();
        aliasMapping.put("T1", "TABLE1");
        aliasMapping.put("T2", "TABLE2");

        ColumnRowType columnRowType1 = ColumnRowTypeMockUtil.mock("TABLE1", "T1",
                ImmutableList.of(Pair.newPair("A", "integer"), //
                        Pair.newPair("B", "integer"), //
                        Pair.newPair("C", "integer"), //
                        Pair.newPair("D", "integer")));

        LinkedHashMap<String, ColumnRowType> mockQueryAlias = Maps.newLinkedHashMap();
        mockQueryAlias.put("TABLE1", columnRowType1);

        QueryAliasMatchInfo queryAliasMatchInfo = new QueryAliasMatchInfo(aliasMapping, mockQueryAlias);

        // test replacement non agg field
        String sqlWithNonAggField = "select t1.a + t1.b, (t1 . a + t1.b + t1.c) as c from table1 as t1 "
                + "group by c, t1 . a + t1.b + t1.c";
        assertEquals("select t1.a + t1.b, (\"T1\".\"cc0\") as c from table1 as t1 group by c, \"T1\".\"cc0\"",
                converter.replaceComputedColumns(sqlWithNonAggField,
                        converter.collectLatentCcExpList(SqlSubqueryFinder.getSubqueries(sqlWithNonAggField).get(0)),
                        mockCCs, queryAliasMatchInfo).getFirst());

        // test replacement non agg field (case when)
        String sqlWithNonAggCaseWhenField = "select case when (t1.a + t1.b) > 1 then 1 else 2 end from table1 as t1 "
                + "group by (t1.a + t1.b)";
        assertEquals(
                "select case when (\"T1\".\"cc2\") > 1 then 1 else 2 end from table1 as t1 group by (\"T1\".\"cc2\")",
                converter.replaceComputedColumns(sqlWithNonAggCaseWhenField,
                        converter.collectLatentCcExpList(
                                SqlSubqueryFinder.getSubqueries(sqlWithNonAggCaseWhenField).get(0)),
                        mockCCs, queryAliasMatchInfo).getFirst());

        String sqlWithNonAggCaseWhenFieldNoReplacement = "select case when (t1.a + t1.b) > 1 then 1 else 2 end "
                + "from table1 as t1 group by a, b";
        assertEquals("select case when (t1.a + t1.b) > 1 then 1 else 2 end from table1 as t1 group by a, b",
                converter.replaceComputedColumns(sqlWithNonAggCaseWhenFieldNoReplacement,
                        converter.collectLatentCcExpList(
                                SqlSubqueryFinder.getSubqueries(sqlWithNonAggCaseWhenFieldNoReplacement).get(0)),
                        mockCCs, queryAliasMatchInfo).getFirst());

        // test replacement order by
        String sqlOrderBy = "select count(1), sum(t1.a + t1.b)\n" + "from table1 as t1\n" + "where t1.a + t1.b > 1\n"
                + "group by t1.a, t1.b\n" + "order by t1.a + t1.b";
        String sqlOrderByExpected = "select count(1), sum(\"T1\".\"cc2\")\n" + "from table1 as t1\n"
                + "where \"T1\".\"cc2\" > 1\n" + "group by t1.a, t1.b\n" + "order by t1.a + t1.b";
        assertEquals(sqlOrderByExpected,
                converter.replaceComputedColumns(sqlOrderBy,
                        converter.collectLatentCcExpList(SqlSubqueryFinder.getSubqueries(sqlOrderBy).get(0)), mockCCs,
                        queryAliasMatchInfo).getFirst());

        String sqlOrderBy1 = "select count(1), sum(t1.a + t1.b)\n" + "from table1 as t1\n" + "where t1.a + t1.b > 1\n"
                + "group by t1.a + t1.b\n" + "order by t1.a + t1.b";
        String sqlOrderByExpected1 = "select count(1), sum(\"T1\".\"cc2\")\n" + "from table1 as t1\n"
                + "where \"T1\".\"cc2\" > 1\n" + "group by \"T1\".\"cc2\"\n" + "order by \"T1\".\"cc2\"";
        assertEquals(sqlOrderByExpected1,
                converter.replaceComputedColumns(sqlOrderBy1,
                        converter.collectLatentCcExpList(SqlSubqueryFinder.getSubqueries(sqlOrderBy1).get(0)), mockCCs,
                        queryAliasMatchInfo).getFirst());

        // test replacement having
        String sqlHaving = "select count(1), sum(t1.a + t1.b)\n" + "from table1 as t1\n" + "where t1.a + t1.b > 1\n"
                + "group by t1.a, t1.b\n" + "having t1.a + t1.b > 2";
        String sqlHavingExpected = "select count(1), sum(\"T1\".\"cc2\")\n" + "from table1 as t1\n"
                + "where \"T1\".\"cc2\" > 1\n" + "group by t1.a, t1.b\n" + "having t1.a + t1.b > 2";
        assertEquals(sqlHavingExpected,
                converter.replaceComputedColumns(sqlHaving,
                        converter.collectLatentCcExpList(SqlSubqueryFinder.getSubqueries(sqlHaving).get(0)), mockCCs,
                        queryAliasMatchInfo).getFirst());

        String sqlHaving1 = "select count(1), sum(t1.a + t1.b)\n" + "from table1 as t1\n" + "where t1.a + t1.b > 1\n"
                + "group by t1.a + t1.b\n" + "having t1.a + t1.b > 2";
        String sqlHavingExpected1 = "select count(1), sum(\"T1\".\"cc2\")\n" + "from table1 as t1\n"
                + "where \"T1\".\"cc2\" > 1\n" + "group by \"T1\".\"cc2\"\n" + "having \"T1\".\"cc2\" > 2";
        assertEquals(sqlHavingExpected1,
                converter.replaceComputedColumns(sqlHaving1,
                        converter.collectLatentCcExpList(SqlSubqueryFinder.getSubqueries(sqlHaving1).get(0)), mockCCs,
                        queryAliasMatchInfo).getFirst());
    }
}

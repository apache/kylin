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

import static org.junit.Assert.assertEquals;

import java.util.LinkedHashMap;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.query.relnode.ColumnRowType;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.model.alias.AliasMapping;
import org.apache.kylin.metadata.model.alias.ExpressionComparator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

public class ExpressionComparatorTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() {
        this.createTestMetadata();
    }

    @After
    public void after() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testBasicEqual() throws SqlParseException {
        String sql0 = "select a.a + a.b + a.c from t as a";
        String sql1 = "select (((a . a +    a.b +    a.c))) from t as a";
        String sql2 = "select a.a + (a.b + a.c) from t as a";

        SqlNode sn0 = CalciteParser.getOnlySelectNode(sql0);
        SqlNode sn1 = CalciteParser.getOnlySelectNode(sql1);
        SqlNode sn3 = CalciteParser.getOnlySelectNode(sql2);

        BiMap<String, String> aliasMapping = HashBiMap.create();
        aliasMapping.put("A", "A");
        QueryAliasMatchInfo matchInfo = new QueryAliasMatchInfo(aliasMapping, null);

        assertEquals(true, ExpressionComparator.isNodeEqual(sn0, sn1, matchInfo, AliasDeduceImpl.NO_OP));
        assertEquals(false, ExpressionComparator.isNodeEqual(sn0, sn3, matchInfo, AliasDeduceImpl.NO_OP));

    }

    @Test
    public void testCommutativeEqual() throws SqlParseException {
        String sql0 = "select a.a + a.b * a.c from t as a";
        String sql1 = "select a.c * a.b + a.a from t as a";

        SqlNode sn0 = CalciteParser.getOnlySelectNode(sql0);
        SqlNode sn1 = CalciteParser.getOnlySelectNode(sql1);

        BiMap<String, String> aliasMapping = HashBiMap.create();
        aliasMapping.put("A", "A");
        QueryAliasMatchInfo matchInfo = new QueryAliasMatchInfo(aliasMapping, null);

        Assert.assertTrue(ExpressionComparator.isNodeEqual(sn0, sn1, matchInfo, AliasDeduceImpl.NO_OP));
    }

    @Test
    public void testAdvancedEqual() throws SqlParseException {
        //treat sql0 as model
        String sql0 = "select a.a + a.b + a.c, cast(a.d as decimal(19,4)) from t as a";

        String sql1 = "select b.a + b.b + b.c, cast(b.d as decimal(19,4)) from t as b";
        String sql2 = "select (a + b) + c, cast(d as decimal(19,4)) from t";

        SqlNode sn0 = CalciteParser.getSelectNode(sql0);
        SqlNode sn1 = CalciteParser.getSelectNode(sql1);
        SqlNode sn2 = CalciteParser.getSelectNode(sql2);

        // when query using different alias than model
        {
            BiMap<String, String> aliasMapping = HashBiMap.create();
            aliasMapping.put("B", "A");

            ColumnRowType columnRowType = ColumnRowTypeMockUtil.mock("T", "B",
                    ImmutableList.of(Pair.newPair("A", "integer"), //
                            Pair.newPair("B", "integer"), //
                            Pair.newPair("C", "integer"), //
                            Pair.newPair("D", "integer")));

            LinkedHashMap<String, ColumnRowType> mockQueryAlias = Maps.newLinkedHashMap();
            mockQueryAlias.put("B", columnRowType);

            QueryAliasMatchInfo matchInfo = new QueryAliasMatchInfo(aliasMapping, mockQueryAlias);
            assertEquals(true, ExpressionComparator.isNodeEqual(sn1, sn0, matchInfo, AliasDeduceImpl.NO_OP));
        }

        // when query not using alias
        {
            BiMap<String, String> aliasMapping = HashBiMap.create();
            aliasMapping.put("T", "A");

            ColumnRowType columnRowType = ColumnRowTypeMockUtil.mock("T", "T", //
                    ImmutableList.of(Pair.newPair("A", "integer"), //
                            Pair.newPair("B", "integer"), //
                            Pair.newPair("C", "integer"), //
                            Pair.newPair("D", "integer")));

            LinkedHashMap<String, ColumnRowType> mockQueryAlias = Maps.newLinkedHashMap();
            mockQueryAlias.put("T", columnRowType);

            QueryAliasMatchInfo matchInfo = new QueryAliasMatchInfo(aliasMapping, mockQueryAlias);
            assertEquals(true, ExpressionComparator.isNodeEqual(sn2, sn0, matchInfo, new AliasDeduceImpl(matchInfo)));
        }

    }

    @Test
    public void testNoNPE() {
        //https://github.com/Kyligence/KAP/issues/10934
        String sql0 = "select a.a + a.b + a.c from t as a";
        String sql1 = "select a.a + a.b + a.c from t as a";
        String sql2 = "select 1";
        String sql3 = "select 1";

        SqlNode sn0 = CalciteParser.getOnlySelectNode(sql0);
        SqlNode sn1 = CalciteParser.getOnlySelectNode(sql1);
        SqlNode sn2 = CalciteParser.getOnlySelectNode(sql2);
        SqlNode sn3 = CalciteParser.getOnlySelectNode(sql3);
        {
            AliasMapping aliasMapping = null;
            ExpressionComparator.AliasMachingSqlNodeComparator matchInfo = new ExpressionComparator.AliasMachingSqlNodeComparator(
                    aliasMapping, null);

            assertEquals(false, matchInfo.isSqlNodeEqual(sn0, sn1));
        }
        {
            AliasMapping aliasMapping = new AliasMapping(null);
            ExpressionComparator.AliasMachingSqlNodeComparator matchInfo = new ExpressionComparator.AliasMachingSqlNodeComparator(
                    aliasMapping, null);
            assertEquals(false, matchInfo.isSqlNodeEqual(sn0, sn1));
        }
        {
            AliasMapping aliasMapping = null;
            ExpressionComparator.AliasMachingSqlNodeComparator matchInfo = new ExpressionComparator.AliasMachingSqlNodeComparator(
                    aliasMapping, null);
            assertEquals(true, matchInfo.isSqlNodeEqual(sn2, sn3));
        }
        {
            AliasMapping aliasMapping = new AliasMapping(null);
            ExpressionComparator.AliasMachingSqlNodeComparator matchInfo = new ExpressionComparator.AliasMachingSqlNodeComparator(
                    aliasMapping, null);
            assertEquals(true, matchInfo.isSqlNodeEqual(sn2, sn3));
        }

    }
}

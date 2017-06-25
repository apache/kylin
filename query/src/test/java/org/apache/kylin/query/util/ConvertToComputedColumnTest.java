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

import java.util.HashMap;
import java.util.Map;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableSortedMap;

public class ConvertToComputedColumnTest {
    @Test
    public void testEqual() throws SqlParseException {
        String sql0 = "select a.a + a.b + a.c from t as a";
        String sql1 = "select (((a . a +    a.b +    a.c))) from t as a";
        String sql2 = "select (a + b) + c  from t";
        String sql3 = "select a.a + (a.b + a.c) from t as a";

        SqlNode sn0 = getSelectNode(sql0);
        SqlNode sn1 = getSelectNode(sql1);
        SqlNode sn2 = getSelectNode(sql2);
        SqlNode sn3 = getSelectNode(sql3);

        Assert.assertEquals(true, ConvertToComputedColumn.isNodeEqual(sn0, sn1));
        Assert.assertEquals(true, ConvertToComputedColumn.isNodeEqual(sn0, sn2));
        Assert.assertEquals(false, ConvertToComputedColumn.isNodeEqual(sn0, sn3));
    }

    @Test
    public void testErrorCase() {
        //computed column is null or empty
        String sql = "select a from t";
        Map<String, String> map = new HashMap<>();
        ImmutableSortedMap<String, String> computedColumns = ConvertToComputedColumn.getMapSortedByValue(map);
        Assert.assertEquals("select a from t", ConvertToComputedColumn.replaceComputedColumn(sql, null));
        Assert.assertEquals("select a from t", ConvertToComputedColumn.replaceComputedColumn(sql, computedColumns));

        //input is null or empty or parse error
        String sql1 = "";
        String sql2 = "select sum(a from t";
        Map<String, String> map2 = new HashMap<>();
        map2.put("cc", "a + b");
        ImmutableSortedMap<String, String> computedColumns2 = ConvertToComputedColumn.getMapSortedByValue(map2);
        Assert.assertEquals("", ConvertToComputedColumn.replaceComputedColumn(null, computedColumns2));
        Assert.assertEquals("", ConvertToComputedColumn.replaceComputedColumn(sql1, computedColumns2));
        Assert.assertEquals("select sum(a from t",
                ConvertToComputedColumn.replaceComputedColumn(sql2, computedColumns2));
    }

    @Test
    public void testReplaceComputedColumn() throws SqlParseException {
        String sql0 = "select (\"DB\".\"t1\" . \"a\" + DB.t1.b + DB.t1.c) as c, substring(substring(lstg_format_name,1,3),1,3) as d from table1 as t1 group by t1.a+   t1.b +     t1.c having t1.a+t1.b+t1.c > 100 order by t1.a +t1.b +t1.c";
        String sql1 = "select sum(sum(a)) from t";
        String sql2 = "select t1.a + t1.b as aa, t2.c + t2.d as bb from table1 t1,table2 t2 where t1.a + t1.b > t2.c + t2.d order by t1.a + t1.b";
        String sql3 = "select substring(substring(lstg_format_name,1,3),1,3) from a";

        String expr0 = "a + b + c";
        String expr1 = "sum(a)";
        String expr2 = "a + b";
        String expr3 = "c + d";
        String expr = "substring(substring(lstg_format_name,1,3),1,3)";

        Map<String, String> map = new HashMap<>();
        map.put("cc0", expr0);
        map.put("cc1", expr1);
        map.put("cc2", expr2);
        map.put("cc3", expr3);
        map.put("cc", expr);

        ImmutableSortedMap<String, String> computedColumns = ConvertToComputedColumn.getMapSortedByValue(map);
        Assert.assertEquals(
                "select (DB.t1.cc0) as c, cc as d from table1 as t1 group by T1.cc0 having T1.cc0 > 100 order by T1.cc0",
                ConvertToComputedColumn.replaceComputedColumn(sql0, computedColumns));
        Assert.assertEquals("select sum(cc1) from t",
                ConvertToComputedColumn.replaceComputedColumn(sql1, computedColumns));
        Assert.assertEquals(
                "select T1.cc2 as aa, T2.cc3 as bb from table1 t1,table2 t2 where T1.cc2 > T2.cc3 order by T1.cc2",
                ConvertToComputedColumn.replaceComputedColumn(sql2, computedColumns));
        Assert.assertEquals("select cc from a", ConvertToComputedColumn.replaceComputedColumn(sql3, computedColumns));

    }

    private static SqlNode getSelectNode(String sql) throws SqlParseException {
        return ((SqlSelect) ConvertToComputedColumn.parse(sql)).getSelectList().get(0);
    }

    @Test
    public void testTwoCCHasSameSubExp() {
        String sql0 = "select a + b + c from t order by a + b";

        String expr0 = "a +         b";
        String expr1 = "a + b + c";

        Map<String, String> map = new HashMap<>();
        map.put("cc1", expr0);
        map.put("cc0", expr1);
        ImmutableSortedMap<String, String> computedColumns = ConvertToComputedColumn.getMapSortedByValue(map);
        Assert.assertEquals("select cc0 from t order by cc1",
                ConvertToComputedColumn.replaceComputedColumn(sql0, computedColumns));

        //防止添加的顺序造成影响
        String expr11 = "a + b + c";
        String expr00 = "a +         b";

        Map<String, String> map2 = new HashMap<>();
        map2.put("cc0", expr11);
        map2.put("cc1", expr00);
        ImmutableSortedMap<String, String> computedColumns1 = ConvertToComputedColumn.getMapSortedByValue(map2);
        Assert.assertEquals("select cc0 from t order by cc1",
                ConvertToComputedColumn.replaceComputedColumn(sql0, computedColumns1));

    }

}

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

package org.apache.kylin.query.engine;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
import org.apache.kylin.query.relnode.ContextUtil;
import org.apache.kylin.query.relnode.OlapContext;
import org.apache.kylin.query.relnode.OlapLimitRel;
import org.apache.kylin.query.relnode.OlapWindowRel;
import org.apache.kylin.query.rules.CalciteRuleTestBase;
import org.apache.kylin.query.util.QueryContextCutter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WindowFunctionPlannerTest extends CalciteRuleTestBase {

    String project = "default";

    QueryExec queryExec;

    @Before
    public void setup() {
        createTestMetadata();
        queryExec = new QueryExec(project, getTestConfig());
    }

    private void prepareOlapContext(String sql) throws SqlParseException {
        RelNode relNode = queryExec.parseAndOptimize(sql);
        List<RelNode> optimizedRelNodes = queryExec.postOptimize(relNode);

        try {
            QueryContextCutter.selectRealization(project, optimizedRelNodes.get(0),
                    QueryContext.current().isForModeling());
        } catch (NoRealizationFoundException e) {
            log.info("No realization found for sql: {}", sql);
        }
    }

    private String getSql(String fileName) throws IOException {
        Pair<String, String> sql = readOneSQL(getTestConfig(), project, "query/sql_window_olap_context", fileName);
        return sql.getSecond();
    }

    @Test
    public void testSelectStarAndWindow() throws SqlParseException, IOException {
        prepareOlapContext(getSql("query00.sql"));

        Assert.assertEquals(1, ContextUtil.listContexts().size());
        OlapContext olapContext = ContextUtil.listContexts().get(0);
        Assert.assertEquals(17, olapContext.getAllColumns().size());
        Assert.assertTrue(olapContext.getTopNode() instanceof OlapWindowRel);
    }

    @Test
    public void testSelectStarAndWindowWithLimit() throws SqlParseException, IOException {
        prepareOlapContext(getSql("query01.sql"));

        Assert.assertEquals(1, ContextUtil.listContexts().size());
        OlapContext olapContext = ContextUtil.listContexts().get(0);
        Assert.assertEquals(17, olapContext.getAllColumns().size());
        Assert.assertTrue(olapContext.getTopNode() instanceof OlapLimitRel);
    }

    @Test
    public void testSelectOnlyWindow() throws IOException, SqlParseException {
        prepareOlapContext(getSql("query02.sql"));

        Assert.assertEquals(1, ContextUtil.listContexts().size());
        OlapContext olapContext = ContextUtil.listContexts().get(0);
        Set<TblColRef> allColumns = olapContext.getAllColumns();
        Assert.assertEquals(2, allColumns.size());
        Set<String> expectedColumns = Sets.newHashSet("SSB.LINEORDER.LO_ORDERDATE", "SSB.LINEORDER.LO_QUANTITY");
        allColumns.forEach(tblColRef -> expectedColumns
                .remove(tblColRef.getTableRef().getTableIdentity() + '.' + tblColRef.getColumnDesc().getName()));
        Assert.assertTrue(expectedColumns.isEmpty());
    }

    @Test
    public void testSelectWindowAndSpecifiedColumn() throws IOException, SqlParseException {
        prepareOlapContext(getSql("query03.sql"));

        Assert.assertEquals(1, ContextUtil.listContexts().size());
        OlapContext olapContext = ContextUtil.listContexts().get(0);
        Set<TblColRef> allColumns = olapContext.getAllColumns();
        Assert.assertEquals(3, allColumns.size());
        Set<String> expectedColumns = Sets.newHashSet("SSB.LINEORDER.LO_ORDERDATE", "SSB.LINEORDER.LO_QUANTITY",
                "SSB.LINEORDER.LO_CUSTKEY");
        allColumns.forEach(tblColRef -> expectedColumns
                .remove(tblColRef.getTableRef().getTableIdentity() + '.' + tblColRef.getColumnDesc().getName()));
        Assert.assertTrue(expectedColumns.isEmpty());
    }

    @Test
    public void testSelectWindowAndFilter() throws IOException, SqlParseException {
        prepareOlapContext(getSql("query04.sql"));

        Assert.assertEquals(1, ContextUtil.listContexts().size());
        OlapContext olapContext = ContextUtil.listContexts().get(0);
        Set<TblColRef> allColumns = olapContext.getAllColumns();
        Assert.assertEquals(4, allColumns.size());
        Set<String> expectedColumns = Sets.newHashSet("SSB.LINEORDER.LO_ORDERDATE", "SSB.LINEORDER.LO_QUANTITY",
                "SSB.LINEORDER.LO_CUSTKEY", "SSB.LINEORDER.LO_PARTKEY");
        allColumns.forEach(tblColRef -> expectedColumns
                .remove(tblColRef.getTableRef().getTableIdentity() + '.' + tblColRef.getColumnDesc().getName()));
        Assert.assertTrue(expectedColumns.isEmpty());
    }

    @Test
    public void testSubQueryWithWidow() throws IOException, SqlParseException {
        prepareOlapContext(getSql("query05.sql"));

        Assert.assertEquals(2, ContextUtil.listContexts().size());

        OlapContext olapContext1 = ContextUtil.listContexts().get(0);
        Set<TblColRef> allColumns1 = olapContext1.getAllColumns();
        Assert.assertEquals(4, allColumns1.size());
        Set<String> expectedColumns1 = Sets.newHashSet("SSB.LINEORDER.LO_ORDERDATE", "SSB.LINEORDER.LO_QUANTITY",
                "SSB.LINEORDER.LO_ORDERKEY", "SSB.LINEORDER.LO_PARTKEY");
        allColumns1.forEach(tblColRef -> expectedColumns1
                .remove(tblColRef.getTableRef().getTableIdentity() + '.' + tblColRef.getColumnDesc().getName()));
        Assert.assertTrue(expectedColumns1.isEmpty());

        OlapContext olapContext2 = ContextUtil.listContexts().get(1);
        Set<TblColRef> allColumns2 = olapContext2.getAllColumns();
        Assert.assertEquals(5, allColumns2.size());
        Set<String> expectedColumns2 = Sets.newHashSet("SSB.LINEORDER.LO_ORDERDATE", "SSB.LINEORDER.LO_QUANTITY",
                "SSB.LINEORDER.LO_ORDERKEY", "SSB.LINEORDER.LO_PARTKEY", "SSB.LINEORDER.LO_CUSTKEY");
        allColumns2.forEach(tblColRef -> expectedColumns2
                .remove(tblColRef.getTableRef().getTableIdentity() + '.' + tblColRef.getColumnDesc().getName()));
        Assert.assertTrue(expectedColumns2.isEmpty());
    }
}

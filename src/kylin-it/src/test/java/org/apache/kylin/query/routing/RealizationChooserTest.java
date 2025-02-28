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

package org.apache.kylin.query.routing;

import java.util.List;
import java.util.Map;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.metadata.cube.cuboid.NLookupCandidate;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.query.relnode.OlapContext;
import org.apache.kylin.query.relnode.OlapSortRel;
import org.apache.kylin.util.MetadataTestUtils;
import org.apache.kylin.util.OlapContextTestUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RealizationChooserTest extends NLocalWithSparkSessionTest {
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.createTestMetadata("src/test/resources/ut_meta/joins_graph_left_or_inner");
    }

    @Override
    protected String[] getOverlay() {
        return new String[] { "src/test/resources/ut_meta/joins_graph_left_or_inner" };
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test
    public void testIsLookupCandidateMatched() throws SqlParseException {
        String sql = "select a.NAME from TEST_BANK_INCOME a ";
        OlapContext olap = OlapContextTestUtil.getOlapContexts(getProject(), sql).get(0);
        boolean b1 = RealizationChooser.isLookupCandidateMatched(olap, NLookupCandidate.Policy.AGG_THEN_SNAPSHOT);
        Assert.assertTrue(b1);
        boolean b2 = RealizationChooser.isLookupCandidateMatched(olap, NLookupCandidate.Policy.SNAPSHOT);
        Assert.assertFalse(b2);
        boolean b3 = RealizationChooser.isLookupCandidateMatched(olap, NLookupCandidate.Policy.AGG_THEN_INTERNAL_TABLE);
        Assert.assertTrue(b3);
        boolean b4 = RealizationChooser.isLookupCandidateMatched(olap, NLookupCandidate.Policy.INTERNAL_TABLE);
        Assert.assertFalse(b4);
    }

    @Test
    public void testDeduceLookupTableType() throws SqlParseException {
        String tableIdentity = "DEFAULT.TEST_BANK_INCOME";
        String snapshotPath = "hdfs://localhost:9000/" + tableIdentity;
        MetadataTestUtils.mockSnapshotPath(getProject(), tableIdentity, snapshotPath);
        {
            String sql = "select a.NAME from TEST_BANK_INCOME a ";
            OlapContext olap = OlapContextTestUtil.getOlapContexts(getProject(), sql).get(0);
            NLookupCandidate.Policy policy = olap.deduceLookupTableType();
            Assert.assertEquals(NLookupCandidate.Policy.SNAPSHOT, policy);
        }

        {
            String sql = "select a.NAME from TEST_BANK_INCOME a group by a.NAME ";
            OlapContext olap = OlapContextTestUtil.getOlapContexts(getProject(), sql).get(0);
            NLookupCandidate.Policy policy = olap.deduceLookupTableType();
            Assert.assertEquals(NLookupCandidate.Policy.AGG_THEN_SNAPSHOT, policy);
        }

        overwriteSystemProp("kylin.internal-table-enabled", "true");
        MetadataTestUtils.mockSnapshotPath(getProject(), tableIdentity, "");
        MetadataTestUtils.mockInternalTable(getProject(), tableIdentity, true);
        {
            String sql = "select a.NAME from TEST_BANK_INCOME a ";
            OlapContext olap = OlapContextTestUtil.getOlapContexts(getProject(), sql).get(0);
            NLookupCandidate.Policy policy = olap.deduceLookupTableType();
            Assert.assertEquals(NLookupCandidate.Policy.INTERNAL_TABLE, policy);
        }

        {
            String sql = "select a.NAME from TEST_BANK_INCOME a group by a.NAME ";
            OlapContext olap = OlapContextTestUtil.getOlapContexts(getProject(), sql).get(0);
            NLookupCandidate.Policy policy = olap.deduceLookupTableType();
            Assert.assertEquals(NLookupCandidate.Policy.AGG_THEN_INTERNAL_TABLE, policy);
        }

    }

    @Test
    public void testCanMatchModelLeftQueryLeft() throws SqlParseException {
        // model: TEST_BANK_INCOME left join TEST_BANK_LOCATION
        // query: TEST_BANK_INCOME left join TEST_BANK_LOCATION with not null filter -> LEFT_OR_INNER
        String project = "joins_graph_left_or_inner";
        final List<String> filters = ImmutableList.of(" b.LOCATION is not null", " b.LOCATION in ('a', 'b')",
                " b.LOCATION like 'a%' ", " b.LOCATION not like 'b%' ", " b.LOCATION between 'a' and 'b' ");
        getTestConfig().setProperty("kylin.query.join-match-optimization-enabled", "true");
        NDataflow dataflow = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("b780e4e4-69af-449e-b09f-05c90dfa04b7");
        for (String filter : filters) {
            String sql = "select a.NAME from TEST_BANK_INCOME a left join TEST_BANK_LOCATION b \n"
                    + " on a.COUNTRY = b.COUNTRY where " + filter;
            OlapContext olapContext = OlapContextTestUtil.getOlapContexts(getProject(), sql).get(0);
            Map<String, String> sqlAlias2ModelName = OlapContextTestUtil.matchJoins(dataflow.getModel(), olapContext);
            Assert.assertFalse(sqlAlias2ModelName.isEmpty());
        }
    }

    @Test
    public void testCanNotMatchModelLeftQueryInner() throws SqlParseException {
        // model: TEST_BANK_INCOME left join TEST_BANK_LOCATION
        // query: TEST_BANK_INCOME inner join TEST_BANK_LOCATION
        String project = "joins_graph_left_or_inner";
        String modelId = "b780e4e4-69af-449e-b09f-05c90dfa04b7";
        overwriteSystemProp("kylin.query.join-match-optimization-enabled", "true");
        String sql = "select a.NAME from TEST_BANK_INCOME a inner join TEST_BANK_LOCATION b on a.COUNTRY = b.COUNTRY";
        NDataflow dataflow = NDataflowManager.getInstance(getTestConfig(), project).getDataflow(modelId);
        OlapContext olapContext = OlapContextTestUtil.getOlapContexts(project, sql, true).get(0);
        Map<String, String> sqlAlias2ModelName = OlapContextTestUtil.matchJoins(dataflow.getModel(), olapContext);
        Assert.assertTrue(sqlAlias2ModelName.isEmpty());
    }

    @Test
    public void testCanNotMatchInnerJoinWithFilter() throws SqlParseException {
        String modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        final List<String> filters = ImmutableList.of(" b.SITE_NAME is null", " b.SITE_NAME is distinct from '%英国%'",
                " b.SITE_NAME is not distinct from null", " b.SITE_NAME is not null or a.TRANS_ID is not null",
                " case when b.SITE_NAME is not null then false else true end" //
        );
        getTestConfig().setProperty("kylin.query.join-match-optimization-enabled", "true");
        getTestConfig().setProperty("kylin.query.realization-chooser-using-multi-threads", "false");
        NDataflow dataflow = NDataflowManager.getInstance(getTestConfig(), getProject()).getDataflow(modelId);
        for (String filter : filters) {
            String sql = "select CAL_DT from test_kylin_fact a inner join EDW.test_sites b \n"
                    + " on a.LSTG_SITE_ID = b.SITE_ID where " + filter;
            OlapContext olapContext = OlapContextTestUtil.getOlapContexts(getProject(), sql, true).get(0);
            Map<String, String> sqlAlias2ModelName = OlapContextTestUtil.matchJoins(dataflow.getModel(), olapContext);
            Assert.assertTrue(sqlAlias2ModelName.isEmpty());
        }
    }

    @Test
    public void testPushSortRelToSubOlapContexts() throws SqlParseException {
        overwriteSystemProp("kylin.query.print-logical-plan", "true");
        String project = "joins_graph_left_or_inner";
        String sql = "select a.NAME from TEST_BANK_INCOME a inner join TEST_BANK_LOCATION b on a.COUNTRY = b.COUNTRY\n"
                + "order by a.INCOME nulls last";
        RelNode relNode = OlapContextTestUtil.cutOlapContextsAndReturnRelNode(project, sql);
        OlapSortRel sortRel = null;
        while (relNode != null) {
            if (relNode instanceof OlapSortRel) {
                sortRel = (OlapSortRel) relNode;
                break;
            }
            relNode = relNode.getInput(0);
        }
        Assert.assertNotNull(sortRel);
        Assert.assertTrue(sortRel.isNeedPushToSubCtx());
    }

    @Test
    public void testCanMatchModelInnerQueryLeft() throws SqlParseException {
        // model: TEST_BANK_INCOME inner join TEST_BANK_LOCATION
        // query: TEST_BANK_INCOME left join TEST_BANK_LOCATION with not null filter
        final List<String> filters = ImmutableList.of(" b.LOCATION is not null", " b.LOCATION in ('a', 'b')",
                " b.LOCATION like 'a%' ", " b.LOCATION not like 'b%' ", " b.LOCATION between 'a' and 'b' ");
        overwriteSystemProp("kylin.query.join-match-optimization-enabled", "true");
        NDataflowManager dfMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
        NDataflow dataflow = dfMgr.getDataflow("b780e4e4-69af-449e-b09f-05c90dfa04b6");
        for (String filter : filters) {
            String sql = "select a.NAME from TEST_BANK_INCOME a left join TEST_BANK_LOCATION b \n"
                    + " on a.COUNTRY = b.COUNTRY where " + filter;
            OlapContext olapContext = OlapContextTestUtil.getOlapContexts(getProject(), sql, true).get(0);
            Map<String, String> sqlAlias2ModelName = OlapContextTestUtil.matchJoins(dataflow.getModel(), olapContext);
            Assert.assertFalse(sqlAlias2ModelName.isEmpty());
        }
    }

    @Test
    public void testCanNotMatchModelInnerQueryLeft() throws SqlParseException {
        // model: TEST_BANK_INCOME inner join TEST_BANK_LOCATION
        // query: TEST_BANK_INCOME left join TEST_BANK_LOCATION without not null filter
        final List<String> filters = ImmutableList.of(" b.LOCATION is null", " b.LOCATION is not distinct from null");
        getTestConfig().setProperty("kylin.query.join-match-optimization-enabled", "true");
        NDataflow dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject())
                .getDataflow("b780e4e4-69af-449e-b09f-05c90dfa04b6");
        for (String filter : filters) {
            String sql = "select a.NAME from TEST_BANK_INCOME a left join TEST_BANK_LOCATION b \n"
                    + " on a.COUNTRY = b.COUNTRY where " + filter;
            OlapContext olapContext = OlapContextTestUtil.getOlapContexts(getProject(), sql).get(0);
            Map<String, String> sqlAlias2ModelNameMap = OlapContextTestUtil.matchJoins(dataflow.getModel(),
                    olapContext);
            Assert.assertTrue(sqlAlias2ModelNameMap.isEmpty());
        }
    }
}

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

package org.apache.kylin.newten;

import java.util.List;
import java.util.Map;

import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.cube.cuboid.NLayoutCandidate;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.query.relnode.OlapContext;
import org.apache.kylin.query.routing.Candidate;
import org.apache.kylin.query.routing.QueryLayoutChooser;
import org.apache.kylin.query.routing.RemoveIncapableRealizationsRule;
import org.apache.kylin.util.OlapContextTestUtil;
import org.apache.spark.sql.SparderEnv;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class NAggIndexPriorityAnswerWithCCExprTest extends NLocalWithSparkSessionTest {

    @Override
    public String getProject() {
        return "aggindex_priority_answer_withccexpr";
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        overwriteSystemProp("kylin.query.use-tableindex-answer-non-raw-query", "true");
        overwriteSystemProp("kylin.query.layout.prefer-aggindex", "true");
        this.createTestMetadata("src/test/resources/ut_meta/aggindex_priority_answer_withccexpr");

        JobContextUtil.cleanUp();
        JobContextUtil.getJobContext(getTestConfig());
    }

    @Override
    protected String[] getOverlay() {
        return new String[] { "src/test/resources/ut_meta/aggindex_priority_answer_withccexpr" };
    }

    @Override
    @After
    public void tearDown() throws Exception {
        JobContextUtil.cleanUp();
        cleanupTestMetadata();
    }

    @Test
    public void testAggIndexPriorityAnswerWithCcExpr() throws Exception {
        String modelId = "8dc395fb-f201-310e-feaa-d4000a7e16cb";
        NIndexPlanManager indexMgr = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        List<LayoutEntity> layouts = indexMgr.getIndexPlan(modelId).getAllLayouts();
        indexDataConstructor.buildIndex(modelId, SegmentRange.TimePartitionedSegmentRange.createInfinite(),
                Sets.newLinkedHashSet(layouts), true);
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());

        String sql = "select D_YEAR,count(CASE WHEN b IN ('1') THEN 1 ELSE NULL END) "
                + "from (select D_YEAR,D_DAYOFWEEK b from SSB.DATES) group by D_YEAR";

        NDataflow dataflow = NDataflowManager.getInstance(getTestConfig(), getProject()).getDataflow(modelId);
        OlapContext context = OlapContextTestUtil.getOlapContexts(getProject(), sql).get(0);

        Map<String, String> sqlAlias2ModelName = OlapContextTestUtil.matchJoins(dataflow.getModel(), context);
        context.fixModel(dataflow.getModel(), sqlAlias2ModelName);
        NLayoutCandidate layoutCandidate = QueryLayoutChooser.selectLayoutCandidate(dataflow,
                dataflow.getQueryableSegments(), context.getSQLDigest());
        assert layoutCandidate != null;
        Assert.assertTrue(layoutCandidate.getCapabilityResult().isCapable());

        RemoveIncapableRealizationsRule removeIncapableRealizationsRule = new RemoveIncapableRealizationsRule();
        Candidate candidate = new Candidate(dataflow.getRealizations().get(0), context, sqlAlias2ModelName);
        candidate.setPrunedSegments(dataflow.getQueryableSegments(), dataflow);
        removeIncapableRealizationsRule.apply(candidate);
        Assert.assertTrue(candidate.getCapability().isCapable());
        NLayoutCandidate selectedCandidate = (NLayoutCandidate) candidate.getCapability().getSelectedCandidate();
        Assert.assertNotNull(selectedCandidate.getLayoutEntity().getIndex().getLayout(50001));
    }
}

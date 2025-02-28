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

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.util.ExecAndComp;
import org.apache.spark.sql.SparderEnv;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LogicalViewTest extends NLocalWithSparkSessionTest {

    private NDataflowManager dfMgr = null;

    @Override
    @Before
    public void setUp() throws Exception {
        JobContextUtil.cleanUp();
        // kylin.source.ddl.logical-view.enabled=true
        super.setUp();
        overwriteSystemProp("kylin.source.ddl.logical-view.enabled", "true");
        this.createTestMetadata("src/test/resources/ut_meta/logical_view");
        dfMgr = NDataflowManager.getInstance(getTestConfig(), getProject());

        JobContextUtil.getJobContext(getTestConfig());
    }

    @Override
    protected String[] getOverlay() {
        return new String[] { "src/test/resources/ut_meta/logical_view" };
    }

    @Override
    @After
    public void tearDown() throws Exception {
        JobContextUtil.cleanUp();
        cleanupTestMetadata();
    }

    @Override
    public String getProject() {
        return "logical_view";
    }

    @Test
    public void testLogicalView() throws Exception {
        String dfID = "451e127a-b684-1474-744b-c9afc14378af";
        NDataflow dataflow = dfMgr.getDataflow(dfID);
        LayoutEntity layout = dataflow.getIndexPlan().getLayoutEntity(20000000001L);
        Assert.assertNotNull(layout);
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        indexDataConstructor.buildIndex(dfID, SegmentRange.TimePartitionedSegmentRange.createInfinite(),
                Sets.newHashSet(dataflow.getIndexPlan().getLayoutEntity(20000000001L),
                        dataflow.getIndexPlan().getLayoutEntity(1L)),
                true);

        List<Pair<String, String>> query = new ArrayList<>();
        String sql1 = "select t1.C_CUSTKEY from KYLIN_LOGICAL_VIEW.LOGICAL_VIEW_TABLE t1"
                + " INNER JOIN SSB.CUSTOMER t2 on t1.C_CUSTKEY = t2.C_CUSTKEY ";
        query.add(Pair.newPair("logical_view", sql1));
        ExecAndComp.execAndCompare(query, getProject(), ExecAndComp.CompareLevel.SAME, "inner");
    }
}

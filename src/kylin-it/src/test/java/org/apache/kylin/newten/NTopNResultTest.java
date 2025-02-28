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
import java.util.stream.Collectors;

import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.util.ExecAndComp;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.sparkproject.guava.collect.Sets;

import lombok.val;

public class NTopNResultTest extends NLocalWithSparkSessionTest {
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.createTestMetadata("src/test/resources/ut_meta/multiple_topn");

        JobContextUtil.cleanUp();
        JobContextUtil.getJobContext(getTestConfig());
    }

    @Override
    protected String[] getOverlay() {
        return new String[] { "src/test/resources/ut_meta/multiple_topn" };
    }

    @Override
    @After
    public void tearDown() throws Exception {
        JobContextUtil.cleanUp();
        cleanupTestMetadata();
    }

    @Override
    public String getProject() {
        return "multiple_topn";
    }

    @Test
    public void testWithTwoSameMeasuresOfTopN() throws Exception {
        String dfID1 = "d9f564ce-bf63-498e-b346-db982fcf91f9";
        String dfID2 = "c6381db2-802f-4a25-98f0-bfe021c304eg";
        String sqlHitCube = "select sum(price)  from TEST_KYLIN_FACT group by TRANS_ID order by sum(price)  desc limit 10";

        List<String> hitCubeResult1 = queryFromCube(dfID1, sqlHitCube);
        List<String> hitCubeResult2 = queryFromCube(dfID2, sqlHitCube);
        Assert.assertEquals(hitCubeResult1.toString(), hitCubeResult2.toString());
    }

    private List<String> queryFromCube(String dfID, String sqlHitCube) throws Exception {
        NDataflowManager dsMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
        NDataflow df = dsMgr.getDataflow(dfID);
        val layouts = df.getIndexPlan().getAllLayouts();
        indexDataConstructor.buildIndex(dfID, SegmentRange.TimePartitionedSegmentRange.createInfinite(),
                Sets.newLinkedHashSet(layouts), true);

        return ExecAndComp.queryModelWithoutCompute(getProject(), sqlHitCube).collectAsList().stream()
                .map(Row::toString).collect(Collectors.toList());
    }
}

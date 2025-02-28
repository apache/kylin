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

public class NTopNWithChineseTest extends NLocalWithSparkSessionTest {
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.createTestMetadata("src/test/resources/ut_meta/topn_with_chinese");

        JobContextUtil.cleanUp();
        JobContextUtil.getJobContext(getTestConfig());
    }

    @Override
    protected String[] getOverlay() {
        return new String[] { "src/test/resources/ut_meta/topn_with_chinese" };
    }

    @Override
    @After
    public void tearDown() throws Exception {
        JobContextUtil.cleanUp();
        cleanupTestMetadata();
    }

    @Override
    public String getProject() {
        return "topn_with_chinese";
    }

    @Test
    public void testTopNWithChinese() throws Exception {
        String dfID = "a6e11e79-40e1-40a6-927b-2c05cfbba81e";
        NDataflowManager dsMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
        NDataflow df = dsMgr.getDataflow(dfID);
        val layouts = df.getIndexPlan().getAllLayouts();
        indexDataConstructor.buildIndex(dfID, SegmentRange.TimePartitionedSegmentRange.createInfinite(),
                Sets.newLinkedHashSet(layouts), true);
        String sqlHitCube = "select city, sum(int_id) as a from topn_with_chinese group by city order by a desc limit 10";
        List<String> hitCubeResult = ExecAndComp.queryModelWithoutCompute(getProject(), sqlHitCube) //
                .collectAsList().stream().map(Row::toString).collect(Collectors.toList());
        Assert.assertEquals(3, hitCubeResult.size());
        Assert.assertEquals("[广州,15.0]", hitCubeResult.get(0));
        Assert.assertEquals("[上海,10.0]", hitCubeResult.get(1));
        Assert.assertEquals("[北京,6.0]", hitCubeResult.get(2));
    }
}

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

package org.apache.kylin.engine.spark.metadata.cube.model;

/*
TODO[xyxy]
import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.job.lock.MockJobLock;
import org.apache.spark.sql.SparderEnv;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.engine.spark.job.KylinBuildEnv;
import org.apache.kylin.metadata.cube.cuboid.NForestSpanningTree;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;

import static org.apache.kylin.common.util.AbstractKylinTestCase.getTestConfig;

@SuppressWarnings("serial")
public class NSpanningTreeCubingJobTest extends NLocalWithSparkSessionTest {

    @Before
    public void setup() {
        this.createTestMetadata("src/test/resources/ut_meta/spanning_tree_build");
        ss.sparkContext().setLogLevel("ERROR");
        System.setProperty("kylin.job.scheduler.poll-interval-second", "1");
        System.setProperty("kylin.engine.spark.cache-threshold", "2");

        NDefaultScheduler.destroyInstance();
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(getProject());
        scheduler.init(new JobEngineConfig(getTestConfig()), new MockJobLock());
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
    }

    @After
    public void after() {
        NDefaultScheduler.destroyInstance();
        cleanupTestMetadata();
        System.clearProperty("kylin.job.scheduler.poll-interval-second");
        System.clearProperty("kylin.engine.spark.cache-threshold");
    }

    */
/** 说明
     * index entity: 100000, dimBitSet={0, 1, 2}, measureBitSet={100000}
     * index entity: 10000,  dimBitSet={0, 1}, measureBitSet={100000}
     * index entity: 20000,  dimBitSet={0, 2}, measureBitSet={100000}
     *
     * index entity: 200000, dimBitSet={0, 1, 3, 4}, measureBitSet={100000}
     * index entity: 30000,  dimBitSet={0, 3, 4}, measureBitSet={100000}
     * index entity: 40000,  dimBitSet={0, 3}, measureBitSet={100000}
     * index entity: 0,      dimBitSet={0}, measureBitSet={100000}
     *
     *
     *  最后生成的树:
     *  roots                    1000000                    2000000
     *                            /   \                        |
     *  level1               10000   20000                   30000
     *                                                         |
     *  level2                                               40000
     *                                                         |
     *  level3                                                 0
     *//*

    @Test
    public void testBuild() throws Exception {
        fullBuildCube("75080248-367e-4bac-9fd7-322517ee0227", getProject());
        // use query to insure build is successful.
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        List<Pair<String, String>> query = new ArrayList<>();
        query.add(Pair.newPair("can_answer", "select count(*) from TEST_KYLIN_FACT"));
        NExecAndComp.execAndCompare(query, getProject(), NExecAndComp.CompareLevel.NONE, "left");

        String segId = getSegId();

        NForestSpanningTree st = (NForestSpanningTree) KylinBuildEnv.get().buildJobInfos().getSpanningTree(segId);
        NForestSpanningTree.TreeNode leafNode = st.getNodesMap().get(0L);
        Assert.assertEquals(3, leafNode.getLevel());
        Assert.assertEquals(40000L, leafNode.getParent().getIndexEntity().getId());
    }

    private String getSegId() {
        NDataflowManager dsMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
        NDataflow df = dsMgr.getDataflow("75080248-367e-4bac-9fd7-322517ee0227");
        return df.getSegments().getFirstSegment().getId();
    }

    @Override
    public String getProject() {
        return "spanning_tree";
    }

}
*/

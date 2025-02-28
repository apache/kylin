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
import java.util.Set;

import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.cube.cuboid.NSpanningTree;
import org.apache.kylin.metadata.cube.cuboid.NSpanningTreeFactory;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.assertj.core.util.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class NSpanningTreeCubingJobTest extends NLocalWithSparkSessionTest {

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        ss.sparkContext().setLogLevel("ERROR");
        overwriteSystemProp("kylin.job.scheduler.poll-interval-second", "1");
        overwriteSystemProp("kylin.engine.spark.cache-threshold", "2");

        this.createTestMetadata("src/test/resources/ut_meta/spanning_tree_build");
    }

    @Override
    protected String[] getOverlay() {
        return new String[] { "src/test/resources/ut_meta/spanning_tree_build" };
    }

    @Override
    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

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
     */
    @Test
    public void testBuild() {
        final String dataflowId = "75080248-367e-4bac-9fd7-322517ee0227";

        NDataflowManager manager = NDataflowManager.getInstance(getTestConfig(), getProject());
        NDataflow dataflow = manager.getDataflow(dataflowId);

        Set<LayoutEntity> layouts = Sets.newLinkedHashSet(dataflow.getIndexPlan().getAllLayouts());
        NSpanningTree spanningTree = NSpanningTreeFactory.fromLayouts(layouts, dataflowId);
        List<IndexEntity> roots = Lists.newArrayList(spanningTree.getRootIndexEntities());
        Assert.assertEquals(2, roots.size());
    }

    @Override
    public String getProject() {
        return "spanning_tree";
    }

}

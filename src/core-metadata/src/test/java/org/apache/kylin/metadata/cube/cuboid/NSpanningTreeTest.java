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

package org.apache.kylin.metadata.cube.cuboid;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.cube.CubeTestUtils;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import lombok.val;
import lombok.var;

public class NSpanningTreeTest extends NLocalFileMetadataTestCase {
    private final String projectDefault = "default";

    @Before
    public void setup() throws Exception {
        this.createTestMetadata("src/test/resources/ut_meta/spanning_tree");
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testBasic() {
        NIndexPlanManager mgr = NIndexPlanManager.getInstance(getTestConfig(), projectDefault);
        IndexPlan indexPlan = mgr.getIndexPlanByModelAlias("nmodel_basic");
        Assert.assertNotNull(indexPlan);

        NSpanningTree spanningTree = NSpanningTreeFactory.fromIndexPlan(indexPlan);
        Assert.assertTrue(spanningTree instanceof NForestSpanningTree);
        Assert.assertNotNull(spanningTree);

        Assert.assertEquals(indexPlan.getAllIndexes().size(), spanningTree.getCuboidCount());
        Assert.assertEquals(indexPlan.getAllIndexes().size(), spanningTree.getAllIndexEntities().size());

        IndexEntity cuboidDesc = spanningTree.getIndexEntity(10000L);
        Assert.assertNotNull(cuboidDesc);
        Assert.assertTrue(indexPlan.getAllIndexes().contains(cuboidDesc));
        Assert.assertEquals(2, cuboidDesc.getLayouts().size());

        LayoutEntity cuboidLayout = spanningTree.getLayoutEntity(10001L);
        Assert.assertNotNull(cuboidLayout);
        Assert.assertSame(spanningTree.getIndexEntity(10000L).getLayouts().get(0), cuboidLayout);

        IndexEntity cubeCuboidRoot = spanningTree.getIndexEntity(1000000L);
        IndexEntity tableIndexCuboidRoot = spanningTree.getIndexEntity(20000020000L);

        Assert.assertEquals(3, spanningTree.getRootIndexEntities().size());
        Assert.assertTrue(spanningTree.getRootIndexEntities().contains(tableIndexCuboidRoot));
        Assert.assertTrue(spanningTree.getRootIndexEntities().contains(cubeCuboidRoot));
    }

    /** 说明
     * index entity: 20000000000 dim={0, 1, 2, 4, 5}, measure={}, rows:10000
     * index entity: 20000010000 dim={0, 1, 2}, measure={}, rows:10000
     *
     * index entity: 1000000     dim={0, 1, 2, 3, 4, 5, 6, 7, 8}, measure={100000, 100001, 100002, 100003, 100004, 100005, 100006}, rows:10000
     * index entity: 10000       dim={0, 1, 2}, measure={100000, 100001, 100002}, rows:100
     * index entity: 20000       dim={0, 1, 3}, measure={100000, 100001, 100002, 100003}, rows:5000
     * index entity: 30000       dim={0, 1, 4}, measure={100000, 100001, 100002, 100003, 100004}, rows:3000
     * index entity: 40000       dim={0, 1, 5}, measure={100000, 100001, 100002, 100003}, rows:3000
     * index entity: 50000       dim={}, measure={100000}, rows:100
     * index entity: 0           dim={0, 1}, measure={100000, 100001, 100002, 100003}, rows:10
     *
     *  最后生成的树:
     *  roots                    1000000                       20000000000
     *                   /      |      |      \                     |
     *  level1        10000   20000  30000   40000             20000010000
     *                  |              |
     *  level2        50000            0
    */
    @Test
    public void testFindDirectChildrenByIndex() {
        val mgr = NIndexPlanManager.getInstance(getTestConfig(), projectDefault);
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), projectDefault);
        val segs = dfMgr.getDataflow("0674f455-c7bd-4d8c-b0e3-374f3d26c315").getSegments();
        Assert.assertEquals(1, segs.size());

        val plan = mgr.getIndexPlanByModelAlias("test_spanning_tree");
        val st = NSpanningTreeFactory.fromIndexPlan(plan);

        val roots = new ArrayList<IndexEntity>(st.getRootIndexEntities());
        // decide the children of roots(level 0)
        st.decideTheNextLayer(roots, segs.get(0));

        // decide the children of next layer(levle 1)
        roots.forEach(root -> {
            val children = st.getChildrenByIndexPlan(root);
            st.decideTheNextLayer(children, segs.get(0));
        });

        // roots (level 0)
        Assert.assertEquals(2, roots.size());

        val r1 = roots.get(0);
        val r2 = roots.get(1);
        Assert.assertEquals(20000000000L, r1.getId());
        Assert.assertEquals(1000000L, r2.getId());

        // level 1
        val r1Children = new ArrayList<IndexEntity>(st.getChildrenByIndexPlan(r1));
        val r2Children = new ArrayList<IndexEntity>(st.getChildrenByIndexPlan(r2));

        Assert.assertEquals(1, r1Children.size());
        Assert.assertEquals(20000010000L, r1Children.get(0).getId());

        Assert.assertEquals(4, r2Children.size());
        Assert.assertEquals(10000L, r2Children.get(0).getId());
        Assert.assertEquals(20000L, r2Children.get(1).getId());
        Assert.assertEquals(30000L, r2Children.get(2).getId());
        Assert.assertEquals(40000L, r2Children.get(3).getId());

        // level 2
        Assert.assertEquals(1, st.getChildrenByIndexPlan(r2Children.get(0)).size());
        Assert.assertEquals(0, st.getChildrenByIndexPlan(r2Children.get(1)).size());
        Assert.assertEquals(1, st.getChildrenByIndexPlan(r2Children.get(2)).size());
        Assert.assertEquals(0, st.getChildrenByIndexPlan(r2Children.get(3)).size());

        Assert.assertEquals(0L, new ArrayList<>(st.getChildrenByIndexPlan(r2Children.get(2))).get(0).getId());
        Assert.assertEquals(50000L, new ArrayList<>(st.getChildrenByIndexPlan(r2Children.get(0))).get(0).getId());

    }

    @Test
    public void testMaxCombination() {
        val mgr = NIndexPlanManager.getInstance(getTestConfig(), projectDefault);
        val indexPlan = mgr.getIndexPlanByModelAlias("nmodel_basic_inner");
        try {
            overwriteSystemProp("kylin.cube.aggrgroup.max-combination", "1");
            NSpanningTreeFactory.fromIndexPlan(indexPlan);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals(
                    "Too many cuboids for the cube. Cuboid combination reached 19 and limit is 10. Abort calculation.",
                    e.getCause().getCause().getMessage());
        }
    }

    @Test
    public void testSpanningTreeForSpecialIndex() throws IOException {
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), "default");
        var newPlan = JsonUtil.readValue(getClass().getResourceAsStream("/empty_cube.json"), IndexPlan.class);
        CubeTestUtils.createTmpModel(getTestConfig(), newPlan);
        newPlan = indexPlanManager.createIndexPlan(newPlan);

        val st = NSpanningTreeFactory.fromIndexPlan(newPlan);
        Assert.assertEquals(0, st.getCuboids().size());
        Assert.assertEquals(0, st.getAllIndexEntities().size());
        Assert.assertEquals(0, st.getCuboidCount());
    }
}

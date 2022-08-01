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

package org.apache.kylin.metadata.cube.model;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;

public class IndexPlanLayoutRemoveTest extends NLocalFileMetadataTestCase {
    private String projectDefault = "default";

    @Before
    public void setup() throws Exception {
        this.createTestMetadata("src/test/resources/ut_meta/flexible_seg_index");
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testRemoveLayout() {
        NIndexPlanManager indexMgr = NIndexPlanManager.getInstance(getTestConfig(), projectDefault);
        IndexPlan indexPlan = indexMgr.getIndexPlanByModelAlias("model1");
        Assert.assertNotNull(indexPlan);

        NDataflowManager dataflowMgr = NDataflowManager.getInstance(getTestConfig(), projectDefault);

        Assert.assertEquals(1, dataflowMgr.getDataflow(indexPlan.getId())
                .getSegment("14fcab15-09a4-4308-a604-1735693dc373").getLayoutSize());
        Assert.assertEquals(1, dataflowMgr.getDataflow(indexPlan.getId())
                .getSegment("fe0afd9e-97d4-47c5-b439-883d0921afa1").getLayoutSize());

        indexMgr.updateIndexPlan(indexPlan.getId(), copyForWrite -> {
            copyForWrite.removeLayouts(Sets.newHashSet(20000000001L), true, true);
        });
        Assert.assertEquals(0, dataflowMgr.getDataflow(indexPlan.getId())
                .getSegment("14fcab15-09a4-4308-a604-1735693dc373").getLayoutSize());
        Assert.assertEquals(1, dataflowMgr.getDataflow(indexPlan.getId())
                .getSegment("fe0afd9e-97d4-47c5-b439-883d0921afa1").getLayoutSize());

        indexMgr.updateIndexPlan(indexPlan.getId(), copyForWrite -> {
            copyForWrite.removeLayouts(Sets.newHashSet(1L), true, true);
        });
        Assert.assertEquals(0, dataflowMgr.getDataflow(indexPlan.getId())
                .getSegment("14fcab15-09a4-4308-a604-1735693dc373").getLayoutSize());
        Assert.assertEquals(0, dataflowMgr.getDataflow(indexPlan.getId())
                .getSegment("fe0afd9e-97d4-47c5-b439-883d0921afa1").getLayoutSize());
    }
}

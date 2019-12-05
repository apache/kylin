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

package io.kyligence.kap.engine.spark.job;

import java.util.List;
import java.util.Set;

import org.apache.kylin.metadata.model.Segments;
import org.junit.Assert;
import org.junit.Test;
import org.spark_project.guava.collect.Sets;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;

public class NSparkCubingUtilTest extends NLocalWithSparkSessionTest {

    @Test
    public void testToSegmentIds() {
        Set<String> expectedSegIds = Sets.newLinkedHashSet();
        expectedSegIds.add("2");
        expectedSegIds.add("1");
        expectedSegIds.add("3");
        NDataSegment firstSeg = new NDataSegment();
        NDataSegment secondSeg = new NDataSegment();
        NDataSegment thirdSeg = new NDataSegment();
        firstSeg.setId("2");
        secondSeg.setId("1");
        thirdSeg.setId("3");

        Assert.assertEquals(expectedSegIds, NSparkCubingUtil.toSegmentIds("2,1,3"));

        Segments<NDataSegment> segments = new Segments<>();
        segments.add(firstSeg);
        segments.add(secondSeg);
        segments.add(thirdSeg);
        segments.add(firstSeg);
        Assert.assertEquals(expectedSegIds, NSparkCubingUtil.toSegmentIds(segments));

        Set<NDataSegment> segmentSet = Sets.newLinkedHashSet();
        segmentSet.add(firstSeg);
        segmentSet.add(secondSeg);
        segmentSet.add(thirdSeg);
        segmentSet.add(firstSeg);
        Assert.assertEquals(expectedSegIds, NSparkCubingUtil.toSegmentIds(segmentSet));
    }

    @Test
    public void testToLayoutIds() {
        Set<Long> expectedLayoutIds = Sets.newLinkedHashSet();
        expectedLayoutIds.add(20001L);
        expectedLayoutIds.add(10001L);
        expectedLayoutIds.add(10002L);
        Assert.assertEquals(expectedLayoutIds, NSparkCubingUtil.toLayoutIds("20001,10001,10002"));

        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        final List<IndexPlan> indexPlans = indexPlanManager.listAllIndexPlans();
        Set<LayoutEntity> layouts = Sets.newLinkedHashSet();
        LayoutEntity one = indexPlans.get(0).getCuboidLayout(20001L);
        LayoutEntity two = indexPlans.get(0).getCuboidLayout(10001L);
        LayoutEntity three = indexPlans.get(0).getCuboidLayout(10002L);
        LayoutEntity four = indexPlans.get(0).getCuboidLayout(20001L);
        layouts.add(one);
        layouts.add(two);
        layouts.add(three);
        layouts.add(four);
        Assert.assertEquals(expectedLayoutIds, NSparkCubingUtil.toLayoutIds(layouts));

        Assert.assertEquals(layouts, NSparkCubingUtil.toLayouts(indexPlans.get(0), expectedLayoutIds));
    }
}

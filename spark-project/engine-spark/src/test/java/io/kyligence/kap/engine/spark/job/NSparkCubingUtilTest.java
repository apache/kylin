/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
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

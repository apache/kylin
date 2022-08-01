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

package org.apache.kylin.job.common;

import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.SegmentStatusEnumToDisplay;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.junit.TimeZoneTestRunner;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import lombok.val;

@RunWith(TimeZoneTestRunner.class)
public class SegmentsTest {

    @After
    public void teardown() {
        Mockito.clearAllCaches();
    }

    @Test
    public void testGetSegmentStatusToDisplay_Building() {
        Segments segments = new Segments();
        val seg = NDataSegment.empty();
        seg.setSegmentRange(new SegmentRange.TimePartitionedSegmentRange(0L, 10L));
        seg.setStatus(SegmentStatusEnum.NEW);
        segments.add(seg);
        SegmentStatusEnumToDisplay status = SegmentUtil.getSegmentStatusToDisplay(segments, seg, null);
        Assert.assertEquals(status, SegmentStatusEnumToDisplay.LOADING);

        seg.setStatus(SegmentStatusEnum.READY);
        Mockito.mockStatic(SegmentUtil.class);
        Mockito.when(SegmentUtil.getSegmentStatusToDisplay(segments, seg, null)).thenCallRealMethod();
        Mockito.when(SegmentUtil.anyIndexJobRunning(seg)).thenReturn(true);
        Assert.assertEquals(status, SegmentStatusEnumToDisplay.LOADING);
    }

    @Test
    public void testGetSegmentStatusToDisplay_Ready() {
        Segments segments = new Segments();
        val seg = NDataSegment.empty();
        seg.setDataflow(new NDataflow());
        seg.setSegmentRange(new SegmentRange.TimePartitionedSegmentRange(0L, 10L));
        seg.setStatus(SegmentStatusEnum.READY);
        segments.add(seg);
        Mockito.mockStatic(SegmentUtil.class);
        Mockito.when(SegmentUtil.getSegmentStatusToDisplay(segments, seg, null)).thenCallRealMethod();
        Mockito.when(SegmentUtil.anyIndexJobRunning(seg)).thenReturn(false);
        SegmentStatusEnumToDisplay status = SegmentUtil.getSegmentStatusToDisplay(segments, seg, null);
        Assert.assertEquals(status, SegmentStatusEnumToDisplay.ONLINE);
    }

    @Test
    public void testGetSegmentStatusToDisplay_Refreshing() {
        Segments segments = new Segments();
        val seg = NDataSegment.empty();
        seg.setId(RandomUtil.randomUUIDStr());
        seg.setSegmentRange(new SegmentRange.TimePartitionedSegmentRange(0L, 10L));
        seg.setStatus(SegmentStatusEnum.READY);
        segments.add(seg);

        val newSeg = NDataSegment.empty();
        newSeg.setId(RandomUtil.randomUUIDStr());
        newSeg.setSegmentRange(new SegmentRange.TimePartitionedSegmentRange(0L, 10L));
        newSeg.setStatus(SegmentStatusEnum.NEW);
        segments.add(newSeg);
        SegmentStatusEnumToDisplay status = SegmentUtil.getSegmentStatusToDisplay(segments, newSeg, null);
        Assert.assertEquals(status, SegmentStatusEnumToDisplay.REFRESHING);

        SegmentStatusEnumToDisplay status2 = SegmentUtil.getSegmentStatusToDisplay(segments, seg, null);
        Assert.assertEquals(status2, SegmentStatusEnumToDisplay.LOCKED);
    }

    @Test
    public void testGetSegmentStatusToDisplay_Warn_Refreshing() {
        Segments segments = new Segments();
        val seg = NDataSegment.empty();
        seg.setId(RandomUtil.randomUUIDStr());
        seg.setSegmentRange(new SegmentRange.TimePartitionedSegmentRange(0L, 10L));
        seg.setStatus(SegmentStatusEnum.WARNING);
        segments.add(seg);

        val newSeg = NDataSegment.empty();
        newSeg.setId(RandomUtil.randomUUIDStr());
        newSeg.setSegmentRange(new SegmentRange.TimePartitionedSegmentRange(0L, 10L));
        newSeg.setStatus(SegmentStatusEnum.NEW);
        segments.add(newSeg);
        SegmentStatusEnumToDisplay status = SegmentUtil.getSegmentStatusToDisplay(segments, newSeg, null);
        Assert.assertEquals(status, SegmentStatusEnumToDisplay.REFRESHING);

        SegmentStatusEnumToDisplay status2 = SegmentUtil.getSegmentStatusToDisplay(segments, seg, null);
        Assert.assertEquals(status2, SegmentStatusEnumToDisplay.LOCKED);
    }

    @Test
    public void testGetSegmentStatusToDisplay_Warn() {
        Segments segments = new Segments();
        val seg = NDataSegment.empty();
        seg.setId(RandomUtil.randomUUIDStr());
        seg.setSegmentRange(new SegmentRange.TimePartitionedSegmentRange(0L, 10L));
        seg.setStatus(SegmentStatusEnum.WARNING);
        segments.add(seg);

        Mockito.mockStatic(SegmentUtil.class);
        Mockito.when(SegmentUtil.getSegmentStatusToDisplay(segments, seg, null)).thenCallRealMethod();
        Mockito.when(SegmentUtil.anyIndexJobRunning(seg)).thenReturn(false);
        SegmentStatusEnumToDisplay status = SegmentUtil.getSegmentStatusToDisplay(segments, seg, null);
        Assert.assertEquals(SegmentStatusEnumToDisplay.WARNING, status);
        Mockito.when(SegmentUtil.getSegmentStatusToDisplay(segments, seg, null)).thenCallRealMethod();
        Mockito.when(SegmentUtil.anyIndexJobRunning(seg)).thenReturn(true);
        SegmentStatusEnumToDisplay status2 = SegmentUtil.getSegmentStatusToDisplay(segments, seg, null);
        Assert.assertEquals(SegmentStatusEnumToDisplay.LOADING, status2);
    }

    @Test
    public void testGetSegmentStatusToDisplay_Merging() {
        Segments segments = new Segments();
        val seg = NDataSegment.empty();
        seg.setId(RandomUtil.randomUUIDStr());
        seg.setSegmentRange(new SegmentRange.TimePartitionedSegmentRange(0L, 10L));
        seg.setStatus(SegmentStatusEnum.READY);
        segments.add(seg);

        val seg2 = NDataSegment.empty();
        seg2.setId(RandomUtil.randomUUIDStr());
        seg2.setSegmentRange(new SegmentRange.TimePartitionedSegmentRange(0L, 20L));
        seg2.setStatus(SegmentStatusEnum.READY);
        segments.add(seg2);

        val newSeg = NDataSegment.empty();
        newSeg.setId(RandomUtil.randomUUIDStr());
        newSeg.setSegmentRange(new SegmentRange.TimePartitionedSegmentRange(0L, 20L));
        newSeg.setStatus(SegmentStatusEnum.NEW);
        segments.add(newSeg);

        SegmentStatusEnumToDisplay status = SegmentUtil.getSegmentStatusToDisplay(segments, newSeg, null);
        Assert.assertEquals(status, SegmentStatusEnumToDisplay.MERGING);

        SegmentStatusEnumToDisplay status2 = SegmentUtil.getSegmentStatusToDisplay(segments, seg, null);
        Assert.assertEquals(status2, SegmentStatusEnumToDisplay.LOCKED);

        SegmentStatusEnumToDisplay status3 = SegmentUtil.getSegmentStatusToDisplay(segments, seg2, null);
        Assert.assertEquals(status3, SegmentStatusEnumToDisplay.LOCKED);

    }

    public NDataSegment newReadySegment(Long startTime, Long endTime) {
        val seg = NDataSegment.empty();
        seg.setId(RandomUtil.randomUUIDStr());
        seg.setSegmentRange(new SegmentRange.TimePartitionedSegmentRange(startTime, endTime));
        seg.setStatus(SegmentStatusEnum.READY);
        return seg;
    }

}

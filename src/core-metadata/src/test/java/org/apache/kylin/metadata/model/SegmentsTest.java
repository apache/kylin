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

package org.apache.kylin.metadata.model;

import java.util.HashMap;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.util.TempMetadataBuilder;
import org.apache.kylin.junit.TimeZoneTestRunner;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import lombok.val;

@RunWith(TimeZoneTestRunner.class)
public class SegmentsTest {
    KylinConfig config;

    @Before
    public void setUp() throws Exception {
        String tempMetadataDir = TempMetadataBuilder.prepareLocalTempMetadata();
        KylinConfig.setKylinConfigForLocalTest(tempMetadataDir);
        config = KylinConfig.getInstanceFromEnv();
    }

    @Test
    public void testGetMergeEnd_ByHour() {
        Segments segments = new Segments();
        long end = segments.getMergeEnd(DateFormat.stringToMillis("2012-02-10 02:03:00"), AutoMergeTimeEnum.HOUR);
        Assert.assertEquals(DateFormat.stringToMillis("2012-02-10 03:00:00"), end);

        end = segments.getMergeEnd(DateFormat.stringToMillis("2012-02-10 03:00:00"), AutoMergeTimeEnum.HOUR);
        Assert.assertEquals(DateFormat.stringToMillis("2012-02-10 04:00:00"), end);
    }

    @Test
    public void testGetRetentionStart_ByHour() {
        Segments segments = new Segments();
        long start = segments.getRetentionEnd(DateFormat.stringToMillis("2012-02-10 02:03:00"), AutoMergeTimeEnum.HOUR,
                -1);
        Assert.assertEquals(DateFormat.stringToMillis("2012-02-10 01:03:00"), start);

        start = segments.getRetentionEnd(DateFormat.stringToMillis("2012-02-10 03:00:00"), AutoMergeTimeEnum.HOUR, -1);
        Assert.assertEquals(DateFormat.stringToMillis("2012-02-10 02:00:00"), start);
    }

    @Test
    public void testGetMergeEnd_ByDay() {
        Segments segments = new Segments();
        long end = segments.getMergeEnd(DateFormat.stringToMillis("2012-02-10 09:00:00"), AutoMergeTimeEnum.DAY);
        Assert.assertEquals(DateFormat.stringToMillis("2012-02-11 00:00:00"), end);

        end = segments.getMergeEnd(DateFormat.stringToMillis("2012-02-29 09:00:00"), AutoMergeTimeEnum.DAY);
        Assert.assertEquals(DateFormat.stringToMillis("2012-03-01 00:00:00"), end);
    }

    @Test
    public void testGetRetentionStart_ByDay() {
        Segments segments = new Segments();
        long start = segments.getRetentionEnd(DateFormat.stringToMillis("2012-02-10 02:03:00"), AutoMergeTimeEnum.DAY,
                -1);
        Assert.assertEquals(DateFormat.stringToMillis("2012-02-09 02:03:00"), start);

        start = segments.getRetentionEnd(DateFormat.stringToMillis("2012-02-01 11:00:00"), AutoMergeTimeEnum.DAY, -2);
        Assert.assertEquals(DateFormat.stringToMillis("2012-01-30 11:00:00"), start);
    }

    @Test
    public void testGetMergeEnd_ByWeek_FirstDayOfWeekMonday() {
        Segments segments = new Segments();
        long end = segments.getMergeEnd(DateFormat.stringToMillis("2012-02-05 01:00:00"), AutoMergeTimeEnum.WEEK);
        Assert.assertEquals(DateFormat.stringToMillis("2012-02-06 00:00:00"), end);

        end = segments.getMergeEnd(DateFormat.stringToMillis("2012-02-06 00:00:00"), AutoMergeTimeEnum.WEEK);
        Assert.assertEquals(DateFormat.stringToMillis("2012-02-13 00:00:00"), end);

        end = segments.getMergeEnd(DateFormat.stringToMillis("2012-02-08 00:00:00"), AutoMergeTimeEnum.WEEK);
        Assert.assertEquals(DateFormat.stringToMillis("2012-02-13 00:00:00"), end);
    }

    @Test
    public void testGetRetentionStart_ByWeek_FirstDayOfWeekMonday() {
        Segments segments = new Segments();
        long start = segments.getRetentionEnd(DateFormat.stringToMillis("2012-02-05 09:00:00"), AutoMergeTimeEnum.WEEK,
                -1);
        Assert.assertEquals(DateFormat.stringToMillis("2012-01-29 09:00:00"), start);
    }

    @Test
    public void testGetMergeEnd_ByWeek_FirstDayOfWeekSunday() {
        config.setProperty("kylin.metadata.first-day-of-week", "sunday");
        Segments segments = new Segments();
        long end = segments.getMergeEnd(DateFormat.stringToMillis("2012-02-05 01:00:00"), AutoMergeTimeEnum.WEEK);
        Assert.assertEquals(DateFormat.stringToMillis("2012-02-12 00:00:00"), end);

        end = segments.getMergeEnd(DateFormat.stringToMillis("2012-02-06 00:00:00"), AutoMergeTimeEnum.WEEK);
        Assert.assertEquals(DateFormat.stringToMillis("2012-02-12 00:00:00"), end);

        end = segments.getMergeEnd(DateFormat.stringToMillis("2012-02-11 04:00:00"), AutoMergeTimeEnum.WEEK);
        Assert.assertEquals(DateFormat.stringToMillis("2012-02-12 00:00:00"), end);
        config.setProperty("kylin.metadata.first-day-of-week", "monday");

    }

    @Test
    public void testGetMergeEnd_ByWeek_AWeekOverlapTwoMonth() {
        Segments segments = new Segments();
        long end = segments.getMergeEnd(DateFormat.stringToMillis("2012-02-28 00:00:00"), AutoMergeTimeEnum.WEEK);
        Assert.assertEquals(DateFormat.stringToMillis("2012-03-01 00:00:00"), end);

    }

    @Test
    public void testGetMergeEnd_ByMonth() {
        Segments segments = new Segments();
        long end = segments.getMergeEnd(DateFormat.stringToMillis("2012-02-28 00:00:00"), AutoMergeTimeEnum.MONTH);
        Assert.assertEquals(DateFormat.stringToMillis("2012-03-01 00:00:00"), end);

        end = segments.getMergeEnd(DateFormat.stringToMillis("2012-03-01 00:00:00"), AutoMergeTimeEnum.MONTH);
        Assert.assertEquals(DateFormat.stringToMillis("2012-04-01 00:00:00"), end);

    }

    @Test
    public void testGetMergeEnd_ByQuarter() {
        Segments segments = new Segments();
        long end = segments.getMergeEnd(DateFormat.stringToMillis("2012-02-28 00:00:00"), AutoMergeTimeEnum.QUARTER);
        Assert.assertEquals(DateFormat.stringToMillis("2012-04-01 00:00:00"), end);

        end = segments.getMergeEnd(DateFormat.stringToMillis("2012-03-01 00:00:00"), AutoMergeTimeEnum.QUARTER);
        Assert.assertEquals(DateFormat.stringToMillis("2012-04-01 00:00:00"), end);

        end = segments.getMergeEnd(DateFormat.stringToMillis("2012-04-28 00:00:00"), AutoMergeTimeEnum.QUARTER);
        Assert.assertEquals(DateFormat.stringToMillis("2012-07-01 00:00:00"), end);

        end = segments.getMergeEnd(DateFormat.stringToMillis("2012-05-01 00:00:00"), AutoMergeTimeEnum.QUARTER);
        Assert.assertEquals(DateFormat.stringToMillis("2012-07-01 00:00:00"), end);

    }

    @Test
    public void testGetRetentionStart_ByMonth() {
        Segments segments = new Segments();
        long start = segments.getRetentionEnd(DateFormat.stringToMillis("2012-03-31 00:00:00"), AutoMergeTimeEnum.MONTH,
                -1);
        Assert.assertEquals(DateFormat.stringToMillis("2012-02-29 00:00:00"), start);
    }

    @Test
    public void testGetMergeEnd_ByYear() {
        Segments segments = new Segments();
        long end = segments.getMergeEnd(DateFormat.stringToMillis("2012-02-28 00:00:00"), AutoMergeTimeEnum.YEAR);
        Assert.assertEquals(DateFormat.stringToMillis("2013-01-01 00:00:00"), end);

        end = segments.getMergeEnd(DateFormat.stringToMillis("2013-01-01 00:00:00"), AutoMergeTimeEnum.YEAR);
        Assert.assertEquals(DateFormat.stringToMillis("2014-01-01 00:00:00"), end);

    }

    @Test
    public void testGetRetentionStart_ByYear() {
        Segments segments = new Segments();
        long start = segments.getRetentionEnd(DateFormat.stringToMillis("2012-02-28 00:00:00"), AutoMergeTimeEnum.YEAR,
                -1);
        Assert.assertEquals(DateFormat.stringToMillis("2011-02-28 00:00:00"), start);
    }

    private Map<Integer, Long> createKafkaPartitionOff(int partition, Long offset) {
        Map<Integer, Long> map = new HashMap<Integer, Long>();
        map.put(partition, offset);
        return map;
    }

    public NDataSegment newReadySegment(Long startTime, Long endTime) {
        val seg = NDataSegment.empty();
        seg.setId(RandomUtil.randomUUIDStr());
        seg.setSegmentRange(new SegmentRange.TimePartitionedSegmentRange(startTime, endTime));
        seg.setStatus(SegmentStatusEnum.READY);
        return seg;
    }

    @Test
    public void testRemoveSegmentsByVolatileRange() {
        Segments sourceSegments = new Segments();
        sourceSegments.add(newReadySegment(1559232000000L, 1561824000000L));
        sourceSegments.add(newReadySegment(1561824000000L, 1564502400000L));
        sourceSegments.add(newReadySegment(1564502400000L, 1567180800000L));

        Segments segmentsYear = sourceSegments.getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING);
        VolatileRange volatileRange = new VolatileRange();
        volatileRange.setVolatileRangeEnabled(true);
        volatileRange.setVolatileRangeNumber(1);
        volatileRange.setVolatileRangeType(AutoMergeTimeEnum.YEAR);
        segmentsYear.removeSegmentsByVolatileRange(segmentsYear, volatileRange);
        Assert.assertEquals(0, segmentsYear.size());

        Segments segmentsMonth = sourceSegments.getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING);
        VolatileRange volatileRangeMonth = new VolatileRange();
        volatileRangeMonth.setVolatileRangeEnabled(true);
        volatileRangeMonth.setVolatileRangeNumber(2);
        volatileRangeMonth.setVolatileRangeType(AutoMergeTimeEnum.MONTH);
        segmentsMonth.removeSegmentsByVolatileRange(segmentsMonth, volatileRangeMonth);
        Assert.assertEquals(1, segmentsMonth.size());

        Segments segmentsWeek = sourceSegments.getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING);
        VolatileRange volatileRangeWeek = new VolatileRange();
        volatileRangeWeek.setVolatileRangeEnabled(true);
        volatileRangeWeek.setVolatileRangeNumber(3);
        volatileRangeWeek.setVolatileRangeType(AutoMergeTimeEnum.WEEK);
        segmentsWeek.removeSegmentsByVolatileRange(segmentsWeek, volatileRangeWeek);
        Assert.assertEquals(2, segmentsWeek.size());

        Segments segmentsDay = sourceSegments.getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING);
        VolatileRange volatileRangeDay = new VolatileRange();
        volatileRangeDay.setVolatileRangeEnabled(true);
        volatileRangeDay.setVolatileRangeNumber(3);
        volatileRangeDay.setVolatileRangeType(AutoMergeTimeEnum.DAY);
        segmentsDay.removeSegmentsByVolatileRange(segmentsDay, volatileRangeDay);
        Assert.assertEquals(2, segmentsDay.size());
    }

    @Test
    public void testSplitVolatileRanges() {
        ISegment segment = newReadySegment(1527782400000L, 1577808000000L);

        VolatileRange volatileRangeDay = new VolatileRange();
        volatileRangeDay.setVolatileRangeEnabled(true);
        volatileRangeDay.setVolatileRangeNumber(375);
        volatileRangeDay.setVolatileRangeType(AutoMergeTimeEnum.DAY);
        val resultDay = Segments.splitVolatileRanges(segment.getSegRange(), volatileRangeDay);
        Assert.assertEquals(375, resultDay.getSecond().size());

        VolatileRange volatileRangeWeek = new VolatileRange();
        volatileRangeWeek.setVolatileRangeEnabled(true);
        volatileRangeWeek.setVolatileRangeNumber(4);
        volatileRangeWeek.setVolatileRangeType(AutoMergeTimeEnum.WEEK);
        val resultWeek = Segments.splitVolatileRanges(segment.getSegRange(), volatileRangeWeek);
        Assert.assertEquals(4, resultWeek.getSecond().size());

        VolatileRange volatileRangeYear = new VolatileRange();
        volatileRangeYear.setVolatileRangeEnabled(true);
        volatileRangeYear.setVolatileRangeNumber(3);
        volatileRangeYear.setVolatileRangeType(AutoMergeTimeEnum.YEAR);
        val resultYear = Segments.splitVolatileRanges(segment.getSegRange(), volatileRangeYear);
        Assert.assertEquals(2, resultYear.getSecond().size());

        VolatileRange volatileRangeMonth = new VolatileRange();
        volatileRangeMonth.setVolatileRangeEnabled(true);
        volatileRangeMonth.setVolatileRangeNumber(6);
        volatileRangeMonth.setVolatileRangeType(AutoMergeTimeEnum.MONTH);
        val resultMonth = Segments.splitVolatileRanges(segment.getSegRange(), volatileRangeMonth);
        Assert.assertEquals(6, resultMonth.getSecond().size());
    }

    @Test
    public void testMergeWeek() {
        Segments segments = new Segments();
        NDataSegment segment1 = newReadySegment(DateFormat.stringToMillis("1995-01-07 00:00:00"),
                DateFormat.stringToMillis("1995-01-08 00:00:00"));

        NDataSegment segment2 = newReadySegment(DateFormat.stringToMillis("1995-01-09 00:00:00"),
                DateFormat.stringToMillis("1995-01-12 00:00:00"));
        NDataSegment segment3 = newReadySegment(DateFormat.stringToMillis("1995-01-12 00:00:00"),
                DateFormat.stringToMillis("1995-01-13 00:00:00"));

        NDataSegment segment4 = newReadySegment(DateFormat.stringToMillis("1995-01-13 00:00:00"),
                DateFormat.stringToMillis("1995-01-16 00:00:00"));
        NDataSegment segment5 = newReadySegment(DateFormat.stringToMillis("1995-01-16 00:00:00"),
                DateFormat.stringToMillis("1995-01-17 00:00:00"));
        segments.add(segment1);
        segments.add(segment2);
        segments.add(segment3);
        segments.add(segment4);
        segments.add(segment5);
        SegmentRange segmentRange = segments.findMergeSegmentsRange(AutoMergeTimeEnum.WEEK);
        Assert.assertEquals(segmentRange.getStart(), DateFormat.stringToMillis("1995-01-09 00:00:00"));
        Assert.assertEquals(segmentRange.getEnd(), DateFormat.stringToMillis("1995-01-16 00:00:00"));
    }

    @Test
    public void testMergeHour() {
        Segments segments = new Segments();
        NDataSegment segment1 = newReadySegment(DateFormat.stringToMillis("1995-01-07 00:03:00"),
                DateFormat.stringToMillis("1995-01-07 00:56:00"));

        NDataSegment segment2 = newReadySegment(DateFormat.stringToMillis("1995-01-07 01:00:00"),
                DateFormat.stringToMillis("1995-01-07 01:04:00"));
        NDataSegment segment3 = newReadySegment(DateFormat.stringToMillis("1995-01-07 01:04:00"),
                DateFormat.stringToMillis("1995-01-07 01:13:00"));

        NDataSegment segment4 = newReadySegment(DateFormat.stringToMillis("1995-01-07 01:13:00"),
                DateFormat.stringToMillis("1995-01-07 02:00:00"));
        NDataSegment segment5 = newReadySegment(DateFormat.stringToMillis("1995-01-07 02:00:00"),
                DateFormat.stringToMillis("1995-01-07 02:24:00"));
        segments.add(segment1);
        segments.add(segment2);
        segments.add(segment3);
        segments.add(segment4);
        segments.add(segment5);
        SegmentRange segmentRange = segments.findMergeSegmentsRange(AutoMergeTimeEnum.HOUR);
        Assert.assertEquals(segmentRange.getStart(), DateFormat.stringToMillis("1995-01-07 01:00:00"));
        Assert.assertEquals(segmentRange.getEnd(), DateFormat.stringToMillis("1995-01-07 02:00:00"));
    }

    @Test
    public void testMergeDay() {
        Segments segments = new Segments();
        NDataSegment segment1 = newReadySegment(DateFormat.stringToMillis("1995-01-07 00:03:00"),
                DateFormat.stringToMillis("1995-01-08 00:00:00"));

        NDataSegment segment2 = newReadySegment(DateFormat.stringToMillis("1995-01-08 00:00:00"),
                DateFormat.stringToMillis("1995-01-08 01:04:00"));
        NDataSegment segment3 = newReadySegment(DateFormat.stringToMillis("1995-01-08 01:04:00"),
                DateFormat.stringToMillis("1995-01-08 09:13:00"));

        NDataSegment segment4 = newReadySegment(DateFormat.stringToMillis("1995-01-08 09:13:00"),
                DateFormat.stringToMillis("1995-01-09 00:00:00"));
        NDataSegment segment5 = newReadySegment(DateFormat.stringToMillis("1995-01-09 00:00:00"),
                DateFormat.stringToMillis("1995-01-09 02:24:00"));
        segments.add(segment1);
        segments.add(segment2);
        segments.add(segment3);
        segments.add(segment4);
        segments.add(segment5);
        SegmentRange segmentRange = segments.findMergeSegmentsRange(AutoMergeTimeEnum.DAY);
        Assert.assertEquals(segmentRange.getStart(), DateFormat.stringToMillis("1995-01-08 00:00:00"));
        Assert.assertEquals(segmentRange.getEnd(), DateFormat.stringToMillis("1995-01-09 00:00:00"));
    }

    @Test
    public void testMergeDayStartCase() {
        Segments segments = new Segments();
        NDataSegment segment1 = newReadySegment(DateFormat.stringToMillis("1995-01-07 00:03:00"),
                DateFormat.stringToMillis("1995-01-07 23:00:00"));

        NDataSegment segment2 = newReadySegment(DateFormat.stringToMillis("1995-01-08 01:00:00"),
                DateFormat.stringToMillis("1995-01-08 01:04:00"));
        NDataSegment segment3 = newReadySegment(DateFormat.stringToMillis("1995-01-08 01:04:00"),
                DateFormat.stringToMillis("1995-01-08 09:13:00"));

        NDataSegment segment4 = newReadySegment(DateFormat.stringToMillis("1995-01-08 09:13:00"),
                DateFormat.stringToMillis("1995-01-09 00:00:00"));
        NDataSegment segment5 = newReadySegment(DateFormat.stringToMillis("1995-01-09 00:00:00"),
                DateFormat.stringToMillis("1995-01-09 02:24:00"));
        segments.add(segment1);
        segments.add(segment2);
        segments.add(segment3);
        segments.add(segment4);
        segments.add(segment5);
        SegmentRange segmentRange = segments.findMergeSegmentsRange(AutoMergeTimeEnum.DAY);
        Assert.assertEquals(segmentRange.getStart(), DateFormat.stringToMillis("1995-01-08 01:00:00"));
        Assert.assertEquals(segmentRange.getEnd(), DateFormat.stringToMillis("1995-01-09 00:00:00"));

    }

    @Test
    public void testNotMergeDayMiddleError() {
        Segments segments = new Segments();
        NDataSegment segment1 = newReadySegment(DateFormat.stringToMillis("1995-01-07 00:03:00"),
                DateFormat.stringToMillis("1995-01-08 01:00:00"));
        NDataSegment segment2 = newReadySegment(DateFormat.stringToMillis("1995-01-08 00:01:00"),
                DateFormat.stringToMillis("1995-01-08 03:04:00"));
        NDataSegment segment4 = newReadySegment(DateFormat.stringToMillis("1995-01-08 10:13:00"),
                DateFormat.stringToMillis("1995-01-09 00:00:00"));
        NDataSegment segment5 = newReadySegment(DateFormat.stringToMillis("1995-01-09 00:00:00"),
                DateFormat.stringToMillis("1995-01-09 02:24:00"));
        segments.add(segment1);
        segments.add(segment2);
        segments.add(segment4);
        segments.add(segment5);
        SegmentRange segmentRange = segments.findMergeSegmentsRange(AutoMergeTimeEnum.DAY);
        assert segmentRange == null;

    }

    @Test
    public void testMergeMonth() {
        Segments segments = new Segments();
        NDataSegment segment1 = newReadySegment(DateFormat.stringToMillis("1995-01-07 00:03:00"),
                DateFormat.stringToMillis("1995-01-07 00:56:00"));

        NDataSegment segment2 = newReadySegment(DateFormat.stringToMillis("1995-01-08 00:00:00"),
                DateFormat.stringToMillis("1995-01-08 01:04:00"));
        NDataSegment segment3 = newReadySegment(DateFormat.stringToMillis("1995-01-08 01:04:00"),
                DateFormat.stringToMillis("1995-01-08 09:13:00"));

        NDataSegment segment4 = newReadySegment(DateFormat.stringToMillis("1995-01-08 09:13:00"),
                DateFormat.stringToMillis("1995-01-09 00:00:00"));
        NDataSegment segment5 = newReadySegment(DateFormat.stringToMillis("1995-01-09 00:00:00"),
                DateFormat.stringToMillis("1995-01-09 02:24:00"));
        segments.add(segment1);
        segments.add(segment2);
        segments.add(segment3);
        segments.add(segment4);
        segments.add(segment5);
        SegmentRange segmentRange = segments.findMergeSegmentsRange(AutoMergeTimeEnum.DAY);
        Assert.assertEquals(segmentRange.getStart(), DateFormat.stringToMillis("1995-01-08 00:00:00"));
        Assert.assertEquals(segmentRange.getEnd(), DateFormat.stringToMillis("1995-01-09 00:00:00"));
    }

    @Test
    public void testMergeYear() {
        Segments segments = new Segments();
        NDataSegment segment1 = newReadySegment(DateFormat.stringToMillis("1994-01-07 00:03:00"),
                DateFormat.stringToMillis("1995-02-07 00:56:00"));

        NDataSegment segment2 = newReadySegment(DateFormat.stringToMillis("1995-01-01 00:00:00"),
                DateFormat.stringToMillis("1995-05-06 01:04:00"));
        NDataSegment segment3 = newReadySegment(DateFormat.stringToMillis("1995-05-06 01:04:00"),
                DateFormat.stringToMillis("1995-06-08 09:13:00"));

        NDataSegment segment4 = newReadySegment(DateFormat.stringToMillis("1995-06-08 09:13:00"),
                DateFormat.stringToMillis("1996-01-01 00:00:00"));
        NDataSegment segment5 = newReadySegment(DateFormat.stringToMillis("1996-07-09 00:00:00"),
                DateFormat.stringToMillis("1996-07-09 02:24:00"));
        segments.add(segment1);
        segments.add(segment2);
        segments.add(segment3);
        segments.add(segment4);
        segments.add(segment5);
        SegmentRange segmentRange = segments.findMergeSegmentsRange(AutoMergeTimeEnum.YEAR);
        Assert.assertEquals(segmentRange.getStart(), DateFormat.stringToMillis("1995-01-01 00:00:00"));
        Assert.assertEquals(segmentRange.getEnd(), DateFormat.stringToMillis("1996-01-01 00:00:00"));
    }

    @Test
    public void testMergeQuarter() {
        Segments segments = new Segments();
        NDataSegment segment1 = newReadySegment(DateFormat.stringToMillis("1995-01-07 00:03:00"),
                DateFormat.stringToMillis("1995-02-07 00:56:00"));

        NDataSegment segment2 = newReadySegment(DateFormat.stringToMillis("1995-04-01 00:00:00"),
                DateFormat.stringToMillis("1995-05-06 01:04:00"));
        NDataSegment segment3 = newReadySegment(DateFormat.stringToMillis("1995-05-06 01:04:00"),
                DateFormat.stringToMillis("1995-06-08 09:13:00"));

        NDataSegment segment4 = newReadySegment(DateFormat.stringToMillis("1995-06-08 09:13:00"),
                DateFormat.stringToMillis("1995-07-01 00:00:00"));
        NDataSegment segment5 = newReadySegment(DateFormat.stringToMillis("1995-07-09 00:00:00"),
                DateFormat.stringToMillis("1995-07-09 02:24:00"));
        segments.add(segment1);
        segments.add(segment2);
        segments.add(segment3);
        segments.add(segment4);
        segments.add(segment5);
        SegmentRange segmentRange = segments.findMergeSegmentsRange(AutoMergeTimeEnum.QUARTER);
        Assert.assertEquals(segmentRange.getStart(), DateFormat.stringToMillis("1995-04-01 00:00:00"));
        Assert.assertEquals(segmentRange.getEnd(), DateFormat.stringToMillis("1995-07-01 00:00:00"));
    }
}

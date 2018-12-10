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

package org.apache.kylin.stream.core.util;

import org.apache.kylin.common.util.DateFormat;
import org.junit.Assert;
import org.junit.Test;

public class TimeDerivedColumnTypeTest {

    @Test
    public void testMinuteStartTimeRangeOverlap() {
        String segmentStartStr = "2017-11-01 08:00:00";
        String segmentEndStr = "2017-11-01 09:00:00";

        long segmentStart = DateFormat.stringToMillis(segmentStartStr);
        long segmentEnd = DateFormat.stringToMillis(segmentEndStr);

        String minStartTime = "2017-11-01 08:05:00";
        boolean overlap = false;
        overlap = TimeDerivedColumnType.MINUTE_START.hasTimeRangeOverlap(segmentStart, segmentEnd, minStartTime);
        Assert.assertTrue(overlap);

        minStartTime = "2017-11-01 08:00:00";
        overlap = TimeDerivedColumnType.MINUTE_START.hasTimeRangeOverlap(segmentStart, segmentEnd, minStartTime);
        Assert.assertTrue(overlap);

        minStartTime = "2017-11-01 09:00:00";
        overlap = TimeDerivedColumnType.MINUTE_START.hasTimeRangeOverlap(segmentStart, segmentEnd, minStartTime);
        Assert.assertFalse(overlap);
    }

    @Test
    public void testHourStartTimeRangeOverlap() {
        String segmentStartStr = "2017-11-01 08:00:00";
        String segmentEndStr = "2017-11-01 09:00:00";

        long segmentStart = DateFormat.stringToMillis(segmentStartStr);
        long segmentEnd = DateFormat.stringToMillis(segmentEndStr);

        String hourStartTime = "2017-11-01 08:00:00";
        boolean overlap = false;
        overlap = TimeDerivedColumnType.MINUTE_START.hasTimeRangeOverlap(segmentStart, segmentEnd, hourStartTime);
        Assert.assertTrue(overlap);

        hourStartTime = "2017-11-01 08:05:00";
        overlap = TimeDerivedColumnType.MINUTE_START.hasTimeRangeOverlap(segmentStart, segmentEnd, hourStartTime);
        Assert.assertTrue(overlap);

        hourStartTime = "2017-11-01 07:00:00";
        overlap = TimeDerivedColumnType.MINUTE_START.hasTimeRangeOverlap(segmentStart, segmentEnd, hourStartTime);
        Assert.assertFalse(overlap);

        hourStartTime = "2017-11-01 09:00:00";
        overlap = TimeDerivedColumnType.MINUTE_START.hasTimeRangeOverlap(segmentStart, segmentEnd, hourStartTime);
        Assert.assertFalse(overlap);

        hourStartTime = "2017-11-01 10:00:00";
        overlap = TimeDerivedColumnType.MINUTE_START.hasTimeRangeOverlap(segmentStart, segmentEnd, hourStartTime);
        Assert.assertFalse(overlap);
    }

    @Test
    public void testDayStartTimeRangeOverlap() {
        String segmentStartStr = "2017-11-01 08:00:00";
        String segmentEndStr = "2017-11-01 09:00:00";

        long segmentStart = DateFormat.stringToMillis(segmentStartStr);
        long segmentEnd = DateFormat.stringToMillis(segmentEndStr);

        String dayStartTime = "2017-11-01";
        boolean overlap = false;
        overlap = TimeDerivedColumnType.DAY_START.hasTimeRangeOverlap(segmentStart, segmentEnd, dayStartTime);
        Assert.assertTrue(overlap);

        dayStartTime = "2017-10-29";
        overlap = TimeDerivedColumnType.DAY_START.hasTimeRangeOverlap(segmentStart, segmentEnd, dayStartTime);
        Assert.assertFalse(overlap);

        dayStartTime = "2017-11-02";
        overlap = TimeDerivedColumnType.DAY_START.hasTimeRangeOverlap(segmentStart, segmentEnd, dayStartTime);
        Assert.assertFalse(overlap);

        segmentStartStr = "2017-11-01 23:00:00";
        segmentEndStr = "2017-11-02 02:00:00";

        segmentStart = DateFormat.stringToMillis(segmentStartStr);
        segmentEnd = DateFormat.stringToMillis(segmentEndStr);

        dayStartTime = "2017-11-02";
        overlap = TimeDerivedColumnType.DAY_START.hasTimeRangeOverlap(segmentStart, segmentEnd, dayStartTime);
        Assert.assertTrue(overlap);

        dayStartTime = "2017-11-01";
        overlap = TimeDerivedColumnType.DAY_START.hasTimeRangeOverlap(segmentStart, segmentEnd, dayStartTime);
        Assert.assertTrue(overlap);

        dayStartTime = "2017-10-30";
        overlap = TimeDerivedColumnType.DAY_START.hasTimeRangeOverlap(segmentStart, segmentEnd, dayStartTime);
        Assert.assertFalse(overlap);
    }
}

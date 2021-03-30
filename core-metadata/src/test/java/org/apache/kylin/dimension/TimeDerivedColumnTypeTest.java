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

package org.apache.kylin.dimension;

import org.apache.kylin.common.util.DateFormat;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
        assertTrue(overlap);

        minStartTime = "2017-11-01 08:00:00";
        overlap = TimeDerivedColumnType.MINUTE_START.hasTimeRangeOverlap(segmentStart, segmentEnd, minStartTime);
        assertTrue(overlap);

        minStartTime = "2017-11-01 09:00:00";
        overlap = TimeDerivedColumnType.MINUTE_START.hasTimeRangeOverlap(segmentStart, segmentEnd, minStartTime);
        assertFalse(overlap);
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
        assertTrue(overlap);

        hourStartTime = "2017-11-01 08:05:00";
        overlap = TimeDerivedColumnType.MINUTE_START.hasTimeRangeOverlap(segmentStart, segmentEnd, hourStartTime);
        assertTrue(overlap);

        hourStartTime = "2017-11-01 07:00:00";
        overlap = TimeDerivedColumnType.MINUTE_START.hasTimeRangeOverlap(segmentStart, segmentEnd, hourStartTime);
        assertFalse(overlap);

        hourStartTime = "2017-11-01 09:00:00";
        overlap = TimeDerivedColumnType.MINUTE_START.hasTimeRangeOverlap(segmentStart, segmentEnd, hourStartTime);
        assertFalse(overlap);

        hourStartTime = "2017-11-01 10:00:00";
        overlap = TimeDerivedColumnType.MINUTE_START.hasTimeRangeOverlap(segmentStart, segmentEnd, hourStartTime);
        assertFalse(overlap);
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
        assertTrue(overlap);

        dayStartTime = "2017-10-29";
        overlap = TimeDerivedColumnType.DAY_START.hasTimeRangeOverlap(segmentStart, segmentEnd, dayStartTime);
        assertFalse(overlap);

        dayStartTime = "2017-11-02";
        overlap = TimeDerivedColumnType.DAY_START.hasTimeRangeOverlap(segmentStart, segmentEnd, dayStartTime);
        assertFalse(overlap);

        segmentStartStr = "2017-11-01 23:00:00";
        segmentEndStr = "2017-11-02 02:00:00";

        segmentStart = DateFormat.stringToMillis(segmentStartStr);
        segmentEnd = DateFormat.stringToMillis(segmentEndStr);

        dayStartTime = "2017-11-02";
        overlap = TimeDerivedColumnType.DAY_START.hasTimeRangeOverlap(segmentStart, segmentEnd, dayStartTime);
        assertTrue(overlap);

        dayStartTime = "2017-11-01";
        overlap = TimeDerivedColumnType.DAY_START.hasTimeRangeOverlap(segmentStart, segmentEnd, dayStartTime);
        assertTrue(overlap);

        dayStartTime = "2017-10-30";
        overlap = TimeDerivedColumnType.DAY_START.hasTimeRangeOverlap(segmentStart, segmentEnd, dayStartTime);
        assertFalse(overlap);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseTimeValueThrowsIllegalArgumentException() {
        TimeDerivedColumnType.parseTimeValue(new Object());
    }

    @Test
    public void testIsTimeDerivedColumnReturningTrue() {
        assertTrue(TimeDerivedColumnType.isTimeDerivedColumn("YEAR_START"));
    }

    @Test
    public void testIsTimeDerivedColumnReturningFalse() {
        assertFalse(TimeDerivedColumnType.isTimeDerivedColumn(""));
    }

    @Test
    public void testNormalizeTimeFormatWithPositive() {
        TimeDerivedColumnType timeDerivedColumnType = TimeDerivedColumnType.MINUTE_START;

        assertEquals("1970-01-01 00:00:00", timeDerivedColumnType.normalizeTimeFormat(1438L));
    }

    @Test
    public void testNormalizeTimeFormatAndNormalizeTimeFormatWithNegativeOne() {
        TimeDerivedColumnType timeDerivedColumnType = TimeDerivedColumnType.WEEK_START;

        assertEquals("1969-12-28", timeDerivedColumnType.normalizeTimeFormat((-65L)));
    }

    @Test
    public void testNormalizeTimeFormatAndNormalizeTimeFormatWithZeroOne() {
        TimeDerivedColumnType timeDerivedColumnType = TimeDerivedColumnType.MONTH_START;

        assertEquals("1970-01-01", timeDerivedColumnType.normalizeTimeFormat(0L));
    }

    @Test
    public void testHasTimeRangeOverlapAndValueOfReturningNonNull() {
        TimeDerivedColumnType timeDerivedColumnType = TimeDerivedColumnType.valueOf("HOUR_START");

        assertFalse(timeDerivedColumnType.hasTimeRangeOverlap((-2160L), (-2074L), new Long((-498L))));
    }

    @Test
    public void testNormalizeTimeFormatAndValueOfReturningNonNull() {
        TimeDerivedColumnType timeDerivedColumnType = TimeDerivedColumnType.valueOf("HOUR_START");

        assertEquals("1970-01-01 00:00:00", timeDerivedColumnType.normalizeTimeFormat((-419L)));
    }

    @Test
    public void testNormalizeTimeFormatAndNormalizeTimeFormatWithZeroTwo() {
        TimeDerivedColumnType timeDerivedColumnType = TimeDerivedColumnType.DAY_START;

        assertEquals("1970-01-01", timeDerivedColumnType.normalizeTimeFormat(0L));
    }

    @Test
    public void testHasTimeRangeOverlapReturningTrue() {
        TimeDerivedColumnType timeDerivedColumnType = TimeDerivedColumnType.QUARTER_START;

        assertTrue(timeDerivedColumnType.hasTimeRangeOverlap((-579L), 641L, new Long(0L)));
    }

    @Test
    public void testNormalizeTimeFormatAndNormalizeTimeFormatWithNegativeTwo() {
        TimeDerivedColumnType timeDerivedColumnType = TimeDerivedColumnType.QUARTER_START;

        assertEquals("1969-10-01", timeDerivedColumnType.normalizeTimeFormat((-1L)));
    }

    @Test
    public void testNormalizeTimeFormatAndNormalizeTimeFormatWithNegativeThree() {
        TimeDerivedColumnType timeDerivedColumnType = TimeDerivedColumnType.YEAR_START;

        assertEquals("1969-01-01", timeDerivedColumnType.normalizeTimeFormat((-170L)));
    }

}

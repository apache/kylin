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

import java.util.Locale;
import java.util.Map;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.TimeUtil;

import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum TimeDerivedColumnType {
    MINUTE_START("minute_start") {
        @Override
        public Pair<Long, Long> calculateTimeUnitRange(long time) {
            long calTimeStart = TimeUtil.getMinuteStart(time);
            long calTimeEnd = calTimeStart + TimeUtil.ONE_MINUTE_TS;
            return new Pair<>(calTimeStart, calTimeEnd);
        }

        @Override
        public String normalizeTimeFormat(long time) {
            return DateFormat.formatToTimeWithoutMilliStr(normalize(time));
        }

        @Override
        public long normalize(long time) {
            return TimeUtil.getMinuteStart(time);
        }
    },
    HOUR_START("hour_start") {
        @Override
        public Pair<Long, Long> calculateTimeUnitRange(long time) {
            long calTimeStart = TimeUtil.getHourStart(time);
            long calTimeEnd = calTimeStart + TimeUtil.ONE_HOUR_TS;
            return new Pair<>(calTimeStart, calTimeEnd);
        }

        @Override
        public String normalizeTimeFormat(long time) {
            return DateFormat.formatToTimeWithoutMilliStr(normalize(time));
        }

        @Override
        public long normalize(long time) {
            return TimeUtil.getHourStart(time);
        }
    },
    DAY_START("day_start") {
        @Override
        public Pair<Long, Long> calculateTimeUnitRange(long time) {
            long calTimeStart = TimeUtil.getDayStart(time);
            long calTimeEnd = calTimeStart + TimeUtil.ONE_DAY_TS;
            return new Pair<>(calTimeStart, calTimeEnd);
        }

        @Override
        public String normalizeTimeFormat(long time) {
            return DateFormat.formatToDateStr(normalize(time));
        }

        @Override
        public long normalize(long time) {
            return TimeUtil.getDayStart(time);
        }
    },
    WEEK_START("week_start") {
        @Override
        public Pair<Long, Long> calculateTimeUnitRange(long time) {
            return new Pair<>(TimeUtil.getWeekStart(time), TimeUtil.getWeekEnd(time));
        }

        @Override
        public String normalizeTimeFormat(long time) {
            return DateFormat.formatToDateStr(normalize(time));
        }

        @Override
        public long normalize(long time) {
            return TimeUtil.getWeekStart(time);
        }
    },
    MONTH_START("month_start") {
        @Override
        public Pair<Long, Long> calculateTimeUnitRange(long time) {
            return new Pair<>(TimeUtil.getMonthStart(time), TimeUtil.getMonthEnd(time));
        }

        @Override
        public String normalizeTimeFormat(long time) {
            return DateFormat.formatToDateStr(normalize(time));
        }

        @Override
        public long normalize(long time) {
            return TimeUtil.getMonthStart(time);
        }
    },
    QUARTER_START("quarter_start") {
        @Override
        public Pair<Long, Long> calculateTimeUnitRange(long time) {
            return new Pair<>(TimeUtil.getQuarterStart(time), TimeUtil.getQuarterEnd(time));
        }

        @Override
        public String normalizeTimeFormat(long time) {
            return DateFormat.formatToDateStr(normalize(time));
        }

        @Override
        public long normalize(long time) {
            return TimeUtil.getQuarterStart(time);
        }
    },
    YEAR_START("year_start") {
        @Override
        public Pair<Long, Long> calculateTimeUnitRange(long time) {
            return new Pair<>(TimeUtil.getYearStart(time), TimeUtil.getYearEnd(time));
        }

        @Override
        public String normalizeTimeFormat(long time) {
            return DateFormat.formatToDateStr(normalize(time));
        }

        @Override
        public long normalize(long time) {
            return TimeUtil.getYearStart(time);
        }
    };

    private static final String MINUTE_START_NAME = "minute_start";
    private static final String HOUR_START_NAME = "hour_start";
    private static final String DAY_START_NAME = "day_start";
    private static final String WEEK_START_NAME = "week_start";
    private static final String MONTH_START_NAME = "month_start";
    private static final String QUARTER_START_NAME = "quarter_start";
    private static final String YEAR_START_NAME = "year_start";
    private static Map<String, TimeDerivedColumnType> nameColumnsMap = Maps.newHashMap();
    private static Logger logger = LoggerFactory.getLogger(TimeDerivedColumnType.class);

    static {
        nameColumnsMap.put(MINUTE_START_NAME, MINUTE_START);
        nameColumnsMap.put(HOUR_START_NAME, HOUR_START);
        nameColumnsMap.put(DAY_START_NAME, DAY_START);
        nameColumnsMap.put(WEEK_START_NAME, WEEK_START);
        nameColumnsMap.put(MONTH_START_NAME, MONTH_START);
        nameColumnsMap.put(QUARTER_START_NAME, QUARTER_START);
        nameColumnsMap.put(YEAR_START_NAME, YEAR_START);
    }

    private String name;

    TimeDerivedColumnType(String name) {
        this.name = name;
    }

    public static boolean isTimeDerivedColumn(String columnName) {
        return nameColumnsMap.containsKey(columnName.toLowerCase(Locale.ROOT));
    }

    public static boolean isTimeDerivedColumnAboveDayLevel(String columnName) {
        if (!isTimeDerivedColumn(columnName))
            return false;
        else {
            return !columnName.equalsIgnoreCase(MINUTE_START_NAME) && !columnName.equalsIgnoreCase(HOUR_START_NAME);
        }
    }

    public static TimeDerivedColumnType getTimeDerivedColumnType(String columnName) {
        return nameColumnsMap.get(columnName.toLowerCase(Locale.ROOT));
    }

    public static long parseTimeValue(Object value) {
        if (value instanceof Long) {
            return (Long) value;
        }
        String dateStr;
        if (value instanceof String) {
            dateStr = (String) value;
        } else if (value instanceof ByteArray) {
            dateStr = Bytes.toString(((ByteArray) value).array());
        } else {
            throw new IllegalArgumentException("unknown type of value:" + value.getClass());
        }
        return DateFormat.stringToMillis(dateStr);
    }

    public boolean hasTimeRangeOverlap(long timeStart, long timeEnd, Object timeValue) {
        long time = parseTimeValue(timeValue);
        Pair<Long, Long> calUnitTimeRange = calculateTimeUnitRange(time);
        if (calUnitTimeRange.getSecond() <= timeStart || calUnitTimeRange.getFirst() >= timeEnd) {
            return false;
        }
        return true;
    }

    public Pair<Long, Long> getTimeUnitRange(Object timeValue) {
        long time = parseTimeValue(timeValue);
        return calculateTimeUnitRange(time);
    }

    public Pair<Long, Long> getTimeUnitRangeTimezoneAware(Object timeValue, long timezoneOffset){
        long ts = parseTimeValue(timeValue);
        Pair<Long, Long> res = calculateTimeUnitRange(ts);
        res = new Pair<>(res.getFirst() - timezoneOffset, res.getSecond() - timezoneOffset);
        return res;
    }

    abstract public Pair<Long, Long> calculateTimeUnitRange(long time);

    abstract public String normalizeTimeFormat(long time);

    abstract public long normalize(long time);
}

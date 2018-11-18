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

package org.apache.kylin.common.util;

import java.util.Calendar;
import java.util.Locale;
import java.util.TimeZone;

/**
 */
public class TimeUtil {

    private TimeUtil() {
        throw new IllegalStateException("Class TimeUtil is an utility class !");
    }

    private static TimeZone gmt = TimeZone.getTimeZone("GMT");
    private static long ONE_MINUTE_TS = 60 * 1000L;
    private static long ONE_HOUR_TS = 60 * ONE_MINUTE_TS;
    private static long ONE_DAY_TS = 24 * ONE_HOUR_TS;

    /** The julian date of the epoch, 1970-01-01. */
    public static final int EPOCH_JULIAN = 2440588;

    public static long getMinuteStart(long ts) {
        return ts / ONE_MINUTE_TS * ONE_MINUTE_TS;
    }

    public static long getHourStart(long ts) {
        return ts / ONE_HOUR_TS * ONE_HOUR_TS;
    }

    public static long getDayStart(long ts) {
        return ts / ONE_DAY_TS * ONE_DAY_TS;
    }

    public static long getWeekStart(long ts) {
        Calendar calendar = Calendar.getInstance(gmt, Locale.ROOT);
        calendar.setTimeInMillis(getDayStart(ts));
        calendar.add(Calendar.DAY_OF_WEEK, calendar.getFirstDayOfWeek() - calendar.get(Calendar.DAY_OF_WEEK));
        return calendar.getTimeInMillis();
    }

    public static long getMonthStart(long ts) {
        Calendar calendar = Calendar.getInstance(gmt, Locale.ROOT);
        calendar.setTimeInMillis(ts);
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH);
        calendar.clear();
        calendar.set(year, month, 1);
        return calendar.getTimeInMillis();
    }

    public static long getQuarterStart(long ts) {
        Calendar calendar = Calendar.getInstance(gmt, Locale.ROOT);
        calendar.setTimeInMillis(ts);
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH);
        calendar.clear();
        calendar.set(year, month / 3 * 3, 1);
        return calendar.getTimeInMillis();
    }

    public static long getYearStart(long ts) {
        Calendar calendar = Calendar.getInstance(gmt, Locale.ROOT);
        calendar.setTimeInMillis(ts);
        int year = calendar.get(Calendar.YEAR);
        calendar.clear();
        calendar.set(year, 0, 1);
        return calendar.getTimeInMillis();
    }

    /**
     * SQL extract(timeUnit from dateTime) function.
     * Adapted from avatica DateTimeUtils.unixDateExtract.
     *
     * @param timeUnit one of YEAR, MONTH, DAY
     * @param dateTime parsable date time string
     * @return value of the `timeUnit` field from `dateTime`
     */
    public static long extract(String timeUnit, String dateTime) {
        long millis = DateFormat.stringToMillis(dateTime);
        int unixDate = (int) (millis / ONE_DAY_TS);
        return julianExtract(timeUnit, unixDate + EPOCH_JULIAN);
    }

    private static int julianExtract(String timeUnit, int julian) {
        // this shifts the epoch back to astronomical year -4800 instead of the
        // start of the Christian era in year AD 1 of the proleptic Gregorian
        // calendar.
        int j = julian + 32044;
        int g = j / 146097;
        int dg = j % 146097;
        int c = (dg / 36524 + 1) * 3 / 4;
        int dc = dg - c * 36524;
        int b = dc / 1461;
        int db = dc % 1461;
        int a = (db / 365 + 1) * 3 / 4;
        int da = db - a * 365;

        // integer number of full years elapsed since March 1, 4801 BC
        int y = g * 400 + c * 100 + b * 4 + a;
        // integer number of full months elapsed since the last March 1
        int m = (da * 5 + 308) / 153 - 2;
        // number of days elapsed since day 1 of the month
        int d = da - (m + 4) * 153 / 5 + 122;
        int year = y - 4800 + (m + 2) / 12;
        int month = (m + 2) % 12 + 1;
        int day = d + 1;

        if ("YEAR".equals(timeUnit)) {
            return year;
        }
        if ("MONTH".equals(timeUnit)) {
            return month;
        }
        if ("DAY".equals(timeUnit)) {
            return day;
        }
        throw new IllegalArgumentException("Invalid TimeUnit: " + timeUnit);
    }

}

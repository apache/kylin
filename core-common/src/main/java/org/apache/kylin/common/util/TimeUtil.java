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
    public static final long ONE_MINUTE_TS = 60 * 1000L;
    public static final long ONE_HOUR_TS = 60 * ONE_MINUTE_TS;
    public static final long ONE_DAY_TS = 24 * ONE_HOUR_TS;

    public static long getMinuteStart(long ts) {
        return ts / ONE_MINUTE_TS * ONE_MINUTE_TS;
    }

    public static long getHourStart(long ts) {
        return ts / ONE_HOUR_TS * ONE_HOUR_TS;
    }

    public static long getDayStart(long ts) {
        return getDayStartWithTimeZone(gmt, ts);
    }

    public static long getDayStartWithTimeZone(TimeZone timeZone, long ts){
        Calendar calendar = Calendar.getInstance(timeZone, Locale.ROOT);
        calendar.setTimeInMillis(ts);
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH);
        int date = calendar.get(Calendar.DATE);
        calendar.clear();
        calendar.set(year, month, date);
        return calendar.getTimeInMillis();
    }

    public static long getWeekStart(long ts) {
        return getWeekStartWithTimeZone(gmt, ts);
    }

    public static long getWeekStartWithTimeZone(TimeZone timeZone, long ts){
        Calendar calendar = Calendar.getInstance(timeZone, Locale.ROOT);
        calendar.setTimeInMillis(getDayStartWithTimeZone(timeZone, ts));
        calendar.add(Calendar.DAY_OF_WEEK, calendar.getFirstDayOfWeek() - calendar.get(Calendar.DAY_OF_WEEK));
        return calendar.getTimeInMillis();
    }

    public static long getMonthStart(long ts) {
        return getMonthStartWithTimeZone(gmt, ts);
    }

    public static long getMonthStartWithTimeZone(TimeZone timeZone, long ts){
        Calendar calendar = Calendar.getInstance(timeZone, Locale.ROOT);
        calendar.setTimeInMillis(ts);
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH);
        calendar.clear();
        calendar.set(year, month, 1);
        return calendar.getTimeInMillis();
    }

    public static long getQuarterStart(long ts) {
        return getQuarterStartWithTimeZone(gmt, ts);
    }

    public static long getQuarterStartWithTimeZone(TimeZone timeZone, long ts) {
        Calendar calendar = Calendar.getInstance(timeZone, Locale.ROOT);
        calendar.setTimeInMillis(ts);
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH);
        calendar.clear();
        calendar.set(year, month / 3 * 3, 1);
        return calendar.getTimeInMillis();
    }

    public static long getYearStart(long ts) {
        return getYearStartWithTimeZone(gmt, ts);
    }

    public static long getYearStartWithTimeZone(TimeZone timeZone, long ts) {
        Calendar calendar = Calendar.getInstance(timeZone, Locale.ROOT);
        calendar.setTimeInMillis(ts);
        int year = calendar.get(Calendar.YEAR);
        calendar.clear();
        calendar.set(year, 0, 1);
        return calendar.getTimeInMillis();
    }

    public static long getWeekEnd(long ts) {
        return getWeekEndWithTimeZone(gmt, ts);
    }

    public static long getWeekEndWithTimeZone(TimeZone timeZone, long ts) {
        Calendar calendar = Calendar.getInstance(timeZone, Locale.ROOT);
        calendar.setTimeInMillis(getWeekStartWithTimeZone(timeZone, ts));
        calendar.add(Calendar.DAY_OF_WEEK, 7);
        return calendar.getTimeInMillis();
    }

    public static long getMonthEnd(long ts) {
        return getMonthEndWithTimeZone(gmt, ts);
    }

    public static long getMonthEndWithTimeZone(TimeZone timeZone, long ts) {
        Calendar calendar = Calendar.getInstance(timeZone, Locale.ROOT);
        calendar.setTimeInMillis(ts);
        calendar.set(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONDAY), calendar.get(Calendar.DAY_OF_MONTH), 0, 0, 0);
        calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMaximum(Calendar.DAY_OF_MONTH));
        calendar.set(Calendar.HOUR_OF_DAY, 24);
        return calendar.getTimeInMillis();
    }

    public static long getQuarterEnd(long ts) {
        return getQuarterEndWithTimeZone(gmt, ts);
    }

    public static long getQuarterEndWithTimeZone(TimeZone timeZone, long ts) {
        Calendar calendar = Calendar.getInstance(timeZone, Locale.ROOT);
        calendar.setTimeInMillis(getQuarterStartWithTimeZone(timeZone, ts));
        calendar.add(Calendar.MONTH, 3);
        return calendar.getTimeInMillis();
    }

    public static long getYearEnd(long ts) {
        return getYearEndWithTimeZone(gmt, ts);
    }

    public static long getYearEndWithTimeZone(TimeZone timeZone, long ts) {
        Calendar calendar = Calendar.getInstance(timeZone, Locale.ROOT);
        calendar.setTimeInMillis(getYearStartWithTimeZone(timeZone, ts));
        calendar.add(Calendar.YEAR, 1);
        return calendar.getTimeInMillis();
    }

}

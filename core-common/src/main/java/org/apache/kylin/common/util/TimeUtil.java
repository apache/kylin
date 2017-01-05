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
import java.util.TimeZone;

/**
 */
public class TimeUtil {

    private static TimeZone gmt = TimeZone.getTimeZone("GMT");
    private static long ONE_MINUTE_TS = 60 * 1000L;
    private static long ONE_HOUR_TS = 60 * ONE_MINUTE_TS;
    private static long ONE_DAY_TS = 24 * ONE_HOUR_TS;

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
        Calendar calendar = Calendar.getInstance(gmt);
        calendar.setTimeInMillis(getDayStart(ts));
        calendar.add(Calendar.DAY_OF_WEEK, calendar.getFirstDayOfWeek() - calendar.get(Calendar.DAY_OF_WEEK));
        return calendar.getTimeInMillis();
    }

    public static long getMonthStart(long ts) {
        Calendar calendar = Calendar.getInstance(gmt);
        calendar.setTimeInMillis(ts);
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH);
        calendar.clear();
        calendar.set(year, month, 1);
        return calendar.getTimeInMillis();
    }

    public static long getQuarterStart(long ts) {
        Calendar calendar = Calendar.getInstance(gmt);
        calendar.setTimeInMillis(ts);
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH);
        calendar.clear();
        calendar.set(year, month / 3 * 3, 1);
        return calendar.getTimeInMillis();
    }

    public static long getYearStart(long ts) {
        Calendar calendar = Calendar.getInstance(gmt);
        calendar.setTimeInMillis(ts);
        int year = calendar.get(Calendar.YEAR);
        calendar.clear();
        calendar.set(year, 0, 1);
        return calendar.getTimeInMillis();
    }

}

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

package org.apache.kylin.metrics.lib.impl;

import org.apache.kylin.common.KylinConfig;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Locale;
import java.util.TimeZone;

public class RecordEventTimeDetail {
    private static final TimeZone timeZone;
    private static final ThreadLocal<SimpleDateFormat> dateFormatThreadLocal = new ThreadLocal<>();
    private static final ThreadLocal<SimpleDateFormat> timeFormatThreadLocal = new ThreadLocal<>();

    static {
        timeZone = TimeZone.getTimeZone(KylinConfig.getInstanceFromEnv().getKylinMetricsEventTimeZone());
    }
    
    public final String year_begin_date;
    public final String month_begin_date;
    public final String date;
    public final String time;
    public final int hour;
    public final int minute;
    public final int second;
    public final String week_begin_date;

    public RecordEventTimeDetail(long timeStamp) {
        Calendar calendar = Calendar.getInstance(timeZone, Locale.ROOT);
        calendar.setTimeInMillis(timeStamp);

        SimpleDateFormat dateFormat = dateFormatThreadLocal.get();
        if (dateFormat == null) {
            dateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.ROOT);
            dateFormat.setTimeZone(timeZone);
            dateFormatThreadLocal.set(dateFormat);
        }
        SimpleDateFormat timeFormat = timeFormatThreadLocal.get();
        if (timeFormat == null) {
            timeFormat = new SimpleDateFormat("HH:mm:ss", Locale.ROOT);
            timeFormat.setTimeZone(timeZone);
            timeFormatThreadLocal.set(timeFormat);
        }

        String yearStr = String.format(Locale.ROOT, "%04d", calendar.get(Calendar.YEAR));
        String monthStr = String.format(Locale.ROOT, "%02d", calendar.get(Calendar.MONTH) + 1);
        this.year_begin_date = yearStr + "-01-01";
        this.month_begin_date = yearStr + "-" + monthStr + "-01";
        this.date = dateFormat.format(calendar.getTime());
        this.time = timeFormat.format(calendar.getTime());
        this.hour = calendar.get(Calendar.HOUR_OF_DAY);
        this.minute = calendar.get(Calendar.MINUTE);
        this.second = calendar.get(Calendar.SECOND);

        long timeStampForWeekBegin = timeStamp;
        timeStampForWeekBegin -= 3600000L * 24 * (calendar.get(Calendar.DAY_OF_WEEK) - 1);
        calendar.setTimeInMillis(timeStampForWeekBegin);
        this.week_begin_date = dateFormat.format(calendar.getTime());
    }
}

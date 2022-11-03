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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.junit.annotation.MultiTimezoneTest;
import org.junit.Assert;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 */
@Slf4j
public class TimeUtilTest {

    public enum NormalizedTimeUnit {
        MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, YEAR
    }

    public static long normalizeTime(long timeMillis, NormalizedTimeUnit unit) {
        log.info(TimeZone.getDefault().toString());
        log.info(Locale.getDefault(Locale.Category.FORMAT).toString());
        Calendar a = Calendar.getInstance(TimeZone.getDefault(), Locale.getDefault(Locale.Category.FORMAT));
        Calendar b = Calendar.getInstance(TimeZone.getDefault(), Locale.getDefault(Locale.Category.FORMAT));
        b.clear();

        a.setTimeInMillis(timeMillis);
        if (unit == NormalizedTimeUnit.MINUTE) {
            b.set(a.get(Calendar.YEAR), a.get(Calendar.MONTH), a.get(Calendar.DAY_OF_MONTH),
                    a.get(Calendar.HOUR_OF_DAY), a.get(Calendar.MINUTE));
        } else if (unit == NormalizedTimeUnit.HOUR) {
            b.set(a.get(Calendar.YEAR), a.get(Calendar.MONTH), a.get(Calendar.DAY_OF_MONTH),
                    a.get(Calendar.HOUR_OF_DAY), 0);
        }
        return b.getTimeInMillis();
    }

    @MultiTimezoneTest(timezones = { "UTC", "GMT+8", "GMT+15" })
    public void basicTest() throws ParseException {
        java.text.DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss",
                Locale.getDefault(Locale.Category.FORMAT));
        dateFormat.setTimeZone(TimeZone.getDefault());

        long t1 = dateFormat.parse("2012/01/01 00:00:01").getTime();
        Assert.assertEquals(normalizeTime(t1, NormalizedTimeUnit.HOUR), TimeUtil.getHourStart(t1));
        Assert.assertEquals(normalizeTime(t1, NormalizedTimeUnit.MINUTE), TimeUtil.getMinuteStart(t1));

        long t2 = dateFormat.parse("2012/12/31 11:02:01").getTime();
        Assert.assertEquals(normalizeTime(t2, NormalizedTimeUnit.HOUR), TimeUtil.getHourStart(t2));
        Assert.assertEquals(normalizeTime(t2, NormalizedTimeUnit.MINUTE), TimeUtil.getMinuteStart(t2));

        long t3 = dateFormat.parse("2012/12/31 11:02:01").getTime();
        Assert.assertEquals(dateFormat.parse("2012/12/1 00:00:00").getTime(), TimeUtil.getMonthStart(t3));
        Assert.assertEquals(dateFormat.parse("2012/10/1 00:00:00").getTime(), TimeUtil.getQuarterStart(t3));
        Assert.assertEquals(dateFormat.parse("2012/1/1 00:00:00").getTime(), TimeUtil.getYearStart(t3));
        Assert.assertEquals(dateFormat.parse("2012/12/30 00:00:00").getTime(), TimeUtil.getWeekStart(t3));

        long t4 = dateFormat.parse("2012/12/32 00:00:00").getTime();
        Assert.assertEquals(dateFormat.parse("2012/12/32 00:00:00").getTime(), TimeUtil.getDayStart(t4));

        long t5 = dateFormat.parse("2015/01/01 10:01:30").getTime();
        Assert.assertEquals(dateFormat.parse("2015/1/1 00:00:00").getTime(), TimeUtil.getMonthStart(t5));
        Assert.assertEquals(dateFormat.parse("2015/1/1 00:00:00").getTime(), TimeUtil.getQuarterStart(t5));
        Assert.assertEquals(dateFormat.parse("2015/1/1 00:00:00").getTime(), TimeUtil.getYearStart(t5));
        Assert.assertEquals(dateFormat.parse("2014/12/28 00:00:00").getTime(), TimeUtil.getWeekStart(t5));

        Assert.assertEquals(24 * 60 * 60 * 1000, TimeUtil.timeStringAs("1d", TimeUnit.MILLISECONDS));
    }

    @MultiTimezoneTest(timezones = { "UTC", "GMT+8", "GMT+15" })
    public void summerTimeChangeTest() throws ParseException {
        log.info(Locale.getDefault(Locale.Category.FORMAT).toString());
        log.info(TimeZone.getDefault().toString());
        val dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss", Locale.getDefault(Locale.Category.FORMAT));
        dateFormat.setTimeZone(TimeZone.getDefault());

        // 2019/11/3 02:00:00 changed from summertime to winter time in pst timezone
        long winterTime = dateFormat.parse("2019/11/3 03:30:00").getTime();
        long summerTime = dateFormat.parse("2019/11/3 00:00:00").getTime();
        Assert.assertEquals(summerTime, TimeUtil.getDayStart(winterTime));
    }

    @MultiTimezoneTest(timezones = { "UTC", "GMT+8", "GMT+15" })
    public void minusDaysTest() throws ParseException {
        val dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss", Locale.getDefault(Locale.Category.FORMAT));
        dateFormat.setTimeZone(TimeZone.getDefault());

        // 2019/11/3 02:00:00 changed from summertime to winter time in pst timezone
        long winterTime = dateFormat.parse("2019/11/5 09:40:00").getTime();
        long sevenDaysBeforeInSummerTime = dateFormat.parse("2019/10/29 09:40:00").getTime();

        Assert.assertEquals(sevenDaysBeforeInSummerTime, TimeUtil.minusDays(winterTime, 7));
    }

}

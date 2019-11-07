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

import org.junit.Assert;
import org.junit.Test;

/**
 */
public class TimeUtilTest {

    public enum NormalizedTimeUnit {
        MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, YEAR
    }

    public static long normalizeTime(long timeMillis, NormalizedTimeUnit unit) {
        Calendar a = Calendar.getInstance(TimeZone.getTimeZone("GMT"), Locale.ROOT);
        Calendar b = Calendar.getInstance(TimeZone.getTimeZone("GMT"), Locale.ROOT);
        b.clear();

        a.setTimeInMillis(timeMillis);
        if (unit == NormalizedTimeUnit.MINUTE) {
            b.set(a.get(Calendar.YEAR), a.get(Calendar.MONTH), a.get(Calendar.DAY_OF_MONTH), a.get(Calendar.HOUR_OF_DAY), a.get(Calendar.MINUTE));
        } else if (unit == NormalizedTimeUnit.HOUR) {
            b.set(a.get(Calendar.YEAR), a.get(Calendar.MONTH), a.get(Calendar.DAY_OF_MONTH), a.get(Calendar.HOUR_OF_DAY), 0);
        }
        return b.getTimeInMillis();
    }

    @Test
    public void basicTest() throws ParseException {
        java.text.DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss", Locale.ROOT);
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

        long t1 = dateFormat.parse("2012/01/01 00:00:01").getTime();
        Assert.assertEquals(normalizeTime(t1, NormalizedTimeUnit.HOUR), TimeUtil.getHourStart(t1));
        Assert.assertEquals(normalizeTime(t1, NormalizedTimeUnit.MINUTE), TimeUtil.getMinuteStart(t1));

        long t2 = dateFormat.parse("2012/12/31 11:02:01").getTime();
        Assert.assertEquals(normalizeTime(t2, NormalizedTimeUnit.HOUR), TimeUtil.getHourStart(t2));
        Assert.assertEquals(normalizeTime(t2, NormalizedTimeUnit.MINUTE), TimeUtil.getMinuteStart(t2));

        long t3 = dateFormat.parse("2012/12/31 11:02:01").getTime();
        Assert.assertEquals(dateFormat.parse("2012/12/31 00:00:00").getTime(), TimeUtil.getDayStart(t3));
        Assert.assertEquals(dateFormat.parse("2012/12/30 00:00:00").getTime(), TimeUtil.getWeekStart(t3));
        Assert.assertEquals(dateFormat.parse("2012/12/1 00:00:00").getTime(), TimeUtil.getMonthStart(t3));
        Assert.assertEquals(dateFormat.parse("2012/10/1 00:00:00").getTime(), TimeUtil.getQuarterStart(t3));
        Assert.assertEquals(dateFormat.parse("2012/1/1 00:00:00").getTime(), TimeUtil.getYearStart(t3));
        Assert.assertEquals(dateFormat.parse("2013/1/6 00:00:00").getTime(), TimeUtil.getWeekEnd(t3));
        Assert.assertEquals(dateFormat.parse("2013/1/1 00:00:00").getTime(), TimeUtil.getMonthEnd(t3));
        Assert.assertEquals(dateFormat.parse("2013/1/1 00:00:00").getTime(), TimeUtil.getQuarterEnd(t3));
        Assert.assertEquals(dateFormat.parse("2013/1/1 00:00:00").getTime(), TimeUtil.getYearEnd(t3));


        long t4 = dateFormat.parse("2012/10/29 11:02:01").getTime();
        Assert.assertEquals(dateFormat.parse("2012/10/29 00:00:00").getTime(), TimeUtil.getDayStart(t4));
        Assert.assertEquals(dateFormat.parse("2012/10/28 00:00:00").getTime(), TimeUtil.getWeekStart(t4));
        Assert.assertEquals(dateFormat.parse("2012/10/1 00:00:00").getTime(), TimeUtil.getMonthStart(t4));
        Assert.assertEquals(dateFormat.parse("2012/10/1 00:00:00").getTime(), TimeUtil.getQuarterStart(t4));
        Assert.assertEquals(dateFormat.parse("2012/1/1 00:00:00").getTime(), TimeUtil.getYearStart(t4));
        Assert.assertEquals(dateFormat.parse("2012/11/4 00:00:00").getTime(), TimeUtil.getWeekEnd(t4));
        Assert.assertEquals(dateFormat.parse("2012/11/1 00:00:00").getTime(), TimeUtil.getMonthEnd(t4));
        Assert.assertEquals(dateFormat.parse("2013/1/1 00:00:00").getTime(), TimeUtil.getQuarterEnd(t4));
        Assert.assertEquals(dateFormat.parse("2013/1/1 00:00:00").getTime(), TimeUtil.getYearEnd(t4));

        long t5 = dateFormat.parse("2012/8/29 23:12:41").getTime();
        Assert.assertEquals(dateFormat.parse("2012/8/29 00:00:00").getTime(), TimeUtil.getDayStart(t5));
        Assert.assertEquals(dateFormat.parse("2012/8/26 00:00:00").getTime(), TimeUtil.getWeekStart(t5));
        Assert.assertEquals(dateFormat.parse("2012/8/1 00:00:00").getTime(), TimeUtil.getMonthStart(t5));
        Assert.assertEquals(dateFormat.parse("2012/7/1 00:00:00").getTime(), TimeUtil.getQuarterStart(t5));
        Assert.assertEquals(dateFormat.parse("2012/1/1 00:00:00").getTime(), TimeUtil.getYearStart(t5));
        Assert.assertEquals(dateFormat.parse("2012/9/2 00:00:00").getTime(), TimeUtil.getWeekEnd(t5));
        Assert.assertEquals(dateFormat.parse("2012/9/1 00:00:00").getTime(), TimeUtil.getMonthEnd(t5));
        Assert.assertEquals(dateFormat.parse("2012/10/1 00:00:00").getTime(), TimeUtil.getQuarterEnd(t5));
        Assert.assertEquals(dateFormat.parse("2013/1/1 00:00:00").getTime(), TimeUtil.getYearEnd(t5));

        long t6 = dateFormat.parse("2015/01/01 10:01:30").getTime();
        Assert.assertEquals(dateFormat.parse("2015/01/01 00:00:00").getTime(), TimeUtil.getDayStart(t6));
        Assert.assertEquals(dateFormat.parse("2014/12/28 00:00:00").getTime(), TimeUtil.getWeekStart(t6));
        Assert.assertEquals(dateFormat.parse("2015/1/1 00:00:00").getTime(), TimeUtil.getMonthStart(t6));
        Assert.assertEquals(dateFormat.parse("2015/1/1 00:00:00").getTime(), TimeUtil.getQuarterStart(t6));
        Assert.assertEquals(dateFormat.parse("2015/1/1 00:00:00").getTime(), TimeUtil.getYearStart(t6));
        Assert.assertEquals(dateFormat.parse("2015/1/4 00:00:00").getTime(), TimeUtil.getWeekEnd(t6));
        Assert.assertEquals(dateFormat.parse("2015/2/1 00:00:00").getTime(), TimeUtil.getMonthEnd(t6));
        Assert.assertEquals(dateFormat.parse("2015/4/1 00:00:00").getTime(), TimeUtil.getQuarterEnd(t6));
        Assert.assertEquals(dateFormat.parse("2016/1/1 00:00:00").getTime(), TimeUtil.getYearEnd(t6));
    }


    @Test
    public void basicTestWithTimeZone() throws ParseException {
        java.text.DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss", Locale.ROOT);
        TimeZone timeZone = TimeZone.getTimeZone("GMT+8");
        dateFormat.setTimeZone(timeZone);

        long t1 = dateFormat.parse("2012/01/01 00:00:01").getTime();
        Assert.assertEquals(normalizeTime(t1, NormalizedTimeUnit.HOUR), TimeUtil.getHourStart(t1));
        Assert.assertEquals(normalizeTime(t1, NormalizedTimeUnit.MINUTE), TimeUtil.getMinuteStart(t1));

        long t2 = dateFormat.parse("2012/12/31 11:02:01").getTime();
        Assert.assertEquals(normalizeTime(t2, NormalizedTimeUnit.HOUR), TimeUtil.getHourStart(t2));
        Assert.assertEquals(normalizeTime(t2, NormalizedTimeUnit.MINUTE), TimeUtil.getMinuteStart(t2));

        long t3 = dateFormat.parse("2012/12/31 11:02:01").getTime();
        Assert.assertEquals(dateFormat.parse("2012/12/31 00:00:00").getTime(), TimeUtil.getDayStartWithTimeZone(timeZone, t3));
        Assert.assertEquals(dateFormat.parse("2012/12/30 00:00:00").getTime(), TimeUtil.getWeekStartWithTimeZone(timeZone, t3));
        Assert.assertEquals(dateFormat.parse("2012/12/1 00:00:00").getTime(), TimeUtil.getMonthStartWithTimeZone(timeZone, t3));
        Assert.assertEquals(dateFormat.parse("2012/10/1 00:00:00").getTime(), TimeUtil.getQuarterStartWithTimeZone(timeZone, t3));
        Assert.assertEquals(dateFormat.parse("2012/1/1 00:00:00").getTime(), TimeUtil.getYearStartWithTimeZone(timeZone, t3));
        Assert.assertEquals(dateFormat.parse("2013/1/6 00:00:00").getTime(), TimeUtil.getWeekEndWithTimeZone(timeZone, t3));
        Assert.assertEquals(dateFormat.parse("2013/1/1 00:00:00").getTime(), TimeUtil.getMonthEndWithTimeZone(timeZone, t3));
        Assert.assertEquals(dateFormat.parse("2013/1/1 00:00:00").getTime(), TimeUtil.getQuarterEndWithTimeZone(timeZone, t3));
        Assert.assertEquals(dateFormat.parse("2013/1/1 00:00:00").getTime(), TimeUtil.getYearEndWithTimeZone(timeZone, t3));


        long t4 = dateFormat.parse("2012/10/29 11:02:01").getTime();
        Assert.assertEquals(dateFormat.parse("2012/10/29 00:00:00").getTime(), TimeUtil.getDayStartWithTimeZone(timeZone, t4));
        Assert.assertEquals(dateFormat.parse("2012/10/28 00:00:00").getTime(), TimeUtil.getWeekStartWithTimeZone(timeZone, t4));
        Assert.assertEquals(dateFormat.parse("2012/10/1 00:00:00").getTime(), TimeUtil.getMonthStartWithTimeZone(timeZone, t4));
        Assert.assertEquals(dateFormat.parse("2012/10/1 00:00:00").getTime(), TimeUtil.getQuarterStartWithTimeZone(timeZone, t4));
        Assert.assertEquals(dateFormat.parse("2012/1/1 00:00:00").getTime(), TimeUtil.getYearStartWithTimeZone(timeZone, t4));
        Assert.assertEquals(dateFormat.parse("2012/11/4 00:00:00").getTime(), TimeUtil.getWeekEndWithTimeZone(timeZone, t4));
        Assert.assertEquals(dateFormat.parse("2012/11/1 00:00:00").getTime(), TimeUtil.getMonthEndWithTimeZone(timeZone, t4));
        Assert.assertEquals(dateFormat.parse("2013/1/1 00:00:00").getTime(), TimeUtil.getQuarterEndWithTimeZone(timeZone, t4));
        Assert.assertEquals(dateFormat.parse("2013/1/1 00:00:00").getTime(), TimeUtil.getYearEndWithTimeZone(timeZone, t4));

        long t5 = dateFormat.parse("2012/8/29 23:12:41").getTime();
        Assert.assertEquals(dateFormat.parse("2012/8/29 00:00:00").getTime(), TimeUtil.getDayStartWithTimeZone(timeZone, t5));
        Assert.assertEquals(dateFormat.parse("2012/8/26 00:00:00").getTime(), TimeUtil.getWeekStartWithTimeZone(timeZone, t5));
        Assert.assertEquals(dateFormat.parse("2012/8/1 00:00:00").getTime(), TimeUtil.getMonthStartWithTimeZone(timeZone, t5));
        Assert.assertEquals(dateFormat.parse("2012/7/1 00:00:00").getTime(), TimeUtil.getQuarterStartWithTimeZone(timeZone, t5));
        Assert.assertEquals(dateFormat.parse("2012/1/1 00:00:00").getTime(), TimeUtil.getYearStartWithTimeZone(timeZone, t5));
        Assert.assertEquals(dateFormat.parse("2012/9/2 00:00:00").getTime(), TimeUtil.getWeekEndWithTimeZone(timeZone, t5));
        Assert.assertEquals(dateFormat.parse("2012/9/1 00:00:00").getTime(), TimeUtil.getMonthEndWithTimeZone(timeZone, t5));
        Assert.assertEquals(dateFormat.parse("2012/10/1 00:00:00").getTime(), TimeUtil.getQuarterEndWithTimeZone(timeZone, t5));
        Assert.assertEquals(dateFormat.parse("2013/1/1 00:00:00").getTime(), TimeUtil.getYearEndWithTimeZone(timeZone, t5));

        long t6 = dateFormat.parse("2015/01/01 10:01:30").getTime();
        Assert.assertEquals(dateFormat.parse("2015/01/01 00:00:00").getTime(), TimeUtil.getDayStartWithTimeZone(timeZone, t6));
        Assert.assertEquals(dateFormat.parse("2014/12/28 00:00:00").getTime(), TimeUtil.getWeekStartWithTimeZone(timeZone, t6));
        Assert.assertEquals(dateFormat.parse("2015/1/1 00:00:00").getTime(), TimeUtil.getMonthStartWithTimeZone(timeZone, t6));
        Assert.assertEquals(dateFormat.parse("2015/1/1 00:00:00").getTime(), TimeUtil.getQuarterStartWithTimeZone(timeZone, t6));
        Assert.assertEquals(dateFormat.parse("2015/1/1 00:00:00").getTime(), TimeUtil.getYearStartWithTimeZone(timeZone, t6));
        Assert.assertEquals(dateFormat.parse("2015/1/4 00:00:00").getTime(), TimeUtil.getWeekEndWithTimeZone(timeZone, t6));
        Assert.assertEquals(dateFormat.parse("2015/2/1 00:00:00").getTime(), TimeUtil.getMonthEndWithTimeZone(timeZone, t6));
        Assert.assertEquals(dateFormat.parse("2015/4/1 00:00:00").getTime(), TimeUtil.getQuarterEndWithTimeZone(timeZone, t6));
        Assert.assertEquals(dateFormat.parse("2016/1/1 00:00:00").getTime(), TimeUtil.getYearEndWithTimeZone(timeZone, t6));
    }
}

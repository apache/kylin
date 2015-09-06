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
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

public class DateFormat {

    public static final String DEFAULT_DATE_PATTERN = "yyyy-MM-dd";
    public static final String DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS = "yyyy-MM-dd HH:mm:ss";
    public static final String DEFAULT_DATETIME_PATTERN_WITH_MILLISECONDS = "yyyy-MM-dd HH:mm:ss.SSS";

    static final private Map<String, ThreadLocal<SimpleDateFormat>> threadLocalMap = new ConcurrentHashMap<String, ThreadLocal<SimpleDateFormat>>();

    public static SimpleDateFormat getDateFormat(String datePattern) {
        ThreadLocal<SimpleDateFormat> formatThreadLocal = threadLocalMap.get(datePattern);
        if (formatThreadLocal == null) {
            threadLocalMap.put(datePattern, formatThreadLocal = new ThreadLocal<SimpleDateFormat>());
        }
        SimpleDateFormat format = formatThreadLocal.get();
        if (format == null) {
            format = new SimpleDateFormat(datePattern);
            format.setTimeZone(TimeZone.getTimeZone("GMT")); // NOTE: this must be GMT to calculate epoch date correctly
            formatThreadLocal.set(format);
        }
        return format;
    }

    public static String formatToDateStr(long millis) {
        return getDateFormat(DEFAULT_DATE_PATTERN).format(new Date(millis));
    }

    public static String formatToTimeStr(long millis) {
        return getDateFormat(DEFAULT_DATETIME_PATTERN_WITH_MILLISECONDS).format(new Date(millis));
    }

    public static String dateToString(Date date) {
        return dateToString(date, DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS);
    }

    public static String dateToString(Date date, String pattern) {
        return getDateFormat(pattern).format(date);
    }

    public static Date stringToDate(String str) {
        return stringToDate(str, DEFAULT_DATE_PATTERN);
    }

    public static Date stringToDate(String str, String pattern) {
        Date date = null;
        try {
            date = getDateFormat(pattern).parse(str);
        } catch (ParseException e) {
            throw new IllegalArgumentException("'" + str + "' is not a valid date of pattern '" + pattern + "'", e);
        }
        return date;
    }

    public static long stringToMillis(String str) {
        if (isAllDigits(str)) {
            return Long.parseLong(str);
        } else if (str.length() == 10) {
            return stringToDate(str, DEFAULT_DATE_PATTERN).getTime();
        } else if (str.length() == 19) {
            return stringToDate(str, DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS).getTime();
        } else if (str.length() == 23) {
            return stringToDate(str, DEFAULT_DATETIME_PATTERN_WITH_MILLISECONDS).getTime();
        } else {
            throw new IllegalArgumentException("there is no valid date pattern for:" + str);
        }
    }

    private static boolean isAllDigits(String str) {
        for (int i = 0, n = str.length(); i < n; i++) {
            if (Character.isDigit(str.charAt(i)) == false)
                return false;
        }
        return true;
    }

}
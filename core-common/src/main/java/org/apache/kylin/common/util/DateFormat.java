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
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.time.FastDateFormat;

public class DateFormat {

    public static final String COMPACT_DATE_PATTERN = "yyyyMMdd";
    public static final String DEFAULT_DATE_PATTERN = "yyyy-MM-dd";
    public static final String DEFAULT_TIME_PATTERN = "HH:mm:ss";
    public static final String DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS = "yyyy-MM-dd HH:mm:ss";
    public static final String DEFAULT_DATETIME_PATTERN_WITH_MILLISECONDS = "yyyy-MM-dd HH:mm:ss.SSS";
    public static final String[] SUPPORTED_DATETIME_PATTERN = { //
            DEFAULT_DATE_PATTERN, //
            DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS, //
            DEFAULT_DATETIME_PATTERN_WITH_MILLISECONDS, //
            COMPACT_DATE_PATTERN };

    static final private Map<String, FastDateFormat> formatMap = new ConcurrentHashMap<String, FastDateFormat>();

    public static FastDateFormat getDateFormat(String datePattern) {
        FastDateFormat r = formatMap.get(datePattern);
        if (r == null) {
            r = FastDateFormat.getInstance(datePattern, TimeZone.getTimeZone("GMT")); // NOTE: this must be GMT to calculate epoch date correctly
            formatMap.put(datePattern, r);
        }
        return r;
    }
    
    public static String formatToCompactDateStr(long millis) {
        return formatToDateStr(millis, COMPACT_DATE_PATTERN);
    }

    public static String formatToDateStr(long millis) {
        return formatToDateStr(millis, DEFAULT_DATE_PATTERN);
    }

    public static String formatToDateStr(long millis, String pattern) {
        return getDateFormat(pattern).format(new Date(millis));
    }

    public static String formatToTimeStr(long millis) {
        return formatToTimeStr(millis, DEFAULT_DATETIME_PATTERN_WITH_MILLISECONDS);
    }

    public static String formatToTimeWithoutMilliStr(long millis) {
        return formatToTimeStr(millis, DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS);
    }

    public static String formatToTimeStr(long millis, String pattern) {
        return getDateFormat(pattern).format(new Date(millis));
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
        // try to be smart and guess the date format
        if (isAllDigits(str)) {
            if (str.length() == 8)
                //TODO: might be prolematic if an actual ts happends to be 8 digits, e.g. 1970-01-01 10:00:01.123
                return stringToDate(str, COMPACT_DATE_PATTERN).getTime();
            else
                return Long.parseLong(str);
        } else if (str.length() == 10) {
            return stringToDate(str, DEFAULT_DATE_PATTERN).getTime();
        } else if (str.length() == 19) {
            return stringToDate(str, DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS).getTime();
        } else if (str.length() > 19) {
            return stringToDate(str, DEFAULT_DATETIME_PATTERN_WITH_MILLISECONDS).getTime();
        } else {
            throw new IllegalArgumentException("there is no valid date pattern for:" + str);
        }
    }

    private static boolean isAllDigits(String str) {
        for (int i = 0, n = str.length(); i < n; i++) {
            if (!Character.isDigit(str.charAt(i))) {
                if (i == 0 && str.charAt(0) == '-') {
                    continue;
                } else {
                    return false;
                }
            }
        }
        return true;
    }

    public static boolean isSupportedDateFormat(String dateStr) {
        assert dateStr != null;
        for (String formatStr : SUPPORTED_DATETIME_PATTERN) {
            try {
                if (dateStr.equals(dateToString(stringToDate(dateStr, formatStr), formatStr))) {
                    return true;
                }
            } catch (Exception ex) {
                continue;
            }
        }
        return false;
    }

    public static boolean isDatePattern(String ptn) {
        return COMPACT_DATE_PATTERN.equals(ptn) || DEFAULT_DATE_PATTERN.equals(ptn);
    }
}

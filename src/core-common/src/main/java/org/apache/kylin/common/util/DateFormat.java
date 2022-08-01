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

import static org.apache.kylin.common.exception.CommonErrorCode.INVALID_TIME_PARTITION_COLUMN;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public class DateFormat {

    public static final String COMPACT_DATE_PATTERN = "yyyyMMdd";
    public static final String DEFAULT_DATE_PATTERN = "yyyy-MM-dd";
    public static final String DEFAULT_DATE_PATTERN_WITH_SLASH = "yyyy/MM/dd";
    public static final String DEFAULT_DATE_PATTERN_WITH_DOT = "yyyy.MM.dd";
    public static final String COMPACT_MONTH_PATTERN = "yyyyMM";
    public static final String DEFAULT_MONTH_PATTERN = "yyyy-MM";

    public static final String DEFAULT_TIME_PATTERN = "HH:mm:ss";
    public static final String DEFAULT_TIME_PATTERN_WITHOUT_SECONDS = "HH:mm";
    public static final String DEFAULT_TIME_PATTERN_WITH_MILLISECONDS_P1 = "HH:mm:ss.SSS";
    public static final String DEFAULT_TIME_PATTERN_WITH_MILLISECONDS_P2 = "HH:mm:ss:SSS";
    public static final String DEFAULT_TIME_PATTERN_WITH_MILLISECONDS_P3 = "HH:mm:ss.SSS'Z'";
    public static final String DEFAULT_TIME_PATTERN_WITH_MILLISECONDS_P4 = "HH:mm:ss.SSSXXX";
    public static final String DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS = "yyyy-MM-dd HH:mm:ss";
    public static final String DEFAULT_DATETIME_PATTERN_WITH_MILLISECONDS = "yyyy-MM-dd HH:mm:ss.SSS";
    public static final String DEFAULT_DATETIME_PATTERN_WITH_TIMEZONE = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

    static final private Map<String, FastDateFormat> formatMap = new ConcurrentHashMap<String, FastDateFormat>();

    private static final Map<String, String> dateFormatRegex = Maps.newHashMap();

    private static final Logger logger = LoggerFactory.getLogger(DateFormat.class);

    static {
        dateFormatRegex.put("^\\d{8}$", COMPACT_DATE_PATTERN);
        dateFormatRegex.put("^\\d{4}-\\d{2}-\\d{2}$", DEFAULT_DATE_PATTERN);
        dateFormatRegex.put("^\\d{4}/\\d{2}/\\d{2}$", DEFAULT_DATE_PATTERN_WITH_SLASH);
        dateFormatRegex.put("^\\d{4}\\.\\d{2}\\.\\d{2}$", DEFAULT_DATE_PATTERN_WITH_DOT);
        dateFormatRegex.put("^\\d{8}\\s\\d{2}:\\d{2}$",
                COMPACT_DATE_PATTERN + " " + DEFAULT_TIME_PATTERN_WITHOUT_SECONDS);
        dateFormatRegex.put("^\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}$",
                DEFAULT_DATE_PATTERN + " " + DEFAULT_TIME_PATTERN_WITHOUT_SECONDS);
        dateFormatRegex.put("^\\d{4}/\\d{2}/\\d{2}\\s\\d{2}:\\d{2}$",
                DEFAULT_DATE_PATTERN_WITH_SLASH + " " + DEFAULT_TIME_PATTERN_WITHOUT_SECONDS);
        dateFormatRegex.put("^\\d{4}\\.\\d{2}\\.\\d{2}\\s\\d{2}:\\d{2}$",
                DEFAULT_DATE_PATTERN_WITH_DOT + " " + DEFAULT_TIME_PATTERN_WITHOUT_SECONDS);
        dateFormatRegex.put("^\\d{8}\\s\\d{2}:\\d{2}:\\d{2}$", COMPACT_DATE_PATTERN + " " + DEFAULT_TIME_PATTERN);
        dateFormatRegex.put("^\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}$",
                DEFAULT_DATE_PATTERN + " " + DEFAULT_TIME_PATTERN);
        dateFormatRegex.put("^\\d{4}/\\d{2}/\\d{2}\\s\\d{2}:\\d{2}:\\d{2}$",
                DEFAULT_DATE_PATTERN_WITH_SLASH + " " + DEFAULT_TIME_PATTERN);
        dateFormatRegex.put("^\\d{4}\\.\\d{2}\\.\\d{2}\\s\\d{2}:\\d{2}:\\d{2}$",
                DEFAULT_DATE_PATTERN_WITH_DOT + " " + DEFAULT_TIME_PATTERN);
        dateFormatRegex.put("^\\d{8}\\s\\d{2}:\\d{2}:\\d{2}\\.\\d{3}$",
                COMPACT_DATE_PATTERN + " " + DEFAULT_TIME_PATTERN_WITH_MILLISECONDS_P1);
        dateFormatRegex.put("^\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\.\\d+$",
                DEFAULT_DATE_PATTERN + " " + DEFAULT_TIME_PATTERN_WITH_MILLISECONDS_P1);
        dateFormatRegex.put("^\\d{4}/\\d{2}/\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\.\\d+$",
                DEFAULT_DATE_PATTERN_WITH_SLASH + " " + DEFAULT_TIME_PATTERN_WITH_MILLISECONDS_P1);
        dateFormatRegex.put("^\\d{4}\\.\\d{2}\\.\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\.\\d+$",
                DEFAULT_DATE_PATTERN_WITH_DOT + " " + DEFAULT_TIME_PATTERN_WITH_MILLISECONDS_P1);

        dateFormatRegex.put("^\\d{8}\\s\\d{2}:\\d{2}:\\d{2}:\\d{3}$",
                COMPACT_DATE_PATTERN + " " + DEFAULT_TIME_PATTERN_WITH_MILLISECONDS_P2);
        dateFormatRegex.put("^\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}:\\d+$",
                DEFAULT_DATE_PATTERN + " " + DEFAULT_TIME_PATTERN_WITH_MILLISECONDS_P2);
        dateFormatRegex.put("^\\d{4}/\\d{2}/\\d{2}\\s\\d{2}:\\d{2}:\\d{2}:\\d+$",
                DEFAULT_DATE_PATTERN_WITH_SLASH + " " + DEFAULT_TIME_PATTERN_WITH_MILLISECONDS_P2);
        dateFormatRegex.put("^\\d{4}\\.\\d{2}\\.\\d{2}\\s\\d{2}:\\d{2}:\\d{2}:\\d+$",
                DEFAULT_DATE_PATTERN_WITH_DOT + " " + DEFAULT_TIME_PATTERN_WITH_MILLISECONDS_P2);

        dateFormatRegex.put("^\\d{8}T\\d{2}:\\d{2}:\\d{2}.\\d{3}Z$",
                COMPACT_DATE_PATTERN + "'T'" + DEFAULT_TIME_PATTERN_WITH_MILLISECONDS_P3);
        dateFormatRegex.put("^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d+Z$",
                DEFAULT_DATE_PATTERN + "'T'" + DEFAULT_TIME_PATTERN_WITH_MILLISECONDS_P3);
        dateFormatRegex.put("^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d+\\+\\d{2}:\\d{2}$",
                DEFAULT_DATE_PATTERN + "'T'" + DEFAULT_TIME_PATTERN_WITH_MILLISECONDS_P4);
        dateFormatRegex.put("^\\d{4}/\\d{2}/\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d+Z$",
                DEFAULT_DATE_PATTERN_WITH_SLASH + "'T'" + DEFAULT_TIME_PATTERN_WITH_MILLISECONDS_P3);
        dateFormatRegex.put("^\\d{4}\\.\\d{2}\\.\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d+Z$",
                DEFAULT_DATE_PATTERN_WITH_DOT + "'T'" + DEFAULT_TIME_PATTERN_WITH_MILLISECONDS_P3);
        dateFormatRegex.put("^\\d{6}$", COMPACT_MONTH_PATTERN);
        dateFormatRegex.put("^\\d{4}-\\d{2}$", DEFAULT_MONTH_PATTERN);
    }

    public static FastDateFormat getDateFormat(String datePattern) {
        FastDateFormat r = formatMap.get(datePattern);
        if (r == null) {
            r = FastDateFormat.getInstance(datePattern, TimeZone.getDefault());
            formatMap.put(datePattern, r);
        }
        return r;
    }

    public static FastDateFormat getDateFormat(String datePattern, TimeZone timeZone) {
        return FastDateFormat.getInstance(datePattern, timeZone);
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

    public static String formatToDateStr(long millis, String pattern, TimeZone timeZone) {
        return getDateFormat(pattern, timeZone).format(new Date(millis));
    }

    public static String formatToTimeStr(long millis) {
        return formatToTimeStr(millis, DEFAULT_DATETIME_PATTERN_WITH_MILLISECONDS);
    }

    public static String formatToTimeStr(long millis, String pattern, TimeZone timeZone) {
        return getDateFormat(pattern, timeZone).format(new Date(millis));
    }

    @VisibleForTesting
    public static String formatToTimeWithoutMilliStr(long millis) {
        return formatToTimeStr(millis, DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS);
    }

    // consistent with Hive and Spark-SQL
    public static String castTimestampToString(long millis, TimeZone timeZone) {
        String formatted = formatToTimeStr(millis, DEFAULT_DATETIME_PATTERN_WITH_MILLISECONDS, timeZone);
        while (formatted.endsWith("0")) {
            formatted = StringUtils.removeEnd(formatted, "0");
        }
        formatted = StringUtils.removeEnd(formatted, ".");
        return formatted;
    }

    public static String castTimestampToString(long millis) {
        return castTimestampToString(millis, null);
    }

    public static String formatToTimeStr(long millis, String pattern) {
        return getDateFormat(pattern).format(new Date(millis));
    }

    public static String formatDayToEpochToDateStr(long daysToEpoch, TimeZone timeZone) {
        return formatToDateStr(daysToEpoch * 24 * 60 * 60 * 1000, DEFAULT_DATE_PATTERN, timeZone);
    }

    public static String dateToString(Date date, String pattern) {
        return getDateFormat(pattern).format(date);
    }

    public static Date stringToDate(String str) {
        return stringToDate(str, DEFAULT_DATE_PATTERN);
    }

    public static Date stringToDate(String str, String pattern) {
        Date date;
        try {
            date = getDateFormat(pattern).parse(str);
        } catch (ParseException e) {
            throw new IllegalArgumentException("'" + str + "' is not a valid date of pattern '" + pattern + "'", e);
        }
        return date;
    }

    public static long stringToMillis(String str) {
        for (Map.Entry<String, String> regexToPattern : dateFormatRegex.entrySet()) {
            if (str.matches(regexToPattern.getKey()))
                return stringToDate(str, regexToPattern.getValue()).getTime();
        }

        // try parse it as days to epoch
        try {
            long daysToEpoch = Long.parseLong(str);
            return daysToEpoch * 24 * 60 * 60 * 1000;
        } catch (NumberFormatException e) {
        }
        throw new KylinException(INVALID_TIME_PARTITION_COLUMN, MsgPicker.getMsg().getInvalidTimeFormat());
    }

    public static boolean isSupportedDateFormat(String dateStr) {
        Preconditions.checkArgument(dateStr != null);
        for (Map.Entry<String, String> regexToPattern : dateFormatRegex.entrySet()) {
            if (dateStr.matches(regexToPattern.getKey()))
                return true;
        }

        return false;
    }

    public static boolean isDatePattern(String ptn) {
        return COMPACT_DATE_PATTERN.equals(ptn) || DEFAULT_DATE_PATTERN.equals(ptn)
                || DEFAULT_DATE_PATTERN_WITH_SLASH.equals(ptn) || DEFAULT_DATE_PATTERN_WITH_DOT.equals(ptn);
    }

    public static boolean isMonthPatten(String ptn) {
        return COMPACT_MONTH_PATTERN.equals(ptn) || DEFAULT_MONTH_PATTERN.equals(ptn);
    }

    public static String proposeDateFormat(String sampleData) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(sampleData));
        for (Map.Entry<String, String> patternMap : dateFormatRegex.entrySet()) {
            if (sampleData.matches(patternMap.getKey()))
                return patternMap.getValue();
        }
        throw new KylinException(INVALID_TIME_PARTITION_COLUMN, MsgPicker.getMsg().getInvalidTimeFormat());
    }

    /**
     * convert String date to String timestamp(millisecond);
     * same date but different timezone will get different result;
     * timezone base on the system default
     *
     * @param date
     * @param datePattern
     * @return
     */
    public static String getFormattedDate(String date, String datePattern) {
        DateTimeFormatter formatter = new DateTimeFormatterBuilder()
                .append(DateTimeFormatter.ofPattern(datePattern, Locale.getDefault(Locale.Category.FORMAT)))
                .parseDefaulting(ChronoField.DAY_OF_MONTH, 1).parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
                .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0).parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
                .toFormatter();
        LocalDateTime localDateTime = LocalDateTime.parse(date, formatter);
        ZonedDateTime zonedDateTime = ZonedDateTime.of(localDateTime, ZoneId.systemDefault());
        return String.valueOf(zonedDateTime.toInstant().toEpochMilli());
    }

    @VisibleForTesting
    public static void cleanCache() {
        formatMap.clear();
    }

    public static Long getFormatTimeStamp(String time, String pattern) {
        try {
            if (StringUtils.isNotBlank(time) && StringUtils.isNotBlank(pattern)) {
                SimpleDateFormat sdf = new SimpleDateFormat(pattern, Locale.getDefault(Locale.Category.FORMAT));
                sdf.setTimeZone(TimeZone.getDefault());
                String timeFormat = sdf.format(new Date(Long.parseLong(time)));
                time = Long.toString(sdf.parse(timeFormat).getTime());
            }
        } catch (Exception e) {
            logger.warn("format time error", e);
        }
        return Long.parseLong(time);
    }

    public static boolean isTimestampFormat(String format) {
        return (DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS.equals(format)
                || DEFAULT_DATETIME_PATTERN_WITH_MILLISECONDS.equals(format)
                || DEFAULT_DATETIME_PATTERN_WITH_TIMEZONE.equals(format));
    }
}

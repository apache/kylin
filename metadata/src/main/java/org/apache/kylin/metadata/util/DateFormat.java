package org.apache.kylin.metadata.util;

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

    static SimpleDateFormat getDateFormat(String datePattern) {
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

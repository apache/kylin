/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kylinolap.dict;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;

/**
 * A dictionary for date string (date only, no time).
 * 
 * Dates are numbered from 1970-1-1 -- 0 for 1970-1-1, 1 for 1-2, 2 for 1-3 and
 * so on. With 2 bytes, 65536 states, can express dates up to the year of 2149.
 * 
 * Note the implementation is not thread-safe.
 * 
 * @author yangli9
 */
public class DateStrDictionary extends Dictionary<String> {

    static final String DEFAULT_DATE_PATTERN = "yyyy-MM-dd";
    static final String DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS = "yyyy-MM-dd HH:mm:ss";
    static final String DEFAULT_DATETIME_PATTERN_WITH_MILLISECONDS = "yyyy-MM-dd HH:mm:ss.SSS";

    static final private Map<String, ThreadLocal<SimpleDateFormat>> threadLocalMap = new ConcurrentHashMap<String, ThreadLocal<SimpleDateFormat>>();

    static SimpleDateFormat getDateFormat(String datePattern) {
        ThreadLocal<SimpleDateFormat> formatThreadLocal = threadLocalMap.get(datePattern);
        if (formatThreadLocal == null) {
            threadLocalMap.put(datePattern, formatThreadLocal = new ThreadLocal<SimpleDateFormat>());
        }
        SimpleDateFormat format = formatThreadLocal.get();
        if (format == null) {
            format = new SimpleDateFormat(datePattern);
            format.setTimeZone(TimeZone.getTimeZone("GMT")); // NOTE: this must
                                                             // be GMT to
                                                             // calculate
                                                             // epoch date
                                                             // correctly
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
        if (str.length() == 10) {
            return stringToDate(str, DEFAULT_DATE_PATTERN).getTime();
        } else if (str.length() == 19) {
            return stringToDate(str, DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS).getTime();
        } else if (str.length() == 23) {
            return stringToDate(str, DEFAULT_DATETIME_PATTERN_WITH_MILLISECONDS).getTime();
        } else {
            throw new IllegalArgumentException("there is no valid date pattern for:" + str);
        }
    }

    // ============================================================================

    private String pattern;
    private int baseId;

    public DateStrDictionary() {
        init(DEFAULT_DATE_PATTERN, 0);
    }

    public DateStrDictionary(String datePattern, int baseId) {
        init(datePattern, baseId);
    }

    private void init(String datePattern, int baseId) {
        this.pattern = datePattern;
        this.baseId = baseId;
    }

    @Override
    public int getMinId() {
        return baseId;
    }

    @Override
    public int getMaxId() {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getSizeOfId() {
        return 3;
    }

    @Override
    public int getSizeOfValue() {
        return pattern.length();
    }

    @Override
    protected boolean isNullByteForm(byte[] value, int offset, int len) {
        return value == null || len == 0;
    }

    @Override
    final protected int getIdFromValueImpl(String value, int roundFlag) {
        Date date = stringToDate(value, pattern);
        int id = calcIdFromSeqNo(getNumOfDaysSince0000(date));
        if (id < 0 || id >= 16777216)
            throw new IllegalArgumentException("'" + value + "' encodes to '" + id + "' which is out of range of 3 bytes");

        return id;
    }

    @Override
    final protected String getValueFromIdImpl(int id) {
        if (id < baseId)
            throw new IllegalArgumentException("ID '" + id + "' must not be less than base ID " + baseId);
        Date d = getDateFromNumOfDaysSince0000(calcSeqNoFromId(id));
        return dateToString(d, pattern);
    }

    private int getNumOfDaysSince0000(Date d) {
        // 86400000 = 1000 * 60 * 60 * 24
        // -719530 is offset of 0000-01-01
        return (int) (d.getTime() / 86400000 + 719530);
    }

    private Date getDateFromNumOfDaysSince0000(int n) {
        long millis = ((long) n - 719530) * 86400000;
        return new Date(millis);
    }

    @Override
    final protected int getIdFromValueBytesImpl(byte[] value, int offset, int len, int roundingFlag) {
        try {
            return getIdFromValue(new String(value, offset, len, "ISO-8859-1"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e); // never happen
        }
    }

    @Override
    final protected int getValueBytesFromIdImpl(int id, byte[] returnValue, int offset) {
        String date = getValueFromId(id);
        byte bytes[];
        try {
            bytes = date.getBytes("ISO-8859-1");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e); // never happen
        }
        System.arraycopy(bytes, 0, returnValue, offset, bytes.length);
        return bytes.length;
    }

    private int calcIdFromSeqNo(int seq) {
        return seq < 0 ? seq : baseId + seq;
    }

    private int calcSeqNoFromId(int id) {
        return id - baseId;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(pattern);
        out.writeInt(baseId);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        String pattern = in.readUTF();
        int baseId = in.readInt();
        init(pattern, baseId);
    }

    @Override
    public int hashCode() {
        return 31 * baseId + pattern.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if ((o instanceof DateStrDictionary) == false)
            return false;
        DateStrDictionary that = (DateStrDictionary) o;
        return StringUtils.equals(this.pattern, that.pattern) && this.baseId == that.baseId;
    }

    @Override
    public void dump(PrintStream out) {
        out.println(this.toString());
    }

    @Override
    public String toString() {
        return "DateStrDictionary [pattern=" + pattern + ", baseId=" + baseId + "]";
    }

}

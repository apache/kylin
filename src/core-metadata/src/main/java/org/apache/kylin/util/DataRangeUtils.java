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
package org.apache.kylin.util;

import static org.apache.kylin.common.exception.code.ErrorCodeServer.TIME_INVALID_RANGE_END_LESS_THAN_EQUALS_START;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.TIME_INVALID_RANGE_LESS_THAN_ZERO;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.TIME_INVALID_RANGE_NOT_CONSISTENT;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.TIME_INVALID_RANGE_NOT_FORMAT_MS;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.metadata.model.PartitionDesc;

public final class DataRangeUtils {
    private DataRangeUtils() {
    }

    public static void validateRange(String start, String end) {
        validateRange(Long.parseLong(start), Long.parseLong(end));
    }

    public static void validateRange(long start, long end) {
        if (start < 0 || end < 0) {
            throw new KylinException(TIME_INVALID_RANGE_LESS_THAN_ZERO);
        }
        if (start >= end) {
            throw new KylinException(TIME_INVALID_RANGE_END_LESS_THAN_EQUALS_START);
        }
    }

    public static void validateDataRange(String start, String end) {
        validateDataRange(start, end, null);
    }

    public static void validateDataRange(String start, String end, String partitionColumnFormat) {
        if (StringUtils.isEmpty(start) && StringUtils.isEmpty(end)) {
            return;
        }
        if (StringUtils.isNotEmpty(start) && StringUtils.isNotEmpty(end)) {
            long startLong = 0;
            long endLong = 0;

            try {
                startLong = Long.parseLong(start);
                endLong = Long.parseLong(end);
            } catch (Exception e) {
                throw new KylinException(TIME_INVALID_RANGE_NOT_FORMAT_MS, e);
            }

            if (startLong < 0 || endLong < 0) {
                throw new KylinException(TIME_INVALID_RANGE_LESS_THAN_ZERO);
            }

            try {
                startLong = DateFormat.getFormatTimeStamp(start, transformTimestamp2Format(partitionColumnFormat));
                endLong = DateFormat.getFormatTimeStamp(end, transformTimestamp2Format(partitionColumnFormat));
            } catch (Exception e) {
                throw new KylinException(TIME_INVALID_RANGE_NOT_FORMAT_MS);
            }

            if (startLong >= endLong) {
                throw new KylinException(TIME_INVALID_RANGE_END_LESS_THAN_EQUALS_START);
            }

        } else {
            throw new KylinException(TIME_INVALID_RANGE_NOT_CONSISTENT);
        }

    }

    private static String transformTimestamp2Format(String columnFormat) {
        for (PartitionDesc.TimestampType timestampType : PartitionDesc.TimestampType.values()) {
            if (timestampType.name.equals(columnFormat)) {
                return timestampType.format;
            }
        }
        return columnFormat;
    }

    /**
     * Checks if the given time range overlaps with any range in the list using half-open interval principle
     * [start, end).
     * The start time is inclusive, but the end time is exclusive.
     *
     * @param rangeList  List of existing time ranges, each range is a String array of [startTime, endTime)
     * @param range      New time range to check, represented as a String array [startTime, endTime)
     * @param dateFormat Date format pattern for parsing String dates (e.g., "yyyy-MM-dd")
     * @return true if the new range overlaps with any existing range, false otherwise
     */
    public static boolean timeOverlap(List<String[]> rangeList, String[] range, String dateFormat) {
        // full build
        Preconditions.checkArgument(rangeList != null);
        Preconditions.checkArgument(range != null);
        if (rangeList.size() == 1 && "0".equals(rangeList.get(0)[0]) && "0".equals(rangeList.get(0)[1])) {
            return true;
        }
        if (rangeList.isEmpty() || range.length != 2 || StringUtils.isEmpty(dateFormat)) {
            return false;
        }
        if ("0".equals(range[0]) && "0".equals(range[1])) {
            return true;
        }
        SimpleDateFormat sdf = new SimpleDateFormat(dateFormat, Locale.ROOT);
        // Ensure strict date parsing
        sdf.setLenient(false);
        try {
            Date newStart = sdf.parse(range[0]);
            Date newEnd = sdf.parse(range[1]);

            if (!newStart.before(newEnd)) {
                throw new IllegalArgumentException("Invalid time range: start time must be before end time");
            }

            for (String[] existingRange : rangeList) {
                if (existingRange.length != 2)
                    continue;

                Date existingStart = sdf.parse(existingRange[0]);
                Date existingEnd = sdf.parse(existingRange[1]);

                // Overlap judgment using the left-closed right-open interval [start, end)
                // The condition for overlapping intervals: newStart < existingEnd && existingStart < newEnd
                if (newStart.before(existingEnd) && existingStart.before(newEnd)) {
                    return true;
                }
            }
        } catch (ParseException e) {
            e.printStackTrace();
            return false;
        }
        return false;
    }

    /***
     * Merge consecutive time ranges
     * e.g. [ 2001, 2002, 2003, 2005, 2007, 2008 ] ---> [ [ 2001, 2004 ], [ 2005,2006], [2007, 2009]]
     * @param values e.g. [ 2001, 2002, 2003, 2005, 2007, 2008 ]
     * @param dateFormat e.g. yyyy
     * @return
     */
    public static List<String[]> mergeTimeRange(List<String> values, String dateFormat) throws ParseException {
        List<String[]> mergedRanges = new ArrayList<>();
        if (values == null || values.isEmpty()) {
            return mergedRanges;
        }

        SimpleDateFormat sdf = new SimpleDateFormat(dateFormat, Locale.ROOT);
        try {
            List<Date> dates = new ArrayList<>();
            for (String value : values) {
                dates.add(sdf.parse(value));
            }
            // Sort the dates
            dates.sort(Date::compareTo);
            Date start = dates.get(0);
            Date end = start;

            for (int i = 1; i < dates.size(); i++) {
                Date current = dates.get(i);
                // Determine if the current date is consecutive to the end date
                boolean isConsecutive = false;
                Calendar calStart = Calendar.getInstance(TimeZone.getDefault(), Locale.ROOT);
                Calendar calEnd = Calendar.getInstance(TimeZone.getDefault(), Locale.ROOT);
                Calendar calCurrent = Calendar.getInstance(TimeZone.getDefault(), Locale.ROOT);
                calStart.setTime(end);
                calEnd.setTime(end);
                calCurrent.setTime(current);

                switch (dateFormat) {
                case "yyyy":
                    isConsecutive = calCurrent.get(Calendar.YEAR) == calEnd.get(Calendar.YEAR) + 1;
                    break;
                case "yyyy-MM-dd":
                case "yyyyMMdd":
                case "yyyy/MM/dd":
                    long diffDays = (current.getTime() - end.getTime()) / (1000 * 60 * 60 * 24);
                    isConsecutive = diffDays == 1;
                    break;
                case "yyyy-MM":
                case "yyyyMM":
                    isConsecutive = (calCurrent.get(Calendar.YEAR) == calEnd.get(Calendar.YEAR)
                            && calCurrent.get(Calendar.MONTH) == calEnd.get(Calendar.MONTH) + 1)
                            || (calCurrent.get(Calendar.YEAR) == calEnd.get(Calendar.YEAR) + 1
                                    && calCurrent.get(Calendar.MONTH) == Calendar.JANUARY
                                    && calEnd.get(Calendar.MONTH) == Calendar.DECEMBER);
                    break;
                default:
                    throw new IllegalArgumentException(
                            String.format(Locale.ROOT, "dateFormat %s is not supported", dateFormat));
                }

                if (isConsecutive) {
                    end = current;
                } else {
                    // Add the current range to the result
                    mergedRanges.add(new String[] { sdf.format(start), incrementDate(end, dateFormat, sdf) });
                    start = current;
                    end = current;
                }
            }
            // Add the last range
            mergedRanges.add(new String[] { sdf.format(start), incrementDate(end, dateFormat, sdf) });
        } catch (ParseException e) {
            e.printStackTrace();
            throw e;
        }
        return mergedRanges;
    }

    /**
     * Increment the date to achieve a left-closed, right-open interval.
     *
     * @param date       The date to increment
     * @param dateFormat The format of the date
     * @param sdf        The SimpleDateFormat object
     * @return The incremented date as a string
     */
    private static String incrementDate(Date date, String dateFormat, SimpleDateFormat sdf) {
        Calendar calendar = Calendar.getInstance(TimeZone.getDefault(), Locale.ROOT);
        calendar.setTime(date);
        switch (dateFormat) {
        case "yyyy":
            calendar.add(Calendar.YEAR, 1);
            break;
        case "yyyy-MM-dd":
        case "yyyyMMdd":
        case "yyyy/MM/dd":
            calendar.add(Calendar.DAY_OF_MONTH, 1);
            break;
        case "yyyy-MM":
        case "yyyyMM":
            calendar.add(Calendar.MONTH, 1);
            break;
        // Add more cases for other date formats as needed
        default:
            throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "dateFormat %s is not supported", dateFormat));
        }
        return sdf.format(calendar.getTime());
    }

    /**
     * Check if dataRange is within the overall range defined by partitionRange
     * @param dataRange The date range to check [startDate, endDate]
     * @param partitionRange List of partition ranges to check against
     * @param timeFmt Date format pattern (e.g., "yyyy-MM-dd")
     * @return true if dataRange is within the overall partition range, false otherwise
     */
    public static boolean timeInRange(String[] dataRange, List<String[]> partitionRange, String timeFmt) {
        if (dataRange == null || dataRange.length != 2 || partitionRange == null || partitionRange.isEmpty()
                || timeFmt == null || timeFmt.isEmpty()) {
            return false;
        }

        SimpleDateFormat sdf = new SimpleDateFormat(timeFmt, Locale.ROOT);
        sdf.setLenient(false);

        try {
            // Parse the data range dates
            Date dataStart = sdf.parse(dataRange[0]);
            Date dataEnd = sdf.parse(dataRange[1]);

            // Validate data range
            if (!dataStart.before(dataEnd)) {
                return false;
            }

            // Initialize min and max dates for partition range
            Date minPartitionStart = null;
            Date maxPartitionEnd = null;

            // Find the min start date and max end date in partitionRange
            for (String[] range : partitionRange) {
                Date partitionStart = sdf.parse(range[0]);
                Date partitionEnd = sdf.parse(range[1]);

                if (minPartitionStart == null || partitionStart.before(minPartitionStart)) {
                    minPartitionStart = partitionStart;
                }
                if (maxPartitionEnd == null || partitionEnd.after(maxPartitionEnd)) {
                    maxPartitionEnd = partitionEnd;
                }
            }

            // Check if dataRange is within the overall partition range
            if (minPartitionStart != null && maxPartitionEnd != null && !dataStart.before(minPartitionStart)
                    && !dataEnd.after(maxPartitionEnd)) {
                return true;
            }
        } catch (ParseException e) {
            e.printStackTrace();
            return false;
        }

        return false;
    }

}

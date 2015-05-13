package org.apache.kylin.common.util;

/**
 */
public class TimeUtil {
    private static int ONE_MINUTE_TS = 60 * 1000;
    private static int ONE_HOUR_TS = 60 * 60 * 1000;

    public static long getMinuteStart(long ts) {
        return ts / ONE_MINUTE_TS * ONE_MINUTE_TS;
    }

    public static long getHourStart(long ts) {
        return ts / ONE_HOUR_TS * ONE_HOUR_TS;
    }
}

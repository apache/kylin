package org.apache.kylin.common.util;

/**
 */
public class TimeUtil {
    public enum NormalizedTimeUnit {
        MINUTE, HOUR, DAY
    }

    private static long ONE_MINUTE_TS = 60 * 1000;
    private static long ONE_HOUR_TS = 60 * ONE_MINUTE_TS;
    private static long ONE_DAY_TS = 24 * ONE_HOUR_TS;

    public static long getMinuteStart(long ts) {
        return ts / ONE_MINUTE_TS * ONE_MINUTE_TS;
    }

    public static long getHourStart(long ts) {
        return ts / ONE_HOUR_TS * ONE_HOUR_TS;
    }

    public static long getDayStart(long ts) {
        return ts / ONE_DAY_TS * ONE_DAY_TS;
    }

    public static long getNextPeriodStart(long ts, long period) {
        return ((ts + period - 1) / period) * period;
    }
}

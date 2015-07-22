package org.apache.kylin.common.util;

import org.apache.kylin.common.util.TimeUtil.NormalizedTimeUnit;
import org.junit.Assert;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 */
public class TimeUtilTest {
    public static long normalizeTime(long timeMillis, NormalizedTimeUnit unit) {
        Calendar a = Calendar.getInstance();
        Calendar b = Calendar.getInstance();
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
        java.text.DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

        long t1 = dateFormat.parse("2012/01/01 00:00:01").getTime();
        Assert.assertEquals(normalizeTime(t1, NormalizedTimeUnit.HOUR), TimeUtil.getHourStart(t1));
        Assert.assertEquals(normalizeTime(t1, NormalizedTimeUnit.MINUTE), TimeUtil.getMinuteStart(t1));

        long t2 = dateFormat.parse("2012/12/31 11:02:01").getTime();
        Assert.assertEquals(normalizeTime(t2, NormalizedTimeUnit.HOUR), TimeUtil.getHourStart(t2));
        Assert.assertEquals(normalizeTime(t2, NormalizedTimeUnit.MINUTE), TimeUtil.getMinuteStart(t2));
    }


}

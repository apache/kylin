package org.apache.kylin.dict;

import org.apache.kylin.common.util.DateFormat;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Date;

/**
 */
public class TimeStrDictionaryTests {
    TimeStrDictionary dict;

    @Before
    public void setup() {
        dict = new TimeStrDictionary();
    }

    @Test
    public void basicTest() {
        int a = dict.getIdFromValue("1999-01-01");
        int b = dict.getIdFromValue("1999-01-01 00:00:00");
        int c = dict.getIdFromValue("1999-01-01 00:00:00.000");
        int d = dict.getIdFromValue("1999-01-01 00:00:00.022");

        Assert.assertEquals(a, b);
        Assert.assertEquals(a, c);
        Assert.assertEquals(a, d);
    }

    @Test
    public void testEncodeDecode() {
        encodeDecode("1999-01-12", DateFormat.DEFAULT_DATE_PATTERN);
        encodeDecode("2038-01-09", DateFormat.DEFAULT_DATE_PATTERN);
        encodeDecode("2038-01-08", DateFormat.DEFAULT_DATE_PATTERN);
        encodeDecode("1970-01-01", DateFormat.DEFAULT_DATE_PATTERN);
        encodeDecode("1970-01-02", DateFormat.DEFAULT_DATE_PATTERN);

        encodeDecode("1999-01-12 11:00:01", DateFormat.DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS);
        encodeDecode("2038-01-09 01:01:02", DateFormat.DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS);
        encodeDecode("2038-01-19 03:14:07", DateFormat.DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS);
        encodeDecode("1970-01-01 23:22:11", DateFormat.DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS);
        encodeDecode("1970-01-02 23:22:11", DateFormat.DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS);
    }

    @Test
    public void testIllegal() {
        Assert.assertEquals(-1, dict.getIdFromValue("2038-01-19 03:14:08"));
    }

    public void encodeDecode(String origin, String pattern) {
        int a = dict.getIdFromValue(origin);
        String v = dict.getValueFromId(a);
        long ts = Long.parseLong(v);
        String back = DateFormat.dateToString(new Date(ts), pattern);
        Assert.assertEquals(origin, back);
    }



}

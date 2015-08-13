package org.apache.kylin.metadata.measure.fixedlen;

import org.apache.kylin.metadata.model.DataType;
import org.junit.Test;

/**
 */
public class FixedPointLongCodecTest {

    @Test
    public void testEncode1() {
        FixedPointLongCodec codec = new FixedPointLongCodec(DataType.getInstance("decimal(18,5)"));
        long x = codec.getValueIgnoringDecimalPoint("12.12345");
        org.junit.Assert.assertEquals(1212345, x);
    }

    @Test
    public void testEncode2() {
        FixedPointLongCodec codec = new FixedPointLongCodec(DataType.getInstance("decimal(18,5)"));
        long x = codec.getValueIgnoringDecimalPoint("12.1234");
        org.junit.Assert.assertEquals(1212340, x);
    }

    @Test
    public void testEncode3() {
        FixedPointLongCodec codec = new FixedPointLongCodec(DataType.getInstance("decimal(18,5)"));
        long x = codec.getValueIgnoringDecimalPoint("12.123456");
        org.junit.Assert.assertEquals(1212345, x);
    }

    @Test
    public void testEncode4() {
        FixedPointLongCodec codec = new FixedPointLongCodec(DataType.getInstance("decimal(18,5)"));
        long x = codec.getValueIgnoringDecimalPoint("12");
        org.junit.Assert.assertEquals(1200000, x);
    }

    @Test
    public void testDecode1() {
        FixedPointLongCodec codec = new FixedPointLongCodec(DataType.getInstance("decimal(18,5)"));
        String x = codec.restoreDecimalPoint(1212345);
        org.junit.Assert.assertEquals("12.12345", x);
    }
}

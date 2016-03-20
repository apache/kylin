package org.apache.kylin.measure.raw;


import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class RawAggregatorTest {
    private RawAggregator agg = new RawAggregator();

    @Test
    public void testNormal(){
        int size = 100;
        List<ByteArray> valueList = new ArrayList<ByteArray>(size);
        for (Integer i = 0; i < size; i++) {
            ByteArray key = new ByteArray(1);
            BytesUtil.writeUnsigned(i, key.array(), 0, key.length());
            valueList.add(key);
        }
        agg.aggregate(valueList);
        agg.aggregate(valueList);
        assertEquals(valueList.size() * 2, agg.getState().size());
    }

    @Test
    public void testNull(){
        agg.aggregate(null);
        assertEquals(agg.getState(), null);
    }
}

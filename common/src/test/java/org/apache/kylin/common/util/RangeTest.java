package org.apache.kylin.common.util;

import com.google.common.collect.Range;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by Hongbin Ma(Binmahone) on 4/14/15.
 */
public class RangeTest {
    @Test
    public void basicTest() {
        Range a0 = Range.closed(2, 5);
        Range a1 = Range.open(2, 5);
        Range a2 = Range.open(2, 5);
        Range a3 = Range.open(2, 3);
        Range a4 = Range.open(2, 9);

        Assert.assertTrue(RangeUtil.remove(a0, a1).size() == 2);
        Assert.assertTrue(RangeUtil.remove(a1, a2).size() == 0);
        Assert.assertTrue(RangeUtil.remove(a1, a3).size() == 1);
        Assert.assertTrue(RangeUtil.remove(a1, a3).get(0).equals(Range.closedOpen(3, 5)));
        Assert.assertTrue(RangeUtil.remove(a1, a4).size() == 0);

    }
}

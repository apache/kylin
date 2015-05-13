package org.apache.kylin.common.util;

import com.google.common.collect.Ranges;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 */
public class RangeUtilTest {
    @Test
    public void testFilter() {
        NavigableMap<Integer, Integer> map = new TreeMap<>();
        map.put(3, 3);
        map.put(1, 1);
        map.put(2, 2);
        Map<Integer, Integer> subMap = RangeUtil.filter(map, Ranges.<Integer> all());
        Assert.assertEquals(subMap.size(), 3);

        subMap = RangeUtil.filter(map, Ranges.atLeast(2));
        Assert.assertEquals(subMap.size(), 2);

        subMap = RangeUtil.filter(map, Ranges.greaterThan(2));
        Assert.assertEquals(subMap.size(), 1);

        subMap = RangeUtil.filter(map, Ranges.greaterThan(0));
        Assert.assertEquals(subMap.size(), 3);

        subMap = RangeUtil.filter(map, Ranges.greaterThan(5));
        Assert.assertEquals(subMap.size(), 0);

        subMap = RangeUtil.filter(map, Ranges.atMost(2));
        Assert.assertEquals(subMap.size(), 2);

        subMap = RangeUtil.filter(map, Ranges.lessThan(2));
        Assert.assertEquals(subMap.size(), 1);

        subMap = RangeUtil.filter(map, Ranges.lessThan(5));
        Assert.assertEquals(subMap.size(), 3);

        subMap = RangeUtil.filter(map, Ranges.lessThan(0));
        Assert.assertEquals(subMap.size(), 0);
    }
}

package com.kylinolap.common.util;

import com.google.common.collect.Lists;
import org.junit.Test;
import static org.junit.Assert.*;
import java.util.Comparator;
import java.util.List;

/**
 * Created by Hongbin Ma(Binmahone) on 1/5/15.
 */
public class PartialSorterTest {
    @Test
    public void basicTest() {
        List<Integer> a = Lists.newArrayList();
        a.add(100);
        a.add(2);
        a.add(92);
        a.add(1);
        a.add(0);
        PartialSorter.partialSort(a, Lists.newArrayList(1, 3, 4), new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o1.compareTo(o2);
            }
        });
        assertArrayEquals(a.toArray(), new Integer[] { 100, 0, 92, 1, 2 });
    }

}

package com.kylinolap.common.util;

import com.google.common.collect.*;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by Hongbin Ma(Binmahone) on 1/13/15.
 */
public class RangeSetTest {
    @Test
    public void test1() throws IOException, InterruptedException {
        RangeSet<Integer> rangeSet = TreeRangeSet.create();
        Range a = Range.closedOpen(1, 2);
        Range b = Range.closedOpen(2, 3);
        Range newa = a.canonical(DiscreteDomain.integers());
        Range newb = b.canonical(DiscreteDomain.integers());
        rangeSet.add(newa);
        rangeSet.add(newb);
        System.out.println(rangeSet);

        for (Range r : rangeSet.asRanges()) {
            ContiguousSet<Integer> s = ContiguousSet.create(r, DiscreteDomain.integers());
            for (Integer x : s) {
                System.out.println(x);
            }
        }

    }
}

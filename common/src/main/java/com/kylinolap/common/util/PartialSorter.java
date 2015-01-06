package com.kylinolap.common.util;

import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Created by Hongbin Ma(Binmahone) on 1/5/15.
 *
 * This utility class sorts only the specified part of a list
 */
public class PartialSorter {
    public static <T> void partialSort(List<T> list, List<Integer> items, Comparator<? super T> c) {
        List<T> temp = Lists.newLinkedList();
        for (int index : items) {
            temp.add(list.get(index));
        }
        Collections.sort(temp, c);
        for (int i = 0; i < temp.size(); ++i) {
            list.set(items.get(i), temp.get(i));
        }
    }
}

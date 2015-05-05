package org.apache.kylin.common.util;

import com.google.common.base.Function;
import com.google.common.collect.TreeMultimap;

import java.util.Iterator;

/**
 * Created by Hongbin Ma(Binmahone) on 5/5/15.
 */
public class SortUtil {
    public static <T extends Comparable, E extends Comparable> Iterator<T> extractAndSort(Iterator<T> input, Function<T, E> extractor) {
        TreeMultimap<E, T> reorgnized = TreeMultimap.create();
        while (input.hasNext()) {
            T t = input.next();
            E e = extractor.apply(t);
            reorgnized.put(e, t);
        }
        return reorgnized.values().iterator();
    }
}

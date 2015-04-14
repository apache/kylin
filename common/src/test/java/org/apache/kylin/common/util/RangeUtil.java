package org.apache.kylin.common.util;

import com.google.common.collect.Range;
import com.google.common.collect.Ranges;

/**
 * Created by Hongbin Ma(Binmahone) on 4/14/15.
 */
public class RangeUtil {
    public static <C extends Comparable<?>> Range<C> retain(Range<C> self, Range<C> other) {
        if (!self.isConnected(other) || self.intersection(other).isEmpty()) {
            return null;
        }
        return null;
    }


}

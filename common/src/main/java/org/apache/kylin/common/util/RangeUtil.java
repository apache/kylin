package org.apache.kylin.common.util;

import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

/**
 * Created by Hongbin Ma(Binmahone) on 4/14/15.
 */
public class RangeUtil {
    /**
     * remove from self the elements that exist in other
     * @return
     */
    public static <C extends Comparable<?>> List<Range<C>> remove(Range<C> self, Range<C> other) {
        RangeSet<C> rangeSet = TreeRangeSet.create();
        rangeSet.add(self);
        rangeSet.remove(other);
        return Lists.newArrayList(rangeSet.asRanges());
    }
}

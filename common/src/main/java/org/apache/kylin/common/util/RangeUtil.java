package org.apache.kylin.common.util;

import java.util.List;

import com.google.common.collect.*;

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

    public static String formatTsRange(Range<Long> tsRange) {
        if(tsRange == null)
            return null;

        StringBuilder sb = new StringBuilder();
        if (tsRange.hasLowerBound()) {
            if (tsRange.lowerBoundType() == BoundType.CLOSED) {
                sb.append("[");
            } else {
                sb.append("(");
            }
            DateFormat.formatToTimeStr(tsRange.lowerEndpoint());
        } else {
            sb.append("(null");
        }

        sb.append("~");

        if (tsRange.hasUpperBound()) {
            DateFormat.formatToTimeStr(tsRange.upperEndpoint());
            if (tsRange.upperBoundType() == BoundType.CLOSED) {
                sb.append("]");
            } else {
                sb.append(")");
            }
        } else {
            sb.append("null)");
        }
        return sb.toString();
    }
}

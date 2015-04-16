package org.apache.kylin.common.util;

import java.util.Collections;
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
        // mimic the following logic in guava 18:
        //        RangeSet<C> rangeSet = TreeRangeSet.create();
        //        rangeSet.add(self);
        //        rangeSet.remove(other);
        //        return Lists.newArrayList(rangeSet.asRanges());

        if (!self.isConnected(other)) {
            return Collections.singletonList(self);
        }
        Range<C> share = self.intersection(other);
        if (share.isEmpty()) {
            return Collections.singletonList(self);
        }

        List<Range<C>> ret = Lists.newArrayList();

        //see left part
        if (!self.hasLowerBound()) {
            if (share.hasLowerBound()) {
                if (share.lowerBoundType() == BoundType.CLOSED) {
                    ret.add(Ranges.lessThan(share.lowerEndpoint()));
                } else {
                    ret.add(Ranges.atMost(share.lowerEndpoint()));
                }
            }
        } else {
            if (self.lowerEndpoint() != share.lowerEndpoint()) {
                if (self.lowerBoundType() == BoundType.CLOSED) {
                    if (share.lowerBoundType() == BoundType.CLOSED) {
                        ret.add(Ranges.closedOpen(self.lowerEndpoint(), share.lowerEndpoint()));
                    } else {
                        ret.add(Ranges.closed(self.lowerEndpoint(), share.lowerEndpoint()));
                    }
                } else {
                    if (share.lowerBoundType() == BoundType.CLOSED) {
                        ret.add(Ranges.open(self.lowerEndpoint(), share.lowerEndpoint()));
                    } else {
                        ret.add(Ranges.openClosed(self.lowerEndpoint(), share.lowerEndpoint()));
                    }
                }
            } else {
                if (self.lowerBoundType() == BoundType.CLOSED && share.lowerBoundType() == BoundType.OPEN) {
                    ret.add(Ranges.closed(self.lowerEndpoint(), share.lowerEndpoint()));
                }
            }
        }

        //see right part 
        if (!self.hasUpperBound()) {
            if (share.hasUpperBound()) {
                if (share.upperBoundType() == BoundType.CLOSED) {
                    ret.add(Ranges.greaterThan(share.upperEndpoint()));
                } else {
                    ret.add(Ranges.atLeast(share.upperEndpoint()));
                }
            }
        } else {
            if (self.upperEndpoint() != share.upperEndpoint()) {
                if (self.upperBoundType() == BoundType.CLOSED) {
                    if (share.upperBoundType() == BoundType.CLOSED) {
                        ret.add(Ranges.openClosed(share.upperEndpoint(), self.upperEndpoint()));
                    } else {
                        ret.add(Ranges.closed(share.upperEndpoint(), self.upperEndpoint()));
                    }
                } else {
                    if (share.upperBoundType() == BoundType.CLOSED) {
                        ret.add(Ranges.open(share.upperEndpoint(), self.upperEndpoint()));
                    } else {
                        ret.add(Ranges.closedOpen(share.upperEndpoint(), self.upperEndpoint()));
                    }
                }
            } else {
                if (self.upperBoundType() == BoundType.CLOSED && share.upperBoundType() == BoundType.OPEN) {
                    ret.add(Ranges.closed(self.upperEndpoint(), share.upperEndpoint()));
                }
            }
        }


        return ret;

    }

    public static String formatTsRange(Range<Long> tsRange) {
        if (tsRange == null)
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

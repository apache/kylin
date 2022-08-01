/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.common.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.SortedSet;

import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;

/**
 */
public class RangeUtil {

    /**
     * for NavigableMap sorted by C, given a range of C, return the sub map whose key falls in the range
     */
    public static <C extends Comparable<?>, V> NavigableMap<C, V> filter(NavigableMap<C, V> values,
            Range<C> filterRange) {
        if (filterRange == null || filterRange.isEmpty()) {
            return Maps.newTreeMap();
        } else if (filterRange.equals(Range.all())) {
            return values;
        }

        if (filterRange.hasUpperBound() && !filterRange.hasLowerBound()) {
            return values.headMap(filterRange.upperEndpoint(), upperBoundInclusive(filterRange));
        } else if (filterRange.hasLowerBound() && !filterRange.hasUpperBound()) {
            return values.tailMap(filterRange.lowerEndpoint(), lowerBoundInclusive(filterRange));
        } else {
            return values.subMap(filterRange.lowerEndpoint(), lowerBoundInclusive(filterRange), //
                    filterRange.upperEndpoint(), upperBoundInclusive(filterRange));
        }
    }

    public static <C extends Comparable<?>> boolean lowerBoundInclusive(Range<C> range) {
        if (!range.hasLowerBound()) {
            throw new IllegalArgumentException(("This range does not have lower bound" + range));
        }
        return range.lowerBoundType() == BoundType.CLOSED;
    }

    public static <C extends Comparable<?>> boolean upperBoundInclusive(Range<C> range) {
        if (!range.hasUpperBound()) {
            throw new IllegalArgumentException(("This range does not have upper bound" + range));
        }
        return range.upperBoundType() == BoundType.CLOSED;
    }

    public static <C extends Comparable<?>> Range<C> merge(Range<C> a, Range<C> b) {
        if (a == null && b == null) {
            return null;
        } else if (a == null || b == null) {
            return a == null ? b : a;
        } else {
            return a.span(b);
        }
    }

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

        if (other == null || !self.isConnected(other)) {
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
                    ret.add(Range.lessThan(share.lowerEndpoint()));
                } else {
                    ret.add(Range.atMost(share.lowerEndpoint()));
                }
            }
        } else {
            if (self.lowerEndpoint() != share.lowerEndpoint()) {
                if (self.lowerBoundType() == BoundType.CLOSED) {
                    if (share.lowerBoundType() == BoundType.CLOSED) {
                        ret.add(Range.closedOpen(self.lowerEndpoint(), share.lowerEndpoint()));
                    } else {
                        ret.add(Range.closed(self.lowerEndpoint(), share.lowerEndpoint()));
                    }
                } else {
                    if (share.lowerBoundType() == BoundType.CLOSED) {
                        ret.add(Range.open(self.lowerEndpoint(), share.lowerEndpoint()));
                    } else {
                        ret.add(Range.openClosed(self.lowerEndpoint(), share.lowerEndpoint()));
                    }
                }
            } else {
                if (self.lowerBoundType() == BoundType.CLOSED && share.lowerBoundType() == BoundType.OPEN) {
                    ret.add(Range.closed(self.lowerEndpoint(), share.lowerEndpoint()));
                }
            }
        }

        //see right part
        if (!self.hasUpperBound()) {
            if (share.hasUpperBound()) {
                if (share.upperBoundType() == BoundType.CLOSED) {
                    ret.add(Range.greaterThan(share.upperEndpoint()));
                } else {
                    ret.add(Range.atLeast(share.upperEndpoint()));
                }
            }
        } else {
            if (self.upperEndpoint() != share.upperEndpoint()) {
                if (self.upperBoundType() == BoundType.CLOSED) {
                    if (share.upperBoundType() == BoundType.CLOSED) {
                        ret.add(Range.openClosed(share.upperEndpoint(), self.upperEndpoint()));
                    } else {
                        ret.add(Range.closed(share.upperEndpoint(), self.upperEndpoint()));
                    }
                } else {
                    if (share.upperBoundType() == BoundType.CLOSED) {
                        ret.add(Range.open(share.upperEndpoint(), self.upperEndpoint()));
                    } else {
                        ret.add(Range.closedOpen(share.upperEndpoint(), self.upperEndpoint()));
                    }
                }
            } else {
                if (self.upperBoundType() == BoundType.CLOSED && share.upperBoundType() == BoundType.OPEN) {
                    ret.add(Range.closed(self.upperEndpoint(), share.upperEndpoint()));
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
            sb.append(DateFormat.formatToTimeStr(tsRange.lowerEndpoint()));
        } else {
            sb.append("(-∞");
        }

        sb.append("~");

        if (tsRange.hasUpperBound()) {
            sb.append(DateFormat.formatToTimeStr(tsRange.upperEndpoint()));
            if (tsRange.upperBoundType() == BoundType.CLOSED) {
                sb.append("]");
            } else {
                sb.append(")");
            }
        } else {
            sb.append("+∞)");
        }
        return sb.toString();
    }

    public static ArrayList<Range<Integer>> buildRanges(SortedSet<Integer> values) {
        ArrayList<Range<Integer>> ranges = Lists.newArrayList();

        if (values == null || values.isEmpty())
            return ranges;

        Iterator<Integer> iter = values.iterator();
        int lastBegin = iter.next();
        int lastEnd = lastBegin;
        int temp = 0;
        for (int index = 1; index < values.size(); index++) {
            temp = iter.next();
            if (temp - lastEnd != 1) {
                ranges.add(Range.closed(lastBegin, lastEnd));
                lastBegin = temp;
            }
            lastEnd = temp;
        }
        ranges.add(Range.closed(lastBegin, lastEnd));
        return ranges;
    }
}

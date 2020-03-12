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

package org.apache.kylin.metadata.model;

import java.io.Serializable;
import java.util.Comparator;

/**
 * SegmentRange and TSRange seem similar but are different concepts.
 * 
 * - SegmentRange defines the range of a segment.
 * - TSRange is the time series range of the segment data.
 * - When segment range is defined by time, the two can be the same, in that case TSRange is a kind of SegmentRange.
 * - Duration segment creation (build/refresh/merge), a new segment is defined by either one of the two, not both.
 * - And the choice must be consistent across all following segment creation.
 */
@SuppressWarnings("serial")
public class SegmentRange<T extends Comparable> implements Comparable<SegmentRange>, Serializable {

    public final Endpoint<T> start;
    public final Endpoint<T> end;

    public SegmentRange(Endpoint start, Endpoint end) {
        this.start = start;
        this.end = end;
        checkState();
    }

    public SegmentRange(T start, T end) {
        if (start != null && end != null && start.getClass() != end.getClass())
            throw new IllegalArgumentException();

        this.start = new Endpoint(start, start == null, false);
        this.end = new Endpoint(end, false, end == null);
        checkState();
    }

    @Override
    public int compareTo(SegmentRange o) {
        int comp = this.start.compareTo(o.start);
        if (comp != 0)
            return comp;

        return this.end.compareTo(o.end);
    }

    private void checkState() {
        if (start.compareTo(end) > 0)
            throw new IllegalStateException();
    }

    public boolean isInfinite() {
        return start.isMin && end.isMax;
    }

    public boolean contains(SegmentRange o) {
        return this.start.compareTo(o.start) <= 0 && o.end.compareTo(this.end) <= 0;
    }

    public boolean overlaps(SegmentRange o) {
        return this.start.compareTo(o.end) < 0 && o.start.compareTo(this.end) < 0;
    }

    public boolean connects(SegmentRange o) {
        return this.end.compareTo(o.start) == 0;
    }

    public boolean apartBefore(SegmentRange o) {
        return this.end.compareTo(o.start) < 0;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[" + start + "," + end + ")";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((end == null) ? 0 : end.hashCode());
        result = prime * result + ((start == null) ? 0 : start.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SegmentRange other = (SegmentRange) obj;
        if (end == null) {
            if (other.end != null)
                return false;
        } else if (!end.equals(other.end))
            return false;
        if (start == null) {
            if (other.start != null)
                return false;
        } else if (!start.equals(other.start))
            return false;
        return true;
    }

    // ============================================================================

    public static class TSRange extends SegmentRange<Long> {

        public TSRange(Long start, Long end) {
            // [0, Long.MAX_VALUE) is full build (for historic reason)
            super(new Endpoint(isInfinite(start, end) ? 0 : start, isInfinite(start, end), false), //
                    new Endpoint(isInfinite(start, end) ? Long.MAX_VALUE : end, false, isInfinite(start, end)));
        }

        private static boolean isInfinite(Long start, Long end) {
            return (start == null || start <= 0) && (end == null || end == Long.MAX_VALUE);
        }

        public long duration() {
            return end.v - start.v;
        }

        public long startValue() {
            return start.v;
        }

        public long endValue() {
            return end.v;
        }
    }

    // ============================================================================

    // immutable
    public static class Endpoint<T extends Comparable> implements Comparable<Endpoint>, Serializable {

        public static final Comparator<Endpoint> comparator = getComparator(new Comparator() {
            @Override
            public int compare(Object o1, Object o2) {
                return ((Comparable) o1).compareTo(o2);
            }
        });

        public static Comparator<Endpoint> getComparator(final Comparator valueComparator) {
            return new Comparator<Endpoint>() {
                @Override
                public int compare(Endpoint a, Endpoint b) {
                    if (a == null || b == null)
                        throw new IllegalStateException();
                    if (a.isMin) {
                        return b.isMin ? 0 : -1;
                    } else if (b.isMin) {
                        return a.isMin ? 0 : 1;
                    } else if (a.isMax) {
                        return b.isMax ? 0 : 1;
                    } else if (b.isMax) {
                        return a.isMax ? 0 : -1;
                    } else {
                        return valueComparator.compare(a.v, b.v);
                    }
                }
            };
        }

        public final T v;
        public final boolean isMin;
        public final boolean isMax;

        private Endpoint(T v, boolean isMin, boolean isMax) {
            this.v = v;
            this.isMin = isMin;
            this.isMax = isMax;
        }

        @Override
        public int compareTo(Endpoint o) {
            return comparator.compare(this, o);
        }
        
        @Override
        public String toString() {
            String s = "" + v;
            if (isMin)
                s += "[min]";
            if (isMax)
                s += "[max]";
            return s;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + (isMax ? 1231 : 1237);
            result = prime * result + (isMin ? 1231 : 1237);
            result = prime * result + ((v == null) ? 0 : v.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            Endpoint other = (Endpoint) obj;
            if (isMax != other.isMax)
                return false;
            if (isMin != other.isMin)
                return false;

            return comparator.compare(this, other) == 0;
        }
    }
}

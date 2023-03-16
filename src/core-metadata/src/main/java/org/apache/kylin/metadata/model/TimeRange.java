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
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import org.apache.kylin.common.util.DateFormat;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;

public class TimeRange implements Serializable {
    long start;
    long end;

    public TimeRange() {
    }

    public TimeRange(Long s, Long e) {
        this.start = (s == null || s <= 0) ? 0 : s;
        this.end = (e == null || e == Long.MAX_VALUE) ? Long.MAX_VALUE : e;
        Preconditions.checkState(this.start <= this.end, getRangeCheckErrorMsg(this.start, this.end));
    }

    public TimeRange(Long s, Long e, Map<Integer, Long> offsetStart, Map<Integer, Long> offsetEnd) {
        this.start = (s == null || s <= 0) ? 0 : s;
        this.end = (e == null || e == Long.MAX_VALUE) ? Long.MAX_VALUE : e;

        Preconditions.checkState(this.start <= this.end, getRangeCheckErrorMsg(this.start, this.end));
        Iterator<Map.Entry<Integer, Long>> iter = offsetStart.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<Integer, Long> entry = iter.next();
            Integer partition = entry.getKey();
            Long startOffset = offsetStart.get(partition);
            Long endOffset = offsetEnd.get(partition);
            Preconditions.checkState(startOffset <= endOffset, getRangeCheckErrorMsg(startOffset, endOffset));
        }
    }

    private String getRangeCheckErrorMsg(Long start, Long end) {
        return String.format(Locale.ROOT, "expect: start <= end, actually: %s <= %s", start, end);
    }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
    }

    public long duration() {
        return end - start;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        TimeRange timeRange = (TimeRange) o;
        return start == timeRange.start && end == timeRange.end;
    }

    @Override
    public int hashCode() {
        return Objects.hash(start, end);
    }

    @Override
    public String toString() {
        return "TimeRange[" + start + "," + end + ")" + ".Time String[" + DateFormat.formatToTimeStr(start) + ","
                + DateFormat.formatToTimeStr(end) + "]";
    }
}

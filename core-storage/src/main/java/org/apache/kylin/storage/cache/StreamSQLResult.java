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

package org.apache.kylin.storage.cache;

import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;

import javax.annotation.Nullable;

import org.apache.kylin.common.util.RangeUtil;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.ITuple;
import org.apache.kylin.storage.tuple.Tuple;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;

/**
 */
public class StreamSQLResult {
    private Range<Long> timeCovered;
    private NavigableMap<Long, List<ITuple>> sortedRows;

    public StreamSQLResult(List<ITuple> rows, Range<Long> timeCovered, TblColRef partitionCol) {

        sortedRows = Maps.newTreeMap();
        for (ITuple row : rows) {

            if (partitionCol != null) {
                long t = Tuple.getTs(row, partitionCol);

                //will only cache rows that are within the time range
                if (timeCovered.contains(t)) {
                    if (!this.sortedRows.containsKey(t)) {
                        this.sortedRows.put(t, Lists.newArrayList(row));
                    } else {
                        this.sortedRows.get(t).add(row);
                    }
                }
            } else {
                if (!this.sortedRows.containsKey(0L)) {
                    this.sortedRows.put(0L, Lists.<ITuple> newArrayList());
                }
                this.sortedRows.get(0L).add(row);
            }
        }
        this.timeCovered = timeCovered;
    }

    public Range<Long> getReusableResults(Range<Long> tsRange) {

        if (tsRange.equals(timeCovered))
            return timeCovered;

        if (!timeCovered.isConnected(tsRange)) {
            //share nothing in common
            return null;
        }

        Range<Long> ret = timeCovered.intersection(tsRange);
        return ret.isEmpty() ? null : ret;
    }

    public Iterator<ITuple> reuse(Range<Long> reusablePeriod) {
        NavigableMap<Long, List<ITuple>> submap = RangeUtil.filter(sortedRows, reusablePeriod);
        return Iterators.concat(Iterators.transform(submap.values().iterator(), new Function<List<ITuple>, Iterator<ITuple>>() {
            @Nullable
            @Override
            public Iterator<ITuple> apply(List<ITuple> input) {
                return input.iterator();
            }
        }));
    }

    @Override
    public String toString() {
        return sortedRows.size() + " tuples cached for period " + RangeUtil.formatTsRange(timeCovered);
    }

    public long getEndTime() {
        return this.timeCovered.upperEndpoint();
    }
}

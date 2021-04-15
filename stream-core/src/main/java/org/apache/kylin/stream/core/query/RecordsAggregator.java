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

package org.apache.kylin.stream.core.query;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;

import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.stream.core.storage.Record;

public class RecordsAggregator implements Iterable<Record>{
    private SortedMap<String[], MeasureAggregator[]> aggBufMap;
    private ResponseResultSchema schema;
    private int[] groupIndexes;
    private int pushDownLimit;
    private TupleFilter havingFilter;

    public RecordsAggregator(ResponseResultSchema schema, Set<TblColRef> groups, TupleFilter havingFilter) {
        this.schema = schema;
        this.havingFilter = havingFilter;
        this.groupIndexes = new int[groups.size()];
        int i = 0;
        for (TblColRef group : groups) {
            groupIndexes[i] = schema.getIndexOfDimension(group);
            i++;
        }
        this.aggBufMap = Maps.newTreeMap(comparator);
        this.pushDownLimit = Integer.MAX_VALUE;
    }

    @Override
    public Iterator<Record> iterator() {
        Iterator<Entry<String[], MeasureAggregator[]>> it = aggBufMap.entrySet().iterator();

        final Iterator<Entry<String[], MeasureAggregator[]>> input = it;
        return new Iterator<Record>() {
            Record oneRecord = new Record(schema.getDimensionCount(), schema.getMetricsCount());
            Entry<String[], MeasureAggregator[]> returningEntry = null;
            final HavingFilterChecker havingFilterChecker = (havingFilter == null) ? null
                    : new HavingFilterChecker(havingFilter, schema);

            @Override
            public boolean hasNext() {
                while (input.hasNext()) {
                    returningEntry = input.next();
                    if (havingFilterChecker != null) {
                        if (havingFilterChecker.check(returningEntry.getValue())) {
                            return true;
                        }
                    } else {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public Record next() {
                String[] dimVals = returningEntry.getKey();
                for (int i = 0; i < dimVals.length; i++) {
                    oneRecord.setDimension(i, dimVals[i]);
                }
                MeasureAggregator[] measures = returningEntry.getValue();
                for (int i = 0; i < measures.length; i++) {
                    oneRecord.setMetric(i, measures[i].getState());
                }
                return oneRecord;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("unSupportOperation!");
            }
        };
    }

    public void aggregate(Iterator<Record> records) {
        while (records.hasNext()) {
            aggregate(records.next());
        }
    }

    public void aggregate(Record record) {
        String[] copyDimVals = new String[schema.getDimensionCount()];
        String[] dimVals = record.getDimensions();
        System.arraycopy(dimVals, 0, copyDimVals, 0, dimVals.length);
        aggregate(copyDimVals, record.getMetrics());
    }

    public void aggregate(String[] dimVals, Object[] metricsVals) {
        MeasureAggregator[] aggrs = aggBufMap.get(dimVals);
        if (aggrs == null) {
            //for storage push down limit
            if (aggBufMap.size() >= pushDownLimit) {
                return;
            }
            aggrs = newAggregators();
            aggBufMap.put(dimVals, aggrs);
        }
        for (int i = 0; i < aggrs.length; i++) {
            aggrs[i].aggregate(metricsVals[i]);
        }
    }

    private MeasureAggregator[] newAggregators() {
        String[] aggrFuncs = schema.getAggrFuncs();
        MeasureAggregator<?>[] result = new MeasureAggregator[aggrFuncs.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = MeasureAggregator.create(aggrFuncs[i], schema.getMetricsDataType(i));
        }
        return result;
    }

    final Comparator<String[]> comparator = new Comparator<String[]>() {
        @Override
        public int compare(String[] o1, String[] o2) {
            int result = 0;
            for (int i = 0; i < groupIndexes.length; i++) {
                int groupIdx = groupIndexes[i];
                if (o1[groupIdx] == null && o2[groupIdx] == null) {
                    continue;
                } else if (o1[groupIdx] != null && o2[groupIdx] == null) {
                    return 1;
                } else if (o1[groupIdx] == null && o2[groupIdx] != null) {
                    return -1;
                } else {
                    result = o1[groupIdx].compareTo(o2[groupIdx]);
                    if (result == 0) {
                        continue;
                    } else {
                        return result;
                    }
                }
            }
            return result;
        }
    };
}

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

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.measure.bitmap.BitmapCounter;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.apache.kylin.measure.percentile.PercentileCounter;
import org.apache.kylin.metadata.filter.IFilterCodeSystem;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.ITuple;

public class HavingFilterChecker {
    final HavingFilterTuple tuple = new HavingFilterTuple();
    final IFilterCodeSystem cs = new HavingFilterCodeSys();
    private TupleFilter havingFilter;
    private ResponseResultSchema schema;

    public HavingFilterChecker(TupleFilter havingFilter, ResponseResultSchema schema) {
        this.havingFilter = havingFilter;
        this.schema = schema;
    }

    public boolean check(MeasureAggregator[] aggregators) {
        tuple.aggrValues = aggregators;
        return havingFilter.evaluate(tuple, cs);
    }

    private class HavingFilterCodeSys implements IFilterCodeSystem {

        Object o2Cache;
        double n2Cache;

        @Override
        public int compare(Object o1, Object o2) {
            if (o1 == null && o2 == null)
                return 0;

            if (o1 == null) // null is bigger to align with CubeCodeSystem
                return 1;

            if (o2 == null) // null is bigger to align with CubeCodeSystem
                return -1;

            // for the 'having clause', we only concern numbers and BigDecimal
            // we try to cache the o2, which should be a constant according to CompareTupleFilter.evaluate()

            double n1;
            if (o1 instanceof Number) {
                n1 = ((Number) o1).doubleValue();
            } else if (o1 instanceof HLLCounter) {
                n1 = ((HLLCounter) o1).getCountEstimate();
            } else if (o1 instanceof BitmapCounter) {
                n1 = ((BitmapCounter) o1).getCount();
            } else if (o1 instanceof PercentileCounter) {
                n1 = ((PercentileCounter) o1).getResultEstimate();
            } else {
                throw new RuntimeException("Unknown datatype: value=" + o1 + ", class=" + o1.getClass());
            }

            double n2 = (o2Cache == o2) ? n2Cache : Double.parseDouble((String) o2);

            if (o2Cache == null) {
                o2Cache = o2;
                n2Cache = n2;
            }

            return Double.compare(n1, n2);
        }

        @Override
        public boolean isNull(Object code) {
            return code == null;
        }

        @Override
        public void serialize(Object code, ByteBuffer buf) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object deserialize(ByteBuffer buf) {
            throw new UnsupportedOperationException();
        }
    }

    private class HavingFilterTuple implements ITuple {
        MeasureAggregator[] aggrValues;

        @Override
        public Object getValue(TblColRef col) {
            return aggrValues[schema.getIndexOfMetrics(col)].getState();
        }

        @Override
        public List<String> getAllFields() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<TblColRef> getAllColumns() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object[] getAllValues() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ITuple makeCopy() {
            throw new UnsupportedOperationException();
        }
    }
}

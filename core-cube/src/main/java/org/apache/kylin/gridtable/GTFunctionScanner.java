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

package org.apache.kylin.gridtable;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.DecimalUtil;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.metadata.expression.TupleExpression;
import org.apache.kylin.metadata.filter.IFilterCodeSystem;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.IEvaluatableTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GTFunctionScanner implements IGTScanner {
    private static final Logger logger = LoggerFactory.getLogger(GTFunctionScanner.class);

    protected IGTScanner rawScanner;
    private final ImmutableBitSet rtAggrMetrics;
    private final Map<Integer, TupleExpression> tupleExpressionMap;
    private final IEvaluatableTuple oneTuple; // avoid instance creation
    private final IFilterCodeSystem<ByteArray> filterCodeSystem;

    private GTRecord next = null;

    protected GTFunctionScanner(IGTScanner rawScanner, GTScanRequest req) {
        this.rawScanner = rawScanner;
        this.tupleExpressionMap = req.getTupleExpressionMap();
        this.rtAggrMetrics = req.getRtAggrMetrics();
        this.oneTuple = new IEvaluatableTuple() {
            @Override
            public Object getValue(TblColRef col) {
                int idx = col.getColumnDesc().getZeroBasedIndex();
                return rtAggrMetrics.get(idx) ? DecimalUtil.toBigDecimal(next.getValue(idx)) : next.get(idx);
            }
        };
        this.filterCodeSystem = GTUtil.wrap(getInfo().codeSystem.getComparator());
    }

    @Override
    public GTInfo getInfo() {
        return rawScanner.getInfo();
    }

    @Override
    public void close() throws IOException {
        rawScanner.close();
    }

    @Override
    public Iterator<GTRecord> iterator() {
        return new Iterator<GTRecord>() {
            private Iterator<GTRecord> inputIterator = rawScanner.iterator();

            @Override
            public boolean hasNext() {
                if (next != null)
                    return true;

                if (inputIterator.hasNext()) {
                    next = inputIterator.next();
                    calculateDynamics();
                    return true;
                }
                return false;
            }

            @Override
            public GTRecord next() {
                // fetch next record
                if (next == null) {
                    hasNext();
                    if (next == null)
                        throw new NoSuchElementException();
                }

                GTRecord result = next;
                next = null;
                return result;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            private void calculateDynamics() {
                for (int c : tupleExpressionMap.keySet()) {
                    TupleExpression tupleExpr = tupleExpressionMap.get(c);
                    Object rtResult = tupleExpr.calculate(oneTuple, filterCodeSystem);
                    next.setValue(c, rtResult);
                }
            }
        };
    }
}

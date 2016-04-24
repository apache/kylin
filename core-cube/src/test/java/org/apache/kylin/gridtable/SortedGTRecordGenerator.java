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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;

import com.google.common.collect.Lists;

public class SortedGTRecordGenerator {

    private GTInfo info;
    private ArrayList<ColSpec> colSpecs = Lists.newArrayList();

    public SortedGTRecordGenerator(GTInfo info) {
        this.info = info;
    }

    public void addDimension(long cardinality, int length, Map<Integer, Integer> weights) {
        assert cardinality > 0;
        ColSpec spec = new ColSpec();
        spec.cardinality = cardinality;
        spec.length = length;
        spec.weights = weights;
        colSpecs.add(spec);
    }

    public void addMeasure(int length) {
        assert length > 0;
        ColSpec spec = new ColSpec();
        spec.length = length;
        colSpecs.add(spec);
    }

    public IGTScanner generate(long nRows) {
        validate();
        return new Generator(nRows);
    }

    private void validate() {
        if (info.getColumnCount() != colSpecs.size())
            throw new IllegalArgumentException();
        for (int i = 0; i < colSpecs.size(); i++) {
            ColSpec spec = colSpecs.get(i);
            if (info.codeSystem.maxCodeLength(i) < spec.length)
                throw new IllegalArgumentException();
        }
    }

    private class ColSpec {
        int length;
        long cardinality;
        Map<Integer, Integer> weights;
        long weightSum;
    }

    private class Generator implements IGTScanner {
        final long nRows;
        final Random rand;

        int counter;
        Distribution[] dist;
        GTRecord rec;

        public Generator(long nRows) {
            this.nRows = nRows;
            this.rand = new Random();

            rec = new GTRecord(info);
            dist = new Distribution[colSpecs.size()];
            for (int i = 0; i < colSpecs.size(); i++) {
                ColSpec spec = colSpecs.get(i);
                rec.set(i, new ByteArray(spec.length));
                dist[i] = new Distribution(spec, 0);
            }

        }

        @Override
        public Iterator<GTRecord> iterator() {
            return new Iterator<GTRecord>() {

                @Override
                public boolean hasNext() {
                    return counter < nRows;
                }

                @Override
                public GTRecord next() {
                    for (int i = 0; i < colSpecs.size(); i++) {
                        ColSpec spec = colSpecs.get(i);
                        // dimension case
                        if (spec.cardinality > 0) {
                            long v = dist[i].next();
                            if (v < 0) {
                                dist[i] = new Distribution(spec, parentLevelCount(i));
                                v = dist[i].next();
                            }
                            ByteArray bytes = rec.get(i);
                            assert bytes.length() == spec.length;
                            BytesUtil.writeLong(v, bytes.array(), bytes.offset(), bytes.length());
                        }
                        // measure case
                        else {
                            rand.nextBytes(rec.get(i).array());
                        }
                    }
                    counter++;
                    return rec;
                }

                private long parentLevelCount(int i) {
                    if (i == 0)
                        return nRows;
                    else
                        return dist[i - 1].leftRowsForCurValue + 1;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public GTInfo getInfo() {
            return info;
        }

        @Override
        public int getScannedRowCount() {
            return counter;
        }

    }

    private class Distribution {
        ColSpec spec;
        long nRows;
        long leftRows;
        long leftRowsForCurValue;
        int curValue;

        public Distribution(ColSpec spec, long nRows) {
            assert spec.cardinality > 0;

            this.spec = spec;
            this.nRows = nRows;
            this.leftRows = nRows;
            this.leftRowsForCurValue = 0;
            this.curValue = -1;

            if (spec.weightSum == 0) {
                spec.weightSum = spec.cardinality; // all value is weight 1 by default
                if (spec.weights != null) {
                    for (Entry<Integer, Integer> entry : spec.weights.entrySet()) {
                        spec.weightSum += entry.getValue() - 1;
                    }
                }
            }
        }

        private long weight(int v) {
            if (spec.weights != null && spec.weights.containsKey(v))
                return spec.weights.get(v);
            else
                return 1;
        }

        public long next() {
            if (leftRows == 0)
                return -1;

            if (leftRowsForCurValue == 0 && curValue < spec.cardinality - 1) {
                curValue++;
                if (curValue == spec.cardinality - 1)
                    leftRowsForCurValue = leftRows;
                else
                    leftRowsForCurValue = (long) (nRows * (double) weight(curValue) / (double) spec.weightSum);
            }

            leftRowsForCurValue = Math.max(leftRowsForCurValue - 1, 0);
            leftRows--;
            return curValue;
        }
    }

}

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

package org.apache.kylin.storage.hbase.cube.v1.coprocessor.observer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.storage.hbase.common.coprocessor.AggrKey;
import org.apache.kylin.storage.hbase.common.coprocessor.AggregationCache;

/**
 * @author yangli9
 */
@SuppressWarnings("rawtypes")
public class ObserverAggregationCache extends AggregationCache {

    private final ObserverAggregators aggregators;

    public ObserverAggregationCache(ObserverAggregators aggregators) {
        this.aggregators = aggregators;
    }

    public RegionScanner getScanner(RegionScanner innerScanner) {
        return new AggregationRegionScanner(innerScanner);
    }

    @Override
    public MeasureAggregator[] createBuffer() {
        return aggregators.createBuffer();
    }

    private class AggregationRegionScanner implements RegionScanner {

        private final RegionScanner innerScanner;
        private final Iterator<Entry<AggrKey, MeasureAggregator[]>> iterator;

        public AggregationRegionScanner(RegionScanner innerScanner) {
            this.innerScanner = innerScanner;
            this.iterator = aggBufMap.entrySet().iterator();
        }

        @Override
        public boolean next(List<Cell> results) throws IOException {
            try {
                // AggregateRegionObserver.LOG.info("Kylin Scanner next()");
                boolean hasMore = false;
                if (iterator.hasNext()) {
                    Entry<AggrKey, MeasureAggregator[]> entry = iterator.next();
                    makeCells(entry, results);
                    hasMore = iterator.hasNext();
                }
                // AggregateRegionObserver.LOG.info("Kylin Scanner next() done");

                return hasMore;
            } catch (Exception e) {
                throw new IOException("Error when calling next", e);
            }
        }

        private void makeCells(Entry<AggrKey, MeasureAggregator[]> entry, List<Cell> results) {
            byte[][] families = aggregators.getHColFamilies();
            byte[][] qualifiers = aggregators.getHColQualifiers();
            int nHCols = aggregators.getHColsNum();

            AggrKey rowKey = entry.getKey();
            MeasureAggregator[] aggBuf = entry.getValue();
            ByteBuffer[] rowValues = aggregators.getHColValues(aggBuf);

            if (nHCols == 0) {
                Cell keyValue = new KeyValue(rowKey.get(), rowKey.offset(), rowKey.length(), //
                        null, 0, 0, //
                        null, 0, 0, //
                        HConstants.LATEST_TIMESTAMP, Type.Put, //
                        null, 0, 0);
                results.add(keyValue);
            } else {
                for (int i = 0; i < nHCols; i++) {
                    Cell keyValue = new KeyValue(rowKey.get(), rowKey.offset(), rowKey.length(), //
                            families[i], 0, families[i].length, //
                            qualifiers[i], 0, qualifiers[i].length, //
                            HConstants.LATEST_TIMESTAMP, Type.Put, //
                            rowValues[i].array(), 0, rowValues[i].position());
                    results.add(keyValue);
                }
            }
        }

        @Override
        public boolean next(List<Cell> result, ScannerContext scannerContext) throws IOException {
            return next(result);
        }

        @Override
        public boolean nextRaw(List<Cell> result) throws IOException {
            return next(result);
        }

        @Override
        public boolean nextRaw(List<Cell> result, ScannerContext scannerContext) throws IOException {
            return next(result);
        }

        @Override
        public void close() throws IOException {
            // AggregateRegionObserver.LOG.info("Kylin Scanner close()");
            innerScanner.close();
            // AggregateRegionObserver.LOG.info("Kylin Scanner close() done");
        }

        @Override
        public HRegionInfo getRegionInfo() {
            // AggregateRegionObserver.LOG.info("Kylin Scanner getRegionInfo()");
            return innerScanner.getRegionInfo();
        }

        @Override
        public long getMaxResultSize() {
            // AggregateRegionObserver.LOG.info("Kylin Scanner getMaxResultSize()");
            return Long.MAX_VALUE;
        }

        @Override
        public boolean isFilterDone() throws IOException {
            // AggregateRegionObserver.LOG.info("Kylin Scanner isFilterDone()");
            return false;
        }

        @Override
        public boolean reseek(byte[] row) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getMvccReadPoint() {
            // AggregateRegionObserver.LOG.info("Kylin Scanner getMvccReadPoint()");
            return Long.MAX_VALUE;
        }

        @Override
        public int getBatch() {
            return innerScanner.getBatch();
        }
    }

}

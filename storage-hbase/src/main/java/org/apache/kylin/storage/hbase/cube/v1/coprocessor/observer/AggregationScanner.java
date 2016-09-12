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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.kylin.gridtable.StorageSideBehavior;
import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.storage.hbase.common.coprocessor.AggrKey;
import org.apache.kylin.storage.hbase.common.coprocessor.CoprocessorFilter;
import org.apache.kylin.storage.hbase.common.coprocessor.CoprocessorProjector;
import org.apache.kylin.storage.hbase.common.coprocessor.CoprocessorRowType;

/**
 * @author yangli9
 * 
 */
public class AggregationScanner implements RegionScanner {

    private RegionScanner outerScanner;
    private StorageSideBehavior behavior;

    public AggregationScanner(CoprocessorRowType type, CoprocessorFilter filter, CoprocessorProjector groupBy, ObserverAggregators aggrs, RegionScanner innerScanner, StorageSideBehavior behavior) throws IOException {

        AggregateRegionObserver.LOG.info("Kylin Coprocessor start");

        this.behavior = behavior;

        ObserverAggregationCache aggCache;
        Stats stats = new Stats();

        aggCache = buildAggrCache(innerScanner, type, groupBy, aggrs, filter, stats);
        stats.countOutputRow(aggCache.getSize());
        this.outerScanner = aggCache.getScanner(innerScanner);

        AggregateRegionObserver.LOG.info("Kylin Coprocessor aggregation done: " + stats);
    }

    @SuppressWarnings("rawtypes")
    ObserverAggregationCache buildAggrCache(final RegionScanner innerScanner, CoprocessorRowType type, CoprocessorProjector projector, ObserverAggregators aggregators, CoprocessorFilter filter, Stats stats) throws IOException {

        ObserverAggregationCache aggCache = new ObserverAggregationCache(aggregators);

        ObserverTuple tuple = new ObserverTuple(type);
        boolean hasMore = true;
        List<Cell> results = new ArrayList<Cell>();
        byte meaninglessByte = 0;

        while (hasMore) {
            results.clear();
            hasMore = innerScanner.nextRaw(results);
            if (results.isEmpty())
                continue;

            if (stats != null)
                stats.countInputRow(results);

            Cell cell = results.get(0);
            tuple.setUnderlying(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());

            if (behavior == StorageSideBehavior.SCAN) {
                //touch every byte of the cell so that the cost of scanning will be trully reflected
                int endIndex = cell.getRowOffset() + cell.getRowLength();
                for (int i = cell.getRowOffset(); i < endIndex; ++i) {
                    meaninglessByte += cell.getRowArray()[i];
                }
            } else {
                if (behavior.filterToggledOn()) {
                    if (filter != null && filter.evaluate(tuple) == false)
                        continue;

                    if (behavior.aggrToggledOn()) {
                        AggrKey aggKey = projector.getAggrKey(results);
                        MeasureAggregator[] bufs = aggCache.getBuffer(aggKey);
                        aggregators.aggregate(bufs, results);

                        if (behavior.ordinal() >= StorageSideBehavior.SCAN_FILTER_AGGR_CHECKMEM.ordinal()) {
                            aggCache.checkMemoryUsage();
                        }
                    }
                }
            }
        }

        if (behavior == StorageSideBehavior.SCAN) {
            System.out.println("meaningless byte is now " + meaninglessByte);
        }

        return aggCache;
    }

    @Override
    public boolean next(List<Cell> results) throws IOException {
        return outerScanner.next(results);
    }

    @Override
    public boolean next(List<Cell> result, int limit) throws IOException {
        return outerScanner.next(result, limit);
    }

    @Override
    public boolean nextRaw(List<Cell> result) throws IOException {
        return outerScanner.nextRaw(result);
    }

    @Override
    public boolean nextRaw(List<Cell> result, int limit) throws IOException {
        return outerScanner.nextRaw(result, limit);
    }

    @Override
    public void close() throws IOException {
        outerScanner.close();
    }

    @Override
    public HRegionInfo getRegionInfo() {
        return outerScanner.getRegionInfo();
    }

    @Override
    public boolean isFilterDone() throws IOException {
        return outerScanner.isFilterDone();
    }

    @Override
    public boolean reseek(byte[] row) throws IOException {
        return outerScanner.reseek(row);
    }

    @Override
    public long getMaxResultSize() {
        return outerScanner.getMaxResultSize();
    }

    @Override
    public long getMvccReadPoint() {
        return outerScanner.getMvccReadPoint();
    }

    private static class Stats {
        long inputRows = 0;
        long inputBytes = 0;
        long outputRows = 0;

        // have no outputBytes because that requires actual serialize all the
        // aggregator buffers

        public void countInputRow(List<Cell> row) {
            inputRows++;
            inputBytes += row.get(0).getRowLength();
            for (int i = 0, n = row.size(); i < n; i++) {
                inputBytes += row.get(i).getValueLength();
            }
        }

        public void countOutputRow(long rowCount) {
            outputRows += rowCount;
        }

        public String toString() {
            double percent = (double) outputRows / inputRows * 100;
            return Math.round(percent) + "% = " + outputRows + " (out rows) / " + inputRows + " (in rows); in bytes = " + inputBytes + "; est. out bytes = " + Math.round(inputBytes * percent / 100);
        }
    }
}

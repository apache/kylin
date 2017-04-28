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

package org.apache.kylin.cube.inmemcubing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.common.util.MemoryBudgetController;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRequestBuilder;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.measure.MeasureAggregators;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * When base cuboid does not fit in memory, cut the input into multiple splits and merge the split outputs at last.
 */
public class DoggedCubeBuilder extends AbstractInMemCubeBuilder {

    private static Logger logger = LoggerFactory.getLogger(DoggedCubeBuilder.class);

    private int splitRowThreshold = Integer.MAX_VALUE;
    private int unitRows = 1000;

    public DoggedCubeBuilder(CubeDesc cubeDesc, IJoinedFlatTableDesc flatDesc, Map<TblColRef, Dictionary<String>> dictionaryMap) {
        super(cubeDesc, flatDesc, dictionaryMap);

        // check memory more often if a single row is big
        if (cubeDesc.hasMemoryHungryMeasures())
            unitRows /= 10;
    }

    public void setSplitRowThreshold(int rowThreshold) {
        this.splitRowThreshold = rowThreshold;
        this.unitRows = Math.min(unitRows, rowThreshold);
    }

    @Override
    public void build(BlockingQueue<List<String>> input, ICuboidWriter output) throws IOException {
        new BuildOnce().build(input, output);
    }

    private class BuildOnce {

        BuildOnce() {
        }

        public void build(BlockingQueue<List<String>> input, ICuboidWriter output) throws IOException {
            final List<SplitThread> splits = new ArrayList<SplitThread>();
            final Merger merger = new Merger();

            long start = System.currentTimeMillis();
            logger.info("Dogged Cube Build start");

            try {
                SplitThread last = null;
                boolean eof = false;

                while (!eof) {

                    if (last != null && shouldCutSplit(splits)) {
                        cutSplit(last);
                        last = null;
                    }

                    checkException(splits);

                    if (last == null) {
                        last = new SplitThread();
                        splits.add(last);
                        last.start();
                        logger.info("Split #" + splits.size() + " kickoff");
                    }

                    eof = feedSomeInput(input, last, unitRows);
                }

                for (SplitThread split : splits) {
                    split.join();
                }
                checkException(splits);
                logger.info("Dogged Cube Build splits complete, took " + (System.currentTimeMillis() - start) + " ms");

                merger.mergeAndOutput(splits, output);

            } catch (Throwable e) {
                logger.error("Dogged Cube Build error", e);
                if (e instanceof Error)
                    throw (Error) e;
                else if (e instanceof RuntimeException)
                    throw (RuntimeException) e;
                else
                    throw new IOException(e);
            } finally {
                output.close();
                closeGirdTables(splits);
                logger.info("Dogged Cube Build end, totally took " + (System.currentTimeMillis() - start) + " ms");
                ensureExit(splits);
                logger.info("Dogged Cube Build return");
            }
        }

        private void closeGirdTables(List<SplitThread> splits) {
            for (SplitThread split : splits) {
                if (split.buildResult != null) {
                    for (CuboidResult r : split.buildResult.values()) {
                        try {
                            r.table.close();
                        } catch (Throwable e) {
                            logger.error("Error closing grid table " + r.table, e);
                        }
                    }
                }
            }
        }

        private void ensureExit(List<SplitThread> splits) throws IOException {
            try {
                for (int i = 0; i < splits.size(); i++) {
                    SplitThread split = splits.get(i);
                    if (split.isAlive()) {
                        abort(splits);
                    }
                }
            } catch (Throwable e) {
                logger.error("Dogged Cube Build error", e);
            }
        }

        private void checkException(List<SplitThread> splits) throws IOException {
            for (int i = 0; i < splits.size(); i++) {
                SplitThread split = splits.get(i);
                if (split.exception != null)
                    abort(splits);
            }
        }

        private void abort(List<SplitThread> splits) throws IOException {
            for (SplitThread split : splits) {
                split.builder.abort();
            }

            ArrayList<Throwable> errors = new ArrayList<Throwable>();
            for (SplitThread split : splits) {
                try {
                    split.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    errors.add(e);
                }
                if (split.exception != null)
                    errors.add(split.exception);
            }

            if (errors.isEmpty()) {
                return;
            } else if (errors.size() == 1) {
                Throwable t = errors.get(0);
                if (t instanceof IOException)
                    throw (IOException) t;
                else
                    throw new IOException(t);
            } else {
                for (Throwable t : errors)
                    logger.error("Exception during in-mem cube build", t);
                throw new IOException(errors.size() + " exceptions during in-mem cube build, cause set to the first, check log for more", errors.get(0));
            }
        }

        private boolean feedSomeInput(BlockingQueue<List<String>> input, SplitThread split, int n) {
            try {
                int i = 0;
                while (i < n) {
                    List<String> record = input.take();
                    i++;

                    while (split.inputQueue.offer(record, 1, TimeUnit.SECONDS) == false) {
                        if (split.exception != null)
                            return true; // got some error
                    }
                    split.inputRowCount++;

                    if (record == null || record.isEmpty()) {
                        return true;
                    }
                }
                return false;

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        private void cutSplit(SplitThread last) {
            try {
                // signal the end of input
                while (last.isAlive()) {
                    if (last.inputQueue.offer(Collections.<String> emptyList())) {
                        break;
                    }
                    Thread.sleep(1000);
                }

                // wait cuboid build done
                last.join();

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        private boolean shouldCutSplit(List<SplitThread> splits) {
            int systemAvailMB = MemoryBudgetController.getSystemAvailMB();
            int nSplit = splits.size();
            long splitRowCount = nSplit == 0 ? 0 : splits.get(nSplit - 1).inputRowCount;

            logger.info(splitRowCount + " records went into split #" + nSplit + "; " + systemAvailMB + " MB left, " + reserveMemoryMB + " MB threshold");

            if (splitRowCount >= splitRowThreshold) {
                logger.info("Split cut due to hitting splitRowThreshold " + splitRowThreshold);
                return true;
            }

            if (systemAvailMB <= reserveMemoryMB * 1.5) {
                logger.info("Split cut due to hitting memory threshold, system avail " + systemAvailMB + " MB <= reserve " + reserveMemoryMB + "*1.5 MB");
                return true;
            }

            return false;
        }
    }

    private class SplitThread extends Thread {
        final BlockingQueue<List<String>> inputQueue = new ArrayBlockingQueue<List<String>>(16);
        final InMemCubeBuilder builder;

        ConcurrentNavigableMap<Long, CuboidResult> buildResult;
        long inputRowCount = 0;
        RuntimeException exception;

        public SplitThread() {
            this.builder = new InMemCubeBuilder(cubeDesc, flatDesc, dictionaryMap);
            this.builder.setConcurrentThreads(taskThreadCount);
            this.builder.setReserveMemoryMB(reserveMemoryMB);
        }

        @Override
        public void run() {
            try {
                buildResult = builder.build(inputQueue);
            } catch (Exception e) {
                if (e instanceof RuntimeException)
                    this.exception = (RuntimeException) e;
                else
                    this.exception = new RuntimeException(e);
            }
        }
    }

    private class Merger {

        MeasureAggregators reuseAggrs;
        Object[] reuseMetricsArray;
        ByteArray reuseMetricsSpace;

        long lastCuboidColumnCount;
        ImmutableBitSet lastMetricsColumns;

        Merger() {
            reuseAggrs = new MeasureAggregators(cubeDesc.getMeasures());
            reuseMetricsArray = new Object[cubeDesc.getMeasures().size()];
        }

        public void mergeAndOutput(List<SplitThread> splits, ICuboidWriter output) throws IOException {
            if (splits.size() == 1) {
                for (CuboidResult cuboidResult : splits.get(0).buildResult.values()) {
                    outputCuboid(cuboidResult.cuboidId, cuboidResult.table, output);
                    cuboidResult.table.close();
                }
                return;
            }

            LinkedList<MergeSlot> open = Lists.newLinkedList();
            for (SplitThread split : splits) {
                open.add(new MergeSlot(split));
            }

            PriorityQueue<MergeSlot> heap = new PriorityQueue<MergeSlot>();

            while (true) {
                // ready records in open slots and add to heap
                while (!open.isEmpty()) {
                    MergeSlot slot = open.removeFirst();
                    if (slot.fetchNext()) {
                        heap.add(slot);
                    }
                }

                // find the smallest on heap
                MergeSlot smallest = heap.poll();
                if (smallest == null)
                    break;
                open.add(smallest);

                // merge with slots having the same key
                if (smallest.isSameKey(heap.peek())) {
                    Object[] metrics = getMetricsValues(smallest.currentRecord);
                    reuseAggrs.reset();
                    reuseAggrs.aggregate(metrics);
                    do {
                        MergeSlot slot = heap.poll();
                        open.add(slot);
                        metrics = getMetricsValues(slot.currentRecord);
                        reuseAggrs.aggregate(metrics);
                    } while (smallest.isSameKey(heap.peek()));

                    reuseAggrs.collectStates(metrics);
                    setMetricsValues(smallest.currentRecord, metrics);
                }

                output.write(smallest.currentCuboidId, smallest.currentRecord);
            }
        }

        private void setMetricsValues(GTRecord record, Object[] metricsValues) {
            ImmutableBitSet metrics = getMetricsColumns(record);

            if (reuseMetricsSpace == null) {
                reuseMetricsSpace = new ByteArray(record.getInfo().getMaxColumnLength(metrics));
            }

            record.setValues(metrics, reuseMetricsSpace, metricsValues);
        }

        private Object[] getMetricsValues(GTRecord record) {
            ImmutableBitSet metrics = getMetricsColumns(record);
            return record.getValues(metrics, reuseMetricsArray);
        }

        private ImmutableBitSet getMetricsColumns(GTRecord record) {
            // metrics columns always come after dimension columns
            if (lastCuboidColumnCount == record.getInfo().getColumnCount())
                return lastMetricsColumns;

            int to = record.getInfo().getColumnCount();
            int from = to - reuseMetricsArray.length;
            lastCuboidColumnCount = record.getInfo().getColumnCount();
            lastMetricsColumns = new ImmutableBitSet(from, to);
            return lastMetricsColumns;
        }
    }

    private static class MergeSlot implements Comparable<MergeSlot> {

        final Iterator<CuboidResult> cuboidIterator;
        IGTScanner scanner;
        Iterator<GTRecord> recordIterator;

        long currentCuboidId;
        GTRecord currentRecord;

        public MergeSlot(SplitThread split) {
            cuboidIterator = split.buildResult.values().iterator();
        }

        public boolean fetchNext() throws IOException {
            if (recordIterator == null) {
                if (cuboidIterator.hasNext()) {
                    CuboidResult cuboid = cuboidIterator.next();
                    currentCuboidId = cuboid.cuboidId;
                    scanner = cuboid.table.scan(new GTScanRequestBuilder().setInfo(cuboid.table.getInfo()).setRanges(null).setDimensions(null).setFilterPushDown(null).createGTScanRequest());
                    recordIterator = scanner.iterator();
                } else {
                    return false;
                }
            }

            if (recordIterator.hasNext()) {
                currentRecord = recordIterator.next();
                return true;
            } else {
                scanner.close();
                recordIterator = null;
                return fetchNext();
            }
        }

        @Override
        public int compareTo(MergeSlot o) {
            long cuboidComp = this.currentCuboidId - o.currentCuboidId;
            if (cuboidComp != 0)
                return cuboidComp < 0 ? -1 : 1;

            // note GTRecord.equals() don't work because the two GTRecord comes from different GridTable
            ImmutableBitSet pk = this.currentRecord.getInfo().getPrimaryKey();
            for (int i = 0; i < pk.trueBitCount(); i++) {
                int c = pk.trueBitAt(i);
                int comp = this.currentRecord.get(c).compareTo(o.currentRecord.get(c));
                if (comp != 0)
                    return comp;
            }
            return 0;
        }

        public boolean isSameKey(MergeSlot o) {
            if (o == null)
                return false;
            else
                return this.compareTo(o) == 0;
        }

    };
}

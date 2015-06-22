/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements. See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.kylin.job.inmemcubing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.metadata.measure.MeasureAggregators;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.cube.CuboidToGridTableMapping;
import org.apache.kylin.storage.gridtable.GTRecord;
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

    public DoggedCubeBuilder(CubeDesc cubeDesc, Map<TblColRef, Dictionary<?>> dictionaryMap) {
        super(cubeDesc, dictionaryMap);
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

        final List<SplitThread> splits = new ArrayList<SplitThread>();
        final Merger merger = new Merger();

        public void build(BlockingQueue<List<String>> input, ICuboidWriter output) throws IOException {
            SplitThread last = null;
            boolean eof = false;

            while (!eof) {

                if (last != null && shouldCutSplit()) {
                    cutSplit(last);
                    last = null;
                }

                checkException(splits);

                if (last == null) {
                    last = new SplitThread(merger);
                    splits.add(last);
                    last.start();
                }

                eof = feedSomeInput(input, last, unitRows);
            }

            merger.mergeAndOutput(splits, output);

            checkException(splits);
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

                // wait cuboid build done (but still pending output)
                while (last.isAlive()) {
                    if (last.builder.isAllCuboidDone()) {
                        break;
                    }
                    Thread.sleep(1000);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        private boolean shouldCutSplit() {
            int systemAvailMB = MemoryBudgetController.getSystemAvailMB();
            int nSplit = splits.size();
            long splitRowCount = nSplit == 0 ? 0 : splits.get(nSplit - 1).inputRowCount;
            
            logger.debug(splitRowCount + " records went into split #" + nSplit + "; " + systemAvailMB + " MB left, " + reserveMemoryMB + " MB threshold");
            
            return splitRowCount >= splitRowThreshold || systemAvailMB <= reserveMemoryMB;
        }
    }

    private class SplitThread extends Thread {
        final BlockingQueue<List<String>> inputQueue = new ArrayBlockingQueue<List<String>>(16);
        final InMemCubeBuilder builder;
        final MergeSlot output;

        long inputRowCount = 0;
        RuntimeException exception;

        public SplitThread(Merger merger) {
            this.builder = new InMemCubeBuilder(cubeDesc, dictionaryMap);
            this.builder.setConcurrentThreads(taskThreadCount);
            this.builder.setOutputOrder(true); // merge sort requires order
            this.builder.setReserveMemoryMB(reserveMemoryMB);

            this.output = merger.newMergeSlot(this);
        }

        @Override
        public void run() {
            try {
                builder.build(inputQueue, output);
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
            MeasureDesc[] measures = CuboidToGridTableMapping.getMeasureSequenceOnGridTable(cubeDesc);
            reuseAggrs = new MeasureAggregators(measures);
            reuseMetricsArray = new Object[measures.length];
        }

        public MergeSlot newMergeSlot(SplitThread split) {
            return new MergeSlot(split);
        }

        public void mergeAndOutput(List<SplitThread> splits, ICuboidWriter output) throws IOException {
            LinkedList<MergeSlot> open = Lists.newLinkedList();
            for (SplitThread split : splits)
                open.add(split.output);

            if (splits.size() == 1) {
                splits.get(0).output.directOutput = output;
            }

            try {
                PriorityQueue<MergeSlot> heap = new PriorityQueue<MergeSlot>();
                boolean hasMore = true;

                while (hasMore) {
                    takeRecordsFromAllOpenSlots(open, heap);
                    hasMore = mergeAndOutputOneRecord(heap, open, output);
                }

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        private void takeRecordsFromAllOpenSlots(LinkedList<MergeSlot> open, PriorityQueue<MergeSlot> heap) throws InterruptedException {
            while (!open.isEmpty()) {
                MergeSlot slot = open.getFirst();
                // ready one record in the slot
                if (slot.readySignal.poll(1, TimeUnit.SECONDS) != null) {
                    open.removeFirst();
                    heap.add(slot);
                } else if (slot.isClosed()) {
                    open.removeFirst();
                }
            }
            return;
        }

        private boolean mergeAndOutputOneRecord(PriorityQueue<MergeSlot> heap, LinkedList<MergeSlot> open, ICuboidWriter output) throws IOException, InterruptedException {
            MergeSlot smallest = heap.poll();
            if (smallest == null)
                return false;
            open.add(smallest);

            if (smallest.isSameKey(heap.peek())) {
                Object[] metrics = getMetricsValues(smallest.record);
                reuseAggrs.reset();
                reuseAggrs.aggregate(metrics);
                do {
                    MergeSlot slot = heap.poll();
                    open.add(slot);
                    metrics = getMetricsValues(slot.record);
                    reuseAggrs.aggregate(metrics);
                } while (smallest.isSameKey(heap.peek()));
                
                reuseAggrs.collectStates(metrics);
                setMetricsValues(smallest.record, metrics);
            }

            output.write(smallest.cuboidId, smallest.record);

            for (MergeSlot slot : open) {
                slot.consumedSignal.put(this);
            }
            return true;
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

    private static class MergeSlot implements ICuboidWriter, Comparable<MergeSlot> {

        final SplitThread split;
        final BlockingQueue<Object> readySignal = new ArrayBlockingQueue<Object>(1);
        final BlockingQueue<Object> consumedSignal = new ArrayBlockingQueue<Object>(1);

        ICuboidWriter directOutput = null;
        long cuboidId;
        GTRecord record;

        public MergeSlot(SplitThread split) {
            this.split = split;
        }

        @Override
        public void write(long cuboidId, GTRecord record) throws IOException {
            // when only one split left
            if (directOutput != null) {
                directOutput.write(cuboidId, record);
                return;
            }

            this.cuboidId = cuboidId;
            this.record = record;

            try {
                // signal record is ready
                readySignal.put(this);

                // wait record be consumed
                consumedSignal.take();

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        public boolean isClosed() {
            return split.isAlive() == false;
        }

        @Override
        public int compareTo(MergeSlot o) {
            long cuboidComp = this.cuboidId - o.cuboidId;
            if (cuboidComp != 0)
                return cuboidComp < 0 ? -1 : 1;
            else
                return this.record.compareTo(o.record);
        }

        public boolean isSameKey(MergeSlot o) {
            if (o == null)
                return false;

            if (this.cuboidId != o.cuboidId)
                return false;

            // note GTRecord.equals() don't work because the two GTRecord comes from different GridTable
            ImmutableBitSet pk = this.record.getInfo().getPrimaryKey();
            for (int i = 0; i < pk.trueBitCount(); i++) {
                int c = pk.trueBitAt(i);
                if (this.record.get(c).equals(o.record.get(c)) == false)
                    return false;
            }
            return true;
        }

    };
}

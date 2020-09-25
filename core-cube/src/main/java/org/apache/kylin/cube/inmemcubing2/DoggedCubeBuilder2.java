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

package org.apache.kylin.cube.inmemcubing2;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.RecursiveTask;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.cube.inmemcubing.AbstractInMemCubeBuilder;
import org.apache.kylin.cube.inmemcubing.CuboidResult;
import org.apache.kylin.cube.inmemcubing.ICuboidWriter;
import org.apache.kylin.cube.inmemcubing.InputConverterUnit;
import org.apache.kylin.cube.inmemcubing.RecordConsumeBlockingQueueController;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRequestBuilder;
import org.apache.kylin.gridtable.GridTable;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.measure.MeasureAggregators;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.base.Stopwatch;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.shaded.com.google.common.collect.Queues;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class DoggedCubeBuilder2 extends AbstractInMemCubeBuilder {
    private static Logger logger = LoggerFactory.getLogger(DoggedCubeBuilder2.class);

    public DoggedCubeBuilder2(CuboidScheduler cuboidScheduler, IJoinedFlatTableDesc flatDesc,
            Map<TblColRef, Dictionary<String>> dictionaryMap) {
        super(cuboidScheduler, flatDesc, dictionaryMap);
    }

    @Override
    public <T> void build(BlockingQueue<T> input, InputConverterUnit<T> inputConverterUnit, ICuboidWriter output)
            throws IOException {
        new BuildOnce().build(input, inputConverterUnit, output);
    }

    private class BuildOnce {
        public <T> void build(BlockingQueue<T> input, InputConverterUnit<T> inputConverterUnit, ICuboidWriter output)
                throws IOException {
            final RecordConsumeBlockingQueueController<T> inputController = RecordConsumeBlockingQueueController
                    .getQueueController(inputConverterUnit, input);

            final List<InMemCubeBuilder2> builderList = new CopyOnWriteArrayList<>();

            ForkJoinWorkerThreadFactory factory = new ForkJoinWorkerThreadFactory() {
                @Override
                public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
                    final ForkJoinWorkerThread worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
                    worker.setName("dogged-cubing-cuboid-worker-" + worker.getPoolIndex());
                    return worker;
                }
            };

            ForkJoinPool builderPool = new ForkJoinPool(taskThreadCount, factory, null, true);
            CuboidResultWatcher resultWatcher = new CuboidResultWatcher(builderList, output);

            Stopwatch sw = Stopwatch.createUnstarted();
            sw.start();
            logger.info("Dogged Cube Build2 start");
            try {
                BaseCuboidTask<T> task = new BaseCuboidTask<>(inputController, 1, resultWatcher);
                builderPool.execute(task);
                do {
                    builderList.add(task.getInternalBuilder());
                    //Exception will be thrown here if cube building failure
                    task.join();
                    task = task.nextTask();
                } while (task != null);

                logger.info("Has finished feeding data, and base cuboid built, start to build child cuboids");
                for (final InMemCubeBuilder2 builder : builderList) {
                    builderPool.submit(new Runnable() {
                        @Override
                        public void run() {
                            builder.startBuildFromBaseCuboid();
                        }
                    });
                }
                resultWatcher.start();
                logger.info("Dogged Cube Build2 splits complete, took " + sw.elapsed(MILLISECONDS) + " ms");
            } catch (Throwable e) {
                logger.error("Dogged Cube Build2 error", e);
                if (e instanceof Error)
                    throw (Error) e;
                else if (e instanceof RuntimeException)
                    throw (RuntimeException) e;
                else
                    throw new IOException(e);
            } finally {
                output.close();
                closeGirdTables(builderList);
                sw.stop();
                builderPool.shutdownNow();
                logger.info("Dogged Cube Build2 end, totally took " + sw.elapsed(MILLISECONDS) + " ms");
                logger.info("Dogged Cube Build2 return");
            }
        }

        private void closeGirdTables(List<InMemCubeBuilder2> builderList) {
            for (InMemCubeBuilder2 inMemCubeBuilder : builderList) {
                for (CuboidResult cuboidResult : inMemCubeBuilder.getResultCollector().getAllResult().values()) {
                    closeGirdTable(cuboidResult.table);
                }
            }
        }

        private void closeGirdTable(GridTable gridTable) {
            try {
                gridTable.close();
            } catch (Throwable e) {
                logger.error("Error closing grid table " + gridTable, e);
            }
        }
    }

    private class BaseCuboidTask<T> extends RecursiveTask<CuboidResult> {
        private static final long serialVersionUID = -5408592502260876799L;

        private final int splitSeq;
        private final ICuboidResultListener resultListener;

        private RecordConsumeBlockingQueueController<T> inputController;
        private InMemCubeBuilder2 builder;

        private volatile BaseCuboidTask<T> next;

        public BaseCuboidTask(final RecordConsumeBlockingQueueController<T> inputController, int splitSeq,
                ICuboidResultListener resultListener) {
            this.inputController = inputController;
            this.splitSeq = splitSeq;
            this.resultListener = resultListener;
            this.builder = new InMemCubeBuilder2(cuboidScheduler, flatDesc, dictionaryMap);
            builder.setReserveMemoryMB(reserveMemoryMB);
            builder.setConcurrentThreads(taskThreadCount);
            logger.info("Split #" + splitSeq + " kickoff");
        }

        @Override
        protected CuboidResult compute() {
            try {
                CuboidResult baseCuboidResult = builder.buildBaseCuboid(inputController, resultListener);
                if (!inputController.ifEnd()) {
                    next = new BaseCuboidTask<>(inputController, splitSeq + 1, resultListener);
                    next.fork();
                }
                logger.info("Split #" + splitSeq + " finished");
                return baseCuboidResult;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public InMemCubeBuilder2 getInternalBuilder() {
            return builder;
        }

        public BaseCuboidTask<T> nextTask() {
            return next;
        }
    }

    /**
     * Class response for watch the cube building result, monitor the cube building process and trigger merge actions if required.
     *
     */
    private class CuboidResultWatcher implements ICuboidResultListener {
        final BlockingQueue<CuboidResult> outputQueue;
        final Map<Long, List<CuboidResult>> pendingQueue = Maps.newHashMap();
        final List<InMemCubeBuilder2> builderList;
        final ICuboidWriter output;

        public CuboidResultWatcher(final List<InMemCubeBuilder2> builderList, final ICuboidWriter output) {
            this.outputQueue = Queues.newLinkedBlockingQueue();
            this.builderList = builderList;
            this.output = output;
        }

        public void start() throws IOException {
            SplitMerger merger = new SplitMerger();
            while (true) {
                if (!outputQueue.isEmpty()) {
                    List<CuboidResult> splitResultReturned = Lists.newArrayList();
                    outputQueue.drainTo(splitResultReturned);
                    for (CuboidResult splitResult : splitResultReturned) {
                        if (builderList.size() == 1) {
                            merger.mergeAndOutput(Lists.newArrayList(splitResult), output);
                        } else {
                            List<CuboidResult> cuboidResultList = pendingQueue.get(splitResult.cuboidId);
                            if (cuboidResultList == null) {
                                cuboidResultList = Lists.newArrayListWithExpectedSize(builderList.size());
                                cuboidResultList.add(splitResult);
                                pendingQueue.put(splitResult.cuboidId, cuboidResultList);
                            } else {
                                cuboidResultList.add(splitResult);
                            }
                            if (cuboidResultList.size() == builderList.size()) {
                                merger.mergeAndOutput(cuboidResultList, output);
                                pendingQueue.remove(splitResult.cuboidId);
                            }
                        }
                    }
                }

                boolean jobFinished = isAllBuildFinished();
                if (outputQueue.isEmpty() && !jobFinished) {
                    boolean ifWait = true;
                    for (InMemCubeBuilder2 builder : builderList) {
                        Queue<CuboidTask> queue = builder.getCompletedTaskQueue();
                        while (queue.size() > 0) {
                            CuboidTask childTask = queue.poll();
                            if (childTask.isCompletedAbnormally()) {
                                throw new RuntimeException(childTask.getException());
                            }
                            ifWait = false;
                        }
                    }
                    if (ifWait) {
                        try {
                            Thread.sleep(100L);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                } else if (outputQueue.isEmpty() && pendingQueue.isEmpty() && jobFinished) {
                    return;
                }
            }
        }

        private boolean isAllBuildFinished() {
            for (InMemCubeBuilder2 split : builderList) {
                if (!split.isAllCuboidDone()) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public void finish(CuboidResult result) {
            Stopwatch stopwatch = Stopwatch.createUnstarted().start();
            int nRetries = 0;
            while (!outputQueue.offer(result)) {
                nRetries++;
                long sleepTime = stopwatch.elapsed(MILLISECONDS);
                if (sleepTime > 3600000L) {
                    stopwatch.stop();
                    throw new RuntimeException(
                            "OutputQueue Full. Cannot offer to the output queue after waiting for one hour!!! Current queue size: "
                                    + outputQueue.size());
                }
                logger.warn("OutputQueue Full. Queue size: " + outputQueue.size() + ". Total sleep time : " + sleepTime
                        + ", and retry count : " + nRetries);
                try {
                    Thread.sleep(5000L);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            stopwatch.stop();
        }
    }

    private class SplitMerger {
        MeasureAggregators reuseAggrs;
        Object[] reuseMetricsArray;
        ByteArray reuseMetricsSpace;

        long lastCuboidColumnCount;
        ImmutableBitSet lastMetricsColumns;

        SplitMerger() {
            reuseAggrs = new MeasureAggregators(cubeDesc.getMeasures());
            reuseMetricsArray = new Object[cubeDesc.getMeasures().size()];
        }

        public void mergeAndOutput(List<CuboidResult> splitResultList, ICuboidWriter output) throws IOException {
            if (splitResultList.size() == 1) {
                CuboidResult cuboidResult = splitResultList.get(0);
                outputCuboid(cuboidResult.cuboidId, cuboidResult.table, output);
                return;
            }
            LinkedList<ResultMergeSlot> open = Lists.newLinkedList();
            for (CuboidResult splitResult : splitResultList) {
                open.add(new ResultMergeSlot(splitResult));
            }

            PriorityQueue<ResultMergeSlot> heap = new PriorityQueue<ResultMergeSlot>();
            while (true) {
                // ready records in open slots and add to heap
                while (!open.isEmpty()) {
                    ResultMergeSlot slot = open.removeFirst();
                    if (slot.fetchNext()) {
                        heap.add(slot);
                    }
                }

                // find the smallest on heap
                ResultMergeSlot smallest = heap.poll();
                if (smallest == null)
                    break;
                open.add(smallest);

                // merge with slots having the same key
                if (smallest.isSameKey(heap.peek())) {
                    Object[] metrics = getMetricsValues(smallest.currentRecord);
                    reuseAggrs.reset();
                    reuseAggrs.aggregate(metrics);
                    do {
                        ResultMergeSlot slot = heap.poll();
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

    private static class ResultMergeSlot implements Comparable<ResultMergeSlot> {
        CuboidResult splitResult;
        IGTScanner scanner;
        Iterator<GTRecord> recordIterator;

        long currentCuboidId;
        GTRecord currentRecord;

        public ResultMergeSlot(CuboidResult splitResult) {
            this.splitResult = splitResult;
        }

        public boolean fetchNext() throws IOException {
            if (recordIterator == null) {
                currentCuboidId = splitResult.cuboidId;
                scanner = splitResult.table.scan(new GTScanRequestBuilder().setInfo(splitResult.table.getInfo())
                        .setRanges(null).setDimensions(null).setFilterPushDown(null).createGTScanRequest());
                recordIterator = scanner.iterator();
            }

            if (recordIterator.hasNext()) {
                currentRecord = recordIterator.next();
                return true;
            } else {
                scanner.close();
                recordIterator = null;
                return false;
            }
        }

        @Override
        public int compareTo(ResultMergeSlot o) {
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

        public boolean isSameKey(ResultMergeSlot o) {
            if (o == null)
                return false;
            else
                return this.compareTo(o) == 0;
        }

    };
}
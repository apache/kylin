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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.common.util.MemoryBudgetController;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.MemoryBudgetController.MemoryWaterLevel;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.cube.gridtable.CubeGridTable;
import org.apache.kylin.cube.kv.CubeDimEncMap;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.gridtable.GTAggregateScanner;
import org.apache.kylin.gridtable.GTBuilder;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.GTScanRequestBuilder;
import org.apache.kylin.gridtable.GridTable;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.measure.topn.Counter;
import org.apache.kylin.measure.topn.TopNCounter;
import org.apache.kylin.metadata.datatype.DoubleMutable;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * Build a cube (many cuboids) in memory. Calculating multiple cuboids at the same time as long as memory permits.
 * Assumes base cuboid fits in memory or otherwise OOM exception will occur.
 */
public class InMemCubeBuilder extends AbstractInMemCubeBuilder {

    private static Logger logger = LoggerFactory.getLogger(InMemCubeBuilder.class);

    // by experience
    private static final double DERIVE_AGGR_CACHE_CONSTANT_FACTOR = 0.1;
    private static final double DERIVE_AGGR_CACHE_VARIABLE_FACTOR = 0.9;

    private final CuboidScheduler cuboidScheduler;
    private final long baseCuboidId;
    private final int totalCuboidCount;
    private final String[] metricsAggrFuncs;
    private final MeasureDesc[] measureDescs;
    private final int measureCount;

    private MemoryBudgetController memBudget;
    private MemoryWaterLevel baseCuboidMemTracker;

    private Thread[] taskThreads;
    private Throwable[] taskThreadExceptions;
    private TreeSet<CuboidTask> taskPending;
    private AtomicInteger taskCuboidCompleted = new AtomicInteger(0);

    private CuboidResult baseResult;
    private Object[] totalSumForSanityCheck;
    private ICuboidCollector resultCollector;

    public InMemCubeBuilder(CubeDesc cubeDesc, IJoinedFlatTableDesc flatDesc, Map<TblColRef, Dictionary<String>> dictionaryMap) {
        super(cubeDesc, flatDesc, dictionaryMap);
        this.cuboidScheduler = new CuboidScheduler(cubeDesc);
        this.baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        this.totalCuboidCount = cuboidScheduler.getCuboidCount();

        this.measureCount = cubeDesc.getMeasures().size();
        this.measureDescs = cubeDesc.getMeasures().toArray(new MeasureDesc[measureCount]);

        List<String> metricsAggrFuncsList = Lists.newArrayList();

        for (int i = 0; i < measureCount; i++) {
            MeasureDesc measureDesc = measureDescs[i];
            metricsAggrFuncsList.add(measureDesc.getFunction().getExpression());
        }
        this.metricsAggrFuncs = metricsAggrFuncsList.toArray(new String[metricsAggrFuncsList.size()]);
    }

    private GridTable newGridTableByCuboidID(long cuboidID) throws IOException {
        GTInfo info = CubeGridTable.newGTInfo(
                Cuboid.findById(cubeDesc, cuboidID),
                new CubeDimEncMap(cubeDesc, dictionaryMap)
        );

        // Below several store implementation are very similar in performance. The ConcurrentDiskStore is the simplest.
        // MemDiskStore store = new MemDiskStore(info, memBudget == null ? MemoryBudgetController.ZERO_BUDGET : memBudget);
        // MemDiskStore store = new MemDiskStore(info, MemoryBudgetController.ZERO_BUDGET);
        ConcurrentDiskStore store = new ConcurrentDiskStore(info);

        GridTable gridTable = new GridTable(info, store);
        return gridTable;
    }

    @Override
    public void build(BlockingQueue<List<String>> input, ICuboidWriter output) throws IOException {
        ConcurrentNavigableMap<Long, CuboidResult> result = build(input);
        try {
            for (CuboidResult cuboidResult : result.values()) {
                outputCuboid(cuboidResult.cuboidId, cuboidResult.table, output);
                cuboidResult.table.close();
            }
        } finally {
            output.close();
        }
    }

    public ConcurrentNavigableMap<Long, CuboidResult> build(BlockingQueue<List<String>> input) throws IOException {
        final ConcurrentNavigableMap<Long, CuboidResult> result = new ConcurrentSkipListMap<Long, CuboidResult>();
        build(input, new ICuboidCollector() {
            @Override
            public void collect(CuboidResult cuboidResult) {
                logger.info("collecting CuboidResult cuboid id:" + cuboidResult.cuboidId);
                result.put(cuboidResult.cuboidId, cuboidResult);
            }
        });
        logger.info("total CuboidResult count:" + result.size());
        return result;
    }

    interface ICuboidCollector {
        void collect(CuboidResult result);
    }

    private void build(BlockingQueue<List<String>> input, ICuboidCollector collector) throws IOException {
        long startTime = System.currentTimeMillis();
        logger.info("In Mem Cube Build start, " + cubeDesc.getName());

        baseCuboidMemTracker = new MemoryWaterLevel();
        baseCuboidMemTracker.markLow();

        // multiple threads to compute cuboid in parallel
        taskPending = new TreeSet<CuboidTask>();
        taskCuboidCompleted.set(0);
        taskThreads = prepareTaskThreads();
        taskThreadExceptions = new Throwable[taskThreadCount];

        // build base cuboid
        resultCollector = collector;
        totalSumForSanityCheck = null;
        baseResult = createBaseCuboid(input);
        if (baseResult.nRows == 0)
            return;

        // plan memory budget
        baseCuboidMemTracker.markLow();
        makeMemoryBudget();

        // kick off N-D cuboid tasks and output
        addChildTasks(baseResult);
        start(taskThreads);

        // wait complete
        join(taskThreads);

        long endTime = System.currentTimeMillis();
        logger.info("In Mem Cube Build end, " + cubeDesc.getName() + ", takes " + (endTime - startTime) + " ms");

        throwExceptionIfAny();
    }

    public void abort() {
        interrupt(taskThreads);
    }

    private void start(Thread... threads) {
        for (Thread t : threads)
            t.start();
    }

    private void interrupt(Thread... threads) {
        for (Thread t : threads)
            t.interrupt();
    }

    private void join(Thread... threads) throws IOException {
        try {
            for (Thread t : threads)
                t.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while waiting task and output complete", e);
        }
    }

    private void throwExceptionIfAny() throws IOException {
        ArrayList<Throwable> errors = new ArrayList<Throwable>();
        for (int i = 0; i < taskThreadCount; i++) {
            Throwable t = taskThreadExceptions[i];
            if (t != null)
                errors.add(t);
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

    private Thread[] prepareTaskThreads() {
        Thread[] result = new Thread[taskThreadCount];
        for (int i = 0; i < taskThreadCount; i++) {
            result[i] = new CuboidTaskThread(i);
        }
        return result;
    }

    public boolean isAllCuboidDone() {
        return taskCuboidCompleted.get() == totalCuboidCount;
    }

    private class CuboidTaskThread extends Thread {
        private int id;

        CuboidTaskThread(int id) {
            super("CuboidTask-" + id);
            this.id = id;
        }

        @Override
        public void run() {
            try {
                while (!isAllCuboidDone()) {
                    CuboidTask task = null;
                    synchronized (taskPending) {
                        while (task == null && taskHasNoException()) {
                            task = taskPending.pollFirst();
                            if (task == null)
                                taskPending.wait(60000);
                        }
                    }
                    // if task error occurs
                    if (task == null)
                        break;

                    CuboidResult newCuboid = buildCuboid(task.parent, task.childCuboidId);
                    addChildTasks(newCuboid);

                    if (isAllCuboidDone()) {
                        for (Thread t : taskThreads) {
                            if (t != Thread.currentThread())
                                t.interrupt();
                        }
                    }
                }
            } catch (Throwable ex) {
                if (!isAllCuboidDone()) {
                    logger.error("task thread exception", ex);
                    taskThreadExceptions[id] = ex;
                }
            }
        }
    }

    private boolean taskHasNoException() {
        for (int i = 0; i < taskThreadExceptions.length; i++)
            if (taskThreadExceptions[i] != null)
                return false;
        return true;
    }

    private void addChildTasks(CuboidResult parent) {
        List<Long> children = cuboidScheduler.getSpanningCuboid(parent.cuboidId);
        if (!children.isEmpty()) {
            synchronized (taskPending) {
                for (Long child : children) {
                    taskPending.add(new CuboidTask(parent, child));
                }
                taskPending.notifyAll();
            }
        }
    }

    private void makeMemoryBudget() {
        baseResult.aggrCacheMB = Math.max(baseCuboidMemTracker.getEstimateMB(), 10); // 10 MB at minimal
        logger.debug("Base cuboid aggr cache is " + baseResult.aggrCacheMB + " MB");
        int systemAvailMB = MemoryBudgetController.gcAndGetSystemAvailMB();
        logger.debug("System avail " + systemAvailMB + " MB");
        int reserve = reserveMemoryMB;
        logger.debug("Reserve " + reserve + " MB for system basics");

        int budget = systemAvailMB - reserve;
        if (budget < baseResult.aggrCacheMB) {
            // make sure we have base aggr cache as minimal
            budget = baseResult.aggrCacheMB;
            logger.warn("System avail memory (" + systemAvailMB + " MB) is less than base aggr cache (" + baseResult.aggrCacheMB + " MB) + minimal reservation (" + reserve + " MB), consider increase JVM heap -Xmx");
        }

        logger.debug("Memory Budget is " + budget + " MB");
        memBudget = new MemoryBudgetController(budget);
    }

    private CuboidResult createBaseCuboid(BlockingQueue<List<String>> input) throws IOException {
        long startTime = System.currentTimeMillis();
        logger.info("Calculating base cuboid " + baseCuboidId);

        GridTable baseCuboid = newGridTableByCuboidID(baseCuboidId);
        GTBuilder baseBuilder = baseCuboid.rebuild();
        IGTScanner baseInput = new InputConverter(baseCuboid.getInfo(), input);

        Pair<ImmutableBitSet, ImmutableBitSet> dimensionMetricsBitSet = InMemCubeBuilderUtils.getDimensionAndMetricColumnBitSet(baseCuboidId, measureCount);
        GTScanRequest req = new GTScanRequestBuilder().setInfo(baseCuboid.getInfo()).setRanges(null).setDimensions(null).setAggrGroupBy(dimensionMetricsBitSet.getFirst()).setAggrMetrics(dimensionMetricsBitSet.getSecond()).setAggrMetricsFuncs(metricsAggrFuncs).setFilterPushDown(null).createGTScanRequest();
        GTAggregateScanner aggregationScanner = new GTAggregateScanner(baseInput, req);
        aggregationScanner.trackMemoryLevel(baseCuboidMemTracker);

        int count = 0;
        for (GTRecord r : aggregationScanner) {
            if (count == 0) {
                baseCuboidMemTracker.markHigh();
            }
            baseBuilder.write(r);
            count++;
        }
        aggregationScanner.close();
        baseBuilder.close();

        long timeSpent = System.currentTimeMillis() - startTime;
        logger.info("Cuboid " + baseCuboidId + " has " + count + " rows, build takes " + timeSpent + "ms");

        int mbEstimateBaseAggrCache = (int) (aggregationScanner.getEstimateSizeOfAggrCache() / MemoryBudgetController.ONE_MB);
        logger.info("Wild estimate of base aggr cache is " + mbEstimateBaseAggrCache + " MB");

        return updateCuboidResult(baseCuboidId, baseCuboid, count, timeSpent, 0);
    }

    private CuboidResult updateCuboidResult(long cuboidId, GridTable table, int nRows, long timeSpent, int aggrCacheMB) {
        if (aggrCacheMB <= 0 && baseResult != null) {
            aggrCacheMB = (int) Math.round(//
                    (DERIVE_AGGR_CACHE_CONSTANT_FACTOR + DERIVE_AGGR_CACHE_VARIABLE_FACTOR * nRows / baseResult.nRows) //
                            * baseResult.aggrCacheMB);
        }

        CuboidResult result = new CuboidResult(cuboidId, table, nRows, timeSpent, aggrCacheMB);
        taskCuboidCompleted.incrementAndGet();

        resultCollector.collect(result);
        return result;
    }

    private CuboidResult buildCuboid(CuboidResult parent, long cuboidId) throws IOException {
        final String consumerName = "AggrCache@Cuboid " + cuboidId;
        MemoryBudgetController.MemoryConsumer consumer = new MemoryBudgetController.MemoryConsumer() {
            @Override
            public int freeUp(int mb) {
                return 0; // cannot free up on demand
            }

            @Override
            public String toString() {
                return consumerName;
            }
        };

        // reserve memory for aggregation cache, can't be larger than the parent
        memBudget.reserveInsist(consumer, parent.aggrCacheMB);
        try {
            return aggregateCuboid(parent, cuboidId);
        } finally {
            memBudget.reserve(consumer, 0);
        }
    }

    private CuboidResult aggregateCuboid(CuboidResult parent, long cuboidId) throws IOException {
        final Pair<ImmutableBitSet, ImmutableBitSet> allNeededColumns = InMemCubeBuilderUtils.getDimensionAndMetricColumnBitSet(parent.cuboidId, cuboidId, measureCount);
        return scanAndAggregateGridTable(parent.table, parent.cuboidId, cuboidId, allNeededColumns.getFirst(), allNeededColumns.getSecond());
    }

    private GTAggregateScanner prepareGTAggregationScanner(GridTable gridTable, long parentId, long cuboidId, ImmutableBitSet aggregationColumns, ImmutableBitSet measureColumns) throws IOException {
        GTInfo info = gridTable.getInfo();
        GTScanRequest req = new GTScanRequestBuilder().setInfo(info).setRanges(null).setDimensions(null).setAggrGroupBy(aggregationColumns).setAggrMetrics(measureColumns).setAggrMetricsFuncs(metricsAggrFuncs).setFilterPushDown(null).createGTScanRequest();
        GTAggregateScanner scanner = (GTAggregateScanner) gridTable.scan(req);

        // for child cuboid, some measures don't need aggregation.
        if (parentId != cuboidId) {
            boolean[] aggrMask = new boolean[measureDescs.length];
            for (int i = 0; i < measureDescs.length; i++) {
                aggrMask[i] = !measureDescs[i].getFunction().getMeasureType().onlyAggrInBaseCuboid();

                if (!aggrMask[i]) {
                    logger.info(measureDescs[i].toString() + " doesn't need aggregation.");
                }
            }
            scanner.setAggrMask(aggrMask);
        }

        return scanner;
    }

    private CuboidResult scanAndAggregateGridTable(GridTable gridTable, long parentId, long cuboidId, ImmutableBitSet aggregationColumns, ImmutableBitSet measureColumns) throws IOException {
        long startTime = System.currentTimeMillis();
        logger.info("Calculating cuboid " + cuboidId);

        GTAggregateScanner scanner = prepareGTAggregationScanner(gridTable, parentId, cuboidId, aggregationColumns, measureColumns);
        GridTable newGridTable = newGridTableByCuboidID(cuboidId);
        GTBuilder builder = newGridTable.rebuild();

        ImmutableBitSet allNeededColumns = aggregationColumns.or(measureColumns);

        GTRecord newRecord = new GTRecord(newGridTable.getInfo());
        int count = 0;
        try {
            for (GTRecord record : scanner) {
                count++;
                for (int i = 0; i < allNeededColumns.trueBitCount(); i++) {
                    int c = allNeededColumns.trueBitAt(i);
                    newRecord.set(i, record.get(c));
                }
                builder.write(newRecord);
            }

            //long t = System.currentTimeMillis();
            //sanityCheck(parentId, cuboidId, scanner.getTotalSumForSanityCheck());
            //logger.info("sanity check for Cuboid " + cuboidId + " cost " + (System.currentTimeMillis() - t) + "ms");
        } finally {
            scanner.close();
            builder.close();
        }

        long timeSpent = System.currentTimeMillis() - startTime;
        logger.info("Cuboid " + cuboidId + " has " + count + " rows, build takes " + timeSpent + "ms");

        return updateCuboidResult(cuboidId, newGridTable, count, timeSpent, 0);
    }

    @SuppressWarnings({ "unused", "rawtypes", "unchecked" })
    private void sanityCheck(long parentId, long cuboidId, Object[] totalSum) {
        // double sum introduces error and causes result not exactly equal
        for (int i = 0; i < totalSum.length; i++) {
            if (totalSum[i] instanceof DoubleMutable) {
                totalSum[i] = Math.round(((DoubleMutable) totalSum[i]).get());
            } else if (totalSum[i] instanceof Double) {
                totalSum[i] = Math.round(((Double) totalSum[i]).doubleValue());
            } else if (totalSum[i] instanceof TopNCounter) {
                TopNCounter counter = (TopNCounter) totalSum[i];
                Iterator<Counter> iterator = counter.iterator();
                double total = 0.0;
                while (iterator.hasNext()) {
                    Counter aCounter = iterator.next();
                    total += aCounter.getCount();
                }
                totalSum[i] = Math.round(total);
            }

        }

        if (totalSumForSanityCheck == null) {
            totalSumForSanityCheck = totalSum;
            return;
        }
        if (Arrays.equals(totalSumForSanityCheck, totalSum) == false) {
            logger.info("sanityCheck failed when calculate " + cuboidId + " from parent " + parentId);
            logger.info("Expected: " + Arrays.toString(totalSumForSanityCheck));
            logger.info("Actually: " + Arrays.toString(totalSum));
            throw new IllegalStateException();
        }
    }

    // ===========================================================================

    private static class CuboidTask implements Comparable<CuboidTask> {
        final CuboidResult parent;
        final long childCuboidId;

        CuboidTask(CuboidResult parent, long childCuboidId) {
            this.parent = parent;
            this.childCuboidId = childCuboidId;
        }

        @Override
        public int compareTo(CuboidTask o) {
            long comp = this.childCuboidId - o.childCuboidId;
            return comp < 0 ? -1 : (comp > 0 ? 1 : 0);
        }
    }

    // ============================================================================

    private class InputConverter implements IGTScanner {
        GTInfo info;
        GTRecord record;
        BlockingQueue<List<String>> input;
        final InMemCubeBuilderInputConverter inMemCubeBuilderInputConverter;

        public InputConverter(GTInfo info, BlockingQueue<List<String>> input) {
            this.info = info;
            this.input = input;
            this.record = new GTRecord(info);
            this.inMemCubeBuilderInputConverter = new InMemCubeBuilderInputConverter(cubeDesc, flatDesc, dictionaryMap, info);
        }

        @Override
        public Iterator<GTRecord> iterator() {
            return new Iterator<GTRecord>() {

                List<String> currentObject = null;

                @Override
                public boolean hasNext() {
                    try {
                        currentObject = input.take();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                    return currentObject != null && currentObject.size() > 0;
                }

                @Override
                public GTRecord next() {
                    if (currentObject.size() == 0)
                        throw new IllegalStateException();

                    inMemCubeBuilderInputConverter.convert(currentObject, record);
                    return record;
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
    }
}

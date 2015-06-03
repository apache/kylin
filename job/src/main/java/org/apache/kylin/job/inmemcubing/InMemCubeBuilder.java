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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.kylin.common.hll.HyperLogLogPlusCounter;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeJoinedFlatTableDesc;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.cube.model.HBaseColumnFamilyDesc;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.metadata.measure.MeasureCodec;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.cube.CubeGridTable;
import org.apache.kylin.storage.gridtable.GTAggregateScanner;
import org.apache.kylin.storage.gridtable.GTBuilder;
import org.apache.kylin.storage.gridtable.GTInfo;
import org.apache.kylin.storage.gridtable.GTRecord;
import org.apache.kylin.storage.gridtable.GTScanRequest;
import org.apache.kylin.storage.gridtable.GridTable;
import org.apache.kylin.storage.gridtable.IGTScanner;
import org.apache.kylin.storage.gridtable.IGTStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 */
public class InMemCubeBuilder implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(InMemCubeBuilder.class);
    private static final LongWritable ONE = new LongWritable(1l);

    private final BlockingQueue<List<String>> inputQueue;
    private final ICuboidWriter outputWriter;

    private final CubeDesc cubeDesc;
    private final long baseCuboidId;
    private final CuboidScheduler cuboidScheduler;
    private final Map<TblColRef, Dictionary<?>> dictionaryMap;
    private final CubeJoinedFlatTableDesc intermediateTableDesc;
    private final MeasureCodec measureCodec;
    private final String[] metricsAggrFuncs;
    private final Map<Integer, Integer> dependentMeasures; // key: index of Measure which depends on another measure; value: index of Measure which is depended on;
    private final int[] hbaseMeasureRefIndex;
    private final MeasureDesc[] measureDescs;
    private final int measureCount;

    private MemoryBudgetController memBudget;
    private int taskThreadCount = 4;
    private Thread[] taskThreads;
    private Throwable[] taskThreadExceptions;
    private TreeSet<CuboidTask> taskPending;
    private AtomicInteger taskCuboidCompleted;
    private CuboidResult baseResult;

    private SortedMap<Long, CuboidResult> outputPending;
    private Thread outputThread;
    private Throwable outputThreadException;
    private int outputCuboidExpected;

    private Object[] totalSumForSanityCheck;

    public InMemCubeBuilder(BlockingQueue<List<String>> queue, CubeDesc cubeDesc, Map<TblColRef, Dictionary<?>> dictionaryMap, ICuboidWriter gtRecordWriter) {
        if (dictionaryMap == null || dictionaryMap.isEmpty()) {
            throw new IllegalArgumentException("dictionary cannot be empty");
        }
        this.inputQueue = queue;
        this.cubeDesc = cubeDesc;
        this.cuboidScheduler = new CuboidScheduler(cubeDesc);
        this.dictionaryMap = dictionaryMap;
        this.outputWriter = gtRecordWriter;
        this.baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        this.intermediateTableDesc = new CubeJoinedFlatTableDesc(cubeDesc, null);
        this.measureCodec = new MeasureCodec(cubeDesc.getMeasures());

        Map<String, Integer> measureIndexMap = Maps.newHashMap();
        List<String> metricsAggrFuncsList = Lists.newArrayList();
        measureCount = cubeDesc.getMeasures().size();

        List<MeasureDesc> measureDescsList = Lists.newArrayList();
        hbaseMeasureRefIndex = new int[measureCount];
        int measureRef = 0;
        for (HBaseColumnFamilyDesc familyDesc : cubeDesc.getHbaseMapping().getColumnFamily()) {
            for (HBaseColumnDesc hbaseColDesc : familyDesc.getColumns()) {
                for (MeasureDesc measure : hbaseColDesc.getMeasures()) {
                    for (int j = 0; j < measureCount; j++) {
                        if (cubeDesc.getMeasures().get(j).equals(measure)) {
                            measureDescsList.add(measure);
                            hbaseMeasureRefIndex[measureRef] = j;
                            break;
                        }
                    }
                    measureRef++;
                }
            }
        }

        for (int i = 0; i < measureCount; i++) {
            MeasureDesc measureDesc = measureDescsList.get(i);
            metricsAggrFuncsList.add(measureDesc.getFunction().getExpression());
            measureIndexMap.put(measureDesc.getName(), i);
        }
        this.metricsAggrFuncs = metricsAggrFuncsList.toArray(new String[metricsAggrFuncsList.size()]);

        this.dependentMeasures = Maps.newHashMap();
        for (int i = 0; i < measureCount; i++) {
            String depMsrRef = measureDescsList.get(i).getDependentMeasureRef();
            if (depMsrRef != null) {
                int index = measureIndexMap.get(depMsrRef);
                dependentMeasures.put(i, index);
            }
        }

        this.measureDescs = cubeDesc.getMeasures().toArray(new MeasureDesc[measureCount]);
    }

    public void setConcurrentThreads(int n) {
        this.taskThreadCount = n;
    }

    private GridTable newGridTableByCuboidID(long cuboidID) throws IOException {
        GTInfo info = CubeGridTable.newGTInfo(cubeDesc, cuboidID, dictionaryMap);

        // Before several store implementation are very similar in performance. The ConcurrentDiskStore is the simplest.
        // MemDiskStore store = new MemDiskStore(info, memBudget == null ? MemoryBudgetController.ZERO_BUDGET : memBudget);
        // MemDiskStore store = new MemDiskStore(info, MemoryBudgetController.ZERO_BUDGET);
        ConcurrentDiskStore store = new ConcurrentDiskStore(info);

        GridTable gridTable = new GridTable(info, store);
        return gridTable;
    }

    private Pair<ImmutableBitSet, ImmutableBitSet> getDimensionAndMetricColumnBitSet(long cuboidId) {
        BitSet bitSet = BitSet.valueOf(new long[] { cuboidId });
        BitSet dimension = new BitSet();
        dimension.set(0, bitSet.cardinality());
        BitSet metrics = new BitSet();
        metrics.set(bitSet.cardinality(), bitSet.cardinality() + this.measureCount);
        return new Pair<ImmutableBitSet, ImmutableBitSet>(new ImmutableBitSet(dimension), new ImmutableBitSet(metrics));
    }

    @Override
    public void run() {
        try {
            build();
        } catch (IOException e) {
            logger.error("Fail to build cube", e);
            throw new RuntimeException(e);
        }

    }

    public void build() throws IOException {
        long startTime = System.currentTimeMillis();
        logger.info("In Mem Cube Build start, " + cubeDesc.getName());

        // multiple threads to compute cuboid in parallel
        taskPending = new TreeSet<CuboidTask>();
        taskCuboidCompleted = new AtomicInteger(0);
        taskThreads = prepareTaskThreads();
        taskThreadExceptions = new Throwable[taskThreadCount];

        // output goes in a separate thread to leverage any async-ness
        outputPending = prepareOutputPending();
        outputCuboidExpected = outputPending.size();
        outputThread = prepareOutputThread();
        outputThreadException = null;

        // build base cuboid
        baseResult = createBaseCuboid();
        taskCuboidCompleted.incrementAndGet();
        if (baseResult.nRows == 0)
            return;

        // plan memory budget
        makeMemoryBudget();

        // kick off N-D cuboid tasks and output
        addChildTasks(baseResult);
        start(taskThreads);
        start(outputThread);

        // wait complete
        join(taskThreads);
        join(outputThread);

        long endTime = System.currentTimeMillis();
        logger.info("In Mem Cube Build end, " + cubeDesc.getName() + ", takes " + (endTime - startTime) + " ms");

        throwExceptionIfAny();
    }

    private void start(Thread... threads) {
        for (Thread t : threads)
            t.start();
    }

    private void join(Thread... threads) throws IOException {
        try {
            for (Thread t : threads)
                t.join();
        } catch (InterruptedException e) {
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
        if (outputThreadException != null) {
            errors.add(outputThreadException);
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

    private class CuboidTaskThread extends Thread {
        private int id;

        CuboidTaskThread(int id) {
            super("CuboidTask-" + id);
            this.id = id;
        }

        @Override
        public void run() {
            try {
                while (taskCuboidCompleted.get() < outputCuboidExpected) {
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
                    taskCuboidCompleted.incrementAndGet();

                    if (taskCuboidCompleted.get() == outputCuboidExpected) {
                        for (Thread t : taskThreads) {
                            if (t != Thread.currentThread())
                                t.interrupt();
                        }
                    }
                }
            } catch (Throwable ex) {
                if (taskCuboidCompleted.get() < outputCuboidExpected) {
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

    private SortedMap<Long, CuboidResult> prepareOutputPending() {
        TreeMap<Long, CuboidResult> result = new TreeMap<Long, CuboidResult>();
        prepareOutputPendingRecursive(Cuboid.getBaseCuboidId(cubeDesc), result);
        return Collections.synchronizedSortedMap(result);
    }

    private void prepareOutputPendingRecursive(Long cuboidId, TreeMap<Long, CuboidResult> result) {
        result.put(cuboidId, new CuboidResult(cuboidId, null, 0, 0, 0));
        for (Long child : cuboidScheduler.getSpanningCuboid(cuboidId)) {
            prepareOutputPendingRecursive(child, result);
        }
    }

    private Thread prepareOutputThread() {
        return new Thread("CuboidOutput") {
            public void run() {
                try {
                    while (!outputPending.isEmpty()) {
                        CuboidResult result = outputPending.get(outputPending.firstKey());
                        synchronized (result) {
                            while (result.table == null && taskHasNoException()) {
                                try {
                                    result.wait(60000);
                                } catch (InterruptedException e) {
                                    logger.error("interrupted", e);
                                }
                            }
                        }

                        // if task error occurs
                        if (result.table == null)
                            break;

                        outputCuboid(result.cuboidId, result.table);
                        outputPending.remove(result.cuboidId);
                    }
                } catch (Throwable ex) {
                    logger.error("output thread exception", ex);
                    outputThreadException = ex;
                }
            }
        };
    }

    private int getSystemAvailMB() {
        Runtime.getRuntime().gc();
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            logger.error("", e);
        }
        return MemoryBudgetController.getSystemAvailMB();
    }

    private void makeMemoryBudget() {
        int systemAvailMB = getSystemAvailMB();
        logger.info("System avail " + systemAvailMB + " MB");
        int reserve = Math.min(100, baseResult.aggrCacheMB / 3);
        logger.info("Reserve " + reserve + " MB for system basics");

        int budget = systemAvailMB - reserve;
        if (budget < baseResult.aggrCacheMB) {
            // make sure we have base aggr cache as minimal
            budget = baseResult.aggrCacheMB;
            logger.warn("!!! System avail memory (" + systemAvailMB + " MB) is less than base aggr cache (" + baseResult.aggrCacheMB + " MB) + minimal reservation (" + reserve + " MB), consider increase JVM heap -Xmx");
        }

        logger.info("Memory Budget is " + budget + " MB");
        if (budget > 0) {
            memBudget = new MemoryBudgetController(budget);
        }
    }

    private CuboidResult createBaseCuboid() throws IOException {
        GridTable baseCuboid = newGridTableByCuboidID(baseCuboidId);
        GTBuilder baseBuilder = baseCuboid.rebuild();
        IGTScanner baseInput = new InputConverter(baseCuboid.getInfo());

        int mbBefore = getSystemAvailMB();
        int mbAfter = 0;

        Pair<ImmutableBitSet, ImmutableBitSet> dimensionMetricsBitSet = getDimensionAndMetricColumnBitSet(baseCuboidId);
        GTScanRequest req = new GTScanRequest(baseCuboid.getInfo(), null, dimensionMetricsBitSet.getFirst(), dimensionMetricsBitSet.getSecond(), metricsAggrFuncs, null);
        GTAggregateScanner aggregationScanner = new GTAggregateScanner(baseInput, req);

        long startTime = System.currentTimeMillis();
        logger.info("Calculating cuboid " + baseCuboidId);

        int count = 0;
        for (GTRecord r : aggregationScanner) {
            if (mbAfter == 0) {
                mbAfter = getSystemAvailMB();
            }
            baseBuilder.write(r);
            count++;
        }
        aggregationScanner.close();
        baseBuilder.close();

        long timeSpent = System.currentTimeMillis() - startTime;
        logger.info("Cuboid " + baseCuboidId + " has " + count + " rows, build takes " + timeSpent + "ms");

        int mbBaseAggrCacheOnHeap = mbBefore - mbAfter;
        int mbEstimateBaseAggrCache = (int) (aggregationScanner.getEstimateSizeOfAggrCache() / MemoryBudgetController.ONE_MB);
        int mbBaseAggrCache = Math.max((int) (mbBaseAggrCacheOnHeap * 1.1), mbEstimateBaseAggrCache);
        mbBaseAggrCache = Math.max(mbBaseAggrCache, 10); // let it be 10 MB at least
        logger.info("Base aggr cache is " + mbBaseAggrCache + " MB (heap " + mbBaseAggrCacheOnHeap + " MB, estimate " + mbEstimateBaseAggrCache + " MB)");

        return updateCuboidResult(baseCuboidId, baseCuboid, count, timeSpent, mbBaseAggrCache);
    }

    private CuboidResult updateCuboidResult(long cuboidId, GridTable table, int nRows, long timeSpent, int mbBaseAggrCache) {
        CuboidResult result = outputPending.get(cuboidId);
        result.table = table;
        result.nRows = nRows;
        result.timeSpent = timeSpent;
        result.aggrCacheMB = mbBaseAggrCache;
        if (result.aggrCacheMB <= 0) {
            result.aggrCacheMB = (int) Math.ceil(1.0 * nRows / baseResult.nRows * baseResult.aggrCacheMB);
        }
        synchronized (result) {
            result.notify();
        }
        return result;
    }

    private CuboidResult buildCuboid(CuboidResult parent, long cuboidId) throws IOException {
        final String consumerName = "AggrCache@Cuboid " + cuboidId;
        MemoryBudgetController.MemoryConsumer consumer = new MemoryBudgetController.MemoryConsumer() {
            @Override
            public int freeUp(int mb) {
                return 0; // cannot free up
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
        Pair<ImmutableBitSet, ImmutableBitSet> columnBitSets = getDimensionAndMetricColumnBitSet(parent.cuboidId);
        ImmutableBitSet parentDimensions = columnBitSets.getFirst();
        ImmutableBitSet measureColumns = columnBitSets.getSecond();
        ImmutableBitSet childDimensions = parentDimensions;

        long mask = Long.highestOneBit(parent.cuboidId);
        long childCuboidId = cuboidId;
        long parentCuboidIdActualLength = Long.SIZE - Long.numberOfLeadingZeros(parent.cuboidId);
        int index = 0;
        for (int i = 0; i < parentCuboidIdActualLength; i++) {
            if ((mask & parent.cuboidId) > 0) {
                if ((mask & childCuboidId) == 0) {
                    // this dim will be aggregated
                    childDimensions = childDimensions.set(index, false);
                }
                index++;
            }
            mask = mask >> 1;
        }

        return scanAndAggregateGridTable(parent.table, cuboidId, childDimensions, measureColumns);
    }

    private CuboidResult scanAndAggregateGridTable(GridTable gridTable, long cuboidId, ImmutableBitSet aggregationColumns, ImmutableBitSet measureColumns) throws IOException {
        long startTime = System.currentTimeMillis();
        logger.info("Calculating cuboid " + cuboidId);

        GTScanRequest req = new GTScanRequest(gridTable.getInfo(), null, aggregationColumns, measureColumns, metricsAggrFuncs, null);
        GTAggregateScanner scanner = (GTAggregateScanner) gridTable.scan(req);
        GridTable newGridTable = newGridTableByCuboidID(cuboidId);
        GTBuilder builder = newGridTable.rebuild();

        ImmutableBitSet allNeededColumns = aggregationColumns.or(measureColumns);

        GTRecord newRecord = new GTRecord(newGridTable.getInfo());
        int count = 0;
        ByteArray byteArray = new ByteArray(8);
        ByteBuffer byteBuffer = ByteBuffer.allocate(8);
        try {
            BitSet dependentMetricsBS = new BitSet(allNeededColumns.cardinality());
            for (Integer i : dependentMeasures.keySet()) {
                dependentMetricsBS.set((allNeededColumns.cardinality() - measureCount + dependentMeasures.get(i)));
            }
            ImmutableBitSet dependentMetrics = new ImmutableBitSet(dependentMetricsBS);

            Object[] hllObjects = new Object[dependentMeasures.keySet().size()];

            for (GTRecord record : scanner) {
                count++;
                for (int i = 0; i < allNeededColumns.trueBitCount(); i++) {
                    int c = allNeededColumns.trueBitAt(i);
                    newRecord.set(i, record.get(c));
                }

                if (dependentMeasures.size() > 0) {
                    // update measures which have 'dependent_measure_ref'
                    newRecord.getValues(dependentMetrics, hllObjects);

                    for (Integer i : dependentMeasures.keySet()) {
                        for (int index = 0; index < dependentMetrics.trueBitCount(); index++) {
                            int c = dependentMetrics.trueBitAt(index);
                            if (c == allNeededColumns.cardinality() - measureCount + dependentMeasures.get(i)) {
                                assert hllObjects[index] instanceof HyperLogLogPlusCounter; // currently only HLL is allowed

                                byteBuffer.clear();
                                BytesUtil.writeVLong(((HyperLogLogPlusCounter) hllObjects[index]).getCountEstimate(), byteBuffer);
                                byteArray.set(byteBuffer.array(), 0, byteBuffer.position());
                                newRecord.set(allNeededColumns.cardinality() - measureCount + i, byteArray);
                            }
                        }

                    }
                }

                builder.write(newRecord);
            }

            sanityCheck(scanner.getTotalSumForSanityCheck());
        } finally {
            scanner.close();
            builder.close();
        }

        long timeSpent = System.currentTimeMillis() - startTime;
        logger.info("Cuboid " + cuboidId + " has " + count + " rows, build takes " + timeSpent + "ms");

        return updateCuboidResult(cuboidId, newGridTable, count, timeSpent, 0);
    }

    private void sanityCheck(Object[] totalSum) {
        // double sum introduces error and causes result not exactly equal
        for (int i = 0; i < totalSum.length; i++) {
            if (totalSum[i] instanceof DoubleWritable) {
                totalSum[i] = Math.round(((DoubleWritable) totalSum[i]).get());
            }
        }
        logger.info(Arrays.toString(totalSum));

        if (totalSumForSanityCheck == null) {
            totalSumForSanityCheck = totalSum;
            return;
        }
        if (Arrays.equals(totalSumForSanityCheck, totalSum) == false) {
            throw new IllegalStateException();
        }
    }

    private void outputCuboid(long cuboidId, GridTable gridTable) throws IOException {
        long startTime = System.currentTimeMillis();
        GTScanRequest req = new GTScanRequest(gridTable.getInfo(), null, null, null);
        IGTScanner scanner = gridTable.scan(req);
        for (GTRecord record : scanner) {
            this.outputWriter.write(cuboidId, record);
        }
        scanner.close();
        logger.info("Cuboid " + cuboidId + " output takes " + (System.currentTimeMillis() - startTime) + "ms");

        closeStore(gridTable);
    }

    private void closeStore(GridTable gt) throws IOException {
        IGTStore store = gt.getStore();
        if (store instanceof Closeable) {
            ((Closeable) store).close();
        }
    }

    // ===========================================================================

    private static class CuboidTask implements Comparable<CuboidTask> {
        CuboidResult parent;
        long childCuboidId;

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

    private static class CuboidResult {
        long cuboidId;
        GridTable table;
        int nRows;
        @SuppressWarnings("unused")
        long timeSpent;
        int aggrCacheMB;

        public CuboidResult(long cuboidId, GridTable table, int nRows, long timeSpent, int aggrCacheMB) {
            this.cuboidId = cuboidId;
            this.table = table;
            this.nRows = nRows;
            this.timeSpent = timeSpent;
            this.aggrCacheMB = aggrCacheMB;
        }
    }

    // ============================================================================

    private class InputConverter implements IGTScanner {
        GTInfo info;
        GTRecord record;

        public InputConverter(GTInfo info) {
            this.info = info;
            this.record = new GTRecord(info);
        }

        @Override
        public Iterator<GTRecord> iterator() {
            return new Iterator<GTRecord>() {

                List<String> currentObject = null;

                @Override
                public boolean hasNext() {
                    try {
                        currentObject = inputQueue.take();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    return currentObject != null && currentObject.size() > 0;
                }

                @Override
                public GTRecord next() {
                    if (currentObject.size() == 0)
                        throw new IllegalStateException();

                    buildGTRecord(currentObject, record);
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

        @Override
        public int getScannedRowCount() {
            return 0;
        }

        @Override
        public int getScannedRowBlockCount() {
            return 0;
        }

        private void buildGTRecord(List<String> row, GTRecord record) {
            Object[] dimensions = buildKey(row);
            Object[] metricsValues = buildValue(row);
            Object[] recordValues = new Object[dimensions.length + metricsValues.length];
            System.arraycopy(dimensions, 0, recordValues, 0, dimensions.length);
            System.arraycopy(metricsValues, 0, recordValues, dimensions.length, metricsValues.length);
            record.setValues(recordValues);
        }

        private Object[] buildKey(List<String> row) {
            int keySize = intermediateTableDesc.getRowKeyColumnIndexes().length;
            Object[] key = new Object[keySize];

            for (int i = 0; i < keySize; i++) {
                key[i] = row.get(intermediateTableDesc.getRowKeyColumnIndexes()[i]);
            }

            return key;
        }

        private Object[] buildValue(List<String> row) {

            Object[] values = new Object[measureCount];
            MeasureDesc measureDesc = null;

            for (int position = 0; position < hbaseMeasureRefIndex.length; position++) {
                int i = hbaseMeasureRefIndex[position];
                measureDesc = measureDescs[i];

                Object value = null;
                int[] flatTableIdx = intermediateTableDesc.getMeasureColumnIndexes()[i];
                FunctionDesc function = cubeDesc.getMeasures().get(i).getFunction();
                if (function.isCount() || function.isHolisticCountDistinct()) {
                    // note for holistic count distinct, this value will be ignored
                    value = ONE;
                } else if (flatTableIdx == null) {
                    value = measureCodec.getSerializer(i).valueOf(measureDesc.getFunction().getParameter().getValue());
                } else if (flatTableIdx.length == 1) {
                    value = measureCodec.getSerializer(i).valueOf(Bytes.toBytes(row.get(flatTableIdx[0])));
                } else {

                    byte[] result = null;
                    for (int x = 0; x < flatTableIdx.length; x++) {
                        byte[] split = Bytes.toBytes(row.get(flatTableIdx[x]));
                        if (result == null) {
                            result = Arrays.copyOf(split, split.length);
                        } else {
                            byte[] newResult = new byte[result.length + split.length];
                            System.arraycopy(result, 0, newResult, 0, result.length);
                            System.arraycopy(split, 0, newResult, result.length, split.length);
                            result = newResult;
                        }
                    }
                    value = measureCodec.getSerializer(i).valueOf(result);
                }
                values[position] = value;
            }
            return values;
        }

    }
}

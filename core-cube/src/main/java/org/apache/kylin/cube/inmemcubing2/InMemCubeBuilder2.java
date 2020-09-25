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
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.common.util.MemoryBudgetController;
import org.apache.kylin.common.util.MemoryBudgetController.MemoryWaterLevel;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.cube.gridtable.CubeGridTable;
import org.apache.kylin.cube.inmemcubing.AbstractInMemCubeBuilder;
import org.apache.kylin.cube.inmemcubing.ConcurrentDiskStore;
import org.apache.kylin.cube.inmemcubing.CuboidResult;
import org.apache.kylin.cube.inmemcubing.ICuboidWriter;
import org.apache.kylin.cube.inmemcubing.InMemCubeBuilderUtils;
import org.apache.kylin.cube.inmemcubing.InputConverter;
import org.apache.kylin.cube.inmemcubing.InputConverterUnit;
import org.apache.kylin.cube.inmemcubing.RecordConsumeBlockingQueueController;
import org.apache.kylin.cube.kv.CubeDimEncMap;
import org.apache.kylin.gridtable.GTAggregateScanner;
import org.apache.kylin.gridtable.GTBuilder;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.GTScanRequestBuilder;
import org.apache.kylin.gridtable.GridTable;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.gridtable.IGTStore;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.base.Stopwatch;
import org.apache.kylin.shaded.com.google.common.collect.Lists;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Build a cube (many cuboids) in memory. Calculating multiple cuboids at the same time as long as memory permits.
 * Assumes base cuboid fits in memory or otherwise OOM exception will occur.
 */
public class InMemCubeBuilder2 extends AbstractInMemCubeBuilder {
    private static Logger logger = LoggerFactory.getLogger(InMemCubeBuilder2.class);

    // by experience
    private static final double DERIVE_AGGR_CACHE_CONSTANT_FACTOR = 0.1;
    private static final double DERIVE_AGGR_CACHE_VARIABLE_FACTOR = 0.9;

    protected final String[] metricsAggrFuncs;
    protected final MeasureDesc[] measureDescs;
    protected final int measureCount;

    private MemoryBudgetController memBudget;
    protected final long baseCuboidId;
    private CuboidResult baseResult;

    private Queue<CuboidTask> completedTaskQueue;
    private AtomicInteger taskCuboidCompleted;

    private ICuboidCollectorWithCallBack resultCollector;

    public InMemCubeBuilder2(CuboidScheduler cuboidScheduler, IJoinedFlatTableDesc flatDesc,
            Map<TblColRef, Dictionary<String>> dictionaryMap) {
        super(cuboidScheduler, flatDesc, dictionaryMap);
        this.measureCount = cubeDesc.getMeasures().size();
        this.measureDescs = cubeDesc.getMeasures().toArray(new MeasureDesc[measureCount]);
        List<String> metricsAggrFuncsList = Lists.newArrayList();

        for (int i = 0; i < measureCount; i++) {
            MeasureDesc measureDesc = measureDescs[i];
            metricsAggrFuncsList.add(measureDesc.getFunction().getExpression());
        }
        this.metricsAggrFuncs = metricsAggrFuncsList.toArray(new String[metricsAggrFuncsList.size()]);
        this.baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
    }

    public int getBaseResultCacheMB() {
        return baseResult.aggrCacheMB;
    }

    private GridTable newGridTableByCuboidID(long cuboidID) throws IOException {
        GTInfo info = CubeGridTable.newGTInfo(Cuboid.findForMandatory(cubeDesc, cuboidID),
                new CubeDimEncMap(cubeDesc, dictionaryMap));

        // Below several store implementation are very similar in performance. The ConcurrentDiskStore is the simplest.
        // MemDiskStore store = new MemDiskStore(info, memBudget == null ? MemoryBudgetController.ZERO_BUDGET : memBudget);
        // MemDiskStore store = new MemDiskStore(info, MemoryBudgetController.ZERO_BUDGET);
        IGTStore store = new ConcurrentDiskStore(info);

        GridTable gridTable = new GridTable(info, store);
        return gridTable;
    }

    @Override
    public <T> void build(BlockingQueue<T> input, InputConverterUnit<T> inputConverterUnit, ICuboidWriter output)
            throws IOException {
        NavigableMap<Long, CuboidResult> result = buildAndCollect(
                RecordConsumeBlockingQueueController.getQueueController(inputConverterUnit, input), null);
        try {
            for (CuboidResult cuboidResult : result.values()) {
                outputCuboid(cuboidResult.cuboidId, cuboidResult.table, output);
                cuboidResult.table.close();
            }
        } finally {
            output.close();
        }
    }

    /**
     * Build all the cuboids and wait for all the tasks finished. 
     * 
     * @param input
     * @param listener
     * @return
     * @throws IOException
     */
    private <T> NavigableMap<Long, CuboidResult> buildAndCollect(final RecordConsumeBlockingQueueController<T> input,
            final ICuboidResultListener listener) throws IOException {

        long startTime = System.currentTimeMillis();
        logger.info("In Mem Cube Build2 start, {}", cubeDesc.getName());

        // build base cuboid
        buildBaseCuboid(input, listener);

        ForkJoinWorkerThreadFactory factory = new ForkJoinWorkerThreadFactory() {
            @Override
            public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
                final ForkJoinWorkerThread worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
                worker.setName("inmem-cubing-cuboid-worker-" + worker.getPoolIndex());
                return worker;
            }
        };
        ForkJoinPool builderPool = new ForkJoinPool(taskThreadCount, factory, null, true);
        ForkJoinTask rootTask = builderPool.submit(new Runnable() {
            @Override
            public void run() {
                startBuildFromBaseCuboid();
            }
        });
        rootTask.join();

        long endTime = System.currentTimeMillis();
        logger.info("In Mem Cube Build2 end, {}, takes {} ms", cubeDesc.getName(), (endTime - startTime));
        logger.info("total CuboidResult count: {}", resultCollector.getAllResult().size());
        return resultCollector.getAllResult();
    }

    public ICuboidCollectorWithCallBack getResultCollector() {
        return resultCollector;
    }

    public <T> CuboidResult buildBaseCuboid(RecordConsumeBlockingQueueController<T> input,
            final ICuboidResultListener listener) throws IOException {
        completedTaskQueue = new LinkedBlockingQueue<CuboidTask>();
        taskCuboidCompleted = new AtomicInteger(0);

        resultCollector = new DefaultCuboidCollectorWithCallBack(listener);

        MemoryBudgetController.MemoryWaterLevel baseCuboidMemTracker = new MemoryWaterLevel();
        baseCuboidMemTracker.markLow();
        baseResult = createBaseCuboid(input, baseCuboidMemTracker);

        if (baseResult.nRows == 0) {
            taskCuboidCompleted.set(cuboidScheduler.getCuboidCount());
            return baseResult;
        }

        baseCuboidMemTracker.markLow();
        baseResult.aggrCacheMB = Math.max(baseCuboidMemTracker.getEstimateMB(), 10); // 10 MB at minimal

        makeMemoryBudget();
        return baseResult;
    }

    public CuboidResult buildCuboid(CuboidTask task) throws IOException {
        CuboidResult newCuboid = buildCuboid(task.parent, task.childCuboidId);
        completedTaskQueue.add(task);
        addChildTasks(newCuboid);
        return newCuboid;
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

    public boolean isAllCuboidDone() {
        return taskCuboidCompleted.get() == cuboidScheduler.getCuboidCount();
    }

    public void startBuildFromBaseCuboid() {
        addChildTasks(baseResult);
    }

    private void addChildTasks(CuboidResult parent) {
        List<Long> children = cuboidScheduler.getSpanningCuboid(parent.cuboidId);
        if (children != null && !children.isEmpty()) {
            List<CuboidTask> childTasks = Lists.newArrayListWithExpectedSize(children.size());
            for (Long child : children) {
                CuboidTask task = new CuboidTask(parent, child, this);
                childTasks.add(task);
                task.fork();
            }
            for (CuboidTask childTask : childTasks) {
                childTask.join();
            }
        }
    }

    public Queue<CuboidTask> getCompletedTaskQueue() {
        return completedTaskQueue;
    }

    private void makeMemoryBudget() {
        int systemAvailMB = MemoryBudgetController.gcAndGetSystemAvailMB();
        logger.info("System avail {} MB", systemAvailMB);
        int reserve = reserveMemoryMB;
        logger.info("Reserve {} MB for system basics", reserve);

        int budget = systemAvailMB - reserve;
        if (budget < baseResult.aggrCacheMB) {
            // make sure we have base aggr cache as minimal
            budget = baseResult.aggrCacheMB;
            logger.warn(
                    "System avail memory ({} MB) is less than base aggr cache ({} MB) + minimal reservation ({} MB), consider increase JVM heap -Xmx",
                    systemAvailMB, baseResult.aggrCacheMB, reserve);
        }

        logger.info("Memory Budget is {} MB", budget);
        memBudget = new MemoryBudgetController(budget);
    }

    private <T> CuboidResult createBaseCuboid(RecordConsumeBlockingQueueController<T> input,
            MemoryBudgetController.MemoryWaterLevel baseCuboidMemTracker) throws IOException {
        logger.info("Calculating base cuboid {}", baseCuboidId);

        Stopwatch sw = Stopwatch.createUnstarted();
        sw.start();
        GridTable baseCuboid = newGridTableByCuboidID(baseCuboidId);
        GTBuilder baseBuilder = baseCuboid.rebuild();
        IGTScanner baseInput = new InputConverter<>(baseCuboid.getInfo(), input);

        Pair<ImmutableBitSet, ImmutableBitSet> dimensionMetricsBitSet = InMemCubeBuilderUtils
                .getDimensionAndMetricColumnBitSet(baseCuboidId, measureCount);
        GTScanRequest req = new GTScanRequestBuilder().setInfo(baseCuboid.getInfo()).setRanges(null).setDimensions(null)
                .setAggrGroupBy(dimensionMetricsBitSet.getFirst()).setAggrMetrics(dimensionMetricsBitSet.getSecond())
                .setAggrMetricsFuncs(metricsAggrFuncs).setFilterPushDown(null).createGTScanRequest();
        try (GTAggregateScanner aggregationScanner = new GTAggregateScanner(baseInput, req)) {
            aggregationScanner.trackMemoryLevel(baseCuboidMemTracker);

            int count = 0;
            for (GTRecord r : aggregationScanner) {
                if (count == 0) {
                    baseCuboidMemTracker.markHigh();
                }
                baseBuilder.write(r);
                count++;
            }

            baseBuilder.close();

            sw.stop();
            logger.info("Cuboid {} has {} rows, build takes {}ms", baseCuboidId, count, sw.elapsed(MILLISECONDS));

            int mbEstimateBaseAggrCache = (int) (aggregationScanner.getEstimateSizeOfAggrCache()
                    / MemoryBudgetController.ONE_MB);
            logger.info("Wild estimate of base aggr cache is {} MB", mbEstimateBaseAggrCache);

            return updateCuboidResult(baseCuboidId, baseCuboid, count, sw.elapsed(MILLISECONDS), 0,
                    input.inputConverterUnit.ifChange());
        }
    }

    private CuboidResult updateCuboidResult(long cuboidId, GridTable table, int nRows, long timeSpent,
            int aggrCacheMB) {
        return updateCuboidResult(cuboidId, table, nRows, timeSpent, aggrCacheMB, true);
    }

    private CuboidResult updateCuboidResult(long cuboidId, GridTable table, int nRows, long timeSpent, int aggrCacheMB,
            boolean ifCollect) {
        if (aggrCacheMB <= 0 && baseResult != null) {
            aggrCacheMB = (int) Math.round(
                    (DERIVE_AGGR_CACHE_CONSTANT_FACTOR + DERIVE_AGGR_CACHE_VARIABLE_FACTOR * nRows / baseResult.nRows) //
                            * baseResult.aggrCacheMB);
        }

        CuboidResult result = new CuboidResult(cuboidId, table, nRows, timeSpent, aggrCacheMB);
        taskCuboidCompleted.incrementAndGet();

        if (ifCollect) {
            resultCollector.collectAndNotify(result);
        }
        return result;
    }

    protected CuboidResult aggregateCuboid(CuboidResult parent, long cuboidId) throws IOException {
        final Pair<ImmutableBitSet, ImmutableBitSet> allNeededColumns = InMemCubeBuilderUtils
                .getDimensionAndMetricColumnBitSet(parent.cuboidId, cuboidId, measureCount);
        return scanAndAggregateGridTable(parent.table, newGridTableByCuboidID(cuboidId), parent.cuboidId,
                cuboidId, allNeededColumns.getFirst(), allNeededColumns.getSecond());
    }

    private GTAggregateScanner prepareGTAggregationScanner(GridTable gridTable, long parentId, long cuboidId,
            ImmutableBitSet aggregationColumns, ImmutableBitSet measureColumns) throws IOException {
        GTInfo info = gridTable.getInfo();
        GTScanRequest req = new GTScanRequestBuilder().setInfo(info).setRanges(null).setDimensions(null)
                .setAggrGroupBy(aggregationColumns).setAggrMetrics(measureColumns).setAggrMetricsFuncs(metricsAggrFuncs)
                .setFilterPushDown(null).createGTScanRequest();
        GTAggregateScanner scanner = (GTAggregateScanner) gridTable.scan(req);

        // for child cuboid, some measures don't need aggregation.
        if (parentId != cuboidId) {
            boolean[] aggrMask = new boolean[measureDescs.length];
            for (int i = 0; i < measureDescs.length; i++) {
                aggrMask[i] = !measureDescs[i].getFunction().getMeasureType().onlyAggrInBaseCuboid();

                if (!aggrMask[i]) {
                    logger.info("{} doesn't need aggregation.", measureDescs[i]);
                }
            }
            scanner.setAggrMask(aggrMask);
        }

        return scanner;
    }

    protected CuboidResult scanAndAggregateGridTable(GridTable gridTable, GridTable newGridTable, long parentId,
            long cuboidId, ImmutableBitSet aggregationColumns, ImmutableBitSet measureColumns) throws IOException {
        Stopwatch sw = Stopwatch.createUnstarted();
        sw.start();
        logger.info("Calculating cuboid {}", cuboidId);

        GTAggregateScanner scanner = prepareGTAggregationScanner(gridTable, parentId, cuboidId, aggregationColumns,
                measureColumns);
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
        } finally {
            scanner.close();
            builder.close();
        }
        sw.stop();
        logger.info("Cuboid {} has {} rows, build takes {}ms", cuboidId, count, sw.elapsed(MILLISECONDS));

        return updateCuboidResult(cuboidId, newGridTable, count, sw.elapsed(MILLISECONDS), 0);
    }
}

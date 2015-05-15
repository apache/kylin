/*
 *
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *
 *  contributor license agreements. See the NOTICE file distributed with
 *
 *  this work for additional information regarding copyright ownership.
 *
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *
 *  (the "License"); you may not use this file except in compliance with
 *
 *  the License. You may obtain a copy of the License at
 *
 *
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 *
 *  Unless required by applicable law or agreed to in writing, software
 *
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *  See the License for the specific language governing permissions and
 *
 *  limitations under the License.
 *
 * /
 */
package org.apache.kylin.job.hadoop.cubev2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.StringUtils;
import org.apache.kylin.common.hll.HyperLogLogPlusCounter;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeJoinedFlatTableDesc;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.metadata.measure.MeasureCodec;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.cube.CubeGridTable;
import org.apache.kylin.storage.gridtable.GTAggregateScanner;
import org.apache.kylin.storage.gridtable.GTBuilder;
import org.apache.kylin.storage.gridtable.GTComboStore;
import org.apache.kylin.storage.gridtable.GTInfo;
import org.apache.kylin.storage.gridtable.GTRecord;
import org.apache.kylin.storage.gridtable.GTScanRequest;
import org.apache.kylin.storage.gridtable.GridTable;
import org.apache.kylin.storage.gridtable.IGTScanner;
import org.apache.kylin.storage.util.SizeOfUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 */
public class InMemCubeBuilder implements Runnable {

    //estimation of (size of aggregation cache) / (size of mem store)
    private static final double AGGREGATION_CACHE_FACTOR = 3;
    private static Logger logger = LoggerFactory.getLogger(InMemCubeBuilder.class);

    private BlockingQueue<List<String>> queue;
    private CubeDesc desc = null;
    private long baseCuboidId;
    private CuboidScheduler cuboidScheduler = null;
    private Map<TblColRef, Dictionary<?>> dictionaryMap = null;
    private CubeJoinedFlatTableDesc intermediateTableDesc;
    private MeasureCodec measureCodec;
    private String[] metricsAggrFuncs = null;
    private Map<Integer, Integer> dependentMeasures = null; // key: index of Measure which depends on another measure; value: index of Measure which is depended on;
    public static final LongWritable ONE = new LongWritable(1l);

    protected IGTRecordWriter gtRecordWriter;


    /**
     * @param queue
     * @param cube
     * @param dictionaryMap
     * @param gtRecordWriter
     */
    public InMemCubeBuilder(BlockingQueue<List<String>> queue, CubeInstance cube, Map<TblColRef, Dictionary<?>> dictionaryMap, IGTRecordWriter gtRecordWriter) {
        if (dictionaryMap == null || dictionaryMap.isEmpty()) {
            throw new IllegalArgumentException();
        }
        this.queue = queue;
        this.desc = cube.getDescriptor();
        this.cuboidScheduler = new CuboidScheduler(desc);
        this.dictionaryMap = dictionaryMap;
        this.gtRecordWriter = gtRecordWriter;
        this.baseCuboidId = Cuboid.getBaseCuboidId(desc);
        this.intermediateTableDesc = new CubeJoinedFlatTableDesc(desc, null);
        this.measureCodec = new MeasureCodec(desc.getMeasures());

        Map<String, Integer> measureIndexMap = Maps.newHashMap();
        List<String> metricsAggrFuncsList = Lists.newArrayList();
        final int measureCount = desc.getMeasures().size();
        for (int i = 0; i < measureCount; i++) {
            MeasureDesc measureDesc = desc.getMeasures().get(i);
            metricsAggrFuncsList.add(measureDesc.getFunction().getExpression());

            measureIndexMap.put(desc.getMeasures().get(i).getName(), i);
        }
        this.metricsAggrFuncs = metricsAggrFuncsList.toArray(new String[metricsAggrFuncsList.size()]);

        this.dependentMeasures = Maps.newHashMap();
        for (int i = 0; i < measureCount; i++) {
            String depMsrRef = desc.getMeasures().get(i).getDependentMeasureRef();
            if (depMsrRef != null) {
                int index = measureIndexMap.get(depMsrRef);
                dependentMeasures.put(i, index);
            }
        }

    }


    private GridTable newGridTableByCuboidID(long cuboidID, boolean memStore) {
        GTInfo info = CubeGridTable.newGTInfo(desc, cuboidID, dictionaryMap);
        GTComboStore store = new GTComboStore(info, memStore);
        GridTable gridTable = new GridTable(info, store);
        return gridTable;
    }

    private GridTable aggregateCuboid(GridTable parentCuboid, long parentCuboidId, long cuboidId, boolean inMem) throws IOException {
        logger.info("Calculating cuboid " + cuboidId + " from parent " + parentCuboidId);
        Pair<BitSet, BitSet> columnBitSets = getDimensionAndMetricColumnBitSet(parentCuboidId);
        BitSet parentDimensions = columnBitSets.getFirst();
        BitSet measureColumns = columnBitSets.getSecond();
        BitSet childDimensions = (BitSet) parentDimensions.clone();

        long mask = Long.highestOneBit(parentCuboidId);
        long childCuboidId = cuboidId;
        long parentCuboidIdActualLength = Long.SIZE - Long.numberOfLeadingZeros(parentCuboidId);
        int index = 0;
        for (int i = 0; i < parentCuboidIdActualLength; i++) {
            if ((mask & parentCuboidId) > 0) {
                if ((mask & childCuboidId) == 0) {
                    // this dim will be aggregated
                    childDimensions.set(index, false);
                }
                index++;
            }
            mask = mask >> 1;
        }

        return scanAndAggregateGridTable(parentCuboid, cuboidId, childDimensions, measureColumns, inMem);

    }

    private GridTable scanAndAggregateGridTable(GridTable gridTable, long cuboidId, BitSet aggregationColumns, BitSet measureColumns, boolean inMem) throws IOException {
        GTScanRequest req = new GTScanRequest(gridTable.getInfo(), null, aggregationColumns, measureColumns, metricsAggrFuncs, null);
        IGTScanner scanner = gridTable.scan(req);
        GridTable newGridTable = newGridTableByCuboidID(cuboidId, inMem);
        GTBuilder builder = newGridTable.rebuild();

        BitSet allNeededColumns = new BitSet();
        allNeededColumns.or(aggregationColumns);
        allNeededColumns.or(measureColumns);

        GTRecord newRecord = new GTRecord(newGridTable.getInfo());
        int counter = 0;
        ByteArray byteArray = new ByteArray(8);
        ByteBuffer byteBuffer = ByteBuffer.allocate(8);
        try {
            BitSet dependentMetrics = new BitSet(allNeededColumns.cardinality());
            for (Integer i : dependentMeasures.keySet()) {
                dependentMetrics.set((allNeededColumns.cardinality() - desc.getMeasures().size() + dependentMeasures.get(i)));
            }

            Object[] hllObjects = new Object[dependentMeasures.keySet().size()];

            for (GTRecord record : scanner) {
                counter++;
                for (int i = allNeededColumns.nextSetBit(0), index = 0; i >= 0; i = allNeededColumns.nextSetBit(i + 1), index++) {
                    newRecord.set(index, record.get(i));
                }

                // update measures which have 'dependent_measure_ref'
                newRecord.getValues(dependentMetrics, hllObjects);

                for (Integer i : dependentMeasures.keySet()) {
                    for (int index = 0, c = dependentMetrics.nextSetBit(0); c >= 0; index++, c = dependentMetrics.nextSetBit(c + 1)) {
                        if (c == allNeededColumns.cardinality() - desc.getMeasures().size() + dependentMeasures.get(i)) {
                            assert hllObjects[index] instanceof HyperLogLogPlusCounter; // currently only HLL is allowed

                            byteBuffer.clear();
                            BytesUtil.writeVLong(((HyperLogLogPlusCounter) hllObjects[index]).getCountEstimate(), byteBuffer);
                            byteArray.set(byteBuffer.array(), 0, byteBuffer.position());
                            newRecord.set(allNeededColumns.cardinality() - desc.getMeasures().size() + i, byteArray);
                        }
                    }

                }

                builder.write(newRecord);
            }
        } finally {
            builder.close();
        }
        logger.info("Cuboid " + cuboidId + " has rows: " + counter);

        return newGridTable;
    }

    private Pair<BitSet, BitSet> getDimensionAndMetricColumnBitSet(long cuboidId) {
        BitSet bitSet = BitSet.valueOf(new long[]{cuboidId});
        BitSet dimension = new BitSet();
        dimension.set(0, bitSet.cardinality());
        BitSet metrics = new BitSet();
        metrics.set(bitSet.cardinality(), bitSet.cardinality() + this.desc.getMeasures().size());
        return new Pair<BitSet, BitSet>(dimension, metrics);
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

        Object[] values = new Object[desc.getMeasures().size()];
        MeasureDesc measureDesc = null;
        for (int i = 0, n = desc.getMeasures().size(); i < n; i++) {
            measureDesc = desc.getMeasures().get(i);
            Object value = null;
            int[] flatTableIdx = intermediateTableDesc.getMeasureColumnIndexes()[i];
            FunctionDesc function = desc.getMeasures().get(i).getFunction();
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
            values[i] = value;
        }
        return values;
    }


    @Override
    public void run() {
        try {
            logger.info("Create base cuboid " + baseCuboidId);
            final GridTable baseCuboidGT = newGridTableByCuboidID(baseCuboidId, true);

            GTBuilder baseGTBuilder = baseCuboidGT.rebuild();
            final GTRecord baseGTRecord = new GTRecord(baseCuboidGT.getInfo());

            IGTScanner queueScanner = new IGTScanner() {

                @Override
                public Iterator<GTRecord> iterator() {
                    return new Iterator<GTRecord>() {

                        List<String> currentObject = null;

                        @Override
                        public boolean hasNext() {
                            try {
                                currentObject = queue.take();
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            return currentObject != null && currentObject.size() > 0;
                        }

                        @Override
                        public GTRecord next() {
                            if (currentObject.size() == 0)
                                throw new IllegalStateException();

                            buildGTRecord(currentObject, baseGTRecord);
                            return baseGTRecord;
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
                    return baseCuboidGT.getInfo();
                }

                @Override
                public int getScannedRowCount() {
                    return 0;
                }

                @Override
                public int getScannedRowBlockCount() {
                    return 0;
                }
            };

            Pair<BitSet, BitSet> dimensionMetricsBitSet = getDimensionAndMetricColumnBitSet(baseCuboidId);
            GTScanRequest req = new GTScanRequest(baseCuboidGT.getInfo(), null, dimensionMetricsBitSet.getFirst(), dimensionMetricsBitSet.getSecond(), metricsAggrFuncs, null);
            IGTScanner aggregationScanner = new GTAggregateScanner(queueScanner, req);

            int counter = 0;
            for (GTRecord r : aggregationScanner) {
                baseGTBuilder.write(r);
                counter++;
            }
            baseGTBuilder.close();
            aggregationScanner.close();

            logger.info("Base cuboid has " + counter + " rows;");
            SimpleGridTableTree tree = new SimpleGridTableTree();
            tree.data = baseCuboidGT;
            tree.id = baseCuboidId;
            tree.parent = null;
            if (counter > 0) {
                List<Long> children = cuboidScheduler.getSpanningCuboid(baseCuboidId);
                Collections.sort(children);
                for (Long childId : children) {
                    createNDCuboidGT(tree, baseCuboidId, childId);
                }
            }
            dropStore(baseCuboidGT);

        } catch (IOException e) {
            logger.error("Fail to build cube", e);
            throw new RuntimeException(e);
        }

    }

    private void buildGTRecord(List<String> row, GTRecord record) {

        Object[] dimensions = buildKey(row);
        Object[] metricsValues = buildValue(row);
        Object[] recordValues = new Object[dimensions.length + metricsValues.length];
        System.arraycopy(dimensions, 0, recordValues, 0, dimensions.length);
        System.arraycopy(metricsValues, 0, recordValues, dimensions.length, metricsValues.length);
        record.setValues(recordValues);
    }

    private long checkMemory(long threshold) {
        final long freeMemory = Runtime.getRuntime().freeMemory();
        logger.info("available memory:" + (freeMemory >> 10) + " KB, memory needed:" + (threshold >> 10) + " KB");
        return freeMemory - threshold;
    }

    private boolean gc(TreeNode<GridTable> parentNode) {
        final long parentCuboidMem = SizeOfUtil.deepSizeOf(parentNode.data.getStore());
        long threshold = (long) (parentCuboidMem * (AGGREGATION_CACHE_FACTOR + 1));
        final List<TreeNode<GridTable>> gridTables = parentNode.getAncestorList();
        long memoryLeft = checkMemory(threshold);
        for (TreeNode<GridTable> gridTable : gridTables) {
            if (memoryLeft >= 0) {
                return true;
            } else {
                logger.info("memory is low, try to select one node to flush to disk from:" + StringUtils.join(",", gridTables));
                final GTComboStore store = (GTComboStore) gridTable.data.getStore();
                if (store.memoryUsage() > 0) {
                    final long storeSize = SizeOfUtil.deepSizeOf(store);
                    memoryLeft += storeSize;
                    logger.info("cuboid id:" + gridTable.id + " selected, memory used:" + (storeSize >> 10) + " KB");
                    long t = System.currentTimeMillis();
                    ((GTComboStore) store).switchToDiskStore();
                    logger.info("switch to disk store cost:" + (System.currentTimeMillis() - t) + "ms");
                }
            }
        }
        if (memoryLeft >= 0) {
            return true;
        } else {
            logger.warn("all ancestor nodes of " + parentNode.id + " has been flushed to disk, memory is still insufficient, usually due to jvm gc not finished, forced to use memory store");
            return true;
        }

    }

    private void createNDCuboidGT(SimpleGridTableTree parentNode, long parentCuboidId, long cuboidId) throws IOException {

        long startTime = System.currentTimeMillis();
        
        GTComboStore parentStore = (GTComboStore) parentNode.data.getStore();
        if (parentStore.memoryUsage() <= 0) {
            long t = System.currentTimeMillis();
            parentStore.switchToMemStore();
            logger.info("node " + parentNode.id + " switch to mem store cost:" + (System.currentTimeMillis() - t) + "ms");
        }

        boolean inMem = gc(parentNode);
        GridTable currentCuboid = aggregateCuboid(parentNode.data, parentCuboidId, cuboidId, inMem);
        SimpleGridTableTree node = new SimpleGridTableTree();
        node.parent = parentNode;
        node.data = currentCuboid;
        node.id = cuboidId;
        parentNode.children.add(node);

        logger.info("Cuboid " + cuboidId + " build takes " + (System.currentTimeMillis() - startTime) + "ms");

        List<Long> children = cuboidScheduler.getSpanningCuboid(cuboidId);
        if (!children.isEmpty()) {
            Collections.sort(children); // sort cuboids
            for (Long childId : children) {
                createNDCuboidGT(node, cuboidId, childId);
            }
        }

        startTime = System.currentTimeMillis();
        //output the grid table
        outputGT(cuboidId, currentCuboid);
        dropStore(currentCuboid);
        parentNode.children.remove(node);
        logger.info("Cuboid" + cuboidId + " output takes " + (System.currentTimeMillis() - startTime) + "ms");

    }

    private void dropStore(GridTable gt) throws IOException {
        ((GTComboStore) gt.getStore()).drop();
    }


    private void outputGT(Long cuboidId, GridTable gridTable) throws IOException {
        GTScanRequest req = new GTScanRequest(gridTable.getInfo(), null, null, null);
        IGTScanner scanner = gridTable.scan(req);
        for (GTRecord record : scanner) {
            this.gtRecordWriter.write(cuboidId, record);
        }
    }

    private static class TreeNode<T> {
        T data;
        long id;
        TreeNode<T> parent;
        List<TreeNode<T>> children = Lists.newArrayList();

        List<TreeNode<T>> getAncestorList() {
            ArrayList<TreeNode<T>> result = Lists.newArrayList();
            TreeNode<T> parent = this.parent;
            while (parent != null) {
                result.add(parent);
                parent = parent.parent;
            }
            return Lists.reverse(result);
        }

        @Override
        public String toString() {
            return id + "";
        }
    }

    private static class SimpleGridTableTree extends TreeNode<GridTable> {}


}

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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.kylin.common.hll.HyperLogLogPlusCounter;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
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
import org.apache.kylin.metadata.serializer.DataTypeSerializer;
import org.apache.kylin.storage.cube.CubeGridTable;
import org.apache.kylin.storage.gridtable.*;
import org.apache.kylin.storage.gridtable.memstore.GTSimpleMemStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.BlockingQueue;

/**
 * Created by shaoshi on 3/12/2015.
 */
@SuppressWarnings("rawtypes")
public class InMemCubeBuilder implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(InMemCubeBuilder.class);

    private BlockingQueue<List<String>> queue;
    private CubeDesc desc = null;
    private long baseCuboidId;
    private CuboidScheduler cuboidScheduler = null;
    private Map<TblColRef, Dictionary<?>> dictionaryMap = null;
    private CubeJoinedFlatTableDesc intermediateTableDesc;
    private MeasureCodec measureCodec;
    private int measureNumber;
    private String[] metricsAggrFuncs = null;
    private Map<Integer, Integer> dependentMeasures = null; // key: index of Measure which depends on another measure; value: index of Measure which is depended on;
    public static final LongWritable ONE = new LongWritable(1l);

    protected IGTRecordWriter gtRecordWriter;
    private GridTable baseCuboidGT;
    private DataTypeSerializer[] serializers;


    /**
     * @param queue
     * @param cube
     * @param dictionaryMap
     * @param gtRecordWriter
     */
    public InMemCubeBuilder(BlockingQueue<List<String>> queue, CubeInstance cube, Map<TblColRef, Dictionary<?>> dictionaryMap, IGTRecordWriter gtRecordWriter) {
        this.queue = queue;
        this.desc = cube.getDescriptor();
        this.cuboidScheduler = new CuboidScheduler(desc);
        this.dictionaryMap = dictionaryMap;
        this.gtRecordWriter = gtRecordWriter;
        baseCuboidId = Cuboid.getBaseCuboidId(desc);

        intermediateTableDesc = new CubeJoinedFlatTableDesc(desc, null);
        measureCodec = new MeasureCodec(desc.getMeasures());
        measureNumber = desc.getMeasures().size();

        dependentMeasures = Maps.newHashMap();

        Map<String, Integer> measureIndexMap = new HashMap<String, Integer>();
        List<String> metricsAggrFuncsList = Lists.newArrayList();
        for (int i = 0, n = desc.getMeasures().size(); i < n; i++) {
            MeasureDesc measureDesc = desc.getMeasures().get(i);
            metricsAggrFuncsList.add(measureDesc.getFunction().getExpression());

            measureIndexMap.put(desc.getMeasures().get(i).getName(), i);
        }
        metricsAggrFuncs = metricsAggrFuncsList.toArray(new String[metricsAggrFuncsList.size()]);

        for (int i = 0; i < measureNumber; i++) {
            String depMsrRef = desc.getMeasures().get(i).getDependentMeasureRef();
            if (depMsrRef != null) {
                int index = measureIndexMap.get(depMsrRef);
                dependentMeasures.put(i, index);
            }
        }

        if (dictionaryMap == null || dictionaryMap.isEmpty())
            throw new IllegalArgumentException();
    }


    private GridTable newGridTableByCuboidID(long cuboidID) {
        GTInfo info = CubeGridTable.newGTInfo(desc, cuboidID, dictionaryMap);
        GTSimpleMemStore store = new GTSimpleMemStore(info);
        GridTable gridTable = new GridTable(info, store);
        return gridTable;
    }

    private GridTable aggregateCuboid(GridTable parentCuboid, long parentCuboidId, long cuboidId) throws IOException {
        logger.info("Calculating cuboid " + cuboidId + " from parent " + parentCuboidId);
        Pair<BitSet, BitSet> columnBitSets = getDimensionAndMetricColumBitSet(parentCuboidId);
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

        return scanAndAggregateGridTable(parentCuboid, cuboidId, childDimensions, measureColumns);

    }

    private GridTable scanAndAggregateGridTable(GridTable gridTable, long cuboidId, BitSet aggregationColumns, BitSet measureColumns) throws IOException {
        GTScanRequest req = new GTScanRequest(gridTable.getInfo(), null, aggregationColumns, measureColumns, metricsAggrFuncs, null);
        IGTScanner scanner = gridTable.scan(req);
        GridTable newGridTable = newGridTableByCuboidID(cuboidId);
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
                dependentMetrics.set((allNeededColumns.cardinality() - measureNumber + dependentMeasures.get(i)));
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
                        if (c == allNeededColumns.cardinality() - measureNumber + dependentMeasures.get(i)) {
                            assert hllObjects[index] instanceof HyperLogLogPlusCounter; // currently only HLL is allowed

                            byteBuffer.clear();
                            BytesUtil.writeVLong(((HyperLogLogPlusCounter) hllObjects[index]).getCountEstimate(), byteBuffer);
                            byteArray.set(byteBuffer.array(), 0, byteBuffer.position());
                            newRecord.set(allNeededColumns.cardinality() - measureNumber + i, byteArray);
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

    private Pair<BitSet, BitSet> getDimensionAndMetricColumBitSet(long cuboidId) {
        BitSet bitSet = BitSet.valueOf(new long[]{cuboidId});
        BitSet dimension = new BitSet();
        dimension.set(0, bitSet.cardinality());
        BitSet metrics = new BitSet();
        metrics.set(bitSet.cardinality(), bitSet.cardinality() + this.measureNumber);
        return new Pair<BitSet, BitSet>(dimension, metrics);
    }

    private Object[] buildKey(List<String> row, DataTypeSerializer[] serializers) {
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
            createBaseCuboidGT();

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

            Pair<BitSet, BitSet> dimensionMetricsBitSet = getDimensionAndMetricColumBitSet(baseCuboidId);
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
            if (counter > 0)
                createNDCuboidGT(null, -1l, baseCuboidId);

        } catch (IOException e) {
            logger.error("Fail to build cube", e);
            throw new RuntimeException(e);
        }

    }

    private void buildGTRecord(List<String> row, GTRecord record) {

        Object[] dimensions = buildKey(row, serializers);
        Object[] metricsValues = buildValue(row);
        Object[] recordValues = new Object[dimensions.length + metricsValues.length];
        System.arraycopy(dimensions, 0, recordValues, 0, dimensions.length);
        System.arraycopy(metricsValues, 0, recordValues, dimensions.length, metricsValues.length);
        record.setValues(recordValues);
    }

    private void createBaseCuboidGT() throws IOException {

        logger.info("Create base cuboid " + baseCuboidId);
        Cuboid baseCuboid = Cuboid.findById(this.desc, baseCuboidId);
        serializers = new DataTypeSerializer[baseCuboid.getColumns().size()];

        for (int i = 0; i < baseCuboid.getColumns().size(); i++) {
            serializers[i] = DataTypeSerializer.create(baseCuboid.getColumns().get(i).getType());
        }

        this.baseCuboidGT = newGridTableByCuboidID(baseCuboidId);
    }


    private void createNDCuboidGT(GridTable parentCuboid, long parentCuboidId, long cuboidId) throws IOException {

        GridTable thisCuboid;
        long startTime = System.currentTimeMillis();
        if (parentCuboidId < 0) {
            thisCuboid = this.baseCuboidGT;
        } else {
            thisCuboid = aggregateCuboid(parentCuboid, parentCuboidId, cuboidId);
        }

        logger.info("Cuboid " + cuboidId + " build takes (second): " + (System.currentTimeMillis() - startTime) / 1000);

        ArrayList<Long> children = (ArrayList<Long>) cuboidScheduler.getSpanningCuboid(cuboidId);
        Collections.sort(children); // sort cuboids
        for (Long childId : children) {
            createNDCuboidGT(thisCuboid, cuboidId, childId);
        }


        startTime = System.currentTimeMillis();
        //output the grid table
        outputGT(cuboidId, thisCuboid);
        logger.info("Cuboid" + cuboidId + " output takes (second) " + (System.currentTimeMillis() - startTime) / 1000);

    }

    private void outputGT(Long cuboidId, GridTable gridTable) throws IOException {
        GTScanRequest req = new GTScanRequest(gridTable.getInfo(), null, null, null);
        IGTScanner scanner = gridTable.scan(req);
        for (GTRecord record : scanner) {
            this.gtRecordWriter.write(cuboidId, record);
        }
    }
}

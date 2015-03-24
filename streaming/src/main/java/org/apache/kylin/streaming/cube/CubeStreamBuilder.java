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
package org.apache.kylin.streaming.cube;

import com.google.common.base.Function;
import com.google.common.collect.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.kylin.common.hll.HyperLogLogPlusCounter;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeJoinedFlatTableDesc;
import org.apache.kylin.cube.model.DimensionDesc;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.dict.DictionaryGenerator;
import org.apache.kylin.metadata.measure.MeasureCodec;
import org.apache.kylin.metadata.model.DataType;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.serializer.DataTypeSerializer;
import org.apache.kylin.storage.gridtable.*;
import org.apache.kylin.storage.gridtable.memstore.GTSimpleMemStore;
import org.apache.kylin.streaming.Stream;
import org.apache.kylin.streaming.StreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by shaoshi on 3/12/2015.
 */
public class CubeStreamBuilder extends StreamBuilder {

    private static Logger logger = LoggerFactory.getLogger(CubeStreamBuilder.class);

    private CubeDesc desc = null;
    private int partitionId = -1;
    private CuboidScheduler cuboidScheduler = null;
    private List<List<String>> table = null;
    private Map<TblColRef, Dictionary<?>> dictionaryMap = null;
    private Cuboid baseCuboid = null;
    private CubeInstance cube;
    private CubeJoinedFlatTableDesc intermediateTableDesc;
    private MeasureCodec measureCodec;
    private int measureNumber;
    private String[] metricsAggrFuncs = null;
    private Map<Integer, Integer> dependentMeasures = null; // key: index of Measure which depends on another measure; value: index of Measure which is depended on;
    public static final LongWritable ONE = new LongWritable(1l);

    public CubeStreamBuilder(LinkedBlockingDeque<Stream> queue, String hTableName, CubeInstance cube, int partitionId) {
        super(queue, 10000);
        this.cube = cube;
        this.desc = cube.getDescriptor();
        this.partitionId = partitionId;
        this.cuboidScheduler = new CuboidScheduler(desc);

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
    }

    @Override
    protected void build(List<Stream> streamsToBuild) {
        long startTime = System.currentTimeMillis();
        intermediateTableDesc = new CubeJoinedFlatTableDesc(cube.getDescriptor(), null);

        table = Lists.transform(streamsToBuild, new Function<Stream, List<String>>() {
            @Nullable
            @Override
            public List<String> apply(@Nullable Stream input) {
                return parseStream(input, desc);
            }
        });

        dictionaryMap = buildDictionary(table, desc);

        long baseCuboidId = Cuboid.getBaseCuboidId(desc);

        List<GridTable> result = new LinkedList<GridTable>();
        try {
            calculateCuboid(null, -1l, baseCuboidId, result);
        } catch (IOException e) {
            e.printStackTrace();
        }

        assert result.size() > 0;
        logger.info("Totally " + result.size() + " cuboids be calculated, takes " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    }


    private void calculateCuboid(GridTable parentCuboid, long parentCuboidId, long cuboidId, List<GridTable> result) throws IOException {

        GridTable thisCuboid;
        if (parentCuboidId < 0) {
            thisCuboid = calculateBaseCuboid(this.table, cuboidId);
        } else {
            thisCuboid = aggregateCuboid(parentCuboid, parentCuboidId, cuboidId);
        }

        ArrayList<Long> children = (ArrayList<Long>) cuboidScheduler.getSpanningCuboid(cuboidId);
        Collections.sort(children); // sort cuboids
        for (Long childId : children) {
            calculateCuboid(thisCuboid, cuboidId, childId, result);
        }

        result.add(thisCuboid);
        logger.info("Cuboid " + cuboidId + " is built.");
        outputGT(thisCuboid);
    }

    private void outputGT(GridTable gridTable) throws IOException {
        IGTScanner scanner = gridTable.scan(null, null, null, null);
        for (GTRecord record : scanner) {
            logger.debug(record.toString());
        }
    }

    private GridTable calculateBaseCuboid(List<List<String>> table, long baseCuboidId) throws IOException {

        logger.info("Calculating base cuboid " + baseCuboidId + ", source records number " + table.size());
        Cuboid baseCuboid = Cuboid.findById(this.desc, baseCuboidId);
        DataTypeSerializer[] serializers = new DataTypeSerializer[baseCuboid.getColumns().size()];

        for (int i = 0; i < baseCuboid.getColumns().size(); i++) {
            serializers[i] = DataTypeSerializer.create(baseCuboid.getColumns().get(i).getType());
        }

        GridTable gridTable = newGridTableByCuboidID(baseCuboidId);
        GTRecord r = new GTRecord(gridTable.getInfo());
        GTBuilder builder = gridTable.rebuild();

        for (List<String> row : table) {
            Object[] dimensions = buildKey(row, serializers);
            Object[] metricsValues = buildValue(row);
            Object[] recordValues = new Object[dimensions.length + metricsValues.length];
            System.arraycopy(dimensions, 0, recordValues, 0, dimensions.length);
            System.arraycopy(metricsValues, 0, recordValues, dimensions.length, metricsValues.length);
            builder.write(r.setValues(recordValues));
        }
        builder.close();
        Pair<BitSet, BitSet> dimensionMetricsBitSet = getDimensionAndMetricColumBitSet(baseCuboidId);

        return scanAndAggregateGridTable(gridTable, baseCuboidId, dimensionMetricsBitSet.getFirst(), dimensionMetricsBitSet.getSecond());
    }

    private GridTable newGridTableByCuboidID(long cuboidID) {
        GTInfo info = newGTInfo(cuboidID);
        GTSimpleMemStore store = new GTSimpleMemStore(info);
        GridTable gridTable = new GridTable(info, store);
        return gridTable;
    }


    private GridTable aggregateCuboid(GridTable parentCuboid, long parentCuboidId, long cuboidId) throws IOException {
        //logger.info("Calculating cuboid " + cuboidId + " from parent " + parentCuboidId);
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

        IGTScanner scanner = gridTable.scanAndAggregate(null, null, aggregationColumns, measureColumns, metricsAggrFuncs, null);
        GridTable newGridTable = newGridTableByCuboidID(cuboidId);
        GTBuilder builder = newGridTable.rebuild();

        BitSet allNeededColumns = new BitSet();
        allNeededColumns.or(aggregationColumns);
        allNeededColumns.or(measureColumns);

        GTRecord newRecord = new GTRecord(newGridTable.getInfo());
        ByteArray byteArray = new ByteArray(8);
        for (GTRecord record : scanner) {
            for (int i = allNeededColumns.nextSetBit(0), index = 0; i >= 0; i = allNeededColumns.nextSetBit(i + 1), index++) {
                newRecord.set(index, record.get(i));
            }

            // update measures which have 'dependent_measure_ref'
            for (Integer i : dependentMeasures.keySet()) {
                Object hllValue = newRecord.getValues()[(allNeededColumns.cardinality() - measureNumber + dependentMeasures.get(i))];
                assert hllValue instanceof HyperLogLogPlusCounter; // currently only HLL is allowed

                BytesUtil.writeVLong(((HyperLogLogPlusCounter) hllValue).getCountEstimate(), byteArray.asBuffer());
                newRecord.set(allNeededColumns.cardinality() - measureNumber + i, byteArray);
            }

            builder.write(newRecord);
        }
        builder.close();

        return newGridTable;
    }

    private Pair<BitSet, BitSet> getDimensionAndMetricColumBitSet(long cuboidId) {
        BitSet bitSet = BitSet.valueOf(new long[]{cuboidId});
        BitSet dimension = new BitSet();
        dimension.set(0, bitSet.cardinality());
        BitSet metrics = new BitSet();
        metrics.set(bitSet.cardinality(), bitSet.cardinality() + this.measureNumber);
        return new Pair<BitSet, BitSet>(
                dimension, metrics
        );
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


    private GTInfo newGTInfo(long cuboidID) {
        Pair<BitSet, BitSet> dimensionMetricsBitSet = getDimensionAndMetricColumBitSet(cuboidID);
        GTInfo.Builder builder = infoBuilder(cuboidID);
        builder.enableColumnBlock(new BitSet[]{dimensionMetricsBitSet.getFirst(), dimensionMetricsBitSet.getSecond()});
        builder.setPrimaryKey(dimensionMetricsBitSet.getFirst());
        GTInfo info = builder.build();
        return info;
    }

    private GTInfo.Builder infoBuilder(long cuboidID) {
        Cuboid cuboid = Cuboid.findById(desc, cuboidID);
        Map<Integer, Dictionary> dictionaryOfThisCuboid = Maps.newHashMap();
        List<DataType> dataTypes = new ArrayList<DataType>(cuboid.getColumns().size() + this.measureNumber);

        int colIndex = 0;
        for (TblColRef col : cuboid.getColumns()) {
            dataTypes.add(col.getType());
            if (this.desc.getRowkey().isUseDictionary(col)) {
                dictionaryOfThisCuboid.put(colIndex, dictionaryMap.get(col));
            }
            colIndex++;
        }

        for (MeasureDesc measure : this.desc.getMeasures()) {
            dataTypes.add(measure.getFunction().getReturnDataType());
        }

        GTInfo.Builder builder = GTInfo.builder();
        builder.setCodeSystem(new GTDictionaryCodeSystem(dictionaryOfThisCuboid));
        builder.setColumns(dataTypes.toArray(new DataType[dataTypes.size()]));
        return builder;
    }


    private Map<TblColRef, Dictionary<?>> buildDictionary(List<List<String>> table, CubeDesc desc) {
        SetMultimap<TblColRef, String> valueMap = HashMultimap.create();

        List<TblColRef> dimColumns = desc.listDimensionColumnsExcludingDerived();
        for (List<String> row : table) {
            for (int i = 0; i < dimColumns.size(); i++) {
                String cell = row.get(i);
                valueMap.put(dimColumns.get(i), cell);
            }
        }
        Map<TblColRef, Dictionary<?>> result = Maps.newHashMap();

        for (DimensionDesc dim : desc.getDimensions()) {
            // dictionary
            for (TblColRef col : dim.getColumnRefs()) {
                if (desc.getRowkey().isUseDictionary(col)) {
                    logger.info("Building dictionary for " + col);
                    result.put(col, DictionaryGenerator.buildDictionaryFromValueList(col.getType(), Collections2.transform(valueMap.get(col), new Function<String, byte[]>() {
                        @Nullable
                        @Override
                        public byte[] apply(String input) {
                            return input.getBytes();
                        }
                    })));
                }
            }
        }

        return result;
    }

    private List<String> parseStream(Stream stream, CubeDesc desc) {
        return Lists.newArrayList(new String(stream.getRawData()).split(","));
    }


}

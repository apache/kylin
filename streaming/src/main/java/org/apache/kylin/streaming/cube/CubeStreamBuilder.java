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
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeJoinedFlatTableDesc;
import org.apache.kylin.cube.model.DimensionDesc;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.dict.DictionaryGenerator;
import org.apache.kylin.metadata.measure.MeasureAggregators;
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
    CuboidScheduler cuboidScheduler = null;
    List<List<String>> table = null;
    Map<TblColRef, Dictionary<?>> dictionaryMap = null;
    private Cuboid baseCuboid = null;
    private CubeInstance cube;
    private CubeSegment cubeSegment = null;
    private CubeJoinedFlatTableDesc intermediateTableDesc;
    private MeasureCodec measureCodec;
    private MeasureAggregators aggs = null;
    private int measureNumber;
    public static final LongWritable ONE = new LongWritable(1l);
    private String[] metricsAggrFuncs = null;

    public CubeStreamBuilder(LinkedBlockingDeque<Stream> queue, String hTableName, CubeDesc desc, int partitionId) {
        super(queue, 10000);
        this.desc = desc;
        this.partitionId = partitionId;
        this.cuboidScheduler = new CuboidScheduler(desc);

        CubeManager cubeManager = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
        List<CubeInstance> cubes = cubeManager.getCubesByDesc(this.desc.getName());
        cube = cubes.get(0);

        measureCodec = new MeasureCodec(desc.getMeasures());
        aggs = new MeasureAggregators(desc.getMeasures());
        measureNumber = desc.getMeasures().size();

        List<String> metricsAggrFuncsList = Lists.newArrayList();
        for (int i = 0, n = desc.getMeasures().size(); i < n; i++) {
            MeasureDesc measureDesc = desc.getMeasures().get(i);
            metricsAggrFuncsList.add(measureDesc.getFunction().getExpression());
        }
        metricsAggrFuncs = metricsAggrFuncsList.toArray(new String[metricsAggrFuncsList.size()]);


    }

    @Override
    protected void build(List<Stream> streamsToBuild) {
        long startTime = System.currentTimeMillis();

        CubeManager cubeManager = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());

        intermediateTableDesc = new CubeJoinedFlatTableDesc(cube.getDescriptor(), null);

        table = Lists.transform(streamsToBuild, new Function<Stream, List<String>>() {
            @Nullable
            @Override
            public List<String> apply(@Nullable Stream input) {
                return parseStream(input, desc);
            }
        });

        // dictionaryMap = buildDictionary(table, desc);

        long baseCuboidId = Cuboid.getBaseCuboidId(desc);

        List<GridTable> result = new LinkedList<GridTable>();
        try {
            calculateCuboid(null, -1l, baseCuboidId, result);
        } catch (IOException e) {
            e.printStackTrace();
        }

        assert result.size() > 0;
        logger.info("Totally " + result.size() + " cuboids be calculated, takes " + (System.currentTimeMillis() - startTime)/1000.0 + " seconds");
    }


    private void calculateCuboid(GridTable parentCuboid, long parentCuboidId, long cuboidId, List<GridTable> result) throws IOException {

        GridTable thisCuboid;
        if (parentCuboidId < 0) {
            thisCuboid = calculateBaseCuboid(this.table, cuboidId);
        } else {
            thisCuboid = aggregateCuboid(parentCuboid, parentCuboidId, cuboidId);
        }

        ArrayList<Long> children = (ArrayList<Long>) cuboidScheduler.getSpanningCuboid(cuboidId);
        Collections.sort(children);
        // making sure the children are sorted in ascending order
        for (Long childId : children) {
            calculateCuboid(thisCuboid, cuboidId, childId, result);
        }

        result.add(thisCuboid);
        logger.info("Cuboid " + cuboidId + " is built.");
      //  outputGT(thisCuboid);
    }

    private void outputGT(GridTable gridTable) throws IOException {
        IGTScanner scanner = gridTable.scan(null, null, null, null);
        for (GTRecord record : scanner) {
           logger.debug(record.toString());
        }
    }

    private GridTable calculateBaseCuboid(List<List<String>> table, long baseCuboidId) throws IOException {

        logger.info("Calculating base cuboid " + baseCuboidId + ", source records number " + table.size() );
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

        IGTScanner scanner = gridTable.scanAndAggregate(null, null, dimensionMetricsBitSet.getFirst(), dimensionMetricsBitSet.getSecond(), metricsAggrFuncs, null);

        GridTable baseCuboidGridTable = newGridTableByCuboidID(baseCuboidId);
        builder = baseCuboidGridTable.rebuild();
        for (GTRecord record : scanner) {
            builder.write(record);
        }
        builder.close();

        return baseCuboidGridTable;
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

        IGTScanner scanner = parentCuboid.scanAndAggregate(null, null, childDimensions, measureColumns, metricsAggrFuncs, null);
        GridTable cuboidGridTable = newGridTableByCuboidID(cuboidId);
        GTBuilder builder = cuboidGridTable.rebuild();
        GTRecord r = new GTRecord(cuboidGridTable.getInfo());

        BitSet allNeededColumns = new BitSet();
        allNeededColumns.or(childDimensions);
        allNeededColumns.or(measureColumns);
        for (GTRecord record : scanner) {
            buildNewGTRecord(record, r, allNeededColumns);
            builder.write(r);
        }
        builder.close();


        return cuboidGridTable;
    }

    private void buildNewGTRecord(GTRecord record, GTRecord newRecord, BitSet dimensions) {
        for (int i = dimensions.nextSetBit(0), index = 0; i >= 0; i = dimensions.nextSetBit(i + 1), index++) {
            newRecord.set(index, record.get(i));
        }
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
            key[i] = serializers[i].valueOf(row.get(intermediateTableDesc.getRowKeyColumnIndexes()[i]));
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
                for (int x : flatTableIdx) {
                    if (value == null)
                        value = row.get(x);
                    else
                        value = value + "," + row.get(x);
                }

                value = measureCodec.getSerializer(i).valueOf(Bytes.toBytes((String) value));
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

        List<DataType> dataTypes = new ArrayList<DataType>(cuboid.getColumns().size() + this.measureNumber);
        for (TblColRef col : cuboid.getColumns()) {
            dataTypes.add(col.getType());
        }
        for (MeasureDesc measure : this.desc.getMeasures()) {
            dataTypes.add(measure.getFunction().getReturnDataType());
        }

        GTInfo.Builder builder = GTInfo.builder();
        builder.setCodeSystem(new GTSampleCodeSystem());
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

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

package org.apache.kylin.engine.mr.steps;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinVersion;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.DimensionRangeInfo;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.cuboid.CuboidUtil;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeJoinedFlatTableEnrich;
import org.apache.kylin.dict.DictionaryGenerator;
import org.apache.kylin.dict.IDictionaryBuilder;
import org.apache.kylin.engine.EngineFactory;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.SerializableConfiguration;
import org.apache.kylin.engine.mr.common.StatisticsDecisionUtil;
import org.apache.kylin.engine.mr.steps.FactDistinctColumnsMapper.CuboidStatCalculator;
import org.apache.kylin.engine.mr.steps.FactDistinctColumnsMapper.DictColDeduper;
import org.apache.kylin.measure.BufferedMeasureCodec;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.apache.kylin.measure.hllc.RegisterType;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FactDistinctColumnsBase {
    private static final Logger logger = LoggerFactory.getLogger(FactDistinctColumnsBase.class);

    private String cubeName;
    private String segmentId;
    private int samplingPercentage;
    private KylinConfig envConfig;

    // map
    protected CubeInstance cube;
    protected CubeSegment cubeSeg;
    protected CubeDesc cubeDesc;
    protected long baseCuboidId;
    protected List<TblColRef> allCols;
    protected CubeJoinedFlatTableEnrich intermediateTableDesc;
    protected int[] columnIndex;
    protected FactDistinctColumnsReducerMapping reducerMapping;
    protected int nRowKey;
    private Integer[][] allCuboidsBitSet = null;
    private HLLCounter[] allCuboidsHLL = null;
    private Long[] cuboidIds;
    private int rowCount = 0;
    private DictColDeduper dictColDeduper;
    private Map<Integer, DimensionRangeInfo> dimensionRangeInfoMap = Maps.newHashMap();
    private CuboidStatCalculator[] cuboidStatCalculators;
    private SelfDefineSortableKey sortableKey = new SelfDefineSortableKey();
    private ByteBuffer tmpbuf;
    private Text outputKey;
    private Text outputValue;
    private Text emptyText;

    // reduce
    public static final String DICT_FILE_POSTFIX = ".rldict";
    public static final String DIMENSION_COL_INFO_FILE_POSTFIX = ".dci";
    private int taskId;
    private boolean isStatistics = false;
    private List<Long> baseCuboidRowCountInMappers;
    protected Map<Long, HLLCounter> cuboidHLLMap = null;
    private TblColRef col = null;
    private long totalRowsBeforeMerge = 0;
    // local build dict
    private boolean buildDictInReducer;
    private IDictionaryBuilder builder;
    private String maxValue = null;
    private String minValue = null;


    public FactDistinctColumnsBase(String cubeName, String segmentId, String metaUrl, SerializableConfiguration conf,
                                   int samplingPercentage) {
        this.cubeName = cubeName;
        this.segmentId = segmentId;
        this.samplingPercentage = samplingPercentage;
        this.envConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(conf, metaUrl);
    }

    public void setupMap() {
        outputKey = new Text();
        outputValue = new Text();
        emptyText = new Text();
        try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig
                .setAndUnsetThreadLocalConfig(envConfig)) {
            cube = CubeManager.getInstance(envConfig).getCube(cubeName);
            cubeSeg = cube.getSegmentById(segmentId);
            cubeDesc = cube.getDescriptor();
            baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
            reducerMapping = new FactDistinctColumnsReducerMapping(cube);
            allCols = reducerMapping.getAllDimDictCols();

            intermediateTableDesc = new CubeJoinedFlatTableEnrich(EngineFactory.getJoinedFlatTableDesc(cubeSeg), cubeDesc);
            columnIndex = new int[allCols.size()];
            for (int i = 0; i < allCols.size(); i++) {
                TblColRef colRef = allCols.get(i);
                int columnIndexOnFlatTbl = intermediateTableDesc.getColumnIndex(colRef);
                columnIndex[i] = columnIndexOnFlatTbl;
            }

            tmpbuf = ByteBuffer.allocate(4096);
            nRowKey = cubeDesc.getRowkey().getRowKeyColumns().length;

            Set<Long> cuboidIdSet = Sets.newHashSet(cubeSeg.getCuboidScheduler().getAllCuboidIds());
            if (StatisticsDecisionUtil.isAbleToOptimizeCubingPlan(cubeSeg)) {
                // For cube planner, for every prebuilt cuboid, its related row count stats should be calculated
                // If the precondition for trigger cube planner phase one is satisfied, we need to calculate row count stats for mandatory cuboids.
                cuboidIdSet.addAll(cubeSeg.getCubeDesc().getMandatoryCuboids());
            }
            cuboidIds = cuboidIdSet.toArray(new Long[cuboidIdSet.size()]);
            allCuboidsBitSet = CuboidUtil.getCuboidBitSet(cuboidIds, nRowKey);

            allCuboidsHLL = new HLLCounter[cuboidIds.length];
            for (int i = 0; i < cuboidIds.length; i++) {
                allCuboidsHLL[i] = new HLLCounter(cubeDesc.getConfig().getCubeStatsHLLPrecision(), RegisterType.DENSE);
            }

            //for KYLIN-2518 backward compatibility
            boolean isUsePutRowKeyToHllNewAlgorithm;
            if (KylinVersion.isBefore200(cubeDesc.getVersion())) {
                isUsePutRowKeyToHllNewAlgorithm = false;
                logger.info("Found KylinVersion : {}. Use old algorithm for cuboid sampling.", cubeDesc.getVersion());
            } else {
                isUsePutRowKeyToHllNewAlgorithm = true;
                logger.info(
                        "Found KylinVersion : {}. Use new algorithm for cuboid sampling. About the details of the new algorithm, please refer to KYLIN-2518",
                        cubeDesc.getVersion());
            }

            int calculatorNum = getStatsThreadNum(cuboidIds.length);
            cuboidStatCalculators = new CuboidStatCalculator[calculatorNum];
            int splitSize = cuboidIds.length / calculatorNum;
            if (splitSize <= 0) {
                splitSize = 1;
            }
            for (int i = 0; i < calculatorNum; i++) {
                HLLCounter[] cuboidsHLLSplit;
                Integer[][] cuboidsBitSetSplit;
                Long[] cuboidIdSplit;
                int start = i * splitSize;
                if (start >= cuboidIds.length) {
                    break;
                }
                int end = (i + 1) * splitSize;
                if (i == calculatorNum - 1) {// last split
                    end = cuboidIds.length;
                }

                cuboidsHLLSplit = Arrays.copyOfRange(allCuboidsHLL, start, end);
                cuboidsBitSetSplit = Arrays.copyOfRange(allCuboidsBitSet, start, end);
                cuboidIdSplit = Arrays.copyOfRange(cuboidIds, start, end);
                CuboidStatCalculator calculator = new CuboidStatCalculator(i,
                        intermediateTableDesc.getRowKeyColumnIndexes(), cuboidIdSplit, cuboidsBitSetSplit,
                        isUsePutRowKeyToHllNewAlgorithm, cuboidsHLLSplit);
                cuboidStatCalculators[i] = calculator;
                calculator.start();
            }

            // setup dict col deduper
            dictColDeduper = new DictColDeduper();
            Set<TblColRef> dictCols = cubeDesc.getAllColumnsNeedDictionaryBuilt();
            for (int i = 0; i < allCols.size(); i++) {
                if (dictCols.contains(allCols.get(i)))
                    dictColDeduper.setIsDictCol(i);
            }
        }
    }

    private int getStatsThreadNum(int cuboidNum) {
        int unitNum = cubeDesc.getConfig().getCuboidNumberPerStatsCalculator();
        if (unitNum <= 0) {
            logger.warn("config from getCuboidNumberPerStatsCalculator() " + unitNum + " is should larger than 0");
            logger.info("Will use single thread for cuboid statistics calculation");
            return 1;
        }

        int maxCalculatorNum = cubeDesc.getConfig().getCuboidStatsCalculatorMaxNumber();
        int calculatorNum = (cuboidNum - 1) / unitNum + 1;
        if (calculatorNum > maxCalculatorNum) {
            calculatorNum = maxCalculatorNum;
        }
        return calculatorNum;
    }

    private int countNewSize(int oldSize, int dataSize) {
        int newSize = oldSize * 2;
        while (newSize < dataSize) {
            newSize = newSize * 2;
        }
        return newSize;
    }

    private void writeFieldValue(DataType type, Integer colIndex, String value, Visitor visitor) {
        int reducerIndex = reducerMapping.getReducerIdForCol(colIndex, value);
        tmpbuf.clear();
        byte[] valueBytes = Bytes.toBytes(value);
        int size = valueBytes.length + 1;
        if (size >= tmpbuf.capacity()) {
            tmpbuf = ByteBuffer.allocate(countNewSize(tmpbuf.capacity(), size));
        }
        tmpbuf.put(Bytes.toBytes(reducerIndex)[3]);
        tmpbuf.put(valueBytes);
        outputKey.set(tmpbuf.array(), 0, tmpbuf.position());
        sortableKey.init(outputKey, type);
        visitor.collect(null, sortableKey, emptyText, null);
        // log a few rows for troubleshooting
        if (rowCount < 10) {
            logger.info("Sample output: " + allCols.get(colIndex) + " '" + value + "' => reducer " + reducerIndex);
        }
    }

    private void putRowKeyToHLL(String[] row) {
        for (CuboidStatCalculator cuboidStatCalculator : cuboidStatCalculators) {
            cuboidStatCalculator.putRow(row);
        }
    }

    public long countSizeInBytes(String[] row) {
        int size = 0;
        for (String s : row) {
            size += s == null ? 1 : StringUtil.utf8Length(s);
            size++; // delimiter
        }
        return size;
    }

    public void map(String[] row, Visitor visitor) {
        for (int i = 0; i < allCols.size(); i++) {
            int colIndex = columnIndex[i];
            int rowSize = row.length;
            String fieldValue = " ";
            if (colIndex <= rowSize - 1) {
                fieldValue = row[colIndex];
            } else {
                logger.debug("colIndex:" + colIndex + " is more than rowSize: " + rowSize + " -1, so set empty value.");
            }
            if (fieldValue == null)
                continue;

            final DataType type = allCols.get(i).getType();

            //for dic column, de dup before write value; for dim not dic column, hold util doCleanup()
            if (dictColDeduper.isDictCol(i)) {
                if (dictColDeduper.add(i, fieldValue)) {
                    writeFieldValue(type, i, fieldValue, visitor);
                }
            } else {
                DimensionRangeInfo old = dimensionRangeInfoMap.get(i);
                if (old == null) {
                    old = new DimensionRangeInfo(fieldValue, fieldValue);
                    dimensionRangeInfoMap.put(i, old);
                } else {
                    old.setMax(type.getOrder().max(old.getMax(), fieldValue));
                    old.setMin(type.getOrder().min(old.getMin(), fieldValue));
                }
            }

            if (rowCount % 100 < samplingPercentage) {
                putRowKeyToHLL(row);
            }

            if (rowCount % 100 == 0) {
                dictColDeduper.resetIfShortOfMem();
            }

            rowCount++;
        }
    }

    public void postMap(Visitor visitor) throws IOException {
        ByteBuffer hllBuf = ByteBuffer.allocate(BufferedMeasureCodec.DEFAULT_BUFFER_SIZE);
        // output each cuboid's hll to reducer, key is 0 - cuboidId
        for (CuboidStatCalculator cuboidStatCalculator : cuboidStatCalculators) {
            cuboidStatCalculator.waitForCompletion();
        }
        for (CuboidStatCalculator cuboidStatCalculator : cuboidStatCalculators) {
            Long[] cuboidIds = cuboidStatCalculator.getCuboidIds();
            HLLCounter[] cuboidsHLL = cuboidStatCalculator.getHLLCounters();
            HLLCounter hll;

            for (int i = 0; i < cuboidIds.length; i++) {
                hll = cuboidsHLL[i];
                tmpbuf.clear();
                tmpbuf.put((byte) FactDistinctColumnsReducerMapping.MARK_FOR_HLL_COUNTER); // one byte
                tmpbuf.putLong(cuboidIds[i]);
                outputKey.set(tmpbuf.array(), 0, tmpbuf.position());
                hllBuf.clear();
                hll.writeRegisters(hllBuf);
                outputValue.set(hllBuf.array(), 0, hllBuf.position());
                sortableKey.init(outputKey, (byte) 0);
                visitor.collect(null, sortableKey, outputValue, null);
            }
        }
        for (Integer colIndex : dimensionRangeInfoMap.keySet()) {
            DimensionRangeInfo rangeInfo = dimensionRangeInfoMap.get(colIndex);
            DataType dataType = allCols.get(colIndex).getType();
            writeFieldValue(dataType, colIndex, rangeInfo.getMin(), visitor);
            writeFieldValue(dataType, colIndex, rangeInfo.getMax(), visitor);
        }
    }


    public void setupReduce(int taskId) throws IOException {
        this.taskId = taskId;
        try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig
                .setAndUnsetThreadLocalConfig(envConfig)) {
            cube = CubeManager.getInstance(envConfig).getCube(cubeName);
            cubeDesc = cube.getDescriptor();
            reducerMapping = new FactDistinctColumnsReducerMapping(cube);
            logger.info("reducer no " + taskId + ", role play " + reducerMapping.getRolePlayOfReducer(taskId));

            if (reducerMapping.isCuboidRowCounterReducer(taskId)) {
                // hll
                isStatistics = true;
                baseCuboidId = cube.getCuboidScheduler().getBaseCuboidId();
                baseCuboidRowCountInMappers = Lists.newArrayList();
                cuboidHLLMap = Maps.newHashMap();
                logger.info("Reducer " + taskId + " handling stats");
            } else {
                // normal col
                col = reducerMapping.getColForReducer(taskId);
                Preconditions.checkNotNull(col);

                // local build dict
                buildDictInReducer = envConfig.isBuildDictInReducerEnabled();
                if (cubeDesc.getDictionaryBuilderClass(col) != null) { // only works with default dictionary builder
                    buildDictInReducer = false;
                }
                if (reducerMapping.getReducerNumForDimCol(col) > 1) {
                    buildDictInReducer = false; // only works if this is the only reducer of a dictionary column
                }
                if (buildDictInReducer) {
                    builder = DictionaryGenerator.newDictionaryBuilder(col.getType());
                    builder.init(null, 0, null);
                }
                logger.info("Reducer " + taskId + " handling column " + col + ", buildDictInReducer=" + buildDictInReducer);
            }
        }
    }

    public void reduce(Pair<SelfDefineSortableKey, Text> kv, Visitor visitor) throws IOException {
        if (isStatistics) {
            // for hll
            long cuboidId = Bytes.toLong(kv.getFirst().getText().getBytes(), 1, Bytes.SIZEOF_LONG);
            HLLCounter hll = new HLLCounter(cubeDesc.getConfig().getCubeStatsHLLPrecision());
            ByteBuffer bf = ByteBuffer.wrap(kv.getSecond().getBytes(), 0, kv.getSecond().getLength());
            hll.readRegisters(bf);

            totalRowsBeforeMerge += hll.getCountEstimate();

            if (cuboidId == baseCuboidId) {
                baseCuboidRowCountInMappers.add(hll.getCountEstimate());
            }

            if (cuboidHLLMap.get(cuboidId) != null) {
                cuboidHLLMap.get(cuboidId).merge(hll);
            } else {
                cuboidHLLMap.put(cuboidId, hll);
            }
        } else {
            String value = Bytes.toString(kv.getFirst().getText().getBytes(), 1, kv.getFirst().getText().getLength() - 1);
            logAFewRows(value);
            // if dimension col, compute max/min value
            if (cubeDesc.listDimensionColumnsExcludingDerived(true).contains(col) &&
                    col.getType().needCompare()) {
                if (minValue == null || col.getType().compare(minValue, value) > 0) {
                    minValue = value;
                }
                if (maxValue == null || col.getType().compare(maxValue, value) < 0) {
                    maxValue = value;
                }
            }

            // if dict column
            if (cubeDesc.getAllColumnsNeedDictionaryBuilt().contains(col)) {
                if (buildDictInReducer) {
                    builder.addValue(value);
                } else {
                    byte[] keyBytes = Bytes.copy(kv.getFirst().getText().getBytes(), 1, kv.getFirst().getText().getLength() - 1);
                    // output written to baseDir/colName/-r-00000 (etc)
                    String fileName = col.getIdentity() + "/";
                    visitor.collect(BatchConstants.CFG_OUTPUT_COLUMN, NullWritable.get(), new Text(keyBytes), fileName);
                }
            }
            rowCount++;
        }
    }

    public void postReduce(Visitor visitor) throws IOException {
        if (isStatistics) {
            //output the hll info;
            List<Long> allCuboids = Lists.newArrayList();
            allCuboids.addAll(cuboidHLLMap.keySet());
            Collections.sort(allCuboids);

            logMapperAndCuboidStatistics(allCuboids); // for human check
            outputStatistics(allCuboids, visitor);
        } else {
            // dimension col
            if (cubeDesc.listDimensionColumnsExcludingDerived(true).contains(col)) {
                outputDimRangeInfo(visitor);
            }
            // dic col
            if (buildDictInReducer) {
                try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig
                        .setAndUnsetThreadLocalConfig(envConfig)) {
                    Dictionary<String> dict = builder.build();
                    outputDict(col, dict, visitor);
                }
            }
        }
    }

    private void logAFewRows(String value) {
        if (rowCount < 10) {
            logger.info("Received value: " + value);
        }
    }

    private void outputDimRangeInfo(Visitor visitor) throws IOException {
        if (col != null && minValue != null) {
            // output written to baseDir/colName/colName.dci-r-00000 (etc)
            String dimRangeFileName = col.getIdentity() + "/" + col.getName() + DIMENSION_COL_INFO_FILE_POSTFIX;

            visitor.collect(BatchConstants.CFG_OUTPUT_PARTITION, NullWritable.get(),
                    new Text(minValue.getBytes(StandardCharsets.UTF_8)), dimRangeFileName);
            visitor.collect(BatchConstants.CFG_OUTPUT_PARTITION, NullWritable.get(),
                    new Text(maxValue.getBytes(StandardCharsets.UTF_8)), dimRangeFileName);
            logger.info("write dimension range info for col : " + col.getName() + "  minValue:" + minValue
                    + " maxValue:" + maxValue);
        }
    }

    private void outputDict(TblColRef col, Dictionary<String> dict, Visitor visitor) throws IOException {
        // output written to baseDir/colName/colName.rldict-r-00000 (etc)
        String dictFileName = col.getIdentity() + "/" + col.getName() + DICT_FILE_POSTFIX;

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream outputStream = new DataOutputStream(baos)) {
            outputStream.writeUTF(dict.getClass().getName());
            dict.write(outputStream);

            visitor.collect(BatchConstants.CFG_OUTPUT_DICT, NullWritable.get(),
                    new ArrayPrimitiveWritable(baos.toByteArray()), dictFileName);
        }
    }

    private void outputStatistics(List<Long> allCuboids, Visitor visitor) throws IOException {
        // output written to baseDir/statistics/statistics-r-00000 (etc)
        String statisticsFileName = BatchConstants.CFG_OUTPUT_STATISTICS + "/" + BatchConstants.CFG_OUTPUT_STATISTICS;

        ByteBuffer valueBuf = ByteBuffer.allocate(BufferedMeasureCodec.DEFAULT_BUFFER_SIZE);

        // mapper overlap ratio at key -1
        long grandTotal = 0;
        for (HLLCounter hll : cuboidHLLMap.values()) {
            grandTotal += hll.getCountEstimate();
        }
        double mapperOverlapRatio = grandTotal == 0 ? 0 : (double) totalRowsBeforeMerge / grandTotal;
        visitor.collect(BatchConstants.CFG_OUTPUT_STATISTICS, new LongWritable(-1),
                new BytesWritable(Bytes.toBytes(mapperOverlapRatio)), statisticsFileName);

        // mapper number at key -2
        visitor.collect(BatchConstants.CFG_OUTPUT_STATISTICS, new LongWritable(-2),
                new BytesWritable(Bytes.toBytes(baseCuboidRowCountInMappers.size())), statisticsFileName);

        // sampling percentage at key 0
        visitor.collect(BatchConstants.CFG_OUTPUT_STATISTICS, new LongWritable(0L),
                new BytesWritable(Bytes.toBytes(samplingPercentage)), statisticsFileName);

        for (long i : allCuboids) {
            valueBuf.clear();
            cuboidHLLMap.get(i).writeRegisters(valueBuf);
            valueBuf.flip();
            visitor.collect(BatchConstants.CFG_OUTPUT_STATISTICS, new LongWritable(i),
                    new BytesWritable(valueBuf.array(), valueBuf.limit()), statisticsFileName);
        }
    }

    private void logMapperAndCuboidStatistics(List<Long> allCuboids) throws IOException {
        logger.info("Cuboid number for task: " + taskId + "\t" + allCuboids.size());
        logger.info("Samping percentage: \t" + samplingPercentage);
        logger.info("The following statistics are collected based on sampling data.");
        logger.info("Number of Mappers: " + baseCuboidRowCountInMappers.size());

        for (int i = 0; i < baseCuboidRowCountInMappers.size(); i++) {
            if (baseCuboidRowCountInMappers.get(i) > 0) {
                logger.info("Base Cuboid in Mapper " + i + " row count: \t " + baseCuboidRowCountInMappers.get(i));
            }
        }

        long grantTotal = 0;
        for (long i : allCuboids) {
            grantTotal += cuboidHLLMap.get(i).getCountEstimate();
            logger.info("Cuboid " + i + " row count is: \t " + cuboidHLLMap.get(i).getCountEstimate());
        }

        logger.info("Sum of row counts (before merge) is: \t " + totalRowsBeforeMerge);
        logger.info("After merge, the row count: \t " + grantTotal);
    }

    public abstract static class Visitor<K, V> {
        public abstract void collect(String namedOutput, K key, V value, String outputPath);
    }
}

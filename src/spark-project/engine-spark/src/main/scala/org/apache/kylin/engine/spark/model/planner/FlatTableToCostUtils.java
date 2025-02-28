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

package org.apache.kylin.engine.spark.model.planner;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.engine.spark.model.SegmentFlatTableDesc;
import org.apache.kylin.measure.BufferedMeasureCodec;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.apache.kylin.measure.hllc.RegisterType;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.RuleBasedIndex;
import org.apache.kylin.metadata.cube.planner.CostBasePlannerUtils;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Row;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import lombok.extern.slf4j.Slf4j;
import scala.Tuple2;

@Slf4j
public class FlatTableToCostUtils {

    private static LayoutEntity createMockRuleBaseLayout(RuleBasedIndex ruleBasedIndex) {
        // create a mock base layout which include all dimensions in the rule index without order
        LayoutEntity baseLayouts = new LayoutEntity();
        baseLayouts.setColOrder(ruleBasedIndex.getDimensions());
        return baseLayouts;
    }

    public static Map<BigInteger, HLLCounter> generateCost(JavaRDD<Row> input, KylinConfig kylinConfig,
            RuleBasedIndex ruleBasedIndex, SegmentFlatTableDesc flatTableDesc) throws IOException {
        // step1: convert each cell to string data type, and get RDD[String[]]
        JavaRDD<String[]> flatTableRDD = input.map(new Function<Row, String[]>() {
            @Override
            public String[] call(Row row) throws Exception {
                String[] result = new String[row.length()];
                for (int i = 0; i < row.length(); i++) {
                    final Object o = row.get(i);
                    if (o != null) {
                        result[i] = o.toString();
                    } else {
                        result[i] = null;
                    }
                }
                return result;
            }
        });
        // step2: calculate the cost for each partition, and get the new RDD.
        // The key is cuboid, and the value is the data encoded from the hll for each partition.
        int rowKeyCount = ruleBasedIndex.countOfIncludeDimension();
        // layouts from the rule index(agg group)
        Set<LayoutEntity> inputLayouts = ruleBasedIndex.genCuboidLayouts();
        // add the mock layout which include all of the dimensions in the rule index
        inputLayouts.add(createMockRuleBaseLayout(ruleBasedIndex));
        BigInteger[] inputCuboids = getCuboIdsFromLayouts(Lists.newArrayList(inputLayouts), rowKeyCount,
                ruleBasedIndex.getColumnIdToRowKeyId());
        // rowkey id ->  column index in the flat table of the flat dataset.
        int[] rowkeyColumnIndexes = getRowkeyColumnIndexes(ruleBasedIndex, flatTableDesc);

        int hllPrecision = kylinConfig.getStatsHLLPrecision();
        log.info("The row key count is {}, and the index/column map is {}", rowKeyCount,
                Lists.newArrayList(rowkeyColumnIndexes));
        JavaPairRDD<BigInteger, byte[]> costRddByPartition = flatTableRDD.mapPartitionsToPair(
                new FlatOutputFunction(hllPrecision, rowKeyCount, inputCuboids, rowkeyColumnIndexes));

        // step3: reduce by cuboid, and merge hll data
        // The key is the cuboid, the value is data encoded from the hll
        int partitionNum = getCuboidHLLCounterReducerNum(inputCuboids.length, kylinConfig);
        log.info("Get the partition count for the HLL reducer: {}", partitionNum);
        JavaPairRDD<BigInteger, byte[]> costRDD = costRddByPartition.reduceByKey(new Partitioner() {
            private int num = partitionNum;
            private BigInteger bigIntegerMod = BigInteger.valueOf(num);

            @Override
            public int numPartitions() {
                return num;
            }

            @Override
            public int getPartition(Object key) {
                // key is the biginteger
                BigInteger value = (BigInteger) key;
                return value.mod(bigIntegerMod).intValue();
            }
        }, new Function2<byte[], byte[], byte[]>() {
            private int precision = hllPrecision;

            @Override
            public byte[] call(byte[] array1, byte[] array2) throws Exception {
                // hll1
                HLLCounter hll1 = new HLLCounter(precision);
                ByteBuffer buffer1 = ByteBuffer.wrap(array1, 0, array1.length);
                hll1.readRegisters(buffer1);
                // hll2
                HLLCounter hll2 = new HLLCounter(precision);
                ByteBuffer buffer2 = ByteBuffer.wrap(array2, 0, array2.length);
                hll2.readRegisters(buffer2);

                // merge two hll
                hll1.merge(hll2);
                ByteBuffer hllBuf = ByteBuffer.allocate(BufferedMeasureCodec.DEFAULT_BUFFER_SIZE);
                hll1.writeRegisters(hllBuf);
                byte[] value = new byte[hllBuf.position()];
                System.arraycopy(hllBuf.array(), 0, value, 0, hllBuf.position());
                return value;
            }
        });
        // step4: collect the final result, and convert value(text) to hll
        // The key is the cuboid, and the value is the estimated statistics
        Map<BigInteger, HLLCounter> resultCost = Maps.newHashMap();
        for (Tuple2<BigInteger, byte[]> pair : costRDD.collect()) {
            HLLCounter hll = new HLLCounter(kylinConfig.getStatsHLLPrecision());
            // value
            ByteArray byteArray = new ByteArray(pair._2);
            hll.readRegisters(byteArray.asBuffer());
            // put key and value
            resultCost.put(pair._1, hll);
        }
        // log data
        if (log.isDebugEnabled()) {
            logMapperAndCuboidStatistics(resultCost, 100);
        }
        return resultCost;
    }

    /**
     * @return reducer number for calculating hll
     */
    private static int getCuboidHLLCounterReducerNum(int nCuboids, KylinConfig kylinConfig) {
        int shardBase = (nCuboids - 1) / kylinConfig.getJobPerReducerHLLCuboidNumber() + 1;
        int hllMaxReducerNumber = kylinConfig.getJobHLLMaxReducerNumber();
        if (shardBase > hllMaxReducerNumber) {
            shardBase = hllMaxReducerNumber;
        }
        return Math.max(shardBase, 1);
    }

    private static BigInteger[] getCuboIdsFromLayouts(List<LayoutEntity> allLayouts, int dimensionCount,
            Map<Integer, Integer> columnIdToRowkeyId) {
        Set<BigInteger> set = new HashSet<>();
        for (LayoutEntity layoutEntity : allLayouts) {
            BigInteger cuboId = CostBasePlannerUtils.convertDimensionsToCuboId(layoutEntity.getDimsIds(),
                    dimensionCount, columnIdToRowkeyId);
            set.add(cuboId);
        }
        return set.toArray(new BigInteger[set.size()]);
    }

    private static int[] getRowkeyColumnIndexes(RuleBasedIndex ruleBasedIndex, SegmentFlatTableDesc flatTableDesc) {
        int rowKeyCount = ruleBasedIndex.countOfIncludeDimension();
        List<Integer> columnIds = flatTableDesc.getColumnIds();
        int[] rowkeyColumnIndexes = new int[rowKeyCount];
        Map<Integer, Integer> rowkeyIdToColumnId = ruleBasedIndex.getRowKeyIdToColumnId();
        for (int rowkeyId = 0; rowkeyId < rowKeyCount; rowkeyId++) {
            if (!rowkeyIdToColumnId.containsKey(rowkeyId)) {
                throw new RuntimeException("Can't find the column id from the rowkey id");
            }
            int columnId = rowkeyIdToColumnId.get(rowkeyId);
            int index = columnIds.indexOf(columnId);
            if (index >= 0) {
                // find the i-th dimension in the index-th column in the flat table or flat data set
                rowkeyColumnIndexes[rowkeyId] = index;
            } else {
                // not find the column id
                throw new RuntimeException(
                        String.format(Locale.ROOT, "Can't find the column id %d, column ids %s", columnId, columnIds));
            }
        }
        return rowkeyColumnIndexes;
    }

    private static class FlatOutputFunction implements PairFlatMapFunction<Iterator<String[]>, BigInteger, byte[]> {
        private transient volatile boolean initialized = false;
        private transient CuboidStatCalculator cuboidStatCalculator;
        private final int samplingPercent = 100;
        private final int hllPrecision;
        private final int rowKeyCount;
        private final BigInteger[] cuboidIds;
        private final int[] rowkeyColumnIndexes;

        public FlatOutputFunction(int hllPrecision, int rowKeyCount, BigInteger[] cuboidIds,
                int[] rowkeyColumnIndexes) {
            this.hllPrecision = hllPrecision;
            this.rowKeyCount = rowKeyCount;
            this.cuboidIds = cuboidIds;
            this.rowkeyColumnIndexes = rowkeyColumnIndexes;
        }

        private Integer[][] getCuboidBitSet(BigInteger[] cuboidIds, int nRowKey) {
            Integer[][] allCuboidsBitSet = new Integer[cuboidIds.length][];
            for (int j = 0; j < cuboidIds.length; j++) {
                BigInteger cuboidId = cuboidIds[j];

                allCuboidsBitSet[j] = new Integer[cuboidId.bitCount()];
                int position = 0;
                for (int i = 0; i < nRowKey; i++) {
                    BigInteger bigMask = BigInteger.ZERO.setBit(nRowKey - 1 - i);
                    if ((bigMask.and(cuboidId).compareTo(BigInteger.ZERO)) > 0) {
                        // bigMask & cuboid > 0
                        allCuboidsBitSet[j][position] = i;
                        position++;
                    }
                }
            }
            return allCuboidsBitSet;
        }

        private HLLCounter[] getInitCuboidsHLL(int cuboidSize, int hllPrecision) {
            HLLCounter[] cuboidsHLL = new HLLCounter[cuboidSize];
            for (int i = 0; i < cuboidSize; i++) {
                cuboidsHLL[i] = new HLLCounter(hllPrecision, RegisterType.DENSE);
            }
            return cuboidsHLL;
        }

        private void init() {
            Integer[][] cuboidsBitSet = getCuboidBitSet(cuboidIds, rowKeyCount);
            HLLCounter[] cuboidsHLL = getInitCuboidsHLL(cuboidIds.length, hllPrecision);
            cuboidStatCalculator = new CuboidStatCalculator(rowkeyColumnIndexes, cuboidIds, cuboidsBitSet, true,
                    cuboidsHLL);
            initialized = true;
        }

        @Override
        public Iterator<Tuple2<BigInteger, byte[]>> call(Iterator<String[]> iterator) throws Exception {
            if (initialized == false) {
                // just sync this object
                synchronized (this) {
                    if (initialized == false) {
                        init();
                    }
                }
            }

            // One tuple is a cost pair, the left is the cuboid and the right is the cost
            int rowCount = 0;
            while (iterator.hasNext()) {
                String[] row = iterator.next();
                if (rowCount % 100 < samplingPercent) {
                    cuboidStatCalculator.putRow(row);
                }
                rowCount++;
            }

            List<Tuple2<BigInteger, byte[]>> result = Lists.newArrayList();
            ByteBuffer hllBuf = ByteBuffer.allocate(BufferedMeasureCodec.DEFAULT_BUFFER_SIZE);
            BigInteger[] cuboidIds = cuboidStatCalculator.getCuboidIds();
            HLLCounter[] cuboidsHLL = cuboidStatCalculator.getHLLCounters();
            HLLCounter hll;
            for (int i = 0; i < cuboidIds.length; i++) {
                // key
                BigInteger outputKey = cuboidIds[i];
                // value
                hll = cuboidsHLL[i];
                hllBuf.clear();
                hll.writeRegisters(hllBuf);
                byte[] value = new byte[hllBuf.position()];
                System.arraycopy(hllBuf.array(), 0, value, 0, hllBuf.position());
                result.add(new Tuple2<BigInteger, byte[]>(outputKey, value));
            }
            return result.iterator();
        }
    }

    static class CuboidStatCalculator {
        private final int nRowKey;
        private final int[] rowkeyColIndex;
        private final BigInteger[] cuboidIds;
        private final Integer[][] cuboidsBitSet;
        private HLLCounter[] cuboidsHLL;

        //about details of the new algorithm, please see KYLIN-2518
        private final boolean isNewAlgorithm;
        private final HashFunction hf;
        private long[] rowHashCodesLong;

        public CuboidStatCalculator(int[] rowkeyColIndex, BigInteger[] cuboidIds, Integer[][] cuboidsBitSet,
                boolean isUsePutRowKeyToHllNewAlgorithm, HLLCounter[] cuboidsHLL) {
            this.nRowKey = rowkeyColIndex.length;
            this.rowkeyColIndex = rowkeyColIndex;
            this.cuboidIds = cuboidIds;
            this.cuboidsBitSet = cuboidsBitSet;
            this.isNewAlgorithm = isUsePutRowKeyToHllNewAlgorithm;
            if (!isNewAlgorithm) {
                this.hf = Hashing.murmur3_32();
            } else {
                rowHashCodesLong = new long[nRowKey];
                this.hf = Hashing.murmur3_128();
            }
            this.cuboidsHLL = cuboidsHLL;
        }

        public void putRow(final String[] row) {
            String[] copyRow = Arrays.copyOf(row, row.length);

            if (isNewAlgorithm) {
                putRowKeyToHLLNew(copyRow);
            } else {
                putRowKeyToHLLOld(copyRow);
            }
        }

        private void putRowKeyToHLLOld(String[] row) {
            //generate hash for each row key column
            byte[][] rowHashCodes = new byte[nRowKey][];
            for (int i = 0; i < nRowKey; i++) {
                Hasher hc = hf.newHasher();
                String colValue = row[rowkeyColIndex[i]];
                if (colValue != null) {
                    rowHashCodes[i] = hc.putString(colValue).hash().asBytes();
                } else {
                    rowHashCodes[i] = hc.putInt(0).hash().asBytes();
                }
            }

            // user the row key column hash to get a consolidated hash for each cuboid
            for (int i = 0, n = cuboidsBitSet.length; i < n; i++) {
                Hasher hc = hf.newHasher();
                for (int position = 0; position < cuboidsBitSet[i].length; position++) {
                    hc.putBytes(rowHashCodes[cuboidsBitSet[i][position]]);
                }

                cuboidsHLL[i].add(hc.hash().asBytes());
            }
        }

        private void putRowKeyToHLLNew(String[] row) {
            //generate hash for each row key column
            for (int i = 0; i < nRowKey; i++) {
                Hasher hc = hf.newHasher();
                String colValue = row[rowkeyColIndex[i]];
                if (colValue == null)
                    colValue = "0";
                byte[] bytes = hc.putString(colValue).hash().asBytes();
                //add column ordinal to the hash value to distinguish between (a,b) and (b,a)
                rowHashCodesLong[i] = (Bytes.toLong(bytes) + i);
            }

            // user the row key column hash to get a consolidated hash for each cuboid
            for (int i = 0, n = cuboidsBitSet.length; i < n; i++) {
                long value = 0;
                for (int position = 0; position < cuboidsBitSet[i].length; position++) {
                    value += rowHashCodesLong[cuboidsBitSet[i][position]];
                }
                cuboidsHLL[i].addHashDirectly(value);
            }
        }

        public HLLCounter[] getHLLCounters() {
            return cuboidsHLL;
        }

        public BigInteger[] getCuboidIds() {
            return cuboidIds;
        }
    }

    private static void logMapperAndCuboidStatistics(Map<BigInteger, HLLCounter> cuboidHLLMap, int samplingPercentage) {
        log.debug("Total cuboid number: \t" + cuboidHLLMap.size());
        log.debug("Sampling percentage: \t" + samplingPercentage);
        log.debug("The following statistics are collected based on sampling data.");

        List<BigInteger> allCuboids = Lists.newArrayList(cuboidHLLMap.keySet());
        Collections.sort(allCuboids);
        for (BigInteger i : allCuboids) {
            log.debug("Cuboid " + i + " row count is: \t " + cuboidHLLMap.get(i).getCountEstimate());
        }
    }

    public static Map<BigInteger, Long> getCuboidRowCountMapFromSampling(Map<BigInteger, HLLCounter> hllcMap) {
        Map<BigInteger, Long> cuboidRowCountMap = Maps.newHashMap();
        for (Map.Entry<BigInteger, HLLCounter> entry : hllcMap.entrySet()) {
            // No need to adjust according sampling percentage. Assumption is that data set is far
            // more than cardinality. Even a percentage of the data should already see all cardinalities.
            cuboidRowCountMap.put(entry.getKey(), entry.getValue().getCountEstimate());
        }
        return cuboidRowCountMap;
    }

    private static Map<BigInteger, Double> getCuboidSizeMapFromSamplingByCount(Map<BigInteger, Long> rowCountMap,
            long sourceCount, RuleBasedIndex ruleBasedIndex, KylinConfig kylinConfig,
            SegmentFlatTableDesc segmentFlatTableDesc) {
        // use the row count to replace the size
        Map<BigInteger, Double> cuboidSizeMap = Maps.newHashMap();
        for (Map.Entry<BigInteger, Long> entry : rowCountMap.entrySet()) {
            double value = entry.getValue();
            cuboidSizeMap.put(entry.getKey(), value);
        }
        return cuboidSizeMap;
    }

    public static Map<BigInteger, Double> getCuboidSizeMapFromSampling(Map<BigInteger, Long> rowCountMap,
            long sourceCount, RuleBasedIndex ruleBasedIndex, KylinConfig kylinConfig,
            SegmentFlatTableDesc segmentFlatTableDesc) {
        // replace the size with the row count
        return getCuboidSizeMapFromSamplingByCount(rowCountMap, sourceCount, ruleBasedIndex, kylinConfig,
                segmentFlatTableDesc);
    }

    private static List<Integer> getRowkeyColumnSize(IndexPlan indexPlan, SegmentFlatTableDesc flatTableDesc) {
        int rowKeyCount = indexPlan.getRuleBasedIndex().countOfIncludeDimension();
        List<Integer> columnIds = flatTableDesc.getColumnIds();
        List<TblColRef> tblColRefs = flatTableDesc.getColumns();
        List<Integer> rowkeyColumnSize = Lists.newArrayList();

        for (int i = 0; i < rowKeyCount; i++) {
            int index = columnIds.indexOf(i);
            if (index >= 0) {
                // find the i-th dimension in the index-th column in the flat table or flat data set.
                TblColRef tblColRef = tblColRefs.get(index);
                // get DimensionEncoding for this table column ref.
                // Noe we just use the row count as the weight
                int length = 0;
                rowkeyColumnSize.add(length);
            } else {
                // not find the column id
                throw new RuntimeException(
                        String.format(Locale.ROOT, "Can't find the column id %d, column ids %s", i, columnIds));
            }
        }
        // the index means column id for the dimension, the value is the estimated size for this dimension
        return rowkeyColumnSize;
    }

    private static double estimateCuboidStorageSize(Set<NDataModel.Measure> measureDescs, long cuboidId, long rowCount,
            long baseCuboidId, long baseCuboidCount, List<Integer> rowKeyColumnLength, long sourceRowCount,
            KylinConfig kylinConfig) {
        // row key header
        int rowkeyLength = 8; // 8 or 10
        long mask = Long.highestOneBit(baseCuboidId);
        // actual: the parentCuboidIdActualLength is rowkey count
        long parentCuboidIdActualLength = (long) Long.SIZE - Long.numberOfLeadingZeros(baseCuboidId);
        // dimension length
        for (int i = 0; i < parentCuboidIdActualLength; i++) {
            if ((mask & cuboidId) > 0) {
                rowkeyLength += rowKeyColumnLength.get(i); //colIO.getColumnLength(columnList.get(i));
            }
            mask = mask >> 1;
        }
        // measure size
        int normalSpace = rowkeyLength;
        int countDistinctSpace = 0;
        double percentileSpace = 0;
        int topNSpace = 0;
        for (MeasureDesc measureDesc : measureDescs) {
            if (rowCount == 0)
                break;
            DataType returnType = measureDesc.getFunction().getReturnDataType();
            if (measureDesc.getFunction().getExpression().equals(FunctionDesc.FUNC_COUNT_DISTINCT)) {
                long estimateDistinctCount = sourceRowCount / rowCount;
                estimateDistinctCount = estimateDistinctCount == 0 ? 1L : estimateDistinctCount;
                countDistinctSpace += returnType.getStorageBytesEstimate(estimateDistinctCount);
            } else if (measureDesc.getFunction().getExpression().equals(FunctionDesc.FUNC_PERCENTILE)) {
                percentileSpace += returnType.getStorageBytesEstimate(baseCuboidCount * 1.0 / rowCount);
            } else if (measureDesc.getFunction().getExpression().equals(FunctionDesc.FUNC_TOP_N)) {
                long estimateTopNCount = sourceRowCount / rowCount;
                estimateTopNCount = estimateTopNCount == 0 ? 1L : estimateTopNCount;
                topNSpace += returnType.getStorageBytesEstimate(estimateTopNCount);
            } else {
                normalSpace += returnType.getStorageBytesEstimate();
            }
        }

        double cuboidSizeRatio = kylinConfig.getJobCuboidSizeRatio();
        double cuboidSizeMemHungryRatio = kylinConfig.getJobCuboidSizeCountDistinctRatio();
        double cuboidSizeTopNRatio = kylinConfig.getJobCuboidSizeTopNRatio();

        double ret = (1.0 * normalSpace * rowCount * cuboidSizeRatio
                + 1.0 * countDistinctSpace * rowCount * cuboidSizeMemHungryRatio + 1.0 * percentileSpace * rowCount
                + 1.0 * topNSpace * rowCount * cuboidSizeTopNRatio) / (1024L * 1024L);
        return ret;
    }
}

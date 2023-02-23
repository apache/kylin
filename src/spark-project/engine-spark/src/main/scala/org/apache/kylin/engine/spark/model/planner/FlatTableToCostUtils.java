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
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.engine.spark.model.SegmentFlatTableDesc;
import org.apache.kylin.measure.BufferedMeasureCodec;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.apache.kylin.measure.hllc.RegisterType;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.RuleBasedIndex;
import org.apache.kylin.metadata.cube.planner.CostBasePlannerUtils;
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
        JavaRDD<String[]> flatTableRDD = input.map((Function<Row, String[]>) row -> {
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
        });
        // step2: calculate the cost for each partition, and get the new RDD.
        // The key is layout, and the value is the data encoded from the hll for each partition.
        int rowKeyCount = ruleBasedIndex.countOfIncludeDimension();
        // layouts from the rule index(agg group)
        Set<LayoutEntity> ruleBasedLayouts = ruleBasedIndex.genCuboidLayouts();
        // add the mock layout which include all the dimensions in the rule index
        ruleBasedLayouts.add(createMockRuleBaseLayout(ruleBasedIndex));
        BigInteger[] inputLayoutIds = getLayoutIdsFromEntity(Lists.newArrayList(ruleBasedLayouts), rowKeyCount,
                ruleBasedIndex.getColumnIdToRowKeyId());
        // rowkey id ->  column index in the flat table of the flat dataset.
        int[] rowkeyColumnIndexes = getRowkeyColumnIndexes(ruleBasedIndex, flatTableDesc);

        int hllPrecision = kylinConfig.getStatsHLLPrecision();
        log.info("The row key count is {}, and the index/column map is {}", rowKeyCount,
                Lists.newArrayList(rowkeyColumnIndexes));
        JavaPairRDD<BigInteger, byte[]> costRddByPartition = flatTableRDD.mapPartitionsToPair(
                new FlatOutputFunction(hllPrecision, rowKeyCount, inputLayoutIds, rowkeyColumnIndexes));

        // step3: reduce by layout, and merge hll data
        // The key is the layout, the value is data encoded from the hll
        int partitionNum = getLayoutHLLCounterReducerNum(inputLayoutIds.length, kylinConfig);
        log.info("Get the partition count for the HLL reducer: {}", partitionNum);
        JavaPairRDD<BigInteger, byte[]> costRDD = costRddByPartition.reduceByKey(new Partitioner() {
            private final int num = partitionNum;
            private final BigInteger bigIntegerMod = BigInteger.valueOf(num);

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
            private final int precision = hllPrecision;

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
        // The key is the layout, and the value is the estimated statistics
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
            logMapperAndLayoutStatistics(resultCost, 100);
        }
        return resultCost;
    }

    /**
     * @return reducer number for calculating hll
     */
    private static int getLayoutHLLCounterReducerNum(int nLayouts, KylinConfig kylinConfig) {
        int shardBase = (nLayouts - 1) / kylinConfig.getJobPerReducerHllLayoutNumber() + 1;
        int hllMaxReducerNumber = kylinConfig.getJobHLLMaxReducerNumber();
        if (shardBase > hllMaxReducerNumber) {
            shardBase = hllMaxReducerNumber;
        }
        return Math.max(shardBase, 1);
    }

    private static BigInteger[] getLayoutIdsFromEntity(List<LayoutEntity> allLayouts, int dimensionCount,
            Map<Integer, Integer> columnIdToRowkeyId) {
        Set<BigInteger> set = new HashSet<>();
        for (LayoutEntity layoutEntity : allLayouts) {
            BigInteger layoutId = CostBasePlannerUtils.convertDimensionsToLayoutId(layoutEntity.getDimsIds(),
                    dimensionCount, columnIdToRowkeyId);
            set.add(layoutId);
        }
        return set.toArray(new BigInteger[0]);
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
                        String.format("Can't find the column id %d, column ids %s", columnId, columnIds.toString()));
            }
        }
        return rowkeyColumnIndexes;
    }

    private static class FlatOutputFunction implements PairFlatMapFunction<Iterator<String[]>, BigInteger, byte[]> {
        private transient volatile boolean initialized = false;
        private transient LayoutStatCalculator layoutStatCalculator;
        private final int samplingPercent = 100;
        private final int hllPrecision;
        private final int rowKeyCount;
        private final BigInteger[] layoutIds;
        private final int[] rowkeyColumnIndexes;

        public FlatOutputFunction(int hllPrecision, int rowKeyCount, BigInteger[] layoutIds,
                int[] rowkeyColumnIndexes) {
            this.hllPrecision = hllPrecision;
            this.rowKeyCount = rowKeyCount;
            this.layoutIds = layoutIds;
            this.rowkeyColumnIndexes = rowkeyColumnIndexes;
        }

        private Integer[][] getLayoutBitSet(BigInteger[] layoutIds, int nRowKey) {
            Integer[][] allLayoutsBitSet = new Integer[layoutIds.length][];
            for (int j = 0; j < layoutIds.length; j++) {
                BigInteger layoutId = layoutIds[j];

                allLayoutsBitSet[j] = new Integer[layoutId.bitCount()];
                int position = 0;
                for (int i = 0; i < nRowKey; i++) {
                    BigInteger bigMask = BigInteger.ZERO.setBit(nRowKey - 1 - i);
                    if ((bigMask.and(layoutId).compareTo(BigInteger.ZERO)) > 0) {
                        // bigMask & layoutId > 0
                        allLayoutsBitSet[j][position] = i;
                        position++;
                    }
                }
            }
            return allLayoutsBitSet;
        }

        private HLLCounter[] getInitLayoutsHLL(int layoutSize, int hllPrecision) {
            HLLCounter[] layoutsHLL = new HLLCounter[layoutSize];
            for (int i = 0; i < layoutSize; i++) {
                layoutsHLL[i] = new HLLCounter(hllPrecision, RegisterType.DENSE);
            }
            return layoutsHLL;
        }

        private void init() {
            Integer[][] layoutsBitSet = getLayoutBitSet(layoutIds, rowKeyCount);
            HLLCounter[] layoutsHLL = getInitLayoutsHLL(layoutIds.length, hllPrecision);
            layoutStatCalculator = new LayoutStatCalculator(rowkeyColumnIndexes, layoutIds, layoutsBitSet, true,
                    layoutsHLL);
            initialized = true;
        }

        @Override
        public Iterator<Tuple2<BigInteger, byte[]>> call(Iterator<String[]> iterator) throws Exception {
            if (!initialized) {
                // just sync this object
                synchronized (this) {
                    if (!initialized) {
                        init();
                    }
                }
            }

            // One tuple is a cost pair, the left is the layout and the right is the cost
            int rowCount = 0;
            while (iterator.hasNext()) {
                String[] row = iterator.next();
                if (rowCount % 100 < samplingPercent) {
                    layoutStatCalculator.putRow(row);
                }
                rowCount++;
            }

            List<Tuple2<BigInteger, byte[]>> result = Lists.newArrayList();
            ByteBuffer hllBuf = ByteBuffer.allocate(BufferedMeasureCodec.DEFAULT_BUFFER_SIZE);
            BigInteger[] layoutIds = layoutStatCalculator.getLayoutIds();
            HLLCounter[] layoutsHLL = layoutStatCalculator.getHLLCounters();
            HLLCounter hll;
            for (int i = 0; i < layoutIds.length; i++) {
                // key
                BigInteger outputKey = layoutIds[i];
                // value
                hll = layoutsHLL[i];
                hllBuf.clear();
                hll.writeRegisters(hllBuf);
                byte[] value = new byte[hllBuf.position()];
                System.arraycopy(hllBuf.array(), 0, value, 0, hllBuf.position());
                result.add(new Tuple2<>(outputKey, value));
            }
            return result.iterator();
        }
    }

    static class LayoutStatCalculator {
        private final int nRowKey;
        private final int[] rowkeyColIndex;
        private final BigInteger[] layoutIds;
        private final Integer[][] layoutsBitSet;
        private final HLLCounter[] layoutsHLL;

        //about details of the new algorithm, please see KYLIN-2518
        private final boolean isNewAlgorithm;
        private final HashFunction hf;
        private long[] rowHashCodesLong;

        public LayoutStatCalculator(int[] rowkeyColIndex, BigInteger[] layoutIds, Integer[][] layoutsBitSet,
                boolean isUsePutRowKeyToHllNewAlgorithm, HLLCounter[] layoutsHLL) {
            this.nRowKey = rowkeyColIndex.length;
            this.rowkeyColIndex = rowkeyColIndex;
            this.layoutIds = layoutIds;
            this.layoutsBitSet = layoutsBitSet;
            this.isNewAlgorithm = isUsePutRowKeyToHllNewAlgorithm;
            if (!isNewAlgorithm) {
                this.hf = Hashing.murmur3_32();
            } else {
                rowHashCodesLong = new long[nRowKey];
                this.hf = Hashing.murmur3_128();
            }
            this.layoutsHLL = layoutsHLL;
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

            // user the row key column hash to get a consolidated hash for each layout
            for (int i = 0, n = layoutsBitSet.length; i < n; i++) {
                Hasher hc = hf.newHasher();
                for (int position = 0; position < layoutsBitSet[i].length; position++) {
                    hc.putBytes(rowHashCodes[layoutsBitSet[i][position]]);
                }

                layoutsHLL[i].add(hc.hash().asBytes());
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

            // user the row key column hash to get a consolidated hash for each layout
            for (int i = 0, n = layoutsBitSet.length; i < n; i++) {
                long value = 0;
                for (int position = 0; position < layoutsBitSet[i].length; position++) {
                    value += rowHashCodesLong[layoutsBitSet[i][position]];
                }
                layoutsHLL[i].addHashDirectly(value);
            }
        }

        public HLLCounter[] getHLLCounters() {
            return layoutsHLL;
        }

        public BigInteger[] getLayoutIds() {
            return layoutIds;
        }
    }

    private static void logMapperAndLayoutStatistics(Map<BigInteger, HLLCounter> layoutHLLMap, int samplingPercentage) {
        log.debug("Total layout number: \t" + layoutHLLMap.size());
        log.debug("Sampling percentage: \t" + samplingPercentage);
        log.debug("The following statistics are collected based on sampling data.");

        List<BigInteger> allLayouts = Lists.newArrayList(layoutHLLMap.keySet());
        Collections.sort(allLayouts);
        for (BigInteger i : allLayouts) {
            log.debug("Layout " + i + " row count is: \t " + layoutHLLMap.get(i).getCountEstimate());
        }
    }

    public static Map<BigInteger, Long> getLayoutRowCountMapFromSampling(Map<BigInteger, HLLCounter> hllcMap) {
        Map<BigInteger, Long> layoutRowCountMap = Maps.newHashMap();
        for (Map.Entry<BigInteger, HLLCounter> entry : hllcMap.entrySet()) {
            // No need to adjust according sampling percentage. Assumption is that data set is far
            // more than cardinality. Even a percentage of the data should already see all cardinalities.
            layoutRowCountMap.put(entry.getKey(), entry.getValue().getCountEstimate());
        }
        return layoutRowCountMap;
    }

    private static Map<BigInteger, Double> getLayoutSizeMapFromSamplingByCount(Map<BigInteger, Long> rowCountMap) {
        // use the row count to replace the size
        Map<BigInteger, Double> layoutSizeMap = Maps.newHashMap();
        for (Map.Entry<BigInteger, Long> entry : rowCountMap.entrySet()) {
            double value = entry.getValue();
            layoutSizeMap.put(entry.getKey(), value);
        }
        return layoutSizeMap;
    }

    public static Map<BigInteger, Double> getLayoutSizeMapFromSampling(Map<BigInteger, Long> rowCountMap) {
        // replace the size with the row count
        return getLayoutSizeMapFromSamplingByCount(rowCountMap);
    }
}

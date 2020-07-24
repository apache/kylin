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

package org.apache.kylin.engine.spark;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinVersion;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.DimensionRangeInfo;
import org.apache.kylin.cube.cuboid.CuboidUtil;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeJoinedFlatTableEnrich;
import org.apache.kylin.cube.util.KeyValueBuilder;
import org.apache.kylin.dict.DictionaryGenerator;
import org.apache.kylin.dict.IDictionaryBuilder;
import org.apache.kylin.engine.EngineFactory;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.SerializableConfiguration;
import org.apache.kylin.engine.mr.common.StatisticsDecisionUtil;
import org.apache.kylin.engine.mr.steps.FactDistinctColumnsMapper.DictColDeduper;
import org.apache.kylin.engine.mr.steps.FactDistinctColumnsReducerMapping;
import org.apache.kylin.engine.mr.steps.SelfDefineSortableKey;
import org.apache.kylin.job.JoinedFlatTable;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.measure.BufferedMeasureCodec;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.apache.kylin.measure.hllc.RegisterType;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.shaded.com.google.common.collect.Sets;
import org.apache.kylin.shaded.com.google.common.hash.HashFunction;
import org.apache.kylin.shaded.com.google.common.hash.Hasher;
import org.apache.kylin.shaded.com.google.common.hash.Hashing;

import scala.Tuple2;
import scala.Tuple3;

public class SparkFactDistinct extends AbstractApplication implements Serializable {

    protected static final Logger logger = LoggerFactory.getLogger(SparkFactDistinct.class);

    public static final Option OPTION_CUBE_NAME = OptionBuilder.withArgName(BatchConstants.ARG_CUBE_NAME).hasArg()
            .isRequired(true).withDescription("Cube Name").create(BatchConstants.ARG_CUBE_NAME);
    public static final Option OPTION_META_URL = OptionBuilder.withArgName("metaUrl").hasArg().isRequired(true)
            .withDescription("HDFS metadata url").create("metaUrl");
    public static final Option OPTION_OUTPUT_PATH = OptionBuilder.withArgName(BatchConstants.ARG_OUTPUT).hasArg()
            .isRequired(true).withDescription("Cube output path").create(BatchConstants.ARG_OUTPUT);
    public static final Option OPTION_SEGMENT_ID = OptionBuilder.withArgName("segmentId").hasArg().isRequired(true)
            .withDescription("Cube Segment Id").create("segmentId");
    public static final Option OPTION_STATS_SAMPLING_PERCENT = OptionBuilder
            .withArgName(BatchConstants.ARG_STATS_SAMPLING_PERCENT).hasArg().isRequired(true)
            .withDescription("Statistics sampling percent").create(BatchConstants.ARG_STATS_SAMPLING_PERCENT);
    public static final Option OPTION_INPUT_TABLE = OptionBuilder.withArgName("hiveTable").hasArg().isRequired(true)
            .withDescription("Hive Intermediate Table").create("hiveTable");
    public static final Option OPTION_INPUT_PATH = OptionBuilder.withArgName(BatchConstants.ARG_INPUT).hasArg()
            .isRequired(true).withDescription("Hive Intermediate Table PATH").create(BatchConstants.ARG_INPUT);
    public static final Option OPTION_COUNTER_PATH = OptionBuilder.withArgName(BatchConstants.ARG_COUNTER_OUTPUT)
            .hasArg().isRequired(true).withDescription("counter output path").create(BatchConstants.ARG_COUNTER_OUTPUT);

    private Options options;

    public SparkFactDistinct() {
        options = new Options();
        options.addOption(OPTION_CUBE_NAME);
        options.addOption(OPTION_META_URL);
        options.addOption(OPTION_OUTPUT_PATH);
        options.addOption(OPTION_INPUT_TABLE);
        options.addOption(OPTION_INPUT_PATH);
        options.addOption(OPTION_SEGMENT_ID);
        options.addOption(OPTION_STATS_SAMPLING_PERCENT);
        options.addOption(OPTION_COUNTER_PATH);
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        String cubeName = optionsHelper.getOptionValue(OPTION_CUBE_NAME);
        String metaUrl = optionsHelper.getOptionValue(OPTION_META_URL);
        String segmentId = optionsHelper.getOptionValue(OPTION_SEGMENT_ID);
        String hiveTable = optionsHelper.getOptionValue(OPTION_INPUT_TABLE);
        String inputPath = optionsHelper.getOptionValue(OPTION_INPUT_PATH);
        String outputPath = optionsHelper.getOptionValue(OPTION_OUTPUT_PATH);
        String counterPath = optionsHelper.getOptionValue(OPTION_COUNTER_PATH);
        int samplingPercent = Integer.parseInt(optionsHelper.getOptionValue(OPTION_STATS_SAMPLING_PERCENT));

        Class[] kryoClassArray = new Class[] { Class.forName("scala.reflect.ClassTag$$anon$1"),
                Class.forName("org.apache.kylin.engine.mr.steps.SelfDefineSortableKey") };

        SparkConf conf = new SparkConf().setAppName("Fact distinct columns for:" + cubeName + " segment " + segmentId);
        //serialization conf
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "org.apache.kylin.engine.spark.KylinKryoRegistrator");
        conf.set("spark.kryo.registrationRequired", "true").registerKryoClasses(kryoClassArray);

        KylinSparkJobListener jobListener = new KylinSparkJobListener();
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            sc.sc().addSparkListener(jobListener);
            HadoopUtil.deletePath(sc.hadoopConfiguration(), new Path(outputPath));

            final SerializableConfiguration sConf = new SerializableConfiguration(sc.hadoopConfiguration());
            KylinConfig envConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(sConf, metaUrl);

            final CubeInstance cubeInstance = CubeManager.getInstance(envConfig).getCube(cubeName);

            final Job job = Job.getInstance(sConf.get());

            final FactDistinctColumnsReducerMapping reducerMapping = new FactDistinctColumnsReducerMapping(
                    cubeInstance);

            logger.info("RDD Output path: {}", outputPath);
            logger.info("getTotalReducerNum: {}", reducerMapping.getTotalReducerNum());
            logger.info("getCuboidRowCounterReducerNum: {}", reducerMapping.getCuboidRowCounterReducerNum());
            logger.info("counter path {}", counterPath);

            boolean isSequenceFile = JoinedFlatTable.SEQUENCEFILE
                    .equalsIgnoreCase(envConfig.getFlatTableStorageFormat());

            // calculate source record bytes size
            final LongAccumulator bytesWritten = sc.sc().longAccumulator();

            final JavaRDD<String[]> recordRDD = SparkUtil.hiveRecordInputRDD(isSequenceFile, sc, inputPath, hiveTable);

            // read record from flat table
            // output:
            //   1, statistic
            //   2, field value of dict col
            //   3, min/max field value of not dict col
            JavaPairRDD<SelfDefineSortableKey, Text> flatOutputRDD = recordRDD.mapPartitionsToPair(
                    new FlatOutputFucntion(cubeName, segmentId, metaUrl, sConf, samplingPercent, bytesWritten));

            // repartition data, make each reducer handle only one col data or the statistic data
            JavaPairRDD<SelfDefineSortableKey, Text> aggredRDD = flatOutputRDD
                .repartitionAndSortWithinPartitions(new FactDistinctPartitioner(cubeName, metaUrl, sConf, reducerMapping.getTotalReducerNum()));

            // multiple output result
            // 1, CFG_OUTPUT_COLUMN: field values of dict col, which will not be built in reducer, like globalDictCol
            // 2, CFG_OUTPUT_DICT: dictionary object built in reducer
            // 3, CFG_OUTPUT_STATISTICS: cube statistic: hll of cuboids ...
            // 4, CFG_OUTPUT_PARTITION: dimension value range(min,max)
            JavaPairRDD<String, Tuple3<Writable, Writable, String>> outputRDD = aggredRDD
                    .mapPartitionsToPair(new MultiOutputFunction(cubeName, metaUrl, sConf, samplingPercent));

            // make each reducer output to respective dir
            MultipleOutputs.addNamedOutput(job, BatchConstants.CFG_OUTPUT_COLUMN, SequenceFileOutputFormat.class,
                    NullWritable.class, Text.class);
            MultipleOutputs.addNamedOutput(job, BatchConstants.CFG_OUTPUT_DICT, SequenceFileOutputFormat.class,
                    NullWritable.class, ArrayPrimitiveWritable.class);
            MultipleOutputs.addNamedOutput(job, BatchConstants.CFG_OUTPUT_STATISTICS, SequenceFileOutputFormat.class,
                    LongWritable.class, BytesWritable.class);
            MultipleOutputs.addNamedOutput(job, BatchConstants.CFG_OUTPUT_PARTITION, TextOutputFormat.class,
                    NullWritable.class, LongWritable.class);

            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            FileOutputFormat.setCompressOutput(job, false);

            // prevent to create zero-sized default output
            LazyOutputFormat.setOutputFormatClass(job, SequenceFileOutputFormat.class);

            MultipleOutputsRDD multipleOutputsRDD = MultipleOutputsRDD.rddToMultipleOutputsRDD(outputRDD);

            multipleOutputsRDD.saveAsNewAPIHadoopDatasetWithMultipleOutputs(job.getConfiguration());

            long recordCount = recordRDD.count();
            logger.info("Map input records={}", recordCount);
            logger.info("HDFS Read: {} HDFS Write", bytesWritten.value());

            Map<String, String> counterMap = Maps.newHashMap();
            counterMap.put(ExecutableConstants.SOURCE_RECORDS_COUNT, String.valueOf(recordCount));
            counterMap.put(ExecutableConstants.SOURCE_RECORDS_SIZE, String.valueOf(bytesWritten.value()));

            // save counter to hdfs
            HadoopUtil.writeToSequenceFile(sc.hadoopConfiguration(), counterPath, counterMap);

            HadoopUtil.deleteHDFSMeta(metaUrl);
        }
    }

    /**
     * output: Tuple2<SelfDefineSortableKey, Text>
     * 1, for statistics, SelfDefineSortableKey = reducerId + cuboidId, Text = hll of cuboidId
     * 2, for dict col, SelfDefineSortableKey = reducerId + field value, Text = ""
     * 3, for not dict col, SelfDefineSortableKey = reducerId + min/max value, Text = ""
     */
    static class FlatOutputFucntion implements PairFlatMapFunction<Iterator<String[]>, SelfDefineSortableKey, Text> {
        private transient volatile boolean initialized = false;
        private String cubeName;
        private String segmentId;
        private String metaUrl;
        private SerializableConfiguration conf;
        private int samplingPercent;
        private transient CuboidStatCalculator cuboidStatCalculator;
        private transient FactDistinctColumnsReducerMapping reducerMapping;
        private List<TblColRef> allCols;
        private int[] columnIndex;
        private transient DictColDeduper dictColDeduper;
        private Map<Integer, DimensionRangeInfo> dimensionRangeInfoMap;
        private transient ByteBuffer tmpbuf;
        private LongAccumulator bytesWritten;
        private KeyValueBuilder keyValueBuilder;

        public FlatOutputFucntion(String cubeName, String segmentId, String metaurl, SerializableConfiguration conf,
                int samplingPercent, LongAccumulator bytesWritten) {
            this.cubeName = cubeName;
            this.segmentId = segmentId;
            this.metaUrl = metaurl;
            this.conf = conf;
            this.samplingPercent = samplingPercent;
            this.dimensionRangeInfoMap = Maps.newHashMap();
            this.bytesWritten = bytesWritten;
        }

        private void init() {
            KylinConfig kConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(conf, metaUrl);
            try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig
                    .setAndUnsetThreadLocalConfig(kConfig)) {
                CubeInstance cubeInstance = CubeManager.getInstance(kConfig).getCube(cubeName);
                CubeDesc cubeDesc = cubeInstance.getDescriptor();
                CubeSegment cubeSegment = cubeInstance.getSegmentById(segmentId);
                CubeJoinedFlatTableEnrich intermediateTableDesc = new CubeJoinedFlatTableEnrich(
                        EngineFactory.getJoinedFlatTableDesc(cubeSegment), cubeDesc);

                keyValueBuilder = new KeyValueBuilder(intermediateTableDesc);
                reducerMapping = new FactDistinctColumnsReducerMapping(cubeInstance);
                tmpbuf = ByteBuffer.allocate(4096);

                int[] rokeyColumnIndexes = intermediateTableDesc.getRowKeyColumnIndexes();

                Long[] cuboidIds = getCuboidIds(cubeSegment);

                Integer[][] cuboidsBitSet = CuboidUtil.getCuboidBitSet(cuboidIds, rokeyColumnIndexes.length);

                boolean isNewAlgorithm = isUsePutRowKeyToHllNewAlgorithm(cubeDesc);

                HLLCounter[] cuboidsHLL = getInitCuboidsHLL(cuboidIds.length,
                        cubeDesc.getConfig().getCubeStatsHLLPrecision());

                cuboidStatCalculator = new CuboidStatCalculator(rokeyColumnIndexes, cuboidIds, cuboidsBitSet,
                        isNewAlgorithm, cuboidsHLL);
                allCols = reducerMapping.getAllDimDictCols();

                initDictColDeduper(cubeDesc);
                initColumnIndex(intermediateTableDesc);

                initialized = true;
            }
        }

        @Override
        public Iterator<Tuple2<SelfDefineSortableKey, Text>> call(Iterator<String[]> rowIterator) throws Exception {
            if (initialized == false) {
                synchronized (SparkFactDistinct.class) {
                    if (initialized == false) {
                        init();
                    }
                }
            }

            List<Tuple2<SelfDefineSortableKey, Text>> result = Lists.newArrayList();

            int rowCount = 0;

            while (rowIterator.hasNext()) {
                String[] row = rowIterator.next();
                bytesWritten.add(countSizeInBytes(row));

                for (int i = 0; i < allCols.size(); i++) {
                    String fieldValue = row[columnIndex[i]];
                    if (fieldValue == null || keyValueBuilder.isNull(fieldValue))
                        continue;

                    final DataType type = allCols.get(i).getType();

                    //for dic column, de dup before write value; for dim not dic column, hold util doCleanup()
                    if (dictColDeduper.isDictCol(i)) {
                        if (dictColDeduper.add(i, fieldValue)) {
                            addFieldValue(type, i, fieldValue, result);
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
                }

                if (rowCount % 100 < samplingPercent) {
                    cuboidStatCalculator.putRow(row);
                }

                if (rowCount % 100 == 0) {
                    dictColDeduper.resetIfShortOfMem();
                }

                rowCount++;
            }

            ByteBuffer hllBuf = ByteBuffer.allocate(BufferedMeasureCodec.DEFAULT_BUFFER_SIZE);

            // output each cuboid's hll to reducer, key is 0 - cuboidId
            Long[] cuboidIds = cuboidStatCalculator.getCuboidIds();
            HLLCounter[] cuboidsHLL = cuboidStatCalculator.getHLLCounters();
            HLLCounter hll;

            for (int i = 0; i < cuboidIds.length; i++) {
                hll = cuboidsHLL[i];
                tmpbuf.clear();
                tmpbuf.put((byte) FactDistinctColumnsReducerMapping.MARK_FOR_HLL_COUNTER); // one byte
                tmpbuf.putLong(cuboidIds[i]);
                Text outputKey = new Text();
                Text outputValue = new Text();
                SelfDefineSortableKey sortableKey = new SelfDefineSortableKey();

                outputKey.set(tmpbuf.array(), 0, tmpbuf.position());
                hllBuf.clear();
                hll.writeRegisters(hllBuf);
                outputValue.set(hllBuf.array(), 0, hllBuf.position());

                sortableKey.init(outputKey, (byte) 0);

                result.add(new Tuple2<SelfDefineSortableKey, Text>(sortableKey, outputValue));
            }

            for (Map.Entry<Integer, DimensionRangeInfo> entry : dimensionRangeInfoMap.entrySet()) {
                int colIndex = entry.getKey();
                DimensionRangeInfo rangeInfo = entry.getValue();
                DataType dataType = allCols.get(colIndex).getType();
                addFieldValue(dataType, colIndex, rangeInfo.getMin(), result);
                addFieldValue(dataType, colIndex, rangeInfo.getMax(), result);
            }

            return result.iterator();
        }

        private boolean isUsePutRowKeyToHllNewAlgorithm(CubeDesc cubeDesc) {
            boolean isUsePutRowKeyToHllNewAlgorithm;
            if (KylinVersion.isBefore200(cubeDesc.getVersion())) {
                isUsePutRowKeyToHllNewAlgorithm = false;
                logger.info("Found KylinVersion: {}. Use old algorithm for cuboid sampling.", cubeDesc.getVersion());
            } else {
                isUsePutRowKeyToHllNewAlgorithm = true;
                logger.info(
                        "Found KylinVersion: {}. Use new algorithm for cuboid sampling. About the details of the new algorithm, please refer to KYLIN-2518",
                        cubeDesc.getVersion());
            }
            return isUsePutRowKeyToHllNewAlgorithm;
        }

        private Long[] getCuboidIds(CubeSegment cubeSegment) {
            Set<Long> cuboidIdSet = Sets.newHashSet(cubeSegment.getCuboidScheduler().getAllCuboidIds());
            if (StatisticsDecisionUtil.isAbleToOptimizeCubingPlan(cubeSegment)) {
                // For cube planner, for every prebuilt cuboid, its related row count stats should be calculated
                // If the precondition for trigger cube planner phase one is satisfied, we need to calculate row count stats for mandatory cuboids.
                cuboidIdSet.addAll(cubeSegment.getCubeDesc().getMandatoryCuboids());
            }

            return cuboidIdSet.toArray(new Long[cuboidIdSet.size()]);
        }

        private HLLCounter[] getInitCuboidsHLL(int cuboidSize, int hllPrecision) {
            HLLCounter[] cuboidsHLL = new HLLCounter[cuboidSize];
            for (int i = 0; i < cuboidSize; i++) {
                cuboidsHLL[i] = new HLLCounter(hllPrecision, RegisterType.DENSE);
            }
            return cuboidsHLL;
        }

        private void initDictColDeduper(CubeDesc cubeDesc) {
            // setup dict col deduper
            dictColDeduper = new DictColDeduper();
            Set<TblColRef> dictCols = cubeDesc.getAllColumnsNeedDictionaryBuilt();
            for (int i = 0; i < allCols.size(); i++) {
                if (dictCols.contains(allCols.get(i)))
                    dictColDeduper.setIsDictCol(i);
            }
        }

        private void initColumnIndex(CubeJoinedFlatTableEnrich intermediateTableDesc) {
            columnIndex = new int[allCols.size()];
            for (int i = 0; i < allCols.size(); i++) {
                TblColRef colRef = allCols.get(i);
                int columnIndexOnFlatTbl = intermediateTableDesc.getColumnIndex(colRef);
                columnIndex[i] = columnIndexOnFlatTbl;
            }
        }

        private void addFieldValue(DataType type, Integer colIndex, String value,
                List<Tuple2<SelfDefineSortableKey, Text>> result) {
            int reducerIndex = reducerMapping.getReducerIdForCol(colIndex, value);
            tmpbuf.clear();
            byte[] valueBytes = Bytes.toBytes(value);
            int size = valueBytes.length + 1;
            if (size >= tmpbuf.capacity()) {
                tmpbuf = ByteBuffer.allocate(countNewSize(tmpbuf.capacity(), size));
            }
            tmpbuf.put(Bytes.toBytes(reducerIndex)[3]);
            tmpbuf.put(valueBytes);

            Text outputKey = new Text();
            SelfDefineSortableKey sortableKey = new SelfDefineSortableKey();

            outputKey.set(tmpbuf.array(), 0, tmpbuf.position());
            sortableKey.init(outputKey, type);

            result.add(new Tuple2<SelfDefineSortableKey, Text>(sortableKey, new Text()));

            // log a few rows for troubleshooting
            if (result.size() < 10) {
                logger.info("Sample output: {} '{}' => reducer {}", allCols.get(colIndex), value, reducerIndex);
            }
        }

        private int countNewSize(int oldSize, int dataSize) {
            int newSize = oldSize * 2;
            while (newSize < dataSize) {
                newSize = newSize * 2;
            }
            return newSize;
        }

        private int countSizeInBytes(String[] row) {
            int size = 0;
            for (String s : row) {
                size += s == null ? 1 : StringUtil.utf8Length(s);
                size++; // delimiter
            }
            return size;
        }
    }

    static class CuboidStatCalculator {
        private final int nRowKey;
        private final int[] rowkeyColIndex;
        private final Long[] cuboidIds;
        private final Integer[][] cuboidsBitSet;
        private HLLCounter[] cuboidsHLL;

        //about details of the new algorithm, please see KYLIN-2518
        private final boolean isNewAlgorithm;
        private final HashFunction hf;
        private long[] rowHashCodesLong;

        public CuboidStatCalculator(int[] rowkeyColIndex, Long[] cuboidIds, Integer[][] cuboidsBitSet,
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
                    rowHashCodes[i] = hc.putUnencodedChars(colValue).hash().asBytes();
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
                byte[] bytes = hc.putUnencodedChars(colValue).hash().asBytes();
                rowHashCodesLong[i] = (Bytes.toLong(bytes) + i);//add column ordinal to the hash value to distinguish between (a,b) and (b,a)
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

        public Long[] getCuboidIds() {
            return cuboidIds;
        }
    }

    static class FactDistinctPartitioner extends Partitioner {
        private transient volatile boolean initialized = false;
        private String cubeName;
        private String metaUrl;
        private SerializableConfiguration conf;
        private int totalReducerNum;
        private transient FactDistinctColumnsReducerMapping reducerMapping;

        public FactDistinctPartitioner(String cubeName, String metaUrl, SerializableConfiguration conf,
                int totalReducerNum) {
            this.cubeName = cubeName;
            this.metaUrl = metaUrl;
            this.conf = conf;
            this.totalReducerNum = totalReducerNum;
        }

        private void init() {
            KylinConfig kConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(conf, metaUrl);
            try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig
                    .setAndUnsetThreadLocalConfig(kConfig)) {
                CubeInstance cubeInstance = CubeManager.getInstance(kConfig).getCube(cubeName);
                reducerMapping = new FactDistinctColumnsReducerMapping(cubeInstance);
                initialized = true;
            }
        }

        @Override
        public int numPartitions() {
            return totalReducerNum;
        }

        @Override
        public int getPartition(Object o) {
            if (initialized == false) {
                synchronized (SparkFactDistinct.class) {
                    if (initialized == false) {
                        init();
                    }
                }
            }

            SelfDefineSortableKey skey = (SelfDefineSortableKey) o;
            Text key = skey.getText();
            if (key.getBytes()[0] == FactDistinctColumnsReducerMapping.MARK_FOR_HLL_COUNTER) {
                Long cuboidId = Bytes.toLong(key.getBytes(), 1, Bytes.SIZEOF_LONG);
                return reducerMapping.getReducerIdForCuboidRowCount(cuboidId);
            } else {
                return BytesUtil.readUnsigned(key.getBytes(), 0, 1);
            }
        }
    }

    static class MultiOutputFunction implements
            PairFlatMapFunction<Iterator<Tuple2<SelfDefineSortableKey, Text>>, String, Tuple3<Writable, Writable, String>> {
        private transient volatile boolean initialized = false;
        private String DICT_FILE_POSTFIX = ".rldict";
        private String DIMENSION_COL_INFO_FILE_POSTFIX = ".dci";
        private String cubeName;
        private String metaUrl;
        private SerializableConfiguration conf;
        private int samplingPercent;
        private transient FactDistinctColumnsReducerMapping reducerMapping;
        private int taskId;
        private boolean isStatistics = false;
        private long baseCuboidId;
        private List<Long> baseCuboidRowCountInMappers;
        private Map<Long, HLLCounter> cuboidHLLMap;
        private TblColRef col;
        private boolean buildDictInReducer;
        private transient IDictionaryBuilder builder;
        private int rowCount = 0;
        private long totalRowsBeforeMerge = 0;
        private KylinConfig cubeConfig;
        private CubeDesc cubeDesc;
        private String maxValue = null;
        private String minValue = null;
        private boolean isDimensionCol;
        private boolean isDictCol;
        private KylinConfig kConfig;
        private List<Tuple2<String, Tuple3<Writable, Writable, String>>> result;

        public MultiOutputFunction(String cubeName, String metaurl, SerializableConfiguration conf,
                int samplingPercent) {
            this.cubeName = cubeName;
            this.metaUrl = metaurl;
            this.conf = conf;
            this.samplingPercent = samplingPercent;
        }

        private void init() throws IOException {
            taskId = TaskContext.getPartitionId();
            kConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(conf, metaUrl);
            try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig
                    .setAndUnsetThreadLocalConfig(kConfig)) {
                CubeInstance cubeInstance = CubeManager.getInstance(kConfig).getCube(cubeName);
                cubeDesc = cubeInstance.getDescriptor();
                cubeConfig = cubeInstance.getConfig();
                reducerMapping = new FactDistinctColumnsReducerMapping(cubeInstance);

                result = Lists.newArrayList();

                if (reducerMapping.isCuboidRowCounterReducer(taskId)) {
                    // hll
                    isStatistics = true;
                    baseCuboidId = cubeInstance.getCuboidScheduler().getBaseCuboidId();
                    baseCuboidRowCountInMappers = Lists.newArrayList();
                    cuboidHLLMap = Maps.newHashMap();

                    logger.info("Partition {} handling stats", taskId);
                } else {
                    // normal col
                    col = reducerMapping.getColForReducer(taskId);
                    Preconditions.checkNotNull(col);

                    isDimensionCol = cubeDesc.listDimensionColumnsExcludingDerived(true).contains(col) && col.getType().needCompare();
                    isDictCol = cubeDesc.getAllColumnsNeedDictionaryBuilt().contains(col);

                    // local build dict
                    buildDictInReducer = kConfig.isBuildDictInReducerEnabled();
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
                    logger.info("Partition {} handling column {}, buildDictInReducer={}", taskId, col, buildDictInReducer);
                }

                initialized = true;
            }
        }

        private void logAFewRows(String value) {
            if (rowCount < 10) {
                logger.info("Received value: {}", value);
            }
        }

        @Override
        public Iterator<Tuple2<String, Tuple3<Writable, Writable, String>>> call(
                Iterator<Tuple2<SelfDefineSortableKey, Text>> tuple2Iterator) throws Exception {
            if (initialized == false) {
                synchronized (SparkFactDistinct.class) {
                    if (initialized == false) {
                        init();
                    }
                }
            }

            if (isStatistics) {
                // calculate hll
                calculateStatistics(tuple2Iterator);

                // output the hll info
                List<Long> allCuboids = Lists.newArrayList();
                allCuboids.addAll(cuboidHLLMap.keySet());
                Collections.sort(allCuboids);

                logMapperAndCuboidStatistics(allCuboids); // for human check
                outputStatistics(allCuboids, result);
            } else {
                // calculate dict/dimRange/
                calculateColData(tuple2Iterator);

                // output dim range
                if (isDimensionCol) {
                    outputDimRangeInfo(result);
                }

                // output dict object
                if (buildDictInReducer) {
                    try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig
                            .setAndUnsetThreadLocalConfig(kConfig)) {
                        Dictionary<String> dict = builder.build();
                        outputDict(col, dict, result);
                    }
                }
            }

            return result.iterator();
        }

        private void calculateStatistics(Iterator<Tuple2<SelfDefineSortableKey, Text>> tuple2Iterator) throws IOException {
            while (tuple2Iterator.hasNext()) {
                HLLCounter hll = new HLLCounter(cubeConfig.getCubeStatsHLLPrecision());

                Tuple2<SelfDefineSortableKey, Text> tuple = tuple2Iterator.next();
                long cuboidId = Bytes.toLong(tuple._1.getText().getBytes(), 1);

                ByteBuffer bf = ByteBuffer.wrap(tuple._2.getBytes(), 0, tuple._2.getLength());
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
            }
        }

        private void calculateColData(Iterator<Tuple2<SelfDefineSortableKey, Text>> tuple2Iterator) {
            while (tuple2Iterator.hasNext()) {
                Tuple2<SelfDefineSortableKey, Text> tuple = tuple2Iterator.next();

                String value = Bytes.toString(tuple._1.getText().getBytes(), 1, tuple._1.getText().getLength() - 1);
                logAFewRows(value);

                // if dimension col, compute max/min value
                // include the col which is both dict col and dim col
                if (isDimensionCol) {
                    if (minValue == null || col.getType().compare(minValue, value) > 0) {
                        minValue = value;
                    }
                    if (maxValue == null || col.getType().compare(maxValue, value) < 0) {
                        maxValue = value;
                    }
                }

                //if dict column
                if (isDictCol) {
                    if (buildDictInReducer) {
                        builder.addValue(value);
                    } else {
                        // output written to baseDir/colName/-r-00000 (etc)
                        result.add(new Tuple2<String, Tuple3<Writable, Writable, String>>(
                                BatchConstants.CFG_OUTPUT_COLUMN, new Tuple3<Writable, Writable, String>(
                                NullWritable.get(), new Text(value.getBytes(StandardCharsets.UTF_8)), col.getIdentity() + "/")));
                    }
                }

                rowCount++;
            }
        }

        private void logMapperAndCuboidStatistics(List<Long> allCuboids) {
            logger.info("Cuboid number for task: {}\t{}", taskId, allCuboids.size());
            logger.info("Samping percentage: \t{}", samplingPercent);
            logger.info("The following statistics are collected based on sampling data. ");
            logger.info("Number of Mappers: {}", baseCuboidRowCountInMappers.size());

            for (int i = 0; i < baseCuboidRowCountInMappers.size(); i++) {
                if (baseCuboidRowCountInMappers.get(i) > 0) {
                    logger.info("Base Cuboid in Mapper {} row count: \t {}", i, baseCuboidRowCountInMappers.get(i));
                }
            }

            long grantTotal = 0;
            for (long i : allCuboids) {
                grantTotal += cuboidHLLMap.get(i).getCountEstimate();
                logger.info("Cuboid {} row count is: \t {}", i, cuboidHLLMap.get(i).getCountEstimate());
            }

            logger.info("Sum of row counts (before merge) is: \t {}", totalRowsBeforeMerge);
            logger.info("After merge, the row count: \t {}", grantTotal);
        }

        private void outputDimRangeInfo(List<Tuple2<String, Tuple3<Writable, Writable, String>>> result) {
            if (col != null && minValue != null) {
                // output written to baseDir/colName/colName.dci-r-00000 (etc)
                String dimRangeFileName = col.getIdentity() + "/" + col.getName() + DIMENSION_COL_INFO_FILE_POSTFIX;

                result.add(new Tuple2<String, Tuple3<Writable, Writable, String>>(BatchConstants.CFG_OUTPUT_PARTITION,
                        new Tuple3<Writable, Writable, String>(NullWritable.get(),
                                new Text(minValue.getBytes(StandardCharsets.UTF_8)), dimRangeFileName)));
                result.add(new Tuple2<String, Tuple3<Writable, Writable, String>>(BatchConstants.CFG_OUTPUT_PARTITION,
                        new Tuple3<Writable, Writable, String>(NullWritable.get(),
                                new Text(maxValue.getBytes(StandardCharsets.UTF_8)), dimRangeFileName)));
                logger.info("write dimension range info for col : {}  minValue:{} maxValue:{}", col.getName(), minValue, maxValue);
            }
        }

        private void outputDict(TblColRef col, Dictionary<String> dict,
                List<Tuple2<String, Tuple3<Writable, Writable, String>>> result)
                throws IOException {
            // output written to baseDir/colName/colName.rldict-r-00000 (etc)
            String dictFileName = col.getIdentity() + "/" + col.getName() + DICT_FILE_POSTFIX;

            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    DataOutputStream outputStream = new DataOutputStream(baos)) {
                outputStream.writeUTF(dict.getClass().getName());
                dict.write(outputStream);

                result.add(new Tuple2<String, Tuple3<Writable, Writable, String>>(BatchConstants.CFG_OUTPUT_DICT,
                        new Tuple3<Writable, Writable, String>(NullWritable.get(),
                                new ArrayPrimitiveWritable(baos.toByteArray()), dictFileName)));
            }
        }

        private void outputStatistics(List<Long> allCuboids,
                List<Tuple2<String, Tuple3<Writable, Writable, String>>> result)
                throws IOException {
            // output written to baseDir/statistics/statistics-r-00000 (etc)
            String statisticsFileName = BatchConstants.CFG_OUTPUT_STATISTICS + "/"
                    + BatchConstants.CFG_OUTPUT_STATISTICS;

            // mapper overlap ratio at key -1
            long grandTotal = 0;

            for (HLLCounter hll : cuboidHLLMap.values()) {
                grandTotal += hll.getCountEstimate();
            }

            double mapperOverlapRatio = grandTotal == 0 ? 0 : (double) totalRowsBeforeMerge / grandTotal;
            result.add(new Tuple2<String, Tuple3<Writable, Writable, String>>(BatchConstants.CFG_OUTPUT_STATISTICS,
                    new Tuple3<Writable, Writable, String>(new LongWritable(-1),
                            new BytesWritable(Bytes.toBytes(mapperOverlapRatio)), statisticsFileName)));

            // mapper number at key -2
            result.add(new Tuple2<String, Tuple3<Writable, Writable, String>>(BatchConstants.CFG_OUTPUT_STATISTICS,
                    new Tuple3<Writable, Writable, String>(new LongWritable(-2),
                            new BytesWritable(Bytes.toBytes(baseCuboidRowCountInMappers.size())), statisticsFileName)));

            // sampling percentage at key 0
            result.add(new Tuple2<String, Tuple3<Writable, Writable, String>>(BatchConstants.CFG_OUTPUT_STATISTICS,
                    new Tuple3<Writable, Writable, String>(new LongWritable(0L),
                            new BytesWritable(Bytes.toBytes(samplingPercent)), statisticsFileName)));

            ByteBuffer valueBuf = ByteBuffer.allocate(BufferedMeasureCodec.DEFAULT_BUFFER_SIZE);

            for (long i : allCuboids) {
                valueBuf.clear();
                cuboidHLLMap.get(i).writeRegisters(valueBuf);
                valueBuf.flip();

                byte[] valueCopy = new byte[valueBuf.limit()];
                System.arraycopy(valueBuf.array(), 0, valueCopy, 0, valueBuf.limit());

                result.add(new Tuple2<String, Tuple3<Writable, Writable, String>>(BatchConstants.CFG_OUTPUT_STATISTICS,
                        new Tuple3<Writable, Writable, String>(new LongWritable(i),
                                new BytesWritable(valueCopy, valueCopy.length), statisticsFileName)));
            }
        }
    }
}

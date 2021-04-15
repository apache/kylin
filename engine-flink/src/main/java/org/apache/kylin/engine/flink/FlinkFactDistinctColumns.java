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

package org.apache.kylin.engine.flink;

import com.google.common.collect.Maps;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.SerializableConfiguration;
import org.apache.kylin.engine.mr.steps.FactDistinctColumnsBase;
import org.apache.kylin.engine.mr.steps.FactDistinctColumnsReducerMapping;
import org.apache.kylin.engine.mr.steps.SelfDefineSortableKey;
import org.apache.kylin.job.JoinedFlatTable;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class FlinkFactDistinctColumns extends AbstractApplication {
    protected static final Logger logger = LoggerFactory.getLogger(FlinkFactDistinctColumns.class);

    public static final Option OPTION_CUBE_NAME = OptionBuilder.withArgName(BatchConstants.ARG_CUBE_NAME).hasArg()
            .isRequired(true).withDescription("Cube Name").create(BatchConstants.ARG_CUBE_NAME);
    public static final Option OPTION_SEGMENT_ID = OptionBuilder.withArgName("segmentId").hasArg().isRequired(true)
            .withDescription("Cube Segment Id").create("segmentId");
    public static final Option OPTION_META_URL = OptionBuilder.withArgName("metaUrl").hasArg().isRequired(true)
            .withDescription("HDFS metadata url").create("metaUrl");
    public static final Option OPTION_INPUT_TABLE = OptionBuilder.withArgName("hiveTable").hasArg().isRequired(true)
            .withDescription("Hive Intermediate Table").create("hiveTable");
    public static final Option OPTION_INPUT_PATH = OptionBuilder.withArgName(BatchConstants.ARG_INPUT).hasArg()
            .isRequired(true).withDescription("Hive Intermediate Table PATH").create(BatchConstants.ARG_INPUT);
    public static final Option OPTION_OUTPUT_PATH = OptionBuilder.withArgName(BatchConstants.ARG_OUTPUT).hasArg()
            .isRequired(true).withDescription("Cube output path").create(BatchConstants.ARG_OUTPUT);
    public static final Option OPTION_COUNTER_PATH = OptionBuilder.withArgName(BatchConstants.ARG_COUNTER_OUTPUT)
            .hasArg().isRequired(true).withDescription("Counter output path").create(BatchConstants.ARG_COUNTER_OUTPUT);
    public static final Option OPTION_STATS_SAMPLING_PERCENT = OptionBuilder
            .withArgName(BatchConstants.ARG_STATS_SAMPLING_PERCENT).hasArg().isRequired(true)
            .withDescription("Statistics sampling percent").create(BatchConstants.ARG_STATS_SAMPLING_PERCENT);
    public static final Option OPTION_ENABLE_OBJECT_REUSE = OptionBuilder.withArgName("enableObjectReuse").hasArg()
            .isRequired(false).withDescription("Enable object reuse").create("enableObjectReuse");

    private Options options;

    public FlinkFactDistinctColumns() {
        options = new Options();
        options.addOption(OPTION_CUBE_NAME);
        options.addOption(OPTION_META_URL);
        options.addOption(OPTION_OUTPUT_PATH);
        options.addOption(OPTION_INPUT_TABLE);
        options.addOption(OPTION_INPUT_PATH);
        options.addOption(OPTION_SEGMENT_ID);
        options.addOption(OPTION_STATS_SAMPLING_PERCENT);
        options.addOption(OPTION_COUNTER_PATH);
        options.addOption(OPTION_ENABLE_OBJECT_REUSE);
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
        String enableObjectReuseOptValue = optionsHelper.getOptionValue(OPTION_ENABLE_OBJECT_REUSE);

        Job job = Job.getInstance();
        FileSystem fs = HadoopUtil.getWorkingFileSystem(job.getConfiguration());
        HadoopUtil.deletePath(job.getConfiguration(), new Path(outputPath));

        final SerializableConfiguration sConf = new SerializableConfiguration(job.getConfiguration());
        KylinConfig envConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(sConf, metaUrl);

        final CubeInstance cubeInstance = CubeManager.getInstance(envConfig).getCube(cubeName);

        final FactDistinctColumnsReducerMapping reducerMapping = new FactDistinctColumnsReducerMapping(cubeInstance);
        final int totalReducer = reducerMapping.getTotalReducerNum();

        logger.info("getTotalReducerNum: {}", totalReducer);
        logger.info("getCuboidRowCounterReducerNum: {}", reducerMapping.getCuboidRowCounterReducerNum());
        logger.info("counter path {}", counterPath);

        boolean isSequenceFile = JoinedFlatTable.SEQUENCEFILE.equalsIgnoreCase(envConfig.getFlatTableStorageFormat());

        // calculate source record bytes size
        final String bytesWrittenName = "byte-writer-counter";
        final String recordCounterName = "record-counter";

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        if (!StringUtil.isEmpty(enableObjectReuseOptValue) &&
                enableObjectReuseOptValue.equalsIgnoreCase("true")) {
            env.getConfig().enableObjectReuse();
        }

        DataSet<String[]> recordDataSet = FlinkUtil.readHiveRecords(isSequenceFile, env, inputPath, hiveTable, job);

        // read record from flat table
        // output:
        //   1, statistic
        //   2, field value of dict col
        //   3, min/max field value of not dict col
        DataSet<Tuple2<SelfDefineSortableKey, Text>> flatOutputDataSet = recordDataSet.mapPartition(
                new FlatOutputMapPartitionFunction(sConf, cubeName, segmentId, metaUrl, samplingPercent,
                        bytesWrittenName, recordCounterName));

        // repartition data, make each reducer handle only one col data or the statistic data
        DataSet<Tuple2<SelfDefineSortableKey, Text>> partitionDataSet = flatOutputDataSet
                .partitionCustom(new FactDistinctColumnPartitioner(cubeName, metaUrl, sConf), 0)
                .setParallelism(totalReducer);

        // multiple output result
        // 1, CFG_OUTPUT_COLUMN: field values of dict col, which will not be built in reducer, like globalDictCol
        // 2, CFG_OUTPUT_DICT: dictionary object built in reducer
        // 3, CFG_OUTPUT_STATISTICS: cube statistic: hll of cuboids ...
        // 4, CFG_OUTPUT_PARTITION: dimension value range(min,max)
        DataSet<Tuple2<String, Tuple3<Writable, Writable, String>>> outputDataSet = partitionDataSet
                .mapPartition(new MultiOutputMapPartitionFunction(sConf, cubeName, segmentId, metaUrl, samplingPercent))
                .setParallelism(totalReducer);

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

        outputDataSet.output(new HadoopMultipleOutputFormat(new LazyOutputFormat(), job));

        JobExecutionResult jobExecutionResult =
                env.execute("Fact distinct columns for:" + cubeName + " segment " + segmentId);
        Map<String, Object> accumulatorResults = jobExecutionResult.getAllAccumulatorResults();
        Long recordCount = (Long) accumulatorResults.get(recordCounterName);
        Long bytesWritten = (Long) accumulatorResults.get(bytesWrittenName);
        logger.info("Map input records={}", recordCount);
        logger.info("HDFS Read: {} HDFS Write", bytesWritten);
        logger.info("HDFS: Number of bytes written=" + FlinkBatchCubingJobBuilder2.getFileSize(outputPath, fs));

        Map<String, String> counterMap = Maps.newHashMap();
        counterMap.put(ExecutableConstants.SOURCE_RECORDS_COUNT, String.valueOf(recordCount));
        counterMap.put(ExecutableConstants.SOURCE_RECORDS_SIZE, String.valueOf(bytesWritten));

        // save counter to hdfs
        HadoopUtil.writeToSequenceFile(job.getConfiguration(), counterPath, counterMap);
    }


    static class FlatOutputMapPartitionFunction extends
            RichMapPartitionFunction<String[], Tuple2<SelfDefineSortableKey, Text>> {
        private String cubeName;
        private String segmentId;
        private String metaUrl;
        private SerializableConfiguration conf;
        private int samplingPercentage;
        private String bytesWrittenName;
        private String recordCounterName;
        private LongCounter bytesWrittenCounter;
        private LongCounter recordCounter;

        private FactDistinctColumnsBase base;

        public FlatOutputMapPartitionFunction(SerializableConfiguration conf, String cubeName, String segmentId,
                                              String metaUrl, int samplingPercentage, String bytesWrittenName,
                                              String recordCounterName) {
            this.cubeName = cubeName;
            this.segmentId = segmentId;
            this.metaUrl = metaUrl;
            this.conf = conf;
            this.samplingPercentage = samplingPercentage;
            this.bytesWrittenName = bytesWrittenName;
            this.recordCounterName = recordCounterName;
            this.bytesWrittenCounter = new LongCounter();
            this.recordCounter = new LongCounter();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            getRuntimeContext().addAccumulator(bytesWrittenName, bytesWrittenCounter);
            getRuntimeContext().addAccumulator(recordCounterName, recordCounter);
            base = new FactDistinctColumnsBase(cubeName, segmentId, metaUrl, conf, samplingPercentage);
            base.setupMap();
        }

        @Override
        public void mapPartition(Iterable<String[]> values, Collector<Tuple2<SelfDefineSortableKey, Text>> out) throws Exception {
            FactDistinctColumnsBase.Visitor visitor = new FactDistinctColumnsBase.Visitor<SelfDefineSortableKey, Text>() {
                @Override
                public void collect(String namedOutput, SelfDefineSortableKey key, Text value, String outputPath) {
                    out.collect(new Tuple2<>(key, value));
                }
            };

            for (String[] row : values) {
                bytesWrittenCounter.add(base.countSizeInBytes(row));
                recordCounter.add(1L);
                base.map(row, visitor);
            }

            base.postMap(visitor);
        }
    }

    static class FactDistinctColumnPartitioner implements Partitioner<SelfDefineSortableKey> {
        private transient volatile boolean initialized = false;
        private String cubeName;
        private String metaUrl;
        private SerializableConfiguration conf;
        private transient FactDistinctColumnsReducerMapping reducerMapping;

        public FactDistinctColumnPartitioner(String cubeName, String metaUrl, SerializableConfiguration conf) {
            this.cubeName = cubeName;
            this.metaUrl = metaUrl;
            this.conf = conf;
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
        public int partition(SelfDefineSortableKey key, int numPartitions) {
            if (initialized == false) {
                synchronized (FlinkFactDistinctColumns.class) {
                    if (initialized == false) {
                        init();
                    }
                }
            }

            Text keyText = key.getText();
            if (keyText.getBytes()[0] == FactDistinctColumnsReducerMapping.MARK_FOR_HLL_COUNTER) {
                Long cuboidId = Bytes.toLong(keyText.getBytes(), 1, Bytes.SIZEOF_LONG);
                return reducerMapping.getReducerIdForCuboidRowCount(cuboidId);
            } else {
                return BytesUtil.readUnsigned(keyText.getBytes(), 0, 1);
            }
        }
    }

    static class MultiOutputMapPartitionFunction extends
            RichMapPartitionFunction<Tuple2<SelfDefineSortableKey, Text>, Tuple2<String, Tuple3<Writable, Writable, String>>> {
        private String cubeName;
        private String segmentId;
        private String metaUrl;
        private SerializableConfiguration conf;
        private int samplingPercentage;

        private FactDistinctColumnsBase base;

        public MultiOutputMapPartitionFunction(SerializableConfiguration conf, String cubeName, String segmentId,
                                               String metaUrl, int samplingPercentage) {
            this.cubeName = cubeName;
            this.segmentId = segmentId;
            this.metaUrl = metaUrl;
            this.conf = conf;
            this.samplingPercentage = samplingPercentage;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            int taskId = getRuntimeContext().getIndexOfThisSubtask();
            base = new FactDistinctColumnsBase(cubeName, segmentId, metaUrl, conf, samplingPercentage);
            base.setupReduce(taskId);
        }

        @Override
        public void mapPartition(Iterable<Tuple2<SelfDefineSortableKey, Text>> values,
                                 Collector<Tuple2<String, Tuple3<Writable, Writable, String>>> out) throws Exception {
            FactDistinctColumnsBase.Visitor visitor = new FactDistinctColumnsBase.Visitor<Writable, Writable>() {
                @Override
                public void collect(String namedOutput, Writable key, Writable value, String outputPath) {
                    out.collect(new Tuple2<>(namedOutput, new Tuple3<>(key, value, outputPath)));
                }
            };

            for (Tuple2<SelfDefineSortableKey, Text> value : values) {
                base.reduce(new Pair<>(value.f0, value.f1), visitor);
            }

            base.postReduce(visitor);
        }
    }
}

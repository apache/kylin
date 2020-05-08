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

package org.apache.kylin.storage.hbase.steps;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.hadoopcompatibility.HadoopInputs;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.KeyValueSerialization;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.cube.model.HBaseColumnFamilyDesc;
import org.apache.kylin.engine.flink.FlinkBatchCubingJobBuilder2;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.SerializableConfiguration;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.measure.MeasureCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public class FlinkCubeHFile extends AbstractApplication implements Serializable {

    protected static final Logger logger = LoggerFactory.getLogger(FlinkCubeHFile.class);

    public static final Option OPTION_CUBE_NAME = OptionBuilder.withArgName(BatchConstants.ARG_CUBE_NAME).hasArg()
            .isRequired(true).withDescription("Cube Name").create(BatchConstants.ARG_CUBE_NAME);
    public static final Option OPTION_SEGMENT_ID = OptionBuilder.withArgName("segment").hasArg().isRequired(true)
            .withDescription("Cube Segment Id").create("segmentId");
    public static final Option OPTION_META_URL = OptionBuilder.withArgName("metaUrl").hasArg().isRequired(true)
            .withDescription("HDFS metadata url").create("metaUrl");
    public static final Option OPTION_OUTPUT_PATH = OptionBuilder.withArgName(BatchConstants.ARG_OUTPUT).hasArg()
            .isRequired(true).withDescription("HFile output path").create(BatchConstants.ARG_OUTPUT);
    public static final Option OPTION_INPUT_PATH = OptionBuilder.withArgName(BatchConstants.ARG_INPUT).hasArg()
            .isRequired(true).withDescription("Cuboid files PATH").create(BatchConstants.ARG_INPUT);
    public static final Option OPTION_PARTITION_FILE_PATH = OptionBuilder.withArgName(BatchConstants.ARG_PARTITION)
            .hasArg().isRequired(true).withDescription("Partition file path.").create(BatchConstants.ARG_PARTITION);
    public static final Option OPTION_COUNTER_PATH = OptionBuilder.withArgName(BatchConstants.ARG_COUNTER_OUTPUT).hasArg()
            .isRequired(true).withDescription("counter output path").create(BatchConstants.ARG_COUNTER_OUTPUT);
    public static final Option OPTION_ENABLE_OBJECT_REUSE = OptionBuilder.withArgName("enableObjectReuse").hasArg()
            .isRequired(false).withDescription("Enable object reuse").create("enableObjectReuse");

    private Options options;

    public FlinkCubeHFile() {
        options = new Options();
        options.addOption(OPTION_INPUT_PATH);
        options.addOption(OPTION_CUBE_NAME);
        options.addOption(OPTION_SEGMENT_ID);
        options.addOption(OPTION_META_URL);
        options.addOption(OPTION_OUTPUT_PATH);
        options.addOption(OPTION_PARTITION_FILE_PATH);
        options.addOption(AbstractHadoopJob.OPTION_HBASE_CONF_PATH);
        options.addOption(OPTION_COUNTER_PATH);
        options.addOption(OPTION_ENABLE_OBJECT_REUSE);
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        final String metaUrl = optionsHelper.getOptionValue(OPTION_META_URL);
        final String inputPath = optionsHelper.getOptionValue(OPTION_INPUT_PATH);
        final String cubeName = optionsHelper.getOptionValue(OPTION_CUBE_NAME);
        final String segmentId = optionsHelper.getOptionValue(OPTION_SEGMENT_ID);
        final String outputPath = optionsHelper.getOptionValue(OPTION_OUTPUT_PATH);
        final Path partitionFilePath = new Path(optionsHelper.getOptionValue(OPTION_PARTITION_FILE_PATH));
        final String hbaseConfFile = optionsHelper.getOptionValue(AbstractHadoopJob.OPTION_HBASE_CONF_PATH);
        final String counterPath = optionsHelper.getOptionValue(OPTION_COUNTER_PATH);
        final String enableObjectReuseOptValue = optionsHelper.getOptionValue(OPTION_ENABLE_OBJECT_REUSE);

        boolean enableObjectReuse = false;
        if (!StringUtil.isEmpty(enableObjectReuseOptValue) &&
                enableObjectReuseOptValue.equalsIgnoreCase("true")) {
            enableObjectReuse = true;
        }

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        if (enableObjectReuse) {
            env.getConfig().enableObjectReuse();
        }

        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        if (!fs.exists(partitionFilePath)) {
            throw new IllegalArgumentException("File not exist: " + partitionFilePath.toString());
        }

        Job job = Job.getInstance();
        HadoopUtil.deletePath(job.getConfiguration(), new Path(outputPath));

        final SerializableConfiguration sConf = new SerializableConfiguration(job.getConfiguration());
        final KylinConfig envConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(sConf, metaUrl);

        final CubeInstance cubeInstance = CubeManager.getInstance(envConfig).getCube(cubeName);
        final CubeDesc cubeDesc = cubeInstance.getDescriptor();
        final CubeSegment cubeSegment = cubeInstance.getSegmentById(segmentId);

        final MeasureCodec inputCodec = new MeasureCodec(cubeDesc.getMeasures());
        final List<KeyValueCreator> keyValueCreators = Lists.newArrayList();

        for (HBaseColumnFamilyDesc cfDesc : cubeDesc.getHbaseMapping().getColumnFamily()) {
            for (HBaseColumnDesc colDesc : cfDesc.getColumns()) {
                keyValueCreators.add(new KeyValueCreator(cubeDesc, colDesc));
            }
        }

        final int cfNum = keyValueCreators.size();
        final boolean quickPath = (keyValueCreators.size() == 1) && keyValueCreators.get(0).isFullCopy;

        logger.info("Input path: {}", inputPath);
        logger.info("Output path: {}", outputPath);
        // read partition split keys
        List<RowKeyWritable> keys = new ArrayList<>();
        try (SequenceFile.Reader reader = new SequenceFile.Reader(fs, partitionFilePath, job.getConfiguration())) {
            RowKeyWritable key = new RowKeyWritable();
            Writable value = NullWritable.get();
            while (reader.next(key, value)) {
                keys.add(key);
                logger.info(" ------- split key: {}", key);
                key = new RowKeyWritable(); // important, new an object!
            }
        }

        logger.info("There are {} split keys, totally {} hfiles", keys.size(), (keys.size() + 1));

        //HBase conf
        logger.info("Loading HBase configuration from:{}", hbaseConfFile);
        final Path hbaseConfFilePath = new Path(hbaseConfFile);
        final FileSystem hbaseClusterFs = hbaseConfFilePath.getFileSystem(job.getConfiguration());

        try (FSDataInputStream confInput = hbaseClusterFs.open(new Path(hbaseConfFile))) {
            Configuration hbaseJobConf = new Configuration();
            hbaseJobConf.addResource(confInput);
            hbaseJobConf.set("dfs.replication", "3"); // HFile, replication=3
            hbaseJobConf.setStrings("io.serializations", new String[]{hbaseJobConf.get("io.serializations"),
                    KeyValueSerialization.class.getName()});
            job = Job.getInstance(hbaseJobConf, cubeSegment.getStorageLocationIdentifier());
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            HadoopOutputFormat<ImmutableBytesWritable, Cell> hadoopOF =
                    new HadoopOutputFormat<>(new HFileOutputFormat3(), job);

            List<DataSet<Tuple2<Text, Text>>> mergingLevels = Lists.newArrayList();
            final int totalLevels = cubeSegment.getCuboidScheduler().getBuildLevel();
            for (int i = 0; i <= totalLevels; i++) {
                String cuboidInputPath = JobBuilderSupport.getCuboidOutputPathsByLevel(inputPath, i);
                DataSet<Tuple2<Text, Text>> levelDataSet = env.createInput(HadoopInputs.readHadoopFile(
                        new SequenceFileInputFormat(), Text.class, Text.class, cuboidInputPath));
                mergingLevels.add(levelDataSet);
            }
            if (mergingLevels.size() > 0) {
                DataSet<Tuple2<Text, Text>> inputDataSet = mergingLevels.get(0);
                for (int i = 1; i < mergingLevels.size(); i++) {
                    inputDataSet = inputDataSet.union(mergingLevels.get(i));
                }

                final DataSet<Tuple2<RowKeyWritable, KeyValue>> hfileDataSet;
                if (quickPath) {
                    hfileDataSet = inputDataSet.mapPartition(new RichMapPartitionFunction<Tuple2<Text, Text>, Tuple2<RowKeyWritable, KeyValue>>() {
                        @Override
                        public void mapPartition(Iterable<Tuple2<Text, Text>> values, Collector<Tuple2<RowKeyWritable, KeyValue>> out) throws Exception {
                            for (Tuple2<Text, Text> value : values) {
                                KeyValue outputValue = keyValueCreators.get(0).create(value.f0, value.f1.getBytes(), 0,
                                        value.f1.getLength());
                                out.collect(new Tuple2<>(new RowKeyWritable(outputValue.getKey()), outputValue));
                            }
                        }
                    });
                } else {
                    hfileDataSet = inputDataSet.mapPartition(new RichMapPartitionFunction<Tuple2<Text, Text>, Tuple2<RowKeyWritable, KeyValue>>() {
                        @Override
                        public void mapPartition(Iterable<Tuple2<Text, Text>> values, Collector<Tuple2<RowKeyWritable, KeyValue>> out) throws Exception {
                            for (Tuple2<Text, Text> value : values) {
                                Object[] inputMeasures = new Object[cubeDesc.getMeasures().size()];
                                inputCodec.decode(ByteBuffer.wrap(value.f1.getBytes(), 0, value.f1.getLength()),
                                        inputMeasures);

                                for (int i = 0; i < cfNum; i++) {
                                    KeyValue outputValue = keyValueCreators.get(i).create(value.f0, inputMeasures);
                                    out.collect(new Tuple2<>(new RowKeyWritable(outputValue.getKey()), outputValue));
                                }
                            }
                        }
                    });
                }

                hfileDataSet
                        .partitionCustom(new HFilePartitioner(keys), 0)
                        .sortPartition(0, Order.ASCENDING)
                        .mapPartition(new RichMapPartitionFunction<Tuple2<RowKeyWritable, KeyValue>, Tuple2<ImmutableBytesWritable, Cell>>() {
                            @Override
                            public void mapPartition(Iterable<Tuple2<RowKeyWritable, KeyValue>> values, Collector<Tuple2<ImmutableBytesWritable, Cell>> out) throws Exception {
                                for (Tuple2<RowKeyWritable, KeyValue> value : values) {
                                    out.collect(new Tuple2<>(new ImmutableBytesWritable(value.f1.getKey()), value.f1));
                                }
                            }
                        })
                        .output(hadoopOF);
            }
        }

        env.execute(String.format(Locale.ROOT, "Convert cuboid to hfile for cube: %s, segment %s", cubeName, segmentId));

        long size = FlinkBatchCubingJobBuilder2.getFileSize(outputPath, fs);
        logger.info("HDFS: Number of bytes written={}", size);
        Map<String, String> counterMap = Maps.newHashMap();
        counterMap.put(ExecutableConstants.HDFS_BYTES_WRITTEN, String.valueOf(size));
        // save counter to hdfs
        HadoopUtil.writeToSequenceFile(job.getConfiguration(), counterPath, counterMap);
    }

    class HFilePartitioner implements Partitioner<RowKeyWritable> {
        private List<RowKeyWritable> keys;

        public HFilePartitioner(List splitKeys) {
            keys = splitKeys;
        }

        @Override
        public int partition(RowKeyWritable key, int numPartitions) {
            int pos = Collections.binarySearch(this.keys, key) + 1;
            return pos < 0 ? -pos : pos;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            HFilePartitioner that = (HFilePartitioner) o;
            return Objects.equals(keys, that.keys);
        }

        @Override
        public int hashCode() {
            return Objects.hash(keys);
        }
    }

}

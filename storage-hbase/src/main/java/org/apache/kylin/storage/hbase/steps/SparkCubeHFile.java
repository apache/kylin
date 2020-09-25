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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.cube.model.HBaseColumnFamilyDesc;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.SerializableConfiguration;
import org.apache.kylin.engine.spark.KylinSparkJobListener;
import org.apache.kylin.engine.spark.SparkUtil;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.measure.MeasureCodec;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

import scala.Tuple2;

/**
 * Spark application to build cube with the "by-layer" algorithm. Only support source data from Hive; Metadata in HBase.
 */
public class SparkCubeHFile extends AbstractApplication implements Serializable {

    protected static final Logger logger = LoggerFactory.getLogger(SparkCubeHFile.class);

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

    private Options options;

    public SparkCubeHFile() {
        options = new Options();
        options.addOption(OPTION_INPUT_PATH);
        options.addOption(OPTION_CUBE_NAME);
        options.addOption(OPTION_SEGMENT_ID);
        options.addOption(OPTION_META_URL);
        options.addOption(OPTION_OUTPUT_PATH);
        options.addOption(OPTION_PARTITION_FILE_PATH);
        options.addOption(AbstractHadoopJob.OPTION_HBASE_CONF_PATH);
        options.addOption(OPTION_COUNTER_PATH);
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

        Class[] kryoClassArray = new Class[] { Class.forName("scala.reflect.ClassTag$$anon$1"), KeyValueCreator.class,
                KeyValue.class, RowKeyWritable.class };

        SparkConf conf = new SparkConf().setAppName("Converting HFile for:" + cubeName + " segment " + segmentId);
        //serialization conf
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "org.apache.kylin.engine.spark.KylinKryoRegistrator");
        conf.set("spark.kryo.registrationRequired", "true").registerKryoClasses(kryoClassArray);

        KylinSparkJobListener jobListener = new KylinSparkJobListener();
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            sc.sc().addSparkListener(jobListener);
            final FileSystem fs = partitionFilePath.getFileSystem(sc.hadoopConfiguration());
            if (!fs.exists(partitionFilePath)) {
                throw new IllegalArgumentException("File not exist: " + partitionFilePath.toString());
            }

            HadoopUtil.deletePath(sc.hadoopConfiguration(), new Path(outputPath));
            final SerializableConfiguration sConf = new SerializableConfiguration(sc.hadoopConfiguration());

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
            try (SequenceFile.Reader reader = new SequenceFile.Reader(fs, partitionFilePath, sc.hadoopConfiguration())) {
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
            final FileSystem hbaseClusterFs = hbaseConfFilePath.getFileSystem(sc.hadoopConfiguration());

            try (FSDataInputStream confInput = hbaseClusterFs.open(new Path(hbaseConfFile))) {
                Configuration hbaseJobConf = new Configuration();
                hbaseJobConf.addResource(confInput);
                hbaseJobConf.set("spark.hadoop.dfs.replication", "3"); // HFile, replication=3
                Job job = Job.getInstance(hbaseJobConf, cubeSegment.getStorageLocationIdentifier());

                FileOutputFormat.setOutputPath(job, new Path(outputPath));

                // inputPath has the same FileSystem as hbaseClusterFs when in HBase standalone mode
                JavaPairRDD<Text, Text> inputRDDs = SparkUtil.parseInputPath(inputPath, hbaseClusterFs, sc, Text.class,
                        Text.class);
                final JavaPairRDD<RowKeyWritable, KeyValue> hfilerdd;
                if (quickPath) {
                    hfilerdd = inputRDDs.mapToPair(new PairFunction<Tuple2<Text, Text>, RowKeyWritable, KeyValue>() {
                        @Override
                        public Tuple2<RowKeyWritable, KeyValue> call(Tuple2<Text, Text> textTextTuple2) throws Exception {
                            KeyValue outputValue = keyValueCreators.get(0).create(textTextTuple2._1,
                                    textTextTuple2._2.getBytes(), 0, textTextTuple2._2.getLength());
                            return new Tuple2<>(new RowKeyWritable(outputValue.createKeyOnly(false).getKey()), outputValue);
                        }
                    });
                } else {
                    hfilerdd = inputRDDs.flatMapToPair(new PairFlatMapFunction<Tuple2<Text, Text>, RowKeyWritable, KeyValue>() {
                        @Override
                        public Iterator<Tuple2<RowKeyWritable, KeyValue>> call(Tuple2<Text, Text> textTextTuple2)
                                throws Exception {

                            List<Tuple2<RowKeyWritable, KeyValue>> result = Lists.newArrayListWithExpectedSize(cfNum);
                            Object[] inputMeasures = new Object[cubeDesc.getMeasures().size()];
                            inputCodec.decode(ByteBuffer.wrap(textTextTuple2._2.getBytes(), 0, textTextTuple2._2.getLength()),
                                    inputMeasures);

                            for (int i = 0; i < cfNum; i++) {
                                KeyValue outputValue = keyValueCreators.get(i).create(textTextTuple2._1, inputMeasures);
                                result.add(new Tuple2<>(new RowKeyWritable(outputValue.createKeyOnly(false).getKey()),
                                        outputValue));
                            }

                            return result.iterator();
                        }
                    });
                }

                hfilerdd.repartitionAndSortWithinPartitions(new HFilePartitioner(keys),
                        RowKeyWritable.RowKeyComparator.INSTANCE)
                        .mapToPair(new PairFunction<Tuple2<RowKeyWritable, KeyValue>, ImmutableBytesWritable, KeyValue>() {
                            @Override
                            public Tuple2<ImmutableBytesWritable, KeyValue> call(
                                    Tuple2<RowKeyWritable, KeyValue> rowKeyWritableKeyValueTuple2) throws Exception {
                                return new Tuple2<>(new ImmutableBytesWritable(rowKeyWritableKeyValueTuple2._2.getKey()),
                                        rowKeyWritableKeyValueTuple2._2);
                            }
                        }).saveAsNewAPIHadoopDataset(job.getConfiguration());
            }

            logger.info("HDFS: Number of bytes written={}", jobListener.metrics.getBytesWritten());

            Map<String, String> counterMap = Maps.newHashMap();
            counterMap.put(ExecutableConstants.HDFS_BYTES_WRITTEN, String.valueOf(jobListener.metrics.getBytesWritten()));

            // save counter to hdfs
            HadoopUtil.writeToSequenceFile(sc.hadoopConfiguration(), counterPath, counterMap);
        }
    }

    static class HFilePartitioner extends Partitioner {
        private List<RowKeyWritable> keys;

        public HFilePartitioner(List splitKyes) {
            keys = splitKyes;
        }

        @Override
        public int numPartitions() {
            return keys.size() + 1;
        }

        @Override
        public int getPartition(Object o) {
            int pos = Collections.binarySearch(this.keys, (RowKeyWritable) o) + 1;
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

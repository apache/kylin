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
package org.apache.kylin.storage.parquet.steps;

import com.google.common.collect.Maps;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.dimension.IDimensionEncodingMap;
import org.apache.kylin.engine.mr.BatchCubingJobBuilder2;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.SerializableConfiguration;
import org.apache.kylin.engine.spark.KylinSparkJobListener;
import org.apache.kylin.engine.spark.SparkUtil;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;


public class SparkCubeParquet extends AbstractApplication implements Serializable{

    protected static final Logger logger = LoggerFactory.getLogger(SparkCubeParquet.class);

    public static final Option OPTION_CUBE_NAME = OptionBuilder.withArgName(BatchConstants.ARG_CUBE_NAME).hasArg()
            .isRequired(true).withDescription("Cube Name").create(BatchConstants.ARG_CUBE_NAME);
    public static final Option OPTION_SEGMENT_ID = OptionBuilder.withArgName("segment").hasArg().isRequired(true)
            .withDescription("Cube Segment Id").create("segmentId");
    public static final Option OPTION_META_URL = OptionBuilder.withArgName("metaUrl").hasArg().isRequired(true)
            .withDescription("HDFS metadata url").create("metaUrl");
    public static final Option OPTION_OUTPUT_PATH = OptionBuilder.withArgName(BatchConstants.ARG_OUTPUT).hasArg()
            .isRequired(true).withDescription("Paqruet output path").create(BatchConstants.ARG_OUTPUT);
    public static final Option OPTION_INPUT_PATH = OptionBuilder.withArgName(BatchConstants.ARG_INPUT).hasArg()
            .isRequired(true).withDescription("Cuboid files PATH").create(BatchConstants.ARG_INPUT);
    public static final Option OPTION_COUNTER_PATH = OptionBuilder.withArgName(BatchConstants.ARG_COUNTER_OUPUT).hasArg()
            .isRequired(true).withDescription("Counter output path").create(BatchConstants.ARG_COUNTER_OUPUT);

    private Options options;

    public SparkCubeParquet(){
        options = new Options();
        options.addOption(OPTION_INPUT_PATH);
        options.addOption(OPTION_CUBE_NAME);
        options.addOption(OPTION_SEGMENT_ID);
        options.addOption(OPTION_META_URL);
        options.addOption(OPTION_OUTPUT_PATH);
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
        final String counterPath = optionsHelper.getOptionValue(OPTION_COUNTER_PATH);

        Class[] kryoClassArray = new Class[] { Class.forName("scala.reflect.ClassTag$$anon$1"), Text.class, Group.class};

        SparkConf conf = new SparkConf().setAppName("Converting Parquet File for: " + cubeName + " segment " + segmentId);
        //serialization conf
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "org.apache.kylin.engine.spark.KylinKryoRegistrator");
        conf.set("spark.kryo.registrationRequired", "true").registerKryoClasses(kryoClassArray);


        KylinSparkJobListener jobListener = new KylinSparkJobListener();
        try (JavaSparkContext sc = new JavaSparkContext(conf)){
            sc.sc().addSparkListener(jobListener);

            HadoopUtil.deletePath(sc.hadoopConfiguration(), new Path(outputPath));
            final SerializableConfiguration sConf = new SerializableConfiguration(sc.hadoopConfiguration());

            final KylinConfig envConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(sConf, metaUrl);

            final CubeInstance cubeInstance = CubeManager.getInstance(envConfig).getCube(cubeName);
            final CubeSegment cubeSegment = cubeInstance.getSegmentById(segmentId);

            final FileSystem fs = new Path(inputPath).getFileSystem(sc.hadoopConfiguration());

            final int totalLevels = cubeSegment.getCuboidScheduler().getBuildLevel();
            JavaPairRDD<Text, Text>[] allRDDs = new JavaPairRDD[totalLevels + 1];

            final Job job = Job.getInstance(sConf.get());
            logger.info("Input path: {}", inputPath);
            logger.info("Output path: {}", outputPath);

            final Map<TblColRef, String> colTypeMap = Maps.newHashMap();
            final Map<MeasureDesc, String> meaTypeMap = Maps.newHashMap();

            final IDimensionEncodingMap dimEncMap = cubeSegment.getDimensionEncodingMap();

            Cuboid baseCuboid = Cuboid.getBaseCuboid(cubeSegment.getCubeDesc());
            MessageType schema = ParquetConvertor.cuboidToMessageType(baseCuboid, dimEncMap, cubeSegment.getCubeDesc());
            ParquetConvertor.generateTypeMap(baseCuboid, dimEncMap, cubeSegment.getCubeDesc(), colTypeMap, meaTypeMap);
            GroupWriteSupport.setSchema(schema, job.getConfiguration());

            GenerateGroupRDDFunction groupPairFunction = new GenerateGroupRDDFunction(cubeName, cubeSegment.getUuid(), metaUrl, new SerializableConfiguration(job.getConfiguration()), colTypeMap, meaTypeMap);


            logger.info("Schema: {}", schema.toString());

            // Read from cuboid and save to parquet
            for (int level = 0; level <= totalLevels; level++) {
                String cuboidPath = BatchCubingJobBuilder2.getCuboidOutputPathsByLevel(inputPath, level);
                allRDDs[level] = SparkUtil.parseInputPath(cuboidPath, fs, sc, Text.class, Text.class);
                saveToParquet(allRDDs[level], groupPairFunction, cubeSegment, outputPath, level, job, envConfig);
            }

            logger.info("HDFS: Number of bytes written={}", jobListener.metrics.getBytesWritten());

            Map<String, String> counterMap = Maps.newHashMap();
            counterMap.put(ExecutableConstants.HDFS_BYTES_WRITTEN, String.valueOf(jobListener.metrics.getBytesWritten()));

            // save counter to hdfs
            HadoopUtil.writeToSequenceFile(sc.hadoopConfiguration(), counterPath, counterMap);
        }

    }

    protected void saveToParquet(JavaPairRDD<Text, Text> rdd, GenerateGroupRDDFunction groupRDDFunction, CubeSegment cubeSeg, String parquetOutput, int level, Job job, KylinConfig kylinConfig) throws IOException {
        final CuboidToPartitionMapping cuboidToPartitionMapping = new CuboidToPartitionMapping(cubeSeg, kylinConfig, level);

        logger.info("CuboidToPartitionMapping: {}", cuboidToPartitionMapping.toString());

        String output = BatchCubingJobBuilder2.getCuboidOutputPathsByLevel(parquetOutput, level);

        job.setOutputFormatClass(CustomParquetOutputFormat.class);
        CustomParquetOutputFormat.setOutputPath(job, new Path(output));
        CustomParquetOutputFormat.setWriteSupportClass(job, GroupWriteSupport.class);
        CustomParquetOutputFormat.setCuboidToPartitionMapping(job, cuboidToPartitionMapping);

        JavaPairRDD<Void, Group> groupRDD = rdd.partitionBy(new CuboidPartitioner(cuboidToPartitionMapping)).mapToPair(groupRDDFunction);

        groupRDD.saveAsNewAPIHadoopDataset(job.getConfiguration());
    }

    static class CuboidPartitioner extends Partitioner {
        private CuboidToPartitionMapping mapping;

        public CuboidPartitioner(CuboidToPartitionMapping cuboidToPartitionMapping) {
            this.mapping = cuboidToPartitionMapping;
        }

        @Override
        public int numPartitions() {
            return mapping.getNumPartitions();
        }

        @Override
        public int getPartition(Object key) {
            Text textKey = (Text)key;
            return mapping.getPartitionByKey(textKey.getBytes());
        }
    }

    public static class CustomParquetOutputFormat extends ParquetOutputFormat {

        @Override
        public Path getDefaultWorkFile(TaskAttemptContext context, String extension) throws IOException {
            FileOutputCommitter committer = (FileOutputCommitter)this.getOutputCommitter(context);
            TaskID taskId = context.getTaskAttemptID().getTaskID();
            int partition = taskId.getId();

            CuboidToPartitionMapping mapping = CuboidToPartitionMapping.deserialize(context.getConfiguration().get(BatchConstants.ARG_CUBOID_TO_PARTITION_MAPPING));

            return new Path(committer.getWorkPath(), getUniqueFile(context, mapping.getPartitionFilePrefix(partition)+ "-" + getOutputName(context), extension));
        }

        public static void setCuboidToPartitionMapping(Job job, CuboidToPartitionMapping cuboidToPartitionMapping) throws IOException {
            String jsonStr = cuboidToPartitionMapping.serialize();

            job.getConfiguration().set(BatchConstants.ARG_CUBOID_TO_PARTITION_MAPPING, jsonStr);
        }
    }

    static class GenerateGroupRDDFunction implements PairFunction<Tuple2<Text, Text>, Void, Group> {
        private volatile transient boolean initialized = false;
        private String cubeName;
        private String segmentId;
        private String metaUrl;
        private SerializableConfiguration conf;
        private Map<TblColRef, String> colTypeMap;
        private Map<MeasureDesc, String> meaTypeMap;

        private transient ParquetConvertor convertor;

        public GenerateGroupRDDFunction(String cubeName, String segmentId, String metaurl, SerializableConfiguration conf, Map<TblColRef, String> colTypeMap, Map<MeasureDesc, String> meaTypeMap) {
            this.cubeName = cubeName;
            this.segmentId = segmentId;
            this.metaUrl = metaurl;
            this.conf = conf;
            this.colTypeMap = colTypeMap;
            this.meaTypeMap = meaTypeMap;
        }

        private void init() {
            KylinConfig kylinConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(conf, metaUrl);
            convertor = new ParquetConvertor(cubeName, segmentId, kylinConfig, conf, colTypeMap, meaTypeMap);
        }

        @Override
        public Tuple2<Void, Group> call(Tuple2<Text, Text> tuple) throws Exception {
            if (initialized == false) {
                synchronized (SparkCubeParquet.class) {
                    if (initialized == false) {
                        init();
                        initialized = true;
                    }
                }
            }
            Group group = convertor.parseValueToGroup(tuple._1(), tuple._2());

            return new Tuple2<>(null, group);
        }
    }
}

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

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.StringSplitter;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.BatchCubingJobBuilder2;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.CubeStatsReader;
import org.apache.kylin.engine.mr.common.SerializableConfiguration;
import org.apache.kylin.engine.mr.steps.SegmentReEncoder;
import org.apache.kylin.measure.BufferedMeasureCodec;
import org.apache.kylin.measure.MeasureAggregators;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;

/**
 */
public class SparkCubingMerge extends AbstractApplication implements Serializable {

    protected static final Logger logger = LoggerFactory.getLogger(SparkCubingMerge.class);

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

    private Options options;

    private String cubeName;
    private String metaUrl;

    public SparkCubingMerge() {
        options = new Options();
        options.addOption(OPTION_META_URL);
        options.addOption(OPTION_CUBE_NAME);
        options.addOption(OPTION_SEGMENT_ID);
        options.addOption(OPTION_INPUT_PATH);
        options.addOption(OPTION_OUTPUT_PATH);
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        this.metaUrl = optionsHelper.getOptionValue(OPTION_META_URL);
        this.cubeName = optionsHelper.getOptionValue(OPTION_CUBE_NAME);
        final String inputPath = optionsHelper.getOptionValue(OPTION_INPUT_PATH);
        final String segmentId = optionsHelper.getOptionValue(OPTION_SEGMENT_ID);
        final String outputPath = optionsHelper.getOptionValue(OPTION_OUTPUT_PATH);

        Class[] kryoClassArray = new Class[] { Class.forName("scala.reflect.ClassTag$$anon$1") };

        SparkConf conf = new SparkConf().setAppName("Merge segments for cube:" + cubeName + ", segment " + segmentId);
        //serialization conf
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "org.apache.kylin.engine.spark.KylinKryoRegistrator");
        conf.set("spark.kryo.registrationRequired", "true").registerKryoClasses(kryoClassArray);

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            SparkUtil.modifySparkHadoopConfiguration(sc.sc()); // set dfs.replication=2 and enable compress
            KylinSparkJobListener jobListener = new KylinSparkJobListener();
            sc.sc().addSparkListener(jobListener);

            HadoopUtil.deletePath(sc.hadoopConfiguration(), new Path(outputPath));
            final SerializableConfiguration sConf = new SerializableConfiguration(sc.hadoopConfiguration());
            final KylinConfig envConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(sConf, metaUrl);

            final CubeInstance cubeInstance = CubeManager.getInstance(envConfig).getCube(cubeName);
            final CubeDesc cubeDesc = CubeDescManager.getInstance(envConfig).getCubeDesc(cubeInstance.getDescName());
            final CubeSegment cubeSegment = cubeInstance.getSegmentById(segmentId);
            final CubeStatsReader cubeStatsReader = new CubeStatsReader(cubeSegment, envConfig);

            logger.info("Input path: {}", inputPath);
            logger.info("Output path: {}", outputPath);

            final Job job = Job.getInstance(sConf.get());

            SparkUtil.setHadoopConfForCuboid(job, cubeSegment, metaUrl);

            final MeasureAggregators aggregators = new MeasureAggregators(cubeDesc.getMeasures());
            final Function2 reduceFunction = new Function2<Object[], Object[], Object[]>() {
                @Override
                public Object[] call(Object[] input1, Object[] input2) throws Exception {
                    Object[] measureObjs = new Object[input1.length];
                    aggregators.aggregate(input1, input2, measureObjs);
                    return measureObjs;
                }
            };

            final PairFunction convertTextFunction = new PairFunction<Tuple2<Text, Object[]>, org.apache.hadoop.io.Text, org.apache.hadoop.io.Text>() {
                private transient volatile boolean initialized = false;
                BufferedMeasureCodec codec;

                @Override
                public Tuple2<org.apache.hadoop.io.Text, org.apache.hadoop.io.Text> call(Tuple2<Text, Object[]> tuple2)
                        throws Exception {

                    if (initialized == false) {
                        synchronized (SparkCubingMerge.class) {
                            if (initialized == false) {
                                synchronized (SparkCubingMerge.class) {
                                    if (initialized == false) {
                                        KylinConfig kylinConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(sConf, metaUrl);
                                        try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig
                                                .setAndUnsetThreadLocalConfig(kylinConfig)) {
                                            CubeDesc desc = CubeDescManager.getInstance(kylinConfig).getCubeDesc(cubeName);
                                            codec = new BufferedMeasureCodec(desc.getMeasures());
                                            initialized = true;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    ByteBuffer valueBuf = codec.encode(tuple2._2());
                    byte[] encodedBytes = new byte[valueBuf.position()];
                    System.arraycopy(valueBuf.array(), 0, encodedBytes, 0, valueBuf.position());
                    return new Tuple2<>(tuple2._1(), new org.apache.hadoop.io.Text(encodedBytes));
                }
            };

            final int totalLevels = cubeSegment.getCuboidScheduler().getBuildLevel();
            final String[] inputFolders = StringSplitter.split(inputPath, ",");
            FileSystem fs = HadoopUtil.getWorkingFileSystem();
            boolean isLegacyMode = false;
            for (String inputFolder : inputFolders) {
                Path baseCuboidPath = new Path(BatchCubingJobBuilder2.getCuboidOutputPathsByLevel(inputFolder, 0));
                if (fs.exists(baseCuboidPath) == false) {
                    // doesn't exist sub folder, that means the merged cuboid in one folder (not by layer)
                    isLegacyMode = true;
                    break;
                }
            }

            if (isLegacyMode == true) {
                // merge all layer's cuboid at once, this might be hard for Spark
                List<JavaPairRDD<Text, Object[]>> mergingSegs = Lists.newArrayListWithExpectedSize(inputFolders.length);
                for (int i = 0; i < inputFolders.length; i++) {
                    String path = inputFolders[i];
                    JavaPairRDD segRdd = SparkUtil.parseInputPath(path, fs, sc, Text.class, Text.class);
                    CubeSegment sourceSegment = findSourceSegment(path, cubeInstance);
                    // re-encode with new dictionaries
                    JavaPairRDD<Text, Object[]> newEcoddedRdd = segRdd.mapToPair(new ReEncodeCuboidFunction(cubeName,
                            sourceSegment.getUuid(), cubeSegment.getUuid(), metaUrl, sConf));
                    mergingSegs.add(newEcoddedRdd);
                }

                FileOutputFormat.setOutputPath(job, new Path(outputPath));
                sc.union(mergingSegs.toArray(new JavaPairRDD[mergingSegs.size()]))
                        .reduceByKey(reduceFunction, SparkUtil.estimateTotalPartitionNum(cubeStatsReader, envConfig))
                        .mapToPair(convertTextFunction).saveAsNewAPIHadoopDataset(job.getConfiguration());

            } else {
                // merge by layer
                for (int level = 0; level <= totalLevels; level++) {
                    List<JavaPairRDD<Text, Object[]>> mergingSegs = Lists.newArrayList();
                    for (int i = 0; i < inputFolders.length; i++) {
                        String path = inputFolders[i];
                        CubeSegment sourceSegment = findSourceSegment(path, cubeInstance);
                        final String cuboidInputPath = BatchCubingJobBuilder2.getCuboidOutputPathsByLevel(path, level);
                        JavaPairRDD<Text, Text> segRdd = sc.sequenceFile(cuboidInputPath, Text.class, Text.class);
                        // re-encode with new dictionaries
                        JavaPairRDD<Text, Object[]> newEcoddedRdd = segRdd.mapToPair(new ReEncodeCuboidFunction(cubeName,
                                sourceSegment.getUuid(), cubeSegment.getUuid(), metaUrl, sConf));
                        mergingSegs.add(newEcoddedRdd);
                    }

                    final String cuboidOutputPath = BatchCubingJobBuilder2.getCuboidOutputPathsByLevel(outputPath, level);
                    FileOutputFormat.setOutputPath(job, new Path(cuboidOutputPath));

                    sc.union(mergingSegs.toArray(new JavaPairRDD[mergingSegs.size()]))
                            .reduceByKey(reduceFunction,
                                    SparkUtil.estimateLayerPartitionNum(level, cubeStatsReader, envConfig))
                            .mapToPair(convertTextFunction).saveAsNewAPIHadoopDataset(job.getConfiguration());
                }
            }
            // output the data size to console, job engine will parse and save the metric
            // please note: this mechanism won't work when spark.submit.deployMode=cluster
            logger.info("HDFS: Number of bytes written={}", jobListener.metrics.getBytesWritten());
        }
    }

    static class ReEncodeCuboidFunction implements PairFunction<Tuple2<Text, Text>, Text, Object[]> {
        private transient volatile boolean initialized = false;
        private String cubeName;
        private String sourceSegmentId;
        private String mergedSegmentId;
        private String metaUrl;
        private SerializableConfiguration conf;
        private transient KylinConfig kylinConfig;
        private transient SegmentReEncoder segmentReEncoder = null;

        ReEncodeCuboidFunction(String cubeName, String sourceSegmentId, String mergedSegmentId, String metaUrl,
                               SerializableConfiguration conf) {
            this.cubeName = cubeName;
            this.sourceSegmentId = sourceSegmentId;
            this.mergedSegmentId = mergedSegmentId;
            this.metaUrl = metaUrl;
            this.conf = conf;
        }

        private void init() {
            this.kylinConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(conf, metaUrl);
            final CubeInstance cube = CubeManager.getInstance(kylinConfig).getCube(cubeName);
            final CubeDesc cubeDesc = CubeDescManager.getInstance(kylinConfig).getCubeDesc(cube.getDescName());
            final CubeSegment sourceSeg = cube.getSegmentById(sourceSegmentId);
            final CubeSegment mergedSeg = cube.getSegmentById(mergedSegmentId);
            this.segmentReEncoder = new SegmentReEncoder(cubeDesc, sourceSeg, mergedSeg, kylinConfig);
        }

        @Override
        public Tuple2<Text, Object[]> call(Tuple2<Text, Text> textTextTuple2) throws Exception {
            if (initialized == false) {
                synchronized (ReEncodeCuboidFunction.class) {
                    if (initialized == false) {
                        init();
                        initialized = true;
                    }
                }
            }
            Pair<Text, Object[]> encodedPair = segmentReEncoder.reEncode2(textTextTuple2._1, textTextTuple2._2);
            return new Tuple2(encodedPair.getFirst(), encodedPair.getSecond());
        }
    }

    private CubeSegment findSourceSegment(String filePath, CubeInstance cube) {
        String jobID = JobBuilderSupport.extractJobIDFromPath(filePath);
        return CubeInstance.findSegmentWithJobId(jobID, cube);
    }

}

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

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.common.RowKeySplitter;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.cube.cuboid.CuboidUtil;
import org.apache.kylin.cube.kv.RowKeyEncoderProvider;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.BatchCubingJobBuilder2;
import org.apache.kylin.engine.mr.IMROutput2;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.CubeStatsReader;
import org.apache.kylin.engine.mr.common.CuboidSchedulerUtil;
import org.apache.kylin.engine.mr.common.NDCuboidBuilder;
import org.apache.kylin.engine.mr.common.SerializableConfiguration;
import org.apache.kylin.measure.BufferedMeasureCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class SparkCubingByLayerForOpt extends AbstractApplication implements Serializable {

    protected static final Logger logger = LoggerFactory.getLogger(SparkCubingByLayerForOpt.class);

    public static final Option OPTION_CUBE_NAME = OptionBuilder.withArgName(BatchConstants.ARG_CUBE_NAME).hasArg()
            .isRequired(true).withDescription("Cube Name").create(BatchConstants.ARG_CUBE_NAME);
    public static final Option OPTION_SEGMENT_ID = OptionBuilder.withArgName("segment").hasArg().isRequired(true)
            .withDescription("Cube Segment Id").create(BatchConstants.ARG_SEGMENT_ID);
    public static final Option OPTION_META_URL = OptionBuilder.withArgName("metaUrl").hasArg().isRequired(true)
            .withDescription("HDFS metadata url").create(BatchConstants.ARG_META_URL);
    public static final Option OPTION_OUTPUT_PATH = OptionBuilder.withArgName(BatchConstants.ARG_OUTPUT).hasArg()
            .isRequired(true).withDescription("Cube output path").create(BatchConstants.ARG_OUTPUT);
    public static final Option OPTION_INPUT_PATH = OptionBuilder.withArgName(BatchConstants.ARG_INPUT).hasArg()
            .isRequired(true).withDescription("Hive Intermediate Table PATH").create(BatchConstants.ARG_INPUT);
    public static final Option OPTION_CUBOID_MODE = OptionBuilder.withArgName(BatchConstants.ARG_CUBOID_MODE).hasArg()
            .isRequired(true).withDescription("CoboId Mode ").create(BatchConstants.ARG_CUBOID_MODE);

    private Options options;

    public SparkCubingByLayerForOpt() {
        options = new Options();
        options.addOption(OPTION_CUBOID_MODE);
        options.addOption(OPTION_INPUT_PATH);
        options.addOption(OPTION_CUBE_NAME);
        options.addOption(OPTION_SEGMENT_ID);
        options.addOption(OPTION_META_URL);
        options.addOption(OPTION_OUTPUT_PATH);
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        String metaUrl = optionsHelper.getOptionValue(OPTION_META_URL);
        String inputPath = optionsHelper.getOptionValue(OPTION_INPUT_PATH);
        String cubeName = optionsHelper.getOptionValue(OPTION_CUBE_NAME);
        String segmentId = optionsHelper.getOptionValue(OPTION_SEGMENT_ID);
        String outputPath = optionsHelper.getOptionValue(OPTION_OUTPUT_PATH);
        String cuboidMode = optionsHelper.getOptionValue(OPTION_CUBOID_MODE);

        SparkConf sparkConf = SparkUtil.setKryoSerializerInConf();
        sparkConf.setAppName("Kylin_Cubing_For_Optimize_" + cubeName + "_With_Spark");

        KylinSparkJobListener jobListener = new KylinSparkJobListener();
        try (JavaSparkContext sc = new JavaSparkContext(sparkConf)) {
            SparkUtil.modifySparkHadoopConfiguration(sc.sc(), AbstractHadoopJob
                    .loadKylinConfigFromHdfs(new SerializableConfiguration(sc.hadoopConfiguration()), metaUrl)); // set dfs.replication and enable compress
            sc.sc().addSparkListener(jobListener);

            final SerializableConfiguration sConf = new SerializableConfiguration(sc.hadoopConfiguration());
            final KylinConfig envConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(sConf, metaUrl);

            final CubeInstance cubeInstance = CubeManager.getInstance(envConfig).getCube(cubeName);
            final CubeDesc cubeDesc = CubeDescManager.getInstance(envConfig).getCubeDesc(cubeInstance.getDescName());
            final CubeSegment cubeSegment = cubeInstance.getSegmentById(segmentId);
            final CubeStatsReader cubeStatsReader = new CubeStatsReader(cubeSegment, envConfig);

            final Job job = Job.getInstance(sConf.get());
            SparkUtil.setHadoopConfForCuboid(job, cubeSegment, metaUrl);

            StorageLevel storageLevel = StorageLevel.fromString(envConfig.getSparkStorageLevel());
            JavaPairRDD<ByteArray, Object[]> baseCuboIdRDD = SparkUtil.getCuboIdRDDFromHdfs(sc, metaUrl, cubeName,
                    cubeSegment, inputPath, cubeDesc.getMeasures().size(), sConf);

            // Don't know statistics so that tree cuboid scheduler is not determined. Determine the maxLevel at runtime
            final Set<Long> cuboidsByMode = cubeSegment.getCubeInstance().getCuboidsByMode(cuboidMode);
            final int maxLevel = CuboidUtil.getLongestDepth(cuboidsByMode);

            logger.info("cuboidMode" + cuboidMode);
            logger.info("maxLevel" + maxLevel);

            CuboidScheduler scheduler = CuboidSchedulerUtil.getCuboidSchedulerByMode(cubeSegment, cuboidMode);
            
            JavaPairRDD<ByteArray, Object[]>[] allRDDs = new JavaPairRDD[maxLevel + 1];
            allRDDs[0] = baseCuboIdRDD;

            SparkCubingByLayer.BaseCuboidReducerFunction2 reducerFunction2 = new SparkCubingByLayer.BaseCuboidReducerFunction2(
                    cubeName, metaUrl, sConf);

            boolean allNormalMeasure = true;
            boolean[] needAggr = new boolean[cubeDesc.getMeasures().size()];
            for (int i = 0; i < cubeDesc.getMeasures().size(); i++) {
                needAggr[i] = !cubeDesc.getMeasures().get(i).getFunction().getMeasureType().onlyAggrInBaseCuboid();
                allNormalMeasure = allNormalMeasure && needAggr[i];
            }

            if (!allNormalMeasure) {
                reducerFunction2 = new SparkCubingByLayer.CuboidReducerFunction2(cubeName, metaUrl, sConf, needAggr);
            }

            for (int i = 1; i <= maxLevel; i++) {
                int partition = SparkUtil.estimateLayerPartitionNum(i, cubeStatsReader, envConfig);
                allRDDs[i] = allRDDs[i - 1]
                        .flatMapToPair(new CuboidFlatMap(cubeName, segmentId, metaUrl, sConf, scheduler))
                        .reduceByKey(reducerFunction2, partition);
                allRDDs[i].persist(storageLevel);
                saveToHDFS(allRDDs[i], metaUrl, cubeName, cubeSegment, outputPath, i, job);
                // must do unpersist after allRDDs[level] is created, otherwise this RDD will be recomputed
                allRDDs[i - 1].unpersist(false);
            }
            allRDDs[maxLevel].unpersist(false);
            logger.info("Finished on calculating needed cuboids For Optimize.");
            logger.info("HDFS: Number of bytes written=" + jobListener.metrics.getBytesWritten());

        }

    }

    static public class CuboidFlatMap
            extends SparkFunction.PairFlatMapFunctionBase<Tuple2<ByteArray, Object[]>, ByteArray, Object[]> {

        private String cubeName;
        private String segmentId;
        private String metaUrl;
        private CubeSegment cubeSegment;
        private CubeDesc cubeDesc;
        private NDCuboidBuilder ndCuboidBuilder;
        private RowKeySplitter rowKeySplitter;
        private SerializableConfiguration conf;
        private CuboidScheduler cuboidScheduler;

        public CuboidFlatMap(String cubeName, String segmentId, String metaUrl, SerializableConfiguration conf,
                CuboidScheduler scheduler) {
            this.cubeName = cubeName;
            this.segmentId = segmentId;
            this.metaUrl = metaUrl;
            this.conf = conf;
            this.cuboidScheduler = scheduler;
        }

        @Override
        protected void doInit() {
            KylinConfig kConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(conf, metaUrl);
            try (KylinConfig.SetAndUnsetThreadLocalConfig autoClose = KylinConfig.setAndUnsetThreadLocalConfig(kConfig)) {
                CubeInstance cubeInstance = CubeManager.getInstance(kConfig).getCube(cubeName);
                this.cubeSegment = cubeInstance.getSegmentById(segmentId);
                this.cubeDesc = cubeInstance.getDescriptor();
                this.rowKeySplitter = new RowKeySplitter(cubeSegment);
                this.ndCuboidBuilder = new NDCuboidBuilder(cubeSegment, new RowKeyEncoderProvider(cubeSegment));
            }
        }

        @Override
        public Iterator<Tuple2<ByteArray, Object[]>> doCall(final Tuple2<ByteArray, Object[]> tuple2) throws Exception {
            byte[] key = tuple2._1().array();
            long cuboidId = rowKeySplitter.parseCuboid(key);
            final List<Long> myChildren = cuboidScheduler.getSpanningCuboid(cuboidId);

            // if still empty or null
            if (myChildren == null || myChildren.size() == 0) {
                return EMTPY_ITERATOR.iterator();
            }
            rowKeySplitter.split(key);
            final Cuboid parentCuboid = Cuboid.findForMandatory(cubeDesc, cuboidId);

            List<Tuple2<ByteArray, Object[]>> tuples = new ArrayList(myChildren.size());
            for (Long child : myChildren) {
                Cuboid childCuboid = Cuboid.findForMandatory(cubeDesc, child);
                ByteArray result = ndCuboidBuilder.buildKey2(parentCuboid, childCuboid,
                        rowKeySplitter.getSplitBuffers());

                tuples.add(new Tuple2<>(result, tuple2._2()));
            }

            return tuples.iterator();
        }
    }

    private static final java.lang.Iterable<Tuple2<ByteArray, Object[]>> EMTPY_ITERATOR = new ArrayList(0);

    protected void saveToHDFS(final JavaPairRDD<ByteArray, Object[]> rdd, final String metaUrl, final String cubeName,
            final CubeSegment cubeSeg, final String hdfsBaseLocation, final int level, final Job job) throws Exception {
        final String cuboidOutputPath = BatchCubingJobBuilder2.getCuboidOutputPathsByLevel(hdfsBaseLocation, level);
        final SerializableConfiguration sConf = new SerializableConfiguration(job.getConfiguration());

        IMROutput2.IMROutputFormat outputFormat = MRUtil.getBatchCubingOutputSide2(cubeSeg).getOutputFormat();
        outputFormat.configureJobOutput(job, cuboidOutputPath, cubeSeg, cubeSeg.getCuboidScheduler(), level);

        rdd.mapToPair(new SparkFunction.PairFunctionBase<Tuple2<ByteArray, Object[]>, Text, Text>() {
            private BufferedMeasureCodec codec;

            @Override
            protected void doInit() {
                KylinConfig kylinConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(sConf, metaUrl);
                try (KylinConfig.SetAndUnsetThreadLocalConfig autoClose = KylinConfig.setAndUnsetThreadLocalConfig(kylinConfig)) {
                    CubeDesc desc = CubeDescManager.getInstance(kylinConfig).getCubeDesc(cubeName);
                    codec = new BufferedMeasureCodec(desc.getMeasures());
                }
            }

            @Override
            public Tuple2<org.apache.hadoop.io.Text, org.apache.hadoop.io.Text> doCall(
                    Tuple2<ByteArray, Object[]> tuple2) throws Exception {
                ByteBuffer valueBuf = codec.encode(tuple2._2());
                org.apache.hadoop.io.Text textResult = new org.apache.hadoop.io.Text();
                textResult.set(valueBuf.array(), 0, valueBuf.position());
                return new Tuple2<>(new org.apache.hadoop.io.Text(tuple2._1().array()), textResult);
            }
        }).saveAsNewAPIHadoopDataset(job.getConfiguration());
        logger.info("Persisting RDD for level " + level + " into " + cuboidOutputPath);
    }
}

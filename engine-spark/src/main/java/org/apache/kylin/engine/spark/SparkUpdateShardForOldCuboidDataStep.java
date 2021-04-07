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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.common.RowKeySplitter;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.kv.RowKeyEncoder;
import org.apache.kylin.cube.kv.RowKeyEncoderProvider;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.MapReduceUtil;
import org.apache.kylin.engine.mr.common.SerializableConfiguration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;

import static org.apache.kylin.engine.mr.JobBuilderSupport.PathNameCuboidBase;
import static org.apache.kylin.engine.mr.JobBuilderSupport.PathNameCuboidOld;
import static org.apache.kylin.engine.spark.SparkUtil.configConvergeCuboidDataReduceOut;
import static org.apache.kylin.engine.spark.SparkUtil.generateFilePath;

public class SparkUpdateShardForOldCuboidDataStep extends AbstractApplication implements Serializable {

    protected static final Logger logger = LoggerFactory.getLogger(SparkUpdateShardForOldCuboidDataStep.class);

    public static final Option OPTION_CUBE_NAME = OptionBuilder.withArgName(BatchConstants.ARG_CUBE_NAME).hasArg()
            .isRequired(true).create(BatchConstants.ARG_CUBE_NAME);
    public static final Option OPTION_SEGMENT_ID = OptionBuilder.withArgName(BatchConstants.ARG_SEGMENT_ID).hasArg().isRequired(true)
            .create(BatchConstants.ARG_SEGMENT_ID);
    public static final Option OPTION_INPUT_PATH = OptionBuilder.withArgName(BatchConstants.ARG_INPUT).hasArg()
            .isRequired(true).create(BatchConstants.ARG_INPUT);
    public static final Option OPTION_OUTPUT_PATH = OptionBuilder.withArgName(BatchConstants.ARG_OUTPUT).hasArg()
            .isRequired(true).create(BatchConstants.ARG_OUTPUT);
    public static final Option OPTION_META_URL = OptionBuilder.withArgName("metaUrl").hasArg().isRequired(true)
            .withDescription("HDFS metadata url").create(BatchConstants.ARG_META_URL);

    private Options options;
    private CubeDesc cubeDesc;
    private RowKeySplitter rowKeySplitter;
    private RowKeyEncoderProvider rowKeyEncoderProvider;

    private byte[] newKeyBodyBuf = new byte[RowConstants.ROWKEY_BUFFER_SIZE];
    private ByteArray newKeyBuf = ByteArray.allocate(RowConstants.ROWKEY_BUFFER_SIZE);

    public SparkUpdateShardForOldCuboidDataStep() {
        options = new Options();
        options.addOption(OPTION_CUBE_NAME);
        options.addOption(OPTION_SEGMENT_ID);
        options.addOption(OPTION_INPUT_PATH);
        options.addOption(OPTION_OUTPUT_PATH);
        options.addOption(OPTION_META_URL);
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        // input is optimizeCuboidRootPath + "*",  output is cuboidRootPath.
        String cubeName = optionsHelper.getOptionValue(OPTION_CUBE_NAME);
        String segmentId = optionsHelper.getOptionValue(OPTION_SEGMENT_ID);
        String inputPath = optionsHelper.getOptionValue(OPTION_INPUT_PATH);
        String outputPath = optionsHelper.getOptionValue(OPTION_OUTPUT_PATH);
        String metaUrl = optionsHelper.getOptionValue(OPTION_META_URL);

        String baseCuboIdInputPath = inputPath + PathNameCuboidBase;
        String oldCuboIdInputPath = inputPath + PathNameCuboidOld;

        SparkConf sparkConf = SparkUtil.setKryoSerializerInConf();
        sparkConf.setAppName("Update_Old_Cuboid_Shard_for_Optimization" + cubeName + "_With_Spark");

        KylinSparkJobListener jobListener = new KylinSparkJobListener();
        try (JavaSparkContext sc = new JavaSparkContext(sparkConf)) {
            sc.sc().addSparkListener(jobListener);

            final SerializableConfiguration sConf = new SerializableConfiguration(sc.hadoopConfiguration());
            KylinConfig config = AbstractHadoopJob.loadKylinConfigFromHdfs(sConf, metaUrl);

            HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration(), new Path(outputPath));

            CubeManager cubeManager = CubeManager.getInstance(config);
            CubeInstance cube = cubeManager.getCube(cubeName);
            CubeSegment optSegment = cube.getSegmentById(segmentId);
            CubeSegment originalSegment = cube.getOriginalSegmentToOptimize(optSegment);
            //
            optSegment.setCubeInstance(originalSegment.getCubeInstance());

            JavaPairRDD<Text, Text> baseCuboIdRDD = sc.sequenceFile(baseCuboIdInputPath, Text.class, Text.class);
            JavaPairRDD<Text, Text> oldCuboIdRDD = sc.sequenceFile(oldCuboIdInputPath, Text.class, Text.class);

            cubeDesc = cube.getDescriptor();

            logger.info("start to calculate nBaseReduceTasks");
            Pair<Integer, Integer> taskNums = MapReduceUtil.getConvergeCuboidDataReduceTaskNums(originalSegment);
            int reduceTasks = taskNums.getFirst();
            int nBaseReduceTasks = taskNums.getSecond();
            logger.info("nBaseReduceTasks is {}", nBaseReduceTasks);

            final Job job = Job.getInstance(sConf.get());
            SparkUtil.setHadoopConfForCuboid(job, originalSegment, metaUrl);

            //UpdateCuboidShard for baseCuboId
            JavaPairRDD<Text, Text> mapBaseCuboIdRDD = baseCuboIdRDD.mapToPair(new SparkFunction.PairFunctionBase<Tuple2<Text, Text>, Text, Text>() {
                @Override
                protected void doInit() {
                    initMethod(sConf, metaUrl, cubeName, optSegment, originalSegment);
                }

                @Override
                protected Tuple2<Text, Text> doCall(Tuple2<Text, Text> tuple2) throws Exception {
                    Text outputKey = new Text();
                    long cuboidID = rowKeySplitter.split(tuple2._1.getBytes());

                    Cuboid cuboid = new Cuboid(cubeDesc, cuboidID, cuboidID);
                    int fullKeySize = buildKey(cuboid, rowKeySplitter.getSplitBuffers());
                    outputKey.set(newKeyBuf.array(), 0, fullKeySize);
                    return new Tuple2<Text, Text>(outputKey, tuple2._2);
                }
            });

            configConvergeCuboidDataReduceOut(job, generateFilePath(PathNameCuboidBase, outputPath));
            mapBaseCuboIdRDD.repartition(nBaseReduceTasks).saveAsNewAPIHadoopDataset(job.getConfiguration());

            //UpdateCuboidShard for oldCuboIds
            JavaPairRDD<Text, Text> mapOldCuboIdRDD = oldCuboIdRDD.mapToPair(new SparkFunction.PairFunctionBase<Tuple2<Text, Text>, Text, Text>() {
                @Override
                protected void doInit() {
                    initMethod(sConf, metaUrl, cubeName, optSegment, originalSegment);
                }

                @Override
                protected Tuple2<Text, Text> doCall(Tuple2<Text, Text> tuple2) throws Exception {
                    Text outputKey = new Text();
                    long cuboidID = rowKeySplitter.split(tuple2._1.getBytes());

                    Cuboid cuboid = new Cuboid(cubeDesc, cuboidID, cuboidID);
                    int fullKeySize = buildKey(cuboid, rowKeySplitter.getSplitBuffers());
                    outputKey.set(newKeyBuf.array(), 0, fullKeySize);
                    return new Tuple2<Text, Text>(outputKey, tuple2._2);
                }
            });

            configConvergeCuboidDataReduceOut(job, generateFilePath(PathNameCuboidOld, outputPath));
            mapOldCuboIdRDD.repartition(reduceTasks).saveAsNewAPIHadoopDataset(job.getConfiguration());

            //SparkUtil.convergeCuboidDataReduce(mapRDD, cube, originalSegment, outputPath, metaUrl, config, sConf);

        }
    }

    private int buildKey(Cuboid cuboid, ByteArray[] splitBuffers) {
        RowKeyEncoder rowkeyEncoder = rowKeyEncoderProvider.getRowkeyEncoder(cuboid);

        int startIdx = rowKeySplitter.getBodySplitOffset(); // skip shard and cuboidId
        int endIdx = startIdx + Long.bitCount(cuboid.getId());
        int offset = 0;
        for (int i = startIdx; i < endIdx; i++) {
            System.arraycopy(splitBuffers[i].array(), splitBuffers[i].offset(), newKeyBodyBuf, offset,
                    splitBuffers[i].length());
            offset += splitBuffers[i].length();
        }

        int fullKeySize = rowkeyEncoder.getBytesLength();
        while (newKeyBuf.array().length < fullKeySize) {
            newKeyBuf = new ByteArray(newKeyBuf.length() * 2);
        }
        newKeyBuf.setLength(fullKeySize);

        rowkeyEncoder.encode(new ByteArray(newKeyBodyBuf, 0, offset), newKeyBuf);

        return fullKeySize;
    }

    private void initMethod(SerializableConfiguration sConf, String metaUrl, String cubeName, CubeSegment optSegment, CubeSegment originalSegment) {
        KylinConfig kylinConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(sConf, metaUrl);
        KylinConfig.setKylinConfigInEnvIfMissing(kylinConfig.exportToProperties());
        CubeInstance cubeInstance = CubeManager.getInstance(kylinConfig).getCube(cubeName);
        CubeSegment originalSeg = cubeInstance.getSegmentById(originalSegment.getUuid());
        CubeSegment optSeg = cubeInstance.getSegmentById(optSegment.getUuid());
        rowKeySplitter = new RowKeySplitter(originalSeg);
        rowKeyEncoderProvider = new RowKeyEncoderProvider(optSeg);
    }
}

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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.common.RowKeySplitter;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.MapReduceUtil;
import org.apache.kylin.engine.mr.common.SerializableConfiguration;
import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;

import java.util.Set;

import static org.apache.kylin.engine.mr.JobBuilderSupport.PathNameCuboidBase;
import static org.apache.kylin.engine.mr.JobBuilderSupport.PathNameCuboidOld;
import static org.apache.kylin.engine.spark.SparkUtil.configConvergeCuboidDataReduceOut;
import static org.apache.kylin.engine.spark.SparkUtil.generateFilePath;

public class SparkFilterRecommendCuboidDataJob extends AbstractApplication implements Serializable {

    protected static final Logger logger = LoggerFactory.getLogger(SparkFilterRecommendCuboidDataJob.class);

    public static final Option OPTION_CUBE_NAME = OptionBuilder.withArgName(BatchConstants.ARG_CUBE_NAME).hasArg()
            .isRequired(true).create(BatchConstants.ARG_CUBE_NAME);
    public static final Option OPTION_SEGMENT_ID = OptionBuilder.withArgName(BatchConstants.ARG_SEGMENT_ID).hasArg().isRequired(true)
            .create(BatchConstants.ARG_SEGMENT_ID);
    public static final Option OPTION_INPUT_PATH = OptionBuilder.withArgName(BatchConstants.ARG_INPUT).hasArg()
            .isRequired(true).create(BatchConstants.ARG_INPUT);
    public static final Option OPTION_OUTPUT_PATH = OptionBuilder.withArgName(BatchConstants.ARG_OUTPUT).hasArg()
            .isRequired(true).create(BatchConstants.ARG_OUTPUT);
    public static final Option OPTION_META_URL = OptionBuilder.withArgName(BatchConstants.ARG_META_URL).hasArg().isRequired(true)
            .withDescription("HDFS metadata url").create(BatchConstants.ARG_META_URL);

    private Options options;

    public SparkFilterRecommendCuboidDataJob() {
        options = new Options();
        options.addOption(OPTION_CUBE_NAME);
        options.addOption(OPTION_INPUT_PATH);
        options.addOption(OPTION_OUTPUT_PATH);
        options.addOption(OPTION_SEGMENT_ID);
        options.addOption(OPTION_META_URL);
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        String cubeName = optionsHelper.getOptionValue(OPTION_CUBE_NAME);
        String segmentId = optionsHelper.getOptionValue(OPTION_SEGMENT_ID);
        String inputPath = optionsHelper.getOptionValue(OPTION_INPUT_PATH);
        String outputPath = optionsHelper.getOptionValue(OPTION_OUTPUT_PATH);
        String metaUrl = optionsHelper.getOptionValue(OPTION_META_URL);

        boolean enableSharding;
        long baseCuboid;
        Set<Long> recommendCuboids;

        SparkConf sparkConf = SparkUtil.setKryoSerializerInConf();
        sparkConf.setAppName("Kylin_Filter_Recommend_Cuboid_Data_" + cubeName + "_With_Spark");

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

            enableSharding = originalSegment.isEnableSharding();
            baseCuboid = cube.getCuboidScheduler().getBaseCuboidId();

            recommendCuboids = cube.getCuboidsRecommend();
            Preconditions.checkNotNull(recommendCuboids, "The recommend cuboid map could not be null");

            FileSystem hdfs = FileSystem.get(sc.hadoopConfiguration());
            if (!hdfs.exists(new Path(inputPath.substring(0, inputPath.length() - 1)))) {
                throw new IOException("OldCuboIdFilePath " + inputPath + " does not exists");
            }

            // inputPath is oldcuboidRootPath
            JavaPairRDD<Text, Text> inputRDD = sc.sequenceFile(inputPath, Text.class, Text.class);

            logger.info("start to calculate nBaseReduceTasks");
            Pair<Integer, Integer> taskNums = MapReduceUtil.getConvergeCuboidDataReduceTaskNums(originalSegment);
            int reduceTasks = taskNums.getFirst();
            int nBaseReduceTasks = taskNums.getSecond();
            logger.info("nBaseReduceTasks is {}", nBaseReduceTasks);

            final Job job = Job.getInstance(sConf.get());
            SparkUtil.setHadoopConfForCuboid(job, originalSegment, metaUrl);

            JavaPairRDD<Text, Text> baseCuboIdRDD = inputRDD.filter(new Function<Tuple2<Text, Text>, Boolean>() {
                @Override
                public Boolean call(Tuple2<Text, Text> v1) throws Exception {
                    long cuboidId = RowKeySplitter.getCuboidId(v1._1.getBytes(), enableSharding);
                    return cuboidId == baseCuboid;
                }
            });

            configConvergeCuboidDataReduceOut(job, generateFilePath(PathNameCuboidBase, outputPath));
            baseCuboIdRDD.coalesce(nBaseReduceTasks).saveAsNewAPIHadoopDataset(job.getConfiguration());

            JavaPairRDD<Text, Text> reuseCuboIdRDD = inputRDD.filter(new Function<Tuple2<Text, Text>, Boolean>() {
                @Override
                public Boolean call(Tuple2<Text, Text> v1) throws Exception {
                    long cuboidId = RowKeySplitter.getCuboidId(v1._1.getBytes(), enableSharding);
                    return recommendCuboids.contains(cuboidId);
                }
            });

            configConvergeCuboidDataReduceOut(job, generateFilePath(PathNameCuboidOld, outputPath));
            reuseCuboIdRDD.coalesce(reduceTasks).saveAsNewAPIHadoopDataset(job.getConfiguration());

        }
    }
}

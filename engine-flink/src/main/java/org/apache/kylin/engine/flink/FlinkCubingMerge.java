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

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.hadoopcompatibility.HadoopInputs;
import org.apache.flink.util.Collector;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.StringSplitter;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.flink.util.PercentileCounterSerializer;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.SerializableConfiguration;
import org.apache.kylin.engine.mr.steps.SegmentReEncoder;
import org.apache.kylin.measure.BufferedMeasureCodec;
import org.apache.kylin.measure.MeasureAggregators;
import org.apache.kylin.measure.percentile.PercentileCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Flink application to merge cube with the "by-layer" algorithm. Only support source data from Hive; Metadata in HBase.
 */
public class FlinkCubingMerge extends AbstractApplication implements Serializable {

    protected static final Logger logger = LoggerFactory.getLogger(FlinkCubingMerge.class);

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
    public static final Option OPTION_ENABLE_OBJECT_REUSE = OptionBuilder.withArgName("enableObjectReuse").hasArg()
            .isRequired(false).withDescription("Enable object reuse").create("enableObjectReuse");

    private Options options;

    private String cubeName;
    private String metaUrl;

    public FlinkCubingMerge() {
        options = new Options();
        options.addOption(OPTION_META_URL);
        options.addOption(OPTION_CUBE_NAME);
        options.addOption(OPTION_SEGMENT_ID);
        options.addOption(OPTION_INPUT_PATH);
        options.addOption(OPTION_OUTPUT_PATH);
        options.addOption(OPTION_ENABLE_OBJECT_REUSE);
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
        final String enableObjectReuseOptValue = optionsHelper.getOptionValue(OPTION_ENABLE_OBJECT_REUSE);

        boolean enableObjectReuse = false;
        if (enableObjectReuseOptValue != null && !enableObjectReuseOptValue.isEmpty()) {
            enableObjectReuse = true;
        }

        Job job = Job.getInstance();
        FlinkUtil.modifyFlinkHadoopConfiguration(job); // set dfs.replication=2 and enable compress

        HadoopUtil.deletePath(job.getConfiguration(), new Path(outputPath));
        final SerializableConfiguration sConf = new SerializableConfiguration(job.getConfiguration());
        final KylinConfig envConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(sConf, metaUrl);

        final CubeInstance cubeInstance = CubeManager.getInstance(envConfig).getCube(cubeName);
        final CubeDesc cubeDesc = CubeDescManager.getInstance(envConfig).getCubeDesc(cubeInstance.getDescName());
        final CubeSegment cubeSegment = cubeInstance.getSegmentById(segmentId);

        logger.info("Input path: {}", inputPath);
        logger.info("Output path: {}", outputPath);

        FlinkUtil.setHadoopConfForCuboid(job, cubeSegment, metaUrl);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        if (enableObjectReuse) {
            env.getConfig().enableObjectReuse();
        }
        env.getConfig().registerKryoType(PercentileCounter.class);
        env.getConfig().registerTypeWithKryoSerializer(PercentileCounter.class, PercentileCounterSerializer.class);

        final MeasureAggregators aggregators = new MeasureAggregators(cubeDesc.getMeasures());

        final int totalLevels = cubeSegment.getCuboidScheduler().getBuildLevel();
        final String[] inputFolders = StringSplitter.split(inputPath, ",");
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        boolean isLegacyMode = false;
        for (String inputFolder : inputFolders) {
            Path baseCuboidPath = new Path(FlinkBatchMergeJobBuilder2.getCuboidOutputPathsByLevel(inputFolder, 0));
            if (fs.exists(baseCuboidPath) == false) {
                // doesn't exist sub folder, that means the merged cuboid in one folder (not by layer)
                isLegacyMode = true;
                break;
            }
        }

        if (isLegacyMode) {
            // merge all layer's cuboid at once, this might be hard for Spark
            List<DataSet<Tuple2<ByteArray, Object[]>>> mergingSegs = Lists.newArrayListWithExpectedSize(inputFolders.length);
            for (int i = 0; i < inputFolders.length; i++) {
                String path = inputFolders[i];
                DataSet segRdd = FlinkUtil.parseInputPath(path, fs, env, Text.class, Text.class);
                CubeSegment sourceSegment = findSourceSegment(path, cubeInstance);
                // re-encode with new dictionaries
                DataSet<Tuple2<ByteArray, Object[]>> newEcoddedRdd = segRdd.map(new ReEncodeCuboidFunction(cubeName,
                        sourceSegment.getUuid(), cubeSegment.getUuid(), metaUrl, sConf));
                mergingSegs.add(newEcoddedRdd);
            }

            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            HadoopOutputFormat<Text, Text> hadoopOF =
                    new HadoopOutputFormat<>(new SequenceFileOutputFormat<>(), job);

            if (mergingSegs.size() > 0) {
                DataSet<Tuple2<ByteArray, Object[]>> unionedDataSet = mergingSegs.get(0);
                for (int i = 1; i < mergingSegs.size(); i++) {
                    unionedDataSet = unionedDataSet.union(mergingSegs.get(i));
                }

                unionedDataSet
                        .groupBy(0)
                        .reduceGroup(new MeasureReduceGroupFunction(aggregators))
                        .map(new ConvertTextMapFunction(sConf, metaUrl, cubeName))
                        .output(hadoopOF);
            }
        } else {
            // merge by layer
            for (int level = 0; level <= totalLevels; level++) {
                List<DataSet<Tuple2<ByteArray, Object[]>>> mergingSegs = Lists.newArrayList();
                for (int i = 0; i < inputFolders.length; i++) {
                    String path = inputFolders[i];
                    CubeSegment sourceSegment = findSourceSegment(path, cubeInstance);
                    final String cuboidInputPath = FlinkBatchMergeJobBuilder2.getCuboidOutputPathsByLevel(path, level);
                    DataSet<Tuple2<Text, Text>> segRdd = env.createInput(HadoopInputs.readHadoopFile(
                            new SequenceFileInputFormat(), Text.class, Text.class, cuboidInputPath));
                    // re-encode with new dictionaries
                    DataSet<Tuple2<ByteArray, Object[]>> newEcoddedRdd = segRdd.map(new ReEncodeCuboidFunction(cubeName,
                            sourceSegment.getUuid(), cubeSegment.getUuid(), metaUrl, sConf));
                    mergingSegs.add(newEcoddedRdd);
                }

                final String cuboidOutputPath = FlinkBatchMergeJobBuilder2.getCuboidOutputPathsByLevel(outputPath, level);

                Job jobInstanceForEachOutputFormat = Job.getInstance();
                FlinkUtil.modifyFlinkHadoopConfiguration(jobInstanceForEachOutputFormat); // set dfs.replication=2 and enable compress
                FlinkUtil.setHadoopConfForCuboid(jobInstanceForEachOutputFormat, cubeSegment, metaUrl);

                FileOutputFormat.setOutputPath(jobInstanceForEachOutputFormat, new Path(cuboidOutputPath));
                HadoopOutputFormat<Text, Text> hadoopOF =
                        new HadoopOutputFormat<>(new SequenceFileOutputFormat<>(), jobInstanceForEachOutputFormat);

                if (mergingSegs.size() > 0) {
                    DataSet<Tuple2<ByteArray, Object[]>> unionedDataSet = mergingSegs.get(0);
                    for (int i = 1; i < mergingSegs.size(); i++) {
                        unionedDataSet = unionedDataSet.union(mergingSegs.get(i));
                    }

                    unionedDataSet
                            .groupBy(0)
                            .reduceGroup(new MeasureReduceGroupFunction(aggregators))
                            .map(new ConvertTextMapFunction(sConf, metaUrl, cubeName))
                            .output(hadoopOF);
                }
            }
        }

        env.execute("Merge segments for cube:" + cubeName + ", segment " + segmentId);
        // output the data size to console, job engine will parse and save the metric
        logger.info("HDFS: Number of bytes written=" + FlinkBatchCubingJobBuilder2.getFileSize(outputPath, fs));
    }

    private CubeSegment findSourceSegment(String filePath, CubeInstance cube) {
        String jobID = JobBuilderSupport.extractJobIDFromPath(filePath);
        return CubeInstance.findSegmentWithJobId(jobID, cube);
    }

    private static class ReEncodeCuboidFunction extends RichMapFunction<Tuple2<Text, Text>, Tuple2<ByteArray, Object[]>> {
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

        @Override
        public void open(Configuration parameters) throws Exception {
            this.kylinConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(conf, metaUrl);
            final CubeInstance cube = CubeManager.getInstance(kylinConfig).getCube(cubeName);
            final CubeDesc cubeDesc = CubeDescManager.getInstance(kylinConfig).getCubeDesc(cube.getDescName());
            final CubeSegment sourceSeg = cube.getSegmentById(sourceSegmentId);
            final CubeSegment mergedSeg = cube.getSegmentById(mergedSegmentId);
            this.segmentReEncoder = new SegmentReEncoder(cubeDesc, sourceSeg, mergedSeg, kylinConfig);
        }

        @Override
        public Tuple2<ByteArray, Object[]> map(Tuple2<Text, Text> textTextTuple2) throws Exception {
            Pair<Text, Object[]> encodedPair = segmentReEncoder.reEncode2(textTextTuple2.f0, textTextTuple2.f1);
            return new Tuple2(new ByteArray(encodedPair.getFirst().getBytes()), encodedPair.getSecond());
        }
    }

    private static class MeasureReduceFunction implements ReduceFunction<Tuple2<ByteArray, Object[]>> {

        private MeasureAggregators aggregators;

        public MeasureReduceFunction(MeasureAggregators aggregators) {
            this.aggregators = aggregators;
        }

        @Override
        public Tuple2<ByteArray, Object[]> reduce(Tuple2<ByteArray, Object[]> input1, Tuple2<ByteArray, Object[]> input2) throws Exception {
            Object[] measureObjs = new Object[input1.f1.length];
            aggregators.aggregate(input1.f1, input2.f1, measureObjs);
            return new Tuple2<>(input1.f0, measureObjs);
        }
    }

    private static class MeasureReduceGroupFunction extends RichGroupReduceFunction<Tuple2<ByteArray, Object[]>, Tuple2<ByteArray, Object[]>> {

        private MeasureAggregators aggregators;

        public MeasureReduceGroupFunction(MeasureAggregators aggregators) {
            this.aggregators = aggregators;
        }

        @Override
        public void reduce(Iterable<Tuple2<ByteArray, Object[]>> values, Collector<Tuple2<ByteArray, Object[]>> out) throws Exception {
            Object[] result = null;
            ByteArray key = null;

            for (Tuple2<ByteArray, Object[]> item : values) {
                key = item.f0;
                if (result == null) {
                    result = item.f1;
                } else {
                    Object[] temp = new Object[result.length];
                    aggregators.aggregate(item.f1, result, temp);
                    result = temp;
                }
            }
            out.collect(new Tuple2<>(key, result));
        }
    }

    private static class ConvertTextMapFunction extends RichMapFunction<Tuple2<ByteArray, Object[]>, Tuple2<Text, Text>> {

        private BufferedMeasureCodec codec;
        private SerializableConfiguration sConf;
        private String metaUrl;
        private String cubeName;

        public ConvertTextMapFunction(SerializableConfiguration sConf, String metaUrl, String cubeName) {
            this.sConf = sConf;
            this.metaUrl = metaUrl;
            this.cubeName = cubeName;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            KylinConfig kylinConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(sConf, metaUrl);
            try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig
                    .setAndUnsetThreadLocalConfig(kylinConfig)) {
                CubeDesc desc = CubeDescManager.getInstance(kylinConfig).getCubeDesc(cubeName);
                codec = new BufferedMeasureCodec(desc.getMeasures());
            }
        }

        @Override
        public Tuple2<Text, Text> map(Tuple2<ByteArray, Object[]> tuple2) throws Exception {
            ByteBuffer valueBuf = codec.encode(tuple2.f1);
            Text result = new Text();
            result.set(valueBuf.array(), 0, valueBuf.position());
            return new Tuple2<>(new Text(tuple2.f0.array()), result);
        }

    }

}

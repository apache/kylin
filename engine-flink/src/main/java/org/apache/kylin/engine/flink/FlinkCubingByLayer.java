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

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.common.RowKeySplitter;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.AbstractRowKeyEncoder;
import org.apache.kylin.cube.kv.RowKeyEncoderProvider;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeJoinedFlatTableEnrich;
import org.apache.kylin.engine.EngineFactory;
import org.apache.kylin.engine.flink.util.PercentileCounterSerializer;
import org.apache.kylin.engine.mr.BatchCubingJobBuilder2;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BaseCuboidBuilder;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.CubeStatsReader;
import org.apache.kylin.engine.mr.common.NDCuboidBuilder;
import org.apache.kylin.engine.mr.common.SerializableConfiguration;
import org.apache.kylin.job.JoinedFlatTable;
import org.apache.kylin.measure.BufferedMeasureCodec;
import org.apache.kylin.measure.MeasureAggregators;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.measure.percentile.PercentileCounter;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Locale;

/**
 * Flink application to build cube with the "by-layer" algorithm.
 */
public class FlinkCubingByLayer extends AbstractApplication implements Serializable {

    protected static final Logger logger = LoggerFactory.getLogger(FlinkCubingByLayer.class);

    public static final Option OPTION_CUBE_NAME = OptionBuilder.withArgName(BatchConstants.ARG_CUBE_NAME).hasArg()
            .isRequired(true).withDescription("Cube Name").create(BatchConstants.ARG_CUBE_NAME);
    public static final Option OPTION_SEGMENT_ID = OptionBuilder.withArgName("segment").hasArg().isRequired(true)
            .withDescription("Cube Segment Id").create("segmentId");
    public static final Option OPTION_META_URL = OptionBuilder.withArgName("metaUrl").hasArg().isRequired(true)
            .withDescription("HDFS metadata url").create("metaUrl");
    public static final Option OPTION_OUTPUT_PATH = OptionBuilder.withArgName(BatchConstants.ARG_OUTPUT).hasArg()
            .isRequired(true).withDescription("Cube output path").create(BatchConstants.ARG_OUTPUT);
    public static final Option OPTION_INPUT_TABLE = OptionBuilder.withArgName("hiveTable").hasArg().isRequired(true)
            .withDescription("Hive Intermediate Table").create("hiveTable");
    public static final Option OPTION_INPUT_PATH = OptionBuilder.withArgName(BatchConstants.ARG_INPUT).hasArg()
            .isRequired(true).withDescription("Hive Intermediate Table PATH").create(BatchConstants.ARG_INPUT);
    public static final Option OPTION_ENABLE_OBJECT_REUSE = OptionBuilder.withArgName("enableObjectReuse").hasArg()
            .isRequired(false).withDescription("Enable object reuse").create("enableObjectReuse");

    private Options options;

    public FlinkCubingByLayer() {
        options = new Options();
        options.addOption(OPTION_INPUT_TABLE);
        options.addOption(OPTION_INPUT_PATH);
        options.addOption(OPTION_CUBE_NAME);
        options.addOption(OPTION_SEGMENT_ID);
        options.addOption(OPTION_META_URL);
        options.addOption(OPTION_OUTPUT_PATH);
        options.addOption(OPTION_ENABLE_OBJECT_REUSE);
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        String metaUrl = optionsHelper.getOptionValue(OPTION_META_URL);
        String hiveTable = optionsHelper.getOptionValue(OPTION_INPUT_TABLE);
        String inputPath = optionsHelper.getOptionValue(OPTION_INPUT_PATH);
        String cubeName = optionsHelper.getOptionValue(OPTION_CUBE_NAME);
        String segmentId = optionsHelper.getOptionValue(OPTION_SEGMENT_ID);
        String outputPath = optionsHelper.getOptionValue(OPTION_OUTPUT_PATH);
        String enableObjectReuseOptValue = optionsHelper.getOptionValue(OPTION_ENABLE_OBJECT_REUSE);

        boolean enableObjectReuse = false;
        if (enableObjectReuseOptValue != null && !enableObjectReuseOptValue.isEmpty()) {
            enableObjectReuse = true;
        }

        Job job = Job.getInstance();
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        HadoopUtil.deletePath(job.getConfiguration(), new Path(outputPath));

        final SerializableConfiguration sConf = new SerializableConfiguration(job.getConfiguration());
        KylinConfig envConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(sConf, metaUrl);

        final CubeInstance cubeInstance = CubeManager.getInstance(envConfig).getCube(cubeName);
        final CubeDesc cubeDesc = cubeInstance.getDescriptor();
        final CubeSegment cubeSegment = cubeInstance.getSegmentById(segmentId);

        logger.info("DataSet input path : {}", inputPath);
        logger.info("DataSet output path : {}", outputPath);

        int countMeasureIndex = 0;
        for (MeasureDesc measureDesc : cubeDesc.getMeasures()) {
            if (measureDesc.getFunction().isCount() == true) {
                break;
            } else {
                countMeasureIndex++;
            }
        }

        final CubeStatsReader cubeStatsReader = new CubeStatsReader(cubeSegment, envConfig);
        boolean[] needAggr = new boolean[cubeDesc.getMeasures().size()];
        boolean allNormalMeasure = true;
        for (int i = 0; i < cubeDesc.getMeasures().size(); i++) {
            needAggr[i] = !cubeDesc.getMeasures().get(i).getFunction().getMeasureType().onlyAggrInBaseCuboid();
            allNormalMeasure = allNormalMeasure && needAggr[i];
        }

        logger.info("All measure are normal (agg on all cuboids) ? : " + allNormalMeasure);

        boolean isSequenceFile = JoinedFlatTable.SEQUENCEFILE.equalsIgnoreCase(envConfig.getFlatTableStorageFormat());

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        if (enableObjectReuse) {
            env.getConfig().enableObjectReuse();
        }
        env.getConfig().registerKryoType(PercentileCounter.class);
        env.getConfig().registerTypeWithKryoSerializer(PercentileCounter.class, PercentileCounterSerializer.class);

        DataSet<String[]> hiveDataSet = FlinkUtil.readHiveRecords(isSequenceFile, env, inputPath, hiveTable, job);

        DataSet<Tuple2<ByteArray, Object[]>> encodedBaseDataSet = hiveDataSet.mapPartition(
                new EncodeBaseCuboidMapPartitionFunction(cubeName, segmentId, metaUrl, sConf));

        Long totalCount = 0L;
        if (envConfig.isFlinkSanityCheckEnabled()) {
            totalCount = encodedBaseDataSet.count();
        }

        final BaseCuboidReduceGroupFunction baseCuboidReducerFunction = new BaseCuboidReduceGroupFunction(cubeName, metaUrl, sConf);

        BaseCuboidReduceGroupFunction reducerFunction = baseCuboidReducerFunction;
        if (!allNormalMeasure) {
            reducerFunction = new CuboidReduceGroupFunction(cubeName, metaUrl, sConf, needAggr);
        }

        final int totalLevels = cubeSegment.getCuboidScheduler().getBuildLevel();
        DataSet<Tuple2<ByteArray, Object[]>>[] allDataSets = new DataSet[totalLevels + 1];
        int level = 0;

        // aggregate to calculate base cuboid
        allDataSets[0] = encodedBaseDataSet.groupBy(0).reduceGroup(baseCuboidReducerFunction);

        sinkToHDFS(allDataSets[0], metaUrl, cubeName, cubeSegment, outputPath, 0, Job.getInstance(), envConfig);

        CuboidMapPartitionFunction mapPartitionFunction = new CuboidMapPartitionFunction(cubeName, segmentId, metaUrl, sConf);

        for (level = 1; level <= totalLevels; level++) {
            allDataSets[level] = allDataSets[level - 1].mapPartition(mapPartitionFunction).groupBy(0).reduceGroup(reducerFunction);
            if (envConfig.isFlinkSanityCheckEnabled()) {
                sanityCheck(allDataSets[level], totalCount, level, cubeStatsReader, countMeasureIndex);
            }
            sinkToHDFS(allDataSets[level], metaUrl, cubeName, cubeSegment, outputPath, level, Job.getInstance(), envConfig);
        }

        env.execute("Cubing for : " + cubeName + " segment " + segmentId);
        logger.info("Finished on calculating all level cuboids.");
        logger.info("HDFS: Number of bytes written=" + FlinkBatchCubingJobBuilder2.getFileSize(outputPath, fs));
    }

    private void sinkToHDFS(
            final DataSet<Tuple2<ByteArray, Object[]>> dataSet,
            final String metaUrl,
            final String cubeName,
            final CubeSegment cubeSeg,
            final String hdfsBaseLocation,
            final int level,
            final Job job,
            final KylinConfig kylinConfig) throws Exception {
        final String cuboidOutputPath = BatchCubingJobBuilder2.getCuboidOutputPathsByLevel(hdfsBaseLocation, level);
        final SerializableConfiguration sConf = new SerializableConfiguration(job.getConfiguration());
        FlinkUtil.modifyFlinkHadoopConfiguration(job); // set dfs.replication=2 and enable compress
        FlinkUtil.setHadoopConfForCuboid(job, cubeSeg, metaUrl);

        HadoopOutputFormat<Text, Text> hadoopOF =
                new HadoopOutputFormat<>(new SequenceFileOutputFormat<>(), job);

        SequenceFileOutputFormat.setOutputPath(job, new Path(cuboidOutputPath));

        dataSet.map(new RichMapFunction<Tuple2<ByteArray, Object[]>, Tuple2<Text, Text>>() {

            BufferedMeasureCodec codec;

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
                org.apache.hadoop.io.Text textResult =  new org.apache.hadoop.io.Text();
                textResult.set(valueBuf.array(), 0, valueBuf.position());
                return new Tuple2<>(new org.apache.hadoop.io.Text(tuple2.f0.array()), textResult);
            }

        }).output(hadoopOF);

        logger.info("Persisting DataSet for level " + level + " into " + cuboidOutputPath);
    }

    private void sanityCheck(DataSet<Tuple2<ByteArray, Object[]>> dataSet, Long totalCount, int thisLevel,
            CubeStatsReader cubeStatsReader, final int countMeasureIndex) throws Exception {
        int thisCuboidNum = cubeStatsReader.getCuboidsByLayer(thisLevel).size();
        Long count2 = getDataSetCountSum(dataSet, countMeasureIndex);
        if (count2 != totalCount * thisCuboidNum) {
            throw new IllegalStateException(
                    String.format(Locale.ROOT, "Sanity check failed, level %s, total count(*) is %s; cuboid number %s",
                            thisLevel, count2, thisCuboidNum));
        } else {
            logger.info("sanity check success for level " + thisLevel + ", count(*) is " + (count2 / thisCuboidNum));
        }
    }

    private Long getDataSetCountSum(DataSet<Tuple2<ByteArray, Object[]>> dataSet, final int countMeasureIndex) throws Exception {
        Long count = dataSet.map((Tuple2<ByteArray, Object[]> byteArrayTuple2) ->
                new Tuple2<>(byteArrayTuple2.f0, (Long) byteArrayTuple2.f1[countMeasureIndex])
        ).sum(1).count();

        return count;
    }

    /**
     * A map function used to encode the base cuboid.
     */
    private static class EncodeBaseCuboidMapFunction extends RichMapFunction<String[], Tuple2<ByteArray, Object[]>> {

        private BaseCuboidBuilder baseCuboidBuilder = null;
        private String cubeName;
        private String segmentId;
        private String metaUrl;
        private SerializableConfiguration conf;

        public EncodeBaseCuboidMapFunction(String cubeName, String segmentId, String metaUrl, SerializableConfiguration conf) {
            this.cubeName = cubeName;
            this.segmentId = segmentId;
            this.metaUrl = metaUrl;
            this.conf = conf;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            KylinConfig kConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(conf, metaUrl);
            try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig
                    .setAndUnsetThreadLocalConfig(kConfig)) {
                CubeInstance cubeInstance = CubeManager.getInstance(kConfig).getCube(cubeName);
                CubeDesc cubeDesc = cubeInstance.getDescriptor();
                CubeSegment cubeSegment = cubeInstance.getSegmentById(segmentId);
                CubeJoinedFlatTableEnrich interDesc = new CubeJoinedFlatTableEnrich(
                        EngineFactory.getJoinedFlatTableDesc(cubeSegment), cubeDesc);
                long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
                Cuboid baseCuboid = Cuboid.findForMandatory(cubeDesc, baseCuboidId);
                baseCuboidBuilder = new BaseCuboidBuilder(kConfig, cubeDesc, cubeSegment, interDesc,
                        AbstractRowKeyEncoder.createInstance(cubeSegment, baseCuboid),
                        MeasureIngester.create(cubeDesc.getMeasures()), cubeSegment.buildDictionaryMap());
            }
        }

        @Override
        public Tuple2<ByteArray, Object[]> map(String[] rowArray) throws Exception {
            baseCuboidBuilder.resetAggrs();
            byte[] rowKey = baseCuboidBuilder.buildKey(rowArray);
            Object[] result = baseCuboidBuilder.buildValueObjects(rowArray);
            return new Tuple2<>(new ByteArray(rowKey), result);
        }
    }

    /**
     * A map partition function used to encode the base cuboid.
     */
    private static class EncodeBaseCuboidMapPartitionFunction extends RichMapPartitionFunction<String[], Tuple2<ByteArray, Object[]>> {

        private BaseCuboidBuilder baseCuboidBuilder = null;
        private String cubeName;
        private String segmentId;
        private String metaUrl;
        private SerializableConfiguration conf;

        public EncodeBaseCuboidMapPartitionFunction(String cubeName, String segmentId, String metaUrl, SerializableConfiguration conf) {
            this.cubeName = cubeName;
            this.segmentId = segmentId;
            this.metaUrl = metaUrl;
            this.conf = conf;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            KylinConfig kConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(conf, metaUrl);
            try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig
                    .setAndUnsetThreadLocalConfig(kConfig)) {
                CubeInstance cubeInstance = CubeManager.getInstance(kConfig).getCube(cubeName);
                CubeDesc cubeDesc = cubeInstance.getDescriptor();
                CubeSegment cubeSegment = cubeInstance.getSegmentById(segmentId);
                CubeJoinedFlatTableEnrich interDesc = new CubeJoinedFlatTableEnrich(
                        EngineFactory.getJoinedFlatTableDesc(cubeSegment), cubeDesc);
                long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
                Cuboid baseCuboid = Cuboid.findForMandatory(cubeDesc, baseCuboidId);
                baseCuboidBuilder = new BaseCuboidBuilder(kConfig, cubeDesc, cubeSegment, interDesc,
                        AbstractRowKeyEncoder.createInstance(cubeSegment, baseCuboid),
                        MeasureIngester.create(cubeDesc.getMeasures()), cubeSegment.buildDictionaryMap());
            }
        }

        @Override
        public void mapPartition(Iterable<String[]> rowArrays, Collector<Tuple2<ByteArray, Object[]>> collector) throws Exception {
            for (String[] rowArray : rowArrays) {
                baseCuboidBuilder.resetAggrs();
                byte[] rowKey = baseCuboidBuilder.buildKey(rowArray);
                Object[] result = baseCuboidBuilder.buildValueObjects(rowArray);
                collector.collect(new Tuple2<>(new ByteArray(rowKey), result));
            }
        }
    }

    private static class BaseCuboidReduceGroupFunction extends RichGroupReduceFunction<Tuple2<ByteArray, Object[]>, Tuple2<ByteArray, Object[]>> {

        protected String cubeName;
        protected String metaUrl;
        protected CubeDesc cubeDesc;
        protected int measureNum;
        protected MeasureAggregators aggregators;
        protected SerializableConfiguration conf;

        public BaseCuboidReduceGroupFunction(String cubeName, String metaUrl, SerializableConfiguration conf) {
            this.cubeName = cubeName;
            this.metaUrl = metaUrl;
            this.conf = conf;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            KylinConfig kConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(conf, metaUrl);
            try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig
                    .setAndUnsetThreadLocalConfig(kConfig)) {
                CubeInstance cubeInstance = CubeManager.getInstance(kConfig).getCube(cubeName);
                cubeDesc = cubeInstance.getDescriptor();
                aggregators = new MeasureAggregators(cubeDesc.getMeasures());
                measureNum = cubeDesc.getMeasures().size();
            }
        }

        @Override
        public void reduce(Iterable<Tuple2<ByteArray, Object[]>> iterable, Collector<Tuple2<ByteArray, Object[]>> collector) throws Exception {
            Object[] result = null;
            ByteArray key = null;

            for (Tuple2<ByteArray, Object[]> item : iterable) {
                key = item.f0;
                if (result == null) {
                    result = item.f1;
                } else {
                    Object[] temp = new Object[measureNum];
                    aggregators.aggregate(item.f1, result, temp);
                    result = temp;
                }
            }

            collector.collect(new Tuple2<>(key, result));
        }
    }

    /**
     * A reduce function used to aggregate base cuboid.
     */
    private static class BaseCuboidReduceFunction extends RichReduceFunction<Tuple2<ByteArray, Object[]>> {

        protected String cubeName;
        protected String metaUrl;
        protected CubeDesc cubeDesc;
        protected int measureNum;
        protected MeasureAggregators aggregators;
        protected SerializableConfiguration conf;

        public BaseCuboidReduceFunction(String cubeName, String metaUrl, SerializableConfiguration conf) {
            this.cubeName = cubeName;
            this.metaUrl = metaUrl;
            this.conf = conf;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            KylinConfig kConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(conf, metaUrl);
            try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig
                    .setAndUnsetThreadLocalConfig(kConfig)) {
                CubeInstance cubeInstance = CubeManager.getInstance(kConfig).getCube(cubeName);
                cubeDesc = cubeInstance.getDescriptor();
                aggregators = new MeasureAggregators(cubeDesc.getMeasures());
                measureNum = cubeDesc.getMeasures().size();
            }
        }

        @Override
        public Tuple2<ByteArray, Object[]> reduce(Tuple2<ByteArray, Object[]> input1,
                Tuple2<ByteArray, Object[]> input2) throws Exception {
            Object[] result = new Object[measureNum];
            aggregators.aggregate(input1.f1, input2.f1, result);
            return new Tuple2<>(input1.f0, result);
        }

    }

    private static class CuboidReduceGroupFunction extends BaseCuboidReduceGroupFunction {
        private boolean[] needAgg;

        public CuboidReduceGroupFunction(String cubeName, String metaUrl, SerializableConfiguration conf, boolean[] needAgg) {
            super(cubeName, metaUrl, conf);
            this.needAgg = needAgg;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }

        @Override
        public void reduce(Iterable<Tuple2<ByteArray, Object[]>> iterable, Collector<Tuple2<ByteArray, Object[]>> collector) throws Exception {
            Object[] result = null;
            ByteArray key = null;

            for (Tuple2<ByteArray, Object[]> item : iterable) {
                key = item.f0;
                if (result == null) {
                    result = item.f1;
                } else {
                    Object[] temp = new Object[measureNum];
                    aggregators.aggregate(item.f1, result, temp, needAgg);
                    result = temp;
                }
            }

            collector.collect(new Tuple2<>(key, result));
        }
    }

    /**
     * A reduce function does aggregation based on boolean flag array.
     */
    private static class CuboidReduceFunction extends BaseCuboidReduceFunction {

        private boolean[] needAgg;

        public CuboidReduceFunction(String cubeName, String metaUrl, SerializableConfiguration conf, boolean[] needAgg) {
            super(cubeName, metaUrl, conf);
            this.needAgg = needAgg;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }

        @Override
        public Tuple2<ByteArray, Object[]> reduce(Tuple2<ByteArray, Object[]> input1,
                Tuple2<ByteArray, Object[]> input2) throws Exception {
            Object[] result = new Object[measureNum];
            aggregators.aggregate(input1.f1, input2.f1, result, needAgg);
            return new Tuple2<>(input1.f0, result);
        }
    }

    /**
     * A map partition function which extracts a cuboid's children cuboids and emit them to the down stream.
     */
    private static class CuboidMapPartitionFunction extends RichMapPartitionFunction<Tuple2<ByteArray, Object[]>, Tuple2<ByteArray, Object[]>> {

        private String cubeName;
        private String segmentId;
        private String metaUrl;
        private CubeSegment cubeSegment;
        private CubeDesc cubeDesc;
        private NDCuboidBuilder ndCuboidBuilder;
        private RowKeySplitter rowKeySplitter;
        private SerializableConfiguration conf;

        public CuboidMapPartitionFunction(String cubeName, String segmentId, String metaUrl, SerializableConfiguration conf) {
            this.cubeName = cubeName;
            this.segmentId = segmentId;
            this.metaUrl = metaUrl;
            this.conf = conf;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            KylinConfig kConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(conf, metaUrl);
            try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig
                    .setAndUnsetThreadLocalConfig(kConfig)) {
                CubeInstance cubeInstance = CubeManager.getInstance(kConfig).getCube(cubeName);
                this.cubeSegment = cubeInstance.getSegmentById(segmentId);
                this.cubeDesc = cubeInstance.getDescriptor();
                this.ndCuboidBuilder = new NDCuboidBuilder(cubeSegment, new RowKeyEncoderProvider(cubeSegment));
                this.rowKeySplitter = new RowKeySplitter(cubeSegment);
            }
        }

        @Override
        public void mapPartition(Iterable<Tuple2<ByteArray, Object[]>> iterable, Collector<Tuple2<ByteArray, Object[]>> collector) throws Exception {
            for (Tuple2<ByteArray, Object[]> item : iterable) {
                byte[] key = item.f0.array();
                long cuboidId = rowKeySplitter.parseCuboid(key);
                final List<Long> myChildren = cubeSegment.getCuboidScheduler().getSpanningCuboid(cuboidId);

                // if still empty or null
                if (myChildren == null || myChildren.size() == 0) {
                    continue;
                }
                rowKeySplitter.split(key);
                final Cuboid parentCuboid = Cuboid.findForMandatory(cubeDesc, cuboidId);

                for (Long child : myChildren) {
                    Cuboid childCuboid = Cuboid.findForMandatory(cubeDesc, child);
                    ByteArray result = ndCuboidBuilder.buildKey2(parentCuboid, childCuboid,
                            rowKeySplitter.getSplitBuffers());

                    collector.collect(new Tuple2<>(result, item.f1));
                }
            }
        }
    }

    /**
     * A flatmap function which extracts a cuboid's children cuboids and emit them to the down stream.
     */
    private static class CuboidFlatMapFunction extends RichFlatMapFunction<Tuple2<ByteArray, Object[]>, Tuple2<ByteArray, Object[]>> {

        private String cubeName;
        private String segmentId;
        private String metaUrl;
        private CubeSegment cubeSegment;
        private CubeDesc cubeDesc;
        private NDCuboidBuilder ndCuboidBuilder;
        private RowKeySplitter rowKeySplitter;
        private SerializableConfiguration conf;

        public CuboidFlatMapFunction(String cubeName, String segmentId, String metaUrl, SerializableConfiguration conf) {
            this.cubeName = cubeName;
            this.segmentId = segmentId;
            this.metaUrl = metaUrl;
            this.conf = conf;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            KylinConfig kConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(conf, metaUrl);
            try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig
                    .setAndUnsetThreadLocalConfig(kConfig)) {
                CubeInstance cubeInstance = CubeManager.getInstance(kConfig).getCube(cubeName);
                this.cubeSegment = cubeInstance.getSegmentById(segmentId);
                this.cubeDesc = cubeInstance.getDescriptor();
                this.ndCuboidBuilder = new NDCuboidBuilder(cubeSegment, new RowKeyEncoderProvider(cubeSegment));
                this.rowKeySplitter = new RowKeySplitter(cubeSegment);
            }
        }

        @Override
        public void flatMap(Tuple2<ByteArray, Object[]> tuple2, Collector<Tuple2<ByteArray, Object[]>> collector) throws Exception {
            byte[] key = tuple2.f0.array();
            long cuboidId = rowKeySplitter.parseCuboid(key);
            final List<Long> myChildren = cubeSegment.getCuboidScheduler().getSpanningCuboid(cuboidId);

            // if still empty or null
            if (myChildren == null || myChildren.size() == 0) {
                return;
            }
            rowKeySplitter.split(key);
            final Cuboid parentCuboid = Cuboid.findForMandatory(cubeDesc, cuboidId);

            for (Long child : myChildren) {
                Cuboid childCuboid = Cuboid.findForMandatory(cubeDesc, child);
                ByteArray result = ndCuboidBuilder.buildKey2(parentCuboid, childCuboid,
                        rowKeySplitter.getSplitBuffers());

                collector.collect(new Tuple2<>(result, tuple2.f1));
            }
        }
    }

}

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

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.common.RowKeySplitter;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.cube.kv.AbstractRowKeyEncoder;
import org.apache.kylin.cube.kv.RowKeyEncoderProvider;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeJoinedFlatTableEnrich;
import org.apache.kylin.engine.EngineFactory;
import org.apache.kylin.engine.mr.BatchCubingJobBuilder2;
import org.apache.kylin.engine.mr.IMROutput2;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BaseCuboidBuilder;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.CubeStatsReader;
import org.apache.kylin.engine.mr.common.NDCuboidBuilder;
import org.apache.kylin.measure.BufferedMeasureCodec;
import org.apache.kylin.measure.MeasureAggregators;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

/**
 * Spark application to build cube with the "by-layer" algorithm. Only support source data from Hive; Metadata in HBase.
 */
public class SparkCubingByLayer extends AbstractApplication implements Serializable {

    protected static final Logger logger = LoggerFactory.getLogger(SparkCubingByLayer.class);

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

    private Options options;

    public SparkCubingByLayer() {
        options = new Options();
        options.addOption(OPTION_INPUT_TABLE);
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
        String hiveTable = optionsHelper.getOptionValue(OPTION_INPUT_TABLE);
        String cubeName = optionsHelper.getOptionValue(OPTION_CUBE_NAME);
        String segmentId = optionsHelper.getOptionValue(OPTION_SEGMENT_ID);
        String outputPath = optionsHelper.getOptionValue(OPTION_OUTPUT_PATH);

        Class[] kryoClassArray = new Class[] { org.apache.hadoop.io.Text.class,
                Class.forName("scala.reflect.ClassTag$$anon$1"), java.lang.Class.class };

        SparkConf conf = new SparkConf().setAppName("Cubing for:" + cubeName + " segment " + segmentId);
        //serialization conf
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "org.apache.kylin.engine.spark.KylinKryoRegistrator");
        conf.set("spark.kryo.registrationRequired", "true").registerKryoClasses(kryoClassArray);

        JavaSparkContext sc = new JavaSparkContext(conf);
        HadoopUtil.deletePath(sc.hadoopConfiguration(), new Path(outputPath));

        KylinConfig envConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(metaUrl);

        final CubeInstance cubeInstance = CubeManager.getInstance(envConfig).getCube(cubeName);
        final CubeDesc cubeDesc = cubeInstance.getDescriptor();
        final CubeSegment cubeSegment = cubeInstance.getSegmentById(segmentId);

        Configuration confOverwrite = new Configuration(sc.hadoopConfiguration());
        confOverwrite.set("dfs.replication", "2"); // cuboid intermediate files, replication=2
        final Job job = Job.getInstance(confOverwrite);

        logger.info("RDD Output path: {}", outputPath);
        setHadoopConf(job, cubeSegment, metaUrl);

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
        StorageLevel storageLevel = StorageLevel.MEMORY_AND_DISK_SER();

        HiveContext sqlContext = new HiveContext(sc.sc());
        final Dataset intermediateTable = sqlContext.table(hiveTable);

        // encode with dimension encoding, transform to <ByteArray, Object[]> RDD
        final JavaPairRDD<ByteArray, Object[]> encodedBaseRDD = intermediateTable.javaRDD()
                .mapToPair(new EncodeBaseCuboid(cubeName, segmentId, metaUrl));

        Long totalCount = 0L;
        if (envConfig.isSparkSanityCheckEnabled()) {
            totalCount = encodedBaseRDD.count();
        }

        final BaseCuboidReducerFunction2 baseCuboidReducerFunction = new BaseCuboidReducerFunction2(cubeName, metaUrl);
        BaseCuboidReducerFunction2 reducerFunction2 = baseCuboidReducerFunction;
        if (allNormalMeasure == false) {
            reducerFunction2 = new CuboidReducerFunction2(cubeName, metaUrl, needAggr);
        }

        final int totalLevels = cubeSegment.getCuboidScheduler().getBuildLevel();
        JavaPairRDD<ByteArray, Object[]>[] allRDDs = new JavaPairRDD[totalLevels + 1];
        int level = 0;
        int partition = estimateRDDPartitionNum(level, cubeStatsReader, envConfig);

        // aggregate to calculate base cuboid
        allRDDs[0] = encodedBaseRDD.reduceByKey(baseCuboidReducerFunction, partition).persist(storageLevel);

        saveToHDFS(allRDDs[0], metaUrl, cubeName, cubeSegment, outputPath, 0, job, envConfig);

        // aggregate to ND cuboids
        for (level = 1; level <= totalLevels; level++) {
            partition = estimateRDDPartitionNum(level, cubeStatsReader, envConfig);
            allRDDs[level] = allRDDs[level - 1].flatMapToPair(new CuboidFlatMap(cubeName, segmentId, metaUrl))
                    .reduceByKey(reducerFunction2, partition).persist(storageLevel);
            if (envConfig.isSparkSanityCheckEnabled() == true) {
                sanityCheck(allRDDs[level], totalCount, level, cubeStatsReader, countMeasureIndex);
            }
            saveToHDFS(allRDDs[level], metaUrl, cubeName, cubeSegment, outputPath, level, job, envConfig);
            allRDDs[level - 1].unpersist();
        }
        allRDDs[totalLevels].unpersist();
        logger.info("Finished on calculating all level cuboids.");
        deleteHDFSMeta(metaUrl);
    }

    protected void setHadoopConf(Job job, CubeSegment segment, String metaUrl) throws Exception {
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
    }

    protected int estimateRDDPartitionNum(int level, CubeStatsReader statsReader, KylinConfig kylinConfig) {
        double baseCuboidSize = statsReader.estimateLayerSize(level);
        float rddCut = kylinConfig.getSparkRDDPartitionCutMB();
        int partition = (int) (baseCuboidSize / rddCut);
        partition = Math.max(kylinConfig.getSparkMinPartition(), partition);
        partition = Math.min(kylinConfig.getSparkMaxPartition(), partition);
        logger.info("Partition for spark cubing: {}", partition);
        return partition;
    }

    protected JavaPairRDD<ByteArray, Object[]> prepareOutput(JavaPairRDD<ByteArray, Object[]> rdd, KylinConfig config,
            CubeSegment segment, int level) {
        return rdd;
    }

    protected void saveToHDFS(final JavaPairRDD<ByteArray, Object[]> rdd, final String metaUrl, final String cubeName,
            final CubeSegment cubeSeg, final String hdfsBaseLocation, final int level, final Job job,
            final KylinConfig kylinConfig) throws Exception {
        final String cuboidOutputPath = BatchCubingJobBuilder2.getCuboidOutputPathsByLevel(hdfsBaseLocation, level);

        IMROutput2.IMROutputFormat outputFormat = MRUtil.getBatchCubingOutputSide2(cubeSeg).getOuputFormat();
        outputFormat.configureJobOutput(job, cuboidOutputPath, cubeSeg, level);

        prepareOutput(rdd, kylinConfig, cubeSeg, level).mapToPair(
                new PairFunction<Tuple2<ByteArray, Object[]>, org.apache.hadoop.io.Text, org.apache.hadoop.io.Text>() {
                    private volatile transient boolean initialized = false;
                    BufferedMeasureCodec codec;

                    @Override
                    public Tuple2<org.apache.hadoop.io.Text, org.apache.hadoop.io.Text> call(
                            Tuple2<ByteArray, Object[]> tuple2) throws Exception {

                        if (initialized == false) {
                            synchronized (SparkCubingByLayer.class) {
                                if (initialized == false) {
                                    KylinConfig kylinConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(metaUrl);
                                    CubeDesc desc = CubeDescManager.getInstance(kylinConfig).getCubeDesc(cubeName);
                                    codec = new BufferedMeasureCodec(desc.getMeasures());
                                    initialized = true;
                                }
                            }
                        }
                        ByteBuffer valueBuf = codec.encode(tuple2._2());
                        byte[] encodedBytes = new byte[valueBuf.position()];
                        System.arraycopy(valueBuf.array(), 0, encodedBytes, 0, valueBuf.position());
                        return new Tuple2<>(new org.apache.hadoop.io.Text(tuple2._1().array()),
                                new org.apache.hadoop.io.Text(encodedBytes));
                    }

                }).saveAsNewAPIHadoopDataset(job.getConfiguration());
        logger.info("Persisting RDD for level " + level + " into " + cuboidOutputPath);
    }

    static public class EncodeBaseCuboid implements PairFunction<Row, ByteArray, Object[]> {
        private volatile transient boolean initialized = false;
        private BaseCuboidBuilder baseCuboidBuilder = null;
        private String cubeName;
        private String segmentId;
        private String metaUrl;

        public EncodeBaseCuboid(String cubeName, String segmentId, String metaurl) {
            this.cubeName = cubeName;
            this.segmentId = segmentId;
            this.metaUrl = metaurl;
        }

        @Override
        public Tuple2<ByteArray, Object[]> call(Row row) throws Exception {
            if (initialized == false) {
                synchronized (SparkCubingByLayer.class) {
                    if (initialized == false) {
                        KylinConfig kConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(metaUrl);
                        CubeInstance cubeInstance = CubeManager.getInstance(kConfig).getCube(cubeName);
                        CubeDesc cubeDesc = cubeInstance.getDescriptor();
                        CubeSegment cubeSegment = cubeInstance.getSegmentById(segmentId);
                        CubeJoinedFlatTableEnrich interDesc = new CubeJoinedFlatTableEnrich(
                                EngineFactory.getJoinedFlatTableDesc(cubeSegment), cubeDesc);
                        long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
                        Cuboid baseCuboid = Cuboid.findById(cubeInstance, baseCuboidId);
                        baseCuboidBuilder = new BaseCuboidBuilder(kConfig, cubeDesc, cubeSegment, interDesc,
                                AbstractRowKeyEncoder.createInstance(cubeSegment, baseCuboid),
                                MeasureIngester.create(cubeDesc.getMeasures()), cubeSegment.buildDictionaryMap());
                        initialized = true;
                    }
                }
            }
            String[] rowArray = rowToArray(row);
            baseCuboidBuilder.resetAggrs();
            byte[] rowKey = baseCuboidBuilder.buildKey(rowArray);
            Object[] result = baseCuboidBuilder.buildValueObjects(rowArray);
            return new Tuple2<>(new ByteArray(rowKey), result);
        }

        private String[] rowToArray(Row row) {
            String[] result = new String[row.size()];
            for (int i = 0; i < row.size(); i++) {
                final Object o = row.get(i);
                if (o != null) {
                    result[i] = o.toString();
                } else {
                    result[i] = null;
                }
            }
            return result;
        }
    }

    static public class BaseCuboidReducerFunction2 implements Function2<Object[], Object[], Object[]> {
        protected String cubeName;
        protected String metaUrl;
        protected CubeDesc cubeDesc;
        protected int measureNum;
        protected MeasureAggregators aggregators;
        protected volatile transient boolean initialized = false;

        public BaseCuboidReducerFunction2(String cubeName, String metaUrl) {
            this.cubeName = cubeName;
            this.metaUrl = metaUrl;
        }

        public void init() {
            KylinConfig kConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(metaUrl);
            CubeInstance cubeInstance = CubeManager.getInstance(kConfig).getCube(cubeName);
            cubeDesc = cubeInstance.getDescriptor();
            aggregators = new MeasureAggregators(cubeDesc.getMeasures());
            measureNum = cubeDesc.getMeasures().size();
        }

        @Override
        public Object[] call(Object[] input1, Object[] input2) throws Exception {
            if (initialized == false) {
                synchronized (SparkCubingByLayer.class) {
                    if (initialized == false) {
                        init();
                        initialized = true;
                    }
                }
            }
            Object[] result = new Object[measureNum];
            aggregators.aggregate(input1, input2, result);
            return result;
        }
    }

    static public class CuboidReducerFunction2 extends BaseCuboidReducerFunction2 {
        private boolean[] needAggr;

        public CuboidReducerFunction2(String cubeName, String metaUrl, boolean[] needAggr) {
            super(cubeName, metaUrl);
            this.needAggr = needAggr;
        }

        @Override
        public Object[] call(Object[] input1, Object[] input2) throws Exception {
            if (initialized == false) {
                synchronized (SparkCubingByLayer.class) {
                    if (initialized == false) {
                        init();
                        initialized = true;
                    }
                }
            }
            Object[] result = new Object[measureNum];
            aggregators.aggregate(input1, input2, result, needAggr);
            return result;
        }
    }

    private static final java.lang.Iterable<Tuple2<ByteArray, Object[]>> EMTPY_ITERATOR = new ArrayList(0);

    static public class CuboidFlatMap implements PairFlatMapFunction<Tuple2<ByteArray, Object[]>, ByteArray, Object[]> {

        private String cubeName;
        private String segmentId;
        private String metaUrl;
        private CubeSegment cubeSegment;
        private CubeDesc cubeDesc;
        private CuboidScheduler cuboidScheduler;
        private NDCuboidBuilder ndCuboidBuilder;
        private RowKeySplitter rowKeySplitter;
        private volatile transient boolean initialized = false;

        public CuboidFlatMap(String cubeName, String segmentId, String metaUrl) {
            this.cubeName = cubeName;
            this.segmentId = segmentId;
            this.metaUrl = metaUrl;
        }

        public void init() {
            KylinConfig kConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(metaUrl);
            CubeInstance cubeInstance = CubeManager.getInstance(kConfig).getCube(cubeName);
            this.cubeSegment = cubeInstance.getSegmentById(segmentId);
            this.cubeDesc = cubeInstance.getDescriptor();
            this.cuboidScheduler = cubeSegment.getCuboidScheduler();
            this.ndCuboidBuilder = new NDCuboidBuilder(cubeSegment, new RowKeyEncoderProvider(cubeSegment));
            this.rowKeySplitter = new RowKeySplitter(cubeSegment, 65, 256);
        }

        @Override
        public Iterator<Tuple2<ByteArray, Object[]>> call(Tuple2<ByteArray, Object[]> tuple2) throws Exception {
            if (initialized == false) {
                synchronized (SparkCubingByLayer.class) {
                    if (initialized == false) {
                        init();
                        initialized = true;
                    }
                }
            }

            byte[] key = tuple2._1().array();
            long cuboidId = rowKeySplitter.split(key);
            Cuboid parentCuboid = Cuboid.findById(cubeSegment, cuboidId);

            Collection<Long> myChildren = cubeSegment.getCuboidScheduler().getSpanningCuboid(cuboidId);

            // if still empty or null
            if (myChildren == null || myChildren.size() == 0) {
                return EMTPY_ITERATOR.iterator();
            }

            List<Tuple2<ByteArray, Object[]>> tuples = new ArrayList(myChildren.size());
            for (Long child : myChildren) {
                Cuboid childCuboid = Cuboid.findById(cubeSegment, child);
                Pair<Integer, ByteArray> result = ndCuboidBuilder.buildKey(parentCuboid, childCuboid,
                        rowKeySplitter.getSplitBuffers());

                byte[] newKey = new byte[result.getFirst()];
                System.arraycopy(result.getSecond().array(), 0, newKey, 0, result.getFirst());

                tuples.add(new Tuple2<>(new ByteArray(newKey), tuple2._2()));
            }

            return tuples.iterator();
        }
    }

    protected void sanityCheck(JavaPairRDD<ByteArray, Object[]> rdd, Long totalCount, int thisLevel,
            CubeStatsReader cubeStatsReader, final int countMeasureIndex) {
        int thisCuboidNum = cubeStatsReader.getCuboidsByLayer(thisLevel).size();
        Long count2 = getRDDCountSum(rdd, countMeasureIndex);
        if (count2 != totalCount * thisCuboidNum) {
            throw new IllegalStateException(
                    String.format("Sanity check failed, level %s, total count(*) is %s; cuboid number %s", thisLevel,
                            count2, thisCuboidNum));
        } else {
            logger.info("sanity check success for level " + thisLevel + ", count(*) is " + (count2 / thisCuboidNum));
        }
    }

    private Long getRDDCountSum(JavaPairRDD<ByteArray, Object[]> rdd, final int countMeasureIndex) {
        final ByteArray ONE = new ByteArray();
        Long count = rdd.mapValues(new Function<Object[], Long>() {
            @Override
            public Long call(Object[] objects) throws Exception {
                return (Long) objects[countMeasureIndex];
            }
        }).reduce(new Function2<Tuple2<ByteArray, Long>, Tuple2<ByteArray, Long>, Tuple2<ByteArray, Long>>() {
            @Override
            public Tuple2<ByteArray, Long> call(Tuple2<ByteArray, Long> longTuple2, Tuple2<ByteArray, Long> longTuple22)
                    throws Exception {
                return new Tuple2<>(ONE, longTuple2._2() + longTuple22._2());
            }
        })._2();
        return count;
    }

    protected void deleteHDFSMeta(String metaUrl) throws IOException {
        int cut = metaUrl.indexOf('@');
        String path = metaUrl.substring(0, cut);
        HadoopUtil.getFileSystem(path).delete(new Path(path), true);
        logger.info("Delete metadata in HDFS for this job: " + path);
    }
}

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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.Pair;
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
import org.apache.kylin.engine.mr.common.BaseCuboidBuilder;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.CubeStatsReader;
import org.apache.kylin.engine.mr.common.NDCuboidBuilder;
import org.apache.kylin.measure.BufferedMeasureCodec;
import org.apache.kylin.measure.MeasureAggregators;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.io.FileFilter;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


/**
 * Spark application to build cube with the "by-layer" algorithm. Only support source data from Hive; Metadata in HBase.
 */
public class SparkCubingByLayer extends AbstractApplication implements Serializable {

    protected static final Logger logger = LoggerFactory.getLogger(SparkCubingByLayer.class);

    public static final Option OPTION_CUBE_NAME = OptionBuilder.withArgName(BatchConstants.ARG_CUBE_NAME).hasArg().isRequired(true).withDescription("Cube Name").create(BatchConstants.ARG_CUBE_NAME);
    public static final Option OPTION_SEGMENT_ID = OptionBuilder.withArgName("segment").hasArg().isRequired(true).withDescription("Cube Segment Id").create("segmentId");
    public static final Option OPTION_CONF_PATH = OptionBuilder.withArgName("confPath").hasArg().isRequired(true).withDescription("Configuration Path").create("confPath");
    public static final Option OPTION_OUTPUT_PATH = OptionBuilder.withArgName(BatchConstants.ARG_OUTPUT).hasArg().isRequired(true).withDescription("Cube output path").create(BatchConstants.ARG_OUTPUT);
    public static final Option OPTION_INPUT_TABLE = OptionBuilder.withArgName("hiveTable").hasArg().isRequired(true).withDescription("Hive Intermediate Table").create("hiveTable");

    private Options options;

    public SparkCubingByLayer() {
        options = new Options();
        options.addOption(OPTION_INPUT_TABLE);
        options.addOption(OPTION_CUBE_NAME);
        options.addOption(OPTION_SEGMENT_ID);
        options.addOption(OPTION_CONF_PATH);
        options.addOption(OPTION_OUTPUT_PATH);
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    private void setupClasspath(JavaSparkContext sc, String confPath) throws Exception {
        ClassUtil.addClasspath(confPath);
        final File[] files = new File(confPath).listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                if (pathname.getAbsolutePath().endsWith(".xml")) {
                    return true;
                }
                if (pathname.getAbsolutePath().endsWith(".properties")) {
                    return true;
                }
                return false;
            }
        });
        for (File file : files) {
            sc.addFile(file.getAbsolutePath());
        }
    }

    private static final void prepare() {
        File file = new File(SparkFiles.get("kylin.properties"));
        String confPath = file.getParentFile().getAbsolutePath();
        logger.info("conf directory:" + confPath);
        System.setProperty(KylinConfig.KYLIN_CONF, confPath);
        ClassUtil.addClasspath(confPath);

    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        final String hiveTable = optionsHelper.getOptionValue(OPTION_INPUT_TABLE);
        final String cubeName = optionsHelper.getOptionValue(OPTION_CUBE_NAME);
        final String segmentId = optionsHelper.getOptionValue(OPTION_SEGMENT_ID);
        final String confPath = optionsHelper.getOptionValue(OPTION_CONF_PATH);
        final String outputPath = optionsHelper.getOptionValue(OPTION_OUTPUT_PATH);

        SparkConf conf = new SparkConf().setAppName("Cubing for:" + cubeName + " segment " + segmentId);
        //serialization conf
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "org.apache.kylin.engine.spark.KylinKryoRegistrator");
        conf.set("spark.kryo.registrationRequired", "true");

        JavaSparkContext sc = new JavaSparkContext(conf);
        setupClasspath(sc, confPath);
        HadoopUtil.deletePath(sc.hadoopConfiguration(), new Path(outputPath));

        System.setProperty(KylinConfig.KYLIN_CONF, confPath);
        final KylinConfig envConfig = KylinConfig.getInstanceFromEnv();

        HiveContext sqlContext = new HiveContext(sc.sc());
        final DataFrame intermediateTable = sqlContext.table(hiveTable);

        final CubeInstance cubeInstance = CubeManager.getInstance(envConfig).getCube(cubeName);
        final CubeDesc cubeDesc = cubeInstance.getDescriptor();
        final CubeSegment cubeSegment = cubeInstance.getSegmentById(segmentId);
        final CubeJoinedFlatTableEnrich intermediateTableDesc = new CubeJoinedFlatTableEnrich(EngineFactory.getJoinedFlatTableDesc(cubeSegment), cubeDesc);

        final KylinConfig kylinConfig = cubeDesc.getConfig();
        final Broadcast<CubeDesc> vCubeDesc = sc.broadcast(cubeDesc);
        final Broadcast<CubeSegment> vCubeSegment = sc.broadcast(cubeSegment);
        final NDCuboidBuilder ndCuboidBuilder = new NDCuboidBuilder(vCubeSegment.getValue(), new RowKeyEncoderProvider(vCubeSegment.getValue()));

        final Broadcast<CuboidScheduler> vCuboidScheduler = sc.broadcast(new CuboidScheduler(vCubeDesc.getValue()));
        final int measureNum = cubeDesc.getMeasures().size();

        int countMeasureIndex = 0;
        for (MeasureDesc measureDesc : cubeDesc.getMeasures()) {
            if (measureDesc.getFunction().isCount() == true) {
                break;
            } else {
                countMeasureIndex++;
            }
        }
        final CubeStatsReader cubeStatsReader = new CubeStatsReader(cubeSegment, kylinConfig);
        boolean[] needAggr = new boolean[cubeDesc.getMeasures().size()];
        boolean allNormalMeasure = true;
        for (int i = 0; i < cubeDesc.getMeasures().size(); i++) {
            needAggr[i] = !cubeDesc.getMeasures().get(i).getFunction().getMeasureType().onlyAggrInBaseCuboid();
            allNormalMeasure = allNormalMeasure && needAggr[i];
        }
        logger.info("All measure are normal (agg on all cuboids) ? : " + allNormalMeasure);

        StorageLevel storageLevel = StorageLevel.MEMORY_AND_DISK_SER();

        // encode with dimension encoding, transform to <ByteArray, Object[]> RDD
        final JavaPairRDD<ByteArray, Object[]> encodedBaseRDD = intermediateTable.javaRDD().mapToPair(new PairFunction<Row, ByteArray, Object[]>() {
            volatile transient boolean initialized = false;
            BaseCuboidBuilder baseCuboidBuilder = null;

            @Override
            public Tuple2<ByteArray, Object[]> call(Row row) throws Exception {
                if (initialized == false) {
                    synchronized (SparkCubingByLayer.class) {
                        if (initialized == false) {
                            prepare();
                            long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
                            Cuboid baseCuboid = Cuboid.findById(cubeDesc, baseCuboidId);
                            baseCuboidBuilder = new BaseCuboidBuilder(kylinConfig, cubeDesc, cubeSegment, intermediateTableDesc, AbstractRowKeyEncoder.createInstance(cubeSegment, baseCuboid), MeasureIngester.create(cubeDesc.getMeasures()), cubeSegment.buildDictionaryMap());
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

        });

        logger.info("encodedBaseRDD partition number: " + encodedBaseRDD.getNumPartitions());
        Long totalCount = 0L;
        if (kylinConfig.isSparkSanityCheckEnabled()) {
            totalCount = encodedBaseRDD.count();
            logger.info("encodedBaseRDD row count: " + encodedBaseRDD.count());
        }

        final MeasureAggregators measureAggregators = new MeasureAggregators(cubeDesc.getMeasures());
        final BaseCuboidReducerFunction2 baseCuboidReducerFunction = new BaseCuboidReducerFunction2(measureNum, vCubeDesc.getValue(), measureAggregators);
        BaseCuboidReducerFunction2 reducerFunction2 = baseCuboidReducerFunction;
        if (allNormalMeasure == false) {
            reducerFunction2 = new CuboidReducerFunction2(measureNum, vCubeDesc.getValue(), measureAggregators, needAggr);
        }

        final int totalLevels = cubeDesc.getBuildLevel();
        JavaPairRDD<ByteArray, Object[]>[] allRDDs = new JavaPairRDD[totalLevels + 1];
        int level = 0;
        int partition = estimateRDDPartitionNum(level, cubeStatsReader, kylinConfig);

        // aggregate to calculate base cuboid
        allRDDs[0] = encodedBaseRDD.reduceByKey(baseCuboidReducerFunction, partition).persist(storageLevel);
        Configuration confOverwrite = new Configuration(sc.hadoopConfiguration());
        confOverwrite.set("dfs.replication", "2"); // cuboid intermediate files, replication=2

        saveToHDFS(allRDDs[0], vCubeDesc.getValue(), outputPath, 0, confOverwrite);

        // aggregate to ND cuboids
        PairFlatMapFunction<Tuple2<ByteArray, Object[]>, ByteArray, Object[]> flatMapFunction = new CuboidFlatMap(vCubeSegment.getValue(), vCubeDesc.getValue(), vCuboidScheduler.getValue(), ndCuboidBuilder);

        for (level = 1; level <= totalLevels; level++) {
            partition = estimateRDDPartitionNum(level, cubeStatsReader, kylinConfig);
            logger.info("Level " + level + " partition number: " + partition);
            allRDDs[level] = allRDDs[level - 1].flatMapToPair(flatMapFunction).reduceByKey(reducerFunction2, partition).persist(storageLevel);
            if (kylinConfig.isSparkSanityCheckEnabled() == true) {
                sanityCheck(allRDDs[level], totalCount, level, cubeStatsReader, countMeasureIndex);
            }
            saveToHDFS(allRDDs[level], vCubeDesc.getValue(), outputPath, level, confOverwrite);
            allRDDs[level - 1].unpersist();
        }
        allRDDs[totalLevels - 1].unpersist();
        logger.info("Finished on calculating all level cuboids.");
    }

    private static int estimateRDDPartitionNum(int level, CubeStatsReader statsReader, KylinConfig kylinConfig) {
        double baseCuboidSize = statsReader.estimateLayerSize(level);
        float rddCut = kylinConfig.getSparkRDDPartitionCutMB();
        int partition = (int) (baseCuboidSize / rddCut);
        partition = Math.max(kylinConfig.getSparkMinPartition(), partition);
        partition = Math.min(kylinConfig.getSparkMaxPartition(), partition);
        logger.debug("Estimated level " + level + " partition number: " + partition);
        return partition;
    }

    private static void saveToHDFS(final JavaPairRDD<ByteArray, Object[]> rdd, final CubeDesc cubeDesc, final String hdfsBaseLocation, int level, Configuration conf) {
        final String cuboidOutputPath = BatchCubingJobBuilder2.getCuboidOutputPathsByLevel(hdfsBaseLocation, level);
        rdd.mapToPair(new PairFunction<Tuple2<ByteArray, Object[]>, org.apache.hadoop.io.Text, org.apache.hadoop.io.Text>() {
            BufferedMeasureCodec codec = new BufferedMeasureCodec(cubeDesc.getMeasures());

            @Override
            public Tuple2<org.apache.hadoop.io.Text, org.apache.hadoop.io.Text> call(Tuple2<ByteArray, Object[]> tuple2) throws Exception {
                ByteBuffer valueBuf = codec.encode(tuple2._2());
                byte[] encodedBytes = new byte[valueBuf.position()];
                System.arraycopy(valueBuf.array(), 0, encodedBytes, 0, valueBuf.position());
                return new Tuple2<>(new org.apache.hadoop.io.Text(tuple2._1().array()), new org.apache.hadoop.io.Text(encodedBytes));
            }
        }).saveAsNewAPIHadoopFile(cuboidOutputPath, org.apache.hadoop.io.Text.class, org.apache.hadoop.io.Text.class, SequenceFileOutputFormat.class, conf);
        logger.info("Persisting RDD for level " + level + " into " + cuboidOutputPath);
    }

    class BaseCuboidReducerFunction2 implements Function2<Object[], Object[], Object[]> {
        CubeDesc cubeDesc;
        int measureNum;
        MeasureAggregators aggregators;

        BaseCuboidReducerFunction2(int measureNum, CubeDesc cubeDesc, MeasureAggregators aggregators) {
            this.cubeDesc = cubeDesc;
            this.measureNum = measureNum;
            this.aggregators = aggregators;
        }

        @Override
        public Object[] call(Object[] input1, Object[] input2) throws Exception {
            Object[] result = new Object[measureNum];
            aggregators.aggregate(input1, input2, result);
            return result;
        }
    }

    class CuboidReducerFunction2 extends BaseCuboidReducerFunction2 {
        boolean[] needAggr;

        CuboidReducerFunction2(int measureNum, CubeDesc cubeDesc, MeasureAggregators aggregators, boolean[] needAggr) {
            super(measureNum, cubeDesc, aggregators);
            this.needAggr = needAggr;
        }

        @Override
        public Object[] call(Object[] input1, Object[] input2) throws Exception {
            Object[] result = new Object[measureNum];
            aggregators.aggregate(input1, input2, result, needAggr);
            return result;
        }
    }

    private static final java.lang.Iterable<Tuple2<ByteArray, Object[]>> EMTPY_ITERATOR = new ArrayList(0);

    class CuboidFlatMap implements PairFlatMapFunction<Tuple2<ByteArray, Object[]>, ByteArray, Object[]> {

        CubeSegment cubeSegment;
        CubeDesc cubeDesc;
        CuboidScheduler cuboidScheduler;
        NDCuboidBuilder ndCuboidBuilder;
        RowKeySplitter rowKeySplitter;
        transient boolean initialized = false;

        CuboidFlatMap(CubeSegment cubeSegment, CubeDesc cubeDesc, CuboidScheduler cuboidScheduler, NDCuboidBuilder ndCuboidBuilder) {
            this.cubeSegment = cubeSegment;
            this.cubeDesc = cubeDesc;
            this.cuboidScheduler = cuboidScheduler;
            this.ndCuboidBuilder = ndCuboidBuilder;
            this.rowKeySplitter = new RowKeySplitter(cubeSegment, 65, 256);
        }

        @Override
        public Iterable<Tuple2<ByteArray, Object[]>> call(Tuple2<ByteArray, Object[]> tuple2) throws Exception {
            if (initialized == false) {
                prepare();
                initialized = true;
            }

            byte[] key = tuple2._1().array();
            long cuboidId = rowKeySplitter.split(key);
            Cuboid parentCuboid = Cuboid.findById(cubeDesc, cuboidId);

            Collection<Long> myChildren = cuboidScheduler.getSpanningCuboid(cuboidId);

            // if still empty or null
            if (myChildren == null || myChildren.size() == 0) {
                return EMTPY_ITERATOR;
            }

            List<Tuple2<ByteArray, Object[]>> tuples = new ArrayList(myChildren.size());
            for (Long child : myChildren) {
                Cuboid childCuboid = Cuboid.findById(cubeDesc, child);
                Pair<Integer, ByteArray> result = ndCuboidBuilder.buildKey(parentCuboid, childCuboid, rowKeySplitter.getSplitBuffers());

                byte[] newKey = new byte[result.getFirst()];
                System.arraycopy(result.getSecond().array(), 0, newKey, 0, result.getFirst());

                tuples.add(new Tuple2<>(new ByteArray(newKey), tuple2._2()));
            }

            return tuples;
        }
    }

    //sanity check

    private void sanityCheck(JavaPairRDD<ByteArray, Object[]> rdd, Long totalCount, int thisLevel, CubeStatsReader cubeStatsReader, final int countMeasureIndex) {
        int thisCuboidNum = cubeStatsReader.getCuboidsByLayer(thisLevel).size();
        Long count2 = getRDDCountSum(rdd, countMeasureIndex);
        if (count2 != totalCount * thisCuboidNum) {
            throw new IllegalStateException(String.format("Sanity check failed, level %s, total count(*) is %s; cuboid number %s", thisLevel, count2, thisCuboidNum));
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
            public Tuple2<ByteArray, Long> call(Tuple2<ByteArray, Long> longTuple2, Tuple2<ByteArray, Long> longTuple22) throws Exception {
                return new Tuple2<>(ONE, longTuple2._2() + longTuple22._2());
            }
        })._2();
        return count;
    }
}

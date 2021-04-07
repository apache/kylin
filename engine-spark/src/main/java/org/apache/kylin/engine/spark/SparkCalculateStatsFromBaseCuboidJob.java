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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinVersion;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.CuboidModeEnum;
import org.apache.kylin.cube.cuboid.CuboidUtil;
import org.apache.kylin.cube.kv.RowKeyDecoder;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.CubeStatsWriter;
import org.apache.kylin.engine.mr.common.SerializableConfiguration;
import org.apache.kylin.measure.BufferedMeasureCodec;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.shaded.com.google.common.hash.HashFunction;
import org.apache.kylin.shaded.com.google.common.hash.Hasher;
import org.apache.kylin.shaded.com.google.common.hash.Hashing;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import scala.Tuple2;

public class SparkCalculateStatsFromBaseCuboidJob extends AbstractApplication implements Serializable {

    protected static final Logger logger = LoggerFactory.getLogger(SparkCalculateStatsFromBaseCuboidJob.class);

    protected Map<Long, HLLCounter> cuboidHLLMap = Maps.newHashMap();

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
    public static final Option OPTION_JOB_MODE = OptionBuilder.withArgName(BatchConstants.ARG_CUBOID_MODE).hasArg().isRequired(true)
            .create(BatchConstants.ARG_CUBOID_MODE);
    public static final Option OPTION_SAMPLING_PERCENT = OptionBuilder.withArgName(BatchConstants.ARG_STATS_SAMPLING_PERCENT).hasArg()
            .isRequired(true).create(BatchConstants.ARG_STATS_SAMPLING_PERCENT);

    private Options options;

    private int samplingPercent;
    private int rowCount = 0;
    private long[] rowHashCodesLong = null;
    //about details of the new algorithm, please see KYLIN-2518
    private boolean isUsePutRowKeyToHllNewAlgorithm;

    private HLLCounter[] allCuboidsHLL = null;
    private Long[] cuboidIds;
    private Integer[][] allCuboidsBitSet = null;
    private HashFunction hf = null;

    protected int nRowKey;
    protected long baseCuboidId;

    RowKeyDecoder rowKeyDecoder;

    public SparkCalculateStatsFromBaseCuboidJob() {
        options = new Options();
        options.addOption(OPTION_CUBE_NAME);
        options.addOption(OPTION_INPUT_PATH);
        options.addOption(OPTION_OUTPUT_PATH);
        options.addOption(OPTION_SEGMENT_ID);
        options.addOption(OPTION_META_URL);
        options.addOption(OPTION_JOB_MODE);
        options.addOption(OPTION_SAMPLING_PERCENT);
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        String cubeName = optionsHelper.getOptionValue(OPTION_CUBE_NAME);
        String segmentId = optionsHelper.getOptionValue(OPTION_SEGMENT_ID);
        String input = optionsHelper.getOptionValue(OPTION_INPUT_PATH);
        String output = optionsHelper.getOptionValue(OPTION_OUTPUT_PATH);
        String metaUrl = optionsHelper.getOptionValue(OPTION_META_URL);
        String jobMode = optionsHelper.getOptionValue(OPTION_JOB_MODE);
        this.samplingPercent = Integer.parseInt(optionsHelper.getOptionValue(OPTION_SAMPLING_PERCENT));

        SparkConf sparkConf = SparkUtil.setKryoSerializerInConf();
        sparkConf.setAppName("Kylin_Calculate_Statics_From_BaseCuboid_Data_" + cubeName + "_With_Spark");

        KylinSparkJobListener jobListener = new KylinSparkJobListener();
        try (JavaSparkContext sc = new JavaSparkContext(sparkConf)) {
            sc.sc().addSparkListener(jobListener);

            HadoopUtil.deletePath(sc.hadoopConfiguration(), new Path(output));

            SerializableConfiguration sConf = new SerializableConfiguration(sc.hadoopConfiguration());
            KylinConfig config = AbstractHadoopJob.loadKylinConfigFromHdfs(sConf, metaUrl);
            CubeSegment optSegment;
            int cubeStatsHLLPrecision;
            try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig
                    .setAndUnsetThreadLocalConfig(config)) {

                CubeManager cubeManager = CubeManager.getInstance(config);
                CubeInstance cube = cubeManager.getCube(cubeName);
                CubeDesc cubeDesc = cube.getDescriptor();
                optSegment = cube.getSegmentById(segmentId);

                baseCuboidId = cube.getCuboidScheduler().getBaseCuboidId();
                nRowKey = cubeDesc.getRowkey().getRowKeyColumns().length;

                Set<Long> cuboids = cube.getCuboidsByMode(jobMode);
                if (cuboids.size() == 0) {
                    Set<Long> current = cube.getCuboidsByMode(CuboidModeEnum.CURRENT);
                    current.removeAll(cube.getCuboidsRecommend());
                    cuboids = current;
                }
                cuboidIds = cuboids.toArray(new Long[cuboids.size()]);
                allCuboidsBitSet = CuboidUtil.getCuboidBitSet(cuboidIds, nRowKey);
                cubeStatsHLLPrecision = config.getCubeStatsHLLPrecision();


                allCuboidsHLL = new HLLCounter[cuboidIds.length];
                for (int i = 0; i < cuboidIds.length; i++) {
                    allCuboidsHLL[i] = new HLLCounter(cubeStatsHLLPrecision);
                }

                //for KYLIN-2518 backward compatibility
                if (KylinVersion.isBefore200(cubeDesc.getVersion())) {
                    isUsePutRowKeyToHllNewAlgorithm = false;
                    hf = Hashing.murmur3_32();
                    logger.info("Found KylinVersion : {}. Use old algorithm for cuboid sampling.", cubeDesc.getVersion());
                } else {
                    isUsePutRowKeyToHllNewAlgorithm = true;
                    rowHashCodesLong = new long[nRowKey];
                    hf = Hashing.murmur3_128();
                    logger.info(
                            "Found KylinVersion : {}. Use new algorithm for cuboid sampling. About the details of the new algorithm, please refer to KYLIN-2518",
                            cubeDesc.getVersion());
                }
            }

            JavaPairRDD<Text, Text> inputRDD = sc.sequenceFile(input, Text.class, Text.class);

            JavaPairRDD<Text, Text> afterMapRDD = inputRDD.mapPartitionsToPair(
                    new SparkFunction.PairFlatMapFunctionBase<Iterator<Tuple2<Text, Text>>, Text, Text>() {
                        @Override
                        protected void doInit() {
                            KylinConfig kylinConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(sConf, metaUrl);
                            CubeInstance cubeInstance = CubeManager.getInstance(kylinConfig).getCube(cubeName);
                            CubeSegment cubeSeg = cubeInstance.getSegmentById(segmentId);
                            rowKeyDecoder = new RowKeyDecoder(cubeSeg);
                        }

                        @Override
                        protected Iterator<Tuple2<Text, Text>> doCall(Iterator<Tuple2<Text, Text>> iterator)
                                throws Exception {
                            while (iterator.hasNext()) {
                                Text key = iterator.next()._1();
                                long cuboidID = rowKeyDecoder.decode(key.getBytes());
                                if (cuboidID != baseCuboidId) {
                                    continue; // Skip data from cuboids which are not the base cuboid
                                }

                                List<String> keyValues = rowKeyDecoder.getValues();

                                if (rowCount < samplingPercent) {
                                    Preconditions.checkArgument(nRowKey == keyValues.size());

                                    String[] row = keyValues.toArray(new String[keyValues.size()]);
                                    if (isUsePutRowKeyToHllNewAlgorithm) {
                                        putRowKeyToHLLNew(row);
                                    } else {
                                        putRowKeyToHLLOld(row);
                                    }
                                }

                                if (++rowCount == 100)
                                    rowCount = 0;
                            }

                            ByteBuffer hllBuf = ByteBuffer.allocate(BufferedMeasureCodec.DEFAULT_BUFFER_SIZE);
                            HLLCounter hll;
                            List<Tuple2<Text, Text>> result = new ArrayList<>();

                            for (int i = 0; i < cuboidIds.length; i++) {
                                hll = allCuboidsHLL[i];
                                Text outputKey = new Text();
                                Text outputValue = new Text();

                                outputKey.set(Bytes.toBytes(cuboidIds[i]));
                                logger.info("Cuboid id to be processed1: " + cuboidIds[i]);
                                hllBuf.clear();
                                hll.writeRegisters(hllBuf);
                                outputValue.set(hllBuf.array(), 0, hllBuf.position());
                                logger.info("Cuboid id to be processed1: " + cuboidIds[i] + "value is "
                                        + hllBuf.array().toString());
                                result.add(new Tuple2<Text, Text>(outputKey, outputValue));
                                logger.info("result size: " + result.size());
                                for (Tuple2<Text, Text> t : result) {
                                    logger.info("result key: " + t._1().toString());
                                    logger.info("result values: " + t._2.toString());
                                }
                            }
                            return result.iterator();
                        }


                    });

            afterMapRDD.groupByKey().foreach(new SparkFunction.VoidFunctionBase<Tuple2<Text, Iterable<Text>>>() {
                @Override
                protected void doInit() {
                    KylinConfig kylinConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(sConf, metaUrl);
                    KylinConfig.setAndUnsetThreadLocalConfig(kylinConfig);
                }

                @Override
                protected void doCall(Tuple2<Text, Iterable<Text>> v1) throws Exception {
                    Text key = v1._1();
                    Iterable<Text> values = v1._2();

                    long cuboidId = Bytes.toLong(key.getBytes());
                    logger.info("Cuboid id to be processed: " + cuboidId);

                    List<Long> baseCuboidRowCountInMappers = Lists.newArrayList();
                    long totalRowsBeforeMerge = 0;

                    for (Text value : values) {
                        HLLCounter hll = new HLLCounter(cubeStatsHLLPrecision);
                        ByteBuffer bf = ByteBuffer.wrap(value.getBytes(), 0, value.getLength());
                        hll.readRegisters(bf);

                        if (cuboidId == baseCuboidId) {
                            baseCuboidRowCountInMappers.add(hll.getCountEstimate());
                        }

                        totalRowsBeforeMerge += hll.getCountEstimate();

                        if (cuboidHLLMap.get(cuboidId) != null) {
                            cuboidHLLMap.get(cuboidId).merge(hll);
                        } else {
                            cuboidHLLMap.put(cuboidId, hll);
                        }
                    }

                    long grandTotal = 0;
                    for (HLLCounter hll : cuboidHLLMap.values()) {
                        grandTotal += hll.getCountEstimate();
                    }
                    double mapperOverlapRatio = grandTotal == 0 ? 0 : (double) totalRowsBeforeMerge / grandTotal;

                    logger.info("writer cuboIdstatic to " + output);
                    CubeStatsWriter.writePartialCuboidStatistics(sConf.get(), new Path(output), cuboidHLLMap,
                            samplingPercent, baseCuboidRowCountInMappers.size(), mapperOverlapRatio,
                            TaskContext.getPartitionId());
                }
            });
        }

    }

    private void putRowKeyToHLLOld(String[] row) {
        //generate hash for each row key column
        byte[][] rowHashCodes = new byte[nRowKey][];
        for (int i = 0; i < nRowKey; i++) {
            Hasher hc = hf.newHasher();
            String colValue = row[i];
            if (colValue != null) {
                rowHashCodes[i] = hc.putUnencodedChars(colValue).hash().asBytes();
            } else {
                rowHashCodes[i] = hc.putInt(0).hash().asBytes();
            }
        }

        // use the row key column hash to get a consolidated hash for each cuboid
        for (int i = 0; i < cuboidIds.length; i++) {
            Hasher hc = hf.newHasher();
            for (int position = 0; position < allCuboidsBitSet[i].length; position++) {
                hc.putBytes(rowHashCodes[allCuboidsBitSet[i][position]]);
            }

            allCuboidsHLL[i].add(hc.hash().asBytes());
        }
    }

    private void putRowKeyToHLLNew(String[] row) {
        //generate hash for each row key column
        for (int i = 0; i < nRowKey; i++) {
            Hasher hc = hf.newHasher();
            String colValue = row[i];
            if (colValue == null)
                colValue = "0";
            byte[] bytes = hc.putUnencodedChars(colValue).hash().asBytes();
            rowHashCodesLong[i] = (Bytes.toLong(bytes) + i);//add column ordinal to the hash value to distinguish between (a,b) and (b,a)
        }

        // user the row key column hash to get a consolidated hash for each cuboid
        for (int i = 0, n = allCuboidsBitSet.length; i < n; i++) {
            long value = 0;
            for (int position = 0; position < allCuboidsBitSet[i].length; position++) {
                value += rowHashCodesLong[allCuboidsBitSet[i][position]];
            }
            allCuboidsHLL[i].addHashDirectly(value);
        }
    }

}

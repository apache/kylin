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

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.dict.DictionaryInfo;
import org.apache.kylin.dict.DictionaryManager;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.CubeStatsWriter;
import org.apache.kylin.engine.mr.common.SerializableConfiguration;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

import scala.Tuple2;

/**
    merge dictionary
 */
public class SparkMergingDictionary extends AbstractApplication implements Serializable {
    protected static final Logger logger = LoggerFactory.getLogger(SparkMergingDictionary.class);

    public static final Option OPTION_CUBE_NAME = OptionBuilder.withArgName(BatchConstants.ARG_CUBE_NAME).hasArg()
            .isRequired(true).withDescription("Cube Name").create(BatchConstants.ARG_CUBE_NAME);
    public static final Option OPTION_SEGMENT_ID = OptionBuilder.withArgName("segmentId").hasArg().isRequired(true)
            .withDescription("Cube Segment Id").create("segmentId");
    public static final Option OPTION_META_URL = OptionBuilder.withArgName("metaUrl").hasArg().isRequired(true)
            .withDescription("HDFS metadata url").create("metaUrl");
    public static final Option OPTION_MERGE_SEGMENT_IDS = OptionBuilder.withArgName("segmentIds").hasArg()
            .isRequired(true).withDescription("Merging Cube Segment Ids").create("segmentIds");
    public static final Option OPTION_OUTPUT_PATH_DICT = OptionBuilder.withArgName("dictOutputPath").hasArg()
            .isRequired(true).withDescription("merged dictionary resource path").create("dictOutputPath");
    public static final Option OPTION_OUTPUT_PATH_STAT = OptionBuilder.withArgName("statOutputPath").hasArg()
            .isRequired(true).withDescription("merged statistics resource path").create("statOutputPath");

    private Options options;

    public SparkMergingDictionary() {
        options = new Options();
        options.addOption(OPTION_CUBE_NAME);
        options.addOption(OPTION_SEGMENT_ID);
        options.addOption(OPTION_META_URL);
        options.addOption(OPTION_MERGE_SEGMENT_IDS);
        options.addOption(OPTION_OUTPUT_PATH_DICT);
        options.addOption(OPTION_OUTPUT_PATH_STAT);
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        final String cubeName = optionsHelper.getOptionValue(OPTION_CUBE_NAME);
        final String segmentId = optionsHelper.getOptionValue(OPTION_SEGMENT_ID);
        final String metaUrl = optionsHelper.getOptionValue(OPTION_META_URL);
        final String segmentIds = optionsHelper.getOptionValue(OPTION_MERGE_SEGMENT_IDS);
        final String dictOutputPath = optionsHelper.getOptionValue(OPTION_OUTPUT_PATH_DICT);
        final String statOutputPath = optionsHelper.getOptionValue(OPTION_OUTPUT_PATH_STAT);

        Class[] kryoClassArray = new Class[] { Class.forName("scala.reflect.ClassTag$$anon$1"),
                Class.forName("scala.collection.mutable.WrappedArray$ofRef") };

        SparkConf conf = new SparkConf().setAppName("Merge dictionary for cube:" + cubeName + ", segment " + segmentId);
        //serialization conf
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "org.apache.kylin.engine.spark.KylinKryoRegistrator");
        conf.set("spark.kryo.registrationRequired", "true").registerKryoClasses(kryoClassArray);

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            KylinSparkJobListener jobListener = new KylinSparkJobListener();
            sc.sc().addSparkListener(jobListener);

            HadoopUtil.deletePath(sc.hadoopConfiguration(), new Path(dictOutputPath));

            final SerializableConfiguration sConf = new SerializableConfiguration(sc.hadoopConfiguration());
            final KylinConfig envConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(sConf, metaUrl);

            final CubeInstance cubeInstance = CubeManager.getInstance(envConfig).getCube(cubeName);
            final CubeDesc cubeDesc = CubeDescManager.getInstance(envConfig).getCubeDesc(cubeInstance.getDescName());

            logger.info("Dictionary output path: {}", dictOutputPath);
            logger.info("Statistics output path: {}", statOutputPath);

            final TblColRef[] tblColRefs = cubeDesc.getAllColumnsNeedDictionaryBuilt().toArray(new TblColRef[0]);
            final int columnLength = tblColRefs.length;

            List<Integer> indexs = Lists.newArrayListWithCapacity(columnLength);

            for (int i = 0; i <= columnLength; i++) {
                indexs.add(i);
            }

            JavaRDD<Integer> indexRDD = sc.parallelize(indexs, columnLength + 1);

            JavaPairRDD<Text, Text> colToDictPathRDD = indexRDD.mapToPair(new MergeDictAndStatsFunction(cubeName,
                    metaUrl, segmentId, StringUtil.splitByComma(segmentIds), statOutputPath, tblColRefs, sConf));

            colToDictPathRDD.coalesce(1, false).saveAsNewAPIHadoopFile(dictOutputPath, Text.class, Text.class,
                    SequenceFileOutputFormat.class);
        }
    }

    public static class MergeDictAndStatsFunction implements PairFunction<Integer, Text, Text> {
        private transient volatile boolean initialized = false;
        private String cubeName;
        private String metaUrl;
        private String segmentId;
        private String[] segmentIds;
        private String statOutputPath;
        private TblColRef[] tblColRefs;
        private SerializableConfiguration conf;
        private transient DictionaryManager dictMgr;
        private KylinConfig kylinConfig;
        private List<CubeSegment> mergingSegments;

        public MergeDictAndStatsFunction(String cubeName, String metaUrl, String segmentId, String[] segmentIds,
                String statOutputPath, TblColRef[] tblColRefs, SerializableConfiguration conf) {
            this.cubeName = cubeName;
            this.metaUrl = metaUrl;
            this.segmentId = segmentId;
            this.segmentIds = segmentIds;
            this.statOutputPath = statOutputPath;
            this.tblColRefs = tblColRefs;
            this.conf = conf;
        }

        private void init() {
            kylinConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(conf, metaUrl);
            try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig
                    .setAndUnsetThreadLocalConfig(kylinConfig)) {
                CubeInstance cubeInstance = CubeManager.getInstance(kylinConfig).getCube(cubeName);
                dictMgr = DictionaryManager.getInstance(kylinConfig);
                mergingSegments = getMergingSegments(cubeInstance, segmentIds);
            }
        }

        @Override
        public Tuple2<Text, Text> call(Integer index) throws Exception {
            if (initialized == false) {
                synchronized (SparkMergingDictionary.class) {
                    if (initialized == false) {
                        init();
                        initialized = true;
                    }
                }
            }

            if (index < tblColRefs.length) {
                // merge dictionary
                TblColRef col = tblColRefs[index];
                List<DictionaryInfo> dictInfos = Lists.newArrayList();
                try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig
                        .setAndUnsetThreadLocalConfig(kylinConfig)) {
                    for (CubeSegment segment : mergingSegments) {
                        if (segment.getDictResPath(col) != null) {
                            DictionaryInfo dictInfo = dictMgr.getDictionaryInfo(segment.getDictResPath(col));
                            if (dictInfo != null && !dictInfos.contains(dictInfo)) {
                                dictInfos.add(dictInfo);
                            }
                        }
                    }

                    DictionaryInfo mergedDictInfo = dictMgr.mergeDictionary(dictInfos);
                    String tblCol = col.getTableAlias() + ":" + col.getName();
                    String dictInfoPath = mergedDictInfo == null ? "" : mergedDictInfo.getResourcePath();

                    return new Tuple2<>(new Text(tblCol), new Text(dictInfoPath));
                }
            } else {
                // merge statistics
                try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig
                        .setAndUnsetThreadLocalConfig(kylinConfig)) {
                    CubeInstance cubeInstance = CubeManager.getInstance(kylinConfig).getCube(cubeName);
                    CubeSegment newSegment = cubeInstance.getSegmentById(segmentId);
                    ResourceStore rs = ResourceStore.getStore(kylinConfig);

                    Map<Long, HLLCounter> cuboidHLLMap = Maps.newHashMap();
                    Configuration conf = null;
                    int averageSamplingPercentage = 0;
                    long sourceRecordCount = 0;
                    long effectiveTimeRange = 0;

                    for (CubeSegment cubeSegment : mergingSegments) {
                        String filePath = cubeSegment.getStatisticsResourcePath();

                        File tempFile = File.createTempFile(segmentId, ".seq");

                        try (InputStream is = rs.getResource(filePath).content();
                                FileOutputStream tempFileStream = new FileOutputStream(tempFile)) {

                            org.apache.commons.io.IOUtils.copy(is, tempFileStream);
                        }

                        FileSystem fs = HadoopUtil.getFileSystem("file:///" + tempFile.getAbsolutePath());

                        conf = HadoopUtil.getCurrentConfiguration();

                        try (SequenceFile.Reader reader = new SequenceFile.Reader(fs,
                                new Path(tempFile.getAbsolutePath()), conf)) {
                            //noinspection deprecation
                            LongWritable key = (LongWritable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
                            BytesWritable value = (BytesWritable) ReflectionUtils.newInstance(reader.getValueClass(),
                                    conf);

                            while (reader.next(key, value)) {
                                if (key.get() == 0L) {
                                    // sampling percentage
                                    averageSamplingPercentage += Bytes.toInt(value.getBytes());
                                } else if (key.get() == -3) {
                                    long perSourceRecordCount = Bytes.toLong(value.getBytes());
                                    if (perSourceRecordCount > 0) {
                                        sourceRecordCount += perSourceRecordCount;
                                        CubeSegment iSegment = cubeInstance.getSegmentById(segmentId);
                                        effectiveTimeRange += iSegment.getTSRange().duration();
                                    }
                                } else if (key.get() > 0) {
                                    HLLCounter hll = new HLLCounter(kylinConfig.getCubeStatsHLLPrecision());
                                    ByteArray byteArray = new ByteArray(value.getBytes());
                                    hll.readRegisters(byteArray.asBuffer());

                                    if (cuboidHLLMap.get(key.get()) != null) {
                                        cuboidHLLMap.get(key.get()).merge(hll);
                                    } else {
                                        cuboidHLLMap.put(key.get(), hll);
                                    }
                                }
                            }
                        }
                    }

                    sourceRecordCount *= effectiveTimeRange == 0 ? 0
                            : (double) newSegment.getTSRange().duration() / effectiveTimeRange;
                    averageSamplingPercentage = averageSamplingPercentage / mergingSegments.size();
                    CubeStatsWriter.writeCuboidStatistics(conf, new Path(statOutputPath), cuboidHLLMap,
                            averageSamplingPercentage, sourceRecordCount);
                    Path statisticsFilePath = new Path(statOutputPath,
                            BatchConstants.CFG_STATISTICS_CUBOID_ESTIMATION_FILENAME);

                    FileSystem fs = HadoopUtil.getFileSystem(statisticsFilePath, conf);
                    FSDataInputStream fis = fs.open(statisticsFilePath);

                    try {
                        // put the statistics to metadata store
                        String statisticsFileName = newSegment.getStatisticsResourcePath();
                        rs.putResource(statisticsFileName, fis, System.currentTimeMillis());
                    } finally {
                        IOUtils.closeStream(fis);
                    }

                    return new Tuple2<>(new Text(""), new Text(""));
                }
            }

        }

        private List<CubeSegment> getMergingSegments(CubeInstance cube, String[] segmentIds) {
            List<CubeSegment> result = Lists.newArrayListWithCapacity(segmentIds.length);
            for (String id : segmentIds) {
                result.add(cube.getSegmentById(id));
            }
            return result;
        }
    }

}

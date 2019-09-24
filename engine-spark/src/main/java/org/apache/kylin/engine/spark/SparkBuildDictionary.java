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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.ByteBufferBackedInputStream;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.DimensionDesc;
import org.apache.kylin.dict.DictionaryInfo;
import org.apache.kylin.dict.DictionaryInfoSerializer;
import org.apache.kylin.dict.lookup.ILookupTable;
import org.apache.kylin.dict.lookup.SnapshotTable;
import org.apache.kylin.dict.lookup.SnapshotTableSerializer;
import org.apache.kylin.engine.mr.SortedColumnDFSFile;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.SerializableConfiguration;
import org.apache.kylin.engine.mr.steps.FactDistinctColumnsReducer;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.source.IReadableTable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;


public class SparkBuildDictionary extends AbstractApplication implements Serializable {

    protected static final Logger logger = LoggerFactory.getLogger(SparkBuildDictionary.class);

    public static final Option OPTION_CUBE_NAME = OptionBuilder.withArgName(BatchConstants.ARG_CUBE_NAME).hasArg()
            .isRequired(true).withDescription("Cube Name").create(BatchConstants.ARG_CUBE_NAME);
    public static final Option OPTION_DICT_PATH = OptionBuilder.withArgName(BatchConstants.ARG_DICT_PATH).hasArg()
            .isRequired(true).withDescription("Cube dictionary output path").create(BatchConstants.ARG_DICT_PATH);
    public static final Option OPTION_SEGMENT_ID = OptionBuilder.withArgName("segmentId").hasArg().isRequired(true)
            .withDescription("Cube Segment Id").create("segmentId");
    public static final Option OPTION_CUBING_JOB_ID = OptionBuilder
            .withArgName(BatchConstants.ARG_CUBING_JOB_ID).hasArg().isRequired(true)
            .withDescription("Cubing job id").create(BatchConstants.ARG_CUBING_JOB_ID);
    public static final Option OPTION_INPUT_PATH = OptionBuilder.withArgName(BatchConstants.ARG_INPUT).hasArg()
            .isRequired(true).withDescription("Hive Intermediate Table PATH").create(BatchConstants.ARG_INPUT);
    public static final Option OPTION_META_URL = OptionBuilder.withArgName("metaUrl").hasArg().isRequired(true)
            .withDescription("HDFS metadata url").create("metaUrl");
    public static final Option OPTION_COUNTER_PATH = OptionBuilder.withArgName(BatchConstants.ARG_COUNTER_OUTPUT).hasArg()
            .isRequired(true).withDescription("counter output path").create(BatchConstants.ARG_COUNTER_OUTPUT);

    private Options options;

    public SparkBuildDictionary() {
        options = new Options();
        options.addOption(OPTION_CUBE_NAME);
        options.addOption(OPTION_DICT_PATH);
        options.addOption(OPTION_INPUT_PATH);
        options.addOption(OPTION_SEGMENT_ID);
        options.addOption(OPTION_CUBING_JOB_ID);
        options.addOption(OPTION_META_URL);
        options.addOption(OPTION_COUNTER_PATH);
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        String cubeName = optionsHelper.getOptionValue(OPTION_CUBE_NAME);
        String segmentId = optionsHelper.getOptionValue(OPTION_SEGMENT_ID);
        String dictPath = optionsHelper.getOptionValue(OPTION_DICT_PATH);
        String factColumnsInputPath = optionsHelper.getOptionValue(OPTION_INPUT_PATH);
        String metaUrl = optionsHelper.getOptionValue(OPTION_META_URL);
        final String jobId = optionsHelper.getOptionValue(OPTION_CUBING_JOB_ID);
        final String counterPath = optionsHelper.getOptionValue(OPTION_COUNTER_PATH);

        Class[] kryoClassArray = new Class[] { Class.forName("scala.reflect.ClassTag$$anon$1"),
                Class.forName("org.apache.kylin.engine.mr.steps.SelfDefineSortableKey") ,
                Class.forName("scala.collection.mutable.WrappedArray$ofRef")};

        SparkConf sparkConf = new SparkConf().setAppName("Build Dimension Dictionary for: " + cubeName + " segment " + segmentId);

        //serialization conf
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.set("spark.kryo.registrator", "org.apache.kylin.engine.spark.KylinKryoRegistrator");
        sparkConf.set("spark.kryo.registrationRequired", "true").registerKryoClasses(kryoClassArray);

        KylinSparkJobListener jobListener = new KylinSparkJobListener();
        try(JavaSparkContext sc = new JavaSparkContext(sparkConf)) {
            sc.sc().addSparkListener(jobListener);
            HadoopUtil.deletePath(sc.hadoopConfiguration(), new Path(dictPath));

            // calculate source record bytes size
            final LongAccumulator bytesWritten = sc.sc().longAccumulator();
            final SerializableConfiguration sConf = new SerializableConfiguration(sc.hadoopConfiguration());
            KylinConfig config = AbstractHadoopJob.loadKylinConfigFromHdfs(sConf, metaUrl);

            CubeInstance cube = CubeManager.getInstance(config).getCube(cubeName);
            CubeSegment cubeSegment = cube.getSegmentById(segmentId);

            Set<TblColRef> tblColRefs = cubeSegment.getCubeDesc().getAllColumnsNeedDictionaryBuilt();
            JavaRDD<TblColRef> tblColRefRDD = sc.parallelize(Lists.newArrayList(tblColRefs));
            logger.info("Dimensions all is :" + cubeSegment.getCubeDesc().getDimensions().toString());

            List<TblColRef> uhcColumns = cube.getDescriptor().getAllUHCColumns();
            logger.info("Spark build dict uhc columns is " + uhcColumns.size());

            JavaPairRDD<Boolean, TblColRef> tblColRefJavaPairRDD = tblColRefRDD.mapPartitionsToPair(
                    new TableColumnRefFunction(cubeName, segmentId, config, factColumnsInputPath, dictPath, uhcColumns));

            long unfinishedNumber = tblColRefJavaPairRDD.filter(tuple -> !tuple._1).count();
            if (unfinishedNumber > 0) {
                logger.warn("Not all dict build! {} columns dictionary should be build, but {} not built", tblColRefs.size(), unfinishedNumber);
            }

            cubeSegment = CubeManager.getInstance(config).reloadCube(cubeName).getSegmentById(segmentId);
            JavaRDD<DimensionDesc> dimensionDescRDD = sc.parallelize(cubeSegment.getCubeDesc().getDimensions());

            JavaPairRDD<String, Iterable<TableRef>> snapShots = dimensionDescRDD.filter(new DimensionDescFilterFunction(cubeName, segmentId, config))
                    .mapToPair(new PairFunction<DimensionDesc, String, TableRef>() {
                        @Override
                        public Tuple2<String, TableRef> call(DimensionDesc dimensionDesc) throws Exception {
                            TableRef table = dimensionDesc.getTableRef();
                            return new Tuple2(table.getTableIdentity(), table);
                        }
                    }).groupByKey();

            snapShots.mapToPair(new DimensionDescPairFunction(cubeName, segmentId, jobId, config))
                    .filter(tuple -> tuple._2).count();
            Boolean dictsAndSnapshotsBuildState = isAllDictsAndSnapshotsReady(config, cubeName, segmentId);
            if(!dictsAndSnapshotsBuildState) {
                logger.error("Not all dictionaries and snapshots ready for cube segment: {}", segmentId);
            } else {
                logger.info("Succeed to build all dictionaries and snapshots for cube segment: {}", segmentId);
            }

            long recordCount = tblColRefRDD.count();
            logger.info("Map input records={}", recordCount);
            logger.info("HDFS Read: {} HDFS Write", bytesWritten.value());
            logger.info("HDFS: Number of bytes written={}", jobListener.metrics.getBytesWritten());

            Map<String, String> counterMap = Maps.newHashMap();
            counterMap.put(ExecutableConstants.HDFS_BYTES_WRITTEN, String.valueOf(jobListener.metrics.getBytesWritten()));
            counterMap.put(ExecutableConstants.SPARK_DIMENSION_DIC_SEGMENT_ID, segmentId);

            // save counter to hdfs
            HadoopUtil.writeToSequenceFile(sc.hadoopConfiguration(), counterPath, counterMap);
        }
    }


    static class TableColumnRefFunction implements PairFlatMapFunction<Iterator<TblColRef>, Boolean, TblColRef> {
        private transient volatile  boolean initialized = false;
        private String cubeName;
        private String segmentId;
        private CubeSegment cubeSegment;
        private CubeManager cubeManager;
        private KylinConfig config;
        private String factColumnsInputPath;
        private String dictPath;
        List<TblColRef> uhcColumns;

        public TableColumnRefFunction(String cubeName, String segmentId, KylinConfig config, String factColumnsInputPath, String dictPath, List<TblColRef> uhcColumns) {
            this.cubeName = cubeName;
            this.segmentId = segmentId;
            this.config = config;
            this.factColumnsInputPath = factColumnsInputPath;
            this.dictPath = dictPath;
            this.uhcColumns = uhcColumns;
            logger.info("Cube name is {}, segment id is {}", cubeName, segmentId);
            logger.info("Fact columns input path is " + factColumnsInputPath);
            logger.info("Fact columns input path is " + dictPath);
        }

        private void init() {
            try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig
                    .setAndUnsetThreadLocalConfig(config)) {
                cubeManager = CubeManager.getInstance(config);
                cubeSegment = cubeManager.getCube(cubeName).getSegmentById(segmentId);
            }
            initialized = true;
        }

        @Override
        public Iterator<Tuple2<Boolean, TblColRef>> call(Iterator<TblColRef> cols) throws Exception {
            if (initialized == false) {
                synchronized (SparkBuildDictionary.class) {
                    if (initialized == false) {
                        init();
                    }
                }
            }

            List<Tuple2<Boolean, TblColRef>> result = Lists.newArrayList();
            try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig
                    .setAndUnsetThreadLocalConfig(config)) {
                while (cols.hasNext()) {
                    TblColRef col = cols.next();
                    logger.info("Building dictionary for column {}", col);
                    IReadableTable inpTable = getDistinctValuesFor(col);

                    Dictionary<String> preBuiltDict = getDictionary(col);

                    if (preBuiltDict != null) {
                        logger.info("Dict for '{}' has already been built, save it", col.getName());
                        cubeManager.saveDictionary(cubeSegment, col, inpTable, preBuiltDict);
                    } else {
                        logger.info("Dict for '{}' not pre-built, build it from {}", col.getName(), inpTable);
                        cubeManager.buildDictionary(cubeSegment, col, inpTable);
                    }

                    result.add(new Tuple2<>(true, col));
                }
            }
            return result.iterator();
        }

        public IReadableTable getDistinctValuesFor(TblColRef col) {
            return new SortedColumnDFSFile(factColumnsInputPath + "/" + col.getIdentity(), col.getType());
        }

        public Dictionary<String> getDictionary(TblColRef col) throws IOException {
            Path colDir;
            if (config.isBuildUHCDictWithMREnabled() && uhcColumns.contains(col)) {
                colDir = new Path(dictPath, col.getIdentity());
            } else {
                colDir = new Path(factColumnsInputPath, col.getIdentity());
            }
            FileSystem fs = HadoopUtil.getWorkingFileSystem();

            Path dictFile = HadoopUtil.getFilterOnlyPath(fs, colDir,
                    col.getName() + FactDistinctColumnsReducer.DICT_FILE_POSTFIX);
            if (dictFile == null) {
                logger.info("Dict for '{}' not pre-built.", col.getName());
                return null;
            }

            try (SequenceFile.Reader reader = new SequenceFile.Reader(HadoopUtil.getCurrentConfiguration(),
                    SequenceFile.Reader.file(dictFile))) {
                NullWritable key = NullWritable.get();
                ArrayPrimitiveWritable value = new ArrayPrimitiveWritable();
                reader.next(key, value);

                ByteBuffer buffer = new ByteArray((byte[]) value.get()).asBuffer();
                try (DataInputStream is = new DataInputStream(new ByteBufferBackedInputStream(buffer))) {
                    String dictClassName = is.readUTF();
                    Dictionary<String> dict = (Dictionary<String>) ClassUtil.newInstance(dictClassName);
                    dict.readFields(is);
                    logger.info("DictionaryProvider read dict from file: {}", dictFile);
                    return dict;
                }
            }
        }
    }

    static class DimensionDescPairFunction implements PairFunction<Tuple2<String, Iterable<TableRef>>, String, Boolean> {

        private String cubeName;
        private String segmentId;
        private String jobId;
        private KylinConfig config;
        private CubeManager cubeManager;
        private CubeSegment cubeSegment;
        private transient volatile  boolean initialized = false;
        public DimensionDescPairFunction(String cubeName, String segmentId, String jobId, KylinConfig config) {
            this.cubeName = cubeName;
            this.segmentId = segmentId;
            this.jobId = jobId;
            this.config = config;
        }

        private void init() {
            try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig
                    .setAndUnsetThreadLocalConfig(config)) {
                cubeManager = CubeManager.getInstance(config);
                cubeSegment = cubeManager.getCube(cubeName).getSegmentById(segmentId);
            }
            initialized = true;
        }

        @Override
        public Tuple2<String, Boolean> call(Tuple2<String, Iterable<TableRef>> snapShot) throws Exception {
            if (initialized == false) {
                synchronized (SparkBuildDictionary.class) {
                    if (initialized == false) {
                        init();
                    }
                }
            }
            String tableIdentity = snapShot._1();
            boolean status = true;
            try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig
                    .setAndUnsetThreadLocalConfig(config)) {
                logger.info("Building snapshot of {}", tableIdentity);

                if (!cubeSegment.getModel().isLookupTable(tableIdentity) || cubeSegment.getCubeDesc().isExtSnapshotTable(tableIdentity)) {
                    return new Tuple2<>(tableIdentity, false);
                }

                cubeManager.buildSnapshotTable(cubeSegment, tableIdentity, jobId);
            }

            try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig
                    .setAndUnsetThreadLocalConfig(config)) {
                Iterator<TableRef> tableRefIterator = snapShot._2().iterator();

                CubeInstance updatedCube = cubeManager.getCube(cubeSegment.getCubeInstance().getName());
                CubeSegment cubeSeg = updatedCube.getSegmentById(cubeSegment.getUuid());

                while (tableRefIterator.hasNext()) {
                    TableRef tableRef = tableRefIterator.next();
                    logger.info("Checking snapshot of {}", tableRef);
                    try {
                        JoinDesc join = cubeSeg.getModel().getJoinsTree().getJoinByPKSide(tableRef);
                        ILookupTable table = cubeManager.getLookupTable(cubeSeg, join);
                        if (table != null) {
                            IOUtils.closeStream(table);
                        }
                    } catch (Exception e) {
                        status = false;
                        logger.error(String.format(Locale.ROOT, "Checking snapshot of %s failed, exception is %s.", tableRef, e.getMessage()));
                    }
                }
            }
            return new Tuple2<>(tableIdentity, status);
        }
    }

    static class DimensionDescFilterFunction implements Function<DimensionDesc, Boolean> {
        private String cubeName;
        private String segmentId;
        private KylinConfig config;
        private CubeSegment cubeSegment;
        private transient volatile  boolean initialized = false;

        public DimensionDescFilterFunction(String cubeName, String segmentId, KylinConfig config) {
            this.cubeName = cubeName;
            this.segmentId = segmentId;
            this.config = config;
        }

        private void init() {
            try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig
                    .setAndUnsetThreadLocalConfig(config)) {
                cubeSegment = CubeManager.getInstance(config).getCube(cubeName).getSegmentById(segmentId);
            }
            initialized = true;
        }

        @Override
        public Boolean call(DimensionDesc dimensionDesc) throws Exception {
            if (initialized == false) {
                synchronized (SparkBuildDictionary.class) {
                    if (initialized == false) {
                        init();
                    }
                }
            }

            return !cubeSegment.getCubeDesc().isExtSnapshotTable(dimensionDesc.getTableRef().getTableIdentity());
        }
    }

    private boolean isAllDictsAndSnapshotsReady(KylinConfig config, String cubeName, String segmentID) {
        final CubeManager cubeManager = CubeManager.getInstance(config);
        CubeInstance cube = cubeManager.reloadCube(cubeName);
        CubeSegment segment = cube.getSegmentById(segmentID);
        ResourceStore store = ResourceStore.getStore(config);

        // check dicts
        logger.info("Begin to check if all dictionaries exist of Segment: {}", segmentID);
        Map<String, String> dictionaries = segment.getDictionaries();
        logger.info("Get dictionaries number: {}", dictionaries.size());
        for (Map.Entry<String, String> entry : dictionaries.entrySet()) {
            String dictResPath = entry.getValue();
            String dictKey = entry.getKey();
            try {
                DictionaryInfo dictInfo = store.getResource(dictResPath, DictionaryInfoSerializer.INFO_SERIALIZER);
                if (dictInfo == null) {
                    logger.warn("Dictionary=[key: {}, resource path: {}] doesn't exist in resource store", dictKey,
                            dictResPath);
                    return false;
                }
            } catch (IOException e) {
                logger.warn("Dictionary=[key: {}, path: {}] failed to check, details: {}", dictKey, dictResPath, e);
                return false;
            }
        }

        // check snapshots
        logger.info("Begin to check if all snapshots exist of Segment: {}", segmentID);
        Map<String, String> snapshots = segment.getSnapshots();
        logger.info("Get snapshot number: {}", snapshots.size());
        for (Map.Entry<String, String> entry : snapshots.entrySet()) {
            String snapshotKey = entry.getKey();
            String snapshotResPath = entry.getValue();
            try {
                SnapshotTable snapshot = store.getResource(snapshotResPath, SnapshotTableSerializer.INFO_SERIALIZER);
                if (snapshot == null) {
                    logger.info("SnapshotTable=[key: {}, resource path: {}] doesn't exist in resource store",
                            snapshotKey, snapshotResPath);
                    return false;
                }
            } catch (IOException e) {
                logger.warn("SnapshotTable=[key: {}, resource path: {}]  failed to check, details: {}", snapshotKey,
                        snapshotResPath, e);
                return false;
            }
        }

        logger.info("All dictionaries and snapshots exist checking succeed for Cube Segment: {}", segmentID);

        return true;
    }
}


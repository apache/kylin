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

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
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
import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.cube.model.DimensionDesc;
import org.apache.kylin.dict.DictionaryInfo;
import org.apache.kylin.dict.DictionaryInfoSerializer;
import org.apache.kylin.dict.DictionaryManager;
import org.apache.kylin.dict.lookup.ILookupTable;
import org.apache.kylin.dict.lookup.SnapshotManager;
import org.apache.kylin.dict.lookup.SnapshotTable;
import org.apache.kylin.dict.lookup.SnapshotTableSerializer;
import org.apache.kylin.engine.mr.SortedColumnDFSFile;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.SerializableConfiguration;
import org.apache.kylin.engine.mr.steps.FactDistinctColumnsReducer;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.source.IReadableTable;
import org.apache.kylin.source.SourceManager;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.Tuple3;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
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

            List<Tuple2<String, Tuple3<String, Integer, Integer>>> dictsInfoMap = tblColRefRDD
                    .mapToPair(new DimensionDictsBuildFunction(cubeName, segmentId, config, factColumnsInputPath, dictPath, uhcColumns))
                    .collect();

            cubeSegment = CubeManager.getInstance(config).reloadCube(cubeName).getSegmentById(segmentId);
            JavaRDD<DimensionDesc> dimensionDescRDD = sc.parallelize(cubeSegment.getCubeDesc().getDimensions());
            List<Tuple2<String, String>> snapshotPathMap = dimensionDescRDD.filter(new SnapshotFilterFunction(cubeName, segmentId, config))
                    .mapToPair(dimensionDesc -> new Tuple2<>(dimensionDesc.getTableRef().getTableIdentity(), dimensionDesc.getTableRef()))
                    .groupByKey()
                    .mapToPair(new SnapshotBuildFunction(cubeName, segmentId, jobId, config))
                    .filter(tuple -> !tuple._2.isEmpty())
                    .collect();

            // work on copy instead of cached objects
            CubeInstance cubeCopy = cubeSegment.getCubeInstance().latestCopyForWrite(); // get a latest copy
            CubeSegment segCopy = cubeCopy.getSegmentById(cubeSegment.getUuid());
            for (Tuple2<String, Tuple3<String, Integer, Integer>> tuple2 : dictsInfoMap) {
                Tuple3<String, Integer, Integer> dictInfo = tuple2._2;
                segCopy.getDictionaries().put(tuple2._1, dictInfo._1());
                segCopy.getRowkeyStats().add(new Object[] { tuple2._1, dictInfo._2(), dictInfo._3() });
            }

            for (Tuple2<String, String> tuple2 : snapshotPathMap) {
                segCopy.putSnapshotResPath(tuple2._1, tuple2._2);
            }

            CubeUpdate update = new CubeUpdate(cubeCopy);
            update.setToUpdateSegs(segCopy);
            CubeManager.getInstance(config).updateCube(update);

            checkSnapshot(CubeManager.getInstance(config), CubeManager.getInstance(config).getCube(cubeName).getSegmentById(segmentId));

            boolean dictsAndSnapshotsBuildState = isAllDictsAndSnapshotsReady(config, cubeName, segmentId);
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

    static class DimensionDictsBuildFunction implements PairFunction<TblColRef, String, Tuple3<String, Integer, Integer>> {
        private transient volatile  boolean initialized = false;
        private String cubeName;
        private String segmentId;
        private CubeSegment cubeSegment;
        private DictionaryManager dictManager;
        private KylinConfig config;
        private String factColumnsInputPath;
        private String dictPath;
        private List<TblColRef> uhcColumns;

        public DimensionDictsBuildFunction(String cubeName, String segmentId, KylinConfig config, String factColumnsInputPath, String dictPath, List<TblColRef> uhcColumns) {
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
                cubeSegment = CubeManager.getInstance(config).getCube(cubeName).getSegmentById(segmentId);
                dictManager = DictionaryManager.getInstance(config);
            }
            initialized = true;
        }

        @Override
        public Tuple2<String, Tuple3<String, Integer, Integer>> call(TblColRef tblColRef) throws Exception {
            if (initialized == false) {
                synchronized (SparkBuildDictionary.class) {
                    if (initialized == false) {
                        init();
                    }
                }
            }

            logger.info("Building dictionary for column {}", tblColRef);
            IReadableTable inpTable = getDistinctValuesFor(tblColRef);
            Dictionary<String> preBuiltDict;
            DictionaryInfo dictInfo;
            try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig
                    .setAndUnsetThreadLocalConfig(config)) {
                preBuiltDict = getDictionary(tblColRef);

                if (preBuiltDict != null) {
                    logger.info("Dict for '{}' has already been built, save it", tblColRef.getName());
                    dictInfo = dictManager.saveDictionary(tblColRef, inpTable, preBuiltDict);
                } else {
                    logger.info("Dict for '{}' not pre-built, build it from {}", tblColRef.getName(), inpTable);
                    String builderClass = cubeSegment.getCubeDesc().getDictionaryBuilderClass(tblColRef);
                    dictInfo = dictManager.buildDictionary(tblColRef, inpTable, builderClass);
                    preBuiltDict = dictInfo.getDictionaryObject();
                }
            }

            return new Tuple2<>(tblColRef.getIdentity(),
                    new Tuple3<>(dictInfo.getResourcePath(), preBuiltDict.getSize(), preBuiltDict.getSizeOfId()));
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
            //FileSystem fs = colDir.getFileSystem(HadoopUtil.getCurrentConfiguration());

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

    static class SnapshotBuildFunction implements PairFunction<Tuple2<String, Iterable<TableRef>>, String, String> {

        private String cubeName;
        private String segmentId;
        private String jobId;
        private KylinConfig config;
        private CubeManager cubeManager;
        private CubeSegment cubeSegment;
        private transient volatile  boolean initialized = false;
        public SnapshotBuildFunction(String cubeName, String segmentId, String jobId, KylinConfig config) {
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
        public Tuple2<String, String> call(Tuple2<String, Iterable<TableRef>> snapShot) throws Exception {
            if (initialized == false) {
                synchronized (SparkBuildDictionary.class) {
                    if (initialized == false) {
                        init();
                    }
                }
            }
            String tableIdentity = snapShot._1();
            String snapshotPath = "";
            try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig
                    .setAndUnsetThreadLocalConfig(config)) {
                logger.info("Building snapshot of {}", tableIdentity);

                if (cubeSegment.getModel().isLookupTable(tableIdentity) && !cubeSegment.getCubeDesc().isExtSnapshotTable(tableIdentity)) {
                    try {
                        snapshotPath = buildSnapshotTable(config, cubeSegment, tableIdentity, jobId);
                    } catch (IOException e) {
                        logger.error("Error while build snapshot table " + tableIdentity + ", " + e.getMessage());
                        return new Tuple2<>(tableIdentity, snapshotPath);
                    }
                }
            }

            return new Tuple2<>(tableIdentity, snapshotPath);
        }

        private String buildSnapshotTable(KylinConfig config, CubeSegment cubeSeg, String lookupTable, String uuid) throws IOException{
            CubeInstance cubeCopy = cubeSeg.getCubeInstance().latestCopyForWrite(); // get a latest copy
            CubeSegment segCopy = cubeCopy.getSegmentById(cubeSeg.getUuid());

            TableMetadataManager metaMgr = TableMetadataManager.getInstance(config);
            SnapshotManager snapshotMgr = SnapshotManager.getInstance(config);

            TableDesc tableDesc = new TableDesc(metaMgr.getTableDesc(lookupTable, segCopy.getProject()));
            IReadableTable hiveTable = SourceManager.createReadableTable(tableDesc, uuid);
            SnapshotTable snapshot = snapshotMgr.buildSnapshot(hiveTable, tableDesc, cubeSeg.getConfig());
            return snapshot.getResourcePath();
        }
    }

    static class SnapshotFilterFunction implements Function<DimensionDesc, Boolean> {
        private String cubeName;
        private String segmentId;
        private KylinConfig config;
        private CubeSegment cubeSegment;
        private transient volatile  boolean initialized = false;

        public SnapshotFilterFunction(String cubeName, String segmentId, KylinConfig config) {
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

    private void checkSnapshot(CubeManager cubeManager, CubeSegment cubeSegment) {
        List<DimensionDesc> dimensionDescs = cubeSegment.getCubeDesc().getDimensions();
        for (DimensionDesc dimensionDesc : dimensionDescs) {
            TableRef lookup = dimensionDesc.getTableRef();
            String tableIdentity = lookup.getTableIdentity();
            if (cubeSegment.getModel().isLookupTable(tableIdentity) && !cubeSegment.getCubeDesc().isExtSnapshotTable(tableIdentity)) {
                logger.info("Checking snapshot of {}", lookup);
                try {
                    JoinDesc join = cubeSegment.getModel().getJoinsTree().getJoinByPKSide(lookup);
                    ILookupTable table = cubeManager.getLookupTable(cubeSegment, join);
                    if (table != null) {
                        IOUtils.closeStream(table);
                    }
                } catch (Throwable th) {
                    throw new RuntimeException(String.format(Locale.ROOT, "Checking snapshot of %s failed.", lookup), th);
                }
            }
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


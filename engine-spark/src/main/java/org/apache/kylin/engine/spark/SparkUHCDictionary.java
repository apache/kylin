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
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.dict.DictionaryGenerator;
import org.apache.kylin.dict.DictionaryInfo;
import org.apache.kylin.dict.IDictionaryBuilder;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.SerializableConfiguration;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.Tuple3;

import java.io.DataOutputStream;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.kylin.engine.mr.steps.FactDistinctColumnsReducer.DICT_FILE_POSTFIX;

public class SparkUHCDictionary extends AbstractApplication implements Serializable {
    protected static final Logger logger = LoggerFactory.getLogger(SparkUHCDictionary.class);

    public static final Option OPTION_CUBE_NAME = OptionBuilder.withArgName(BatchConstants.ARG_CUBE_NAME).hasArg()
            .isRequired(true).withDescription("Cube Name").create(BatchConstants.ARG_CUBE_NAME);
    public static final Option OPTION_META_URL = OptionBuilder.withArgName("metaUrl").hasArg().isRequired(true)
            .withDescription("HDFS metadata url").create("metaUrl");
    public static final Option OPTION_OUTPUT_PATH = OptionBuilder.withArgName(BatchConstants.ARG_OUTPUT).hasArg()
            .isRequired(true).withDescription("Cube output path").create(BatchConstants.ARG_OUTPUT);
    public static final Option OPTION_SEGMENT_ID = OptionBuilder.withArgName("segmentId").hasArg().isRequired(true)
            .withDescription("Cube Segment Id").create("segmentId");
    public static final Option OPTION_INPUT_PATH = OptionBuilder.withArgName(BatchConstants.ARG_INPUT).hasArg()
            .isRequired(true).withDescription("Hive Intermediate Table PATH").create(BatchConstants.ARG_INPUT);
    public static final Option OPTION_CUBING_JOB_ID = OptionBuilder
            .withArgName(BatchConstants.ARG_CUBING_JOB_ID).hasArg().isRequired(true)
            .withDescription("Cubing job id").create(BatchConstants.ARG_CUBING_JOB_ID);
    public static final Option OPTION_COUNTER_PATH = OptionBuilder.withArgName(BatchConstants.ARG_COUNTER_OUTPUT)
            .hasArg().isRequired(true).withDescription("counter output path").create(BatchConstants.ARG_COUNTER_OUTPUT);


    private Options options;

    public SparkUHCDictionary() {
        options = new Options();
        options.addOption(OPTION_CUBE_NAME);
        options.addOption(OPTION_META_URL);
        options.addOption(OPTION_OUTPUT_PATH);
        options.addOption(OPTION_INPUT_PATH);
        options.addOption(OPTION_SEGMENT_ID);
        options.addOption(OPTION_CUBING_JOB_ID);
        options.addOption(OPTION_COUNTER_PATH);
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        String cubeName = optionsHelper.getOptionValue(OPTION_CUBE_NAME);
        String metaUrl = optionsHelper.getOptionValue(OPTION_META_URL);
        String segmentId = optionsHelper.getOptionValue(OPTION_SEGMENT_ID);
        String inputPath = optionsHelper.getOptionValue(OPTION_INPUT_PATH);
        String outputPath = optionsHelper.getOptionValue(OPTION_OUTPUT_PATH);
        String counterPath = optionsHelper.getOptionValue(OPTION_COUNTER_PATH);

        Class[] kryoClassArray = new Class[]{Class.forName("scala.reflect.ClassTag$$anon$1"),
                Class.forName("org.apache.kylin.engine.mr.steps.SelfDefineSortableKey")};

        SparkConf conf = new SparkConf().setAppName("Build uhc dictionary with spark for:" + cubeName + " segment " + segmentId);
        //serialization conf
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "org.apache.kylin.engine.spark.KylinKryoRegistrator");
        conf.set("spark.kryo.registrationRequired", "true").registerKryoClasses(kryoClassArray);

        KylinSparkJobListener jobListener = new KylinSparkJobListener();
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            sc.sc().addSparkListener(jobListener);
            HadoopUtil.deletePath(sc.hadoopConfiguration(), new Path(outputPath));

            Configuration hadoopConf = sc.hadoopConfiguration();
            hadoopConf.set("mapreduce.input.pathFilter.class", "org.apache.kylin.engine.mr.steps.filter.UHCDictPathFilter");

            final SerializableConfiguration sConf = new SerializableConfiguration(hadoopConf);
            KylinConfig config = AbstractHadoopJob.loadKylinConfigFromHdfs(sConf, metaUrl);

            CubeManager cubeMgr = CubeManager.getInstance(config);
            CubeInstance cube = cubeMgr.getCube(cubeName);
            final Job job = Job.getInstance(sConf.get());

            // calculate source record bytes size
            final LongAccumulator bytesWritten = sc.sc().longAccumulator();
            String hdfsDir = sc.hadoopConfiguration().get(BatchConstants.CFG_GLOBAL_DICT_BASE_DIR);

            List<TblColRef> uhcColumns = cube.getDescriptor().getAllUHCColumns();
            int reducerCount = uhcColumns.size();
            if (reducerCount == 0) {
                return;
            }

            logger.info("RDD Output path: {}", outputPath);
            logger.info("getTotalReducerNum: {}", reducerCount);
            logger.info("counter path {}", counterPath);

            JavaPairRDD<String, String> wholeSequenceFileNames = null;
            for (TblColRef tblColRef : uhcColumns) {
                String columnPath = inputPath + "/" + tblColRef.getIdentity();
                if (!HadoopUtil.getFileSystem(columnPath).exists(new Path(columnPath))) {
                    continue;
                }
                if (wholeSequenceFileNames == null) {
                    wholeSequenceFileNames = sc.wholeTextFiles(columnPath);
                } else {
                    wholeSequenceFileNames = wholeSequenceFileNames.union(sc.wholeTextFiles(columnPath));
                }
            }

            if (wholeSequenceFileNames == null) {
                logger.error("There're no sequence files at " + inputPath + " !");
                return;
            }

            JavaPairRDD<String, Tuple3<Writable, Writable, String>> pairRDD = wholeSequenceFileNames.map(tuple -> tuple._1)
                    .mapToPair(new InputPathAndFilterAddFunction2(config, uhcColumns))
                    .filter(tuple -> tuple._1 != -1)
                    .reduceByKey((list1, list2) -> combineAllColumnDistinctValues(list1, list2))
                    .mapToPair(new ProcessUHCColumnValues(cubeName, config, hdfsDir, uhcColumns));

            MultipleOutputs.addNamedOutput(job, BatchConstants.CFG_OUTPUT_DICT, SequenceFileOutputFormat.class,
                    NullWritable.class, ArrayPrimitiveWritable.class);

            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            job.getConfiguration().set(BatchConstants.CFG_OUTPUT_PATH, outputPath);
            //prevent to create zero-sized default output
            LazyOutputFormat.setOutputFormatClass(job, SequenceFileOutputFormat.class);

            MultipleOutputsRDD multipleOutputsRDD = MultipleOutputsRDD.rddToMultipleOutputsRDD(pairRDD);
            multipleOutputsRDD.saveAsNewAPIHadoopDatasetWithMultipleOutputs(job.getConfiguration());

            logger.info("Map input records={}", reducerCount);
            logger.info("HDFS Read: {} HDFS Write", bytesWritten.value());

            Map<String, String> counterMap = Maps.newHashMap();
            counterMap.put(ExecutableConstants.SOURCE_RECORDS_COUNT, String.valueOf(reducerCount));
            counterMap.put(ExecutableConstants.SOURCE_RECORDS_SIZE, String.valueOf(bytesWritten.value()));

            // save counter to hdfs
            HadoopUtil.writeToSequenceFile(sc.hadoopConfiguration(), counterPath, counterMap);
            HadoopUtil.deleteHDFSMeta(metaUrl);
        }
    }

    static class InputPathAndFilterAddFunction2 implements PairFunction<String, Integer, List<String>> {
        private List<TblColRef> uhcColumns;
        private KylinConfig config;

        public InputPathAndFilterAddFunction2(KylinConfig config, List<TblColRef> uhcColumns) {
            this.config = config;
            this.uhcColumns = uhcColumns;
        }

        @Override
        public Tuple2<Integer, List<String>> call(String sequenceFilePath) throws Exception {
            Path path = new Path(sequenceFilePath);
            logger.info("Column absolute path is " + path.toString());
            if (!HadoopUtil.getFileSystem(path).exists(path)) {
                return new Tuple2<>(-1, null);
            }

            String columnName = path.getParent().getName();
            int index = -1;
            for (int i = 0;i < uhcColumns.size(); i++) {
                if (uhcColumns.get(i).getIdentity().equalsIgnoreCase(columnName)) {
                    index = i;
                    break;
                }
            }

            if (index == -1) {
                return new Tuple2<>(-1, null);
            }

            try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig
                    .setAndUnsetThreadLocalConfig(config)) {
                List<String> values = Lists.newArrayList();
                values.addAll(HadoopUtil.readDistinctColumnValues(sequenceFilePath));

                logger.info("UHC column " + columnName + " contains distinct values " + values);

                return new Tuple2<>(index, values);
            }
        }
    }

    static class ProcessUHCColumnValues implements PairFunction<Tuple2<Integer, List<String>>, String, Tuple3<Writable, Writable, String>> {
        private transient volatile  boolean initialized = false;
        private String cubeName;
        private KylinConfig config;
        private IDictionaryBuilder builder;
        private CubeInstance cube;
        private String hdfsDir;
        private CubeDesc cubeDesc;
        private List<TblColRef> uhcColumns;

        public ProcessUHCColumnValues(String cubeName, KylinConfig config, String hdfsDir, List<TblColRef> uhcColumns) {
            this.cubeName = cubeName;
            this.config = config;
            this.uhcColumns = uhcColumns;
            this.hdfsDir = hdfsDir;
        }

        private void init() {
            try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig
                    .setAndUnsetThreadLocalConfig(config)) {
                cube = CubeManager.getInstance(config).getCube(cubeName);
                cubeDesc = CubeDescManager.getInstance(config).getCubeDesc(cubeName);
            }
            initialized = true;
        }


        @Override
        public Tuple2<String, Tuple3<Writable, Writable, String>> call(Tuple2<Integer, List<String>> columnValues) throws Exception {
            if (initialized == false) {
                synchronized (SparkFactDistinct.class) {
                    if (initialized == false) {
                        init();
                    }
                }
            }

            try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig.setAndUnsetThreadLocalConfig(config);
                 ByteArrayOutputStream baos = new ByteArrayOutputStream();
                 DataOutputStream outputStream = new DataOutputStream(baos)) {
                TblColRef col = uhcColumns.get(columnValues._1);
                logger.info("Processing column " + col.getName());
                if (cube.getDescriptor().getShardByColumns().contains(col)) {
                    //for ShardByColumns
                    builder = DictionaryGenerator.newDictionaryBuilder(col.getType());
                    builder.init(null, 0, null);
                } else {
                    //for GlobalDictionaryColumns
                    DictionaryInfo dictionaryInfo = new DictionaryInfo(col.getColumnDesc(), col.getDatatype());
                    String builderClass = cubeDesc.getDictionaryBuilderClass(col);
                    builder = (IDictionaryBuilder) ClassUtil.newInstance(builderClass);
                    builder.init(dictionaryInfo, 0, hdfsDir);
                }
                Iterator<String> values = columnValues._2.iterator();
                while (values.hasNext()) {
                    builder.addValue(values.next());
                }
                Dictionary<String> dict = builder.build();
                String dictFileName = col.getIdentity() + "/" + col.getName() + DICT_FILE_POSTFIX;
                logger.info("Dictionary file name is " + dictFileName);

                outputStream.writeUTF(dict.getClass().getName());
                dict.write(outputStream);
                Tuple3 tuple3 = new Tuple3(NullWritable.get(), new ArrayPrimitiveWritable(baos.toByteArray()), dictFileName);
                return new Tuple2<>(BatchConstants.CFG_OUTPUT_DICT, tuple3);
            }
        }
    }

    private List combineAllColumnDistinctValues(List<String> list1, List<String> list2) {
        list1.addAll(list2);
        return list1;
    }
}

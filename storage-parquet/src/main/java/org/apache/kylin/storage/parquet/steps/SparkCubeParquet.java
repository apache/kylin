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
package org.apache.kylin.storage.parquet.steps;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.kv.RowKeyDecoder;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.dimension.AbstractDateDimEnc;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.dimension.FixedLenDimEnc;
import org.apache.kylin.dimension.FixedLenHexDimEnc;
import org.apache.kylin.dimension.IDimensionEncodingMap;
import org.apache.kylin.engine.mr.BatchCubingJobBuilder2;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.CubeStatsReader;
import org.apache.kylin.engine.mr.common.SerializableConfiguration;
import org.apache.kylin.engine.spark.KylinSparkJobListener;
import org.apache.kylin.engine.spark.SparkUtil;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.measure.BufferedMeasureCodec;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.basic.BasicMeasureType;
import org.apache.kylin.measure.basic.BigDecimalIngester;
import org.apache.kylin.measure.basic.DoubleIngester;
import org.apache.kylin.measure.basic.LongIngester;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Random;


public class SparkCubeParquet extends AbstractApplication implements Serializable{

    protected static final Logger logger = LoggerFactory.getLogger(SparkCubeParquet.class);

    public static final Option OPTION_CUBE_NAME = OptionBuilder.withArgName(BatchConstants.ARG_CUBE_NAME).hasArg()
            .isRequired(true).withDescription("Cube Name").create(BatchConstants.ARG_CUBE_NAME);
    public static final Option OPTION_SEGMENT_ID = OptionBuilder.withArgName("segment").hasArg().isRequired(true)
            .withDescription("Cube Segment Id").create("segmentId");
    public static final Option OPTION_META_URL = OptionBuilder.withArgName("metaUrl").hasArg().isRequired(true)
            .withDescription("HDFS metadata url").create("metaUrl");
    public static final Option OPTION_OUTPUT_PATH = OptionBuilder.withArgName(BatchConstants.ARG_OUTPUT).hasArg()
            .isRequired(true).withDescription("Paqruet output path").create(BatchConstants.ARG_OUTPUT);
    public static final Option OPTION_INPUT_PATH = OptionBuilder.withArgName(BatchConstants.ARG_INPUT).hasArg()
            .isRequired(true).withDescription("Cuboid files PATH").create(BatchConstants.ARG_INPUT);
    public static final Option OPTION_COUNTER_PATH = OptionBuilder.withArgName(BatchConstants.ARG_COUNTER_OUPUT).hasArg()
            .isRequired(true).withDescription("Counter output path").create(BatchConstants.ARG_COUNTER_OUPUT);

    private Options options;

    public SparkCubeParquet(){
        options = new Options();
        options.addOption(OPTION_INPUT_PATH);
        options.addOption(OPTION_CUBE_NAME);
        options.addOption(OPTION_SEGMENT_ID);
        options.addOption(OPTION_META_URL);
        options.addOption(OPTION_OUTPUT_PATH);
        options.addOption(OPTION_COUNTER_PATH);
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        final String metaUrl = optionsHelper.getOptionValue(OPTION_META_URL);
        final String inputPath = optionsHelper.getOptionValue(OPTION_INPUT_PATH);
        final String cubeName = optionsHelper.getOptionValue(OPTION_CUBE_NAME);
        final String segmentId = optionsHelper.getOptionValue(OPTION_SEGMENT_ID);
        final String outputPath = optionsHelper.getOptionValue(OPTION_OUTPUT_PATH);
        final String counterPath = optionsHelper.getOptionValue(OPTION_COUNTER_PATH);

        Class[] kryoClassArray = new Class[] { Class.forName("scala.reflect.ClassTag$$anon$1"), Text.class, Group.class};

        SparkConf conf = new SparkConf().setAppName("Converting Parquet File for: " + cubeName + " segment " + segmentId);
        //serialization conf
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "org.apache.kylin.engine.spark.KylinKryoRegistrator");
        conf.set("spark.kryo.registrationRequired", "true").registerKryoClasses(kryoClassArray);


        KylinSparkJobListener jobListener = new KylinSparkJobListener();
        try (JavaSparkContext sc = new JavaSparkContext(conf)){
            sc.sc().addSparkListener(jobListener);
            final SerializableConfiguration sConf = new SerializableConfiguration(sc.hadoopConfiguration());

            final KylinConfig envConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(sConf, metaUrl);

            final CubeInstance cubeInstance = CubeManager.getInstance(envConfig).getCube(cubeName);
            final CubeSegment cubeSegment = cubeInstance.getSegmentById(segmentId);


            final FileSystem fs = new Path(inputPath).getFileSystem(sc.hadoopConfiguration());

            final int totalLevels = cubeSegment.getCuboidScheduler().getBuildLevel();
            JavaPairRDD<Text, Text>[] allRDDs = new JavaPairRDD[totalLevels + 1];

            final Job job = Job.getInstance(sConf.get());
            logger.info("Input path: {}", inputPath);
            logger.info("Output path: {}", outputPath);

            // Read from cuboid and save to parquet
            for (int level = 0; level <= totalLevels; level++) {
                String cuboidPath = BatchCubingJobBuilder2.getCuboidOutputPathsByLevel(inputPath, level);
                allRDDs[level] = SparkUtil.parseInputPath(cuboidPath, fs, sc, Text.class, Text.class);
                saveToParquet(allRDDs[level], metaUrl, cubeName, cubeSegment, outputPath, level, job, envConfig);
            }

            Map<String, String> counterMap = Maps.newHashMap();
            counterMap.put(ExecutableConstants.HDFS_BYTES_WRITTEN, String.valueOf(jobListener.metrics.getBytesWritten()));

            // save counter to hdfs
            HadoopUtil.writeToSequenceFile(sc.hadoopConfiguration(), counterPath, counterMap);
        }

    }

    protected void saveToParquet(JavaPairRDD<Text, Text> rdd, String metaUrl, String cubeName, CubeSegment cubeSeg, String parquetOutput, int level, Job job, KylinConfig kylinConfig) throws Exception {
        final IDimensionEncodingMap dimEncMap = cubeSeg.getDimensionEncodingMap();

        Cuboid baseCuboid = Cuboid.getBaseCuboid(cubeSeg.getCubeDesc());

        final Map<TblColRef, String> colTypeMap = Maps.newHashMap();
        final Map<MeasureDesc, String> meaTypeMap = Maps.newHashMap();

        MessageType schema = cuboidToMessageType(baseCuboid, dimEncMap, cubeSeg.getCubeDesc(), colTypeMap, meaTypeMap);

        logger.info("Schema: {}", schema.toString());

        final CuboidToPartitionMapping cuboidToPartitionMapping = new CuboidToPartitionMapping(cubeSeg, kylinConfig, level);

        logger.info("CuboidToPartitionMapping: {}", cuboidToPartitionMapping.toString());

        JavaPairRDD<Text, Text> repartitionedRDD = rdd.partitionBy(new CuboidPartitioner(cuboidToPartitionMapping));

        String output = BatchCubingJobBuilder2.getCuboidOutputPathsByLevel(parquetOutput, level);

        job.setOutputFormatClass(CustomParquetOutputFormat.class);
        GroupWriteSupport.setSchema(schema, job.getConfiguration());
        CustomParquetOutputFormat.setOutputPath(job, new Path(output));
        CustomParquetOutputFormat.setWriteSupportClass(job, GroupWriteSupport.class);
        CustomParquetOutputFormat.setCuboidToPartitionMapping(job, cuboidToPartitionMapping);

        JavaPairRDD<Void, Group> groupRDD = rdd.partitionBy(new CuboidPartitioner(cuboidToPartitionMapping))
                                                .mapToPair(new GenerateGroupRDDFunction(cubeName, cubeSeg.getUuid(), metaUrl, new SerializableConfiguration(job.getConfiguration()), colTypeMap, meaTypeMap));

        groupRDD.saveAsNewAPIHadoopDataset(job.getConfiguration());
    }

    static class CuboidPartitioner extends Partitioner {

        private CuboidToPartitionMapping mapping;

        public CuboidPartitioner(CuboidToPartitionMapping cuboidToPartitionMapping) {
            this.mapping = cuboidToPartitionMapping;
        }

        @Override
        public int numPartitions() {
            return mapping.getNumPartitions();
        }

        @Override
        public int getPartition(Object key) {
            Text textKey = (Text)key;
            return mapping.getPartitionForCuboidId(textKey.getBytes());
        }
    }

    public static class CuboidToPartitionMapping implements Serializable {
        private Map<Long, List<Integer>> cuboidPartitions;
        private int partitionNum;

        public CuboidToPartitionMapping(Map<Long, List<Integer>> cuboidPartitions) {
            this.cuboidPartitions = cuboidPartitions;
            int partitions = 0;
            for (Map.Entry<Long, List<Integer>> entry : cuboidPartitions.entrySet()) {
                partitions = partitions + entry.getValue().size();
            }
            this.partitionNum = partitions;
        }

        public CuboidToPartitionMapping(CubeSegment cubeSeg, KylinConfig kylinConfig, int level) throws IOException {
            cuboidPartitions = Maps.newHashMap();

            List<Long> layeredCuboids = cubeSeg.getCuboidScheduler().getCuboidsByLayer().get(level);
            CubeStatsReader cubeStatsReader = new CubeStatsReader(cubeSeg, kylinConfig);

            int position = 0;
            for (Long cuboidId : layeredCuboids) {
                int partition = estimateCuboidPartitionNum(cuboidId, cubeStatsReader, kylinConfig);
                List<Integer> positions = Lists.newArrayListWithCapacity(partition);

                for (int i = position; i < position + partition; i++) {
                    positions.add(i);
                }

                cuboidPartitions.put(cuboidId, positions);
                position = position + partition;
            }

            this.partitionNum = position;
        }

        public String serialize() throws JsonProcessingException {
            return JsonUtil.writeValueAsString(cuboidPartitions);
        }

        public static CuboidToPartitionMapping deserialize(String jsonMapping) throws IOException {
            Map<Long, List<Integer>> cuboidPartitions = JsonUtil.readValue(jsonMapping, new TypeReference<Map<Long, List<Integer>>>() {});
            return new CuboidToPartitionMapping(cuboidPartitions);
        }

        public int getNumPartitions() {
            return this.partitionNum;
        }

        public long getCuboidIdByPartition(int partition) {
            for (Map.Entry<Long, List<Integer>> entry : cuboidPartitions.entrySet()) {
                if (entry.getValue().contains(partition)) {
                    return entry.getKey();
                }
            }

            throw new IllegalArgumentException("No cuboidId for partition id: " + partition);
        }

        public int getPartitionForCuboidId(byte[] key) {
            long cuboidId = Bytes.toLong(key, RowConstants.ROWKEY_SHARDID_LEN, RowConstants.ROWKEY_CUBOIDID_LEN);
            List<Integer> partitions = cuboidPartitions.get(cuboidId);
            int partitionKey = mod(key, RowConstants.ROWKEY_COL_DEFAULT_LENGTH, key.length, partitions.size());

            return partitions.get(partitionKey);
        }

        private int mod(byte[] src, int start, int end, int total) {
            int sum = Bytes.hashBytes(src, start, end - start);
            int mod = sum % total;
            if (mod < 0)
                mod += total;

            return mod;
        }

        public int getPartitionNumForCuboidId(long cuboidId) {
            return cuboidPartitions.get(cuboidId).size();
        }

        public String getPartitionFilePrefix(int partition) {
            String prefix = "cuboid_";
            long cuboid = getCuboidIdByPartition(partition);
            int partNum = partition % getPartitionNumForCuboidId(cuboid);
            prefix = prefix + cuboid + "_part" + partNum;

            return prefix;
        }

        private int estimateCuboidPartitionNum(long cuboidId, CubeStatsReader cubeStatsReader, KylinConfig kylinConfig) {
            double cuboidSize = cubeStatsReader.estimateCuboidSize(cuboidId);
            float rddCut = kylinConfig.getSparkRDDPartitionCutMB();
            int partition = (int) (cuboidSize / (rddCut * 10));
            partition = Math.max(kylinConfig.getSparkMinPartition(), partition);
            partition = Math.min(kylinConfig.getSparkMaxPartition(), partition);
            return partition;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<Long, List<Integer>> entry : cuboidPartitions.entrySet()) {
                sb.append("cuboidId:").append(entry.getKey()).append(" [").append(StringUtils.join(entry.getValue(), ",")).append("]\n");
            }

            return sb.toString();
        }
    }

    public static class CustomParquetOutputFormat extends ParquetOutputFormat {
        public static final String CUBOID_TO_PARTITION_MAPPING = "cuboidToPartitionMapping";

        @Override
        public Path getDefaultWorkFile(TaskAttemptContext context, String extension) throws IOException {
            FileOutputCommitter committer = (FileOutputCommitter)this.getOutputCommitter(context);
            TaskID taskId = context.getTaskAttemptID().getTaskID();
            int partition = taskId.getId();

            CuboidToPartitionMapping mapping = CuboidToPartitionMapping.deserialize(context.getConfiguration().get(CUBOID_TO_PARTITION_MAPPING));

            return new Path(committer.getWorkPath(), getUniqueFile(context, mapping.getPartitionFilePrefix(partition)+ "-" + getOutputName(context), extension));
        }

        public static void setCuboidToPartitionMapping(Job job, CuboidToPartitionMapping cuboidToPartitionMapping) throws IOException {
            String jsonStr = cuboidToPartitionMapping.serialize();

            job.getConfiguration().set(CUBOID_TO_PARTITION_MAPPING, jsonStr);
        }
    }

    static class GenerateGroupRDDFunction implements PairFunction<Tuple2<Text, Text>, Void, Group> {
        private volatile transient boolean initialized = false;
        private String cubeName;
        private String segmentId;
        private String metaUrl;
        private SerializableConfiguration conf;
        private List<MeasureDesc> measureDescs;
        private RowKeyDecoder decoder;
        private Map<TblColRef, String> colTypeMap;
        private Map<MeasureDesc, String> meaTypeMap;
        private GroupFactory factory;
        private BufferedMeasureCodec measureCodec;

        public GenerateGroupRDDFunction(String cubeName, String segmentId, String metaurl, SerializableConfiguration conf, Map<TblColRef, String> colTypeMap, Map<MeasureDesc, String> meaTypeMap) {
            this.cubeName = cubeName;
            this.segmentId = segmentId;
            this.metaUrl = metaurl;
            this.conf = conf;
            this.colTypeMap = colTypeMap;
            this.meaTypeMap = meaTypeMap;
        }

        private void init() {
            KylinConfig kConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(conf, metaUrl);
            KylinConfig.setAndUnsetThreadLocalConfig(kConfig);
            CubeInstance cubeInstance = CubeManager.getInstance(kConfig).getCube(cubeName);
            CubeDesc cubeDesc = cubeInstance.getDescriptor();
            CubeSegment cubeSegment = cubeInstance.getSegmentById(segmentId);
            measureDescs = cubeDesc.getMeasures();
            decoder = new RowKeyDecoder(cubeSegment);
            factory = new SimpleGroupFactory(GroupWriteSupport.getSchema(conf.get()));
            measureCodec = new BufferedMeasureCodec(cubeDesc.getMeasures());
        }

        @Override
        public Tuple2<Void, Group> call(Tuple2<Text, Text> tuple) throws Exception {

            logger.debug("call: transfer Text to byte[]");
            if (initialized == false) {
                synchronized (SparkCubeParquet.class) {
                    if (initialized == false) {
                        init();
                        initialized = true;
                    }
                }
            }

            long cuboid = decoder.decode(tuple._1.getBytes());
            List<String> values = decoder.getValues();
            List<TblColRef> columns = decoder.getColumns();

            Group group = factory.newGroup();

            // for check
            group.append("cuboidId", cuboid);

            for (int i = 0; i < columns.size(); i++) {
                TblColRef column = columns.get(i);
                parseColValue(group, column, values.get(i));
            }


            byte[] encodedBytes = tuple._2().copyBytes();
            int[] valueLengths = measureCodec.getCodec().getPeekLength(ByteBuffer.wrap(encodedBytes));

            int valueOffset = 0;
            for (int i = 0; i < valueLengths.length; ++i) {
                MeasureDesc measureDesc = measureDescs.get(i);
                parseMeaValue(group, measureDesc, encodedBytes, valueOffset, valueLengths[i]);
                valueOffset += valueLengths[i];
            }

            return new Tuple2<>(null, group);
        }

        private void parseColValue(final Group group, final TblColRef colRef, final String value) {
            if (value==null) {
                logger.info("value is null");
                return;
            }
            switch (colTypeMap.get(colRef)) {
                case "int":
                    group.append(colRef.getTableAlias() + "_" + colRef.getName(), Integer.valueOf(value));
                    break;
                case "long":
                    group.append(colRef.getTableAlias() + "_" + colRef.getName(), Long.valueOf(value));
                    break;
                default:
                    group.append(colRef.getTableAlias() + "_" + colRef.getName(), Binary.fromString(value));
                    break;
            }
        }

        private void parseMeaValue(final Group group, final MeasureDesc measureDesc, final byte[] value, final int offset, final int length) throws IOException {
            if (value==null) {
                logger.info("value is null");
                return;
            }
            switch (meaTypeMap.get(measureDesc)) {
                case "long":
                    group.append(measureDesc.getName(), BytesUtil.readLong(value, offset, length));
                    break;
                case "double":
                    group.append(measureDesc.getName(), ByteBuffer.wrap(value, offset, length).getDouble());
                    break;
                default:
                    group.append(measureDesc.getName(), Binary.fromConstantByteArray(value, offset, length));
                    break;
            }
        }
    }

    private MessageType cuboidToMessageType(Cuboid cuboid, IDimensionEncodingMap dimEncMap, CubeDesc cubeDesc, Map<TblColRef, String> colTypeMap, Map<MeasureDesc, String> meaTypeMap) {
        Types.MessageTypeBuilder builder = Types.buildMessage();

        List<TblColRef> colRefs = cuboid.getColumns();

        builder.optional(PrimitiveType.PrimitiveTypeName.INT64).named("cuboidId");

        for (TblColRef colRef : colRefs) {
            DimensionEncoding dimEnc = dimEncMap.get(colRef);

            if (dimEnc instanceof AbstractDateDimEnc) {
                builder.optional(PrimitiveType.PrimitiveTypeName.INT64).named(getColName(colRef));
                colTypeMap.put(colRef, "long");
            } else if (dimEnc instanceof FixedLenDimEnc || dimEnc instanceof FixedLenHexDimEnc) {
                org.apache.kylin.metadata.datatype.DataType colDataType = colRef.getType();
                if (colDataType.isNumberFamily() || colDataType.isDateTimeFamily()){
                    builder.optional(PrimitiveType.PrimitiveTypeName.INT64).named(getColName(colRef));
                    colTypeMap.put(colRef, "long");
                } else {
                    // stringFamily && default
                    builder.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named(getColName(colRef));
                    colTypeMap.put(colRef, "string");
                }
            } else {
                builder.optional(PrimitiveType.PrimitiveTypeName.INT32).named(getColName(colRef));
                colTypeMap.put(colRef, "int");
            }
        }

        MeasureIngester[] aggrIngesters = MeasureIngester.create(cubeDesc.getMeasures());

        for (int i = 0; i < cubeDesc.getMeasures().size(); i++) {
            MeasureDesc measureDesc = cubeDesc.getMeasures().get(i);
            org.apache.kylin.metadata.datatype.DataType meaDataType = measureDesc.getFunction().getReturnDataType();
            MeasureType measureType = measureDesc.getFunction().getMeasureType();

            if (measureType instanceof BasicMeasureType) {
                MeasureIngester measureIngester = aggrIngesters[i];
                if (measureIngester instanceof LongIngester) {
                    builder.required(PrimitiveType.PrimitiveTypeName.INT64).named(measureDesc.getName());
                    meaTypeMap.put(measureDesc, "long");
                } else if (measureIngester instanceof DoubleIngester) {
                    builder.required(PrimitiveType.PrimitiveTypeName.DOUBLE).named(measureDesc.getName());
                    meaTypeMap.put(measureDesc, "double");
                } else if (measureIngester instanceof BigDecimalIngester) {
                    builder.required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.DECIMAL).precision(meaDataType.getPrecision()).scale(meaDataType.getScale()).named(measureDesc.getName());
                    meaTypeMap.put(measureDesc, "decimal");
                } else {
                    builder.required(PrimitiveType.PrimitiveTypeName.BINARY).named(measureDesc.getName());
                    meaTypeMap.put(measureDesc, "binary");
                }
            } else {
                builder.required(PrimitiveType.PrimitiveTypeName.BINARY).named(measureDesc.getName());
                meaTypeMap.put(measureDesc, "binary");
            }
        }

        return builder.named(String.valueOf(cuboid.getId()));
    }

    private String getColName(TblColRef colRef) {
        return colRef.getTableAlias() + "_" + colRef.getName();
    }
}

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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.JsonUtil;
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
import org.apache.kylin.engine.mr.common.CubeStatsReader;
import org.apache.kylin.engine.mr.common.SerializableConfiguration;
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
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class SparkCubingByLayerParquet extends SparkCubingByLayer {
    @Override
    protected void saveToHDFS(JavaPairRDD<ByteArray, Object[]> rdd, String metaUrl, String cubeName, CubeSegment cubeSeg, String hdfsBaseLocation, int level, Job job, KylinConfig kylinConfig) throws Exception {
        final IDimensionEncodingMap dimEncMap = cubeSeg.getDimensionEncodingMap();

        Cuboid baseCuboid = Cuboid.getBaseCuboid(cubeSeg.getCubeDesc());

        final Map<TblColRef, String> colTypeMap = Maps.newHashMap();
        final Map<MeasureDesc, String> meaTypeMap = Maps.newHashMap();

        MessageType schema = cuboidToMessageType(baseCuboid, dimEncMap, cubeSeg.getCubeDesc(), colTypeMap, meaTypeMap);

        logger.info("Schema: {}", schema.toString());

        final CuboidToPartitionMapping cuboidToPartitionMapping = new CuboidToPartitionMapping(cubeSeg, kylinConfig, level);

        logger.info("CuboidToPartitionMapping: {}", cuboidToPartitionMapping.toString());

        JavaPairRDD<ByteArray, Object[]> repartitionedRDD = rdd.repartitionAndSortWithinPartitions(new CuboidPartitioner(cuboidToPartitionMapping));

        String output = BatchCubingJobBuilder2.getCuboidOutputPathsByLevel(hdfsBaseLocation, level);

        job.setOutputFormatClass(CustomParquetOutputFormat.class);
        GroupWriteSupport.setSchema(schema, job.getConfiguration());
        CustomParquetOutputFormat.setOutputPath(job, new Path(output));
        CustomParquetOutputFormat.setWriteSupportClass(job, GroupWriteSupport.class);
        CustomParquetOutputFormat.setCuboidToPartitionMapping(job, cuboidToPartitionMapping);

        JavaPairRDD<Void, Group> groupRDD = repartitionedRDD.mapToPair(new GenerateGroupRDDFunction(cubeName, cubeSeg.getUuid(), metaUrl, new SerializableConfiguration(job.getConfiguration()), colTypeMap, meaTypeMap));

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
            ByteArray byteArray = (ByteArray) key;
            long cuboidId = Bytes.toLong(byteArray.array(), RowConstants.ROWKEY_SHARDID_LEN, RowConstants.ROWKEY_CUBOIDID_LEN);

            return mapping.getRandomPartitionForCuboidId(cuboidId);
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

        public int getRandomPartitionForCuboidId(long cuboidId) {
            List<Integer> partitions = cuboidPartitions.get(cuboidId);
            return partitions.get(new Random().nextInt(partitions.size()));
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
            int partition = (int) (cuboidSize / rddCut);
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

    static class GenerateGroupRDDFunction implements PairFunction<Tuple2<ByteArray, Object[]>, Void, Group> {
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
        public Tuple2<Void, Group> call(Tuple2<ByteArray, Object[]> tuple) throws Exception {
            if (initialized == false) {
                synchronized (SparkCubingByLayer.class) {
                    if (initialized == false) {
                        init();
                    }
                }
            }

            long cuboid = decoder.decode(tuple._1.array());
            List<String> values = decoder.getValues();
            List<TblColRef> columns = decoder.getColumns();

            Group group = factory.newGroup();

            // for check
            group.append("cuboidId", cuboid);

            for (int i = 0; i < columns.size(); i++) {
                TblColRef column = columns.get(i);
                parseColValue(group, column, values.get(i));
            }

            ByteBuffer valueBuf = measureCodec.encode(tuple._2());
            byte[] encodedBytes = new byte[valueBuf.position()];
            System.arraycopy(valueBuf.array(), 0, encodedBytes, 0, valueBuf.position());

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

        private void parseMeaValue(final Group group, final MeasureDesc measureDesc, final byte[] value, final int offset, final int length) {
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

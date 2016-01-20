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
import java.io.FileFilter;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import javax.annotation.Nullable;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.hll.HyperLogLogPlusCounter;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.cube.inmemcubing.AbstractInMemCubeBuilder;
import org.apache.kylin.cube.inmemcubing.DoggedCubeBuilder;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeJoinedFlatTableDesc;
import org.apache.kylin.cube.model.DimensionDesc;
import org.apache.kylin.cube.model.HBaseColumnFamilyDesc;
import org.apache.kylin.cube.model.RowKeyDesc;
import org.apache.kylin.cube.util.CubingUtils;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.dict.DictionaryGenerator;
import org.apache.kylin.dict.IterableDictionaryValueEnumerator;
import org.apache.kylin.engine.mr.common.CubeStatsReader;
import org.apache.kylin.engine.spark.cube.BufferedCuboidWriter;
import org.apache.kylin.engine.spark.cube.DefaultTupleConverter;
import org.apache.kylin.engine.spark.util.IteratorUtils;
import org.apache.kylin.metadata.measure.MeasureAggregators;
import org.apache.kylin.metadata.measure.MeasureCodec;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.storage.hbase.HBaseConnection;
import org.apache.kylin.storage.hbase.steps.CreateHTableJob;
import org.apache.kylin.storage.hbase.steps.CubeHTableUtil;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import org.reflections.Reflections;

import scala.Tuple2;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.primitives.UnsignedBytes;

/**
 */
public class SparkCubing extends AbstractApplication {

    private static final Option OPTION_INPUT_PATH = OptionBuilder.withArgName("path").hasArg().isRequired(true).withDescription("Hive Intermediate Table").create("hiveTable");
    private static final Option OPTION_CUBE_NAME = OptionBuilder.withArgName("cube").hasArg().isRequired(true).withDescription("Cube Name").create("cubeName");
    private static final Option OPTION_SEGMENT_ID = OptionBuilder.withArgName("segment").hasArg().isRequired(true).withDescription("Cube Segment Id").create("segmentId");
    private static final Option OPTION_CONF_PATH = OptionBuilder.withArgName("confPath").hasArg().isRequired(true).withDescription("Configuration Path").create("confPath");
    private static final Option OPTION_COPROCESSOR = OptionBuilder.withArgName("coprocessor").hasArg().isRequired(true).withDescription("Coprocessor Jar Path").create("coprocessor");

    private Options options;

    public SparkCubing() {
        options = new Options();
        options.addOption(OPTION_INPUT_PATH);
        options.addOption(OPTION_CUBE_NAME);
        options.addOption(OPTION_SEGMENT_ID);
        options.addOption(OPTION_CONF_PATH);
        options.addOption(OPTION_COPROCESSOR);

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

    private void writeDictionary(DataFrame intermediateTable, String cubeName, String segmentId) throws Exception {
        final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        final CubeManager cubeManager = CubeManager.getInstance(kylinConfig);
        final CubeInstance cubeInstance = cubeManager.reloadCubeLocal(cubeName);
        final String[] columns = intermediateTable.columns();
        final CubeDesc cubeDesc = cubeInstance.getDescriptor();
        final HashMap<Integer, TblColRef> tblColRefMap = Maps.newHashMap();
        final CubeJoinedFlatTableDesc flatTableDesc = new CubeJoinedFlatTableDesc(cubeDesc, null);
        final List<TblColRef> baseCuboidColumn = Cuboid.findById(cubeDesc, Cuboid.getBaseCuboidId(cubeDesc)).getColumns();
        final long start = System.currentTimeMillis();
        final RowKeyDesc rowKey = cubeDesc.getRowkey();
        for (int i = 0; i < baseCuboidColumn.size(); i++) {
            TblColRef col = baseCuboidColumn.get(i);
            if (!rowKey.isUseDictionary(col)) {
                continue;
            }
            final int rowKeyColumnIndex = flatTableDesc.getRowKeyColumnIndexes()[i];
            tblColRefMap.put(rowKeyColumnIndex, col);
        }

        Map<TblColRef, Dictionary<?>> dictionaryMap = Maps.newHashMap();
        for (Map.Entry<Integer, TblColRef> entry : tblColRefMap.entrySet()) {
            final String column = columns[entry.getKey()];
            final TblColRef tblColRef = entry.getValue();
            final DataFrame frame = intermediateTable.select(column).distinct();

            final Row[] rows = frame.collect();
            dictionaryMap.put(tblColRef, DictionaryGenerator.buildDictionaryFromValueEnumerator(tblColRef.getType(), new IterableDictionaryValueEnumerator(new Iterable<byte[]>() {
                @Override
                public Iterator<byte[]> iterator() {
                    return new Iterator<byte[]>() {
                        int i = 0;

                        @Override
                        public boolean hasNext() {
                            return i < rows.length;
                        }

                        @Override
                        public byte[] next() {
                            if (hasNext()) {
                                final Row row = rows[i++];
                                final Object o = row.get(0);
                                return o != null ? o.toString().getBytes() : null;
                            } else {
                                throw new NoSuchElementException();
                            }
                        }

                        @Override
                        public void remove() {
                            throw new UnsupportedOperationException();
                        }
                    };
                }
            })));
        }
        final long end = System.currentTimeMillis();
        CubingUtils.writeDictionary(cubeInstance.getSegmentById(segmentId), dictionaryMap, start, end);
        try {
            CubeUpdate cubeBuilder = new CubeUpdate(cubeInstance);
            cubeBuilder.setToUpdateSegs(cubeInstance.getSegmentById(segmentId));
            cubeManager.updateCube(cubeBuilder);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deal with the request: " + e.getLocalizedMessage());
        }
    }

    private Map<Long, HyperLogLogPlusCounter> sampling(final JavaRDD<List<String>> rowJavaRDD, final String cubeName) throws Exception {
        CubeInstance cubeInstance = CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).reloadCubeLocal(cubeName);
        CubeDesc cubeDesc = cubeInstance.getDescriptor();
        CuboidScheduler cuboidScheduler = new CuboidScheduler(cubeDesc);
        List<Long> allCuboidIds = cuboidScheduler.getAllCuboidIds();
        final HashMap<Long, HyperLogLogPlusCounter> zeroValue = Maps.newHashMap();
        for (Long id : allCuboidIds) {
            zeroValue.put(id, new HyperLogLogPlusCounter(14));
        }

        CubeJoinedFlatTableDesc flatTableDesc = new CubeJoinedFlatTableDesc(cubeDesc, null);

        final int[] rowKeyColumnIndexes = flatTableDesc.getRowKeyColumnIndexes();
        final int nRowKey = cubeDesc.getRowkey().getRowKeyColumns().length;
        final long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        final Map<Long, Integer[]> allCuboidsBitSet = Maps.newHashMapWithExpectedSize(allCuboidIds.size());
        final ByteArray[] row_hashcodes = new ByteArray[nRowKey];

        for (Long cuboidId : allCuboidIds) {
            BitSet bitSet = BitSet.valueOf(new long[]{cuboidId});
            Integer[] cuboidBitSet = new Integer[bitSet.cardinality()];

            long mask = Long.highestOneBit(baseCuboidId);
            int position = 0;
            for (int i = 0; i < nRowKey; i++) {
                if ((mask & cuboidId) > 0) {
                    cuboidBitSet[position] = i;
                    position++;
                }
                mask = mask >> 1;
            }
            allCuboidsBitSet.put(cuboidId, cuboidBitSet);
        }
        for (int i = 0; i < nRowKey; ++i) {
            row_hashcodes[i] = new ByteArray();
        }

        final HashMap<Long, HyperLogLogPlusCounter> samplingResult = rowJavaRDD.aggregate(zeroValue,
                new Function2<HashMap<Long, HyperLogLogPlusCounter>,
                        List<String>,
                        HashMap<Long, HyperLogLogPlusCounter>>() {

                    final HashFunction hashFunction = Hashing.murmur3_128();

                    @Override
                    public HashMap<Long, HyperLogLogPlusCounter> call(HashMap<Long, HyperLogLogPlusCounter> v1, List<String> v2) throws Exception {
                        for (int i = 0; i < nRowKey; i++) {
                            Hasher hc = hashFunction.newHasher();
                            String colValue = v2.get(rowKeyColumnIndexes[i]);
                            if (colValue != null) {
                                row_hashcodes[i].set(hc.putString(colValue).hash().asBytes());
                            } else {
                                row_hashcodes[i].set(hc.putInt(0).hash().asBytes());
                            }
                        }

                        for (Map.Entry<Long, Integer[]> entry : allCuboidsBitSet.entrySet()) {
                            Hasher hc = hashFunction.newHasher();
                            HyperLogLogPlusCounter counter = v1.get(entry.getKey());
                            final Integer[] cuboidBitSet = entry.getValue();
                            for (int position = 0; position < cuboidBitSet.length; position++) {
                                hc.putBytes(row_hashcodes[cuboidBitSet[position]].array());
                            }
                            counter.add(hc.hash().asBytes());
                        }
                        return v1;
                    }
                },
                new Function2<HashMap<Long, HyperLogLogPlusCounter>,
                        HashMap<Long, HyperLogLogPlusCounter>,
                        HashMap<Long, HyperLogLogPlusCounter>>() {
                    @Override
                    public HashMap<Long, HyperLogLogPlusCounter> call(HashMap<Long, HyperLogLogPlusCounter> v1, HashMap<Long, HyperLogLogPlusCounter> v2) throws Exception {
                        Preconditions.checkArgument(v1.size() == v2.size());
                        Preconditions.checkArgument(v1.size() > 0);
                        for (Map.Entry<Long, HyperLogLogPlusCounter> entry : v1.entrySet()) {
                            final HyperLogLogPlusCounter counter1 = entry.getValue();
                            final HyperLogLogPlusCounter counter2 = v2.get(entry.getKey());
                            counter1.merge(Preconditions.checkNotNull(counter2, "counter cannot be null"));
                        }
                        return v1;
                    }

                });
        return samplingResult;
    }

    /*
    return hfile location
     */
    private String build(JavaRDD<List<String>> javaRDD, final String cubeName, final String segmentId, final byte[][] splitKeys) throws Exception {
        CubeInstance cubeInstance = CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).getCube(cubeName);
        CubeDesc cubeDesc = cubeInstance.getDescriptor();
        final CubeSegment cubeSegment = cubeInstance.getSegmentById(segmentId);
        List<TblColRef> baseCuboidColumn = Cuboid.findById(cubeDesc, Cuboid.getBaseCuboidId(cubeDesc)).getColumns();
        final Map<TblColRef, Integer> columnLengthMap = Maps.newHashMap();
        for (TblColRef tblColRef : baseCuboidColumn) {
            columnLengthMap.put(tblColRef, cubeSegment.getColumnLength(tblColRef));
        }
        final Map<TblColRef, Dictionary<?>> dictionaryMap = Maps.newHashMap();
        for (DimensionDesc dim : cubeDesc.getDimensions()) {
            // dictionary
            for (TblColRef col : dim.getColumnRefs()) {
                if (cubeDesc.getRowkey().isUseDictionary(col)) {
                    Dictionary<?> dict = cubeSegment.getDictionary(col);
                    if (dict == null) {
                        System.err.println("Dictionary for " + col + " was not found.");
                    }
                    dictionaryMap.put(col, dict);
                    System.out.println("col:" + col + " dictionary size:" + dict.getSize());
                }
            }
        }

        for (MeasureDesc measureDesc : cubeDesc.getMeasures()) {
            if (measureDesc.getFunction().isTopN()) {
                List<TblColRef> colRefs = measureDesc.getFunction().getParameter().getColRefs();
                TblColRef col = colRefs.get(colRefs.size() - 1);
                dictionaryMap.put(col, cubeSegment.getDictionary(col));
            }
        }


        final JavaPairRDD<byte[], byte[]> javaPairRDD = javaRDD.glom().mapPartitionsToPair(new PairFlatMapFunction<Iterator<List<List<String>>>, byte[], byte[]>() {

            @Override
            public Iterable<Tuple2<byte[], byte[]>> call(Iterator<List<List<String>>> listIterator) throws Exception {
                long t = System.currentTimeMillis();
                prepare();

                final CubeInstance cubeInstance = CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).getCube(cubeName);

                LinkedBlockingQueue<List<String>> blockingQueue = new LinkedBlockingQueue();
                System.out.println("load properties finished");
                AbstractInMemCubeBuilder inMemCubeBuilder = new DoggedCubeBuilder(cubeInstance.getDescriptor(), dictionaryMap);
                final SparkCuboidWriter sparkCuboidWriter = new BufferedCuboidWriter(new DefaultTupleConverter(cubeInstance.getSegmentById(segmentId), columnLengthMap));
                Executors.newCachedThreadPool().submit(inMemCubeBuilder.buildAsRunnable(blockingQueue, sparkCuboidWriter));
                try {
                    while (listIterator.hasNext()) {
                        for (List<String> row : listIterator.next()) {
                            blockingQueue.put(row);
                        }
                    }
                    blockingQueue.put(Collections.<String>emptyList());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                System.out.println("build partition cost: " + (System.currentTimeMillis() - t) + "ms");
                return sparkCuboidWriter.getResult();
            }
        });

        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        Configuration conf = getConfigurationForHFile(cubeSegment.getStorageLocationIdentifier());
        Path path = new Path(kylinConfig.getHdfsWorkingDirectory(), "hfile_" + UUID.randomUUID().toString());
        Preconditions.checkArgument(!FileSystem.get(conf).exists(path));
        String url = conf.get("fs.defaultFS") + path.toString();
        System.out.println("use " + url + " as hfile");
        List<MeasureDesc> measuresDescs = cubeDesc.getMeasures();
        final int measureSize = measuresDescs.size();
        final String[] dataTypes = new String[measureSize];
        for (int i = 0; i < dataTypes.length; i++) {
            dataTypes[i] = measuresDescs.get(i).getFunction().getReturnType();
        }
        final MeasureAggregators aggs = new MeasureAggregators(measuresDescs);
        writeToHFile2(javaPairRDD, dataTypes, measureSize, aggs, splitKeys, conf, url);
        return url;
    }

    private void writeToHFile2(final JavaPairRDD<byte[], byte[]> javaPairRDD, final String[] dataTypes, final int measureSize, final MeasureAggregators aggs, final byte[][] splitKeys, final Configuration conf, final String hFileLocation) {
        javaPairRDD.repartitionAndSortWithinPartitions(new Partitioner() {
            @Override
            public int numPartitions() {
                return splitKeys.length + 1;
            }

            @Override
            public int getPartition(Object key) {
                Preconditions.checkArgument(key instanceof byte[]);
                for (int i = 0, n = splitKeys.length; i < n; ++i) {
                    if (UnsignedBytes.lexicographicalComparator().compare((byte[]) key, splitKeys[i]) < 0) {
                        return i;
                    }
                }
                return splitKeys.length;
            }
        }, UnsignedBytes.lexicographicalComparator()).mapPartitions(new FlatMapFunction<Iterator<Tuple2<byte[], byte[]>>, Tuple2<byte[], byte[]>>() {
            @Override
            public Iterable<Tuple2<byte[], byte[]>> call(final Iterator<Tuple2<byte[], byte[]>> tuple2Iterator) throws Exception {
                return new Iterable<Tuple2<byte[], byte[]>>() {
                    final ByteBuffer buffer = ByteBuffer.allocate(RowConstants.ROWVALUE_BUFFER_SIZE);
                    final MeasureCodec codec = new MeasureCodec(dataTypes);
                    final Object[] input = new Object[measureSize];
                    final Object[] result = new Object[measureSize];

                    @Override
                    public Iterator<Tuple2<byte[], byte[]>> iterator() {
                        return IteratorUtils.merge(tuple2Iterator, UnsignedBytes.lexicographicalComparator(), new Function<Iterable<byte[]>, byte[]>() {
                            @Override
                            public byte[] call(Iterable<byte[]> v1) throws Exception {
                                final LinkedList<byte[]> list = Lists.newLinkedList(v1);
                                if (list.size() == 1) {
                                    return list.get(0);
                                }
                                aggs.reset();
                                for (byte[] v : list) {
                                    codec.decode(ByteBuffer.wrap(v), input);
                                    aggs.aggregate(input);
                                }
                                aggs.collectStates(result);
                                buffer.clear();
                                codec.encode(result, buffer);
                                byte[] bytes = new byte[buffer.position()];
                                System.arraycopy(buffer.array(), buffer.arrayOffset(), bytes, 0, buffer.position());
                                return bytes;
                            }
                        });
                    }
                };
            }
        }, true).mapToPair(new PairFunction<Tuple2<byte[], byte[]>, ImmutableBytesWritable, KeyValue>() {
            @Override
            public Tuple2<ImmutableBytesWritable, KeyValue> call(Tuple2<byte[], byte[]> tuple2) throws Exception {
                ImmutableBytesWritable key = new ImmutableBytesWritable(tuple2._1());
                KeyValue value = new KeyValue(tuple2._1(), "F1".getBytes(), "M".getBytes(), tuple2._2());
                return new Tuple2(key, value);
            }
        }).saveAsNewAPIHadoopFile(hFileLocation, ImmutableBytesWritable.class, KeyValue.class, HFileOutputFormat.class, conf);
    }

    private static void prepare() throws Exception {
        final File file = new File(SparkFiles.get("kylin.properties"));
        final String confPath = file.getParentFile().getAbsolutePath();
        System.out.println("conf directory:" + confPath);
        System.setProperty(KylinConfig.KYLIN_CONF, confPath);
        ClassUtil.addClasspath(confPath);
    }

    private byte[][] createHTable(String cubeName, String segmentId, Map<Long, HyperLogLogPlusCounter> samplingResult) throws Exception {
        final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        final CubeInstance cubeInstance = CubeManager.getInstance(kylinConfig).getCube(cubeName);
        final CubeSegment cubeSegment = cubeInstance.getSegmentById(segmentId);
        final Map<Long, Long> rowCountMap = CubeStatsReader.getCuboidRowCountMapFromSampling(samplingResult, 100);
        final Map<Long, Double> cubeSizeMap = CubeStatsReader.getCuboidSizeMapFromRowCount(cubeSegment, rowCountMap);
        System.out.println("cube size estimation:" + cubeSizeMap);
        final byte[][] splitKeys = CreateHTableJob.getSplitsFromCuboidStatistics(cubeSizeMap, kylinConfig, cubeSegment);
        CubeHTableUtil.createHTable(cubeSegment, splitKeys);
        System.out.println(cubeSegment.getStorageLocationIdentifier() + " table created");
        return splitKeys;
    }

    private Configuration getConfigurationForHFile(String hTableName) throws IOException {
        final Configuration conf = HBaseConnection.getCurrentHBaseConfiguration();
        Job job = Job.getInstance(conf);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);
        HTable table = new HTable(conf, hTableName);
        HFileOutputFormat.configureIncrementalLoad(job, table);
        return conf;
    }

    private void bulkLoadHFile(String cubeName, String segmentId, String hfileLocation) throws Exception {
        final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        final CubeInstance cubeInstance = CubeManager.getInstance(kylinConfig).getCube(cubeName);
        final CubeDesc cubeDesc = cubeInstance.getDescriptor();
        final CubeSegment cubeSegment = cubeInstance.getSegmentById(segmentId);
        final Configuration hbaseConf = HBaseConnection.getCurrentHBaseConfiguration();
        FileSystem fs = FileSystem.get(hbaseConf);
        FsPermission permission = new FsPermission((short) 0777);
        for (HBaseColumnFamilyDesc cf : cubeDesc.getHBaseMapping().getColumnFamily()) {
            String cfName = cf.getName();
            Path columnFamilyPath = new Path(hfileLocation, cfName);

            // File may have already been auto-loaded (in the case of MapR DB)
            if (fs.exists(columnFamilyPath)) {
                fs.setPermission(columnFamilyPath, permission);
            }
        }

        String[] newArgs = new String[2];
        newArgs[0] = hfileLocation;
        newArgs[1] = cubeSegment.getStorageLocationIdentifier();

        int ret = ToolRunner.run(new LoadIncrementalHFiles(hbaseConf), newArgs);
        System.out.println("incremental load result:" + ret);

        cubeSegment.setStatus(SegmentStatusEnum.READY);
        try {
            CubeUpdate cubeBuilder = new CubeUpdate(cubeInstance);
            cubeInstance.setStatus(RealizationStatusEnum.READY);
            cubeSegment.setStatus(SegmentStatusEnum.READY);
            cubeBuilder.setToUpdateSegs(cubeSegment);
            CubeManager.getInstance(kylinConfig).updateCube(cubeBuilder);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deal with the request: " + e.getLocalizedMessage());
        }
    }

    private Collection<String> getKyroClasses() {
        Set<Class> kyroClasses = Sets.newHashSet();
        kyroClasses.addAll(new Reflections("org.apache.kylin").getSubTypesOf(Serializable.class));
        kyroClasses.addAll(new Reflections("org.apache.kylin.cube.model").getSubTypesOf(Object.class));
        kyroClasses.addAll(new Reflections("org.apache.kylin.metadata.model").getSubTypesOf(Object.class));
        kyroClasses.addAll(new Reflections("org.apache.kylin.metadata.measure").getSubTypesOf(Object.class));
        kyroClasses.add(HashMap.class);
        kyroClasses.add(org.apache.spark.sql.Row[].class);
        kyroClasses.add(org.apache.spark.sql.Row.class);
        kyroClasses.add(org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema.class);
        kyroClasses.add(org.apache.spark.sql.types.StructType.class);
        kyroClasses.add(org.apache.spark.sql.types.StructField[].class);
        kyroClasses.add(org.apache.spark.sql.types.StructField.class);
        kyroClasses.add(org.apache.spark.sql.types.DateType$.class);
        kyroClasses.add(org.apache.spark.sql.types.Metadata.class);
        kyroClasses.add(Object[].class);
        kyroClasses.add(org.apache.spark.sql.types.StringType$.class);
        kyroClasses.add(Hashing.murmur3_128().getClass());
        kyroClasses.add(org.apache.spark.sql.columnar.CachedBatch.class);
        kyroClasses.add(byte[][].class);
        kyroClasses.add(org.apache.spark.sql.types.Decimal.class);
        kyroClasses.add(scala.math.BigDecimal.class);
        kyroClasses.add(java.math.BigDecimal.class);
        kyroClasses.add(java.math.MathContext.class);
        kyroClasses.add(java.math.RoundingMode.class);
        kyroClasses.add(java.util.ArrayList.class);
        kyroClasses.add(java.util.LinkedList.class);
        
        
        ArrayList<String> result = Lists.newArrayList();
        for (Class kyroClass : kyroClasses) {
            result.add(kyroClass.getName());
        }
        result.add("scala.collection.immutable.Map$EmptyMap$");
        result.add("org.apache.spark.sql.catalyst.expressions.GenericInternalRow");
        result.add("org.apache.spark.unsafe.types.UTF8String");
        return result;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        final String hiveTable = optionsHelper.getOptionValue(OPTION_INPUT_PATH);
        SparkConf conf = new SparkConf().setAppName("Simple Application");
        //memory conf
        conf.set("spark.executor.memory", "6g");
        conf.set("spark.storage.memoryFraction", "0.3");

        //serialization conf
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrationRequired", "true");
        final Iterable<String> allClasses = Iterables.filter(
                Iterables.concat(Lists.newArrayList(conf.get("spark.kryo.classesToRegister", "").split(",")), getKyroClasses()),
                new Predicate<String>() {
                    @Override
                    public boolean apply(@Nullable String input) {
                        return input != null && input.trim().length() > 0;
                    }
                });
        System.out.println("kyro classes:" + allClasses.toString());
        conf.set("spark.kryo.classesToRegister", StringUtils.join(allClasses, ","));

        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext sqlContext = new HiveContext(sc.sc());
        final DataFrame intermediateTable = sqlContext.sql("select * from " + hiveTable);
        final String cubeName = optionsHelper.getOptionValue(OPTION_CUBE_NAME);
        final String segmentId = optionsHelper.getOptionValue(OPTION_SEGMENT_ID);
        final String confPath = optionsHelper.getOptionValue(OPTION_CONF_PATH);
        final String coprocessor = optionsHelper.getOptionValue(OPTION_COPROCESSOR);
        final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        kylinConfig.overrideCoprocessorLocalJar(coprocessor);

        setupClasspath(sc, confPath);
        intermediateTable.cache();
        writeDictionary(intermediateTable, cubeName, segmentId);
        final JavaRDD<List<String>> rowJavaRDD = intermediateTable.javaRDD().map(new org.apache.spark.api.java.function.Function<Row, List<String>>() {
            @Override
            public List<String> call(Row v1) throws Exception {
                ArrayList<String> result = Lists.newArrayListWithExpectedSize(v1.size());
                for (int i = 0; i < v1.size(); i++) {
                    final Object o = v1.get(i);
                    if (o != null) {
                        result.add(o.toString());
                    } else {
                        result.add(null);
                    }
                }
                return result;

            }
        });
        final Map<Long, HyperLogLogPlusCounter> samplingResult = sampling(rowJavaRDD, cubeName);
        final byte[][] splitKeys = createHTable(cubeName, segmentId, samplingResult);

        final String hfile = build(rowJavaRDD, cubeName, segmentId, splitKeys);
        bulkLoadHFile(cubeName, segmentId, hfile);
    }

}

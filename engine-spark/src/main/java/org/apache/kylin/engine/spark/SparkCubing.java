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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.hll.HyperLogLogPlusCounter;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.cube.inmemcubing.ICuboidWriter;
import org.apache.kylin.cube.inmemcubing.InMemCubeBuilder;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.model.*;
import org.apache.kylin.cube.util.CubingUtils;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.dict.DictionaryGenerator;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.job.common.OptionsHelper;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.hbase.HBaseConnection;
import org.apache.kylin.storage.hbase.steps.CreateHTableJob;
import org.apache.kylin.storage.hbase.steps.CubeHTableUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

/**
 */
public class SparkCubing extends AbstractSparkApplication {

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
        long start = System.currentTimeMillis();
        final CubeDesc cubeDesc = cubeInstance.getDescriptor();
        final HashMap<Integer, TblColRef> tblColRefMap = Maps.newHashMap();
        final CubeJoinedFlatTableDesc flatTableDesc = new CubeJoinedFlatTableDesc(cubeDesc, null);
        final List<TblColRef> baseCuboidColumn = Cuboid.findById(cubeDesc, Cuboid.getBaseCuboidId(cubeDesc)).getColumns();
        RowKeyDesc rowKey = cubeDesc.getRowkey();
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
            dictionaryMap.put(tblColRef, DictionaryGenerator.buildDictionaryFromValueList(tblColRef.getType(), new Iterable<byte[]>() {
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
            }));
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
        final CubeInstance cubeInstance = CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).reloadCubeLocal(cubeName);
        HashMap<Long, HyperLogLogPlusCounter> zeroValue = Maps.newHashMap();
        for (Long id : new CuboidScheduler(cubeInstance.getDescriptor()).getAllCuboidIds()) {
            zeroValue.put(id, new HyperLogLogPlusCounter(14));
        }

        final HashMap<Long, HyperLogLogPlusCounter> samplingResult = rowJavaRDD.aggregate(zeroValue,
                new Function2<HashMap<Long, HyperLogLogPlusCounter>,
                        List<String>,
                        HashMap<Long, HyperLogLogPlusCounter>>() {

                    @Override
                    public HashMap<Long, HyperLogLogPlusCounter> call(HashMap<Long, HyperLogLogPlusCounter> v1, List<String> v2) throws Exception {
                        prepare();
                        final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
                        final CubeManager cubeManager = CubeManager.getInstance(kylinConfig);
                        final CubeInstance cubeInstance = cubeManager.reloadCubeLocal(cubeName);
                        final CubeDesc cubeDesc = cubeInstance.getDescriptor();
                        final CubeJoinedFlatTableDesc flatTableDesc = new CubeJoinedFlatTableDesc(cubeDesc, null);
                        final int nRowKey = cubeDesc.getRowkey().getRowKeyColumns().length;
                        ByteArray[] row_hashcodes = new ByteArray[nRowKey];
                        for (int i = 0; i < nRowKey; ++i) {
                            row_hashcodes[i] = new ByteArray();
                        }
                        for (int i = 0; i < nRowKey; i++) {
                            Hasher hc = Hashing.murmur3_32().newHasher();
                            String colValue = v2.get(flatTableDesc.getRowKeyColumnIndexes()[i]);
                            if (colValue != null) {
                                row_hashcodes[i].set(hc.putString(colValue).hash().asBytes());
                            } else {
                                row_hashcodes[i].set(hc.putInt(0).hash().asBytes());
                            }
                        }
                        final CuboidScheduler cuboidScheduler = new CuboidScheduler(cubeDesc);
                        final long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
                        final List<Long> allCuboidIds = cuboidScheduler.getAllCuboidIds();
                        final Map<Long, Integer[]> allCuboidsBitSet = Maps.newHashMapWithExpectedSize(allCuboidIds.size());
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

                        HashMap<Long, HyperLogLogPlusCounter> result = Maps.newHashMapWithExpectedSize(allCuboidIds.size());
                        for (Long cuboidId : allCuboidIds) {
                            Hasher hc = Hashing.murmur3_32().newHasher();
                            HyperLogLogPlusCounter counter = v1.get(cuboidId);
                            final Integer[] cuboidBitSet = allCuboidsBitSet.get(cuboidId);
                            for (int position = 0; position < cuboidBitSet.length; position++) {
                                hc.putBytes(row_hashcodes[cuboidBitSet[position]].array());
                            }
                            counter.add(hc.hash().asBytes());
                            result.put(cuboidId, counter);
                        }
                        return result;
                    }
                },
                new Function2<HashMap<Long, HyperLogLogPlusCounter>,
                        HashMap<Long, HyperLogLogPlusCounter>,
                        HashMap<Long, HyperLogLogPlusCounter>>() {
                    @Override
                    public HashMap<Long, HyperLogLogPlusCounter> call(HashMap<Long, HyperLogLogPlusCounter> v1, HashMap<Long, HyperLogLogPlusCounter> v2) throws Exception {
                        Preconditions.checkArgument(v1.size() == v2.size());
                        Preconditions.checkArgument(v1.size() > 0);
                        final HashMap<Long, HyperLogLogPlusCounter> result = Maps.newHashMapWithExpectedSize(v1.size());
                        for (Map.Entry<Long, HyperLogLogPlusCounter> entry : v1.entrySet()) {
                            final HyperLogLogPlusCounter counter1 = entry.getValue();
                            final HyperLogLogPlusCounter counter2 = v2.get(entry.getKey());
                            if (counter2 != null) {
                                counter1.merge(counter2);
                            }
                            result.put(entry.getKey(), counter1);
                        }
                        return result;
                    }

                    private HashMap<Long, HyperLogLogPlusCounter> copy(HashMap<Long, HyperLogLogPlusCounter> v1, HashMap<Long, HyperLogLogPlusCounter> v2) {
                        final HashMap<Long, HyperLogLogPlusCounter> result = Maps.newHashMapWithExpectedSize(v1.size());
                        for (Map.Entry<Long, HyperLogLogPlusCounter> entry : v1.entrySet()) {
                            final HyperLogLogPlusCounter counter1 = entry.getValue();
                            final HyperLogLogPlusCounter counter2 = v2.get(entry.getKey());
                            if (counter2 != null) {
                                counter1.merge(counter2);
                            }
                            result.put(entry.getKey(), counter1);
                        }
                        return result;
                    }

                });
        return samplingResult;
    }

    /*
    return hfile location
     */
    private String build(JavaRDD<List<List<String>>> javaRDD, final String cubeName, final String segmentId) throws Exception {


        final JavaPairRDD<byte[], byte[]> javaPairRDD = javaRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<List<List<String>>>, byte[], byte[]>() {

            @Override
            public Iterable<Tuple2<byte[], byte[]>> call(Iterator<List<List<String>>> listIterator) throws Exception {
                prepare();

                final CubeInstance cubeInstance = CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).getCube(cubeName);
                final CubeDesc cubeDesc = cubeInstance.getDescriptor();
                final CubeSegment cubeSegment = cubeInstance.getSegmentById(segmentId);

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
                        }
                    }
                }
                final Iterator<Iterator<Tuple2<byte[], byte[]>>> iterator = Iterators.transform(listIterator, new Function<List<List<String>>, Iterator<Tuple2<byte[], byte[]>>>() {
                    @Nullable
                    @Override
                    public Iterator<Tuple2<byte[], byte[]>> apply(@Nullable List<List<String>> input) {
                        if (input.isEmpty()) {
                            return Collections.emptyIterator();
                        }
                        LinkedBlockingQueue<List<String>> blockingQueue = new LinkedBlockingQueue<List<String>>();
                        final List<Tuple2<byte[], byte[]>> result = Lists.newLinkedList();
                        InMemCubeBuilder inMemCubeBuilder = new InMemCubeBuilder(cubeInstance.getDescriptor(), dictionaryMap);
                        final Future<?> future = Executors.newCachedThreadPool().submit(inMemCubeBuilder.buildAsRunnable(blockingQueue, new ICuboidWriter() {

                            final int measureCount = cubeDesc.getMeasures().size();
                            int[] measureColumnsIndex = new int[measureCount];
                            ByteBuffer valueBuf = ByteBuffer.allocate(RowConstants.ROWVALUE_BUFFER_SIZE);

                            @Override
                            public void write(long cuboidId, GTRecord record) throws IOException {
                                int bytesLength = RowConstants.ROWKEY_CUBOIDID_LEN;
                                Cuboid cuboid = Cuboid.findById(cubeDesc, cuboidId);
                                for (TblColRef column : cuboid.getColumns()) {
                                    bytesLength += cubeSegment.getColumnLength(column);
                                    final Dictionary<?> dictionary = cubeSegment.getDictionary(column);
                                }

                                final int dimensions = BitSet.valueOf(new long[]{cuboidId}).cardinality();
                                for (int i = 0; i < measureCount; i++) {
                                    measureColumnsIndex[i] = dimensions + i;
                                }

                                byte[] key = new byte[bytesLength];
                                System.arraycopy(Bytes.toBytes(cuboidId), 0, key, 0, RowConstants.ROWKEY_CUBOIDID_LEN);
                                int offSet = RowConstants.ROWKEY_CUBOIDID_LEN;
                                for (int x = 0; x < dimensions; x++) {
                                    final ByteArray byteArray = record.get(x);
                                    System.arraycopy(byteArray.array(), byteArray.offset(), key, offSet, byteArray.length());
                                    offSet += byteArray.length();
                                }


                                //output measures
                                valueBuf.clear();
                                record.exportColumns(measureColumnsIndex, valueBuf);

                                byte[] value = new byte[valueBuf.position()];
                                System.arraycopy(valueBuf.array(), 0, value, 0, valueBuf.position());
                                result.add(new Tuple2<byte[], byte[]>(key, value));
                            }

                            @Override
                            public void flush() {

                            }
                        }));
                        try {
                            for (List<String> row : input) {
                                blockingQueue.put(row);
                            }
                            blockingQueue.put(Collections.<String>emptyList());
                            future.get();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                        return result.iterator();
                    }
                });
                return new Iterable<Tuple2<byte[], byte[]>>() {
                    @Override
                    public Iterator<Tuple2<byte[], byte[]>> iterator() {
                        return Iterators.concat(iterator);
                    }
                };
            }
        });
        final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        final Configuration conf = HBaseConnection.newHBaseConfiguration(KylinConfig.getInstanceFromEnv().getStorageUrl());
        Path path = new Path(kylinConfig.getHdfsWorkingDirectory(), "hfile_" + UUID.randomUUID().toString());
        Preconditions.checkArgument(!FileSystem.get(conf).exists(path));
        String url = conf.get("fs.defaultFS") + path.toString();
        System.out.println("use " + url + " as hfile");
        javaPairRDD.mapToPair(new PairFunction<Tuple2<byte[], byte[]>, ImmutableBytesWritable, KeyValue>() {
            @Override
            public Tuple2<ImmutableBytesWritable, KeyValue> call(Tuple2<byte[], byte[]> tuple2) throws Exception {
                ImmutableBytesWritable key = new ImmutableBytesWritable(tuple2._1());
                KeyValue value = new KeyValue(tuple2._1(), "F1".getBytes(), "M".getBytes(), tuple2._2());
                return new Tuple2(key, value);
            }
        }).saveAsNewAPIHadoopFile(url, ImmutableBytesWritable.class, KeyValue.class, HFileOutputFormat.class, conf);
        return url;
    }

    private static void prepare() throws Exception {
        final File file = new File(SparkFiles.get("kylin.properties"));
        final String confPath = file.getParentFile().getAbsolutePath();
        System.out.println("conf directory:" + confPath);
        System.setProperty(KylinConfig.KYLIN_CONF, confPath);
        ClassUtil.addClasspath(confPath);
    }
    
    private void createHTable(String cubeName, String segmentId, Map<Long, HyperLogLogPlusCounter> samplingResult) throws Exception {
        final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        final CubeInstance cubeInstance = CubeManager.getInstance(kylinConfig).getCube(cubeName);
        final CubeDesc cubeDesc = cubeInstance.getDescriptor();
        final CubeSegment cubeSegment = cubeInstance.getSegmentById(segmentId);
        final Map<Long, Long> cubeSizeMap = CreateHTableJob.getCubeRowCountMapFromCuboidStatistics(samplingResult, 100);
        final byte[][] splitKeys = CreateHTableJob.getSplitsFromCuboidStatistics(cubeSizeMap, kylinConfig, cubeSegment);
        CubeHTableUtil.createHTable(cubeDesc, cubeSegment.getStorageLocationIdentifier(), splitKeys);
        System.out.println(cubeSegment.getStorageLocationIdentifier() + " table created");
    }
    
    private void bulkLoadHFile(String cubeName, String segmentId, String hfileLocation) throws Exception {
        final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        final CubeInstance cubeInstance = CubeManager.getInstance(kylinConfig).getCube(cubeName);
        final CubeDesc cubeDesc = cubeInstance.getDescriptor();
        final CubeSegment cubeSegment = cubeInstance.getSegmentById(segmentId);
        final Configuration hbaseConf = HBaseConnection.newHBaseConfiguration(kylinConfig.getStorageUrl());
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
            cubeBuilder.setToUpdateSegs(cubeInstance.getSegmentById(segmentId));
            CubeManager.getInstance(kylinConfig).updateCube(cubeBuilder);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deal with the request: " + e.getLocalizedMessage());
        }
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        final String hiveTable = optionsHelper.getOptionValue(OPTION_INPUT_PATH);
        SparkConf conf = new SparkConf().setAppName("Simple Application");
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
        createHTable(cubeName, segmentId, samplingResult);
        
        final String hfile = build(rowJavaRDD.glom(), cubeName, segmentId);
        bulkLoadHFile(cubeName, segmentId, hfile);
    }

}

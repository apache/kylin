package org.apache.kylin.job.streaming;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.hll.HyperLogLogPlusCounter;
import org.apache.kylin.common.persistence.HBaseConnection;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeBuilder;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeJoinedFlatTableDesc;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.cube.model.HBaseColumnFamilyDesc;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.dict.DictionaryGenerator;
import org.apache.kylin.dict.DictionaryInfo;
import org.apache.kylin.dict.DictionaryManager;
import org.apache.kylin.dict.lookup.ReadableTable;
import org.apache.kylin.dict.lookup.TableSignature;
import org.apache.kylin.job.constant.BatchConstants;
import org.apache.kylin.job.hadoop.cube.FactDistinctColumnsReducer;
import org.apache.kylin.job.hadoop.cubev2.InMemKeyValueCreator;
import org.apache.kylin.job.hadoop.hbase.CubeHTableUtil;
import org.apache.kylin.job.inmemcubing.ICuboidWriter;
import org.apache.kylin.job.inmemcubing.InMemCubeBuilder;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.cube.CuboidToGridTableMapping;
import org.apache.kylin.storage.gridtable.GTRecord;
import org.apache.kylin.streaming.MicroStreamBatch;
import org.apache.kylin.streaming.StreamBuilder;
import org.apache.kylin.streaming.StreamMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 */
public class CubeStreamBuilder extends StreamBuilder {

    private static final Logger logger = LoggerFactory.getLogger(CubeStreamBuilder.class);

    private final CubeManager cubeManager;
    private final String cubeName;
    private final KylinConfig kylinConfig;
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();


    public CubeStreamBuilder(BlockingQueue<StreamMessage> streamMessageQueue, String cubeName) {
        super(streamMessageQueue);
        this.kylinConfig = KylinConfig.getInstanceFromEnv();
        this.cubeManager = CubeManager.getInstance(kylinConfig);
        this.cubeName = cubeName;
    }

    @Override
    protected void build(MicroStreamBatch microStreamBatch) throws Exception {
        if (microStreamBatch.size() == 0) {
            logger.info("nothing to build, skip to next iteration");
            return;
        }
        final List<List<String>> parsedStreamMessages = microStreamBatch.getStreams();
        long startOffset = microStreamBatch.getOffset().getFirst();
        long endOffset = microStreamBatch.getOffset().getSecond();
        LinkedBlockingQueue<List<String>> blockingQueue = new LinkedBlockingQueue<List<String>>(parsedStreamMessages);
        blockingQueue.put(Collections.<String>emptyList());

        final CubeInstance cubeInstance = cubeManager.reloadCubeLocal(cubeName);
        final CubeDesc cubeDesc = cubeInstance.getDescriptor();
        final CubeSegment cubeSegment = cubeManager.appendSegments(cubeManager.getCube(cubeName), System.currentTimeMillis(), false, false);
        final Map<Long, HyperLogLogPlusCounter> samplingResult = sampling(cubeInstance.getDescriptor(), parsedStreamMessages);

        final Configuration conf = HadoopUtil.getCurrentConfiguration();
        final Path outputPath = new Path("file:///tmp/cuboidstatistics/" + UUID.randomUUID().toString());
        FactDistinctColumnsReducer.writeCuboidStatistics(conf, outputPath, samplingResult, 100);
        ResourceStore.getStore(kylinConfig).putResource(cubeSegment.getStatisticsResourcePath(), FileSystem.getLocal(conf).open(new Path(outputPath, BatchConstants.CFG_STATISTICS_CUBOID_ESTIMATION)), 0);

        final Map<TblColRef, Dictionary<?>> dictionaryMap = buildDictionary(getTblColRefMap(cubeInstance), parsedStreamMessages);
        writeDictionary(cubeSegment, dictionaryMap, startOffset, endOffset);

        final HTableInterface hTable = createHTable(cubeSegment);

        final CubeStreamRecordWriter gtRecordWriter = new CubeStreamRecordWriter(cubeDesc, hTable);
        InMemCubeBuilder inMemCubeBuilder = new InMemCubeBuilder(blockingQueue, cubeInstance.getDescriptor(),
                dictionaryMap, gtRecordWriter);

        executorService.submit(inMemCubeBuilder).get();
        gtRecordWriter.flush();
        commitSegment(cubeSegment);
    }

    private void writeDictionary(CubeSegment cubeSegment, Map<TblColRef, Dictionary<?>> dictionaryMap, long startOffset, long endOffset) {
        for (Map.Entry<TblColRef, Dictionary<?>> entry : dictionaryMap.entrySet()) {
            final TblColRef tblColRef = entry.getKey();
            final Dictionary<?> dictionary = entry.getValue();
            TableSignature signature = new TableSignature();
            signature.setLastModifiedTime(System.currentTimeMillis());
            signature.setPath(String.format("streaming_%s_%s", startOffset, endOffset));
            signature.setSize(endOffset - startOffset);
            DictionaryInfo dictInfo = new DictionaryInfo(tblColRef.getTable(),
                    tblColRef.getName(),
                    tblColRef.getColumnDesc().getZeroBasedIndex(),
                    tblColRef.getDatatype(),
                    signature,
                    ReadableTable.DELIM_AUTO);
            logger.info("writing dictionary for TblColRef:" + tblColRef.toString());
            DictionaryManager dictionaryManager = DictionaryManager.getInstance(kylinConfig);
            try {
                cubeSegment.putDictResPath(tblColRef, dictionaryManager.trySaveNewDict(dictionary, dictInfo).getResourcePath());
            } catch (IOException e) {
                logger.error("error save dictionary for column:" + tblColRef, e);
                throw new RuntimeException("error save dictionary for column:" + tblColRef, e);
            }
        }
    }

    private class CubeStreamRecordWriter implements ICuboidWriter {
        final List<InMemKeyValueCreator> keyValueCreators;
        final int nColumns;
        final HTableInterface hTable;
        private final ByteBuffer byteBuffer;
        private final CubeDesc cubeDesc;
        private List<Put> puts = Lists.newArrayList();

        private CubeStreamRecordWriter(CubeDesc cubeDesc, HTableInterface hTable) {
            this.keyValueCreators = Lists.newArrayList();
            this.cubeDesc = cubeDesc;
            int startPosition = 0;
            for (HBaseColumnFamilyDesc cfDesc : cubeDesc.getHBaseMapping().getColumnFamily()) {
                for (HBaseColumnDesc colDesc : cfDesc.getColumns()) {
                    keyValueCreators.add(new InMemKeyValueCreator(colDesc, startPosition));
                    startPosition += colDesc.getMeasures().length;
                }
            }
            this.nColumns = keyValueCreators.size();
            this.hTable = hTable;
            this.byteBuffer = ByteBuffer.allocate(1<<20);
        }

        private byte[] copy(byte[] array, int offset, int length) {
            byte[] result = new byte[length];
            System.arraycopy(array, offset, result, 0, length);
            return result;
        }

        private ByteBuffer createKey(Long cuboidId, GTRecord record) {
            byteBuffer.clear();
            byteBuffer.put(Bytes.toBytes(cuboidId));
            final int cardinality = BitSet.valueOf(new long[]{cuboidId}).cardinality();
            for (int i = 0; i < cardinality; i++) {
                final ByteArray byteArray = record.get(i);
                byteBuffer.put(byteArray.array(), byteArray.offset(), byteArray.length());
            }
            return byteBuffer;
        }

        @Override
        public void write(long cuboidId, GTRecord record) throws IOException {
            final ByteBuffer key = createKey(cuboidId, record);
            final CuboidToGridTableMapping mapping = new CuboidToGridTableMapping(Cuboid.findById(cubeDesc, cuboidId));
            final ImmutableBitSet bitSet = new ImmutableBitSet(mapping.getDimensionCount(), mapping.getColumnCount());
            for (int i = 0; i < nColumns; i++) {
                final KeyValue keyValue = keyValueCreators.get(i).create(key.array(), 0, key.position(), record.getValues(bitSet, new Object[bitSet.cardinality()]));
                final Put put = new Put(copy(key.array(), 0, key.position()));
                byte[] family = copy(keyValue.getFamilyArray(), keyValue.getFamilyOffset(), keyValue.getFamilyLength());
                byte[] qualifier = copy(keyValue.getQualifierArray(), keyValue.getQualifierOffset(), keyValue.getQualifierLength());
                byte[] value = copy(keyValue.getValueArray(), keyValue.getValueOffset(), keyValue.getValueLength());
                put.add(family, qualifier, value);
                puts.add(put);
            }
            if (puts.size() >= batchSize()) {
                flush();
            }
        }

        public final void flush() {
            try {
                if (!puts.isEmpty()) {
                    long t = System.currentTimeMillis();
                    if (hTable != null) {
                        hTable.put(puts);
                        hTable.flushCommits();
                    }
                    logger.info("commit total " + puts.size() + " puts, totally cost:" + (System.currentTimeMillis() - t) + "ms");
                    puts.clear();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private Map<Integer, TblColRef> getTblColRefMap(CubeInstance cubeInstance) {
        final List<TblColRef> columns = cubeInstance.getAllColumns();
        final List<TblColRef> allDimensions = cubeInstance.getAllDimensions();
        final HashMap<Integer, TblColRef> result = Maps.newHashMap();
        for (int i = 0; i < columns.size(); i++) {
            final TblColRef tblColRef = columns.get(i);
            if (allDimensions.contains(tblColRef)) {
                result.put(i, tblColRef);
            }
        }
        return result;
    }

    private Map<TblColRef, Dictionary<?>> buildDictionary(final Map<Integer, TblColRef> tblColRefMap, List<List<String>> recordList) throws IOException {
        HashMap<TblColRef, Dictionary<?>> result = Maps.newHashMap();

        HashMultimap<TblColRef, String> valueMap = HashMultimap.create();
        for (List<String> row : recordList) {
            for (int i = 0; i < row.size(); i++) {
                String cell = row.get(i);
                if (tblColRefMap.containsKey(i)) {
                    valueMap.put(tblColRefMap.get(i), cell);
                }
            }
        }
        for (TblColRef tblColRef : valueMap.keySet()) {
            final Collection<byte[]> bytes = Collections2.transform(valueMap.get(tblColRef), new Function<String, byte[]>() {
                @Nullable
                @Override
                public byte[] apply(String input) {
                    return input == null ? null : input.getBytes();
                }
            });
            final Dictionary<?> dict = DictionaryGenerator.buildDictionaryFromValueList(tblColRef.getType(), bytes);
            result.put(tblColRef, dict);
        }
        return result;
    }

    private Map<Long, HyperLogLogPlusCounter> sampling(CubeDesc cubeDesc, List<List<String>> streams) {
        CubeJoinedFlatTableDesc intermediateTableDesc = new CubeJoinedFlatTableDesc(cubeDesc, null);
        final int rowkeyLength = cubeDesc.getRowkey().getRowKeyColumns().length;
        final List<Long> allCuboidIds = getAllCuboidIds(cubeDesc);
        final long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        final Map<Long, Integer[]> allCuboidsBitSet = Maps.newHashMap();


        Lists.transform(allCuboidIds, new Function<Long, Integer[]>() {
            @Nullable
            @Override
            public Integer[] apply(@Nullable Long cuboidId) {
                BitSet bitSet = BitSet.valueOf(new long[]{cuboidId});
                Integer[] result = new Integer[bitSet.cardinality()];

                long mask = Long.highestOneBit(baseCuboidId);
                int position = 0;
                for (int i = 0; i < rowkeyLength; i++) {
                    if ((mask & cuboidId) > 0) {
                        result[position] = i;
                        position++;
                    }
                    mask = mask >> 1;
                }
                return result;
            }
        });
        final Map<Long, HyperLogLogPlusCounter> result = Maps.newHashMapWithExpectedSize(allCuboidIds.size());
        for (Long cuboidId : allCuboidIds) {
            result.put(cuboidId, new HyperLogLogPlusCounter(14));
            BitSet bitSet = BitSet.valueOf(new long[]{cuboidId});
            Integer[] cuboidBitSet = new Integer[bitSet.cardinality()];

            long mask = Long.highestOneBit(baseCuboidId);
            int position = 0;
            for (int i = 0; i < rowkeyLength; i++) {
                if ((mask & cuboidId) > 0) {
                    cuboidBitSet[position] = i;
                    position++;
                }
                mask = mask >> 1;
            }
            allCuboidsBitSet.put(cuboidId, cuboidBitSet);
        }

        HashFunction hf = Hashing.murmur3_32();
        ByteArray[] row_hashcodes = new ByteArray[rowkeyLength];
        for (int i = 0; i < rowkeyLength; i++) {
            row_hashcodes[i] = new ByteArray();
        }
        for (List<String> row : streams) {
            //generate hash for each row key column
            for (int i = 0; i < rowkeyLength; i++) {
                Hasher hc = hf.newHasher();
                final String cell = row.get(intermediateTableDesc.getRowKeyColumnIndexes()[i]);
                if (cell != null) {
                    row_hashcodes[i].set(hc.putString(cell).hash().asBytes());
                } else {
                    row_hashcodes[i].set(hc.putInt(0).hash().asBytes());
                }
            }

            for (Map.Entry<Long, HyperLogLogPlusCounter> longHyperLogLogPlusCounterEntry : result.entrySet()) {
                Long cuboidId = longHyperLogLogPlusCounterEntry.getKey();
                HyperLogLogPlusCounter counter = longHyperLogLogPlusCounterEntry.getValue();
                Hasher hc = hf.newHasher();
                final Integer[] cuboidBitSet = allCuboidsBitSet.get(cuboidId);
                for (int position = 0; position < cuboidBitSet.length; position++) {
                    hc.putBytes(row_hashcodes[cuboidBitSet[position]].array());
                }
                counter.add(hc.hash().asBytes());
            }
        }
        return result;
    }

    //TODO: should we use cubeManager.promoteNewlyBuiltSegments?
    private void commitSegment(CubeSegment cubeSegment) throws IOException {
        cubeSegment.setStatus(SegmentStatusEnum.READY);
        CubeBuilder cubeBuilder = new CubeBuilder(cubeSegment.getCubeInstance());
        cubeBuilder.setToAddSegs(cubeSegment);
        CubeManager.getInstance(kylinConfig).updateCube(cubeBuilder);
    }

    private List<Long> getAllCuboidIds(CubeDesc cubeDesc) {
        final long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        List<Long> result = Lists.newArrayList();
        CuboidScheduler cuboidScheduler = new CuboidScheduler(cubeDesc);
        getSubCuboidIds(cuboidScheduler, baseCuboidId, result);
        return result;
    }

    private void getSubCuboidIds(CuboidScheduler cuboidScheduler, long parentCuboidId, List<Long> result) {
        result.add(parentCuboidId);
        for (Long cuboidId: cuboidScheduler.getSpanningCuboid(parentCuboidId)) {
            getSubCuboidIds(cuboidScheduler, cuboidId, result);
        }
    }


    private HTableInterface createHTable(final CubeSegment cubeSegment) throws Exception {
        final String hTableName = cubeSegment.getStorageLocationIdentifier();
        CubeHTableUtil.createHTable(cubeSegment.getCubeDesc(), hTableName, null);
        final HTableInterface hTable = HBaseConnection.get(KylinConfig.getInstanceFromEnv().getStorageUrl()).getTable(hTableName);
        logger.info("hTable:" + hTableName + " for segment:" + cubeSegment.getName() + " created!");
        return hTable;
    }

    @Override
    protected void onStop() {

    }

    @Override
    protected int batchInterval() {
        return 5 * 60 * 1000;//5 min
    }

    @Override
    protected int batchSize() {
        return 1000;
    }
}

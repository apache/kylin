package org.apache.kylin.storage.hbase.cube.v2.coprocessor.endpoint;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.HBaseZeroCopyByteString;
import com.google.protobuf.RpcCallback;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.TestRowProcessorEndpoint;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.*;
import org.apache.kylin.cube.gridtable.CubeCodeSystem;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.dict.StringBytesConverter;
import org.apache.kylin.dict.TrieDictionaryBuilder;
import org.apache.kylin.dimension.DateDimEnc;
import org.apache.kylin.dimension.DictionaryDimEnc;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.gridtable.*;
import org.apache.kylin.gridtable.memstore.GTSimpleMemStore;
import org.apache.kylin.measure.bitmap.BitmapCounter;
import org.apache.kylin.measure.bitmap.RoaringBitmapCounterFactory;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.storage.gtrecord.PartitionResultIterator;
import org.apache.kylin.storage.hbase.cube.v2.RawScan;
import org.apache.kylin.storage.hbase.cube.v2.coprocessor.endpoint.generated.CubeVisitProtos;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.apache.commons.lang.builder.ToStringStyle.SHORT_PREFIX_STYLE;

/**
 * CubeVisitServiceWithCompressionTest
 * created by haihuang.hhl @2020/1/10
 */
public class CubeVisitServiceWithCompressionTest extends LocalFileMetadataTestCase {

    static class TestStatInfo {
        long testDataRows;
        String algorithm;
        long returnRows;
        long returnBitmapSize;
        long scanBytesSize;
        long scannedRowCount;
        long compressedSize;
        long compressTimeCost;
        long decompressTimeCost;

        public long getTestDataRows() {
            return testDataRows;
        }

        public void setTestDataRows(long testDataRows) {
            this.testDataRows = testDataRows;
        }


        public long getScannedRowCount() {
            return scannedRowCount;
        }

        public void setScannedRowCount(long scannedRowCount) {
            this.scannedRowCount = scannedRowCount;
        }


        public String getAlgorithm() {
            return algorithm;
        }

        public void setAlgorithm(String algorithm) {
            this.algorithm = algorithm;
        }

        public long getReturnRows() {
            return returnRows;
        }

        public void setReturnRows(long returnRows) {
            this.returnRows = returnRows;
        }

        public long getReturnBitmapSize() {
            return returnBitmapSize;
        }

        public void setReturnBitmapSize(long returnBitmapSize) {
            this.returnBitmapSize = returnBitmapSize;
        }

        public long getScanBytesSize() {
            return scanBytesSize;
        }

        public void setScanBytesSize(long scanBytesSize) {
            this.scanBytesSize = scanBytesSize;
        }

        public long getCompressedSize() {
            return compressedSize;
        }

        public void setCompressedSize(long compressedSize) {
            this.compressedSize = compressedSize;
        }

        public long getCompressTimeCost() {
            return compressTimeCost;
        }

        public void setCompressTimeCost(long compressTimeCost) {
            this.compressTimeCost = compressTimeCost;
        }

        public long getDecompressTimeCost() {
            return decompressTimeCost;
        }

        public void setDecompressTimeCost(long decompressTimeCost) {
            this.decompressTimeCost = decompressTimeCost;
        }

        public String toString() {
            return ToStringBuilder.reflectionToString(this, SHORT_PREFIX_STYLE);
        }
    }

    private static final TableName TABLE = TableName.valueOf("KYLIN_TEST_COMPRESSION");
    private static HBaseTestingUtility util = new HBaseTestingUtility();
    private volatile static HRegion region = null;
    private volatile static GTInfo gtInfo = null;

    private static final long baseCuboid = 3L;


    private final static byte[] FAM = Bytes.toBytes("f1");
    private final static byte[] COL_M = Bytes.toBytes("m");


    private static List<String> dateList;
    private static List<String> countryList;
    private static BitmapCounter bitmapCounter;

    private static final Map<String, Long> expCountryRet = Maps.newHashMap();
    private static List<TestStatInfo> testStatInfos = new ArrayList<>();


    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        util.shutdownMiniCluster();
        staticCleanupTestMetadata();
    }

    private static List<String> generateBigTestData(int nday) throws ParseException {
        String datestr = "2010-01-01";
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date date = sdf.parse(datestr);
        cal.setTime(date);
        List<String> dateList = new ArrayList<String>();
        for (int i = 0; i < nday; i++) {
            dateList.add(sdf.format(cal.getTime()));
            cal.add(Calendar.DATE, -1);
        }
        return dateList;
    }

    private static BitmapCounter getBitmap() {
        BitmapCounter bitmap = RoaringBitmapCounterFactory.INSTANCE.newBitmap();
        Random random = new Random();
        for (int i = 0; i < 1_000_000; i++) {
            if (random.nextInt(100) < 20) {
                bitmap.add(i);
            }
        }
        return bitmap;
    }

    public static void prepareTestData(int rows) throws IOException, ParseException {
        // rows 1K,2k,3K,4K...
        dateList = generateBigTestData(rows / 10); //4w
        countryList = Lists.newArrayList("CHN", "JAP", "USA", "RUS", "ENG", "AU", "CA", "DE", "IN", "IT");
        bitmapCounter = getBitmap();
        // step1: create test Hbase table and region
        try {
            util.getHBaseAdmin().disableTable(TABLE);
            util.getHBaseAdmin().deleteTable(TABLE);
        } catch (Exception e) {
            // ignore table not found
        }
        Table table = util.createTable(TABLE, FAM);
        HRegionInfo hRegionInfo = new HRegionInfo(table.getName());
        region = util.createLocalHRegion(hRegionInfo, table.getTableDescriptor());

        // step2: create test gtInfo
        gtInfo = newInfo();
        GridTable gridTable = newTable(gtInfo);
        IGTScanner scanner = gridTable.scan(new GTScanRequestBuilder().setInfo(gtInfo).setRanges(null)
                .setDimensions(null).setFilterPushDown(null).createGTScanRequest());
        for (GTRecord record : scanner) {
            byte[] value = record.exportColumns(gtInfo.getPrimaryKey()).toBytes();
            byte[] key = new byte[RowConstants.ROWKEY_SHARD_AND_CUBOID_LEN + value.length];
            System.arraycopy(Bytes.toBytes(baseCuboid), 0, key, RowConstants.ROWKEY_SHARDID_LEN,
                    RowConstants.ROWKEY_CUBOIDID_LEN);
            System.arraycopy(value, 0, key, RowConstants.ROWKEY_SHARD_AND_CUBOID_LEN, value.length);
            Put put = new Put(key);
            put.addColumn(FAM, COL_M, record.exportColumns(gtInfo.getColumnBlock(1)).toBytes());
            region.put(put);
        }
    }

    @BeforeClass
    public static void setupBeforeClass() throws Exception {

        Configuration conf = util.getConfiguration();
        conf.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
                TestRowProcessorEndpoint.RowProcessorEndpoint.class.getName());
        conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);
        conf.setInt(HConstants.MASTER_PORT, 17000);
        conf.setInt(HConstants.MASTER_INFO_PORT, 17010);
        conf.setInt(HConstants.REGIONSERVER_PORT, 17020);
        conf.setLong("hbase.hregion.row.processor.timeout", 1000L);
        util.startMiniCluster();
        staticCreateTestMetadata();
    }

    @Test
    public void test() throws Exception {
        int[] rows = new int[]{1000};
        String[] algos = new String[]{"zstd", "lz4", ""};
        for (int row : rows) {
            for (String algo : algos) {
                testOne(row, algo);
            }
        }

        for (int i = 0; i < testStatInfos.size(); i++) {
            System.out.println("test " + i + " " + testStatInfos.get(i).toString());
        }
    }

    public void testOne(int rows, String algorithm) throws Exception {
        KylinConfig.getInstanceFromEnv().setProperty("kylin.storage.hbase.endpoint-compress-algorithm", algorithm);
        prepareTestData(rows);
        TestStatInfo statInfo = new TestStatInfo();
        statInfo.setTestDataRows(rows);
        if ("".equals(algorithm)) {
            algorithm = "default";
        }
        statInfo.setAlgorithm(algorithm);
        testVisitCube(statInfo);
        testStatInfos.add(statInfo);
    }

    private static RawScan mockFullScan(GTInfo gtInfo, KylinConfig kylinConfig) {
        final List<Pair<byte[], byte[]>> selectedColumns = Lists.newArrayList();
        selectedColumns.add(new Pair<>(FAM, COL_M));

        int headerLength = RowConstants.ROWKEY_SHARD_AND_CUBOID_LEN;
        int bodyLength = 0;
        ImmutableBitSet primaryKey = gtInfo.getPrimaryKey();
        for (int i = 0; i < primaryKey.trueBitCount(); i++) {
            bodyLength += gtInfo.getCodeSystem().getDimEnc(primaryKey.trueBitAt(i)).getLengthOfEncoding();
        }
        //Mock start key
        byte[] start = new byte[headerLength + bodyLength];
        BytesUtil.writeShort((short) 0, start, 0, RowConstants.ROWKEY_SHARDID_LEN);
        System.arraycopy(Bytes.toBytes(baseCuboid), 0, start, RowConstants.ROWKEY_SHARDID_LEN,
                RowConstants.ROWKEY_CUBOIDID_LEN);

        //Mock end key
        byte[] end = new byte[headerLength + bodyLength + 1];
        for (int i = 0; i < end.length - 1; i++) {
            end[i] = RowConstants.ROWKEY_UPPER_BYTE;
        }
        BytesUtil.writeShort((short) 0, end, 0, RowConstants.ROWKEY_SHARDID_LEN);
        System.arraycopy(Bytes.toBytes(baseCuboid), 0, end, RowConstants.ROWKEY_SHARDID_LEN,
                RowConstants.ROWKEY_CUBOIDID_LEN);

        //Mock fuzzy key
        List<Pair<byte[], byte[]>> fuzzyKeys = Collections.emptyList();

        return new RawScan(start, end, selectedColumns, fuzzyKeys, kylinConfig.getHBaseScanCacheRows(),
                kylinConfig.getHBaseScanMaxResultSize());
    }


    public void testVisitCube(TestStatInfo testStatInfo) throws Exception {
        RawScan rawScan = mockFullScan(gtInfo, getTestConfig());

        CoprocessorEnvironment env = PowerMockito.mock(RegionCoprocessorEnvironment.class);
        PowerMockito.when(env, "getRegion").thenReturn(region);

        final CubeVisitService service = new CubeVisitService();
        service.start(env);

        CubeVisitProtos.CubeVisitRequest request = CubeVisitServiceTest.mockFullScanRequest(gtInfo, Lists.newArrayList(rawScan));

        RpcCallback<CubeVisitProtos.CubeVisitResponse> done = new RpcCallback<CubeVisitProtos.CubeVisitResponse>() {
            @Override
            public void run(CubeVisitProtos.CubeVisitResponse result) {
                CubeVisitProtos.CubeVisitResponse.Stats stats = result.getStats();
                Assert.assertEquals(0L, stats.getAggregatedRowCount());
                Assert.assertEquals(0L, stats.getFilteredRowCount());
                Assert.assertEquals(dateList.size() * countryList.size(), stats.getScannedRowCount());
                testStatInfo.setCompressedSize(result.getCompressedRows().size());
                testStatInfo.setCompressTimeCost(stats.getCompressionTimeCost());
                testStatInfo.setScannedRowCount(stats.getScannedRowCount());
                try {
                    long dc_start = System.currentTimeMillis();
                    byte[] rawData = CompressionUtils
                            .decompress(HBaseZeroCopyByteString.zeroCopyGetBytes(result.getCompressedRows()), KylinConfig.getInstanceFromEnv().getCompressionAlgorithm());
                    long dc_end = System.currentTimeMillis();
                    PartitionResultIterator iterator = new PartitionResultIterator(rawData, gtInfo, setOf(0, 1, 2, 3));
                    int nReturn = 0;
                    long bitMapsize = 0;
                    while (iterator.hasNext()) {
                        GTRecord record = iterator.next();
                        if (nReturn == 1) {
                            BitmapCounter bitmap = (BitmapCounter) record.getValue(3);
                            assert bitmap.getCount() == bitmapCounter.getCount();
                            testStatInfo.setReturnBitmapSize(bitmap.getCount());
                        }
                        nReturn++;
                    }
                    testStatInfo.setReturnRows(nReturn);
                    testStatInfo.setDecompressTimeCost(dc_end - dc_start);
                    Assert.assertEquals(dateList.size() * countryList.size(), nReturn);
                } catch (Exception e) {
                    Assert.fail("Fail due to " + e);
                }
            }
        };
        service.visitCube(null, request, done);
    }


    public static ImmutableBitSet setOf(int... values) {
        BitSet set = new BitSet();
        for (int i : values)
            set.set(i);
        return new ImmutableBitSet(set);
    }

    private static Dictionary strsToDict(Collection<String> strs) {
        TrieDictionaryBuilder<String> builder = new TrieDictionaryBuilder<>(new StringBytesConverter());
        for (String str : strs) {
            builder.addValue(str);
        }
        return builder.build(0);
    }

    private static GTInfo newInfo(GTInfo.Builder builder) {
        //Dimension
        ImmutableBitSet dimensionColumns = setOf(0, 1);
        DimensionEncoding[] dimEncs = new DimensionEncoding[2];
        dimEncs[0] = new DateDimEnc();
        dimEncs[1] = new DictionaryDimEnc(strsToDict(countryList));
        builder.setCodeSystem(new CubeCodeSystem(dimEncs));
        builder.setPrimaryKey(dimensionColumns);

        //Measure
        ImmutableBitSet measureColumns = setOf(2, 3);

        builder.enableColumnBlock(new ImmutableBitSet[]{dimensionColumns, measureColumns});
        GTInfo info = builder.build();
        return info;
    }

    private static GTInfo newInfo() {
        GTInfo.Builder builder = GTInfo.builder();
        builder.setColumns(//
                DataType.getType("date"), //  date
                DataType.getType("string"), // country
                DataType.getType("bigint"), // pv
                DataType.getType("bitmap") // uv
        );
        return newInfo(builder);
    }

    private static GridTable newTable(GTInfo info) throws IOException, ParseException {
        GTSimpleMemStore store = new GTSimpleMemStore(info);
        GridTable table = new GridTable(info, store);
        GTRecord record = new GTRecord(info);

        Random rand = new Random();
        GTBuilder builder = table.rebuild();
        Map<String, List<Long>> contents = Maps.newHashMap();
        for (String date : dateList) {
            for (String country : countryList) {
                List<Long> innerList = contents.get(country);
                if (innerList == null) {
                    innerList = Lists.newArrayList();
                    contents.put(country, innerList);
                }

                Long value = (long) rand.nextDouble() * 100000000;
                innerList.add(value);

                builder.write(record.setValues(date, country, value, bitmapCounter));
            }
        }
        for (String country : contents.keySet()) {
            Long sum = 1L;
            for (Long innerValue : contents.get(country)) {
                sum = sum + innerValue;
            }
            expCountryRet.put(country, sum);
        }
        builder.close();

        return table;
    }


}

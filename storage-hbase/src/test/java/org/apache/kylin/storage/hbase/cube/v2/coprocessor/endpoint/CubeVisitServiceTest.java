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

package org.apache.kylin.storage.hbase.cube.v2.coprocessor.endpoint;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.TestRowProcessorEndpoint;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.CompressionUtils;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.gridtable.CubeCodeSystem;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.dict.StringBytesConverter;
import org.apache.kylin.dict.TrieDictionaryBuilder;
import org.apache.kylin.dimension.DateDimEnc;
import org.apache.kylin.dimension.DictionaryDimEnc;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.gridtable.GTBuilder;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.GTScanRequestBuilder;
import org.apache.kylin.gridtable.GridTable;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.gridtable.memstore.GTSimpleMemStore;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.expression.BinaryTupleExpression;
import org.apache.kylin.metadata.expression.CaseTupleExpression;
import org.apache.kylin.metadata.expression.ColumnTupleExpression;
import org.apache.kylin.metadata.expression.NumberTupleExpression;
import org.apache.kylin.metadata.expression.TupleExpression;
import org.apache.kylin.metadata.expression.TupleExpression.ExpressionOperatorEnum;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.gtrecord.PartitionResultIterator;
import org.apache.kylin.storage.hbase.cube.v2.CubeHBaseEndpointRPC;
import org.apache.kylin.storage.hbase.cube.v2.RawScan;
import org.apache.kylin.storage.hbase.cube.v2.coprocessor.endpoint.generated.CubeVisitProtos;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import com.google.protobuf.HBaseZeroCopyByteString;
import com.google.protobuf.RpcCallback;

public class CubeVisitServiceTest extends LocalFileMetadataTestCase {

    private static final TableName TABLE = TableName.valueOf("KYLIN_testtable");

    private static HBaseTestingUtility util = new HBaseTestingUtility();

    private volatile static HRegion region = null;
    private volatile static GTInfo gtInfo = null;
    private static final long baseCuboid = 3L;

    private final static byte[] FAM = Bytes.toBytes("f1");
    private final static byte[] COL_M = Bytes.toBytes("m");

    private static final List<String> dateList = Lists.newArrayList("2018-01-14", "2018-01-15", "2018-01-16");
    private static final List<String> userList = Lists.newArrayList("Ken", "Lisa", "Gang", "Kalin", "Julian", "John");
    private static final List<BigDecimal> priceList = Lists.newArrayList(new BigDecimal("10.5"),
            new BigDecimal("15.5"));

    private static final Map<String, Double> expUserStddevRet = Maps.newHashMap();
    private static final Map<String, BigDecimal> expUserRet = Maps.newHashMap();
    private static final BigDecimal userCnt = new BigDecimal(dateList.size());

    public static void prepareTestData() throws Exception {
        try {
            util.getHBaseAdmin().disableTable(TABLE);
            util.getHBaseAdmin().deleteTable(TABLE);
        } catch (Exception e) {
            // ignore table not found
        }
        Table table = util.createTable(TABLE, FAM);
        HRegionInfo hRegionInfo = new HRegionInfo(table.getName());
        region = util.createLocalHRegion(hRegionInfo, table.getTableDescriptor());

        gtInfo = newInfo();
        GridTable gridTable = newTable(gtInfo);
        try (IGTScanner scanner = gridTable.scan(new GTScanRequestBuilder().setInfo(gtInfo).setRanges(null)
                .setDimensions(null).setFilterPushDown(null).createGTScanRequest())) {
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

        prepareTestData();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        util.shutdownMiniCluster();
        staticCleanupTestMetadata();
    }

    @Test(expected = CoprocessorException.class)
    public void testStart() throws IOException {
        CoprocessorEnvironment env = PowerMockito.mock(RegionServerCoprocessorEnvironment.class);
        CubeVisitService service = new CubeVisitService();
        service.start(env);
    }

    @Test
    public void testVisitCube() throws Exception {
        RawScan rawScan = mockFullScan(gtInfo, getTestConfig());

        CoprocessorEnvironment env = PowerMockito.mock(RegionCoprocessorEnvironment.class);
        PowerMockito.when(env, "getRegion").thenReturn(region);

        final CubeVisitService service = new CubeVisitService();
        service.start(env);

        CubeVisitProtos.CubeVisitRequest request = mockFullScanRequest(gtInfo, Lists.newArrayList(rawScan));

        RpcCallback<CubeVisitProtos.CubeVisitResponse> done = new RpcCallback<CubeVisitProtos.CubeVisitResponse>() {
            @Override
            public void run(CubeVisitProtos.CubeVisitResponse result) {
                CubeVisitProtos.CubeVisitResponse.Stats stats = result.getStats();
                Assert.assertEquals(0L, stats.getAggregatedRowCount());
                Assert.assertEquals(0L, stats.getFilteredRowCount());
                Assert.assertEquals(dateList.size() * userList.size(), stats.getScannedRowCount());

                try {
                    byte[] rawData = CompressionUtils
                            .decompress(HBaseZeroCopyByteString.zeroCopyGetBytes(result.getCompressedRows()));
                    PartitionResultIterator iterator = new PartitionResultIterator(rawData, gtInfo, setOf(0, 1, 2, 3));
                    int nReturn = 0;
                    while (iterator.hasNext()) {
                        iterator.next();
                        nReturn++;
                    }
                    Assert.assertEquals(dateList.size() * userList.size(), nReturn);
                } catch (Exception e) {
                    Assert.fail("Fail due to " + e);
                }
            }
        };
        service.visitCube(null, request, done);
    }

    @Test
    public void testVisitCubeWithRuntimeAggregates() throws Exception {
        RawScan rawScan = mockFullScan(gtInfo, getTestConfig());

        CoprocessorEnvironment env = PowerMockito.mock(RegionCoprocessorEnvironment.class);
        PowerMockito.when(env, "getRegion").thenReturn(region);

        final CubeVisitService service = new CubeVisitService();
        service.start(env);

        final CubeVisitProtos.CubeVisitRequest request = mockScanRequestWithRuntimeAggregates(gtInfo,
                Lists.newArrayList(rawScan));

        RpcCallback<CubeVisitProtos.CubeVisitResponse> done = new RpcCallback<CubeVisitProtos.CubeVisitResponse>() {
            @Override
            public void run(CubeVisitProtos.CubeVisitResponse result) {
                try {
                    byte[] rawData = CompressionUtils
                            .decompress(HBaseZeroCopyByteString.zeroCopyGetBytes(result.getCompressedRows()));
                    PartitionResultIterator iterator = new PartitionResultIterator(rawData, gtInfo, setOf(1, 3));
                    Map<String, BigDecimal> actRet = Maps.newHashMap();
                    while (iterator.hasNext()) {
                        GTRecord record = iterator.next();
                        String key = (String) record.decodeValue(1);
                        BigDecimal value = (BigDecimal) record.decodeValue(3);
                        actRet.put(key, value);
                    }

                    Map<String, BigDecimal> innerExpUserRet = Maps.newHashMap();
                    for (String key : expUserRet.keySet()) {
                        BigDecimal value = new BigDecimal(0);
                        if (key.equals("Ken")) {
                            value = value.add(expUserRet.get(key));
                            value = value.multiply(new BigDecimal(2));
                            value = value.add(userCnt);
                        } else {
                            value = value.add(userCnt);
                        }
                        innerExpUserRet.put(key, value);
                    }
                    Assert.assertEquals(innerExpUserRet, actRet);
                } catch (Exception e) {
                    Assert.fail("Fail due to " + e);
                }
            }
        };
        service.visitCube(null, request, done);
    }

    @Test
    public void testVisitCubeWithRuntimeDimensions() throws Exception {
        GTInfo.Builder builder = GTInfo.builder();
        builder.setColumns(//
                DataType.getType("date"), //
                DataType.getType("string"), //
                DataType.getType("decimal"), //
                DataType.getType("decimal") // for runtime aggregation
        );
        builder.enableDynamicDims(setOf(3));

        final GTInfo gtInfo = newInfo(builder);
        RawScan rawScan = mockFullScan(gtInfo, getTestConfig());

        CoprocessorEnvironment env = PowerMockito.mock(RegionCoprocessorEnvironment.class);
        PowerMockito.when(env, "getRegion").thenReturn(region);

        final CubeVisitService service = new CubeVisitService();
        service.start(env);

        CubeVisitProtos.CubeVisitRequest request = mockScanRequestWithRuntimeDimensions(gtInfo,
                Lists.newArrayList(rawScan));

        RpcCallback<CubeVisitProtos.CubeVisitResponse> done = new RpcCallback<CubeVisitProtos.CubeVisitResponse>() {
            @Override
            public void run(CubeVisitProtos.CubeVisitResponse result) {
                try {
                    byte[] rawData = CompressionUtils
                            .decompress(HBaseZeroCopyByteString.zeroCopyGetBytes(result.getCompressedRows()));
                    PartitionResultIterator iterator = new PartitionResultIterator(rawData, gtInfo, setOf(2, 3));
                    Map<BigDecimal, BigDecimal> actRet = Maps.newHashMap();
                    while (iterator.hasNext()) {
                        GTRecord record = iterator.next();
                        BigDecimal key = (BigDecimal) record.decodeValue(3);
                        BigDecimal value = (BigDecimal) record.decodeValue(2);
                        actRet.put(key, value);
                    }

                    Map<BigDecimal, BigDecimal> innerExpUserRet = Maps.newHashMap();
                    for (String key : expUserRet.keySet()) {
                        BigDecimal keyI;
                        if (key.equals("Ken")) {
                            keyI = new BigDecimal(1);
                        } else {
                            keyI = new BigDecimal(2);
                        }
                        BigDecimal value = innerExpUserRet.get(keyI);
                        if (value == null) {
                            value = new BigDecimal(0);
                        }
                        value = value.add(expUserRet.get(key));
                        innerExpUserRet.put(keyI, value);
                    }
                    Assert.assertEquals(innerExpUserRet, actRet);
                } catch (Exception e) {
                    Assert.fail("Fail due to " + e);
                }
            }
        };
        service.visitCube(null, request, done);
    }

    public static CubeVisitProtos.CubeVisitRequest mockScanRequestWithRuntimeDimensions(GTInfo gtInfo,
            List<RawScan> rawScans) throws IOException {
        ImmutableBitSet dimensions = setOf();
        ImmutableBitSet aggrGroupBy = setOf(3);
        ImmutableBitSet aggrMetrics = setOf(2);
        String[] aggrMetricsFuncs = { "SUM" };
        ImmutableBitSet dynColumns = setOf(3);

        TupleFilter whenFilter = getCompareTupleFilter(1, "Ken");
        TupleExpression thenExpr = new NumberTupleExpression(1);

        List<Pair<TupleFilter, TupleExpression>> whenList = Lists.newArrayList();
        whenList.add(new Pair<>(whenFilter, thenExpr));

        TupleExpression elseExpr = new NumberTupleExpression(2);

        /**
         * case
         *  when user = 'Ken' then 1
         *  else 2
         * end
         */
        TupleExpression caseExpression = new CaseTupleExpression(whenList, elseExpr);

        Map<Integer, TupleExpression> tupleExpressionMap = Maps.newHashMap();
        tupleExpressionMap.put(3, caseExpression);

        GTScanRequest scanRequest = new GTScanRequestBuilder().setInfo(gtInfo).setRanges(null)//
                .setDimensions(dimensions).setAggrGroupBy(aggrGroupBy)//
                .setAggrMetrics(aggrMetrics).setAggrMetricsFuncs(aggrMetricsFuncs)//
                .setDynamicColumns(dynColumns).setExprsPushDown(tupleExpressionMap)//
                .setStartTime(System.currentTimeMillis()).createGTScanRequest();

        final List<CubeVisitProtos.CubeVisitRequest.IntList> intListList = mockIntList(setOf(2));
        return mockScanRequest(rawScans, scanRequest, intListList);
    }

    public static CubeVisitProtos.CubeVisitRequest mockScanRequestWithRuntimeAggregates(GTInfo gtInfo,
            List<RawScan> rawScans) throws IOException {
        ImmutableBitSet dimensions = setOf(1);
        ImmutableBitSet aggrGroupBy = setOf(1);
        ImmutableBitSet aggrMetrics = setOf(3);
        String[] aggrMetricsFuncs = { "SUM" };
        ImmutableBitSet dynColumns = setOf(3);
        ImmutableBitSet rtAggrMetrics = setOf(2);

        TupleFilter whenFilter = getCompareTupleFilter(1, "Ken");
        TupleExpression colExpression = new ColumnTupleExpression(gtInfo.colRef(2));
        TupleExpression constExpression1 = new NumberTupleExpression(1);
        TupleExpression constExpression2 = new NumberTupleExpression(2);
        TupleExpression biExpression = new BinaryTupleExpression(ExpressionOperatorEnum.MULTIPLE,
                Lists.newArrayList(colExpression, constExpression2));
        TupleExpression thenExpression = new BinaryTupleExpression(ExpressionOperatorEnum.PLUS,
                Lists.newArrayList(biExpression, constExpression1));

        List<Pair<TupleFilter, TupleExpression>> whenList = Lists.newArrayList();
        whenList.add(new Pair<>(whenFilter, thenExpression));

        TupleExpression elseExpression = new NumberTupleExpression(1);

        /**
         * case
         *  when user = 'Ken' then price * 2 + 1
         *  else 1
         * end
         */
        TupleExpression caseExpression = new CaseTupleExpression(whenList, elseExpression);

        Map<Integer, TupleExpression> tupleExpressionMap = Maps.newHashMap();
        tupleExpressionMap.put(3, caseExpression);

        GTScanRequest scanRequest = new GTScanRequestBuilder().setInfo(gtInfo).setRanges(null)//
                .setDimensions(dimensions).setAggrGroupBy(aggrGroupBy)//
                .setAggrMetrics(aggrMetrics).setAggrMetricsFuncs(aggrMetricsFuncs)//
                .setRtAggrMetrics(rtAggrMetrics)//
                .setDynamicColumns(dynColumns).setExprsPushDown(tupleExpressionMap)//
                .setStartTime(System.currentTimeMillis()).createGTScanRequest();

        final List<CubeVisitProtos.CubeVisitRequest.IntList> intListList = mockIntList(setOf(2));
        return mockScanRequest(rawScans, scanRequest, intListList);
    }

    public static CompareTupleFilter getCompareTupleFilter(int col, Object value) {
        TblColRef colRef = gtInfo.colRef(col);
        ColumnTupleFilter colFilter = new ColumnTupleFilter(colRef);

        ByteArray space = new ByteArray(gtInfo.getCodeSystem().maxCodeLength(col));
        gtInfo.getCodeSystem().encodeColumnValue(col, value, space.asBuffer());
        ConstantTupleFilter constFilter = new ConstantTupleFilter(space);

        CompareTupleFilter compareFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.EQ);
        compareFilter.addChild(colFilter);
        compareFilter.addChild(constFilter);

        return compareFilter;
    }

    public static CubeVisitProtos.CubeVisitRequest mockFullScanRequest(GTInfo gtInfo, List<RawScan> rawScans)
            throws IOException {
        GTScanRequest scanRequest = new GTScanRequestBuilder().setInfo(gtInfo).setRanges(null).setDimensions(null)
                .setStartTime(System.currentTimeMillis()).createGTScanRequest();

        final List<CubeVisitProtos.CubeVisitRequest.IntList> intListList = mockIntList(setOf(2, 3));
        return mockScanRequest(rawScans, scanRequest, intListList);
    }

    public static CubeVisitProtos.CubeVisitRequest mockScanRequest(List<RawScan> rawScans, GTScanRequest scanRequest,
            List<CubeVisitProtos.CubeVisitRequest.IntList> intListList) throws IOException {
        final CubeVisitProtos.CubeVisitRequest.Builder builder = CubeVisitProtos.CubeVisitRequest.newBuilder();
        builder.setGtScanRequest(CubeHBaseEndpointRPC.serializeGTScanReq(scanRequest))
                .setHbaseRawScan(CubeHBaseEndpointRPC.serializeRawScans(rawScans));
        for (CubeVisitProtos.CubeVisitRequest.IntList intList : intListList) {
            builder.addHbaseColumnsToGT(intList);
        }
        builder.setRowkeyPreambleSize(RowConstants.ROWKEY_SHARD_AND_CUBOID_LEN);
        builder.setKylinProperties(getTestConfig().exportAllToString());
        builder.setQueryId(UUID.randomUUID().toString());
        builder.setSpillEnabled(getTestConfig().getQueryCoprocessorSpillEnabled());
        builder.setMaxScanBytes(getTestConfig().getPartitionMaxScanBytes());

        return builder.build();
    }

    private static List<CubeVisitProtos.CubeVisitRequest.IntList> mockIntList(ImmutableBitSet selectedCols) {
        List<List<Integer>> hbaseColumnsToGT = Lists.newArrayList();
        hbaseColumnsToGT.add(Lists.newArrayList(selectedCols.iterator()));

        List<CubeVisitProtos.CubeVisitRequest.IntList> hbaseColumnsToGTIntList = Lists.newArrayList();
        for (List<Integer> list : hbaseColumnsToGT) {
            hbaseColumnsToGTIntList.add(CubeVisitProtos.CubeVisitRequest.IntList.newBuilder().addAllInts(list).build());
        }

        return hbaseColumnsToGTIntList;
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

    private static GridTable newTable(GTInfo info) throws IOException {
        GTSimpleMemStore store = new GTSimpleMemStore(info);
        GridTable table = new GridTable(info, store);
        GTRecord record = new GTRecord(info);

        Random rand = new Random();
        GTBuilder builder = table.rebuild();
        expUserRet.clear();
        Map<String, List<BigDecimal>> contents = Maps.newHashMap();
        for (String date : dateList) {
            for (String user : userList) {
                List<BigDecimal> innerList = contents.get(user);
                if (innerList == null) {
                    innerList = Lists.newArrayList();
                    contents.put(user, innerList);
                }

                BigDecimal value = priceList.get(rand.nextInt(priceList.size()));
                innerList.add(value);

                builder.write(record.setValues(date, user, value, new BigDecimal(0)));
            }
        }
        for (String user : contents.keySet()) {
            BigDecimal sum = new BigDecimal(0);
            for (BigDecimal innerValue : contents.get(user)) {
                sum = sum.add(innerValue);
            }
            expUserRet.put(user, sum);
        }
        builder.close();

        return table;
    }

    private static GTInfo newInfo() {
        GTInfo.Builder builder = GTInfo.builder();
        builder.setColumns(//
                DataType.getType("date"), //
                DataType.getType("string"), //
                DataType.getType("decimal"), //
                DataType.getType("decimal") // for runtime aggregation
        );
        return newInfo(builder);
    }

    private static GTInfo newInfo(GTInfo.Builder builder) {
        //Dimension
        ImmutableBitSet dimensionColumns = setOf(0, 1);
        DimensionEncoding[] dimEncs = new DimensionEncoding[2];
        dimEncs[0] = new DateDimEnc();
        dimEncs[1] = new DictionaryDimEnc(strsToDict(userList));
        builder.setCodeSystem(new CubeCodeSystem(dimEncs));
        builder.setPrimaryKey(dimensionColumns);

        //Measure
        ImmutableBitSet measureColumns = setOf(2, 3);

        builder.enableColumnBlock(new ImmutableBitSet[] { dimensionColumns, measureColumns });
        GTInfo info = builder.build();
        return info;
    }

    @SuppressWarnings("rawtypes")
    private static Dictionary strsToDict(Collection<String> strs) {
        TrieDictionaryBuilder<String> builder = new TrieDictionaryBuilder<>(new StringBytesConverter());
        for (String str : strs) {
            builder.addValue(str);
        }
        return builder.build(0);
    }

    public static ImmutableBitSet setOf(int... values) {
        BitSet set = new BitSet();
        for (int i : values)
            set.set(i);
        return new ImmutableBitSet(set);
    }
}

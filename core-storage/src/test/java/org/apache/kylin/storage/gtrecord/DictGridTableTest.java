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

package org.apache.kylin.storage.gtrecord;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesSerializer;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.gridtable.CubeCodeSystem;
import org.apache.kylin.dict.NumberDictionaryBuilder;
import org.apache.kylin.dict.StringBytesConverter;
import org.apache.kylin.dict.TrieDictionaryBuilder;
import org.apache.kylin.dimension.DictionaryDimEnc;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.gridtable.GTBuilder;
import org.apache.kylin.gridtable.GTFilterScanner.FilterResultCache;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTInfo.Builder;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRange;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.GTScanRequestBuilder;
import org.apache.kylin.gridtable.GTUtil;
import org.apache.kylin.gridtable.GridTable;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.gridtable.memstore.GTSimpleMemStore;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.ExtractTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter.FilterOperatorEnum;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class DictGridTableTest extends LocalFileMetadataTestCase {

    private GridTable table;
    private GTInfo info;
    private CompareTupleFilter timeComp0;
    private CompareTupleFilter timeComp1;
    private CompareTupleFilter timeComp2;
    private CompareTupleFilter timeComp3;
    private CompareTupleFilter timeComp4;
    private CompareTupleFilter timeComp5;
    private CompareTupleFilter timeComp6;
    private CompareTupleFilter ageComp1;
    private CompareTupleFilter ageComp2;
    private CompareTupleFilter ageComp3;
    private CompareTupleFilter ageComp4;

    @After
    public void after() throws Exception {

        this.cleanupTestMetadata();
    }

    @Before
    public void setup() throws IOException {

        this.createTestMetadata();

        table = newTestTable();
        info = table.getInfo();

        timeComp0 = compare(info.colRef(0), FilterOperatorEnum.LT, enc(info, 0, "2015-01-14"));
        timeComp1 = compare(info.colRef(0), FilterOperatorEnum.GT, enc(info, 0, "2015-01-14"));
        timeComp2 = compare(info.colRef(0), FilterOperatorEnum.LT, enc(info, 0, "2015-01-13"));
        timeComp3 = compare(info.colRef(0), FilterOperatorEnum.LT, enc(info, 0, "2015-01-15"));
        timeComp4 = compare(info.colRef(0), FilterOperatorEnum.EQ, enc(info, 0, "2015-01-15"));
        timeComp5 = compare(info.colRef(0), FilterOperatorEnum.GT, enc(info, 0, "2015-01-15"));
        timeComp6 = compare(info.colRef(0), FilterOperatorEnum.EQ, enc(info, 0, "2015-01-14"));
        ageComp1 = compare(info.colRef(1), FilterOperatorEnum.EQ, enc(info, 1, "10"));
        ageComp2 = compare(info.colRef(1), FilterOperatorEnum.EQ, enc(info, 1, "20"));
        ageComp3 = compare(info.colRef(1), FilterOperatorEnum.EQ, enc(info, 1, "30"));
        ageComp4 = compare(info.colRef(1), FilterOperatorEnum.NEQ, enc(info, 1, "30"));

    }

    @Test
    public void verifySegmentSkipping() {

        ByteArray segmentStart = enc(info, 0, "2015-01-14");
        ByteArray segmentStartX = enc(info, 0, "2015-01-14 00:00:00");//when partition col is dict encoded, time format will be free
        ByteArray segmentEnd = enc(info, 0, "2015-01-15");
        assertEquals(segmentStart, segmentStartX);

        {
            LogicalTupleFilter filter = and(timeComp0, ageComp1);
            CubeScanRangePlanner planner = new CubeScanRangePlanner(info, Pair.newPair(segmentStart, segmentEnd), info.colRef(0), filter);
            List<GTScanRange> r = planner.planScanRanges();
            assertEquals(1, r.size());//scan range are [close,close]
            assertEquals("[null, 10]-[1421193600000, 10]", r.get(0).toString());
            assertEquals(1, r.get(0).fuzzyKeys.size());
            assertEquals("[[null, 10, null, null, null]]", r.get(0).fuzzyKeys.toString());
        }
        {
            LogicalTupleFilter filter = and(timeComp2, ageComp1);
            CubeScanRangePlanner planner = new CubeScanRangePlanner(info, Pair.newPair(segmentStart, segmentEnd), info.colRef(0), filter);
            List<GTScanRange> r = planner.planScanRanges();
            assertEquals(0, r.size());
        }
        {
            LogicalTupleFilter filter = and(timeComp4, ageComp1);
            CubeScanRangePlanner planner = new CubeScanRangePlanner(info, Pair.newPair(segmentStart, segmentEnd), info.colRef(0), filter);
            List<GTScanRange> r = planner.planScanRanges();
            assertEquals(0, r.size());
        }
        {
            LogicalTupleFilter filter = and(timeComp5, ageComp1);
            CubeScanRangePlanner planner = new CubeScanRangePlanner(info, Pair.newPair(segmentStart, segmentEnd), info.colRef(0), filter);
            List<GTScanRange> r = planner.planScanRanges();
            assertEquals(0, r.size());
        }
        {
            LogicalTupleFilter filter = or(and(timeComp2, ageComp1), and(timeComp1, ageComp1), and(timeComp6, ageComp1));
            CubeScanRangePlanner planner = new CubeScanRangePlanner(info, Pair.newPair(segmentStart, segmentEnd), info.colRef(0), filter);
            List<GTScanRange> r = planner.planScanRanges();
            assertEquals(1, r.size());
            assertEquals("[1421193600000, 10]-[null, 10]", r.get(0).toString());
            assertEquals("[[null, 10, null, null, null], [1421193600000, 10, null, null, null]]", r.get(0).fuzzyKeys.toString());
        }
        {
            LogicalTupleFilter filter = or(timeComp2, timeComp1, timeComp6);
            CubeScanRangePlanner planner = new CubeScanRangePlanner(info, Pair.newPair(segmentStart, segmentEnd), info.colRef(0), filter);
            List<GTScanRange> r = planner.planScanRanges();
            assertEquals(1, r.size());
            assertEquals("[1421193600000, null]-[null, null]", r.get(0).toString());
            assertEquals(0, r.get(0).fuzzyKeys.size());
        }
        {
            //skip FALSE filter
            LogicalTupleFilter filter = and(ageComp1, ConstantTupleFilter.FALSE);
            CubeScanRangePlanner planner = new CubeScanRangePlanner(info, Pair.newPair(segmentStart, segmentEnd), info.colRef(0), filter);
            List<GTScanRange> r = planner.planScanRanges();
            assertEquals(0, r.size());
        }
        {
            //TRUE or FALSE filter
            LogicalTupleFilter filter = or(ConstantTupleFilter.TRUE, ConstantTupleFilter.FALSE);
            CubeScanRangePlanner planner = new CubeScanRangePlanner(info, Pair.newPair(segmentStart, segmentEnd), info.colRef(0), filter);
            List<GTScanRange> r = planner.planScanRanges();
            assertEquals(1, r.size());
            assertEquals("[null, null]-[null, null]", r.get(0).toString());
        }
        {
            //TRUE or other filter
            LogicalTupleFilter filter = or(ageComp1, ConstantTupleFilter.TRUE);
            CubeScanRangePlanner planner = new CubeScanRangePlanner(info, Pair.newPair(segmentStart, segmentEnd), info.colRef(0), filter);
            List<GTScanRange> r = planner.planScanRanges();
            assertEquals(1, r.size());
            assertEquals("[null, null]-[null, null]", r.get(0).toString());
        }
    }

    @Test
    public void verifySegmentSkipping2() {
        ByteArray segmentEnd = enc(info, 0, "2015-01-15");

        {
            LogicalTupleFilter filter = and(timeComp0, ageComp1);
            CubeScanRangePlanner planner = new CubeScanRangePlanner(info, Pair.newPair(new ByteArray(), segmentEnd), info.colRef(0), filter);
            List<GTScanRange> r = planner.planScanRanges();
            assertEquals(1, r.size());//scan range are [close,close]
            assertEquals("[null, 10]-[1421193600000, 10]", r.get(0).toString());
            assertEquals(1, r.get(0).fuzzyKeys.size());
            assertEquals("[[null, 10, null, null, null]]", r.get(0).fuzzyKeys.toString());
        }

        {
            LogicalTupleFilter filter = and(timeComp5, ageComp1);
            CubeScanRangePlanner planner = new CubeScanRangePlanner(info, Pair.newPair(new ByteArray(), segmentEnd), info.colRef(0), filter);
            List<GTScanRange> r = planner.planScanRanges();
            assertEquals(0, r.size());//scan range are [close,close]
        }
    }

    @Test
    public void verifyScanRangePlanner() {

        // flatten or-and & hbase fuzzy value
        {
            LogicalTupleFilter filter = and(timeComp1, or(ageComp1, ageComp2));
            CubeScanRangePlanner planner = new CubeScanRangePlanner(info, null, null, filter);
            List<GTScanRange> r = planner.planScanRanges();
            assertEquals(1, r.size());
            assertEquals("[1421193600000, 10]-[null, 20]", r.get(0).toString());
            assertEquals("[[null, 10, null, null, null], [null, 20, null, null, null]]", r.get(0).fuzzyKeys.toString());
        }

        // pre-evaluate ever false
        {
            LogicalTupleFilter filter = and(timeComp1, timeComp2);
            CubeScanRangePlanner planner = new CubeScanRangePlanner(info, null, null, filter);
            List<GTScanRange> r = planner.planScanRanges();
            assertEquals(0, r.size());
        }

        // pre-evaluate ever true
        {
            LogicalTupleFilter filter = or(timeComp1, ageComp4);
            CubeScanRangePlanner planner = new CubeScanRangePlanner(info, null, null, filter);
            List<GTScanRange> r = planner.planScanRanges();
            assertEquals("[[null, null]-[null, null]]", r.toString());
        }

        // merge overlap range
        {
            LogicalTupleFilter filter = or(timeComp1, timeComp3);
            CubeScanRangePlanner planner = new CubeScanRangePlanner(info, null, null, filter);
            List<GTScanRange> r = planner.planScanRanges();
            assertEquals("[[null, null]-[null, null]]", r.toString());
        }

        // merge too many ranges
        {
            LogicalTupleFilter filter = or(and(timeComp4, ageComp1), and(timeComp4, ageComp2), and(timeComp4, ageComp3));
            CubeScanRangePlanner planner = new CubeScanRangePlanner(info, null, null, filter);
            List<GTScanRange> r = planner.planScanRanges();
            assertEquals(3, r.size());
            assertEquals("[1421280000000, 10]-[1421280000000, 10]", r.get(0).toString());
            assertEquals("[1421280000000, 20]-[1421280000000, 20]", r.get(1).toString());
            assertEquals("[1421280000000, 30]-[1421280000000, 30]", r.get(2).toString());
            planner.setMaxScanRanges(2);
            List<GTScanRange> r2 = planner.planScanRanges();
            assertEquals("[[1421280000000, 10]-[1421280000000, 30]]", r2.toString());
        }
    }

    @Test
    public void verifyFirstRow() throws IOException {
        doScanAndVerify(table, new GTScanRequestBuilder().setInfo(table.getInfo()).setRanges(null).setDimensions(null).setFilterPushDown(null).createGTScanRequest(), "[1421193600000, 30, Yang, 10, 10.5]", //
                "[1421193600000, 30, Luke, 10, 10.5]", //
                "[1421280000000, 20, Dong, 10, 10.5]", //
                "[1421280000000, 20, Jason, 10, 10.5]", //
                "[1421280000000, 30, Xu, 10, 10.5]", //
                "[1421366400000, 20, Mahone, 10, 10.5]", //
                "[1421366400000, 20, Qianhao, 10, 10.5]", //
                "[1421366400000, 30, George, 10, 10.5]", //
                "[1421366400000, 30, Shaofeng, 10, 10.5]", //
                "[1421452800000, 10, Kejia, 10, 10.5]");
    }

    //for testing GTScanRequest serialization and deserialization
    public static GTScanRequest useDeserializedGTScanRequest(GTScanRequest origin) {
        ByteBuffer buffer = ByteBuffer.allocate(BytesSerializer.SERIALIZE_BUFFER_SIZE);
        GTScanRequest.serializer.serialize(origin, buffer);
        buffer.flip();
        GTScanRequest sGTScanRequest = GTScanRequest.serializer.deserialize(buffer);

        Assert.assertArrayEquals(origin.getAggrMetricsFuncs(), sGTScanRequest.getAggrMetricsFuncs());
        Assert.assertEquals(origin.getAggCacheMemThreshold(), sGTScanRequest.getAggCacheMemThreshold(), 0.01);
        return sGTScanRequest;
    }

    @Test
    public void verifyScanWithUnevaluatableFilter() throws IOException {
        GTInfo info = table.getInfo();

        CompareTupleFilter fComp = compare(info.colRef(0), FilterOperatorEnum.GT, enc(info, 0, "2015-01-14"));
        ExtractTupleFilter fUnevaluatable = unevaluatable(info.colRef(1));
        LogicalTupleFilter fNotPlusUnevaluatable = not(unevaluatable(info.colRef(1)));
        LogicalTupleFilter filter = and(fComp, fUnevaluatable, fNotPlusUnevaluatable);

        GTScanRequest req = new GTScanRequestBuilder().setInfo(info).setRanges(null).setDimensions(null).setAggrGroupBy(setOf(0)).setAggrMetrics(setOf(3)).setAggrMetricsFuncs(new String[] { "sum" }).setFilterPushDown(filter).createGTScanRequest();

        // note the unEvaluatable column 1 in filter is added to group by
        assertEquals("GTScanRequest [range=[[null, null]-[null, null]], columns={0, 1, 3}, filterPushDown=AND [UNKNOWN_MODEL:NULL.GT_MOCKUP_TABLE.0 GT [\\x00\\x00\\x01J\\xE5\\xBD\\x5C\\x00], [null], [null]], aggrGroupBy={0, 1}, aggrMetrics={3}, aggrMetricsFuncs=[sum]]", req.toString());

        doScanAndVerify(table, useDeserializedGTScanRequest(req), "[1421280000000, 20, null, 20, null]", "[1421280000000, 30, null, 10, null]", "[1421366400000, 20, null, 20, null]", "[1421366400000, 30, null, 20, null]", "[1421452800000, 10, null, 10, null]");
    }

    @Test
    public void verifyScanWithEvaluatableFilter() throws IOException {
        GTInfo info = table.getInfo();

        CompareTupleFilter fComp1 = compare(info.colRef(0), FilterOperatorEnum.GT, enc(info, 0, "2015-01-14"));
        CompareTupleFilter fComp2 = compare(info.colRef(1), FilterOperatorEnum.GT, enc(info, 1, "10"));
        LogicalTupleFilter filter = and(fComp1, fComp2);

        GTScanRequest req = new GTScanRequestBuilder().setInfo(info).setRanges(null).setDimensions(null).setAggrGroupBy(setOf(0)).setAggrMetrics(setOf(3)).setAggrMetricsFuncs(new String[] { "sum" }).setFilterPushDown(filter).createGTScanRequest();
        // note the evaluatable column 1 in filter is added to returned columns but not in group by
        assertEquals("GTScanRequest [range=[[null, null]-[null, null]], columns={0, 1, 3}, filterPushDown=AND [UNKNOWN_MODEL:NULL.GT_MOCKUP_TABLE.0 GT [\\x00\\x00\\x01J\\xE5\\xBD\\x5C\\x00], UNKNOWN_MODEL:NULL.GT_MOCKUP_TABLE.1 GT [\\x00]], aggrGroupBy={0}, aggrMetrics={3}, aggrMetricsFuncs=[sum]]", req.toString());

        doScanAndVerify(table, useDeserializedGTScanRequest(req), "[1421280000000, 20, null, 30, null]", "[1421366400000, 20, null, 40, null]");
    }

    @Test
    public void testFilterScannerPerf() throws IOException {
        GridTable table = newTestPerfTable();
        GTInfo info = table.getInfo();

        CompareTupleFilter fComp1 = compare(info.colRef(0), FilterOperatorEnum.GT, enc(info, 0, "2015-01-14"));
        CompareTupleFilter fComp2 = compare(info.colRef(1), FilterOperatorEnum.GT, enc(info, 1, "10"));
        LogicalTupleFilter filter = and(fComp1, fComp2);

        FilterResultCache.ENABLED = false;
        testFilterScannerPerfInner(table, info, filter);
        FilterResultCache.ENABLED = true;
        testFilterScannerPerfInner(table, info, filter);
        FilterResultCache.ENABLED = false;
        testFilterScannerPerfInner(table, info, filter);
        FilterResultCache.ENABLED = true;
        testFilterScannerPerfInner(table, info, filter);
    }

    @SuppressWarnings("unused")
    private void testFilterScannerPerfInner(GridTable table, GTInfo info, LogicalTupleFilter filter) throws IOException {
        long start = System.currentTimeMillis();
        GTScanRequest req = new GTScanRequestBuilder().setInfo(info).setRanges(null).setDimensions(null).setFilterPushDown(filter).createGTScanRequest();
        IGTScanner scanner = table.scan(req);
        int i = 0;
        for (GTRecord r : scanner) {
            i++;
        }
        scanner.close();
        long end = System.currentTimeMillis();
        System.out.println((end - start) + "ms with filter cache enabled=" + FilterResultCache.ENABLED + ", " + i + " rows");
    }

    @Test
    public void verifyConvertFilterConstants1() {
        GTInfo info = table.getInfo();

        TableDesc extTable = TableDesc.mockup("ext");
        TblColRef extColA = TblColRef.mockup(extTable, 1, "A", "timestamp");
        TblColRef extColB = TblColRef.mockup(extTable, 2, "B", "integer");

        CompareTupleFilter fComp1 = compare(extColA, FilterOperatorEnum.GT, "2015-01-14");
        CompareTupleFilter fComp2 = compare(extColB, FilterOperatorEnum.EQ, "10");
        LogicalTupleFilter filter = and(fComp1, fComp2);

        List<TblColRef> colMapping = Lists.newArrayList();
        colMapping.add(extColA);
        colMapping.add(extColB);

        TupleFilter newFilter = GTUtil.convertFilterColumnsAndConstants(filter, info, colMapping, null);
        assertEquals("AND [UNKNOWN_MODEL:NULL.GT_MOCKUP_TABLE.0 GT [\\x00\\x00\\x01J\\xE5\\xBD\\x5C\\x00], UNKNOWN_MODEL:NULL.GT_MOCKUP_TABLE.1 EQ [\\x00]]", newFilter.toString());
    }

    @Test
    public void verifyConvertFilterConstants2() {
        GTInfo info = table.getInfo();

        TableDesc extTable = TableDesc.mockup("ext");
        TblColRef extColA = TblColRef.mockup(extTable, 1, "A", "timestamp");
        TblColRef extColB = TblColRef.mockup(extTable, 2, "B", "integer");

        CompareTupleFilter fComp1 = compare(extColA, FilterOperatorEnum.GT, "2015-01-14");
        CompareTupleFilter fComp2 = compare(extColB, FilterOperatorEnum.LT, "9");
        LogicalTupleFilter filter = and(fComp1, fComp2);

        List<TblColRef> colMapping = Lists.newArrayList();
        colMapping.add(extColA);
        colMapping.add(extColB);

        // $1<"9" round up to $1<"10"
        TupleFilter newFilter = GTUtil.convertFilterColumnsAndConstants(filter, info, colMapping, null);
        assertEquals("AND [UNKNOWN_MODEL:NULL.GT_MOCKUP_TABLE.0 GT [\\x00\\x00\\x01J\\xE5\\xBD\\x5C\\x00], UNKNOWN_MODEL:NULL.GT_MOCKUP_TABLE.1 LT [\\x00]]", newFilter.toString());
    }

    @Test
    public void verifyConvertFilterConstants3() {
        GTInfo info = table.getInfo();

        TableDesc extTable = TableDesc.mockup("ext");
        TblColRef extColA = TblColRef.mockup(extTable, 1, "A", "timestamp");
        TblColRef extColB = TblColRef.mockup(extTable, 2, "B", "integer");

        CompareTupleFilter fComp1 = compare(extColA, FilterOperatorEnum.GT, "2015-01-14");
        CompareTupleFilter fComp2 = compare(extColB, FilterOperatorEnum.LTE, "9");
        LogicalTupleFilter filter = and(fComp1, fComp2);

        List<TblColRef> colMapping = Lists.newArrayList();
        colMapping.add(extColA);
        colMapping.add(extColB);

        // $1<="9" round down to FALSE
        TupleFilter newFilter = GTUtil.convertFilterColumnsAndConstants(filter, info, colMapping, null);
        assertEquals("AND [UNKNOWN_MODEL:NULL.GT_MOCKUP_TABLE.0 GT [\\x00\\x00\\x01J\\xE5\\xBD\\x5C\\x00], []]", newFilter.toString());
    }

    @Test
    public void verifyConvertFilterConstants4() {
        GTInfo info = table.getInfo();

        TableDesc extTable = TableDesc.mockup("ext");
        TblColRef extColA = TblColRef.mockup(extTable, 1, "A", "timestamp");
        TblColRef extColB = TblColRef.mockup(extTable, 2, "B", "integer");

        CompareTupleFilter fComp1 = compare(extColA, FilterOperatorEnum.GT, "2015-01-14");
        CompareTupleFilter fComp2 = compare(extColB, FilterOperatorEnum.IN, "9", "10", "15");
        LogicalTupleFilter filter = and(fComp1, fComp2);

        List<TblColRef> colMapping = Lists.newArrayList();
        colMapping.add(extColA);
        colMapping.add(extColB);

        // $1 in ("9", "10", "15") has only "10" left
        TupleFilter newFilter = GTUtil.convertFilterColumnsAndConstants(filter, info, colMapping, null);
        assertEquals("AND [UNKNOWN_MODEL:NULL.GT_MOCKUP_TABLE.0 GT [\\x00\\x00\\x01J\\xE5\\xBD\\x5C\\x00], UNKNOWN_MODEL:NULL.GT_MOCKUP_TABLE.1 IN [\\x00]]", newFilter.toString());
    }

    private void doScanAndVerify(GridTable table, GTScanRequest req, String... verifyRows) throws IOException {
        System.out.println(req);
        IGTScanner scanner = table.scan(req);
        int i = 0;
        for (GTRecord r : scanner) {
            System.out.println(r);
            if (verifyRows == null || i >= verifyRows.length) {
                Assert.fail();
            }
            assertEquals(verifyRows[i], r.toString());
            i++;
        }
        scanner.close();
    }

    public static ByteArray enc(GTInfo info, int col, String value) {
        ByteBuffer buf = ByteBuffer.allocate(info.getMaxColumnLength());
        info.getCodeSystem().encodeColumnValue(col, value, buf);
        return ByteArray.copyOf(buf.array(), buf.arrayOffset(), buf.position());
    }

    public static ExtractTupleFilter unevaluatable(TblColRef col) {
        ExtractTupleFilter r = new ExtractTupleFilter(FilterOperatorEnum.EXTRACT);
        r.addChild(new ColumnTupleFilter(col));
        return r;
    }

    public static CompareTupleFilter compare(TblColRef col, FilterOperatorEnum op, Object... value) {
        CompareTupleFilter result = new CompareTupleFilter(op);
        result.addChild(new ColumnTupleFilter(col));
        result.addChild(new ConstantTupleFilter(Arrays.asList(value)));
        return result;
    }

    public static LogicalTupleFilter and(TupleFilter... children) {
        return logic(FilterOperatorEnum.AND, children);
    }

    public static LogicalTupleFilter or(TupleFilter... children) {
        return logic(FilterOperatorEnum.OR, children);
    }

    public static LogicalTupleFilter not(TupleFilter child) {
        return logic(FilterOperatorEnum.NOT, child);
    }

    public static LogicalTupleFilter logic(FilterOperatorEnum op, TupleFilter... children) {
        LogicalTupleFilter result = new LogicalTupleFilter(op);
        for (TupleFilter c : children) {
            result.addChild(c);
        }
        return result;
    }

    public static GridTable newTestTable() throws IOException {
        GTInfo info = newInfo();
        GTSimpleMemStore store = new GTSimpleMemStore(info);
        GridTable table = new GridTable(info, store);

        GTRecord r = new GTRecord(table.getInfo());
        GTBuilder builder = table.rebuild();

        builder.write(r.setValues("2015-01-14", "30", "Yang", new Long(10), new BigDecimal("10.5")));
        builder.write(r.setValues("2015-01-14", "30", "Luke", new Long(10), new BigDecimal("10.5")));
        builder.write(r.setValues("2015-01-15", "20", "Dong", new Long(10), new BigDecimal("10.5")));
        builder.write(r.setValues("2015-01-15", "20", "Jason", new Long(10), new BigDecimal("10.5")));
        builder.write(r.setValues("2015-01-15", "30", "Xu", new Long(10), new BigDecimal("10.5")));
        builder.write(r.setValues("2015-01-16", "20", "Mahone", new Long(10), new BigDecimal("10.5")));
        builder.write(r.setValues("2015-01-16", "20", "Qianhao", new Long(10), new BigDecimal("10.5")));
        builder.write(r.setValues("2015-01-16", "30", "George", new Long(10), new BigDecimal("10.5")));
        builder.write(r.setValues("2015-01-16", "30", "Shaofeng", new Long(10), new BigDecimal("10.5")));
        builder.write(r.setValues("2015-01-17", "10", "Kejia", new Long(10), new BigDecimal("10.5")));
        builder.close();

        return table;
    }

    static GridTable newTestPerfTable() throws IOException {
        GTInfo info = newInfo();
        GTSimpleMemStore store = new GTSimpleMemStore(info);
        GridTable table = new GridTable(info, store);

        GTRecord r = new GTRecord(table.getInfo());
        GTBuilder builder = table.rebuild();

        for (int i = 0; i < 100000; i++) {
            for (int j = 0; j < 10; j++)
                builder.write(r.setValues("2015-01-14", "30", "Yang", new Long(10), new BigDecimal("10.5")));

            for (int j = 0; j < 10; j++)
                builder.write(r.setValues("2015-01-14", "30", "Luke", new Long(10), new BigDecimal("10.5")));

            for (int j = 0; j < 10; j++)
                builder.write(r.setValues("2015-01-15", "20", "Dong", new Long(10), new BigDecimal("10.5")));

            for (int j = 0; j < 10; j++)
                builder.write(r.setValues("2015-01-15", "20", "Jason", new Long(10), new BigDecimal("10.5")));

            for (int j = 0; j < 10; j++)
                builder.write(r.setValues("2015-01-15", "30", "Xu", new Long(10), new BigDecimal("10.5")));

            for (int j = 0; j < 10; j++)
                builder.write(r.setValues("2015-01-16", "20", "Mahone", new Long(10), new BigDecimal("10.5")));

            for (int j = 0; j < 10; j++)
                builder.write(r.setValues("2015-01-16", "20", "Qianhao", new Long(10), new BigDecimal("10.5")));

            for (int j = 0; j < 10; j++)
                builder.write(r.setValues("2015-01-16", "30", "George", new Long(10), new BigDecimal("10.5")));

            for (int j = 0; j < 10; j++)
                builder.write(r.setValues("2015-01-16", "30", "Shaofeng", new Long(10), new BigDecimal("10.5")));

            for (int j = 0; j < 10; j++)
                builder.write(r.setValues("2015-01-17", "10", "Kejia", new Long(10), new BigDecimal("10.5")));
        }
        builder.close();

        return table;
    }

    static GTInfo newInfo() {
        Builder builder = GTInfo.builder();
        builder.setCodeSystem(newDictCodeSystem());
        builder.setColumns(//
                DataType.getType("timestamp"), //
                DataType.getType("integer"), //
                DataType.getType("varchar(10)"), //
                DataType.getType("bigint"), //
                DataType.getType("decimal") //
        );
        builder.setPrimaryKey(setOf(0, 1));
        builder.setColumnPreferIndex(setOf(0));
        builder.enableColumnBlock(new ImmutableBitSet[] { setOf(0, 1), setOf(2), setOf(3, 4) });
        builder.enableRowBlock(4);
        GTInfo info = builder.build();
        return info;
    }

    @SuppressWarnings("unchecked")
    private static CubeCodeSystem newDictCodeSystem() {
        DimensionEncoding[] dimEncs = new DimensionEncoding[3];
        dimEncs[1] = new DictionaryDimEnc(newDictionaryOfInteger());
        dimEncs[2] = new DictionaryDimEnc(newDictionaryOfString());
        return new CubeCodeSystem(dimEncs);
    }

    @SuppressWarnings("rawtypes")
    private static Dictionary newDictionaryOfString() {
        TrieDictionaryBuilder<String> builder = new TrieDictionaryBuilder<>(new StringBytesConverter());
        builder.addValue("Dong");
        builder.addValue("George");
        builder.addValue("Jason");
        builder.addValue("Kejia");
        builder.addValue("Luke");
        builder.addValue("Mahone");
        builder.addValue("Qianhao");
        builder.addValue("Shaofeng");
        builder.addValue("Xu");
        builder.addValue("Yang");
        return builder.build(0);
    }

    @SuppressWarnings("rawtypes")
    private static Dictionary newDictionaryOfInteger() {
        NumberDictionaryBuilder builder = new NumberDictionaryBuilder();
        builder.addValue("10");
        builder.addValue("20");
        builder.addValue("30");
        builder.addValue("40");
        builder.addValue("50");
        builder.addValue("60");
        builder.addValue("70");
        builder.addValue("80");
        builder.addValue("90");
        builder.addValue("100");
        return builder.build(0);
    }

    public static ImmutableBitSet setOf(int... values) {
        BitSet set = new BitSet();
        for (int i : values)
            set.set(i);
        return new ImmutableBitSet(set);
    }
}

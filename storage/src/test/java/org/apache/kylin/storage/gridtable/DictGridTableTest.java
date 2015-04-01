package org.apache.kylin.storage.gridtable;

import static org.junit.Assert.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.dict.NumberDictionaryBuilder;
import org.apache.kylin.dict.StringBytesConverter;
import org.apache.kylin.dict.TrieDictionaryBuilder;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.ExtractTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter.FilterOperatorEnum;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.DataType;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.gridtable.GTInfo.Builder;
import org.apache.kylin.storage.gridtable.memstore.GTSimpleMemStore;
import org.junit.Test;

import com.google.common.collect.Maps;

public class DictGridTableTest {

    @Test
    public void test() throws IOException {
        GridTable table = newTestTable();
        verifyScanRangePlanner(table);
        verifyFirstRow(table);
        verifyScanWithUnevaluatableFilter(table);
        verifyScanWithEvaluatableFilter(table);
        verifyConvertFilterConstants1(table);
        verifyConvertFilterConstants2(table);
        verifyConvertFilterConstants3(table);
        verifyConvertFilterConstants4(table);
    }

    private void verifyScanRangePlanner(GridTable table) {
        GTInfo info = table.getInfo();
        GTScanRangePlanner planner = new GTScanRangePlanner(info);
        
        CompareTupleFilter timeComp1 = compare(info.colRef(0), FilterOperatorEnum.GT, enc(info, 0, "2015-01-14"));
        CompareTupleFilter timeComp2 = compare(info.colRef(0), FilterOperatorEnum.LT, enc(info, 0, "2015-01-13"));
        CompareTupleFilter timeComp3 = compare(info.colRef(0), FilterOperatorEnum.LT, enc(info, 0, "2015-01-15"));
        CompareTupleFilter timeComp4 = compare(info.colRef(0), FilterOperatorEnum.EQ, enc(info, 0, "2015-01-15"));
        CompareTupleFilter ageComp1 = compare(info.colRef(1), FilterOperatorEnum.EQ, enc(info, 1, "10"));
        CompareTupleFilter ageComp2 = compare(info.colRef(1), FilterOperatorEnum.EQ, enc(info, 1, "20"));
        CompareTupleFilter ageComp3 = compare(info.colRef(1), FilterOperatorEnum.EQ, enc(info, 1, "30"));
        CompareTupleFilter ageComp4 = compare(info.colRef(1), FilterOperatorEnum.NEQ, enc(info, 1, "30"));
        
        // flatten or-and & hbase fuzzy value
        {
            LogicalTupleFilter filter = and(timeComp1, or(ageComp1, ageComp2));
            List<GTScanRange> r = planner.planScanRanges(filter);
            assertEquals(1, r.size());
            assertEquals("[1421193600000, 10]-[null, null]", r.get(0).toString());
            assertEquals("[[null, 10, null, null, null], [null, 20, null, null, null]]", r.get(0).hbaseFuzzyKeys.toString());
        }
        
        // pre-evaluate ever false
        {
            LogicalTupleFilter filter = and(timeComp1, timeComp2);
            List<GTScanRange> r = planner.planScanRanges(filter);
            assertEquals(0, r.size());
        }
        
        // pre-evaluate ever true
        {
            LogicalTupleFilter filter = or(timeComp1, ageComp4);
            List<GTScanRange> r = planner.planScanRanges(filter);
            assertEquals("[[null, null]-[null, null]]", r.toString());
        }
        
        // merge overlap range
        {
            LogicalTupleFilter filter = or(timeComp1, timeComp3);
            List<GTScanRange> r = planner.planScanRanges(filter);
            assertEquals("[[null, null]-[null, null]]", r.toString());
        }
        
        // merge too many ranges
        {
            LogicalTupleFilter filter = or(and(timeComp4, ageComp1), and(timeComp4, ageComp2), and(timeComp4, ageComp3));
            List<GTScanRange> r = planner.planScanRanges(filter);
            assertEquals(3, r.size());
            assertEquals("[1421280000000, 10]-[1421280000000, 10]", r.get(0).toString());
            assertEquals("[1421280000000, 20]-[1421280000000, 20]", r.get(1).toString());
            assertEquals("[1421280000000, 30]-[1421280000000, 30]", r.get(2).toString());
            List<GTScanRange> r2 = planner.planScanRanges(filter, 2);
            assertEquals("[[1421280000000, 10]-[1421280000000, 30]]", r2.toString());
        }
    }

    private void verifyFirstRow(GridTable table) throws IOException {
        doScanAndVerify(table, new GTScanRequest(table.getInfo()), "[1421193600000, 30, Yang, 10, 10.5]");
    }

    private void verifyScanWithUnevaluatableFilter(GridTable table) throws IOException {
        GTInfo info = table.getInfo();

        CompareTupleFilter fComp = compare(info.colRef(0), FilterOperatorEnum.GT, enc(info, 0, "2015-01-14"));
        ExtractTupleFilter fUnevaluatable = unevaluatable(info.colRef(1));
        LogicalTupleFilter fNotPlusUnevaluatable = not(unevaluatable(info.colRef(1)));
        LogicalTupleFilter filter = and(fComp, fUnevaluatable, fNotPlusUnevaluatable);

        GTScanRequest req = new GTScanRequest(info, null, setOf(0), setOf(3), new String[] { "sum" }, filter);

        // note the unEvaluatable column 1 in filter is added to group by
        assertEquals("GTScanRequest [range=[null, null]-[null, null], columns={0, 1, 3}, filterPushDown=AND [NULL.GT_MOCKUP_TABLE.0 GT [\\x00\\x00\\x01J\\xE5\\xBD\\x5C\\x00], [null], [null]], aggrGroupBy={0, 1}, aggrMetrics={3}, aggrMetricsFuncs=[sum]]", req.toString());
        
        doScanAndVerify(table, req, "[1421280000000, 20, null, 20, null]");
    }
    
    private void verifyScanWithEvaluatableFilter(GridTable table) throws IOException {
        GTInfo info = table.getInfo();

        CompareTupleFilter fComp1 = compare(info.colRef(0), FilterOperatorEnum.GT, enc(info, 0, "2015-01-14"));
        CompareTupleFilter fComp2 = compare(info.colRef(1), FilterOperatorEnum.GT, enc(info, 1, "10"));
        LogicalTupleFilter filter = and(fComp1, fComp2);

        GTScanRequest req = new GTScanRequest(info, null, setOf(0), setOf(3), new String[] { "sum" }, filter);
        
        // note the evaluatable column 1 in filter is added to returned columns but not in group by
        assertEquals("GTScanRequest [range=[null, null]-[null, null], columns={0, 1, 3}, filterPushDown=AND [NULL.GT_MOCKUP_TABLE.0 GT [\\x00\\x00\\x01J\\xE5\\xBD\\x5C\\x00], NULL.GT_MOCKUP_TABLE.1 GT [\\x00]], aggrGroupBy={0}, aggrMetrics={3}, aggrMetricsFuncs=[sum]]", req.toString());
        
        doScanAndVerify(table, req, "[1421280000000, 20, null, 30, null]", "[1421366400000, 20, null, 40, null]");
    }

    private void verifyConvertFilterConstants1(GridTable table) {
        GTInfo info = table.getInfo();
        
        TableDesc extTable = TableDesc.mockup("ext");
        TblColRef extColA = new TblColRef(ColumnDesc.mockup(extTable, 1, "A", "timestamp"));
        TblColRef extColB = new TblColRef(ColumnDesc.mockup(extTable, 2, "B", "integer"));

        CompareTupleFilter fComp1 = compare(extColA, FilterOperatorEnum.GT, "2015-01-14");
        CompareTupleFilter fComp2 = compare(extColB, FilterOperatorEnum.EQ, "10");
        LogicalTupleFilter filter = and(fComp1, fComp2);
        
        Map<TblColRef, Integer> colMapping = Maps.newHashMap();
        colMapping.put(extColA, 0);
        colMapping.put(extColB, 1);
        
        TupleFilter newFilter = GTUtil.convertFilterColumnsAndConstants(filter, info, colMapping, null);
        assertEquals("AND [NULL.GT_MOCKUP_TABLE.0 GT [\\x00\\x00\\x01J\\xE5\\xBD\\x5C\\x00], NULL.GT_MOCKUP_TABLE.1 EQ [\\x00]]", newFilter.toString());
    }

    private void verifyConvertFilterConstants2(GridTable table) {
        GTInfo info = table.getInfo();
        
        TableDesc extTable = TableDesc.mockup("ext");
        TblColRef extColA = new TblColRef(ColumnDesc.mockup(extTable, 1, "A", "timestamp"));
        TblColRef extColB = new TblColRef(ColumnDesc.mockup(extTable, 2, "B", "integer"));
        
        CompareTupleFilter fComp1 = compare(extColA, FilterOperatorEnum.GT, "2015-01-14");
        CompareTupleFilter fComp2 = compare(extColB, FilterOperatorEnum.LT, "9");
        LogicalTupleFilter filter = and(fComp1, fComp2);
        
        Map<TblColRef, Integer> colMapping = Maps.newHashMap();
        colMapping.put(extColA, 0);
        colMapping.put(extColB, 1);
        
        // $1<"9" round up to $1<"10"
        TupleFilter newFilter = GTUtil.convertFilterColumnsAndConstants(filter, info, colMapping, null);
        assertEquals("AND [NULL.GT_MOCKUP_TABLE.0 GT [\\x00\\x00\\x01J\\xE5\\xBD\\x5C\\x00], NULL.GT_MOCKUP_TABLE.1 LT [\\x00]]", newFilter.toString());
    }
    
    private void verifyConvertFilterConstants3(GridTable table) {
        GTInfo info = table.getInfo();
        
        TableDesc extTable = TableDesc.mockup("ext");
        TblColRef extColA = new TblColRef(ColumnDesc.mockup(extTable, 1, "A", "timestamp"));
        TblColRef extColB = new TblColRef(ColumnDesc.mockup(extTable, 2, "B", "integer"));
        
        CompareTupleFilter fComp1 = compare(extColA, FilterOperatorEnum.GT, "2015-01-14");
        CompareTupleFilter fComp2 = compare(extColB, FilterOperatorEnum.LTE, "9");
        LogicalTupleFilter filter = and(fComp1, fComp2);
        
        Map<TblColRef, Integer> colMapping = Maps.newHashMap();
        colMapping.put(extColA, 0);
        colMapping.put(extColB, 1);
        
        // $1<="9" round down to FALSE
        TupleFilter newFilter = GTUtil.convertFilterColumnsAndConstants(filter, info, colMapping, null);
        assertEquals("AND [NULL.GT_MOCKUP_TABLE.0 GT [\\x00\\x00\\x01J\\xE5\\xBD\\x5C\\x00], []]", newFilter.toString());
    }
    
    private void verifyConvertFilterConstants4(GridTable table) {
        GTInfo info = table.getInfo();
        
        TableDesc extTable = TableDesc.mockup("ext");
        TblColRef extColA = new TblColRef(ColumnDesc.mockup(extTable, 1, "A", "timestamp"));
        TblColRef extColB = new TblColRef(ColumnDesc.mockup(extTable, 2, "B", "integer"));
        
        CompareTupleFilter fComp1 = compare(extColA, FilterOperatorEnum.GT, "2015-01-14");
        CompareTupleFilter fComp2 = compare(extColB, FilterOperatorEnum.IN, "9", "10", "15");
        LogicalTupleFilter filter = and(fComp1, fComp2);
        
        Map<TblColRef, Integer> colMapping = Maps.newHashMap();
        colMapping.put(extColA, 0);
        colMapping.put(extColB, 1);
        
        // $1 in ("9", "10", "15") has only "10" left
        TupleFilter newFilter = GTUtil.convertFilterColumnsAndConstants(filter, info, colMapping, null);
        assertEquals("AND [NULL.GT_MOCKUP_TABLE.0 GT [\\x00\\x00\\x01J\\xE5\\xBD\\x5C\\x00], NULL.GT_MOCKUP_TABLE.1 IN [\\x00]]", newFilter.toString());
    }
    
    private void doScanAndVerify(GridTable table, GTScanRequest req, String... verifyRows) throws IOException {
        System.out.println(req);
        IGTScanner scanner = table.scan(req);
        int i = 0;
        for (GTRecord r : scanner) {
            System.out.println(r);
            if (verifyRows != null && i < verifyRows.length) {
                assertEquals(verifyRows[i], r.toString());
            }
            i++;
        }
        scanner.close();
    }

    private Object enc(GTInfo info, int col, String value) {
        ByteBuffer buf = ByteBuffer.allocate(info.maxRecordLength);
        info.codeSystem.encodeColumnValue(col, value, buf);
        return ByteArray.copyOf(buf.array(), buf.arrayOffset(), buf.position());
    }

    private ExtractTupleFilter unevaluatable(TblColRef col) {
        ExtractTupleFilter r = new ExtractTupleFilter(FilterOperatorEnum.EXTRACT);
        r.addChild(new ColumnTupleFilter(col));
        return r;
    }

    private CompareTupleFilter compare(TblColRef col, FilterOperatorEnum op, Object... value) {
        CompareTupleFilter result = new CompareTupleFilter(op);
        result.addChild(new ColumnTupleFilter(col));
        result.addChild(new ConstantTupleFilter(Arrays.asList(value)));
        return result;
    }

    private LogicalTupleFilter and(TupleFilter... children) {
        return logic(FilterOperatorEnum.AND, children);
    }

    private LogicalTupleFilter or(TupleFilter... children) {
        return logic(FilterOperatorEnum.OR, children);
    }

    private LogicalTupleFilter not(TupleFilter child) {
        return logic(FilterOperatorEnum.NOT, child);
    }

    private LogicalTupleFilter logic(FilterOperatorEnum op, TupleFilter... children) {
        LogicalTupleFilter result = new LogicalTupleFilter(op);
        for (TupleFilter c : children) {
            result.addChild(c);
        }
        return result;
    }

    static GridTable newTestTable() throws IOException {
        GTInfo info = newInfo();
        GTSimpleMemStore store = new GTSimpleMemStore(info);
        GridTable table = new GridTable(info, store);

        GTRecord r = new GTRecord(table.getInfo());
        GTBuilder builder = table.rebuild();

        builder.write(r.setValues("2015-01-14", "30", "Yang", new LongWritable(10), new BigDecimal("10.5")));
        builder.write(r.setValues("2015-01-14", "30", "Luke", new LongWritable(10), new BigDecimal("10.5")));
        builder.write(r.setValues("2015-01-15", "20", "Dong", new LongWritable(10), new BigDecimal("10.5")));
        builder.write(r.setValues("2015-01-15", "20", "Jason", new LongWritable(10), new BigDecimal("10.5")));
        builder.write(r.setValues("2015-01-15", "30", "Xu", new LongWritable(10), new BigDecimal("10.5")));
        builder.write(r.setValues("2015-01-16", "20", "Mahone", new LongWritable(10), new BigDecimal("10.5")));
        builder.write(r.setValues("2015-01-16", "20", "Qianhao", new LongWritable(10), new BigDecimal("10.5")));
        builder.write(r.setValues("2015-01-16", "30", "George", new LongWritable(10), new BigDecimal("10.5")));
        builder.write(r.setValues("2015-01-16", "30", "Shaofeng", new LongWritable(10), new BigDecimal("10.5")));
        builder.write(r.setValues("2015-01-17", "10", "Kejia", new LongWritable(10), new BigDecimal("10.5")));
        builder.close();

        return table;
    }

    static GTInfo newInfo() {
        Builder builder = GTInfo.builder();
        builder.setCodeSystem(newDictCodeSystem());
        builder.setColumns( //
                DataType.getInstance("timestamp"), //
                DataType.getInstance("integer"), //
                DataType.getInstance("varchar"), //
                DataType.getInstance("bigint"), //
                DataType.getInstance("decimal") //
        );
        builder.setPrimaryKey(setOf(0, 1));
        builder.setColumnPreferIndex(setOf(0));
        builder.enableColumnBlock(new BitSet[] { setOf(0, 1, 2), setOf(3, 4) });
        builder.enableRowBlock(4);
        GTInfo info = builder.build();
        return info;
    }

    @SuppressWarnings("rawtypes")
    private static GTDictionaryCodeSystem newDictCodeSystem() {
        Map<Integer, Dictionary> dictionaryMap = Maps.newHashMap();
        dictionaryMap.put(1, newDictionaryOfInteger());
        dictionaryMap.put(2, newDictionaryOfString());
        return new GTDictionaryCodeSystem(dictionaryMap);
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
        NumberDictionaryBuilder<String> builder = new NumberDictionaryBuilder<>(new StringBytesConverter());
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

    private static BitSet setOf(int... values) {
        BitSet set = new BitSet();
        for (int i : values)
            set.set(i);
        return set;
    }
}

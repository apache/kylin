package org.apache.kylin.storage.gridtable;

import static org.junit.Assert.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.BitSet;
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
        verifyFirstRow(table);
        verifyScanWithUnevaluatableFilter(table);
        verifyScanWithEvaluatableFilter(table);
        verifyConvertFilterConstants(table);
    }

    private void verifyFirstRow(GridTable table) throws IOException {
        doScanAndVerify(table, new GTScanRequest(table.getInfo()), "[1421193600000, 30, Yang, 10, 10.5]");
    }

    private void verifyScanWithUnevaluatableFilter(GridTable table) throws IOException {
        GTInfo info = table.getInfo();

        CompareTupleFilter fcomp = compare(info.colRef(0), FilterOperatorEnum.GT, enc(info, 0, "2015-01-14"));
        ExtractTupleFilter funevaluatable = unevaluatable(info.colRef(1));
        LogicalTupleFilter filter = and(fcomp, funevaluatable);

        GTScanRequest req = new GTScanRequest(info, null, setOf(0), setOf(3), new String[] { "sum" }, filter);

        // note the unEvaluatable column 1 in filter is added to group by
        assertEquals("GTScanRequest [range=null-null, columns={0, 1, 3}, filterPushDown=AND [NULL.GT_MOCKUP_TABLE.0 GT [\\x00\\x00\\x01J\\xE5\\xBD\\x5C\\x00], [null]], aggrGroupBy={0, 1}, aggrMetrics={3}, aggrMetricsFuncs=[sum]]", req.toString());
        
        doScanAndVerify(table, req, "[1421280000000, 20, null, 20, null]");
    }
    
    private void verifyScanWithEvaluatableFilter(GridTable table) throws IOException {
        GTInfo info = table.getInfo();

        CompareTupleFilter fcomp1 = compare(info.colRef(0), FilterOperatorEnum.GT, enc(info, 0, "2015-01-14"));
        CompareTupleFilter fcomp2 = compare(info.colRef(1), FilterOperatorEnum.GT, enc(info, 1, "10"));
        LogicalTupleFilter filter = and(fcomp1, fcomp2);

        GTScanRequest req = new GTScanRequest(info, null, setOf(0), setOf(3), new String[] { "sum" }, filter);
        
        // note the evaluatable column 1 in filter is added to returned columns but not in group by
        assertEquals("GTScanRequest [range=null-null, columns={0, 1, 3}, filterPushDown=AND [NULL.GT_MOCKUP_TABLE.0 GT [\\x00\\x00\\x01J\\xE5\\xBD\\x5C\\x00], NULL.GT_MOCKUP_TABLE.1 GT [\\x00]], aggrGroupBy={0}, aggrMetrics={3}, aggrMetricsFuncs=[sum]]", req.toString());
        
        doScanAndVerify(table, req, "[1421280000000, 30, null, 30, null]", "[1421366400000, 20, null, 40, null]");
    }

    private void verifyConvertFilterConstants(GridTable table) {
        GTInfo info = table.getInfo();
        
        TableDesc extTable = TableDesc.mockup("ext");
        TblColRef extColA = new TblColRef(ColumnDesc.mockup(extTable, 1, "A", "timestamp"));
        TblColRef extColB = new TblColRef(ColumnDesc.mockup(extTable, 2, "B", "integer"));

        CompareTupleFilter fcomp1 = compare(extColA, FilterOperatorEnum.GT, "2015-01-14");
        CompareTupleFilter fcomp2 = compare(extColB, FilterOperatorEnum.EQ, "10");
        LogicalTupleFilter filter = and(fcomp1, fcomp2);
        
        Map<TblColRef, Integer> colMapping = Maps.newHashMap();
        colMapping.put(extColA, 0);
        colMapping.put(extColB, 1);
        
        TupleFilter newFilter = GTUtil.convertFilterColumnsAndConstants(filter, info, colMapping, null);
        assertEquals("AND [NULL.GT_MOCKUP_TABLE.0 GT [\\x00\\x00\\x01J\\xE5\\xBD\\x5C\\x00], NULL.GT_MOCKUP_TABLE.1 EQ [\\x00]]", newFilter.toString());
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

    private CompareTupleFilter compare(TblColRef col, FilterOperatorEnum op, Object value) {
        CompareTupleFilter result = new CompareTupleFilter(op);
        result.addChild(new ColumnTupleFilter(col));
        result.addChild(new ConstantTupleFilter(value));
        return result;
    }

    private LogicalTupleFilter and(TupleFilter... children) {
        return logic(FilterOperatorEnum.AND, children);
    }

    private LogicalTupleFilter or(TupleFilter... children) {
        return logic(FilterOperatorEnum.AND, children);
    }

    private LogicalTupleFilter not(TupleFilter child) {
        return logic(FilterOperatorEnum.AND, child);
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
        builder.write(r.setValues("2015-01-15", "30", "Xu", new LongWritable(10), new BigDecimal("10.5")));
        builder.write(r.setValues("2015-01-15", "20", "Dong", new LongWritable(10), new BigDecimal("10.5")));
        builder.write(r.setValues("2015-01-15", "20", "Jason", new LongWritable(10), new BigDecimal("10.5")));
        builder.write(r.setValues("2015-01-16", "20", "Mahone", new LongWritable(10), new BigDecimal("10.5")));
        builder.write(r.setValues("2015-01-16", "30", "Shaofeng", new LongWritable(10), new BigDecimal("10.5")));
        builder.write(r.setValues("2015-01-16", "20", "Qianhao", new LongWritable(10), new BigDecimal("10.5")));
        builder.write(r.setValues("2015-01-16", "30", "George", new LongWritable(10), new BigDecimal("10.5")));
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
        builder.setPrimaryKey(setOf(0));
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

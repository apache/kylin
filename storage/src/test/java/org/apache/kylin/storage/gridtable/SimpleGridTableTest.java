/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements. See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.kylin.storage.gridtable;

import static org.junit.Assert.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.metadata.model.DataType;
import org.apache.kylin.storage.gridtable.GTInfo.Builder;
import org.apache.kylin.storage.gridtable.memstore.GTSimpleMemStore;
import org.junit.Test;

public class SimpleGridTableTest {

    @Test
    public void testBasics() throws IOException {
        GTInfo info = basicInfo();
        GTSimpleMemStore store = new GTSimpleMemStore(info);
        GridTable table = new GridTable(info, store);

        GTBuilder builder = rebuild(table);
        IGTScanner scanner = scan(table);
        assertEquals(builder.getWrittenRowBlockCount(), scanner.getScannedRowBlockCount());
        assertEquals(builder.getWrittenRowCount(), scanner.getScannedRowCount());
    }

    @Test
    public void testAdvanced() throws IOException {
        GTInfo info = advancedInfo();
        GTSimpleMemStore store = new GTSimpleMemStore(info);
        GridTable table = new GridTable(info, store);

        GTBuilder builder = rebuild(table);
        IGTScanner scanner = scan(table);
        assertEquals(builder.getWrittenRowBlockCount(), scanner.getScannedRowBlockCount());
        assertEquals(builder.getWrittenRowCount(), scanner.getScannedRowCount());
    }

    @Test
    public void testAggregate() throws IOException {
        GTInfo info = advancedInfo();
        GTSimpleMemStore store = new GTSimpleMemStore(info);
        GridTable table = new GridTable(info, store);

        GTBuilder builder = rebuild(table);
        IGTScanner scanner = scanAndAggregate(table);
        assertEquals(builder.getWrittenRowBlockCount(), scanner.getScannedRowBlockCount());
        assertEquals(builder.getWrittenRowCount(), scanner.getScannedRowCount());
    }

    @Test
    public void testAppend() throws IOException {
        GTInfo info = advancedInfo();
        GTSimpleMemStore store = new GTSimpleMemStore(info);
        GridTable table = new GridTable(info, store);

        rebuildViaAppend(table);
        IGTScanner scanner = scan(table);
        assertEquals(3, scanner.getScannedRowBlockCount());
        assertEquals(10, scanner.getScannedRowCount());
    }

    private IGTScanner scan(GridTable table) throws IOException {
        GTScanRequest req = new GTScanRequest(table.getInfo());
        IGTScanner scanner = table.scan(req);
        for (GTRecord r : scanner) {
            Object[] v = r.getValues();
            assertTrue(((String) v[0]).startsWith("2015-"));
            assertTrue(((String) v[2]).equals("Food"));
            assertTrue(((LongWritable) v[3]).get() == 10);
            assertTrue(((BigDecimal) v[4]).doubleValue() == 10.5);
            System.out.println(r);
        }
        scanner.close();
        System.out.println("Scanned Row Block Count: " + scanner.getScannedRowBlockCount());
        System.out.println("Scanned Row Count: " + scanner.getScannedRowCount());
        return scanner;
    }

    private IGTScanner scanAndAggregate(GridTable table) throws IOException {
        GTScanRequest req = new GTScanRequest(table.getInfo(), null, setOf(0, 2), setOf(3, 4), new String[] { "count", "sum" }, null);
        IGTScanner scanner = table.scan(req);
        int i = 0;
        for (GTRecord r : scanner) {
            Object[] v = r.getValues();
            switch (i) {
            case 0:
                assertTrue(((LongWritable) v[3]).get() == 20);
                assertTrue(((BigDecimal) v[4]).doubleValue() == 21.0);
                break;
            case 1:
                assertTrue(((LongWritable) v[3]).get() == 30);
                assertTrue(((BigDecimal) v[4]).doubleValue() == 31.5);
                break;
            case 2:
                assertTrue(((LongWritable) v[3]).get() == 40);
                assertTrue(((BigDecimal) v[4]).doubleValue() == 42.0);
                break;
            case 3:
                assertTrue(((LongWritable) v[3]).get() == 10);
                assertTrue(((BigDecimal) v[4]).doubleValue() == 10.5);
                break;
            default:
                fail();
            }
            i++;
            System.out.println(r);
        }
        scanner.close();
        System.out.println("Scanned Row Block Count: " + scanner.getScannedRowBlockCount());
        System.out.println("Scanned Row Count: " + scanner.getScannedRowCount());
        return scanner;
    }

    static GTBuilder rebuild(GridTable table) throws IOException {
        GTBuilder builder = table.rebuild();
        for (GTRecord rec : mockupData(table.getInfo(), 10)) {
            builder.write(rec);
        }
        builder.close();

        System.out.println("Written Row Block Count: " + builder.getWrittenRowBlockCount());
        System.out.println("Written Row Count: " + builder.getWrittenRowCount());
        return builder;
    }
    
    public static List<GTRecord> mockupData(GTInfo info, int nRows) {
        List<GTRecord> result = new ArrayList<GTRecord>(nRows);
        int round = nRows / 10;
        for (int i = 0; i < round; i++) {
            String d_01_14 = datePlus("2015-01-14", i * 4);
            String d_01_15 = datePlus("2015-01-15", i * 4);
            String d_01_16 = datePlus("2015-01-16", i * 4);
            String d_01_17 = datePlus("2015-01-17", i * 4);
            result.add(newRec(info, d_01_14, "Yang", "Food", new LongWritable(10), new BigDecimal("10.5")));
            result.add(newRec(info, d_01_14, "Luke", "Food", new LongWritable(10), new BigDecimal("10.5")));
            result.add(newRec(info, d_01_15, "Xu", "Food", new LongWritable(10), new BigDecimal("10.5")));
            result.add(newRec(info, d_01_15, "Dong", "Food", new LongWritable(10), new BigDecimal("10.5")));
            result.add(newRec(info, d_01_15, "Jason", "Food", new LongWritable(10), new BigDecimal("10.5")));
            result.add(newRec(info, d_01_16, "Mahone", "Food", new LongWritable(10), new BigDecimal("10.5")));
            result.add(newRec(info, d_01_16, "Shaofeng", "Food", new LongWritable(10), new BigDecimal("10.5")));
            result.add(newRec(info, d_01_16, "Qianhao", "Food", new LongWritable(10), new BigDecimal("10.5")));
            result.add(newRec(info, d_01_16, "George", "Food", new LongWritable(10), new BigDecimal("10.5")));
            result.add(newRec(info, d_01_17, "Kejia", "Food", new LongWritable(10), new BigDecimal("10.5")));
        }
        return result;
    }
    
    private static String datePlus(String date, int plusDays) {
        long millis = DateFormat.stringToMillis(date);
        millis += (1000L * 3600L * 24L) * plusDays;
        return DateFormat.formatToDateStr(millis);
    }

    private static GTRecord newRec(GTInfo info, String date, String name, String category, LongWritable amount, BigDecimal price) {
        GTRecord rec = new GTRecord(info);
        return rec.setValues(date, name, category, amount, price);
    }

    static void rebuildViaAppend(GridTable table) throws IOException {
        List<GTRecord> data = mockupData(table.getInfo(), 10);
        GTBuilder builder;
        int i = 0;

        builder = table.append();
        builder.write(data.get(i++));
        builder.write(data.get(i++));
        builder.write(data.get(i++));
        builder.write(data.get(i++));
        builder.close();
        System.out.println("Written Row Block Count: " + builder.getWrittenRowBlockCount());
        System.out.println("Written Row Count: " + builder.getWrittenRowCount());

        builder = table.append();
        builder.write(data.get(i++));
        builder.write(data.get(i++));
        builder.write(data.get(i++));
        builder.close();
        System.out.println("Written Row Block Count: " + builder.getWrittenRowBlockCount());
        System.out.println("Written Row Count: " + builder.getWrittenRowCount());

        builder = table.append();
        builder.write(data.get(i++));
        builder.write(data.get(i++));
        builder.close();
        System.out.println("Written Row Block Count: " + builder.getWrittenRowBlockCount());
        System.out.println("Written Row Count: " + builder.getWrittenRowCount());

        builder = table.append();
        builder.write(data.get(i++));
        builder.close();
        System.out.println("Written Row Block Count: " + builder.getWrittenRowBlockCount());
        System.out.println("Written Row Count: " + builder.getWrittenRowCount());
    }

    public static GTInfo basicInfo() {
        Builder builder = infoBuilder();
        GTInfo info = builder.build();
        return info;
    }

    public static GTInfo advancedInfo() {
        Builder builder = infoBuilder();
        builder.enableColumnBlock(new ImmutableBitSet[] { setOf(0), setOf(1, 2), setOf(3, 4) });
        builder.enableRowBlock(4);
        GTInfo info = builder.build();
        return info;
    }

    private static Builder infoBuilder() {
        Builder builder = GTInfo.builder();
        builder.setCodeSystem(new GTSampleCodeSystem());
        builder.setColumns( //
                DataType.getInstance("varchar(10)"), //
                DataType.getInstance("varchar(10)"), //
                DataType.getInstance("varchar(10)"), //
                DataType.getInstance("bigint"), //
                DataType.getInstance("decimal") //
        );
        builder.setPrimaryKey(setOf(0));
        builder.setColumnPreferIndex(setOf(0));
        return builder;
    }

    private static ImmutableBitSet setOf(int... values) {
        BitSet set = new BitSet();
        for (int i : values)
            set.set(i);
        return new ImmutableBitSet(set);
    }
}

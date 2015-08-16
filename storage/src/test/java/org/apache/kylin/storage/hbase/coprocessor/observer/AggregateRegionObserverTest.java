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

package org.apache.kylin.storage.hbase.coprocessor.observer;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.io.LongWritable;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.hbase.coprocessor.CoprocessorFilter;
import org.apache.kylin.storage.hbase.coprocessor.CoprocessorProjector;
import org.apache.kylin.storage.hbase.coprocessor.CoprocessorRowType;
import org.apache.kylin.storage.hbase.coprocessor.observer.ObserverAggregators.HCol;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * @author yangli9
 */
public class AggregateRegionObserverTest {
    ByteBuffer buf = ByteBuffer.allocate(RowConstants.ROWVALUE_BUFFER_SIZE);

    byte[] mask = new byte[] { (byte) 0xff, (byte) 0xff, 0, 0 };
    byte[] k1 = new byte[] { 0x01, 0x01, 0, 0x01 };
    byte[] k2 = new byte[] { 0x01, 0x01, 0, 0x02 };
    byte[] k3 = new byte[] { 0x02, 0x02, 0, 0x03 };
    byte[] k4 = new byte[] { 0x02, 0x02, 0, 0x04 };

    ArrayList<Cell> cellsInput = Lists.newArrayList();

    byte[] family = Bytes.toBytes("f");
    byte[] q1 = Bytes.toBytes("q1");
    byte[] q2 = Bytes.toBytes("q2");

    HCol c1 = new HCol(family, q1, new String[] { "SUM", "COUNT" }, new String[] { "decimal", "long" });
    HCol c2 = new HCol(family, q2, new String[] { "SUM" }, new String[] { "decimal" });

    @Before
    public void setup() {
        cellsInput.add(newCell(k1, c1, "10.5", 1));
        cellsInput.add(newCell(k2, c1, "11.5", 2));
        cellsInput.add(newCell(k3, c1, "12.5", 3));
        cellsInput.add(newCell(k4, c1, "13.5", 4));

        cellsInput.add(newCell(k1, c2, "21.5"));
        cellsInput.add(newCell(k2, c2, "22.5"));
        cellsInput.add(newCell(k3, c2, "23.5"));
        cellsInput.add(newCell(k4, c2, "24.5"));

    }

    private Cell newCell(byte[] key, HCol col, String decimal) {
        return newCell(key, col, decimal, Integer.MIN_VALUE);
    }

    private Cell newCell(byte[] key, HCol col, String decimal, int number) {
        Object[] values = number == Integer.MIN_VALUE ? //
        new Object[] { new BigDecimal(decimal) } //
                : new Object[] { new BigDecimal(decimal), new LongWritable(number) };
        buf.clear();
        col.measureCodec.encode(values, buf);

        Cell keyValue = new KeyValue(key, 0, key.length, //
                col.family, 0, col.family.length, //
                col.qualifier, 0, col.qualifier.length, //
                HConstants.LATEST_TIMESTAMP, Type.Put, //
                buf.array(), 0, buf.position());

        return keyValue;
    }

    @Test
    public void test() throws IOException {

        CoprocessorRowType rowType = newRowType();
        CoprocessorProjector projector = new CoprocessorProjector(mask);
        ObserverAggregators aggregators = new ObserverAggregators(new HCol[] { c1, c2 });
        CoprocessorFilter filter = CoprocessorFilter.deserialize(null); // a default,
        // always-true,
        // filter
        HashSet<String> expectedResult = new HashSet<String>();

        expectedResult.add("\\x02\\x02\\x00\\x00, f:q1, [26.0, 7]");
        expectedResult.add("\\x02\\x02\\x00\\x00, f:q2, [48.0]");
        expectedResult.add("\\x01\\x01\\x00\\x00, f:q1, [22.0, 3]");
        expectedResult.add("\\x01\\x01\\x00\\x00, f:q2, [44.0]");

        MockupRegionScanner innerScanner = new MockupRegionScanner(cellsInput);

        RegionScanner aggrScanner = new AggregationScanner(rowType, filter, projector, aggregators, innerScanner, ObserverBehavior.SCAN_FILTER_AGGR);
        ArrayList<Cell> result = Lists.newArrayList();
        boolean hasMore = true;
        while (hasMore) {
            result.clear();
            hasMore = aggrScanner.next(result);
            if (result.isEmpty())
                continue;

            Cell cell = result.get(0);
            HCol hcol = null;
            if (ObserverAggregators.match(c1, cell)) {
                hcol = c1;
            } else if (ObserverAggregators.match(c2, cell)) {
                hcol = c2;
            } else
                fail();

            hcol.measureCodec.decode(ByteBuffer.wrap(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()), hcol.measureValues);

            String rowKey = toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(), mask);
            String col = Bytes.toString(hcol.family) + ":" + Bytes.toString(hcol.qualifier);
            String values = Arrays.toString(hcol.measureValues);

            System.out.println(rowKey);
            System.out.println(col);
            System.out.println(values);

            assertTrue(expectedResult.contains(rowKey + ", " + col + ", " + values));
        }
        aggrScanner.close();
    }

    @Test
    public void testNoMeasure() throws IOException {

        CoprocessorRowType rowType = newRowType();
        CoprocessorProjector projector = new CoprocessorProjector(mask);
        ObserverAggregators aggregators = new ObserverAggregators(new HCol[] {});
        CoprocessorFilter filter = CoprocessorFilter.deserialize(null); // a default,
        // always-true,
        // filter
        HashSet<String> expectedResult = new HashSet<String>();

        expectedResult.add("\\x02\\x02\\x00\\x00");
        expectedResult.add("\\x01\\x01\\x00\\x00");

        MockupRegionScanner innerScanner = new MockupRegionScanner(cellsInput);

        RegionScanner aggrScanner = new AggregationScanner(rowType, filter, projector, aggregators, innerScanner, ObserverBehavior.SCAN_FILTER_AGGR);
        ArrayList<Cell> result = Lists.newArrayList();
        boolean hasMore = true;
        while (hasMore) {
            result.clear();
            hasMore = aggrScanner.next(result);
            if (result.isEmpty())
                continue;

            Cell cell = result.get(0);

            String rowKey = toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(), mask);

            assertTrue(expectedResult.contains(rowKey));
        }
        aggrScanner.close();
    }

    private String toString(byte[] array, int offset, short length, byte[] mask) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < length; i++) {
            int ch = array[offset + i] & 0xFF & mask[i];
            result.append(String.format("\\x%02X", ch));
        }
        return result.toString();
    }

    private CoprocessorRowType newRowType() {
        TableDesc t = new TableDesc();
        t.setName("TABLE");
        t.setDatabase("DEFAULT");
        TblColRef[] cols = new TblColRef[] { newCol("A", t), newCol("B", t), newCol("C", t), newCol("D", t) };
        int[] sizes = new int[] { 1, 1, 1, 1 };
        return new CoprocessorRowType(cols, sizes);
    }

    private TblColRef newCol(String name, TableDesc t) {
        ColumnDesc col = new ColumnDesc();
        col.setName(name);
        col.setTable(t);
        return new TblColRef(col);
    }

    public static class MockupRegionScanner implements RegionScanner {
        List<Cell> input;
        int i = 0;

        public MockupRegionScanner(List<Cell> cellInputs) {
            this.input = cellInputs;
        }

        @Override
        public boolean next(List<Cell> results) throws IOException {
            return nextRaw(results);
        }

        @Override
        public boolean next(List<Cell> result, ScannerContext scannerContext) throws IOException {
            return next(result);
        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public HRegionInfo getRegionInfo() {
            return null;
        }

        @Override
        public boolean isFilterDone() throws IOException {
            return false;
        }

        @Override
        public boolean reseek(byte[] row) throws IOException {
            return false;
        }

        @Override
        public long getMaxResultSize() {
            return 0;
        }

        @Override
        public long getMvccReadPoint() {
            return 0;
        }

        @Override
        public boolean nextRaw(List<Cell> result) throws IOException {
            if (i < input.size()) {
                result.add(input.get(i));
                i++;
            }
            return i < input.size();
        }

        @Override
        public boolean nextRaw(List<Cell> result, ScannerContext scannerContext) throws IOException {
            return nextRaw(result);
        }

        @Override
        public int getBatch() {
            return -1;
        }
    }

}

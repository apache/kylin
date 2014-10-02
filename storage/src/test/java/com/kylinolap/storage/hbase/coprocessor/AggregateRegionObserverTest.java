/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.storage.hbase.coprocessor;

import com.google.common.collect.Lists;
import com.kylinolap.cube.kv.RowConstants;
import com.kylinolap.storage.hbase.coprocessor.RowAggregators.HCol;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author yangli9
 */
public class AggregateRegionObserverTest {
    ByteBuffer buf = ByteBuffer.allocate(RowConstants.ROWVALUE_BUFFER_SIZE);

    byte[] mask = new byte[]{(byte) 0xff, (byte) 0xff, 0, 0};
    byte[] k1 = new byte[]{0x01, 0x01, 0, 0x01};
    byte[] k2 = new byte[]{0x01, 0x01, 0, 0x02};
    byte[] k3 = new byte[]{0x02, 0x02, 0, 0x03};
    byte[] k4 = new byte[]{0x02, 0x02, 0, 0x04};

    ArrayList<Cell> cellsInput = Lists.newArrayList();

    byte[] family = Bytes.toBytes("f");
    byte[] q1 = Bytes.toBytes("q1");
    byte[] q2 = Bytes.toBytes("q2");

    HCol c1 = new HCol(family, q1, new String[]{"SUM", "COUNT"}, new String[]{"decimal", "long"});
    HCol c2 = new HCol(family, q2, new String[]{"SUM"}, new String[]{"decimal"});

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
                new Object[]{new BigDecimal(decimal)} //
                : new Object[]{new BigDecimal(decimal), new LongWritable(number)};
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

        RowProjector projector = new RowProjector(mask);
        RowAggregators aggregators = new RowAggregators(new HCol[]{c1, c2});
        RowFilter filter = RowFilter.deserialize(null); // a default, always-true, filter
        HashSet<String> expectedResult = new HashSet<String>();

        expectedResult.add("\\x02\\x02\\x00\\x00, f:q1, [26.0, 7]");
        expectedResult.add("\\x02\\x02\\x00\\x00, f:q2, [48.0]");
        expectedResult.add("\\x01\\x01\\x00\\x00, f:q1, [22.0, 3]");
        expectedResult.add("\\x01\\x01\\x00\\x00, f:q2, [44.0]");

        MockupRegionScanner innerScanner = new MockupRegionScanner(cellsInput);

        RegionScanner aggrScanner = new AggregationFilter(null, filter, projector, aggregators, innerScanner);
        ArrayList<Cell> result = Lists.newArrayList();
        boolean hasMore = true;
        while (hasMore) {
            result.clear();
            hasMore = aggrScanner.next(result);
            if (result.isEmpty())
                continue;

            Cell cell = result.get(0);
            HCol hcol = null;
            if (RowAggregators.match(c1, cell)) {
                hcol = c1;
            } else if (RowAggregators.match(c2, cell)) {
                hcol = c2;
            } else
                fail();

            hcol.measureCodec.decode(
                    ByteBuffer.wrap(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()),
                    hcol.measureValues);

            String rowKey =
                    Bytes.toStringBinary(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
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

        RowProjector projector = new RowProjector(mask);
        RowAggregators aggregators = new RowAggregators(new HCol[]{});
        RowFilter filter = RowFilter.deserialize(null); // a default, always-true, filter
        HashSet<String> expectedResult = new HashSet<String>();

        expectedResult.add("\\x02\\x02\\x00\\x00");
        expectedResult.add("\\x01\\x01\\x00\\x00");

        MockupRegionScanner innerScanner = new MockupRegionScanner(cellsInput);

        RegionScanner aggrScanner = new AggregationFilter(null, filter, projector, aggregators, innerScanner);
        ArrayList<Cell> result = Lists.newArrayList();
        boolean hasMore = true;
        while (hasMore) {
            result.clear();
            hasMore = aggrScanner.next(result);
            if (result.isEmpty())
                continue;

            Cell cell = result.get(0);

            String rowKey =
                    Bytes.toStringBinary(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());

            assertTrue(expectedResult.contains(rowKey));
        }
        aggrScanner.close();
    }

    public static class MockupRegionScanner implements RegionScanner {
        List<Cell> input;
        int i = 0;

        public MockupRegionScanner(List<Cell> cellInputs) {
            this.input = cellInputs;
        }

        /* (non-Javadoc)
         * @see org.apache.hadoop.hbase.regionserver.InternalScanner#next(java.util.List)
         */
        @Override
        public boolean next(List<Cell> results) throws IOException {
            return nextRaw(results);
        }

        /* (non-Javadoc)
         * @see org.apache.hadoop.hbase.regionserver.InternalScanner#next(java.util.List, int)
         */
        @Override
        public boolean next(List<Cell> result, int limit) throws IOException {
            return next(result);
        }

        /* (non-Javadoc)
         * @see org.apache.hadoop.hbase.regionserver.InternalScanner#close()
         */
        @Override
        public void close() throws IOException {

        }

        /* (non-Javadoc)
         * @see org.apache.hadoop.hbase.regionserver.RegionScanner#getRegionInfo()
         */
        @Override
        public HRegionInfo getRegionInfo() {
            return null;
        }

        /* (non-Javadoc)
         * @see org.apache.hadoop.hbase.regionserver.RegionScanner#isFilterDone()
         */
        @Override
        public boolean isFilterDone() throws IOException {
            return false;
        }

        /* (non-Javadoc)
         * @see org.apache.hadoop.hbase.regionserver.RegionScanner#reseek(byte[])
         */
        @Override
        public boolean reseek(byte[] row) throws IOException {
            return false;
        }

        /* (non-Javadoc)
         * @see org.apache.hadoop.hbase.regionserver.RegionScanner#getMaxResultSize()
         */
        @Override
        public long getMaxResultSize() {
            return 0;
        }

        /* (non-Javadoc)
         * @see org.apache.hadoop.hbase.regionserver.RegionScanner#getMvccReadPoint()
         */
        @Override
        public long getMvccReadPoint() {
            return 0;
        }

        /* (non-Javadoc)
         * @see org.apache.hadoop.hbase.regionserver.RegionScanner#nextRaw(java.util.List)
         */
        @Override
        public boolean nextRaw(List<Cell> result) throws IOException {
            if (i < input.size()) {
                result.add(input.get(i));
                i++;
            }
            return i < input.size();
        }

        /* (non-Javadoc)
         * @see org.apache.hadoop.hbase.regionserver.RegionScanner#nextRaw(java.util.List, int)
         */
        @Override
        public boolean nextRaw(List<Cell> result, int limit) throws IOException {
            return nextRaw(result);
        }

    }

}

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

package org.apache.kylin.gridtable;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;

import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.gridtable.memstore.GTSimpleMemStore;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SimpleGridTableTest extends LocalFileMetadataTestCase {

    @BeforeClass
    public static void setUp() throws Exception {
        staticCreateTestMetadata();
    }

    @AfterClass
    public static void after() throws Exception {
        cleanAfterClass();
    }

    @Test
    public void testBasics() throws IOException {
        GTInfo info = UnitTestSupport.basicInfo();
        GTSimpleMemStore store = new GTSimpleMemStore(info);
        GridTable table = new GridTable(info, store);

        GTBuilder builder = rebuild(table);
        scan(table);
    }

    @Test
    public void testAdvanced() throws IOException {
        GTInfo info = UnitTestSupport.advancedInfo();
        GTSimpleMemStore store = new GTSimpleMemStore(info);
        GridTable table = new GridTable(info, store);

        GTBuilder builder = rebuild(table);
        scan(table);
    }

    @Test
    public void testAggregate() throws IOException {
        GTInfo info = UnitTestSupport.advancedInfo();
        GTSimpleMemStore store = new GTSimpleMemStore(info);
        GridTable table = new GridTable(info, store);

        GTBuilder builder = rebuild(table);
        scanAndAggregate(table);
    }

    @Test
    public void testAppend() throws IOException {
        GTInfo info = UnitTestSupport.advancedInfo();
        GTSimpleMemStore store = new GTSimpleMemStore(info);
        GridTable table = new GridTable(info, store);

        rebuildViaAppend(table);
        scan(table);
    }

    private void scan(GridTable table) throws IOException {
        GTScanRequest req = new GTScanRequestBuilder().setInfo(table.getInfo()).setRanges(null).setDimensions(null).setFilterPushDown(null).createGTScanRequest();
        try (IGTScanner scanner = table.scan(req)) {
            for (GTRecord r : scanner) {
                Object[] v = r.getValues();
                assertTrue(((String) v[0]).startsWith("2015-"));
                assertTrue(((String) v[2]).equals("Food"));
                assertTrue(((Long) v[3]).longValue() == 10);
                assertTrue(((BigDecimal) v[4]).doubleValue() == 10.5);
                System.out.println(r);
            }
        }
    }

    private void scanAndAggregate(GridTable table) throws IOException {
        GTScanRequest req = new GTScanRequestBuilder().setInfo(table.getInfo()).setRanges(null).setDimensions(null).setAggrGroupBy(setOf(0, 2)).setAggrMetrics(setOf(3, 4)).setAggrMetricsFuncs(new String[] { "count", "sum" }).setFilterPushDown(null).createGTScanRequest();
        try (IGTScanner scanner = table.scan(req)) {
            int i = 0;
            for (GTRecord r : scanner) {
                Object[] v = r.getValues();
                switch (i) {
                    case 0:
                        assertTrue(((Long) v[3]).longValue() == 20);
                        assertTrue(((BigDecimal) v[4]).doubleValue() == 21.0);
                        break;
                    case 1:
                        assertTrue(((Long) v[3]).longValue() == 30);
                        assertTrue(((BigDecimal) v[4]).doubleValue() == 31.5);
                        break;
                    case 2:
                        assertTrue(((Long) v[3]).longValue() == 40);
                        assertTrue(((BigDecimal) v[4]).doubleValue() == 42.0);
                        break;
                    case 3:
                        assertTrue(((Long) v[3]).longValue() == 10);
                        assertTrue(((BigDecimal) v[4]).doubleValue() == 10.5);
                        break;
                    default:
                        fail();
                }
                i++;
                System.out.println(r);
            }
        }
    }

    static GTBuilder rebuild(GridTable table) throws IOException {
        GTBuilder builder = table.rebuild();
        for (GTRecord rec : UnitTestSupport.mockupData(table.getInfo(), 10)) {
            builder.write(rec);
        }
        builder.close();

        System.out.println("Written Row Count: " + builder.getWrittenRowCount());
        return builder;
    }

    static void rebuildViaAppend(GridTable table) throws IOException {
        List<GTRecord> data = UnitTestSupport.mockupData(table.getInfo(), 10);
        GTBuilder builder;
        int i = 0;

        builder = table.append();
        builder.write(data.get(i++));
        builder.write(data.get(i++));
        builder.write(data.get(i++));
        builder.write(data.get(i++));
        builder.close();
        System.out.println("Written Row Count: " + builder.getWrittenRowCount());

        builder = table.append();
        builder.write(data.get(i++));
        builder.write(data.get(i++));
        builder.write(data.get(i++));
        builder.close();
        System.out.println("Written Row Count: " + builder.getWrittenRowCount());

        builder = table.append();
        builder.write(data.get(i++));
        builder.write(data.get(i++));
        builder.close();
        System.out.println("Written Row Count: " + builder.getWrittenRowCount());

        builder = table.append();
        builder.write(data.get(i++));
        builder.close();
        System.out.println("Written Row Count: " + builder.getWrittenRowCount());
    }

    private static ImmutableBitSet setOf(int... values) {
        return ImmutableBitSet.valueOf(values);
    }
}

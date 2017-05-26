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

package org.apache.kylin.storage.hbase.cube;

import java.io.IOException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTInfo.Builder;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTSampleCodeSystem;
import org.apache.kylin.gridtable.GTScanRequestBuilder;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.gridtable.IGTWriter;
import org.apache.kylin.gridtable.benchmark.SortedGTRecordGenerator;
import org.apache.kylin.metadata.datatype.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Benchmark of processing 10 million GTRecords. 5 dimensions of type int4, and 2 measures of type long8.
 */
public class HBaseScannerBenchmark {

    static final Logger logger = LoggerFactory.getLogger(HBaseScannerBenchmark.class);

    final GTInfo info;
    final SortedGTRecordGenerator gen;

    final ImmutableBitSet dimensions = ImmutableBitSet.valueOf(0, 1, 2, 3, 4);
    final ImmutableBitSet metrics = ImmutableBitSet.valueOf(5, 6);
    final String[] aggrFuncs = new String[] { "SUM", "SUM" };

    final long N = 1000000; // 1M, note the limit memory of HBase in sandbox

    final TableName htableName = TableName.valueOf("HBaseScannerBenchmark");
    final SimpleHBaseStore simpleStore;

    public HBaseScannerBenchmark() throws IOException {
        Builder builder = GTInfo.builder();
        builder.setCodeSystem(new GTSampleCodeSystem());
        DataType tint = DataType.getType("int4");
        DataType tlong = DataType.getType("long8");
        builder.setColumns(tint, tint, tint, tint, tint, tlong, tlong);
        builder.setPrimaryKey(ImmutableBitSet.valueOf(0, 1, 2, 3, 4));
        info = builder.build();

        gen = new SortedGTRecordGenerator(info);
        gen.addDimension(10, 4, null);
        gen.addDimension(10, 4, null);
        gen.addDimension(10, 4, null);
        gen.addDimension(10, 4, null);
        gen.addDimension(100, 4, null);
        gen.addMeasure(8);
        gen.addMeasure(8);

        simpleStore = new SimpleHBaseStore(info, htableName);
    }

    private void buildTable() throws IOException {
        IGTWriter builder = simpleStore.rebuild();

        logger.info("Writing " + N + " records");
        long t = System.currentTimeMillis();

        long count = 0;
        for (GTRecord rec : gen.generate(N)) {
            builder.write(rec);
            count++;
            if (count % 100000 == 0)
                logger.info(count + " rows written");
        }
        builder.close();

        t = System.currentTimeMillis() - t;
        logger.info(count + " rows written, " + speed(t) + "K row/sec");
    }

    public void testScan() throws IOException {
        int rounds = 5;

        for (int i = 0; i < rounds; i++) {
            testScanRaw("Scan raw " + (i + 1) + " of " + rounds);
            testScanRecords("Scan records " + (i + 1) + " of " + rounds);
        }
    }

    @SuppressWarnings("unused")
    private void testScanRaw(String msg) throws IOException {
        long t = System.currentTimeMillis();

        IGTScanner scan = simpleStore.scan(new GTScanRequestBuilder().setInfo(info).setRanges(null).setDimensions(null).setFilterPushDown(null).createGTScanRequest());
        ResultScanner innerScanner = ((SimpleHBaseStore.Reader) scan).getHBaseScanner();
        int count = 0;
        for (Result r : innerScanner) {
            count++;
        }
        scan.close();

        t = System.currentTimeMillis() - t;
        logger.info(msg + ", " + count + " rows, " + speed(t) + "K row/sec");
    }

    @SuppressWarnings("unused")
    private void testScanRecords(String msg) throws IOException {
        long t = System.currentTimeMillis();

        IGTScanner scan = simpleStore.scan(new GTScanRequestBuilder().setInfo(info).setRanges(null).setDimensions(null).setFilterPushDown(null).createGTScanRequest());
        int count = 0;
        for (GTRecord rec : scan) {
            count++;
        }
        scan.close();

        t = System.currentTimeMillis() - t;
        logger.info(msg + ", " + count + " records, " + speed(t) + "K rec/sec");
    }

    private int speed(long t) {
        double sec = (double) t / 1000;
        return (int) (N / sec / 1000);
    }

    public void cleanup() throws IOException {
        simpleStore.cleanup();
    }

    public static void main(String[] args) throws IOException {
        boolean createTable = true;
        boolean deleteTable = true;
        if (args != null && args.length > 1) {
            try {
                createTable = Boolean.parseBoolean(args[0]);
            } catch (Exception e) {
                createTable = true;
            }

            try {
                deleteTable = Boolean.parseBoolean(args[1]);
            } catch (Exception e) {
                deleteTable = true;
            }
        }

        KylinConfig.setSandboxEnvIfPossible();

        HBaseScannerBenchmark benchmark = new HBaseScannerBenchmark();
        if (createTable) {
            benchmark.buildTable();
        }

        benchmark.testScan();
        if (deleteTable) {
            benchmark.cleanup();
        }
    }

}

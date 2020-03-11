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

package org.apache.kylin.gridtable.benchmark;

import java.io.IOException;
import java.util.List;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTInfo.Builder;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTSampleCodeSystem;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.GTScanRequestBuilder;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter.FilterOperatorEnum;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

/**
 * Benchmark of processing 10 million GTRecords. 5 dimensions of type int4, and 2 measures of type long8.
 */
public class GTScannerBenchmark {

    final GTInfo info;
    final SortedGTRecordGenerator gen;

    final ImmutableBitSet dimensions = ImmutableBitSet.valueOf(0, 1, 2, 3, 4);
    final ImmutableBitSet metrics = ImmutableBitSet.valueOf(5, 6);
    final String[] aggrFuncs = new String[] { "SUM", "SUM" };

    final long N = 10000000; // 10M
    final long genTime;

    public GTScannerBenchmark() {
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

        // warm up
        long t = System.currentTimeMillis();
        testGenerate();
        genTime = System.currentTimeMillis() - t;
    }

    @SuppressWarnings("unused")
    public void testGenerate() {
        long count = 0;
        for (GTRecord rec : gen.generate(N)) {
            count++;
        }
    }

    //@Test
    public void testAggregate2() throws IOException {
        testAggregate(ImmutableBitSet.valueOf(0, 1));
    }

    //@Test
    public void testAggregate2_() throws IOException {
        testAggregate(ImmutableBitSet.valueOf(0, 2));
    }

    //@Test
    public void testAggregate4() throws IOException {
        testAggregate(ImmutableBitSet.valueOf(0, 1, 2, 3));
    }

    //@Test
    public void testAggregate5() throws IOException {
        testAggregate(ImmutableBitSet.valueOf(0, 1, 2, 3, 4));
    }

    @SuppressWarnings("unused")
    private void testAggregate(ImmutableBitSet groupBy) throws IOException {
        long t = System.currentTimeMillis();
        GTScanRequest req = new GTScanRequestBuilder().setInfo(info).setRanges(null).setDimensions(dimensions).setAggrGroupBy(groupBy).setAggrMetrics(metrics).setAggrMetricsFuncs(aggrFuncs).setFilterPushDown(null).createGTScanRequest();
        IGTScanner scanner = req.decorateScanner(gen.generate(N));

        long count = 0;
        for (GTRecord rec : scanner) {
            count++;
        }

        t = System.currentTimeMillis() - t;
        System.out.println(N + " records aggregated to " + count + ", " + calcSpeed(t) + "K rec/sec");
    }

    private int calcSpeed(long t) {
        double sec = (double) (t - genTime) / 1000;
        return (int) (N / sec / 1000);
    }

    //@Test
    public void testFilter1() throws IOException {
        testFilter(eq(col(1), 1, 5, 7));
    }

    //@Test
    public void testFilter2() throws IOException {
        testFilter(//
                and(//
                        gt(col(0), 5), //
                        eq(col(2), 2, 4)));
    }

    //@Test
    public void testFilter3() throws IOException {
        testFilter(//
                and(//
                        gt(col(0), 2), //
                        eq(col(4), 1, 3, 5, 9, 12, 14, 23, 43, 52, 78, 92), //
                        or(//
                                eq(col(1), 2, 4), //
                                eq(col(2), 2, 4, 5, 9))));
    }

    @SuppressWarnings("unused")
    private void testFilter(TupleFilter filter) throws IOException {
        long t = System.currentTimeMillis();
        GTScanRequest req = new GTScanRequestBuilder().setInfo(info).setRanges(null).setDimensions(info.getAllColumns()).setFilterPushDown(filter).createGTScanRequest();
        IGTScanner scanner = req.decorateScanner(gen.generate(N));

        long count = 0;
        for (GTRecord rec : scanner) {
            count++;
        }

        t = System.currentTimeMillis() - t;
        System.out.println(N + " records filtered to " + count + ", " + calcSpeed(t) + "K rec/sec");
    }

    private LogicalTupleFilter and(TupleFilter... filters) {
        return logical(FilterOperatorEnum.AND, filters);
    }

    private LogicalTupleFilter or(TupleFilter... filters) {
        return logical(FilterOperatorEnum.OR, filters);
    }

    private LogicalTupleFilter logical(FilterOperatorEnum op, TupleFilter[] filters) {
        LogicalTupleFilter r = new LogicalTupleFilter(op);
        for (TupleFilter f : filters)
            r.addChild(f);
        return r;
    }

    private CompareTupleFilter gt(ColumnTupleFilter col, int v) {
        CompareTupleFilter r = new CompareTupleFilter(FilterOperatorEnum.GT);
        r.addChild(col);

        int c = col.getColumn().getColumnDesc().getZeroBasedIndex();
        int len = info.getCodeSystem().maxCodeLength(c);
        ByteArray bytes = new ByteArray(len);
        BytesUtil.writeLong(v, bytes.array(), bytes.offset(), len);
        r.addChild(new ConstantTupleFilter(bytes));

        return r;
    }

    private CompareTupleFilter eq(ColumnTupleFilter col, int... values) {
        CompareTupleFilter r = new CompareTupleFilter(FilterOperatorEnum.IN);
        r.addChild(col);

        List<ByteArray> list = Lists.newArrayList();
        for (int v : values) {
            int c = col.getColumn().getColumnDesc().getZeroBasedIndex();
            int len = info.getCodeSystem().maxCodeLength(c);
            ByteArray bytes = new ByteArray(len);
            BytesUtil.writeLong(v, bytes.array(), bytes.offset(), len);
            list.add(bytes);
        }
        r.addChild(new ConstantTupleFilter(list));
        return r;
    }

    private ColumnTupleFilter col(int i) {
        return new ColumnTupleFilter(info.colRef(i));
    }

    public static void main(String[] args) throws IOException {
        GTScannerBenchmark benchmark = new GTScannerBenchmark();

        benchmark.testFilter1();
        benchmark.testFilter2();
        benchmark.testFilter3();

        benchmark.testAggregate2();
        benchmark.testAggregate2_();
        benchmark.testAggregate4();
        benchmark.testAggregate5();
    }
}

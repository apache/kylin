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

package org.apache.kylin.gridtable;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.measure.basic.BigDecimalSumAggregator;
import org.apache.kylin.measure.basic.DoubleSumAggregator;
import org.apache.kylin.measure.basic.LongSumAggregator;
import org.apache.kylin.measure.hllc.HLLCAggregator;
import org.apache.kylin.measure.hllc.HyperLogLogPlusCounter;
import org.apache.kylin.metadata.datatype.DoubleMutable;
import org.apache.kylin.metadata.datatype.LongMutable;
import org.junit.Test;

public class AggregationCacheMemSizeTest {

    public static final int NUM_OF_OBJS = 1000000 / 2;

    interface CreateAnObject {
        Object create();
    }

    @Test
    public void testHLLCAggregatorSize() throws InterruptedException {
        int est = estimateObjectSize(new CreateAnObject() {
            @Override
            public Object create() {
                HLLCAggregator aggr = new HLLCAggregator(10);
                aggr.aggregate(new HyperLogLogPlusCounter(10));
                return aggr;
            }
        });
        System.out.println("HLLC: " + est);
    }

    @Test
    public void testBigDecimalAggregatorSize() throws InterruptedException {
        int est = estimateObjectSize(new CreateAnObject() {
            @Override
            public Object create() {
                return newBigDecimalAggr();
            }

        });
        System.out.println("BigDecimal: " + est);
    }

    private BigDecimalSumAggregator newBigDecimalAggr() {
        BigDecimalSumAggregator aggr = new BigDecimalSumAggregator();
        aggr.aggregate(new BigDecimal("12345678901234567890.123456789"));
        return aggr;
    }

    @Test
    public void testLongAggregatorSize() throws InterruptedException {
        int est = estimateObjectSize(new CreateAnObject() {
            @Override
            public Object create() {
                return newLongAggr();
            }
        });
        System.out.println("Long: " + est);
    }

    private LongSumAggregator newLongAggr() {
        LongSumAggregator aggr = new LongSumAggregator();
        aggr.aggregate(new LongMutable(10));
        return aggr;
    }

    @Test
    public void testDoubleAggregatorSize() throws InterruptedException {
        int est = estimateObjectSize(new CreateAnObject() {
            @Override
            public Object create() {
                return newDoubleAggr();
            }
        });
        System.out.println("Double: " + est);
    }

    private DoubleSumAggregator newDoubleAggr() {
        DoubleSumAggregator aggr = new DoubleSumAggregator();
        aggr.aggregate(new DoubleMutable(10));
        return aggr;
    }

    @Test
    public void testByteArraySize() throws InterruptedException {
        int est = estimateObjectSize(new CreateAnObject() {
            @Override
            public Object create() {
                return new byte[10];
            }
        });
        System.out.println("byte[10]: " + est);
    }

    @Test
    public void testAggregatorArraySize() throws InterruptedException {
        int est = estimateObjectSize(new CreateAnObject() {
            @Override
            public Object create() {
                return new MeasureAggregator[7];
            }
        });
        System.out.println("MeasureAggregator[7]: " + est);
    }

    @Test
    public void testTreeMapSize() throws InterruptedException {
        final SortedMap<byte[], Object> map = new TreeMap<byte[], Object>(new Comparator<byte[]>() {
            @Override
            public int compare(byte[] o1, byte[] o2) {
                return Bytes.compareTo(o1, o2);
            }
        });
        final Random rand = new Random();
        int est = estimateObjectSize(new CreateAnObject() {
            @Override
            public Object create() {
                byte[] key = new byte[10];
                rand.nextBytes(key);
                map.put(key, null);
                return null;
            }
        });
        System.out.println("TreeMap entry: " + (est - 20)); // -20 is to exclude byte[10]
    }

    @Test
    public void testAggregationCacheSize() throws InterruptedException {
        final SortedMap<byte[], Object> map = new TreeMap<byte[], Object>(new Comparator<byte[]>() {
            @Override
            public int compare(byte[] o1, byte[] o2) {
                return Bytes.compareTo(o1, o2);
            }
        });
        final Random rand = new Random();

        long bytesBefore = memLeft();
        byte[] key = null;
        MeasureAggregator<?>[] aggrs = null;
        for (int i = 0; i < NUM_OF_OBJS; i++) {
            key = new byte[10];
            rand.nextBytes(key);
            aggrs = new MeasureAggregator[4];
            aggrs[0] = newBigDecimalAggr();
            aggrs[1] = newLongAggr();
            aggrs[2] = newDoubleAggr();
            aggrs[3] = newDoubleAggr();
            map.put(key, aggrs);
        }

        long bytesAfter = memLeft();

        long mapActualSize = bytesBefore - bytesAfter;
        long mapExpectSize = GTAggregateScanner.estimateSizeOfAggrCache(key, aggrs, map.size());
        System.out.println("Actual cache size: " + mapActualSize);
        System.out.println("Expect cache size: " + mapExpectSize);
    }

    private int estimateObjectSize(CreateAnObject factory) throws InterruptedException {
        Object[] hold = new Object[NUM_OF_OBJS];
        long bytesBefore = memLeft();

        for (int i = 0; i < hold.length; i++) {
            hold[i] = factory.create();
        }

        long bytesAfter = memLeft();
        return (int) ((bytesBefore - bytesAfter) / hold.length);
    }

    private long memLeft() throws InterruptedException {
        Runtime.getRuntime().gc();
        Thread.sleep(500);
        return getSystemAvailBytes();
    }

    private long getSystemAvailBytes() {
        Runtime runtime = Runtime.getRuntime();
        long totalMemory = runtime.totalMemory(); // current heap allocated to the VM process
        long freeMemory = runtime.freeMemory(); // out of the current heap, how much is free
        long maxMemory = runtime.maxMemory(); // Max heap VM can use e.g. Xmx setting
        long usedMemory = totalMemory - freeMemory; // how much of the current heap the VM is using
        long availableMemory = maxMemory - usedMemory; // available memory i.e. Maximum heap size minus the current amount used
        return availableMemory;
    }

}

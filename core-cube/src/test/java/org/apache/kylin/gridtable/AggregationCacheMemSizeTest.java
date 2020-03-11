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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.measure.basic.BigDecimalSumAggregator;
import org.apache.kylin.measure.basic.DoubleSumAggregator;
import org.apache.kylin.measure.basic.LongSumAggregator;
import org.apache.kylin.measure.bitmap.BitmapAggregator;
import org.apache.kylin.measure.bitmap.BitmapCounter;
import org.apache.kylin.measure.bitmap.BitmapCounterFactory;
import org.apache.kylin.measure.bitmap.RoaringBitmapCounterFactory;
import org.apache.kylin.measure.hllc.HLLCAggregator;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.github.jamm.MemoryMeter;
import org.junit.Test;

import org.apache.kylin.shaded.com.google.common.base.Stopwatch;

public class AggregationCacheMemSizeTest {
    private static final MemoryMeter meter = new MemoryMeter();
    private static final BitmapCounterFactory bitmapFactory = RoaringBitmapCounterFactory.INSTANCE;
    private static final BitmapCounter[] bitmaps = new BitmapCounter[5];
    private static final Random random = new Random();

    // consider bitmaps with variant cardinality
    static {
        for (int i = 0; i < bitmaps.length; i++) {
            bitmaps[i] = bitmapFactory.newBitmap();
        }

        final int totalBits = 1_000_000;

        // case 0: sparse, low-cardinality bitmap
        for (int i = 0; i < 100; i++) {
            bitmaps[0].add(random.nextInt(totalBits));
        }

        // case 1: 20% full bitmap
        for (int i = 0; i < totalBits; i++) {
            if (random.nextInt(100) < 20) {
                bitmaps[1].add(i);
            }
        }

        // case 2: half full bitmap
        for (int i = 0; i < totalBits; i++) {
            if (random.nextInt(100) < 50) {
                bitmaps[2].add(i);
            }
        }

        // case 3: 80% full bitmap
        for (int i = 0; i < totalBits; i++) {
            if (random.nextInt(100) < 80) {
                bitmaps[3].add(i);
            }
        }

        // case 4: dense, high-cardinality bitmap
        for (int i = 0; i < totalBits; i++) {
            if (random.nextInt(totalBits) < 100) {
                continue;
            }
            bitmaps[4].add(i);
        }
    }

    enum Settings {
        WITHOUT_MEM_HUNGRY, // only test basic aggrs
        WITH_HLLC, // basic aggrs + hllc
        WITH_LOW_CARD_BITMAP, // basic aggrs + bitmap
        WITH_HIGH_CARD_BITMAP // basic aggrs + bitmap
    }

    private MeasureAggregator<?>[] createNoMemHungryAggrs() {
        LongSumAggregator longSum = new LongSumAggregator();
        longSum.aggregate(new Long(10));

        DoubleSumAggregator doubleSum = new DoubleSumAggregator();
        doubleSum.aggregate(new Double(10));

        BigDecimalSumAggregator decimalSum = new BigDecimalSumAggregator();
        decimalSum.aggregate(new BigDecimal("12345678901234567890.123456789"));

        return new MeasureAggregator[] { longSum, doubleSum, decimalSum };
    }

    private HLLCAggregator createHLLCAggr() {
        HLLCAggregator hllcAggregator = new HLLCAggregator(14);
        hllcAggregator.aggregate(new HLLCounter(14));
        return hllcAggregator;
    }

    private BitmapAggregator createBitmapAggr(boolean lowCardinality) {
        BitmapCounter counter = bitmapFactory.newBitmap();
        counter.orWith(lowCardinality ? bitmaps[0] : bitmaps[3]);

        BitmapAggregator result = new BitmapAggregator();
        result.aggregate(counter);
        return result;
    }

    private MeasureAggregator<?>[] createAggrs(Settings settings) {
        List<MeasureAggregator<?>> aggregators = new ArrayList<>();
        aggregators.addAll(Arrays.asList(createNoMemHungryAggrs()));

        switch (settings) {
        case WITHOUT_MEM_HUNGRY:
            break;
        case WITH_HLLC:
            aggregators.add(createHLLCAggr());
            break;
        case WITH_LOW_CARD_BITMAP:
            aggregators.add(createBitmapAggr(true));
            break;
        case WITH_HIGH_CARD_BITMAP:
            aggregators.add(createBitmapAggr(false));
            break;
        default:
            break;
        }

        return aggregators.toArray(new MeasureAggregator[aggregators.size()]);
    }

    @Test
    public void testEstimateBitmapMemSize() {
        BitmapAggregator[] bitmapAggrs = new BitmapAggregator[bitmaps.length];
        for (int i = 0; i < bitmapAggrs.length; i++) {
            bitmapAggrs[i] = new BitmapAggregator();
            bitmapAggrs[i].aggregate(bitmaps[i]);
        }

        System.out.printf(Locale.ROOT, "%-15s %-10s %-10s\n", "cardinality", "estimate", "actual");
        for (BitmapAggregator aggr : bitmapAggrs) {
            System.out.printf(Locale.ROOT, "%-15d %-10d %-10d\n", aggr.getState().getCount(),
                    aggr.getMemBytesEstimate(), meter.measureDeep(aggr));
        }
    }

    @Test
    public void testEstimateMemSize() throws InterruptedException {
        int scale = Integer.parseInt(System.getProperty("scale", "1"));
        scale = Math.max(1, Math.min(10, scale));

        testSetting(Settings.WITHOUT_MEM_HUNGRY, scale * 100000);
        testSetting(Settings.WITH_HLLC, scale * 5000);
        testSetting(Settings.WITH_LOW_CARD_BITMAP, scale * 10000);
        testSetting(Settings.WITH_HIGH_CARD_BITMAP, scale * 1000);
    }

    private void testSetting(Settings settings, int inputCount) {
        SortedMap<byte[], Object> map = new TreeMap<>(new Comparator<byte[]>() {
            @Override
            public int compare(byte[] o1, byte[] o2) {
                return Bytes.compareTo(o1, o2);
            }
        });

        final int reportInterval = inputCount / 10;
        final Stopwatch stopwatch = Stopwatch.createUnstarted();
        long estimateMillis = 0;
        long actualMillis = 0;

        System.out.println("Settings: " + settings);
        System.out.printf(Locale.ROOT, "%15s %15s %15s %15s %15s\n", "Size", "Estimate(bytes)", "Actual(bytes)",
                "Estimate(ms)", "Actual(ms)");

        for (int i = 0; i < inputCount; i++) {
            byte[] key = new byte[10];
            random.nextBytes(key);
            MeasureAggregator[] values = createAggrs(settings);
            map.put(key, values);

            if ((i + 1) % reportInterval == 0) {
                stopwatch.start();
                long estimateBytes = GTAggregateScanner.estimateSizeOfAggrCache(key, values, map.size());
                estimateMillis += stopwatch.elapsed(MILLISECONDS);
                stopwatch.reset();

                stopwatch.start();
                long actualBytes = meter.measureDeep(map);
                actualMillis += stopwatch.elapsed(MILLISECONDS);
                stopwatch.reset();

                System.out.printf(Locale.ROOT, "%,15d %,15d %,15d %,15d %,15d\n", map.size(), estimateBytes,
                        actualBytes, estimateMillis, actualMillis);
            }
        }
        System.out.println("---------------------------------------\n");

        map = null;
        System.gc();
    }
}

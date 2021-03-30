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

package org.apache.kylin.measure;

import java.math.BigDecimal;
import java.util.List;
import java.util.Locale;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.measure.basic.BigDecimalMaxAggregator;
import org.apache.kylin.measure.basic.BigDecimalMinAggregator;
import org.apache.kylin.measure.basic.BigDecimalSumAggregator;
import org.apache.kylin.measure.basic.DoubleMaxAggregator;
import org.apache.kylin.measure.basic.DoubleMinAggregator;
import org.apache.kylin.measure.basic.DoubleSumAggregator;
import org.apache.kylin.measure.basic.LongMaxAggregator;
import org.apache.kylin.measure.basic.LongMinAggregator;
import org.apache.kylin.measure.basic.LongSumAggregator;
import org.apache.kylin.measure.bitmap.BitmapAggregator;
import org.apache.kylin.measure.bitmap.BitmapCounter;
import org.apache.kylin.measure.bitmap.RoaringBitmapCounterFactory;
import org.apache.kylin.measure.extendedcolumn.ExtendedColumnMeasureType;
import org.apache.kylin.measure.hllc.HLLCAggregator;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.apache.kylin.metadata.datatype.DataType;
import org.github.jamm.MemoryMeter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

public class AggregatorMemEstimateTest extends LocalFileMetadataTestCase {
    private static final MemoryMeter meter = new MemoryMeter();

    @BeforeClass
    public static void setUp() throws Exception {
        staticCreateTestMetadata();
    }

    @AfterClass
    public static void after() throws Exception {
        cleanAfterClass();
    }

    private List<? extends MeasureAggregator> basicAggregators() {
        Long longVal = new Long(1000);
        LongMinAggregator longMin = new LongMinAggregator();
        LongMaxAggregator longMax = new LongMaxAggregator();
        LongSumAggregator longSum = new LongSumAggregator();
        longMin.aggregate(longVal);
        longMax.aggregate(longVal);
        longSum.aggregate(longVal);

        Double doubleVal = new Double(1.0);
        DoubleMinAggregator doubleMin = new DoubleMinAggregator();
        DoubleMaxAggregator doubleMax = new DoubleMaxAggregator();
        DoubleSumAggregator doubleSum = new DoubleSumAggregator();
        doubleMin.aggregate(doubleVal);
        doubleMax.aggregate(doubleVal);
        doubleSum.aggregate(doubleVal);

        BigDecimalMinAggregator decimalMin = new BigDecimalMinAggregator();
        BigDecimalMaxAggregator decimalMax = new BigDecimalMaxAggregator();
        BigDecimalSumAggregator decimalSum = new BigDecimalSumAggregator();
        BigDecimal decimal = new BigDecimal("12345678901234567890.123456789");
        decimalMin.aggregate(decimal);
        decimalMax.aggregate(decimal);
        decimalSum.aggregate(decimal);

        return Lists.newArrayList(longMin, longMax, longSum, doubleMin, doubleMax, doubleSum, decimalMin, decimalMax,
                decimalSum);
    }

    private String getAggregatorName(Class<? extends MeasureAggregator> clazz) {
        if (!clazz.isAnonymousClass()) {
            return clazz.getSimpleName();
        }
        String[] parts = clazz.getName().split("\\.");
        return parts[parts.length - 1];
    }

    @Test
    public void testAggregatorEstimate() {
        HLLCAggregator hllcAggregator = new HLLCAggregator(14);
        hllcAggregator.aggregate(new HLLCounter(14));

        BitmapAggregator bitmapAggregator = new BitmapAggregator();
        BitmapCounter bitmapCounter = RoaringBitmapCounterFactory.INSTANCE.newBitmap();
        for (int i = 4000; i <= 100000; i += 2) {
            bitmapCounter.add(i);
        }
        bitmapAggregator.aggregate(bitmapCounter);

        ExtendedColumnMeasureType extendedColumnType = new ExtendedColumnMeasureType(
                DataType.getType("extendedcolumn(100)"));
        MeasureAggregator<ByteArray> extendedColumnAggregator = extendedColumnType.newAggregator();
        extendedColumnAggregator.aggregate(new ByteArray(100));

        List<MeasureAggregator> aggregators = Lists.newArrayList(basicAggregators());
        aggregators.add(hllcAggregator);
        aggregators.add(bitmapAggregator);
        aggregators.add(extendedColumnAggregator);

        System.out.printf(Locale.ROOT, "%40s %10s %10s\n", "Class", "Estimate", "Actual");
        for (MeasureAggregator aggregator : aggregators) {
            String clzName = getAggregatorName(aggregator.getClass());
            System.out.printf(Locale.ROOT, "%40s %10d %10d\n", clzName, aggregator.getMemBytesEstimate(),
                    meter.measureDeep(aggregator));
        }
    }

}

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

package org.apache.kylin.measure.basic;

import java.math.BigDecimal;

import org.apache.kylin.measure.MeasureAggregator;
import org.junit.Assert;
import org.junit.Test;

public class BasicAggregatorTest {

    @Test
    public void testSumNull() throws Exception {
        // Sum NULL
        testNullAggregator(LongSumAggregator.class);
        testNullAggregator(DoubleSumAggregator.class);
        testNullAggregator(BigDecimalSumAggregator.class);

        // Max NULL
        testNullAggregator(LongMaxAggregator.class);
        testNullAggregator(DoubleMaxAggregator.class);
        testNullAggregator(BigDecimalMaxAggregator.class);

        // Min NULL
        testNullAggregator(LongMinAggregator.class);
        testNullAggregator(DoubleMinAggregator.class);
        testNullAggregator(BigDecimalMinAggregator.class);
    }

    @Test
    public void testSumNormal() throws Exception {
        // sum long
        LongSumAggregator sumAggregator = new LongSumAggregator();
        testNormalAggregator(sumAggregator, 0L, 1234L);
        testNormalAggregator(sumAggregator, 1234L, 1234L);
        testNormalAggregator(sumAggregator, -1234L, 2345L);
        testNormalAggregator(sumAggregator, -1234L, -2345L);
        testNormalAggregator(sumAggregator, Long.MAX_VALUE - 1, 1L);
        testNormalAggregator(sumAggregator, Long.MIN_VALUE + 1, Long.MAX_VALUE);
        // sum double
        DoubleSumAggregator doubleSumAggregator = new DoubleSumAggregator();
        testNormalAggregator(doubleSumAggregator, 0.0d, 1234.6d);
        testNormalAggregator(doubleSumAggregator, -1234.5678d, 1234.65d);
        testNormalAggregator(doubleSumAggregator, -1234.5678d, -1234.65d);
        testNormalAggregator(doubleSumAggregator, Double.MAX_VALUE - 0.34d, 0.34d);
        testNormalAggregator(doubleSumAggregator, Double.MIN_VALUE, Double.MIN_VALUE);
        testNormalAggregator(doubleSumAggregator, -Double.MAX_VALUE, -Double.MIN_VALUE);
        // sum big-decimal
        BigDecimalSumAggregator bigDecimalSumAggregator = new BigDecimalSumAggregator();
        testNormalAggregator(bigDecimalSumAggregator, new BigDecimal(0), new BigDecimal(0));
        testNormalAggregator(bigDecimalSumAggregator, new BigDecimal(12345678.098765432),
                new BigDecimal(123456789.09876543211));
        testNormalAggregator(bigDecimalSumAggregator, new BigDecimal(-1222345678.098765432),
                new BigDecimal(-456123456789.09876543211));
        testNormalAggregator(bigDecimalSumAggregator, new BigDecimal(-456712345678.098765432),
                new BigDecimal(123456789.09876543211));

        // max long
        LongMaxAggregator maxLongAggregator = new LongMaxAggregator();
        testNormalAggregator(maxLongAggregator, 0L, 1234L);
        testNormalAggregator(maxLongAggregator, 1234L, 1234L);
        testNormalAggregator(maxLongAggregator, -1234L, 2345L);
        testNormalAggregator(maxLongAggregator, -1234L, -2345L);
        testNormalAggregator(maxLongAggregator, Long.MAX_VALUE - 1, 1L);
        testNormalAggregator(maxLongAggregator, Long.MIN_VALUE + 1, Long.MAX_VALUE);
        // max double
        DoubleMaxAggregator doubleMaxAggregator = new DoubleMaxAggregator();
        testNormalAggregator(doubleMaxAggregator, 0.0d, 1234.6d);
        testNormalAggregator(doubleMaxAggregator, -1234.5678d, 1234.65d);
        testNormalAggregator(doubleMaxAggregator, -1234.5678d, -1234.65d);
        testNormalAggregator(doubleMaxAggregator, Double.MAX_VALUE - 0.34d, 0.34d);
        testNormalAggregator(doubleMaxAggregator, Double.MIN_VALUE, Double.MIN_VALUE);
        testNormalAggregator(doubleMaxAggregator, -Double.MAX_VALUE, -Double.MIN_VALUE);
        // max big-decimal
        BigDecimalMaxAggregator bigDecimalMaxAggregator = new BigDecimalMaxAggregator();
        testNormalAggregator(bigDecimalMaxAggregator, new BigDecimal(0), new BigDecimal(0));
        testNormalAggregator(bigDecimalMaxAggregator, new BigDecimal(12345678.098765432),
                new BigDecimal(123456789.09876543211));
        testNormalAggregator(bigDecimalMaxAggregator, new BigDecimal(-1222345678.098765432),
                new BigDecimal(-456123456789.09876543211));
        testNormalAggregator(bigDecimalMaxAggregator, new BigDecimal(-456712345678.098765432),
                new BigDecimal(123456789.09876543211));

        // min long
        LongMinAggregator longMinAggregator = new LongMinAggregator();
        testNormalAggregator(longMinAggregator, 0L, 1234L);
        testNormalAggregator(longMinAggregator, 1234L, 1234L);
        testNormalAggregator(longMinAggregator, -1234L, 2345L);
        testNormalAggregator(longMinAggregator, -1234L, -2345L);
        testNormalAggregator(longMinAggregator, Long.MAX_VALUE - 1, 1L);
        testNormalAggregator(longMinAggregator, Long.MIN_VALUE + 1, Long.MAX_VALUE);
        // min double
        DoubleMinAggregator doubleMinAggregator = new DoubleMinAggregator();
        testNormalAggregator(doubleMinAggregator, 0.0d, 1234.6d);
        testNormalAggregator(doubleMinAggregator, -1234.5678d, 1234.65d);
        testNormalAggregator(doubleMinAggregator, -1234.5678d, -1234.65d);
        testNormalAggregator(doubleMinAggregator, Double.MAX_VALUE - 0.34d, 0.34d);
        testNormalAggregator(doubleMinAggregator, Double.MIN_VALUE, Double.MIN_VALUE);
        testNormalAggregator(doubleMinAggregator, -Double.MAX_VALUE, -Double.MIN_VALUE);
        // min big-decimal
        BigDecimalMinAggregator bigDecimalMinAggregator = new BigDecimalMinAggregator();
        testNormalAggregator(bigDecimalMinAggregator, new BigDecimal(0), new BigDecimal(0));
        testNormalAggregator(bigDecimalMinAggregator, new BigDecimal(12345678.098765432),
                new BigDecimal(123456789.09876543211));
        testNormalAggregator(bigDecimalMinAggregator, new BigDecimal(-1222345678.098765432),
                new BigDecimal(-456123456789.09876543211));
        testNormalAggregator(bigDecimalMinAggregator, new BigDecimal(-456712345678.098765432),
                new BigDecimal(123456789.09876543211));
    }

    private void testNullAggregator(Class<? extends MeasureAggregator> clazz) throws Exception {
        MeasureAggregator aggregator = clazz.newInstance();
        aggregator.aggregate(null);
        Assert.assertEquals(null, aggregator.getState());
        Assert.assertEquals(null, aggregator.aggregate(null, null));

        Number value = null;
        if (aggregator.getState() instanceof Long) {
            value = 1234L;
        } else if (aggregator.getState() instanceof Double) {
            value = 1234.456;
        } else if (aggregator.getState() instanceof BigDecimal) {
            value = new BigDecimal(12340.56789);
        }
        aggregator.aggregate(value);
        Assert.assertEquals(value, aggregator.getState());
    }

    private <V> void testNormalAggregator(MeasureAggregator<V> aggregator, V value1, V value2) {
        aggregator.reset();
        V res = aggregator.aggregate(value1, value2);
        if (aggregator.getClass().getName().contains("Max")) {
            // max
            if (value1 instanceof Long)
                Assert.assertEquals(Math.max((Long) value1, (Long) value2), res);
            else if (value1 instanceof Double)
                Assert.assertEquals(Math.max((Double) value1, (Double) value2), (Double) res, 0.000001);
            else
                Assert.assertEquals(((BigDecimal) value1).max((BigDecimal) value2), res);

        } else if (aggregator.getClass().getName().contains("Min")) {
            // min
            if (value1 instanceof Long)
                Assert.assertEquals(Math.min((Long) value1, (Long) value2), res);
            else if (value1 instanceof Double)
                Assert.assertEquals(Math.min((Double) value1, (Double) value2), (Double) res, 0.0000001);
            else
                Assert.assertEquals(((BigDecimal) value1).min((BigDecimal) value2), res);

        } else {
            // sum
            if (value1 instanceof Long)
                Assert.assertEquals(((Long) value1) + ((Long) value2), res);
            else if (value1 instanceof Double)
                Assert.assertEquals(((Double) value1) + ((Double) value2), (Double) res, 0.000001);
            else
                Assert.assertEquals(((BigDecimal) value1).add((BigDecimal) value2), res);
        }
    }

}

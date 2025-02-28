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

package org.apache.kylin.measure.percentile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.kylin.common.util.MathUtil;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.junit.Test;

public class PercentileAggregatorTest {
    private static int DEFAULT_COMPRESSION = 100;
    private static int[] compressions = new int[] { 100, 1000, 10000 };

    @Test
    public void testAggregate() {
        double compression = 100;
        int datasize = 10000;
        PercentileAggregator aggregator = new PercentileAggregator(compression);
        Random random = new Random();
        List<Double> dataset = Lists.newArrayListWithCapacity(datasize);
        for (int i = 0; i < datasize; i++) {
            double d = random.nextDouble();
            dataset.add(d);

            PercentileCounter c = new PercentileCounter(compression, 0.5);
            c.add(d);
            aggregator.aggregate(c);
        }
        Collections.sort(dataset);

        double actualResult = aggregator.getState().getResultEstimate();
        double expectResult = MathUtil.findMedianInSortedList(dataset);
        assertEquals(expectResult, actualResult, 0.001);
    }

    @Test
    public void testLargeSize() throws Exception {
        for (int compression : compressions) {
            testPercentileSize(2000000, null, compression);
        }
    }

    @Test
    public void testSmallSize() {
        for (int compression : compressions) {
            for (int i = compression; i < 4 * compression; i += compression / 3) {
                PercentileAggregator aggregator = createPercentileAggreator(i, null, compression);

                double estimate = getEstimateSize(i, 1, compression);
                assertTrue(Math.abs(getActualSize(aggregator) - estimate) / estimate < 0.3);

                aggregator.reset();
            }
        }
    }

    @Test
    public void testAggregation() throws Exception {
        for (int i = 5; i < 10; i += 3) {
            testAggregation(100000 * i);
        }
    }

    private void testAggregation(int numColumns) {
        List<PercentileAggregator> aggregators = Lists.newArrayList();
        int mergeNum = new Random().nextInt(numColumns);
        for (int i = 1; i <= numColumns; i++) {
            mergeNum = new Random().nextInt(numColumns - mergeNum);
            PercentileAggregator aggregator1 = createPercentileAggreator(mergeNum, 10, null);
            aggregators.add(aggregator1);
            i = i + mergeNum;
        }

        double sum = 0.0;
        for (PercentileAggregator aggregator2 : aggregators) {
            sum += getActualSize(aggregator2);
        }

        assertTrue(
                Math.abs(aggregators.size() * getEstimateSize(numColumns, aggregators.size()) - sum) * 1.0 / sum < 0.5);
        aggregators.clear();
    }

    private void testPercentileSize(int sumNums, Integer sqrtNum, Integer compresion) throws Exception {
        compresion = compresion == null ? DEFAULT_COMPRESSION : compresion;
        PercentileAggregator aggregator = createPercentileAggreator(sumNums, sqrtNum, compresion);

        double actual = getActualSize(aggregator);
        double estimate = getEstimateSize((int) aggregator.getState().getRegisters().size(), 1, compresion);

        assertTrue(Math.abs(actual - estimate) / actual < 0.3);
        aggregator.reset();
    }

    private PercentileAggregator createPercentileAggreator(int sumNums, Integer sqrtNum, Integer compression) {
        compression = compression == null ? DEFAULT_COMPRESSION : compression;
        PercentileAggregator aggregator = new PercentileAggregator(compression);
        Random random = new Random();

        for (int i = 0; i < sumNums; i++) {
            double d = 0;
            if (sqrtNum == null)
                d = random.nextInt(1000000000);
            else
                d = Math.sqrt(sqrtNum.intValue()) * random.nextGaussian();

            PercentileCounter c = new PercentileCounter(compression, 0.5);
            c.add(d);
            aggregator.aggregate(c);
        }
        return aggregator;
    }

    private double getEstimateSize(int numBefore, int numAfter) {
        return getEstimateSize(numBefore, numAfter, 0);
    }

    private double getEstimateSize(int numBefore, int numAfter, int compression) {
        return new PercentileCounter(compression == 0 ? DEFAULT_COMPRESSION : compression)
                .getBytesEstimate(numBefore * 1.0 / numAfter);
    }

    private double getActualSize(PercentileAggregator aggregator) {
        ByteBuffer buffer = ByteBuffer.allocate(10 * 1024 * 1024);
        aggregator.getState().writeRegisters(buffer);
        double actual = buffer.position();
        buffer.clear();
        return actual;
    }

}

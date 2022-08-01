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

package org.apache.kylin.measure.hllc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.kylin.common.util.Bytes;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author yangli9
 *
 */
@SuppressWarnings("deprecation")
@Ignore("HLLCounter takes over")
public class HLLCounterOldTest {

    ByteBuffer buf = ByteBuffer.allocate(1024 * 1024);
    Random rand1 = new Random(1);
    Random rand2 = new Random(2);
    Random rand3 = new Random(3);
    int errorCount1 = 0;
    int errorCount2 = 0;
    int errorCount3 = 0;

    @Test
    public void testOneAdd() throws IOException {
        HLLCounterOld hllc = new HLLCounterOld(14);
        HLLCounterOld one = new HLLCounterOld(14);
        for (int i = 0; i < 1000000; i++) {
            one.clear();
            one.add(rand1.nextInt());
            hllc.merge(one);
        }
        assertTrue(hllc.getCountEstimate() > 1000000 * 0.9);
    }

    @Test
    public void testPeekLength() throws IOException {
        HLLCounterOld hllc = new HLLCounterOld(10);
        HLLCounterOld copy = new HLLCounterOld(10);
        byte[] value = new byte[10];
        for (int i = 0; i < 200000; i++) {
            rand1.nextBytes(value);
            hllc.add(value);

            buf.clear();
            hllc.writeRegisters(buf);

            int len = buf.position();
            buf.position(0);
            assertEquals(len, hllc.peekLength(buf));

            copy.readRegisters(buf);
            assertEquals(len, buf.position());
            assertEquals(hllc, copy);
        }
        buf.clear();
    }

    private Set<String> generateTestData(int n) {
        Set<String> testData = new HashSet<String>();
        for (int i = 0; i < n; i++) {
            String[] samples = generateSampleData();
            for (String sample : samples) {
                testData.add(sample);
            }
        }
        return testData;
    }

    // simulate the visit (=visitor+id)
    private String[] generateSampleData() {

        StringBuilder buf = new StringBuilder();
        for (int i = 0; i < 19; i++) {
            buf.append(Math.abs(rand1.nextInt()) % 10);
        }
        String header = buf.toString();

        int size = Math.abs(rand3.nextInt()) % 9 + 1;
        String[] samples = new String[size];
        for (int k = 0; k < size; k++) {
            buf = new StringBuilder(header);
            buf.append("-");
            for (int i = 0; i < 10; i++) {
                buf.append(Math.abs(rand3.nextInt()) % 10);
            }
            samples[k] = buf.toString();
        }

        return samples;
    }

    @Test
    public void countTest() throws IOException {
        int n = 10;
        for (int i = 0; i < 5; i++) {
            count(n);
            n *= 10;
        }
    }

    private void count(int n) throws IOException {
        Set<String> testSet = generateTestData(n);

        HLLCounterOld hllc = newHLLC();
        for (String testData : testSet) {
            hllc.add(Bytes.toBytes(testData));
        }
        long estimate = hllc.getCountEstimate();
        double errorRate = hllc.getErrorRate();
        double actualError = (double) Math.abs(testSet.size() - estimate) / testSet.size();
        System.out.println(estimate);
        System.out.println(testSet.size());
        System.out.println(errorRate);
        System.out.println("=" + actualError);
        Assert.assertTrue(actualError < errorRate * 3.0);

        checkSerialize(hllc);
    }

    private void checkSerialize(HLLCounterOld hllc) throws IOException {
        long estimate = hllc.getCountEstimate();
        buf.clear();
        hllc.writeRegisters(buf);
        buf.flip();
        hllc.readRegisters(buf);
        Assert.assertEquals(estimate, hllc.getCountEstimate());
    }

    @Test
    public void mergeTest() throws IOException {
        double error = 0;
        int n = 100;
        for (int i = 0; i < n; i += 10) {
            double e = merge(i);
            error += e;
        }
        System.out.println("Total average error is " + error / n);

        System.out.println("  errorRateCount1 is " + errorCount1 + "!");
        System.out.println("  errorRateCount2 is " + errorCount2 + "!");
        System.out.println("  errorRateCount3 is " + errorCount3 + "!");

        Assert.assertTrue(errorCount1 <= n * 0.30);
        Assert.assertTrue(errorCount2 <= n * 0.05);
        Assert.assertTrue(errorCount3 <= n * 0.02);
    }

    private double merge(int round) throws IOException {
        int ln = 20;
        int dn = 100 * (round + 1);
        Set<String> testSet = new HashSet<String>();
        HLLCounterOld[] hllcs = new HLLCounterOld[ln];
        for (int i = 0; i < ln; i++) {
            hllcs[i] = newHLLC();
            for (int k = 0; k < dn; k++) {
                String[] samples = generateSampleData();
                for (String data : samples) {
                    testSet.add(data);
                    hllcs[i].add(Bytes.toBytes(data));
                }
            }
        }
        HLLCounterOld mergeHllc = newHLLC();
        for (HLLCounterOld hllc : hllcs) {
            mergeHllc.merge(serDes(hllc));
        }

        double errorRate = mergeHllc.getErrorRate();
        long estimate = mergeHllc.getCountEstimate();
        double actualError = Math.abs((double) (testSet.size() - estimate) / testSet.size());

        System.out.println(testSet.size() + "-" + estimate + " ~ " + actualError);
        Assert.assertTrue(actualError < 0.1);

        if (actualError > errorRate) {
            errorCount1++;
        }
        if (actualError > 2 * errorRate) {
            errorCount2++;
        }
        if (actualError > 3 * errorRate) {
            errorCount3++;
        }

        return actualError;
    }

    private HLLCounterOld serDes(HLLCounterOld hllc) throws IOException {
        buf.clear();
        hllc.writeRegisters(buf);
        buf.flip();
        HLLCounterOld copy = new HLLCounterOld(hllc.getPrecision());
        copy.readRegisters(buf);
        Assert.assertEquals(copy.getCountEstimate(), hllc.getCountEstimate());
        return copy;
    }

    @Test
    public void testPerformance() throws IOException {
        int N = 3; // reduce N HLLC into one
        int M = 1000; // for M times, use 100000 for real perf test

        HLLCounterOld[] samples = new HLLCounterOld[N];
        for (int i = 0; i < N; i++) {
            samples[i] = newHLLC();
            for (String str : generateTestData(10000))
                samples[i].add(str);
        }

        System.out.println("Perf test running ... ");
        long start = System.currentTimeMillis();
        HLLCounterOld sum = newHLLC();
        for (int i = 0; i < M; i++) {
            sum.clear();
            for (int j = 0; j < N; j++) {
                sum.merge(samples[j]);
                checkSerialize(sum);
            }
        }
        long duration = System.currentTimeMillis() - start;
        System.out.println("Perf test result: " + duration / 1000 + " seconds");
    }

    @Test
    public void testEquivalence() {
        byte[] a = new byte[] { 0, 3, 4, 42, 2, 2 };
        byte[] b = new byte[] { 3, 4, 42 };
        HLLCounterOld ha = new HLLCounterOld();
        HLLCounterOld hb = new HLLCounterOld();
        ha.add(a, 1, 3);
        hb.add(b);

        Assert.assertTrue(ha.getCountEstimate() == hb.getCountEstimate());
    }

    private HLLCounterOld newHLLC() {
        return new HLLCounterOld(16);
    }
}

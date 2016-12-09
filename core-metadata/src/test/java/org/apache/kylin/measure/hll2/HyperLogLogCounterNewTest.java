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
package org.apache.kylin.measure.hll2;

import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.measure.hllc.HyperLogLogPlusCounterOld;
import org.apache.kylin.measure.hllc.HyperLogLogPlusCounterNew;
import org.apache.kylin.measure.hllc.RegisterType;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by xiefan on 16-12-12.
 */
public class HyperLogLogCounterNewTest {
    ByteBuffer buf = ByteBuffer.allocate(1024 * 1024);
    Random rand1 = new Random(1);
    Random rand2 = new Random(2);
    Random rand3 = new Random(3);
    int errorCount1 = 0;
    int errorCount2 = 0;
    int errorCount3 = 0;

    @Test
    public void testOneAdd() throws IOException {
        HyperLogLogPlusCounterNew hllc = new HyperLogLogPlusCounterNew(14);
        HyperLogLogPlusCounterNew one = new HyperLogLogPlusCounterNew(14);
        for (int i = 0; i < 1000000; i++) {
            one.clear();
            one.add(rand1.nextInt());
            hllc.merge(one);
        }
        System.out.println(hllc.getCountEstimate());
        assertTrue(hllc.getCountEstimate() > 1000000 * 0.9);
    }

    @Test
    public void tesSparseEstimate() throws IOException {
        HyperLogLogPlusCounterNew hllc = new HyperLogLogPlusCounterNew(14);
        for (int i = 0; i < 10; i++) {
            hllc.add(i);
        }
        System.out.println(hllc.getCountEstimate());
        assertTrue(hllc.getCountEstimate() > 10 * 0.9);
    }

    @Test
    public void countTest() throws IOException {
        int n = 10;
        for (int i = 0; i < 5; i++) {
            count(n);
            n *= 10;
        }
    }

    @Test
    public void mergeTest() throws IOException {
        double error = 0;
        int n = 100;
        for (int i = 0; i < n; i++) {
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

    /*
    compare the result of two different hll counter
     */
    @Test
    public void compareResult() {
        int p = 12; //4096
        int m = 1 << p;
    
        for (int t = 0; t < 5; t++) {
            //compare sparse
            HyperLogLogPlusCounterOld oldCounter = new HyperLogLogPlusCounterOld(p);
            HyperLogLogPlusCounterNew newCounter = new HyperLogLogPlusCounterNew(p);
    
            for (int i = 0; i < 20; i++) {
                //int r = rand1.nextInt();
                oldCounter.add(i);
                newCounter.add(i);
            }
            assertEquals(RegisterType.SPARSE, newCounter.getRegisterType());
            assertEquals(oldCounter.getCountEstimate(), newCounter.getCountEstimate());
            //compare dense
            for (int i = 0; i < m; i++) {
                oldCounter.add(i);
                newCounter.add(i);
            }
            assertEquals(RegisterType.DENSE, newCounter.getRegisterType());
            assertEquals(oldCounter.getCountEstimate(), newCounter.getCountEstimate());
        }
    
    }

    @Test
    public void testPeekLength() throws IOException {
        HyperLogLogPlusCounterNew hllc = new HyperLogLogPlusCounterNew(10);
        HyperLogLogPlusCounterNew copy = new HyperLogLogPlusCounterNew(10);
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

    @Test
    public void testEquivalence() {
        byte[] a = new byte[] { 0, 3, 4, 42, 2, 2 };
        byte[] b = new byte[] { 3, 4, 42 };
        HyperLogLogPlusCounterNew ha = new HyperLogLogPlusCounterNew();
        HyperLogLogPlusCounterNew hb = new HyperLogLogPlusCounterNew();
        ha.add(a, 1, 3);
        hb.add(b);

        Assert.assertTrue(ha.getCountEstimate() == hb.getCountEstimate());
    }

    @Test
    public void testAutoChangeToSparse() {
        int p = 15;
        int m = 1 << p;
        HyperLogLogPlusCounterNew counter = new HyperLogLogPlusCounterNew(p);
        assertEquals(RegisterType.SPARSE, counter.getRegisterType());
        double over = HyperLogLogPlusCounterNew.overflowFactor * m;
        int overFlow = (int) over + 1000;
        for (int i = 0; i < overFlow; i++)
            counter.add(i);
        assertEquals(RegisterType.DENSE, counter.getRegisterType());
    }

    @Test
    public void testSerialilze() throws Exception {
        //test sparse serialize
        int p = 15;
        int m = 1 << p;
        HyperLogLogPlusCounterNew counter = new HyperLogLogPlusCounterNew(p);
        counter.add(123);
        assertEquals(RegisterType.SPARSE, counter.getRegisterType());
        checkSerialize(counter);
        //test dense serialize
        double over = HyperLogLogPlusCounterNew.overflowFactor * m;
        int overFlow = (int) over + 1000;
        for (int i = 0; i < overFlow; i++)
            counter.add(i);
        assertEquals(RegisterType.DENSE, counter.getRegisterType());
        checkSerialize(counter);
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

    private double merge(int round) throws IOException {
        int ln = 20;
        int dn = 100 * (round + 1);
        Set<String> testSet = new HashSet<String>();
        HyperLogLogPlusCounterNew[] hllcs = new HyperLogLogPlusCounterNew[ln];
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
        HyperLogLogPlusCounterNew mergeHllc = newHLLC();
        for (HyperLogLogPlusCounterNew hllc : hllcs) {
            mergeHllc.merge(hllc);
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

    private HyperLogLogPlusCounterNew newHLLC() {
        return new HyperLogLogPlusCounterNew(16);
    }

    private void count(int n) throws IOException {
        Set<String> testSet = generateTestData(n);

        HyperLogLogPlusCounterNew hllc = newHLLC();
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

    private void checkSerialize(HyperLogLogPlusCounterNew hllc) throws IOException {
        long estimate = hllc.getCountEstimate();
        buf.clear();
        hllc.writeRegisters(buf);
        buf.flip();
        hllc.readRegisters(buf);
        Assert.assertEquals(estimate, hllc.getCountEstimate());
    }
}

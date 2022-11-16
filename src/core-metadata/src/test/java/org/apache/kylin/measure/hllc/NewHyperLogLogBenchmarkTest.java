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

import java.nio.ByteBuffer;
import java.util.Random;

import org.junit.Ignore;
import org.junit.Test;

/**
 * Created by xiefan on 16-12-12.
 */
@Ignore("Save UT time")
@SuppressWarnings("deprecation")
public class NewHyperLogLogBenchmarkTest {

    public static final Random rand = new Random(1);

    final int testTimes = 100000;

    @Test
    public void denseToDenseRegisterMergeBenchmark() throws Exception {
        final int p = 15;
        int m = 1 << p;

        System.out.println("denseToDenseRegisterMergeBenchmark(), m : " + m);
        double oldFactor = HLLCounter.OVERFLOW_FACTOR;
        HLLCounter.OVERFLOW_FACTOR = 1.1; //keep sparse
        for (int cardinality : new int[] { m / 10, m / 5, m / 2, m }) {
            final HLLCounterOld oldCounter = new HLLCounterOld(p);
            final HLLCounterOld oldCounter2 = getRandOldCounter(p, cardinality);
            long oldTime = runTestCase(new TestCase() {
                @Override
                public void run() {

                    for (int i = 0; i < testTimes; i++) {
                        oldCounter.merge(oldCounter2);
                    }
                }
            });
            final HLLCounter newCounter = new HLLCounter(p, RegisterType.DENSE);
            final HLLCounter newCounter2 = new HLLCounter(p, RegisterType.DENSE);
            for (int i = 0; i < testTimes; i++)
                newCounter2.add(i);
            long newTime = runTestCase(new TestCase() {
                @Override
                public void run() {
                    for (int i = 0; i < testTimes; i++) {
                        newCounter.merge(newCounter2);
                    }
                }
            });
            assertEquals(RegisterType.DENSE, newCounter.getRegisterType());
            assertEquals(RegisterType.DENSE, newCounter2.getRegisterType());
            System.out.println("----------------------------");
            System.out.println("cardinality : " + cardinality);
            System.out.println("old time : " + oldTime);
            System.out.println("new time : " + newTime);
        }
        HLLCounter.OVERFLOW_FACTOR = oldFactor;
    }

    @Test
    public void sparseToSparseMergeBenchmark() throws Exception {
        final int p = 15;
        int m = 1 << p;
        System.out.println("sparseToSparseMergeBenchmark(), m : " + m);
        double oldFactor = HLLCounter.OVERFLOW_FACTOR;
        HLLCounter.OVERFLOW_FACTOR = 1.1; //keep sparse
        for (int cardinality : getTestDataDivide(m)) {
            final HLLCounterOld oldCounter = new HLLCounterOld(p);
            final HLLCounterOld oldCounter2 = getRandOldCounter(p, cardinality);
            long oldTime = runTestCase(new TestCase() {
                @Override
                public void run() {

                    for (int i = 0; i < testTimes; i++) {
                        oldCounter.merge(oldCounter2);
                    }
                }
            });
            final HLLCounter newCounter = new HLLCounter(p, RegisterType.SPARSE);
            final HLLCounter newCounter2 = getRandNewCounter(p, cardinality);
            long newTime = runTestCase(new TestCase() {
                @Override
                public void run() {
                    for (int i = 0; i < testTimes; i++) {
                        newCounter.merge(newCounter2);
                    }
                }
            });
            assertEquals(RegisterType.SPARSE, newCounter.getRegisterType());
            if (cardinality == 1) {
                assertEquals(RegisterType.SINGLE_VALUE, newCounter2.getRegisterType());
            } else {
                assertEquals(RegisterType.SPARSE, newCounter2.getRegisterType());
            }
            System.out.println("----------------------------");
            System.out.println("cardinality : " + cardinality);
            System.out.println("old time : " + oldTime);
            System.out.println("new time : " + newTime);
        }
        HLLCounter.OVERFLOW_FACTOR = oldFactor;
    }

    @Test
    public void sparseToDenseRegisterMergeBenchmark() throws Exception {
        final int p = 15;
        int m = 1 << p;
        System.out.println("sparseToDenseRegisterMergeBenchmark(), m : " + m);
        double oldFactor = HLLCounter.OVERFLOW_FACTOR;
        HLLCounter.OVERFLOW_FACTOR = 1.1; //keep sparse
        for (int cardinality : getTestDataDivide(m)) {
            System.out.println("----------------------------");
            System.out.println("cardinality : " + cardinality);
            final HLLCounterOld oldCounter = new HLLCounterOld(p);
            final HLLCounterOld oldCounter2 = getRandOldCounter(p, cardinality);
            long oldTime = runTestCase(new TestCase() {
                @Override
                public void run() {
                    for (int i = 0; i < testTimes; i++) {
                        oldCounter.merge(oldCounter2);
                    }
                }
            });
            final HLLCounter newCounter = new HLLCounter(p, RegisterType.DENSE);
            final HLLCounter newCounter2 = getRandNewCounter(p, cardinality);
            long newTime = runTestCase(new TestCase() {
                @Override
                public void run() {
                    for (int i = 0; i < testTimes; i++) {
                        newCounter.merge(newCounter2);
                    }
                }
            });
            assertEquals(RegisterType.DENSE, newCounter.getRegisterType());
            if (cardinality == 1) {
                assertEquals(RegisterType.SINGLE_VALUE, newCounter2.getRegisterType());
            } else {
                assertEquals(RegisterType.SPARSE, newCounter2.getRegisterType());
            }
            System.out.println("old time : " + oldTime);
            System.out.println("new time : " + newTime);
        }
        HLLCounter.OVERFLOW_FACTOR = oldFactor;
    }

    @Test
    public void sparseSerializeBenchmark() throws Exception {
        final int p = 15;
        int m = 1 << p;
        double oldFactor = HLLCounter.OVERFLOW_FACTOR;
        HLLCounter.OVERFLOW_FACTOR = 1.1; //keep sparse
        System.out.println("sparseSerializeBenchmark()");
        for (int cardinality : getTestDataDivide(m)) {
            System.out.println("----------------------------");
            System.out.println("cardinality : " + cardinality);
            final HLLCounterOld oldCounter = getRandOldCounter(p, cardinality);
            long oldTime = runTestCase(new TestCase() {
                @Override
                public void run() throws Exception {
                    ByteBuffer buf = ByteBuffer.allocate(1024 * 1024);
                    long totalBytes = 0;
                    for (int i = 0; i < testTimes; i++) {
                        buf.clear();
                        oldCounter.writeRegisters(buf);
                        totalBytes += buf.position();
                        buf.flip();
                        oldCounter.readRegisters(buf);
                    }
                    System.out.println("old serialize bytes : " + totalBytes / testTimes + "B");
                }
            });
            final HLLCounter newCounter = getRandNewCounter(p, cardinality);
            long newTime = runTestCase(new TestCase() {
                @Override
                public void run() throws Exception {
                    ByteBuffer buf = ByteBuffer.allocate(1024 * 1024);
                    long totalBytes = 0;
                    for (int i = 0; i < testTimes; i++) {
                        buf.clear();
                        newCounter.writeRegisters(buf);
                        totalBytes += buf.position();
                        buf.flip();
                        newCounter.readRegisters(buf);
                    }
                    System.out.println("new serialize bytes : " + totalBytes / testTimes + "B");
                }
            });
            if (cardinality == 1) {
                assertEquals(RegisterType.SINGLE_VALUE, newCounter.getRegisterType());
            } else {
                assertEquals(RegisterType.SPARSE, newCounter.getRegisterType());
            }
            System.out.println("old serialize time : " + oldTime);
            System.out.println("new serialize time : " + newTime);
        }
        HLLCounter.OVERFLOW_FACTOR = oldFactor;
    }

    @Test
    public void denseSerializeBenchmark() throws Exception {
        final int p = 15;
        final int m = 1 << p;
        double oldFactor = HLLCounter.OVERFLOW_FACTOR;
        HLLCounter.OVERFLOW_FACTOR = 0; //keep sparse
        System.out.println("denseSerializeBenchmark()");
        for (int cardinality : getTestDataDivide(m)) {
            System.out.println("----------------------------");
            System.out.println("cardinality : " + cardinality);
            final HLLCounterOld oldCounter = getRandOldCounter(p, cardinality);
            long oldTime = runTestCase(new TestCase() {
                @Override
                public void run() throws Exception {
                    ByteBuffer buf = ByteBuffer.allocate(1024 * 1024);
                    long totalBytes = 0;
                    for (int i = 0; i < testTimes; i++) {
                        buf.clear();
                        oldCounter.writeRegisters(buf);
                        totalBytes += buf.position();
                        buf.flip();
                        oldCounter.readRegisters(buf);
                    }
                    System.out.println("old serialize bytes : " + totalBytes / testTimes + "B");
                }
            });
            final HLLCounter newCounter = getRandNewCounter(p, cardinality, RegisterType.DENSE);
            long newTime = runTestCase(new TestCase() {
                @Override
                public void run() throws Exception {
                    ByteBuffer buf = ByteBuffer.allocate(1024 * 1024);
                    long totalBytes = 0;
                    for (int i = 0; i < testTimes; i++) {
                        buf.clear();
                        newCounter.writeRegisters(buf);
                        totalBytes += buf.position();
                        buf.flip();
                        newCounter.readRegisters(buf);
                    }
                    System.out.println("new serialize bytes : " + totalBytes / testTimes + "B");
                }
            });
            assertEquals(RegisterType.DENSE, newCounter.getRegisterType());
            System.out.println("old serialize time : " + oldTime);
            System.out.println("new serialize time : " + newTime);
        }
        HLLCounter.OVERFLOW_FACTOR = oldFactor;
    }

    interface TestCase {
        void run() throws Exception;
    }

    public long runTestCase(TestCase testCase) throws Exception {
        long startTime = System.currentTimeMillis();
        testCase.run();
        return System.currentTimeMillis() - startTime;
    }

    public HLLCounterOld getRandOldCounter(int p, int num) {
        HLLCounterOld c = new HLLCounterOld(p);
        for (int i = 0; i < num; i++)
            c.add(i);
        return c;
    }

    public HLLCounter getRandNewCounter(int p, int num) {
        HLLCounter c = new HLLCounter(p);
        for (int i = 0; i < num; i++)
            c.add(i);
        return c;
    }

    public HLLCounter getRandNewCounter(int p, int num, RegisterType type) {
        HLLCounter c = new HLLCounter(p, type);
        for (int i = 0; i < num; i++)
            c.add(i);
        return c;
    }

    public static int[] getTestDataDivide(int m) {
        return new int[] { 1, 5, 10, 100, m / 200, m / 100, m / 50, m / 20, m / 10 };
    }
}

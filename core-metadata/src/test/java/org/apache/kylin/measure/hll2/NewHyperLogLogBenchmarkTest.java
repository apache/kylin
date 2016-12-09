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

import org.apache.kylin.measure.hllc.HyperLogLogPlusCounterOld;
import org.apache.kylin.measure.hllc.HyperLogLogPlusCounterNew;
import org.apache.kylin.measure.hllc.RegisterType;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * Created by xiefan on 16-12-12.
 */
public class NewHyperLogLogBenchmarkTest {

    public static final Random rand = new Random(1);

    final int testTimes = 10000;

    @Test
    public void denseToDenseRegisterMergeBenchmark() throws Exception {
        final int p = 15;
        int m = 1 << p;

        System.out.println("m : " + m);
        double oldFactor = HyperLogLogPlusCounterNew.overflowFactor;
        HyperLogLogPlusCounterNew.overflowFactor = 1.1; //keep sparse
        for (int cardinality : getTestDataDivide(m)) {
            final HyperLogLogPlusCounterOld oldCounter = new HyperLogLogPlusCounterOld(p);
            final HyperLogLogPlusCounterOld oldCounter2 = getRandOldCounter(p, cardinality);
            long oldTime = runTestCase(new TestCase() {
                @Override
                public void run() {

                    for (int i = 0; i < testTimes; i++) {
                        oldCounter.merge(oldCounter2);
                    }
                }
            });
            final HyperLogLogPlusCounterNew newCounter = new HyperLogLogPlusCounterNew(p, RegisterType.DENSE);
            final HyperLogLogPlusCounterNew newCounter2 = new HyperLogLogPlusCounterNew(p, RegisterType.DENSE);
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
        HyperLogLogPlusCounterNew.overflowFactor = oldFactor;
    }

    @Test
    public void sparseToSparseMergeBenchmark() throws Exception {
        final int p = 15;
        int m = 1 << p;
        System.out.println("m : " + m);
        double oldFactor = HyperLogLogPlusCounterNew.overflowFactor;
        HyperLogLogPlusCounterNew.overflowFactor = 1.1; //keep sparse
        for (int cardinality : getTestDataDivide(m)) {
            final HyperLogLogPlusCounterOld oldCounter = new HyperLogLogPlusCounterOld(p);
            final HyperLogLogPlusCounterOld oldCounter2 = getRandOldCounter(p, cardinality);
            long oldTime = runTestCase(new TestCase() {
                @Override
                public void run() {

                    for (int i = 0; i < testTimes; i++) {
                        oldCounter.merge(oldCounter2);
                    }
                }
            });
            final HyperLogLogPlusCounterNew newCounter = new HyperLogLogPlusCounterNew(p);
            final HyperLogLogPlusCounterNew newCounter2 = getRandNewCounter(p, cardinality);
            long newTime = runTestCase(new TestCase() {
                @Override
                public void run() {
                    for (int i = 0; i < testTimes; i++) {
                        newCounter.merge(newCounter2);
                    }
                }
            });
            assertEquals(RegisterType.SPARSE, newCounter.getRegisterType());
            assertEquals(RegisterType.SPARSE, newCounter2.getRegisterType());
            System.out.println("----------------------------");
            System.out.println("cardinality : " + cardinality);
            System.out.println("old time : " + oldTime);
            System.out.println("new time : " + newTime);
        }
        HyperLogLogPlusCounterNew.overflowFactor = oldFactor;
    }

    @Test
    public void sparseToDenseRegisterMergeBenchmark() throws Exception {
        final int p = 15;
        int m = 1 << p;
        System.out.println("m : " + m);
        double oldFactor = HyperLogLogPlusCounterNew.overflowFactor;
        HyperLogLogPlusCounterNew.overflowFactor = 1.1; //keep sparse
        for (int cardinality : getTestDataDivide(m)) {
            System.out.println("----------------------------");
            System.out.println("cardinality : " + cardinality);
            final HyperLogLogPlusCounterOld oldCounter = new HyperLogLogPlusCounterOld(p);
            final HyperLogLogPlusCounterOld oldCounter2 = getRandOldCounter(p, cardinality);
            long oldTime = runTestCase(new TestCase() {
                @Override
                public void run() {
                    for (int i = 0; i < testTimes; i++) {
                        oldCounter.merge(oldCounter2);
                    }
                }
            });
            final HyperLogLogPlusCounterNew newCounter = new HyperLogLogPlusCounterNew(p, RegisterType.DENSE);
            final HyperLogLogPlusCounterNew newCounter2 = getRandNewCounter(p, cardinality);
            long newTime = runTestCase(new TestCase() {
                @Override
                public void run() {
                    for (int i = 0; i < testTimes; i++) {
                        newCounter.merge(newCounter2);
                    }
                }
            });
            assertEquals(RegisterType.DENSE, newCounter.getRegisterType());
            assertEquals(RegisterType.SPARSE, newCounter2.getRegisterType());
            System.out.println("old time : " + oldTime);
            System.out.println("new time : " + newTime);
        }
        HyperLogLogPlusCounterNew.overflowFactor = oldFactor;
    }

    @Test
    public void sparseSerializeBenchmark() throws Exception {
        final int p = 15;
        int m = 1 << p;
        double oldFactor = HyperLogLogPlusCounterNew.overflowFactor;
        HyperLogLogPlusCounterNew.overflowFactor = 1.1; //keep sparse
        for (int cardinality : getTestDataDivide(m)) {
            System.out.println("----------------------------");
            System.out.println("cardinality : " + cardinality);
            final HyperLogLogPlusCounterOld oldCounter = getRandOldCounter(p, cardinality);
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
            final HyperLogLogPlusCounterNew newCounter = getRandNewCounter(p, cardinality);
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
            assertEquals(RegisterType.SPARSE, newCounter.getRegisterType());
            System.out.println("old serialize time : " + oldTime);
            System.out.println("new serialize time : " + newTime);
        }
        HyperLogLogPlusCounterNew.overflowFactor = oldFactor;
    }

    @Test
    public void denseSerializeBenchmark() throws Exception {
        final int p = 15;
        int m = 1 << p;
        double oldFactor = HyperLogLogPlusCounterNew.overflowFactor;
        HyperLogLogPlusCounterNew.overflowFactor = 0; //keep sparse
        for (int cardinality : getTestDataDivide(m)) {
            System.out.println("----------------------------");
            System.out.println("cardinality : " + cardinality);
            final HyperLogLogPlusCounterOld oldCounter = getRandOldCounter(p, cardinality);
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
            final HyperLogLogPlusCounterNew newCounter = getRandNewCounter(p, cardinality, RegisterType.DENSE);
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
        HyperLogLogPlusCounterNew.overflowFactor = oldFactor;
    }

    interface TestCase {
        void run() throws Exception;
    }

    public long runTestCase(TestCase testCase) throws Exception {
        long startTime = System.currentTimeMillis();
        testCase.run();
        return System.currentTimeMillis() - startTime;
    }

    public HyperLogLogPlusCounterOld getRandOldCounter(int p, int num) {
        HyperLogLogPlusCounterOld c = new HyperLogLogPlusCounterOld(p);
        for (int i = 0; i < num; i++)
            c.add(i);
        return c;
    }

    public HyperLogLogPlusCounterNew getRandNewCounter(int p, int num) {
        HyperLogLogPlusCounterNew c = new HyperLogLogPlusCounterNew(p);
        for (int i = 0; i < num; i++)
            c.add(i);
        return c;
    }

    public HyperLogLogPlusCounterNew getRandNewCounter(int p, int num, RegisterType type) {
        HyperLogLogPlusCounterNew c = new HyperLogLogPlusCounterNew(p, type);
        for (int i = 0; i < num; i++)
            c.add(i);
        return c;
    }

    public static int[] getTestDataDivide(int m) {
        return new int[] { 1, 5, 10, 100, m / 200, m / 100, m / 50, m / 20, m / 10, m };
    }
}

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


package org.apache.kylin.engine.mr.steps;

import com.google.common.collect.Lists;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.apache.kylin.measure.hllc.RegisterType;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

@Ignore
public class NewCubeSamplingMethodTest {

    private static final int ROW_LENGTH = 10;

    private Integer[][] allCuboidsBitSet;

    private long baseCuboidId;

    private final int rowCount = 500000;

    @Before
    public void setup() {
        baseCuboidId = (1L << ROW_LENGTH) - 1;
        createAllCuboidBitSet();
        System.out.println("Totally have " + allCuboidsBitSet.length + " cuboids.");
    }

    @Ignore
    @Test
    public void testRandomData() throws Exception {
        List<List<String>> dataSet = getRandomDataset(rowCount);
        comparePerformanceBasic(dataSet);
        compareAccuracyBasic(dataSet);
    }


    @Ignore
    @Test
    public void testSmallCardData() throws Exception {
        List<List<String>> dataSet = getSmallCardDataset(rowCount);
        comparePerformanceBasic(dataSet);
        compareAccuracyBasic(dataSet);
    }


    public void comparePerformanceBasic(final List<List<String>> rows) throws Exception {
        //old hash method
        ByteArray[] colHashValues = getNewColHashValues(ROW_LENGTH);
        HLLCounter[] cuboidCounters = getNewCuboidCounters(allCuboidsBitSet.length);
        long start = System.currentTimeMillis();
        for (List<String> row : rows) {
            putRowKeyToHLL(row, colHashValues, cuboidCounters, Hashing.murmur3_32());
        }
        long totalTime = System.currentTimeMillis() - start;
        System.out.println("old method cost time : " + totalTime);
        //new hash method
        colHashValues = getNewColHashValues(ROW_LENGTH);
        cuboidCounters = getNewCuboidCounters(allCuboidsBitSet.length);
        start = System.currentTimeMillis();
        long[] valueHashLong = new long[allCuboidsBitSet.length];
        for (List<String> row : rows) {
            putRowKeyToHLLNew(row, valueHashLong, cuboidCounters, Hashing.murmur3_128());
        }
        totalTime = System.currentTimeMillis() - start;
        System.out.println("new method cost time : " + totalTime);
    }

    //test accuracy
    public void compareAccuracyBasic(final List<List<String>> rows) throws Exception {
        final long realCardinality = countCardinality(rows);
        System.out.println("real cardinality : " + realCardinality);
        //test1
        long t1 = runAndGetTime(new TestCase() {
            @Override
            public void run() throws Exception {
                HLLCounter counter = new HLLCounter(14, RegisterType.DENSE);
                final ByteArray[] colHashValues = getNewColHashValues(ROW_LENGTH);
                HashFunction hf = Hashing.murmur3_32();
                for (List<String> row : rows) {

                    int x = 0;
                    for (String field : row) {
                        Hasher hc = hf.newHasher();
                        colHashValues[x++].set(hc.putString(field).hash().asBytes());
                    }

                    Hasher hc = hf.newHasher();
                    for (int position = 0; position < colHashValues.length; position++) {
                        hc.putBytes(colHashValues[position].array());
                    }
                    counter.add(hc.hash().asBytes());
                }
                long estimate = counter.getCountEstimate();
                System.out.println("old method finished. Estimate cardinality : " + estimate + ". Error rate : " + countErrorRate(estimate, realCardinality));
            }
        });


        long t2 = runAndGetTime(new TestCase() {
            @Override
            public void run() throws Exception {
                HLLCounter counter = new HLLCounter(14, RegisterType.DENSE);
                HashFunction hf2 = Hashing.murmur3_128();
                long[] valueHashLong = new long[allCuboidsBitSet.length];
                for (List<String> row : rows) {

                    int x = 0;
                    for (String field : row) {
                        Hasher hc = hf2.newHasher();
                        byte[] bytes = hc.putString(x + field).hash().asBytes();
                        valueHashLong[x++] = Bytes.toLong(bytes);
                    }

                    long value = 0;
                    for (int position = 0; position < row.size(); position++) {
                        value += valueHashLong[position];
                    }
                    counter.addHashDirectly(value);
                }
                long estimate = counter.getCountEstimate();
                System.out.println("new method finished. Estimate cardinality : " + estimate + ". Error rate : " + countErrorRate(estimate, realCardinality));
            }
        });
    }

    public void createAllCuboidBitSet() {
        List<Long> allCuboids = Lists.newArrayList();
        List<Integer[]> allCuboidsBitSetList = Lists.newArrayList();
        for (long i = 1; i < baseCuboidId; i++) {
            allCuboids.add(i);
            addCuboidBitSet(i, allCuboidsBitSetList);
        }
        allCuboidsBitSet = allCuboidsBitSetList.toArray(new Integer[allCuboidsBitSetList.size()][]);
    }

    private ByteArray[] getNewColHashValues(int rowLength) {
        ByteArray[] colHashValues = new ByteArray[rowLength];
        for (int i = 0; i < rowLength; i++) {
            colHashValues[i] = new ByteArray();
        }
        return colHashValues;
    }

    private HLLCounter[] getNewCuboidCounters(int cuboidNum) {
        HLLCounter[] counters = new HLLCounter[cuboidNum];
        for (int i = 0; i < counters.length; i++)
            counters[i] = new HLLCounter(14, RegisterType.DENSE);
        return counters;
    }


    private void addCuboidBitSet(long cuboidId, List<Integer[]> allCuboidsBitSet) {
        Integer[] indice = new Integer[Long.bitCount(cuboidId)];

        long mask = Long.highestOneBit(baseCuboidId);
        int position = 0;
        for (int i = 0; i < ROW_LENGTH; i++) {
            if ((mask & cuboidId) > 0) {
                indice[position] = i;
                position++;
            }
            mask = mask >> 1;
        }

        allCuboidsBitSet.add(indice);

    }

    private long runAndGetTime(TestCase testCase) throws Exception {
        long start = System.currentTimeMillis();
        testCase.run();
        long totalTime = System.currentTimeMillis() - start;
        return totalTime;
    }

    interface TestCase {
        void run() throws Exception;
    }

    private void putRowKeyToHLL(List<String> row, ByteArray[] colHashValues, HLLCounter[] cuboidCounters, HashFunction hashFunction) {
        int x = 0;
        for (String field : row) {
            Hasher hc = hashFunction.newHasher();
            colHashValues[x++].set(hc.putString(field).hash().asBytes());
        }

        for (int i = 0, n = allCuboidsBitSet.length; i < n; i++) {
            Hasher hc = hashFunction.newHasher();
            for (int position = 0; position < allCuboidsBitSet[i].length; position++) {
                hc.putBytes(colHashValues[allCuboidsBitSet[i][position]].array());
                //hc.putBytes(seperator);
            }
            cuboidCounters[i].add(hc.hash().asBytes());
        }
    }

    private void putRowKeyToHLLNew(List<String> row, long[] hashValuesLong, HLLCounter[] cuboidCounters, HashFunction hashFunction) {
        int x = 0;
        for (String field : row) {
            Hasher hc = hashFunction.newHasher();
            byte[] bytes = hc.putString(x + field).hash().asBytes();
            hashValuesLong[x++] = Bytes.toLong(bytes);
        }

        for (int i = 0, n = allCuboidsBitSet.length; i < n; i++) {
            long value = 0;
            for (int position = 0; position < allCuboidsBitSet[i].length; position++) {
                value += hashValuesLong[allCuboidsBitSet[i][position]];
            }
            cuboidCounters[i].addHashDirectly(value);
        }
    }

    private List<List<String>> getRandomDataset(int size) {
        List<List<String>> rows = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            rows.add(getRandomRow());
        }
        return rows;
    }

    private List<List<String>> getSmallCardDataset(int size) {
        List<List<String>> rows = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            rows.add(getSmallCardRow());
        }
        return rows;
    }

    private List<String> getRandomRow() {
        List<String> row = new ArrayList<>();
        for (int i = 0; i < ROW_LENGTH; i++) {
            row.add(RandomStringUtils.random(10));
        }
        return row;
    }

    private String[] smallCardRow = {"abc", "bcd", "jifea", "feaifj"};

    private Random rand = new Random(System.currentTimeMillis());

    private List<String> getSmallCardRow() {
        List<String> row = new ArrayList<>();
        row.add(smallCardRow[rand.nextInt(smallCardRow.length)]);
        for (int i = 1; i < ROW_LENGTH; i++) {
            row.add("abc");
        }
        return row;
    }


    private int countCardinality(List<List<String>> rows) {
        Set<String> diffCols = new HashSet<String>();
        for (List<String> row : rows) {
            StringBuilder sb = new StringBuilder();
            for (String str : row) {
                sb.append(str);
            }
            diffCols.add(sb.toString());
        }
        return diffCols.size();
    }

    private double countErrorRate(long estimate, long real) {
        double rate = Math.abs((estimate - real) * 1.0) / real;
        return rate;
    }
}

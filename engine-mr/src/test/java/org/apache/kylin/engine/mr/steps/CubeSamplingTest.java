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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.junit.Before;
import org.junit.Test;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.hash.HashFunction;
import org.apache.kylin.shaded.com.google.common.hash.Hasher;
import org.apache.kylin.shaded.com.google.common.hash.Hashing;

/**
 */
public class CubeSamplingTest {

    private static final int ROW_LENGTH = 10;

    private final List<String> row = new ArrayList<String>(ROW_LENGTH);

    private Integer[][] allCuboidsBitSet;
    private HashFunction hf = null;
    private long baseCuboidId;
    private HLLCounter[] allCuboidsHLL = null;
    private final byte[] seperator = Bytes.toBytes(",");

    @Before
    public void setup() {

        baseCuboidId = (1L << ROW_LENGTH) - 1;
        List<Long> allCuboids = Lists.newArrayList();
        List<Integer[]> allCuboidsBitSetList = Lists.newArrayList();
        for (long i = 1; i < baseCuboidId; i++) {
            allCuboids.add(i);
            addCuboidBitSet(i, allCuboidsBitSetList);
        }

        allCuboidsBitSet = allCuboidsBitSetList.toArray(new Integer[allCuboidsBitSetList.size()][]);
        System.out.println("Totally have " + allCuboidsBitSet.length + " cuboids.");
        allCuboidsHLL = new HLLCounter[allCuboids.size()];
        for (int i = 0; i < allCuboids.size(); i++) {
            allCuboidsHLL[i] = new HLLCounter(14);
        }

        //  hf = Hashing.goodFastHash(32);
        //        hf = Hashing.md5();
        hf = Hashing.murmur3_32();
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

    @Test
    public void test() {

        long start = System.currentTimeMillis();
        List<String> row;
        for (int i = 0; i < 10000; i++) {
            row = getRandomRow();
            putRowKeyToHLL(row);
        }

        long duration = System.currentTimeMillis() - start;
        System.out.println("The test takes " + duration / 1000 + "seconds.");
    }

    private void putRowKeyToHLL(List<String> row) {
        byte[][] row_index = new byte[ROW_LENGTH][];
        int x = 0;
        for (String field : row) {
            Hasher hc = hf.newHasher();
            row_index[x++] = hc.putUnencodedChars(field).hash().asBytes();
        }

        for (int i = 0, n = allCuboidsBitSet.length; i < n; i++) {
            Hasher hc = hf.newHasher();
            for (int position = 0; position < allCuboidsBitSet[i].length; position++) {
                hc.putBytes(row_index[allCuboidsBitSet[i][position]]);
                hc.putBytes(seperator);
            }
            allCuboidsHLL[i].add(hc.hash().asBytes());
        }
    }

    private List<String> getRandomRow() {
        row.clear();
        for (int i = 0; i < ROW_LENGTH; i++) {
            row.add(RandomStringUtils.random(10));
        }
        return row;
    }
}

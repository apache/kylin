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

package org.apache.kylin.dict;

import org.apache.kylin.common.util.Dictionary;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.UUID;

/**
 * Created by xiefan on 16-12-28.
 */
@Ignore
public class TrieDictionaryForestBenchmark {

    private static final Random rand = new Random(System.currentTimeMillis());

    private CacheDictionary<String> oldDict;

    private CacheDictionary<String> newDict;

    private ArrayList<String> rawData;

    private int cardnality = 100;

    private int testTimes = 100000;

    @Before
    public void before() {
        int dataSize = 100 * 10000;
        TrieDictionaryBuilder<String> b1 = new TrieDictionaryBuilder<>(new StringBytesConverter());
        TrieDictionaryForestBuilder<String> b2 = new TrieDictionaryForestBuilder<String>(new StringBytesConverter(), 0, 5);
        this.rawData = genStringDataSet(dataSize);
        for (String str : this.rawData) {
            b1.addValue(str);
            b2.addValue(str);
        }
        this.oldDict = b1.build(0);
        this.newDict = b2.build();
        System.out.println("new dict split tree size : " + ((TrieDictionaryForest<String>) newDict).getTrees().size());
    }

    @Test
    public void testAll() {
        benchmarkWithoutCache();
        benchmarkWithCache();
    }

    @Test
    public void benchmarkWithoutCache() {
        oldDict.disableCache();
        newDict.disableCache();
        runBenchmark("benchmarkWithoutCache");
    }

    @Test
    public void benchmarkWithCache() {
        oldDict.enableCache();
        newDict.enableCache();
        runBenchmark("benchmarkWithCache");
    }

    private void runBenchmark(String testName) {
        long oldTime = runQueryValue(oldDict, cardnality, testTimes);
        long oldTime2 = runQueryId(rawData, oldDict, cardnality, testTimes);
        long oldTime3 = runQueryValueBytes(oldDict, cardnality, testTimes);
        long oldTime4 = runQueryValueBytes2(oldDict, cardnality, testTimes);
        long oldTime5 = runQueryIdByValueBytes(rawData, oldDict, cardnality, testTimes);
        long newTime = runQueryValue(newDict, cardnality, testTimes);
        long newTime2 = runQueryId(rawData, newDict, cardnality, testTimes);
        long newTime3 = runQueryValueBytes(newDict, cardnality, testTimes);
        long newTime4 = runQueryValueBytes2(newDict, cardnality, testTimes);
        long newTime5 = runQueryIdByValueBytes(rawData, newDict, cardnality, testTimes);
        System.out.println(testName);
        System.out.println("old dict value --> id : " + oldTime2);
        System.out.println("new dict value --> id :" + newTime2);
        System.out.println("old dict value bytes --> id : " + oldTime5);
        System.out.println("new dict value bytes--> id :" + newTime5);
        System.out.println("old dict id --> value : " + oldTime);
        System.out.println("new dict id --> value : " + newTime);
        System.out.println("old dict id --> value bytes : " + oldTime3);
        System.out.println("new dict id --> value bytes : " + newTime3);
        System.out.println("old dict id --> value bytes (method 2): " + oldTime4);
        System.out.println("new dict id --> value bytes (method 2): " + newTime4);
    }

    //id -- value
    private long runQueryValue(Dictionary<String> dict, int cardnality, int testTimes) {
        long startTime = System.currentTimeMillis();
        int step = 1;
        for (int i = 0; i < testTimes; i++) {
            for (int j = 0; j < cardnality; j++) {
                step |= dict.getValueFromId(j).length();
            }
        }
        return System.currentTimeMillis() - startTime;
    }

    private long runQueryValueBytes(Dictionary<String> dict, int cardnality, int testTimes) {
        long startTime = System.currentTimeMillis();
        int step = 1;
        for (int i = 0; i < testTimes; i++) {
            for (int j = 0; j < cardnality; j++) {
                //step |= dict.getValueBytesFromId(j).length;
                step |= dict.getValueFromId(j).length();
            }
        }
        return System.currentTimeMillis() - startTime;
    }

    private long runQueryValueBytes2(Dictionary<String> dict, int cardnality, int testTimes) {
        long startTime = System.currentTimeMillis();
        int step = 1;
        byte[] returnValue = new byte[2048];
        for (int i = 0; i < testTimes; i++) {
            for (int j = 0; j < cardnality; j++) {
                step |= dict.getValueFromId(j).length();
            }
        }
        return System.currentTimeMillis() - startTime;
    }

    private long runQueryId(ArrayList<String> rawData, Dictionary<String> dict, int cardnality, int testTimes) {
        long startTime = System.currentTimeMillis();
        int step = 1;
        for (int i = 0; i < testTimes; i++) {
            for (int j = 0; j < cardnality; j++) {
                step |= dict.getIdFromValue(rawData.get(j));
            }
        }
        return System.currentTimeMillis() - startTime;
    }

    private long runQueryIdByValueBytes(ArrayList<String> rawData, Dictionary<String> dict, int cardnality, int testTimes) {
        long startTime = System.currentTimeMillis();
        int step = 1;
        for (int i = 0; i < testTimes; i++) {
            for (int j = 0; j < cardnality; j++) {
                step |= dict.getIdFromValue(rawData.get(j));
            }
        }
        return System.currentTimeMillis() - startTime;
    }

    private ArrayList<String> genStringDataSet(int totalSize) {
        ArrayList<String> data = new ArrayList<>();
        for (int i = 0; i < totalSize; i++) {
            data.add(UUID.randomUUID().toString());
        }
        Collections.sort(data);
        return data;
    }
}

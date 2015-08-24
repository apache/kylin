/*
 * Copyright (C) 2011 Clearspring Technologies, Inc. 
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.common.topn;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.kylin.common.util.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class TopNCounterComparisonTest {

    private static final int TOP_K = 100;

    private static final int KEY_SPACE = 100 * TOP_K;

    private static final int TOTAL_RECORDS = 100 * KEY_SPACE;

    private static final int SPACE_SAVING_ROOM = 100;

    @Before
    public void setup() {
    }

    @After
    public void tearDown() {
    }

    protected String prepareTestDate() throws IOException {
        String[] allKeys = new String[KEY_SPACE];

        for (int i = 0; i < KEY_SPACE; i++) {
            allKeys[i] = RandomStringUtils.randomAlphabetic(10);
        }

        System.out.println("Start to create test random data...");
        long startTime = System.currentTimeMillis();
        ZipfDistribution zipf = new ZipfDistribution(KEY_SPACE - 1, 0.8);
        int keyIndex;

        File tempFile = File.createTempFile("ZipfDistribution", ".txt");

        if (tempFile.exists())
            FileUtils.forceDelete(tempFile);
        FileWriter fw = new FileWriter(tempFile);
        try {
            for (int i = 0; i < TOTAL_RECORDS; i++) {
                keyIndex = zipf.sample();
                fw.write(allKeys[keyIndex]);
                fw.write('\n');
            }
        } finally {
            if (fw != null)
                fw.close();
        }

        System.out.println("Create test data takes : " + (System.currentTimeMillis() - startTime) / 1000 + " seconds.");
        System.out.println("Test data in : " + tempFile.getAbsolutePath());

        return tempFile.getAbsolutePath();
    }

    //@Test
    public void testCorrectness() throws IOException {
        String dataFile = prepareTestDate();
        TopNCounterComparisonTest.SpaceSavingConsumer spaceSavingCounter = new TopNCounterComparisonTest.SpaceSavingConsumer();
        TopNCounterComparisonTest.HashMapConsumer accurateCounter = new TopNCounterComparisonTest.HashMapConsumer();

        for (TopNCounterComparisonTest.TestDataConsumer consumer : new TopNCounterComparisonTest.TestDataConsumer[] { spaceSavingCounter, accurateCounter }) {
            feedDataToConsumer(dataFile, consumer, 0, TOTAL_RECORDS);
        }

        FileUtils.forceDelete(new File(dataFile));

        compareResult(spaceSavingCounter, accurateCounter);
    }

    private void compareResult(TopNCounterComparisonTest.TestDataConsumer firstConsumer, TopNCounterComparisonTest.TestDataConsumer secondConsumer) {
        List<Pair<String, Double>> topResult1 = firstConsumer.getTopN(TOP_K);
        System.out.println("Get topN, Space saving takes " + firstConsumer.getSpentTime() / 1000 + " seconds");
        List<Pair<String, Double>> realSequence = secondConsumer.getTopN(TOP_K);
        System.out.println("Get topN, Merge sort takes " + secondConsumer.getSpentTime() / 1000 + " seconds");

        int error = 0;
        for (int i = 0; i < topResult1.size(); i++) {
            System.out.println("Compare " + i);

            //            if (topResult1.get(i).getSecond().doubleValue() == realSequence.get(i).getSecond().doubleValue()) {
            if (topResult1.get(i).getFirst().equals(realSequence.get(i).getFirst())
                    && topResult1.get(i).getSecond().doubleValue() == realSequence.get(i).getSecond().doubleValue()) {
                System.out.println("Passed; key:" + topResult1.get(i).getFirst() + ", value:" + topResult1.get(i).getSecond());
            } else {
                System.out.println("Failed; space saving key:" + topResult1.get(i).getFirst() + ", value:" + topResult1.get(i).getSecond());
                System.out.println("Failed; correct key:" + realSequence.get(i).getFirst() + ", value:" + realSequence.get(i).getSecond());
                error++;
            }
        }

        Assert.assertEquals(0, error);
    }

    @Test
    public void testParallelSpaceSaving() throws IOException, ClassNotFoundException {
        String dataFile = prepareTestDate();

        int PARALLEL = 10;
        TopNCounterComparisonTest.SpaceSavingConsumer[] parallelCounters = new TopNCounterComparisonTest.SpaceSavingConsumer[PARALLEL];

        for (int i = 0; i < PARALLEL; i++) {
            parallelCounters[i] = new TopNCounterComparisonTest.SpaceSavingConsumer();
        }

        int slice = TOTAL_RECORDS / PARALLEL;
        int startPosition = 0;
        for (int i = 0; i < PARALLEL; i++) {
            feedDataToConsumer(dataFile, parallelCounters[i], startPosition, startPosition + slice);
            startPosition += slice;
        }

        // merge counters

        //        for (int i = 1; i < PARALLEL; i++) {
        //            parallelCounters[0].vs.merge(parallelCounters[i].vs);
        //        }

        TopNCounterComparisonTest.SpaceSavingConsumer[] mergedCounters = mergeSpaceSavingConsumer(parallelCounters);

        TopNCounterComparisonTest.HashMapConsumer accurateCounter = new TopNCounterComparisonTest.HashMapConsumer();
        feedDataToConsumer(dataFile, accurateCounter, 0, TOTAL_RECORDS);

        compareResult(mergedCounters[0], accurateCounter);
        FileUtils.forceDelete(new File(dataFile));

    }

    private TopNCounterComparisonTest.SpaceSavingConsumer[] mergeSpaceSavingConsumer(TopNCounterComparisonTest.SpaceSavingConsumer[] consumers) throws IOException, ClassNotFoundException {
        List<TopNCounterComparisonTest.SpaceSavingConsumer> list = Lists.newArrayList();
        if (consumers.length == 1)
            return consumers;

        for (int i = 0, n = consumers.length; i < n; i = i + 2) {
            if (i + 1 < n) {
                consumers[i].vs.merge(consumers[i + 1].vs);
            }

            list.add(consumers[i]);
        }

        return mergeSpaceSavingConsumer(list.toArray(new TopNCounterComparisonTest.SpaceSavingConsumer[list.size()]));
    }

    private void feedDataToConsumer(String dataFile, TopNCounterComparisonTest.TestDataConsumer consumer, int startLine, int endLine) throws IOException {
        long startTime = System.currentTimeMillis();
        BufferedReader bufferedReader = new BufferedReader(new FileReader(dataFile));

        int lineNum = 0;
        String line = bufferedReader.readLine();
        while (line != null) {
            if (lineNum >= startLine && lineNum < endLine) {
                consumer.addElement(line, 1.0);
            }
            line = bufferedReader.readLine();
            lineNum++;
        }

        bufferedReader.close();
        System.out.println("feed data to " + consumer.getClass().getCanonicalName() + " take time (seconds): " + (System.currentTimeMillis() - startTime) / 1000);
    }

    private static interface TestDataConsumer {
        public void addElement(String elementKey, double value);

        public List<Pair<String, Double>> getTopN(int k);

        public long getSpentTime();
    }

    private class SpaceSavingConsumer implements TopNCounterComparisonTest.TestDataConsumer {
        private long timeSpent = 0;
        protected TopNCounter<String> vs;

        public SpaceSavingConsumer() {
            vs = new TopNCounter<String>(TOP_K * SPACE_SAVING_ROOM);

        }

        public void addElement(String key, double value) {
            //System.out.println("Adding " + key + ":" + incrementCount);
            long startTime = System.currentTimeMillis();
            vs.offer(key, value);
            timeSpent += (System.currentTimeMillis() - startTime);
        }

        @Override
        public List<Pair<String, Double>> getTopN(int k) {
            long startTime = System.currentTimeMillis();
            List<Counter<String>> tops = vs.topK(k);
            List<Pair<String, Double>> allRecords = Lists.newArrayList();

            for (Counter<String> counter : tops)
                allRecords.add(new Pair(counter.getItem(), counter.getCount()));
            timeSpent += (System.currentTimeMillis() - startTime);
            return allRecords;
        }

        @Override
        public long getSpentTime() {
            return timeSpent;
        }
    }

    private class HashMapConsumer implements TopNCounterComparisonTest.TestDataConsumer {

        private long timeSpent = 0;
        private Map<String, Double> hashMap;

        public HashMapConsumer() {
            hashMap = Maps.newHashMap();
        }

        public void addElement(String key, double value) {
            long startTime = System.currentTimeMillis();
            if (hashMap.containsKey(key)) {
                hashMap.put(key, hashMap.get(key) + value);
            } else {
                hashMap.put(key, value);
            }
            timeSpent += (System.currentTimeMillis() - startTime);
        }

        @Override
        public List<Pair<String, Double>> getTopN(int k) {
            long startTime = System.currentTimeMillis();
            List<Pair<String, Double>> allRecords = Lists.newArrayList();

            for (Map.Entry<String, Double> entry : hashMap.entrySet()) {
                allRecords.add(new Pair(entry.getKey(), entry.getValue()));
            }

            Collections.sort(allRecords, new Comparator<Pair<String, Double>>() {
                @Override
                public int compare(Pair<String, Double> o1, Pair<String, Double> o2) {
                    return o1.getSecond() < o2.getSecond() ? 1 : (o1.getSecond() > o2.getSecond() ? -1 : 0);
                }
            });
            timeSpent += (System.currentTimeMillis() - startTime);
            return allRecords.subList(0, k);
        }

        @Override
        public long getSpentTime() {
            return timeSpent;
        }
    }

}

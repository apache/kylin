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


import org.apache.kylin.common.util.MemoryBudgetController;
import org.junit.Ignore;
import org.junit.Test;

import java.io.*;
import java.util.*;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

/**
 * Created by xiefan on 16-10-26.
 */

public class TrieDictionaryForestTest {


    @Test
    public void testBasicFound() {
        ArrayList<String> strs = new ArrayList<String>();
        strs.add("part");
        strs.add("par");
        strs.add("partition");
        strs.add("party");
        strs.add("parties");
        strs.add("paint");
        Collections.sort(strs);
        int baseId = 0;
        TrieDictionaryForestBuilder<String> builder = newDictBuilder(strs, baseId);
        TrieDictionaryForest<String> dict = builder.build();
        dict.dump(System.out);
        int expectId = baseId;
        for (String s : strs) {
            System.out.println("value:" + s + "  expect id:" + expectId);
            assertEquals(expectId, dict.getIdFromValue(s));
            expectId++;
        }
        System.out.println("test ok");
    }

    @Test  //one string one tree
    public void testMultiTree() {
        ArrayList<String> strs = new ArrayList<String>();
        strs.add("part");
        strs.add("par");
        strs.add("partition");
        strs.add("party");
        strs.add("parties");
        strs.add("paint");
        strs.add("一二三");  //Chinese test
        strs.add("四五六");
        strs.add("");
        Collections.sort(strs, new ByteComparator<String>(new StringBytesConverter()));
        int baseId = 5;
        int maxTreeSize = 0;
        TrieDictionaryForestBuilder<String> builder = newDictBuilder(strs, baseId, maxTreeSize);
        TrieDictionaryForest<String> dict = builder.build();
        dict.dump(System.out);
        assertEquals(strs.size(), dict.getTrees().size());
        int expectId = baseId;
        for (String s : strs) {
            System.out.println("value:" + s + "  expect id:" + expectId);
            assertEquals(expectId, dict.getIdFromValue(s));
            expectId++;
        }
        System.out.println("test ok");
    }

    public void duplicateDataTest() {
        //todo
    }

    @Test
    public void testBigDataSet() {
        //h=generate data
        ArrayList<String> strs = new ArrayList<>();
        Iterator<String> it = new RandomStrings(100 * 10000).iterator();
        int totalSize = 0;
        final StringBytesConverter converter = new StringBytesConverter();
        while (it.hasNext()) {
            String str = it.next();
            byte[] data = converter.convertToBytes(str);
            if (data != null) {
                totalSize += data.length;
            }
            strs.add(str);
        }
        Collections.sort(strs);
        int baseId = 20;
        int maxTreeSize = totalSize / 10;
        System.out.println("data size:" + totalSize / 1024 + "KB  max tree size:" + maxTreeSize / 1024 + "KB");
        //create the answer set
        Map<String, Integer> idMap = rightIdMap(baseId, strs);
        //build tree
        TrieDictionaryForestBuilder<String> builder = newDictBuilder(strs, baseId, maxTreeSize);
        TrieDictionaryForest<String> dict = builder.build();
        System.out.println("tree num:" + dict.getTrees().size());
        //check
        for (Map.Entry<String, Integer> entry : idMap.entrySet()) {
            //System.out.println("my id:"+dict.getIdFromValue(entry.getKey())+" right id:"+entry.getValue());
            assertEquals(0, dict.getIdFromValue(entry.getKey()) - entry.getValue());
            assertEquals(entry.getKey(), dict.getValueFromId(entry.getValue()));
        }
    }

    @Test
    public void partOverflowTest() {
        ArrayList<String> str = new ArrayList<String>();
        // str.add("");
        str.add("part");
        str.add("par");
        str.add("partition");
        str.add("party");
        str.add("parties");
        str.add("paint");
        String longStr = "paintjkjdfklajkdljfkdsajklfjklsadjkjekjrklewjrklewjklrjklewjkljkljkljkljweklrjewkljrklewjrlkjewkljrkljkljkjlkjjkljkljkljkljlkjlkjlkjljdfadfads" + "dddddddddddddddddddddddddddddddddddddddddddddddddkfjadslkfjdsakljflksadjklfjklsjfkljwelkrjewkljrklewjklrjelkwjrklewjrlkjwkljerklkljlkjrlkwejrk" + "dddddddddddddddddddddddddddddddddddddddddddddddddkfjadslkfjdsakljflksadjklfjklsjfkljwelkrjewkljrklewjklrjelkwjrklewjrlkjwkljerklkljlkjrlkwejrk" + "dddddddddddddddddddddddddddddddddddddddddddddddddkfjadslkfjdsakljflksadjklfjklsjfkljwelkrjewkljrklewjklrjelkwjrklewjrlkjwkljerklkljlkjrlkwejrk" + "dddddddddddddddddddddddddddddddddddddddddddddddddkfjadslkfjdsakljflksadjklfjklsjfkljwelkrjewkljrklewjklrjelkwjrklewjrlkjwkljerklkljlkjrlkwejrk" + "dddddddddddddddddddddddddddddddddddddddddddddddddkfjadslkfjdsakljflksadjklfjklsjfkljwelkrjewkljrklewjklrjelkwjrklewjrlkjwkljerklkljlkjrlkwejrk"
                + "dddddddddddddddddddddddddddddddddddddddddddddddddkfjadslkfjdsakljflksadjklfjklsjfkljwelkrjewkljrklewjklrjelkwjrklewjrlkjwkljerklkljlkjrlkwejrk" + "dddddddddddddddddddddddddddddddddddddddddddddddddkfjadslkfjdsakljflksadjklfjklsjfkljwelkrjewkljrklewjklrjelkwjrklewjrlkjwkljerklkljlkjrlkwejrk";
        System.out.println("The length of the long string is " + longStr.length());
        str.add(longStr);

        str.add("zzzzzz" + longStr);// another long string
        int baseId = 10;
        int maxSize = 100 * 1024 * 1024;
        TrieDictionaryForestBuilder<String> b = newDictBuilder(str, baseId, maxSize);
        TrieDictionaryForest<String> dict = b.build();
        TreeSet<String> set = new TreeSet<String>();
        for (String s : str) {
            set.add(s);
        }
        // test basic id<==>value
        Iterator<String> it = set.iterator();
        int id = 0;
        int previousId = -1;
        for (; it.hasNext(); id++) {
            String value = it.next();

            // in case of overflow parts, there exist interpolation nodes
            // they exist to make sure that any node's part is shorter than 255
            int actualId = dict.getIdFromValue(value);
            assertTrue(actualId >= id);
            assertTrue(actualId > previousId);
            previousId = actualId;

            assertEquals(value, dict.getValueFromId(actualId));
        }
    }

    @Test
    public void notFoundTest() {
        ArrayList<String> str = new ArrayList<String>();
        str.add("part");
        str.add("par");
        str.add("partition");
        str.add("party");
        str.add("parties");
        str.add("paint");
        Collections.sort(str, new ByteComparator<String>(new StringBytesConverter()));

        ArrayList<String> notFound = new ArrayList<String>();
        notFound.add("");
        notFound.add("p");
        notFound.add("pa");
        notFound.add("pb");
        notFound.add("parti");
        notFound.add("partz");
        notFound.add("partyz");

        testStringDictionary(str, notFound);
    }


    @Test
    public void dictionaryContainTest() {
        ArrayList<String> str = new ArrayList<String>();
        str.add("part");
        str.add("part"); // meant to be dup
        str.add("par");
        str.add("partition");
        str.add("party");
        str.add("parties");
        str.add("paint");
        Collections.sort(str, new ByteComparator<String>(new StringBytesConverter()));
        int baseId = new Random().nextInt(100);
        TrieDictionaryForestBuilder<String> b = newDictBuilder(str, baseId);
        TrieDictionaryForest<String> dict = b.build();
        str.add("py");
        Collections.sort(str, new ByteComparator<String>(new StringBytesConverter()));
        b = newDictBuilder(str, baseId);
        baseId = new Random().nextInt(100);
        TrieDictionaryForest<String> dict2 = b.build();

        assertEquals(true, dict2.contains(dict));
        assertEquals(false, dict.contains(dict2));
    }

    @Test
    public void englishWordsTest() throws Exception {
        InputStream is = new FileInputStream("src/test/resources/dict/english-words.80 (scowl-2015.05.18).txt");
        ArrayList<String> str = loadStrings(is);
        Collections.sort(str, new ByteComparator<String>(new StringBytesConverter()));
        testStringDictionary(str, null);
    }

    @Test
    @Ignore
    public void categoryNamesTest() throws Exception {
        InputStream is = new FileInputStream("src/test/resources/dict/dw_category_grouping_names.dat");
        ArrayList<String> str = loadStrings(is);
        Collections.sort(str, new ByteComparator<String>(new StringBytesConverter()));
        testStringDictionary(str, null);
    }

    @Test
    public void serializeTest() {
        ArrayList<String> testData = getTestData(10);
        TrieDictionaryForestBuilder<String> b = newDictBuilder(testData, 10, 0);
        TrieDictionaryForest<String> dict = b.build();
        dict = testSerialize(dict);
        dict.dump(System.out);
        for (String str : testData) {
            assertEquals(str, dict.getValueFromId(dict.getIdFromValue(str)));
        }
    }


    private static TrieDictionaryForest<String> testSerialize(TrieDictionaryForest<String> dict) {
        try {
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            DataOutputStream dataout = new DataOutputStream(bout);
            dict.write(dataout);
            dataout.close();
            ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
            DataInputStream datain = new DataInputStream(bin);
            TrieDictionaryForest<String> r = new TrieDictionaryForest<>();
            //r.dump(System.out);
            r.readFields(datain);
            //r.dump(System.out);
            datain.close();
            return r;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /*@Test
    public void getIdFromValueBytesTest() throws Exception{
        String value = "一二三";
        BytesConverter<String> converter = new StringBytesConverter();
        TrieDictionaryForestBuilder<String> b = new TrieDictionaryForestBuilder<>(converter,0);
        b.addValue(value);
        TrieDictionaryForest<String> dict = b.build();
        dict.dump(System.out);
        byte[] data = converter.convertToBytes(value);
        int id = dict.getIdFromValueBytes(data,0,data.length);

    }*/

    //benchmark
    @Deprecated
    public void memoryUsageBenchmarkTest() throws Exception {
        //create data
        ArrayList<String> testData = getTestData((int) (Integer.MAX_VALUE * 0.8 / 640));
        int testTimes = 1;
        System.out.println("start memory:" + Runtime.getRuntime().maxMemory());
        System.out.println("start memory:" + Runtime.getRuntime().totalMemory());
        for (int i = 0; i < testTimes; i++) {
            long start = MemoryBudgetController.gcAndGetSystemAvailMB();
            TrieDictionaryBuilder<String> b = new TrieDictionaryBuilder<>(new StringBytesConverter());
            for (String str : testData)
                b.addValue(str);
            long end = MemoryBudgetController.gcAndGetSystemAvailMB();
            System.out.println("object trie memory usage:" + (end - start) + "MB");
            System.out.println("start memory:" + Runtime.getRuntime().maxMemory());
            System.out.println("start memory:" + Runtime.getRuntime().totalMemory());
            /*System.out.println(b == null);
            startMemUse = getSystemCurUsedMemory();
            TrieDictionary<String> dict = b.build(0);
            memUse = getSystemCurUsedMemory();
            System.out.println("array trie memory usage:"+(memUse-startMemUse)/(1024*1024)+"MB");
            System.out.println(b == null );
            System.out.println(dict == null);*/
        }


    }

    @Deprecated
    private long getSystemCurUsedMemory() throws Exception {
        System.gc();
        Thread.currentThread().sleep(1000);
        long totalMem = Runtime.getRuntime().totalMemory();
        long useMem = totalMem - Runtime.getRuntime().freeMemory();
        return useMem;
    }

    //@Test
    public void buildTimeBenchmarkTest() throws Exception {
        //create data
        ArrayList<String> testData = getTestData((int) (Integer.MAX_VALUE * 0.8 / 640));
        //build time compare
        int testTimes = 5;
        long oldDictTotalBuildTime = 0;
        long newDictTotalBuildTime = 0;

        //old dict
        System.gc();
        Thread.currentThread().sleep(1000);
        for (int i = 0; i < testTimes; i++) {
            int keep = 0;
            long startTime = System.currentTimeMillis();
            TrieDictionaryBuilder<String> oldTrieBuilder = new TrieDictionaryBuilder<>(new StringBytesConverter());
            for (String str : testData)
                oldTrieBuilder.addValue(str);
            TrieDictionary<String> oldDict = oldTrieBuilder.build(0);
            keep |= oldDict.getIdFromValue(testData.get(0));
            oldDictTotalBuildTime += (System.currentTimeMillis() - startTime);
            System.out.println("times:" + i);
        }

        //new dict
        System.gc();
        Thread.currentThread().sleep(1000);
        for (int i = 0; i < testTimes; i++) {
            int keep = 0;
            long startTime = System.currentTimeMillis();
            BytesConverter<String> converter = new StringBytesConverter();
            TrieDictionaryForestBuilder<String> newTrieBuilder = new TrieDictionaryForestBuilder<String>(converter, 0);
            for (String str : testData)
                newTrieBuilder.addValue(str);
            TrieDictionaryForest<String> newDict = newTrieBuilder.build();
            keep |= newDict.getIdFromValue(testData.get(0));
            newDictTotalBuildTime += (System.currentTimeMillis() - startTime);
            System.out.println("times:" + i);
        }


        System.out.println("compare build time.  Old trie : " + oldDictTotalBuildTime / 1000.0 + "s.New trie : " + newDictTotalBuildTime / 1000.0 + "s");
    }


    @Test
    public void queryTimeBenchmarkTest() throws Exception {
        int count = (int) (Integer.MAX_VALUE * 0.8 / 640);
        //int count = (int) (2);
        benchmarkStringDictionary(new RandomStrings(count));
    }


    private void evaluateDataSize(ArrayList<String> list) {
        long size = 0;
        for (String str : list)
            size += str.getBytes().length;
        System.out.println("test data size : " + size / (1024 * 1024) + " MB");
    }

    private void evaluateDataSize(int count) {
        RandomStrings rs = new RandomStrings(count);
        Iterator<String> itr = rs.iterator();
        long bytesCount = 0;
        while (itr.hasNext())
            bytesCount += itr.next().getBytes().length;
        System.out.println("test data size : " + bytesCount / (1024 * 1024) + " MB");
    }

    private static void benchmarkStringDictionary(Iterable<String> str) throws IOException {
        //System.out.println("test values:");
        Iterator<String> itr = str.iterator();
        ArrayList<String> testData = new ArrayList<>();
        while (itr.hasNext())
            testData.add(itr.next());
        Collections.sort(testData);
        TrieDictionaryForestBuilder<String> b = newDictBuilder(testData, 0);
        TrieDictionaryForest<String> dict = b.build();
        System.out.println("tree size:" + dict.getTrees().size());
        BytesConverter<String> converter = new StringBytesConverter();
        TreeSet<String> set = new TreeSet<String>();
        for (String s : testData) {
            set.add(s);
        }
        //System.out.println("print set");
        //System.out.println(set);
        //dict.dump(System.out);
        // prepare id==>value array and value==>id map
        HashMap<String, Integer> map = new HashMap<String, Integer>();
        String[] strArray = new String[set.size()];
        byte[][] array = new byte[set.size()][];
        Iterator<String> it = set.iterator();
        for (int id = 0; it.hasNext(); id++) {
            String value = it.next();
            map.put(value, id);
            strArray[id] = value;
            //array[id] = value.getBytes("UTF-8");
            array[id] = converter.convertToBytes(value);
        }


        // System.out.println("Dict size in bytes:  " +
        //MemoryUtil.deepMemoryUsageOf(dict));
        // System.out.println("Map size in bytes:   " +
        // MemoryUtil.deepMemoryUsageOf(map));
        // System.out.println("Array size in bytes: " +
        // MemoryUtil.deepMemoryUsageOf(strArray));

        // warm-up, said that code only got JIT after run 1k-10k times,
        // following jvm options may help
        // -XX:CompileThreshold=1500
        // -XX:+PrintCompilation
        System.out.println("Benchmark awaitig...");
        benchmark("Warm up", dict, set, map, strArray, array);
        benchmark("Benchmark", dict, set, map, strArray, array);
    }

    private static int benchmark(String msg, TrieDictionaryForest<String> dict, TreeSet<String> set, HashMap<String, Integer> map, String[] strArray, byte[][] array) {
        int n = set.size();
        int times = Math.max(10 * 1000 * 1000 / n, 1); // run 10 million lookups
        int keep = 0; // make sure JIT don't OPT OUT function calls under test
        byte[] valueBytes = new byte[dict.getSizeOfValue()];
        long start;

        // benchmark value==>id, via HashMap
        System.out.println(msg + " HashMap lookup value==>id");
        start = System.currentTimeMillis();
        for (int i = 0; i < times; i++) {
            for (int j = 0; j < n; j++) {
                keep |= map.get(strArray[j]);
            }
        }
        long timeValueToIdByMap = System.currentTimeMillis() - start;
        System.out.println(timeValueToIdByMap);

        // benchmark value==>id, via Dict
        System.out.println(msg + " Dictionary lookup value==>id");
        //dict.dump(System.out);

        start = System.currentTimeMillis();
        for (int i = 0; i < times; i++) {
            for (int j = 0; j < n; j++) {
                //System.out.println("looking for value:"+new String(array[j]));
                keep |= dict.getIdFromValueBytes(array[j], 0, array[j].length);
            }
        }
        long timeValueToIdByDict = System.currentTimeMillis() - start;
        System.out.println(timeValueToIdByDict);
        /*System.out.println("detail time.  get index time"+dict.getValueIndexTime.get()+" get value time"+
        dict.getValueTime.get() +"  binary search time:"+dict.binarySearchTime.get() + " copy time:"+
        dict.copyTime.get());*/

        // benchmark id==>value, via Array
        System.out.println(msg + " Array lookup id==>value");
        start = System.currentTimeMillis();
        for (int i = 0; i < times; i++) {
            for (int j = 0; j < n; j++) {
                keep |= strArray[j].length();
            }
        }
        long timeIdToValueByArray = System.currentTimeMillis() - start;
        System.out.println(timeIdToValueByArray);

        // benchmark id==>value, via Dict
        System.out.println(msg + " Dictionary lookup id==>value");
        start = System.currentTimeMillis();
        for (int i = 0; i < times; i++) {
            for (int j = 0; j < n; j++) {
                keep |= dict.getValueBytesFromId(j, valueBytes, 0);
            }
        }
        long timeIdToValueByDict = System.currentTimeMillis() - start;
        System.out.println(timeIdToValueByDict);
        /*System.out.println("detail time.  get index time"+dict.getValueIndexTime2.get()+" get value time"+
                dict.getValueTime2.get());*/

        return keep;
    }

    private static void testStringDictionary(ArrayList<String> str, ArrayList<String> notFound) {
        int baseId = new Random().nextInt(100);
        TrieDictionaryForestBuilder<String> b = newDictBuilder(str, baseId, 2);
        TrieDictionaryForest<String> dict = b.build();
        //dict.dump(System.out);
        TreeSet<String> set = new TreeSet<String>();
        for (String s : str) {
            set.add(s);
        }

        // test serialize
        //dict = testSerialize(dict);

        // test basic id<==>value
        Iterator<String> it = set.iterator();
        int id = baseId;
        for (; it.hasNext(); id++) {
            String value = it.next();
            // System.out.println("checking " + id + " <==> " + value);

            assertEquals(id, dict.getIdFromValue(value));
            assertEquals(value, dict.getValueFromId(id));
        }

        //test not found value
        if (notFound != null) {
            for (String s : notFound) {
                try {
                    int nullId = dict.getIdFromValue(s);
                    System.out.println("null value id:" + nullId);
                    fail("For not found value '" + s + "', IllegalArgumentException is expected");
                } catch (IllegalArgumentException e) {
                    // good
                }
            }
        }
        int maxId = dict.getMaxId();
        int[] notExistIds = {-10, -20, -Integer.MIN_VALUE, -Integer.MAX_VALUE, maxId + 1, maxId + 2};
        for (Integer i : notExistIds) {
            try {
                dict.getValueFromId(i);
                fail("For not found id '" + i + "', IllegalArgumentException is expected");
            } catch (IllegalArgumentException e) {
                // good
            }
        }

        // test null value
        int nullId = dict.getIdFromValue(null);
        assertNull(dict.getValueFromId(nullId));
        int nullId2 = dict.getIdFromValueBytes(null, 0, 0);
        assertEquals(dict.getValueBytesFromId(nullId2, null, 0), -1);
        assertEquals(nullId, nullId2);
    }

    private Map<String, Integer> rightIdMap(int baseId, ArrayList<String> strs) {
        Map<String, Integer> result = new HashMap<>();
        int expectId = baseId;
        for (String str : strs) {
            result.put(str, expectId);
            expectId++;
        }
        return result;
    }

    private static TrieDictionaryForestBuilder<String> newDictBuilder(Iterable<String> strs, int baseId) {
        TrieDictionaryForestBuilder<String> b = new TrieDictionaryForestBuilder<String>(new StringBytesConverter(), baseId);
        for (String s : strs)
            b.addValue(s);
        return b;
    }

    private static TrieDictionaryForestBuilder<String> newDictBuilder(Iterable<String> strs, int baseId, int treeSize) {
        TrieDictionaryForestBuilder<String> b = new TrieDictionaryForestBuilder<String>(new StringBytesConverter(), baseId);
        TrieDictionaryForestBuilder.MaxTrieTreeSize = treeSize;
        for (String s : strs)
            b.addValue(s);
        return b;
    }

    private static class RandomStrings implements Iterable<String> {
        final private int size;

        public RandomStrings(int size) {
            this.size = size;
            //System.out.println("size = " + size);
        }

        @Override
        public Iterator<String> iterator() {
            return new Iterator<String>() {
                Random rand = new Random(System.currentTimeMillis());
                int i = 0;

                @Override
                public boolean hasNext() {
                    return i < size;
                }

                @Override
                public String next() {
                    if (hasNext() == false)
                        throw new NoSuchElementException();

                    i++;
                    //if (i % 1000000 == 0)
                    //System.out.println(i);

                    return nextString();
                }

                private String nextString() {
                    StringBuffer buf = new StringBuffer();
                    for (int i = 0; i < 64; i++) {
                        int v = rand.nextInt(16);
                        char c;
                        if (v >= 0 && v <= 9)
                            c = (char) ('0' + v);
                        else
                            c = (char) ('a' + v - 10);
                        buf.append(c);
                    }
                    return buf.toString();
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }

    private static ArrayList<String> loadStrings(InputStream is) throws Exception {
        ArrayList<String> r = new ArrayList<String>();
        BufferedReader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
        try {
            String word;
            while ((word = reader.readLine()) != null) {
                word = word.trim();
                if (word.isEmpty() == false)
                    r.add(word);
            }
        } finally {
            reader.close();
            is.close();
        }
        return r;
    }


    private ArrayList<String> getTestData(int count) {
        RandomStrings rs = new RandomStrings(count);
        Iterator<String> itr = rs.iterator();
        ArrayList<String> testData = new ArrayList<>();
        while (itr.hasNext())
            testData.add(itr.next());
        Collections.sort(testData, new ByteComparator<String>(new StringBytesConverter()));
        evaluateDataSize(testData);
        return testData;
    }

}

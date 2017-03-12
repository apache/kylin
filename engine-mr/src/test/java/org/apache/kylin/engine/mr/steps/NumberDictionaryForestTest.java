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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.io.Text;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.dict.NumberDictionary;
import org.apache.kylin.dict.NumberDictionaryBuilder;
import org.apache.kylin.dict.NumberDictionaryForestBuilder;
import org.apache.kylin.dict.TrieDictionaryForest;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Created by xiefan on 16-11-2.
 */


public class NumberDictionaryForestTest {

    @Test
    public void testNumberDictionaryForestLong() {
        List<String> list = randomLongData(100);
        testData(list, SelfDefineSortableKey.TypeFlag.INTEGER_FAMILY_TYPE);
    }

    @Test
    public void testNumberDictionaryForestDouble() {
        List<String> list = randomDoubleData(100);
        testData(list, SelfDefineSortableKey.TypeFlag.DOUBLE_FAMILY_TYPE);
    }

    private void testData(List<String> list, SelfDefineSortableKey.TypeFlag flag) {
        //stimulate map-reduce job
        ArrayList<SelfDefineSortableKey> keyList = createKeyList(list, (byte) flag.ordinal());
        Collections.sort(keyList);
        //build tree
        NumberDictionaryForestBuilder b = new NumberDictionaryForestBuilder(0, 0);

        for (SelfDefineSortableKey key : keyList) {
            String fieldValue = printKey(key);
            b.addValue(fieldValue);
        }
        TrieDictionaryForest<String> dict = b.build();
        dict.dump(System.out);
        ArrayList<Integer> resultIds = new ArrayList<>();
        for (SelfDefineSortableKey key : keyList) {
            String fieldValue = getFieldValue(key);
            resultIds.add(dict.getIdFromValue(fieldValue));
            assertEquals(fieldValue, dict.getValueFromId(dict.getIdFromValue(fieldValue)));
        }
        assertTrue(isIncreasedOrder(resultIds, new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o1.compareTo(o2);
            }
        }));
    }

    @Test
    public void serializeTest() {
        List<String> testData = new ArrayList<>();
        testData.add("1");
        testData.add("2");
        testData.add("100");
        //TrieDictionaryForestBuilder.MaxTrieTreeSize = 0;
        NumberDictionaryForestBuilder b = new NumberDictionaryForestBuilder();
        for (String str : testData)
            b.addValue(str);
        TrieDictionaryForest<String> dict = b.build();
        dict = testSerialize(dict);
        dict.dump(System.out);
        for (String str : testData) {
            assertEquals(str, dict.getValueFromId(dict.getIdFromValue(str)));
        }
    }


    @Test
    public void testVerySmallDouble() {
        List<String> testData = new ArrayList<>();
        testData.add(-1.0 + "");
        testData.add(Double.MIN_VALUE + "");
        testData.add("1.01");
        testData.add("2.0");
        NumberDictionaryForestBuilder b = new NumberDictionaryForestBuilder();
        for (String str : testData)
            b.addValue(str);
        TrieDictionaryForest<String> dict = b.build();
        dict.dump(System.out);

        NumberDictionaryBuilder b2 = new NumberDictionaryBuilder();
        for (String str : testData)
            b2.addValue(str);
        NumberDictionary<String> dict2 = b2.build(0);
        dict2.dump(System.out);

    }

    @Test
    public void testMerge() {
        // mimic the logic as in MergeCuboidMapper
        NumberDictionaryForestBuilder b1 = new NumberDictionaryForestBuilder();
        b1.addValue("0");
        b1.addValue("3");
        b1.addValue("23");
        TrieDictionaryForest<String> dict1 = b1.build();

        NumberDictionaryForestBuilder b2 = new NumberDictionaryForestBuilder();
        b2.addValue("0");
        b2.addValue("2");
        b2.addValue("3");
        b2.addValue("15");
        b2.addValue("23");
        TrieDictionaryForest<String> dict2 = b2.build();

        assertTrue(dict1.getSizeOfId() == dict2.getSizeOfId());
        assertTrue(dict1.getSizeOfValue() == dict2.getSizeOfValue());

        byte[] buf = new byte[dict1.getSizeOfValue()];

        {
            int newId = dict2.getIdFromValue(dict1.getValueFromId(0));
            assertTrue(newId == 0);
        }
        {

            int newId = dict2.getIdFromValue(dict1.getValueFromId(1));
            assertTrue(newId == 2);
        }
        {
            int newId = dict2.getIdFromValue(dict1.getValueFromId(2));
            assertTrue(newId == 4);
        }
    }

    @Ignore
    @Test
    public void testDecimalsWithBeginZero() {
        List<String> testData = new ArrayList<>();
        testData.add("000000000000000000000000000.4868");
        testData.add("00000000000000000000000000000000000000");
        NumberDictionaryForestBuilder b = new NumberDictionaryForestBuilder();
        for (String str : testData)
            b.addValue(str);
        TrieDictionaryForest<String> dict = b.build();
        dict.dump(System.out);
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

    private List<String> randomLongData(int count) {
        Random rand = new Random(System.currentTimeMillis());
        ArrayList<String> list = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            list.add(rand.nextLong() + "");
        }
        list.add(Long.MAX_VALUE + "");
        list.add(Long.MIN_VALUE + "");
        return list;
    }

    private List<String> randomDoubleData(int count) {
        Random rand = new Random(System.currentTimeMillis());
        ArrayList<String> list = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            String str = rand.nextDouble() + "";
            if (str.contains("E"))
                continue;
            list.add(str);
        }
        list.add("-1");
        return list;
    }

    private ArrayList<SelfDefineSortableKey> createKeyList(List<String> strNumList, byte typeFlag) {
        int partationId = 0;
        ArrayList<SelfDefineSortableKey> keyList = new ArrayList<>();
        for (String str : strNumList) {
            ByteBuffer keyBuffer = ByteBuffer.allocate(4096);
            int offset = keyBuffer.position();
            keyBuffer.put(Bytes.toBytes(partationId)[3]);
            keyBuffer.put(Bytes.toBytes(str));
            //System.out.println(Arrays.toString(keyBuffer.array()));
            byte[] valueField = Bytes.copy(keyBuffer.array(), 1, keyBuffer.position() - offset - 1);
            //System.out.println("new string:"+new String(valueField));
            //System.out.println("arrays toString:"+Arrays.toString(valueField));
            Text outputKey = new Text();
            outputKey.set(keyBuffer.array(), offset, keyBuffer.position() - offset);
            SelfDefineSortableKey sortableKey = new SelfDefineSortableKey();
            sortableKey.init(outputKey, typeFlag);
            keyList.add(sortableKey);
        }
        return keyList;
    }

    private String printKey(SelfDefineSortableKey key) {
        Text data = key.getText();
        String fieldValue = Bytes.toString(data.getBytes(), 1, data.getLength() - 1);
        System.out.println("type flag:" + key.getTypeId() + " fieldValue:" + fieldValue);
        return fieldValue;
    }

    private String getFieldValue(SelfDefineSortableKey key) {
        Text data = key.getText();
        return Bytes.toString(data.getBytes(), 1, data.getLength() - 1);
    }

    private <T> boolean isIncreasedOrder(List<T> list, Comparator<T> comp) {
        int flag;
        T previous = null;
        for (T t : list) {
            if (previous == null) previous = t;
            else {
                flag = comp.compare(previous, t);
                if (flag > 0) return false;
                previous = t;
            }
        }
        return true;
    }
}

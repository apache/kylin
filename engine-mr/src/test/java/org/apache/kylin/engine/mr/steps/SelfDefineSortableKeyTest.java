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

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.hadoop.io.Text;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.junit.Test;

/**
 * Created by xiefan on 16-11-2.
 */
public class SelfDefineSortableKeyTest {

    @Test
    public void testSortLong() {
        Random rand = new Random(System.currentTimeMillis());
        ArrayList<Long> longList = new ArrayList<>();
        int count = 10;
        for (int i = 0; i < count; i++) {
            longList.add(rand.nextLong());
        }
        longList.add(0L);
        longList.add(0L); //test duplicate
        longList.add(-1L); //test negative number
        longList.add(Long.MAX_VALUE);
        longList.add(Long.MIN_VALUE);

        System.out.println("test numbers:" + longList);
        ArrayList<String> strNumList = listToStringList(longList);
        //System.out.println("test num strs list:"+strNumList);
        ArrayList<SelfDefineSortableKey> keyList = createKeyList(strNumList, (byte) SelfDefineSortableKey.TypeFlag.INTEGER_FAMILY_TYPE.ordinal());
        System.out.println(keyList.get(0).isIntegerFamily());
        Collections.sort(keyList);
        ArrayList<String> strListAftereSort = new ArrayList<>();
        for (SelfDefineSortableKey key : keyList) {
            String str = printKey(key);
            strListAftereSort.add(str);
        }
        assertTrue(isIncreasedOrder(strListAftereSort, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                Long l1 = Long.parseLong(o1);
                Long l2 = Long.parseLong(o2);
                return l1.compareTo(l2);
            }
        }));
    }

    @Test
    public void testSortDouble() {
        Random rand = new Random(System.currentTimeMillis());
        ArrayList<Double> doubleList = new ArrayList<>();
        int count = 10;
        for (int i = 0; i < count; i++) {
            doubleList.add(rand.nextDouble());
        }
        doubleList.add(0.0);
        doubleList.add(0.0); //test duplicate
        doubleList.add(-1.0); //test negative number
        doubleList.add(Double.MAX_VALUE);
        doubleList.add(-Double.MAX_VALUE);
        //System.out.println(Double.MIN_VALUE);

        System.out.println("test numbers:" + doubleList);
        ArrayList<String> strNumList = listToStringList(doubleList);
        //System.out.println("test num strs list:"+strNumList);
        ArrayList<SelfDefineSortableKey> keyList = createKeyList(strNumList, (byte) SelfDefineSortableKey.TypeFlag.DOUBLE_FAMILY_TYPE.ordinal());
        Collections.sort(keyList);
        ArrayList<String> strListAftereSort = new ArrayList<>();
        for (SelfDefineSortableKey key : keyList) {
            String str = printKey(key);
            strListAftereSort.add(str);
        }
        assertTrue(isIncreasedOrder(strListAftereSort, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                Double d1 = Double.parseDouble(o1);
                Double d2 = Double.parseDouble(o2);
                return d1.compareTo(d2);
            }
        }));
    }

    @Test
    public void testSortNormalString() {
        int count = 10;
        ArrayList<String> strList = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            UUID uuid = UUID.randomUUID();
            strList.add(uuid.toString());
        }
        strList.add("hello");
        strList.add("hello"); //duplicate
        strList.add("123");
        strList.add("");
        ArrayList<SelfDefineSortableKey> keyList = createKeyList(strList, (byte) SelfDefineSortableKey.TypeFlag.NONE_NUMERIC_TYPE.ordinal());
        Collections.sort(keyList);
        ArrayList<String> strListAftereSort = new ArrayList<>();
        for (SelfDefineSortableKey key : keyList) {
            String str = printKey(key);
            strListAftereSort.add(str);
        }
        assertTrue(isIncreasedOrder(strListAftereSort, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }
        }));
    }

    @Test
    public void testPerformance() {
        Random rand = new Random(System.currentTimeMillis());
        ArrayList<Double> doubleList = new ArrayList<>();
        int count = 10 * 10000;
        for (int i = 0; i < count; i++) {
            doubleList.add(rand.nextDouble());
        }
        doubleList.add(0.0);
        doubleList.add(0.0); //test duplicate
        doubleList.add(-1.0); //test negative number
        doubleList.add(Double.MAX_VALUE);
        doubleList.add(-Double.MAX_VALUE);
        //System.out.println(Double.MIN_VALUE);


        ArrayList<String> strNumList = listToStringList(doubleList);
        //System.out.println("test num strs list:"+strNumList);
        ArrayList<SelfDefineSortableKey> keyList = createKeyList(strNumList, (byte) SelfDefineSortableKey.TypeFlag.DOUBLE_FAMILY_TYPE.ordinal());

        System.out.println("start to test str sort");
        long start = System.currentTimeMillis();
        Collections.sort(strNumList);
        System.out.println("sort time : " + (System.currentTimeMillis() - start));


        System.out.println("start to test double sort");
        start = System.currentTimeMillis();
        Collections.sort(keyList);
        System.out.println("sort time : " + (System.currentTimeMillis() - start));

        List<ByteArray> byteList = new ArrayList<>();
        for (String str : strNumList) {
            byteList.add(new ByteArray(Bytes.toBytes(str)));
        }
        System.out.println("start to test byte array sort");
        start = System.currentTimeMillis();
        Collections.sort(byteList);
        System.out.println("sort time : " + (System.currentTimeMillis() - start));

        //test new key
        List<SelfDefineSortableKey> newKeyList = new ArrayList<>();
        for (String str : strNumList) {
            SelfDefineSortableKey key = new SelfDefineSortableKey();
            key.init(new Text(str), (byte) SelfDefineSortableKey.TypeFlag.DOUBLE_FAMILY_TYPE.ordinal());
            newKeyList.add(key);
        }
        System.out.println("start to test new sortable key");
        start = System.currentTimeMillis();
        Collections.sort(newKeyList);
        System.out.println("sort time : " + (System.currentTimeMillis() - start));
    }


    @Test
    public void testIllegalNumber() {
        Random rand = new Random(System.currentTimeMillis());
        ArrayList<Double> doubleList = new ArrayList<>();
        int count = 10;
        for (int i = 0; i < count; i++) {
            doubleList.add(rand.nextDouble());
        }
        doubleList.add(0.0);
        doubleList.add(0.0); //test duplicate
        doubleList.add(-1.0); //test negative number
        doubleList.add(Double.MAX_VALUE);
        doubleList.add(-Double.MAX_VALUE);
        //System.out.println(Double.MIN_VALUE);

        System.out.println("test numbers:" + doubleList);
        ArrayList<String> strNumList = listToStringList(doubleList);
        strNumList.add("fjaeif"); //illegal type
        //System.out.println("test num strs list:"+strNumList);
        try {
            ArrayList<SelfDefineSortableKey> keyList = createKeyList(strNumList, (byte) SelfDefineSortableKey.TypeFlag.DOUBLE_FAMILY_TYPE.ordinal());
            Collections.sort(keyList);
            fail("Need catch exception");
        }catch(Exception e){
            //correct
        }

    }

    @Test
    public void testEnum() {
        SelfDefineSortableKey.TypeFlag flag = SelfDefineSortableKey.TypeFlag.DOUBLE_FAMILY_TYPE;
        System.out.println((byte) flag.ordinal());
        int t = (byte) flag.ordinal();
        System.out.println(t);
    }

    private <T> ArrayList<String> listToStringList(ArrayList<T> list) {
        ArrayList<String> strList = new ArrayList<>();
        for (T t : list) {
            strList.add(t.toString());
        }
        return strList;
    }

    private ArrayList<SelfDefineSortableKey> createKeyList(List<String> strNumList, byte typeFlag) {
        int partationId = 0;
        ArrayList<SelfDefineSortableKey> keyList = new ArrayList<>();
        for (String str : strNumList) {
            ByteBuffer keyBuffer = ByteBuffer.allocate(4096);
            int offset = keyBuffer.position();
            keyBuffer.put(Bytes.toBytes(partationId)[3]);
            keyBuffer.put(Bytes.toBytes(str));
            Bytes.copy(keyBuffer.array(), 1, keyBuffer.position() - offset - 1);
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

    private <T> boolean isIncreasedOrder(List<T> list, Comparator<T> comp) {
        int flag;
        T previous = null;
        for (T t : list) {
            if (previous == null)
                previous = t;
            else {
                flag = comp.compare(previous, t);
                if (flag > 0)
                    return false;
                previous = t;
            }
        }
        return true;
    }

}

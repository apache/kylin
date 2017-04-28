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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.util.Dictionary;
import org.junit.Test;

/**
 * Created by sunyerui on 16/8/2.
 */
public class MultipleDictionaryValueEnumeratorTest {

    private static DictionaryInfo createDictInfo(int[] values) {
        MockDictionary mockDict = new MockDictionary();
        mockDict.values = values;
        DictionaryInfo info = new DictionaryInfo();
        info.setDictionaryObject(mockDict);
        return info;
    }

    private static Integer[] enumerateDictInfoList(List<DictionaryInfo> dictionaryInfoList) throws IOException {
        MultipleDictionaryValueEnumerator enumerator = new MultipleDictionaryValueEnumerator(dictionaryInfoList);
        List<Integer> values = new ArrayList<>();
        while (enumerator.moveNext()) {
            values.add(Integer.parseInt(enumerator.current()));
        }
        return values.toArray(new Integer[0]);
    }

    @Test
    public void testNormalDicts() throws IOException {
        List<DictionaryInfo> dictionaryInfoList = new ArrayList<>(2);
        dictionaryInfoList.add(createDictInfo(new int[]{0, 1, 2}));
        dictionaryInfoList.add(createDictInfo(new int[]{4, 5, 6}));

        Integer[] values = enumerateDictInfoList(dictionaryInfoList);
        assertEquals(6, values.length);
        assertArrayEquals(new Integer[]{0, 1, 2, 4, 5, 6}, values);
    }

    @Test
    public void testFirstEmptyDicts() throws IOException {
        List<DictionaryInfo> dictionaryInfoList = new ArrayList<>(2);
        dictionaryInfoList.add(createDictInfo(new int[]{}));
        dictionaryInfoList.add(createDictInfo(new int[]{4, 5, 6}));

        Integer[] values = enumerateDictInfoList(dictionaryInfoList);
        assertEquals(3, values.length);
        assertArrayEquals(new Integer[]{4, 5, 6}, values);
    }

    @Test
    public void testMiddleEmptyDicts() throws IOException {
        List<DictionaryInfo> dictionaryInfoList = new ArrayList<>(3);
        dictionaryInfoList.add(createDictInfo(new int[]{0, 1, 2}));
        dictionaryInfoList.add(createDictInfo(new int[]{}));
        dictionaryInfoList.add(createDictInfo(new int[]{7, 8, 9}));

        Integer[] values = enumerateDictInfoList(dictionaryInfoList);
        assertEquals(6, values.length);
        assertArrayEquals(new Integer[]{0, 1, 2, 7, 8, 9}, values);
    }

    @Test
    public void testLastEmptyDicts() throws IOException {
        List<DictionaryInfo> dictionaryInfoList = new ArrayList<>(3);
        dictionaryInfoList.add(createDictInfo(new int[]{0, 1, 2}));
        dictionaryInfoList.add(createDictInfo(new int[]{6, 7, 8}));
        dictionaryInfoList.add(createDictInfo(new int[]{}));

        Integer[] values = enumerateDictInfoList(dictionaryInfoList);
        assertEquals(6, values.length);
        assertArrayEquals(new Integer[]{0, 1, 2, 6, 7, 8}, values);
    }

    public static class MockDictionary extends Dictionary<String> {
        private static final long serialVersionUID = 1L;
        
        public int[] values;

        @Override
        public int getMinId() {
            return 0;
        }

        @Override
        public int getMaxId() {
            return values.length-1;
        }

        @Override
        public int getSizeOfId() {
            return 4;
        }

        @Override
        public int getSizeOfValue() {
            return 4;
        }

        @Override
        protected int getIdFromValueImpl(String value, int roundingFlag) {
            return 0;
        }

        @Override
        protected String getValueFromIdImpl(int id) {
            return "" + values[id];
        }


        @Override
        public void dump(PrintStream out) {}

        @Override
        public void write(DataOutput out) throws IOException {}

        @Override
        public void readFields(DataInput in) throws IOException {}


        @Override
        public boolean contains(Dictionary another) {
            return false;
        }
    }

}
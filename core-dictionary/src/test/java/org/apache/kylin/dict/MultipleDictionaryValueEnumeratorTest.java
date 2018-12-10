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
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.apache.kylin.metadata.datatype.DataType;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.Nullable;

/**
 * Created by sunyerui on 16/8/2.
 */
public class MultipleDictionaryValueEnumeratorTest {
    private MultipleDictionaryValueEnumerator enumerator;

    @BeforeClass
    public static void beforeClass() throws Exception {
        ClassUtil.addClasspath(new File(HBaseMetadataTestCase.SANDBOX_TEST_DATA).getAbsolutePath());
        System.setProperty(KylinConfig.KYLIN_CONF, HBaseMetadataTestCase.SANDBOX_TEST_DATA);
    }

    private static DictionaryInfo createDictInfo(String[] values) {
        MockDictionary mockDict = new MockDictionary();
        mockDict.values = values;
        DictionaryInfo info = new DictionaryInfo();
        info.setDictionaryObject(mockDict);
        return info;
    }

    private String[] enumerateDictInfoList(List<DictionaryInfo> dictionaryInfoList, String dataType) throws IOException {
        List<Dictionary<String>> dictList = Lists.transform(dictionaryInfoList, new Function<DictionaryInfo, Dictionary<String>>() {
            @Nullable
            @Override
            public Dictionary<String> apply(@Nullable DictionaryInfo input) {
                return input.dictionaryObject;
            }
        });
        enumerator = new MultipleDictionaryValueEnumerator(DataType.getType(dataType), dictList);
        List<String> values = new ArrayList<>();
        while (enumerator.moveNext()) {
            values.add(enumerator.current());
        }
        return values.toArray(new String[0]);
    }

    @Test
    public void testNormalDicts() throws IOException {
        List<DictionaryInfo> dictionaryInfoList = new ArrayList<>(2);
        dictionaryInfoList.add(createDictInfo(new String[] { "0", "11", "21" }));
        dictionaryInfoList.add(createDictInfo(new String[] { "4", "5", "6" }));

        String[] values = enumerateDictInfoList(dictionaryInfoList, "string");
        assertEquals(6, values.length);
        assertArrayEquals(new String[] { "0", "11", "21", "4", "5", "6" }, values);

        String[] values2 = enumerateDictInfoList(dictionaryInfoList, "integer");
        assertEquals(6, values2.length);
        assertArrayEquals(new String[] { "0", "4", "5", "6", "11", "21" }, values2);
    }

    @Test
    public void testNormalDictsWithDate() throws IOException {
        List<DictionaryInfo> dictionaryInfoList = new ArrayList<>(2);
        dictionaryInfoList.add(createDictInfo(new String[] { "2017-01-02", "2017-01-11", "2017-05-10" }));
        dictionaryInfoList.add(createDictInfo(new String[] { "2017-01-21", "2017-03-01", "2017-04-12" }));

        String[] values = enumerateDictInfoList(dictionaryInfoList, "date");
        assertEquals(6, values.length);
        assertArrayEquals(new String[] { "2017-01-02", "2017-01-11", "2017-01-21", "2017-03-01", "2017-04-12",
                "2017-05-10" }, values);
    }

    @Test
    public void testNormalDictsWithNumbers() throws IOException {
        List<DictionaryInfo> dictionaryInfoList = new ArrayList<>(2);
        dictionaryInfoList.add(createDictInfo(new String[] { "6.25", "11.25", "1000.25779" }));
        dictionaryInfoList.add(createDictInfo(new String[] { "9.88", "1000.25778", "8765.456" }));

        String[] values = enumerateDictInfoList(dictionaryInfoList, "float");
        assertEquals(6, values.length);
        assertArrayEquals(new String[] { "6.25", "9.88", "11.25", "1000.25778", "1000.25779", "8765.456" }, values);
    }

    @Test
    public void testFirstEmptyDicts() throws IOException {
        List<DictionaryInfo> dictionaryInfoList = new ArrayList<>(2);
        dictionaryInfoList.add(createDictInfo(new String[] {}));
        dictionaryInfoList.add(createDictInfo(new String[] { "4", "5", "6" }));

        String[] values = enumerateDictInfoList(dictionaryInfoList, "integer");
        assertEquals(3, values.length);
        assertArrayEquals(new String[] { "4", "5", "6" }, values);
    }

    @Test
    public void testMiddleEmptyDicts() throws IOException {
        List<DictionaryInfo> dictionaryInfoList = new ArrayList<>(3);
        dictionaryInfoList.add(createDictInfo(new String[] { "0", "1", "2" }));
        dictionaryInfoList.add(createDictInfo(new String[] {}));
        dictionaryInfoList.add(createDictInfo(new String[] { "7", "8", "9" }));

        String[] values = enumerateDictInfoList(dictionaryInfoList, "integer");
        assertEquals(6, values.length);
        assertArrayEquals(new String[] { "0", "1", "2", "7", "8", "9" }, values);
    }

    @Test
    public void testLastEmptyDicts() throws IOException {
        List<DictionaryInfo> dictionaryInfoList = new ArrayList<>(3);
        dictionaryInfoList.add(createDictInfo(new String[] { "0", "1", "2" }));
        dictionaryInfoList.add(createDictInfo(new String[] { "6", "7", "8" }));
        dictionaryInfoList.add(createDictInfo(new String[] {}));

        String[] values = enumerateDictInfoList(dictionaryInfoList, "integer");
        assertEquals(6, values.length);
        assertArrayEquals(new String[] { "0", "1", "2", "6", "7", "8" }, values);
    }

    @Test
    public void testUnorderedDicts() throws IOException {
        List<DictionaryInfo> dictionaryInfoList = new ArrayList<>(3);
        dictionaryInfoList.add(createDictInfo(new String[] { "0", "1", "6" }));
        dictionaryInfoList.add(createDictInfo(new String[] { "3", "7", "8" }));
        dictionaryInfoList.add(createDictInfo(new String[] { "2", "7", "9" }));
        String[] values = enumerateDictInfoList(dictionaryInfoList, "integer");
        assertEquals(9, values.length);
        assertArrayEquals(new String[] { "0", "1", "2", "3", "6", "7", "7", "8", "9" }, values);
    }

    public static class MockDictionary extends Dictionary<String> {
        private static final long serialVersionUID = 1L;

        public String[] values;

        @Override
        public int getMinId() {
            return 0;
        }

        @Override
        public int getMaxId() {
            return values.length - 1;
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
        public void dump(PrintStream out) {
        }

        @Override
        public void write(DataOutput out) throws IOException {
        }

        @Override
        public void readFields(DataInput in) throws IOException {
        }

        @Override
        public boolean contains(Dictionary another) {
            return false;
        }
    }

}
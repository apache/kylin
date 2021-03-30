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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ShrunkenDictionaryTest {
    private static List<String> testData;
    private static Dictionary originDict;
    private static Dictionary shrunkenDict;

    @BeforeClass
    public static void setUp() {
        LocalFileMetadataTestCase.staticCreateTestMetadata();
        prepareTestData();
    }

    @AfterClass
    public static void after() {
        LocalFileMetadataTestCase.staticCleanupTestMetadata();
    }

    @Test
    public void testStringDictionary() {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(bos);

            shrunkenDict.write(dos);

            ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
            DataInputStream dis = new DataInputStream(bis);

            ShrunkenDictionary.StringValueSerializer valueSerializer = new ShrunkenDictionary.StringValueSerializer();
            Dictionary<String> dShrunkenDict = new ShrunkenDictionary<>(valueSerializer);
            dShrunkenDict.readFields(dis);

            for (int i = 0; i < testData.size(); i += 2) {
                String value = testData.get(i);
                Assert.assertEquals(originDict.getIdFromValue(value), dShrunkenDict.getIdFromValue(value));
            }
        } catch (IOException e) {
        }
    }

    @Test
    public void testGetMinId() {
        assertEquals(0, shrunkenDict.getMinId());
    }

    @Test
    public void testGetMaxId() {
        assertEquals(6, shrunkenDict.getMaxId());
    }

    @Test
    public void testGetSizeOfId() {
        assertEquals(1, shrunkenDict.getSizeOfId());
    }

    @Test
    public void testGetSizeOfValue() {
        assertEquals(9, shrunkenDict.getSizeOfValue());
    }

    @Test
    public void testContains() {
        assertFalse(shrunkenDict.contains(originDict));
    }

    @Test
    public void testGetValueFromIdImpl() {
        for (int i = 0; i < testData.size(); i += 2) {
            assertEquals(testData.get(i), shrunkenDict.getValueFromId(originDict.getIdFromValue(testData.get(i))));
        }
    }

    private static void prepareTestData() {
        testData = new ArrayList<>();
        testData.add("");
        testData.add("part");
        testData.add("par");
        testData.add("partition");
        testData.add("party");
        testData.add("parties");
        testData.add("paint");

        originDict = constructOriginDict();
        ShrunkenDictionary.StringValueSerializer valueSerializer = new ShrunkenDictionary.StringValueSerializer();
        shrunkenDict = constructShrunkenDict(originDict, valueSerializer);
    }

    private static Dictionary constructOriginDict() {
        TrieDictionaryBuilder<String> dictBuilder = new TrieDictionaryBuilder<>(new StringBytesConverter());
        for (String str : testData) {
            dictBuilder.addValue(str);
        }
        Dictionary<String> dict = dictBuilder.build(0);
        return dict;
    }

    private static Dictionary constructShrunkenDict(Dictionary dictionary,
            ShrunkenDictionary.ValueSerializer valueSerializer) {
        ShrunkenDictionaryBuilder<String> shrunkenDictBuilder = new ShrunkenDictionaryBuilder<>(dictionary);
        for (int i = 0; i < testData.size(); i += 2) {
            System.out.println(testData.get(i));
            shrunkenDictBuilder.addValue(testData.get(i));
        }
        Dictionary<String> shrunkenDict = shrunkenDictBuilder.build(valueSerializer);
        return shrunkenDict;
    }
}

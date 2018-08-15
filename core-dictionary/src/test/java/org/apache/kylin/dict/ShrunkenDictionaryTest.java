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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.kylin.common.util.Dictionary;
import org.junit.Assert;
import org.junit.Test;

public class ShrunkenDictionaryTest {

    @Test
    public void testStringDictionary() {
        ArrayList<String> strList = new ArrayList<String>();
        strList.add("");
        strList.add("part");
        strList.add("par");
        strList.add("partition");
        strList.add("party");
        strList.add("parties");
        strList.add("paint");

        TrieDictionaryBuilder<String> dictBuilder = new TrieDictionaryBuilder<>(new StringBytesConverter());
        for (String str : strList) {
            dictBuilder.addValue(str);
        }
        Dictionary<String> dict = dictBuilder.build(0);

        ShrunkenDictionary.StringValueSerializer valueSerializer = new ShrunkenDictionary.StringValueSerializer();
        ShrunkenDictionaryBuilder<String> shrunkenDictBuilder = new ShrunkenDictionaryBuilder<>(dict);
        for (int i = 0; i < strList.size(); i += 2) {
            shrunkenDictBuilder.addValue(strList.get(i));
        }
        Dictionary<String> shrunkenDict = shrunkenDictBuilder.build(valueSerializer);

        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(bos);

            shrunkenDict.write(dos);

            ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
            DataInputStream dis = new DataInputStream(bis);

            Dictionary<String> dShrunkenDict = new ShrunkenDictionary<>(valueSerializer);
            dShrunkenDict.readFields(dis);

            for (int i = 0; i < strList.size(); i += 2) {
                String value = strList.get(i);
                Assert.assertEquals(dict.getIdFromValue(value), dShrunkenDict.getIdFromValue(value));
            }
        } catch (IOException e) {
        }
    }
}

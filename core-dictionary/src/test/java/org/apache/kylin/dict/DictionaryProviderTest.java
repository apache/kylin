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

import static org.junit.Assert.assertTrue;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.metadata.datatype.DataType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DictionaryProviderTest extends LocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testReadWrite() throws Exception {
        //string dict
        Dictionary<String> dict = getDict(DataType.getType("string"),
                Arrays.asList(new String[] { "a", "b" }).iterator());
        readWriteTest(dict);
        //number dict
        Dictionary<String> dict2 = getDict(DataType.getType("long"),
                Arrays.asList(new String[] { "1", "2" }).iterator());
        readWriteTest(dict2);
    }

    @Test
    public void testReadWriteTime() {
        System.out.println(Long.MAX_VALUE);
        System.out.println(Long.MIN_VALUE);
    }

    private Dictionary<String> getDict(DataType type, Iterator<String> values) throws Exception {
        IDictionaryBuilder builder = DictionaryGenerator.newDictionaryBuilder(type);
        builder.init(null, 0, null);
        while (values.hasNext()) {
            builder.addValue(values.next());
        }
        return builder.build();
    }

    private void readWriteTest(Dictionary<String> dict) throws Exception {
        final String path = "src/test/resources/dict/tmp_dict";
        File f = new File(path);
        f.deleteOnExit();
        f.createNewFile();
        String dictClassName = dict.getClass().getName();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(f));
        out.writeUTF(dictClassName);
        dict.write(out);
        out.close();
        //read dict
        DataInputStream in = null;
        Dictionary<String> dict2 = null;
        try {
            File f2 = new File(path);
            in = new DataInputStream(new FileInputStream(f2));
            String dictClassName2 = in.readUTF();
            dict2 = (Dictionary<String>) ClassUtil.newInstance(dictClassName2);
            dict2.readFields(in);
        } finally {
            if (in != null) {
                in.close();
            }
        }
        assertTrue(dict.equals(dict2));
    }
}

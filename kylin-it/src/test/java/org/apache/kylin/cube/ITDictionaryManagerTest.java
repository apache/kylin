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

package org.apache.kylin.cube;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Set;

import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.dict.DictionaryInfo;
import org.apache.kylin.dict.DictionaryManager;
import org.apache.kylin.dict.DistinctColumnValuesProvider;
import org.apache.kylin.engine.mr.DFSFileTable;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.source.ReadableTable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;

public class ITDictionaryManagerTest extends LocalFileMetadataTestCase {

    DictionaryManager dictMgr;

    @Before
    public void setup() throws Exception {
        createTestMetadata();
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void basic() throws Exception {
        dictMgr = DictionaryManager.getInstance(getTestConfig());
        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc("test_kylin_cube_without_slr_desc");
        TblColRef col = cubeDesc.findColumnRef("DEFAULT.TEST_KYLIN_FACT", "LSTG_FORMAT_NAME");

        MockDistinctColumnValuesProvider mockupData = new MockDistinctColumnValuesProvider("A", "B", "C");

        DictionaryInfo info1 = dictMgr.buildDictionary(cubeDesc.getModel(), col, mockupData.getDistinctValuesFor(col));
        System.out.println(JsonUtil.writeValueAsIndentString(info1));

        DictionaryInfo info2 = dictMgr.buildDictionary(cubeDesc.getModel(), col, mockupData.getDistinctValuesFor(col));
        System.out.println(JsonUtil.writeValueAsIndentString(info2));

        // test check duplicate
        assertTrue(info1.getUuid() == info2.getUuid());
        assertTrue(info1 == dictMgr.getDictionaryInfo(info1.getResourcePath()));
        assertTrue(info2 == dictMgr.getDictionaryInfo(info2.getResourcePath()));
        assertTrue(info1.getDictionaryObject() == info2.getDictionaryObject());

        // verify dictionary entries
        @SuppressWarnings("unchecked")
        Dictionary<String> dict = (Dictionary<String>) info1.getDictionaryObject();
        int id = 0;
        for (String v : mockupData.set) {
            assertEquals(id, dict.getIdFromValue(v, 0));
            assertEquals(v, dict.getValueFromId(id));
            id++;
        }

        // test empty dictionary
        MockDistinctColumnValuesProvider mockupEmpty = new MockDistinctColumnValuesProvider();
        DictionaryInfo info3 = dictMgr.buildDictionary(cubeDesc.getModel(), col, mockupEmpty.getDistinctValuesFor(col));
        System.out.println(JsonUtil.writeValueAsIndentString(info3));
        assertEquals(0, info3.getCardinality());
        assertEquals(0, info3.getDictionaryObject().getSize());
        System.out.println(info3.getDictionaryObject().getMaxId());
        System.out.println(info3.getDictionaryObject().getMinId());
        System.out.println(info3.getDictionaryObject().getSizeOfId());
    }

    private static class MockDistinctColumnValuesProvider implements DistinctColumnValuesProvider {

        String tmpFilePath;
        Set<String> set;

        public MockDistinctColumnValuesProvider(String... values) throws IOException {
            File tmpFile = File.createTempFile("MockDistinctColumnValuesProvider", ".txt");
            PrintWriter out = new PrintWriter(tmpFile);

            set = Sets.newTreeSet();
            for (String value : values) {
                out.println(value);
                set.add(value);
            }
            out.close();

            tmpFilePath = HadoopUtil.fixWindowsPath("file://" + tmpFile.getAbsolutePath());
            tmpFile.deleteOnExit();
        }

        @Override
        public ReadableTable getDistinctValuesFor(TblColRef col) {
            return new DFSFileTable(tmpFilePath, -1);
        }

    }
}

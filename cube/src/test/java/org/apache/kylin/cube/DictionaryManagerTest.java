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

import java.util.HashSet;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.dict.DictionaryInfo;
import org.apache.kylin.dict.DictionaryManager;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class DictionaryManagerTest extends LocalFileMetadataTestCase {

    DictionaryManager dictMgr;

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        dictMgr = DictionaryManager.getInstance(getTestConfig());
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    @Ignore("hive not ready")
    public void basic() throws Exception {
        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc("test_kylin_cube_without_slr_desc");
        TblColRef col = cubeDesc.findColumnRef("DEFAULT.TEST_CATEGORY_GROUPINGS", "META_CATEG_NAME");

        DictionaryInfo info1 = dictMgr.buildDictionary(cubeDesc.getModel(), cubeDesc.getRowkey().getDictionary(col), col, null);
        System.out.println(JsonUtil.writeValueAsIndentString(info1));

        DictionaryInfo info2 = dictMgr.buildDictionary(cubeDesc.getModel(), cubeDesc.getRowkey().getDictionary(col), col, null);
        System.out.println(JsonUtil.writeValueAsIndentString(info2));

        assertTrue(info1.getUuid() == info2.getUuid());

        assertTrue(info1 == dictMgr.getDictionaryInfo(info1.getResourcePath()));
        assertTrue(info2 == dictMgr.getDictionaryInfo(info2.getResourcePath()));

        assertTrue(info1.getDictionaryObject() == info2.getDictionaryObject());

        touchDictValues(info1);
    }

    @SuppressWarnings("unchecked")
    private void touchDictValues(DictionaryInfo info1) {
        Dictionary<String> dict = (Dictionary<String>) info1.getDictionaryObject();

        HashSet<String> set = new HashSet<String>();
        for (int i = 0, n = info1.getCardinality(); i < n; i++) {
            set.add(dict.getValueFromId(i));
        }
        assertEquals(info1.getCardinality(), set.size());
    }
}

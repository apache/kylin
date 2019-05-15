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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DataModelManager;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DictionaryManagerTest extends LocalFileMetadataTestCase {

    @Before
    public void setup() throws Exception {
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testBuildSaveDictionary() throws IOException, InterruptedException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        DictionaryManager dictMgr = DictionaryManager.getInstance(config);
        DataModelManager metaMgr = DataModelManager.getInstance(config);
        DataModelDesc model = metaMgr.getDataModelDesc("test_kylin_inner_join_model_desc");
        TblColRef col = model.findColumn("lstg_format_name");

        // non-exist input returns null;
        DictionaryInfo nullInfo = dictMgr.buildDictionary(col, MockupReadableTable.newNonExistTable("/a/path"));
        assertEquals(null, nullInfo);

        DictionaryInfo info1 = dictMgr.buildDictionary(col,
                MockupReadableTable.newSingleColumnTable("/a/path", "1", "2", "3"));
        assertEquals(3, info1.getDictionaryObject().getSize());

        long info1LastModified = info1.getLastModified();

        // same input returns same dict
        // sleep 1 second to avoid file resource store timestamp precision lost when update
        Thread.sleep(1000);
        DictionaryInfo info2 = dictMgr.buildDictionary(col,
                MockupReadableTable.newSingleColumnTable("/a/path", "1", "2", "3"));
        assertTrue(info1 != info2);
        assertEquals(info1.getResourcePath(), info2.getResourcePath());

        // update last modified when reused dict
        long info2LastModified = info2.getLastModified();
        assertTrue(info2LastModified > info1LastModified);

        // same input values (different path) returns same dict
        // sleep 1 second to avoid file resource store timestamp precision lost when update
        Thread.sleep(1000);
        DictionaryInfo info3 = dictMgr.buildDictionary(col,
                MockupReadableTable.newSingleColumnTable("/a/different/path", "1", "2", "3"));
        assertTrue(info1 != info3);
        assertTrue(info2 != info3);
        assertEquals(info1.getResourcePath(), info3.getResourcePath());
        assertEquals(info2.getResourcePath(), info3.getResourcePath());

        // update last modified when reused dict
        long info3LastModified = info3.getLastModified();
        assertTrue(info3LastModified > info2LastModified);

        // save dictionary works in spite of non-exist table
        Dictionary<String> dict = DictionaryGenerator.buildDictionary(col.getType(),
                new IterableDictionaryValueEnumerator("1", "2", "3"));
        DictionaryInfo info4 = dictMgr.saveDictionary(col, MockupReadableTable.newNonExistTable("/a/path"), dict);
        assertEquals(info1.getResourcePath(), info4.getResourcePath());

        Dictionary<String> dict2 = DictionaryGenerator.buildDictionary(col.getType(),
                new IterableDictionaryValueEnumerator("1", "2", "3", "4"));
        DictionaryInfo info5 = dictMgr.saveDictionary(col, MockupReadableTable.newNonExistTable("/a/path"), dict2);
        assertNotEquals(info1.getResourcePath(), info5.getResourcePath());
    }
}

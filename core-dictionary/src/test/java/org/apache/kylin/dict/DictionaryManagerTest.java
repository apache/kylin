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
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
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
    public void testDecideSourceData() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();

        DictionaryManager dictMgr = DictionaryManager.getInstance(config);
        MetadataManager metaMgr = MetadataManager.getInstance(config);

        {
            DataModelDesc innerModel = metaMgr.getDataModelDesc("test_kylin_inner_join_model_desc");
            TblColRef factDate = innerModel.findColumn("TEST_KYLIN_FACT.CAL_DT");
            TblColRef lookupDate = innerModel.findColumn("TEST_CAL_DT.CAL_DT");
            TblColRef formatName = innerModel.findColumn("lstg_format_name");
            assertEquals(lookupDate, dictMgr.decideSourceData(innerModel, factDate));
            assertEquals(lookupDate, dictMgr.decideSourceData(innerModel, lookupDate));
            assertEquals(formatName, dictMgr.decideSourceData(innerModel, formatName));
        }
        
        {
            DataModelDesc outerModel = metaMgr.getDataModelDesc("test_kylin_left_join_model_desc");
            TblColRef factDate = outerModel.findColumn("TEST_KYLIN_FACT.CAL_DT");
            assertEquals(factDate, dictMgr.decideSourceData(outerModel, factDate));
        }
    }
    
    @Test
    public void testBuildSaveDictionary() throws IOException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        DictionaryManager dictMgr = DictionaryManager.getInstance(config);
        MetadataManager metaMgr = MetadataManager.getInstance(config);
        DataModelDesc model = metaMgr.getDataModelDesc("test_kylin_inner_join_model_desc");
        TblColRef col = model.findColumn("lstg_format_name");

        // non-exist input returns null;
        DictionaryInfo nullInfo = dictMgr.buildDictionary(model, col, MockupReadableTable.newNonExistTable("/a/path"));
        assertEquals(null, nullInfo);
        
        DictionaryInfo info1 = dictMgr.buildDictionary(model, col, MockupReadableTable.newSingleColumnTable("/a/path", "1", "2", "3"));
        assertEquals(3, info1.getDictionaryObject().getSize());

        // same input returns same dict
        DictionaryInfo info2 = dictMgr.buildDictionary(model, col, MockupReadableTable.newSingleColumnTable("/a/path", "1", "2", "3"));
        assertTrue(info1 == info2);
        
        // same input values (different path) returns same dict
        DictionaryInfo info3 = dictMgr.buildDictionary(model, col, MockupReadableTable.newSingleColumnTable("/a/different/path", "1", "2", "3"));
        assertTrue(info1 == info3);
        
        // save dictionary works in spite of non-exist table
        Dictionary<String> dict = DictionaryGenerator.buildDictionary(col.getType(), new IterableDictionaryValueEnumerator("1", "2", "3"));
        DictionaryInfo info4 = dictMgr.saveDictionary(model, col, MockupReadableTable.newNonExistTable("/a/path"), dict);
        assertTrue(info1 == info4);
        
        Dictionary<String> dict2 = DictionaryGenerator.buildDictionary(col.getType(), new IterableDictionaryValueEnumerator("1", "2", "3", "4"));
        DictionaryInfo info5 = dictMgr.saveDictionary(model, col, MockupReadableTable.newNonExistTable("/a/path"), dict2);
        assertTrue(info1 != info5);
    }
}

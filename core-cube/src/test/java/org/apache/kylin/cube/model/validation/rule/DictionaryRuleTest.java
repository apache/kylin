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

package org.apache.kylin.cube.model.validation.rule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.DictionaryDesc;
import org.apache.kylin.cube.model.validation.ValidateContext;
import org.apache.kylin.dict.GlobalDictionaryBuilder;
import org.apache.kylin.metadata.MetadataManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by sunyerui on 16/6/1.
 */
public class DictionaryRuleTest extends LocalFileMetadataTestCase {
    private static KylinConfig config;
    private static MetadataManager metadataManager;

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        config = KylinConfig.getInstanceFromEnv();
        metadataManager = MetadataManager.getInstance(config);
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGoodDesc() throws IOException {
        DictionaryRule rule = new DictionaryRule();

        for (File f : new File(LocalFileMetadataTestCase.LOCALMETA_TEST_DATA + "/cube_desc/").listFiles()) {
            CubeDesc desc = JsonUtil.readValue(new FileInputStream(f), CubeDesc.class);
            desc.init(config, metadataManager.getAllTablesMap());
            ValidateContext vContext = new ValidateContext();
            rule.validate(desc, vContext);
            vContext.print(System.out);
            assertTrue(vContext.getResults().length == 0);
        }
    }

    @Test
    public void testBadDesc() throws IOException {
        testDictionaryDesc("Column EDW.TEST_SITES.SITE_NAME has inconsistent builders " + "FakeBuilderClass and org.apache.kylin.dict.GlobalDictionaryBuilder", DictionaryDesc.create("SITE_NAME", null, "FakeBuilderClass"));
    }

    @Test
    public void testBadDesc2() throws IOException {
        testDictionaryDesc("Column DEFAULT.TEST_KYLIN_FACT.LSTG_SITE_ID cannot have builder and reuse column both", DictionaryDesc.create("lstg_site_id", "SITE_NAME", "FakeBuilderClass"));
    }

    @Test
    public void testBadDesc3() throws IOException {
        testDictionaryDesc("Column DEFAULT.TEST_KYLIN_FACT.LSTG_SITE_ID cannot have builder and reuse column both empty", DictionaryDesc.create("lstg_site_id", null, null));
    }
    
    @Test
    public void testGoodDesc2() throws IOException {
        testDictionaryDesc(null, DictionaryDesc.create("lstg_site_id", null, GlobalDictionaryBuilder.class.getName()));
    }

    private void testDictionaryDesc(String expectMessage, DictionaryDesc... descs) throws IOException {
        DictionaryRule rule = new DictionaryRule();
        File f = new File(LocalFileMetadataTestCase.LOCALMETA_TEST_DATA + "/cube_desc/test_kylin_cube_without_slr_left_join_desc.json");
        CubeDesc desc = JsonUtil.readValue(new FileInputStream(f), CubeDesc.class);

        for (DictionaryDesc dictDesc : descs) {
            desc.getDictionaries().add(dictDesc);
        }

        desc.init(config, metadataManager.getAllTablesMap());
        ValidateContext vContext = new ValidateContext();
        rule.validate(desc, vContext);
        vContext.print(System.out);

        if (expectMessage == null) {
            assertTrue(vContext.getResults().length == 0);
        } else {
            assertTrue(vContext.getResults().length >= 1);
            assertEquals(expectMessage, vContext.getResults()[0].getMessage());
        }
    }
}
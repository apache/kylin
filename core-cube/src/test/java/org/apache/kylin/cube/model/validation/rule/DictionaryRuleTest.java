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

import static org.apache.kylin.cube.model.validation.rule.DictionaryRule.ERROR_DUPLICATE_DICTIONARY_COLUMN;
import static org.apache.kylin.cube.model.validation.rule.DictionaryRule.ERROR_GLOBAL_DICTIONNARY_ONLY_MEASURE;
import static org.apache.kylin.cube.model.validation.rule.DictionaryRule.ERROR_REUSE_BUILDER_BOTH_EMPTY;
import static org.apache.kylin.cube.model.validation.rule.DictionaryRule.ERROR_REUSE_BUILDER_BOTH_SET;
import static org.apache.kylin.cube.model.validation.rule.DictionaryRule.ERROR_TRANSITIVE_REUSE;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DictionaryRuleTest extends LocalFileMetadataTestCase {
    private static KylinConfig config;

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        config = KylinConfig.getInstanceFromEnv();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGoodDesc() throws IOException {
        DictionaryRule rule = new DictionaryRule();

        for (File f : new File(LocalFileMetadataTestCase.LOCALMETA_TEST_DATA + "/cube_desc/").listFiles()) {
            if (!f.getName().endsWith("json")) {
                continue;
            }
            CubeDesc desc = JsonUtil.readValue(new FileInputStream(f), CubeDesc.class);
            desc.init(config);
            ValidateContext vContext = new ValidateContext();
            rule.validate(desc, vContext);
            assertTrue(vContext.getResults().length == 0);
        }
    }

    @Test
    public void testBadDesc() throws IOException {
        testDictionaryDesc(ERROR_DUPLICATE_DICTIONARY_COLUMN, DictionaryDesc.create("ORDER_ID", null, "FakeBuilderClass"));
        testDictionaryDesc(ERROR_DUPLICATE_DICTIONARY_COLUMN, DictionaryDesc.create("ORDER_ID", null, GlobalDictionaryBuilder.class.getName()));
    }

    @Test
    public void testBadDesc2() throws IOException {
        testDictionaryDesc(ERROR_REUSE_BUILDER_BOTH_SET, DictionaryDesc.create("lstg_site_id", "SITE_NAME", "FakeBuilderClass"));
    }

    @Test
    public void testBadDesc3() throws IOException {
        testDictionaryDesc(ERROR_REUSE_BUILDER_BOTH_EMPTY, DictionaryDesc.create("lstg_site_id", null, null));
    }

    @Test
    public void testBadDesc4() throws IOException {
        testDictionaryDesc(ERROR_TRANSITIVE_REUSE,
                DictionaryDesc.create("lstg_site_id", "SELLER_ID", null),
                DictionaryDesc.create("price", "lstg_site_id", null));
    }

    @Test
    public void testBadDesc5() throws IOException {
        testDictionaryDesc(ERROR_GLOBAL_DICTIONNARY_ONLY_MEASURE,
                DictionaryDesc.create("CATEG_LVL2_NAME", null, GlobalDictionaryBuilder.class.getName()));
    }

    @Test
    public void testGoodDesc2() throws IOException {
        testDictionaryDesc(null, DictionaryDesc.create("SELLER_ID", null, GlobalDictionaryBuilder.class.getName()));
    }

    private void testDictionaryDesc(String expectMessage, DictionaryDesc... descs) throws IOException {
        DictionaryRule rule = new DictionaryRule();
        File f = new File(LocalFileMetadataTestCase.LOCALMETA_TEST_DATA + "/cube_desc/test_kylin_cube_without_slr_left_join_desc.json");
        CubeDesc desc = JsonUtil.readValue(new FileInputStream(f), CubeDesc.class);

        for (DictionaryDesc dictDesc : descs) {
            desc.getDictionaries().add(dictDesc);
        }

        desc.init(config);
        ValidateContext vContext = new ValidateContext();
        rule.validate(desc, vContext);

        if (expectMessage == null) {
            assertTrue(vContext.getResults().length == 0);
        } else {
            assertTrue(vContext.getResults().length == 1);
            String actualMessage = vContext.getResults()[0].getMessage();
            assertTrue(actualMessage.startsWith(expectMessage));
        }
    }
}
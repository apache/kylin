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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.validation.ValidateContext;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FunctionRuleTest extends LocalFileMetadataTestCase {
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
        FunctionRule rule = new FunctionRule();

        File f = new File(LocalFileMetadataTestCase.LOCALMETA_TEST_DATA + "/cube_desc/ssb.json");
        CubeDesc desc = JsonUtil.readValue(new FileInputStream(f), CubeDesc.class);
        desc.init(config, metadataManager.getAllTablesMap());
        ValidateContext vContext = new ValidateContext();
        rule.validate(desc, vContext);
        vContext.print(System.out);
        assertTrue(vContext.getResults().length == 0);
    }

    @Test
    public void testValidateMeasureNamesDuplicated() throws IOException {
        FunctionRule rule = new FunctionRule();

        File f = new File(LocalFileMetadataTestCase.LOCALMETA_TEST_DATA + "/cube_desc/ssb.json");
        CubeDesc desc = JsonUtil.readValue(new FileInputStream(f), CubeDesc.class);

        MeasureDesc measureDescDuplicated = desc.getMeasures().get(1);
        desc.getMeasures().add(measureDescDuplicated);

        desc.init(config, metadataManager.getAllTablesMap());
        ValidateContext vContext = new ValidateContext();
        rule.validate(desc, vContext);

        vContext.print(System.out);
        assertTrue(vContext.getResults().length >= 1);
        assertEquals("There is duplicated measure's name: " + measureDescDuplicated.getName(), vContext.getResults()[0].getMessage());
    }
}

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

package org.apache.kylin.job;

import static org.junit.Assert.assertTrue;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.job.dataGen.FactTableGenerator;
import org.apache.kylin.metadata.MetadataManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class DataGenTest extends LocalFileMetadataTestCase {

    @Before
    public void before() throws Exception {
        this.createTestMetadata();
        MetadataManager.clearCache();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testBasics() throws Exception {
        String content = FactTableGenerator.generate("test_kylin_cube_with_slr_ready", "10000", "1", null);// default  settings
        //System.out.println(content);
        assertTrue(content.contains("FP-non GTC"));
        assertTrue(content.contains("ABIN"));

        //DeployUtil.overrideFactTableData(content, "default.test_kylin_fact");
    }

}

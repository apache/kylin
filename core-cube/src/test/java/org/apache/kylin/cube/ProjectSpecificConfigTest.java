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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.model.CubeDesc;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ProjectSpecificConfigTest extends LocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testProject1() {
        KylinConfig baseConfig = KylinConfig.getInstanceFromEnv();
        CubeDesc cubeDesc = CubeDescManager.getInstance(baseConfig).getCubeDesc("ssb");
        verifyProjectOverride(baseConfig, cubeDesc.getConfig());
    }

    @Test
    public void testProject2() {
        KylinConfig baseConfig = KylinConfig.getInstanceFromEnv();
        CubeInstance cube = CubeManager.getInstance(baseConfig).getCube("ssb");
        verifyProjectOverride(baseConfig, cube.getConfig());
    }

    private void verifyProjectOverride(KylinConfig base, KylinConfig override) {
        assertEquals("whoami@kylin.apache.org", base.getKylinOwner());
        assertEquals("kylin@kylin.apache.org", override.getKylinOwner());
    }
}

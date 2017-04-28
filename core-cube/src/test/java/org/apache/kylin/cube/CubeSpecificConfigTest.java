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
import org.apache.kylin.common.util.HotLoadKylinPropertiesTestCase;
import org.apache.kylin.cube.model.CubeDesc;
import org.junit.Test;

import java.io.IOException;

public class CubeSpecificConfigTest extends HotLoadKylinPropertiesTestCase {
    @Test
    public void test() {
        KylinConfig baseConfig = KylinConfig.getInstanceFromEnv();
        CubeDesc cubeDesc = CubeDescManager.getInstance(baseConfig).getCubeDesc("ssb");
        verifyOverride(baseConfig, cubeDesc.getConfig());
    }

    @Test
    public void test2() {
        KylinConfig baseConfig = KylinConfig.getInstanceFromEnv();
        CubeInstance cube = CubeManager.getInstance(baseConfig).getCube("ssb");
        verifyOverride(baseConfig, cube.getConfig());
    }

    private void verifyOverride(KylinConfig base, KylinConfig override) {
        assertEquals("snappy", base.getHbaseDefaultCompressionCodec());
        assertEquals("lz4", override.getHbaseDefaultCompressionCodec());
    }

    @Test
    public void testPropertiesHotLoad() throws IOException {
        KylinConfig baseConfig = KylinConfig.getInstanceFromEnv();
        KylinConfig oldCubeDescConfig = CubeDescManager.getInstance(baseConfig).getCubeDesc("ssb").getConfig();
        assertEquals(10, oldCubeDescConfig.getMaxConcurrentJobLimit());

        //hot load Properties
        updateProperty("kylin.job.max-concurrent-jobs", "20");
        KylinConfig.getInstanceFromEnv().hotLoadKylinProperties();
        CubeDescManager.getInstance(baseConfig).reloadCubeDescLocal("ssb");

        //test cubeDescConfig
        KylinConfig newCubeDescConfig = CubeDescManager.getInstance(baseConfig).getCubeDesc("ssb").getConfig();
        assertEquals(20, newCubeDescConfig.getMaxConcurrentJobLimit());

        //test cubeConfig
        KylinConfig newCubeConfig = CubeManager.getInstance(baseConfig).getCube("ssb").getConfig();
        assertEquals(20, newCubeConfig.getMaxConcurrentJobLimit());
    }
}

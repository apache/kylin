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

package org.apache.kylin.rest.signature;

import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.signature.RealizationSignature.CubeSignature;
import org.apache.kylin.rest.util.SQLResponseSignatureUtil;
import org.apache.kylin.storage.hybrid.HybridInstance;
import org.apache.kylin.storage.hybrid.HybridManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Maps;

public class SignatureCalculatorTest extends LocalFileMetadataTestCase {

    private final String projectName = "default";
    private KylinConfig config;

    @Before
    public void setup() throws Exception {
        this.createTestMetadata();
        this.config = getTestConfig();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGetRealizationSignature() {
        RealizationSignature signature1 = RealizationSetCalculator.getRealizationSignature(this.config,
                "Test" + System.currentTimeMillis());
        Assert.assertNull(signature1);

        CubeSignature signature2 = (CubeSignature) RealizationSetCalculator.getRealizationSignature(this.config, "ssb");
        Assert.assertEquals(RealizationStatusEnum.DISABLED, signature2.status);
        Assert.assertNull(signature2.segmentSignatureSet);

        CubeSignature signature3 = (CubeSignature) RealizationSetCalculator.getRealizationSignature(this.config,
                "test_kylin_cube_with_slr_left_join_ready");
        Assert.assertNotNull(signature3.segmentSignatureSet);
    }

    @Test
    public void testRealizationSetCalculator() {
        KylinConfig config = KylinConfig.createKylinConfig(getTestConfig());
        Map<String, String> overrides = Maps.newHashMap();
        overrides.put("kylin.query.signature-class", "org.apache.kylin.rest.signature.RealizationSetCalculator");

        ProjectInstance projectInstance = ProjectManager.getInstance(config).getProject(projectName);
        projectInstance.setConfig(KylinConfigExt.createInstance(config, overrides));

        HybridManager hybridManager = HybridManager.getInstance(config);
        HybridInstance hybrid1 = hybridManager.getHybridInstance("test_kylin_hybrid_ready");

        CubeManager cubeManager = CubeManager.getInstance(config);
        CubeInstance cube1 = cubeManager.getCube("test_kylin_cube_with_slr_left_join_ready");
        CubeInstance cube2 = cubeManager.getCube("test_kylin_cube_without_slr_ready");

        String cubes = hybrid1.getCanonicalName() + "," + cube1.getCanonicalName() + "," + cube2.getCanonicalName();

        SQLResponse sqlResponse = new SQLResponse();
        sqlResponse.setCube(cubes);

        String signature = SQLResponseSignatureUtil.createSignature(config, sqlResponse, projectName);
        sqlResponse.setSignature(signature);

        Assert.assertTrue(SQLResponseSignatureUtil.checkSignature(config, sqlResponse, projectName));

        cube1.setStatus(RealizationStatusEnum.DISABLED);
        Assert.assertFalse(SQLResponseSignatureUtil.checkSignature(config, sqlResponse, projectName));

        cube1.setStatus(RealizationStatusEnum.READY);
        Assert.assertTrue(SQLResponseSignatureUtil.checkSignature(config, sqlResponse, projectName));

        CubeInstance cube3 = cubeManager.getCube("test_kylin_cube_with_slr_ready_2_segments");
        cube3.getSegments().remove(0);
        Assert.assertFalse(SQLResponseSignatureUtil.checkSignature(config, sqlResponse, projectName));
    }
}

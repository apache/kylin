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

package org.apache.kylin.rest.util;

import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.util.RealizationSignature.CubeSignature;
import org.apache.kylin.storage.hybrid.HybridInstance;
import org.apache.kylin.storage.hybrid.HybridManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RealizationSignatureUtilTest extends LocalFileMetadataTestCase {

    private KylinConfig config;

    @Before
    public void setup() throws Exception {
        this.createTestMetadata();
        this.config = KylinConfig.getInstanceFromEnv();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGetRealizationSignature() {
        RealizationSignature signature1 = RealizationSignatureUtil.getRealizationSignature(this.config,
                "Test" + System.currentTimeMillis());
        Assert.assertNull(signature1);

        CubeSignature signature2 = (CubeSignature) RealizationSignatureUtil.getRealizationSignature(this.config, "ssb");
        Assert.assertEquals(RealizationStatusEnum.DISABLED, signature2.status);
        Assert.assertNull(signature2.segmentSignatureSet);

        CubeSignature signature3 = (CubeSignature) RealizationSignatureUtil.getRealizationSignature(this.config,
                "test_kylin_cube_with_slr_left_join_ready");
        Assert.assertNotNull(signature3.segmentSignatureSet);
    }

    @Test
    public void testRealizationSignature() {
        HybridManager hybridManager = HybridManager.getInstance(config);
        HybridInstance hybrid1 = hybridManager.getHybridInstance("test_kylin_hybrid_ready");

        CubeManager cubeManager = CubeManager.getInstance(config);
        CubeInstance cube1 = cubeManager.getCube("test_kylin_cube_with_slr_left_join_ready");
        CubeInstance cube2 = cubeManager.getCube("test_kylin_cube_without_slr_ready");

        String cubes = hybrid1.getCanonicalName() + "," + cube1.getCanonicalName() + "," + cube2.getCanonicalName();
        Set<RealizationSignature> signatureSet = RealizationSignatureUtil.getSignature(config, cubes);

        SQLResponse sqlResponse = new SQLResponse();
        sqlResponse.setCube(cubes);
        sqlResponse.setSignature(signatureSet);

        Assert.assertTrue(RealizationSignatureUtil.checkSignature(config, sqlResponse));

        cube1.setStatus(RealizationStatusEnum.DISABLED);
        Assert.assertFalse(RealizationSignatureUtil.checkSignature(config, sqlResponse));

        cube1.setStatus(RealizationStatusEnum.READY);
        Assert.assertTrue(RealizationSignatureUtil.checkSignature(config, sqlResponse));

        CubeInstance cube3 = cubeManager.getCube("test_kylin_cube_with_slr_ready_2_segments");
        cube3.getSegments().remove(0);
        Assert.assertFalse(RealizationSignatureUtil.checkSignature(config, sqlResponse));
    }
}

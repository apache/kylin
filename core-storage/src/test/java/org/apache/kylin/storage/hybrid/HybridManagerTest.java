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

package org.apache.kylin.storage.hybrid;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.metadata.realization.IRealization;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 */
public class HybridManagerTest extends LocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testBasics() throws Exception {
        HybridInstance hybridInstance = getHybridManager().getHybridInstance("test_kylin_hybrid_ready");
        System.out.println(JsonUtil.writeValueAsIndentString(hybridInstance));

        IRealization[] realizations = hybridInstance.getRealizations();
        Assert.assertEquals(realizations.length, 2);

        IRealization lastReal = hybridInstance.getLatestRealization();
        Assert.assertTrue(lastReal instanceof CubeInstance);
        Assert.assertEquals(lastReal.getName(), "test_kylin_cube_with_slr_ready_2_segments");

    }

    public HybridManager getHybridManager() {
        return HybridManager.getInstance(getTestConfig());
    }
}

/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.cube;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.kylinolap.common.util.JsonUtil;
import com.kylinolap.common.util.LocalFileMetadataTestCase;
import com.kylinolap.cube.model.CubeDesc;

/**
 * @author yangli9
 */
public class CubeDescTest extends LocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testSerialize() throws Exception {
        CubeDesc desc = CubeDescManager.getInstance(this.getTestConfig()).getCubeDesc("test_kylin_cube_with_slr_desc");
        String str = JsonUtil.writeValueAsIndentString(desc);
        System.out.println(str);
        @SuppressWarnings("unused")
        CubeDesc desc2 = JsonUtil.readValue(str, CubeDesc.class);
    }
    

    @Test
    public void testGetCubeDesc() throws Exception {
        CubeDesc cubeDesc = CubeDescManager.getInstance(this.getTestConfig()).getCubeDesc("test_kylin_cube_with_slr_desc");
        Assert.assertNotNull(cubeDesc);
    }

}

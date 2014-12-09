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

import static org.junit.Assert.*;

import com.kylinolap.cube.project.CubeRealizationManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.kylinolap.common.persistence.ResourceStore;
import com.kylinolap.common.util.LocalFileMetadataTestCase;
import com.kylinolap.cube.model.CubeDesc;
import com.kylinolap.metadata.MetadataManager;

/**
 * @author yangli9
 * 
 */
public class CubeManagerCacheTest extends LocalFileMetadataTestCase {

    private CubeManager cubeManager;

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        MetadataManager.removeInstance(this.getTestConfig());
        CubeManager.removeInstance(this.getTestConfig());
        CubeRealizationManager.removeInstance(this.getTestConfig());
        cubeManager = CubeManager.getInstance(this.getTestConfig());
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testReloadCache() throws Exception {
        ResourceStore store = getStore();

        // clean legacy in case last run failed
        store.deleteResource("/cube/a_whole_new_cube.json");
        CubeDescManager cubeDescMgr = getCubeDescManager();
        CubeDesc desc = cubeDescMgr.getCubeDesc("test_kylin_cube_with_slr_desc");
        cubeManager.createCube("a_whole_new_cube", "default", desc, null);

        CubeInstance createdCube = cubeManager.getCube("a_whole_new_cube");
        assertEquals(0, createdCube.getSegments().size());
        assertEquals(CubeStatusEnum.DISABLED, createdCube.getStatus());
        createdCube.setStatus(CubeStatusEnum.DESCBROKEN);

        cubeManager.updateCube(createdCube);
        assertEquals(CubeStatusEnum.DESCBROKEN, cubeManager.getCube("a_whole_new_cube").getStatus());
    }

    public CubeDescManager getCubeDescManager() {
        return CubeDescManager.getInstance(getTestConfig());
    }
}

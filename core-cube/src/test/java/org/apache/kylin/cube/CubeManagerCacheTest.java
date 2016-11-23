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

import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author yangli9
 * 
 */
public class CubeManagerCacheTest extends LocalFileMetadataTestCase {

    private CubeManager cubeManager;

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        MetadataManager.clearCache();
        CubeManager.clearCache();
        ProjectManager.clearCache();
        cubeManager = CubeManager.getInstance(getTestConfig());
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
        assertEquals(RealizationStatusEnum.DISABLED, createdCube.getStatus());
        createdCube.setStatus(RealizationStatusEnum.DESCBROKEN);
        CubeUpdate cubeBuilder = new CubeUpdate(createdCube);

        cubeManager.updateCube(cubeBuilder);
        assertEquals(RealizationStatusEnum.DESCBROKEN, cubeManager.getCube("a_whole_new_cube").getStatus());
    }

    public CubeDescManager getCubeDescManager() {
        return CubeDescManager.getInstance(getTestConfig());
    }
}

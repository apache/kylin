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

package org.apache.kylin.metadata;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.DimensionDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.junit.*;

import java.util.List;

/**
 * Test the data model upgrade
 * @author shaoshi
 *
 */
@Ignore("Not needed, the migrate and test has been moved to CubeMetadataUpgrade.java")
public class MetadataUpgradeTest extends LocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        staticCreateTestMetadata(LOCALMETA_TEST_DATA_V1);
    }

    @After
    public void after() throws Exception {
        //this.cleanupTestMetadata();
    }

    @Test
    public void testCubeDescUpgrade() throws Exception {

        String[] sampleCubeDescs = new String[] { "test_kylin_cube_with_slr_desc", "test_kylin_cube_with_slr_left_join_desc", "test_kylin_cube_without_slr_desc", "test_kylin_cube_without_slr_left_join_desc" };

        for (String name : sampleCubeDescs)
            checkCubeDesc(name);

    }
    
    @Test
    public void testTableDescUpgrade() throws Exception {

        MetadataManager metaMgr = MetadataManager.getInstance(KylinConfig.getInstanceFromEnv());
        TableDesc fact = metaMgr.getTableDesc("default.test_kylin_fact");
        
        @SuppressWarnings("deprecation")
        String oldResLocation = fact.getResourcePathV1();
        String newResLocation = fact.getResourcePath();
        
        ResourceStore store = ResourceStore.getStore(KylinConfig.getInstanceFromEnv());
        
        Assert.assertTrue(store.exists(newResLocation));
        Assert.assertTrue(!store.exists(oldResLocation));
        
        
        String oldExdResLocation = TableDesc.concatExdResourcePath("test_kylin_fact".toUpperCase());
        String newExdResLocation = TableDesc.concatExdResourcePath("default.test_kylin_fact".toUpperCase());
        
        Assert.assertTrue(store.exists(newExdResLocation));
        Assert.assertTrue(!store.exists(oldExdResLocation));
        
    }

    private void checkCubeDesc(String descName) {
        CubeDescManager cubeDescMgr = CubeDescManager.getInstance(KylinConfig.getInstanceFromEnv());
        CubeDesc cubedesc1 = cubeDescMgr.getCubeDesc(descName);
        Assert.assertNotNull(cubedesc1);
        DataModelDesc model = cubedesc1.getModel();
        Assert.assertNotNull(model);
        Assert.assertTrue(model.getLookups().length > 0);

        List<DimensionDesc> dims = cubedesc1.getDimensions();

        Assert.assertTrue(dims.size() > 0);

        for (DimensionDesc dim : dims) {
            Assert.assertTrue(dim.getColumn().length > 0);
        }

        Assert.assertTrue(cubedesc1.getMeasures().size() > 0);

        CubeManager cubeMgr = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
        List<CubeInstance> cubes = cubeMgr.getCubesByDesc(descName);

        Assert.assertTrue(cubes.size() > 0);
    }

}

package com.kylinolap.metadata;

import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import com.kylinolap.cube.CubeDescManager;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.cube.model.CubeDesc;
import com.kylinolap.cube.model.DimensionDesc;
import com.kylinolap.metadata.model.DataModelDesc;
import com.kylinolap.metadata.model.TableDesc;

/**
 * Test the data model upgrade
 * @author shaoshi
 *
 */
@Ignore("Not needed, the migrate and test has been moved to CubeMetadataUpgrade.java")
public class MetadataUpgradeTest extends LocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata(LOCALMETA_TEST_DATA_V1);
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

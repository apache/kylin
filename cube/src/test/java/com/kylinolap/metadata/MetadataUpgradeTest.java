package com.kylinolap.metadata;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.util.LocalFileMetadataTestCase;
import com.kylinolap.cube.CubeDescManager;

/**
 * Test the data model upgrade
 * @author shaoshi
 *
 */
public class MetadataUpgradeTest  extends LocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata(LOCALMETA_TEST_DATA_V1);
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }
    
    @Test
    public void testUpgrade() throws Exception {
        MetadataManager metaMgr = MetadataManager.getInstance(KylinConfig.getInstanceFromEnv());
        
        CubeDescManager cubeDescMgr = CubeDescManager.getInstance(KylinConfig.getInstanceFromEnv());
        
        
    }

}

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

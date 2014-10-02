package com.kylinolap.cube.dataGen;

import com.kylinolap.common.util.LocalFileMetadataTestCase;
import com.kylinolap.metadata.MetadataManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by honma on 6/19/14.
 */
public class DataGenTest extends LocalFileMetadataTestCase {

    @Before
    public void before() throws Exception {
        this.createTestMetadata();
        MetadataManager.removeInstance(this.getTestConfig());
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testBasics() throws Exception {
        FactTableGenerator.generate("test_kylin_cube_with_slr_ready", "10000", "0.6", null, "left");//default settings
    }

}

package com.kylinolap.job;

import com.kylinolap.job.dataGen.FactTableGenerator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.kylinolap.common.util.LocalFileMetadataTestCase;
import com.kylinolap.metadata.MetadataManager;

import static org.junit.Assert.*;

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
        String content = FactTableGenerator.generate("test_kylin_cube_with_slr_ready", "10000", "1", null, "inner");// default  settings
        System.out.println(content);
        assertTrue(content.contains("FP-non GTC"));
        assertTrue(content.contains("ABIN"));

        DeployUtil.overrideFactTableData(content,"test_kylin_fact","inner");
    }

}

package org.apache.kylin.job;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 */
public class BasicLocalMetaTest extends LocalFileMetadataTestCase {
    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void basicTest() {
    }
}

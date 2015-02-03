package com.kylinolap.job.tools;

import java.io.File;
import java.io.IOException;

import org.codehaus.jettison.json.JSONException;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.kylin.common.util.AbstractKylinTestCase;
import org.apache.kylin.common.util.ClasspathUtil;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;

/**
 * Created by honma on 9/17/14.
 */
@Ignore("convenient trial tool for dev")
public class CubeMigrationTests extends LocalFileMetadataTestCase {
    @Before
    public void setup() throws Exception {
        super.createTestMetadata();
        ClasspathUtil.addClasspath(new File(AbstractKylinTestCase.SANDBOX_TEST_DATA).getAbsolutePath());
    }

    @After
    public void clean() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testMigrate() throws IOException, JSONException, InterruptedException {

        // CubeMigrationCLI.moveCube(KylinConfig.getInstanceFromEnv(),
        // KylinConfig.getInstanceFromEnv(),
        // "test_kylin_cube_with_slr_empty", "migration", "true", "false");
    }

}

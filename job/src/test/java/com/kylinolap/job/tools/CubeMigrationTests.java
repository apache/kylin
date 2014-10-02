package com.kylinolap.job.tools;

import com.kylinolap.common.util.ClasspathUtil;
import com.kylinolap.common.util.LocalFileMetadataTestCase;
import org.codehaus.jettison.json.JSONException;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * Created by honma on 9/17/14.
 */
@Ignore
public class CubeMigrationTests extends LocalFileMetadataTestCase {
    @Before
    public void setup() throws Exception {
        super.createTestMetadata();
        ClasspathUtil.addClasspath(new File("../examples/test_case_data/hadoop-site").getAbsolutePath());
    }

    @After
    public void clean() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testMigrate() throws IOException, JSONException, InterruptedException {


        //CubeMigrationCLI.moveCube(KylinConfig.getInstanceFromEnv(), KylinConfig.getInstanceFromEnv(),
        //"test_kylin_cube_with_slr_empty", "migration", "true", "false");
    }

}

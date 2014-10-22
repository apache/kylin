package com.kylinolap.job;

import java.io.File;
import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.util.ClasspathUtil;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;

/**
 * Created by honma on 9/24/14.
 * <p/>
 * This class is only used for building a sample cube in the one-line deployment
 * tool
 */

public class SampleCubeSetupTest extends CubeDevelopTestCase {

    @Before
    public void before() throws Exception {

        String confPaths = System.getenv("KYLIN_HBASE_CONF_PATH");
        System.out.println("The conf paths is " + confPaths);
        if(confPaths!= null) {
            String[] paths = confPaths.split(":");
            for (String path : paths) {
                if (!StringUtils.isEmpty(path)) {
                    try {
                        ClasspathUtil.addClasspath(new File(path).getAbsolutePath());
                    } catch (Exception e) {
                        System.out.println(e.getLocalizedMessage());
                        System.out.println(e.getStackTrace());
                    }
                }
            }
        }


        //this.createTestMetadata();
        String lzoSupportness = System.getenv("KYLIN_LZO_SUPPORTED");
        boolean lzoAvailable = "true".equalsIgnoreCase(lzoSupportness);
        initEnv(false, lzoAvailable);//This test case is run by deploy.sh, which will deploy the adjusted kylin.properties at first

    }

    @After
    public void after() throws IOException {
        // remove all other cubes to keep it clean
        CubeManager cubeManager = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
        for (CubeInstance cubeInstance : cubeManager.listAllCubes()) {
            if (!cubeInstance.getName().equalsIgnoreCase("test_kylin_cube_without_slr_empty") && !cubeInstance.getName().equalsIgnoreCase("test_kylin_cube_with_slr_empty"))
                cubeManager.dropCube(cubeInstance.getName(), false);
        }
    }

    @Test
    public void testCubes() throws Exception {
        // start job schedule engine
        this.prepareTestData("inner");// default settings;
    }
}

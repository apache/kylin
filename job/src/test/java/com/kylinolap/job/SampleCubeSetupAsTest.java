package com.kylinolap.job;

import java.io.File;

import org.apache.commons.lang3.StringUtils;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.util.ClasspathUtil;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by honma on 9/24/14.
 * <p/>
 * This class is only used for building a sample cube in the one-line deployment tool.
 */
public class SampleCubeSetupAsTest {

    @Before
    public void init() throws Exception {
        try {
            testConnectivity();
        } catch (Exception e) {
            System.out.println("Failed to connect to remote CLI with given password");
            throw e;
        }

        String confPaths = System.getenv("KYLIN_HBASE_CONF_PATH");
        System.out.println("The conf paths is " + confPaths);
        if (confPaths != null) {
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
    }

    private void testConnectivity() throws Exception {
        KylinConfig cfg = KylinConfig.getInstanceFromEnv();
        cfg.getCliCommandExecutor().execute("echo hello");
    }

    @Test
    public void prepareCubesAsTest() throws Exception {
        DeployUtil.initCliWorkDir();
        DeployUtil.deployMetadata();
        DeployUtil.deployJobJars();
        DeployUtil.prepareTestData("inner", "test_kylin_cube_with_slr_empty");

        // remove all other cubes to keep it clean
        CubeManager cubeManager = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
        for (CubeInstance cubeInstance : cubeManager.listAllCubes()) {
            if (!cubeInstance.getName().equalsIgnoreCase("test_kylin_cube_without_slr_empty") && !cubeInstance.getName().equalsIgnoreCase("test_kylin_cube_with_slr_empty"))
                cubeManager.dropCube(cubeInstance.getName(), false);
        }
    }

}

package com.kylinolap.job;

import java.io.File;
import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.codehaus.plexus.util.FileUtils;
import org.junit.Before;
import org.junit.Test;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.util.ClasspathUtil;
import com.kylinolap.common.util.HBaseMetadataTestCase;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.job.engine.JobEngineConfig;

/**
 * Created by honma on 9/24/14.
 * <p/>
 * This class is only used for building a sample cube in the one-line deployment tool.
 */
public class SampleCubeSetupTest extends HBaseMetadataTestCase {

    @Before
    public void before() throws Exception {

        try {
            this.testConnectivity();
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
    public void testCubes() throws Exception {
        DeployUtil.initCliWorkDir();
        DeployUtil.deployMetadata();
        DeployUtil.deployJobJars();
        deployJobConfToEtc();
        DeployUtil.prepareTestData("inner", "test_kylin_cube_with_slr_empty");

        // remove all other cubes to keep it clean
        CubeManager cubeManager = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
        for (CubeInstance cubeInstance : cubeManager.listAllCubes()) {
            if (!cubeInstance.getName().equalsIgnoreCase("test_kylin_cube_without_slr_empty") && !cubeInstance.getName().equalsIgnoreCase("test_kylin_cube_with_slr_empty"))
                cubeManager.dropCube(cubeInstance.getName(), false);
        }

    }

    private void deployJobConfToEtc() throws IOException {
        String lzoSupportness = System.getenv("KYLIN_LZO_SUPPORTED");
        boolean enableLzo = "true".equalsIgnoreCase(lzoSupportness);
        DeployUtil.overrideJobConf(SANDBOX_TEST_DATA, enableLzo);
        
        File src = new File(SANDBOX_TEST_DATA, JobEngineConfig.HADOOP_JOB_CONF_FILENAME + ".xml");
        File dst = new File("/etc/kylin", src.getName());
        FileUtils.copyFile(src, dst);
    }

}

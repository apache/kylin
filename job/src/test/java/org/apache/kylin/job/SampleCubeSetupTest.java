/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.kylin.job;

import java.io.File;
import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.codehaus.plexus.util.FileUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by honma on 9/24/14.
 * <p/>
 * This class is only used for building a sample cube in the one-line deployment tool.
 */
public class SampleCubeSetupTest extends HBaseMetadataTestCase {

    @Before
    public void before() throws Exception {

        String confPaths = System.getenv("KYLIN_HBASE_CONF_PATH");
        System.out.println("The conf paths is " + confPaths);
        if (confPaths != null) {
            String[] paths = confPaths.split(":");
            for (String path : paths) {
                if (!StringUtils.isEmpty(path)) {
                    try {
                        ClassUtil.addClasspath(new File(path).getAbsolutePath());
                    } catch (Exception e) {
                        System.out.println(e.getLocalizedMessage());
                        System.out.println(e.getStackTrace());
                    }
                }
            }
        }
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
        File src = new File(SANDBOX_TEST_DATA, JobEngineConfig.HADOOP_JOB_CONF_FILENAME + ".xml");
        File dst = new File("/etc/kylin", src.getName());
        FileUtils.copyFile(src, dst);
    }

}

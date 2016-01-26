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

package org.apache.kylin.provision;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.TimeZone;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AbstractKylinTestCase;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.engine.spark.SparkBatchCubingEngine;
import org.apache.kylin.job.DeployUtil;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.impl.threadpool.DefaultScheduler;
import org.apache.kylin.job.lock.MockJobLock;
import org.apache.kylin.job.manager.ExecutableManager;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Lists;

//TODO: convert it to a normal class rather than a test case, like in BuildCubeWithEngine 
@Ignore
public class BuildCubeWithSpark {

    private CubeManager cubeManager;
    private DefaultScheduler scheduler;
    protected ExecutableManager jobService;

    private static final Log logger = LogFactory.getLog(BuildCubeWithSpark.class);

    protected void waitForJob(String jobId) {
        while (true) {
            AbstractExecutable job = jobService.getJob(jobId);
            if (job.getStatus() == ExecutableState.SUCCEED || job.getStatus() == ExecutableState.ERROR) {
                break;
            } else {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        logger.info("Adding to classpath: " + new File(HBaseMetadataTestCase.SANDBOX_TEST_DATA).getAbsolutePath());
        ClassUtil.addClasspath(new File(HBaseMetadataTestCase.SANDBOX_TEST_DATA).getAbsolutePath());
        System.setProperty(KylinConfig.KYLIN_CONF, "../examples/test_case_data/sandbox");
        if (StringUtils.isEmpty(System.getProperty("hdp.version"))) {
            throw new RuntimeException("No hdp.version set; Please set hdp.version in your jvm option, for example: -Dhdp.version=2.2.4.2-2");
        }

    }

    @Before
    public void before() throws Exception {
        HBaseMetadataTestCase.staticCreateTestMetadata(AbstractKylinTestCase.SANDBOX_TEST_DATA);

        DeployUtil.initCliWorkDir();
        DeployUtil.deployMetadata();
        DeployUtil.overrideJobJarLocations();

        final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        jobService = ExecutableManager.getInstance(kylinConfig);
        for (String jobId : jobService.getAllJobIds()) {
            jobService.deleteJob(jobId);
        }
        scheduler = DefaultScheduler.getInstance();
        scheduler.init(new JobEngineConfig(kylinConfig), new MockJobLock());
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
        cubeManager = CubeManager.getInstance(kylinConfig);

    }

    @After
    public void after() {
        HBaseMetadataTestCase.staticCleanupTestMetadata();
    }

    @Test
    public void test() throws Exception {
        final CubeSegment segment = createSegment();
        String confPath = new File(AbstractKylinTestCase.SANDBOX_TEST_DATA).getAbsolutePath();
        KylinConfig.getInstanceFromEnv().getCoprocessorLocalJar();
        String coprocessor = KylinConfig.getInstanceFromEnv().getCoprocessorLocalJar();
        logger.info("confPath location:" + confPath);
        logger.info("coprocessor location:" + coprocessor);
        final DefaultChainedExecutable cubingJob = new SparkBatchCubingEngine(confPath, coprocessor).createBatchCubingJob(segment, "BuildCubeWithSpark");
        jobService.addJob(cubingJob);
        waitForJob(cubingJob.getId());

        if (jobService.getOutput(cubingJob.getId()).getState() != ExecutableState.SUCCEED) {
            throw new RuntimeException("The job '" + cubingJob.getId() + "' is failed.");
        }
    }

    private void clearSegment(String cubeName) throws Exception {
        CubeInstance cube = cubeManager.getCube(cubeName);
        // remove all existing segments
        CubeUpdate cubeBuilder = new CubeUpdate(cube);
        cubeBuilder.setToRemoveSegs(cube.getSegments().toArray(new CubeSegment[cube.getSegments().size()]));
        cubeManager.updateCube(cubeBuilder);
    }

    private CubeSegment createSegment() throws Exception {
        String cubeName = "test_kylin_cube_with_slr_left_join_empty";
        clearSegment(cubeName);

        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
        f.setTimeZone(TimeZone.getTimeZone("GMT"));
        long dateStart = cubeManager.getCube(cubeName).getDescriptor().getPartitionDateStart();
        long dateEnd = f.parse("2050-11-12").getTime();

        // this cube's start date is 0, end date is 20501112000000
        List<String> result = Lists.newArrayList();
        return cubeManager.appendSegments(cubeManager.getCube(cubeName), dateEnd);

    }

}
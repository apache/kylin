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

package org.apache.kylin.engine.spark.job;

import io.kyligence.kap.engine.spark.job.NSparkCubingJob;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceTool;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.spark.LocalWithSparkSessionTest;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.CheckpointExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.impl.threadpool.DefaultScheduler;
import org.apache.kylin.job.lock.MockJobLock;
import org.apache.kylin.metadata.model.SegmentRange;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.collect.Sets;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.TimeZone;

public class SparkCubingJobTest extends LocalWithSparkSessionTest {

    private static final Logger logger = LoggerFactory.getLogger(SparkCubingJobTest.class);

    private CubeManager cubeManager;
    private DefaultScheduler scheduler;
    private ExecutableManager jobService;


    @Before
    public void setup() throws Exception{
        ss.sparkContext().setLogLevel("ERROR");
        System.setProperty("kylin.job.scheduler.poll-interval-second", "1");
        System.setProperty("kap.engine.persist-flattable-threshold", "0");
        System.setProperty("kylin.metadata.distributed-lock-impl", "io.kyligence.kap.engine.spark.utils.MockedDistributedLock$MockedFactory");
        System.setProperty(KylinConfig.KYLIN_CONF, HBaseMetadataTestCase.SANDBOX_TEST_DATA);
        final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        cubeManager = CubeManager.getInstance(kylinConfig);
        jobService = ExecutableManager.getInstance(kylinConfig);
        scheduler = DefaultScheduler.createInstance();
        scheduler.init(new JobEngineConfig(kylinConfig), new MockJobLock());
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
        for (String jobId : jobService.getAllJobIds()) {
            AbstractExecutable executable = jobService.getJob(jobId);
            if (executable instanceof CheckpointExecutable) {
                jobService.deleteJob(jobId);
            }
        }

        //initEnv();
    }

    @Test
    public void testBuildJob() throws Exception{

        String cubeName = "ci_inner_join_cube";
        CubeInstance cubeInstance = cubeManager.getCube(cubeName);
        clearSegment(cubeInstance);

        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd", Locale.ROOT);
        f.setTimeZone(TimeZone.getTimeZone("GMT"));
        long date1 = 0;
        long date2 = f.parse("2012-06-01").getTime();
        long date3 = f.parse("2013-07-01").getTime();

        CubeSegment segment = cubeManager.appendSegment(cubeInstance, new SegmentRange.TSRange(date1, date2));
        NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(segment), "ADMIN");
        jobService.addJob(job);
        // wait job done
        ExecutableState state = waitForJob(job.getId());
        state = wait(job);
        Assert.assertEquals(ExecutableState.SUCCEED, state);

        CubeSegment segment2 = cubeManager.appendSegment(cubeInstance, new SegmentRange.TSRange(date2, date3));
        NSparkCubingJob job2 = NSparkCubingJob.create(Sets.newHashSet(segment2), "ADMIN");
        jobService.addJob(job);
        // wait job done
        ExecutableState state2 = waitForJob(job2.getId());
        state = wait(job2);
        Assert.assertEquals(ExecutableState.SUCCEED, state2);

        //TODO: test merge job
        //merge

    }

    private void clearSegment(CubeInstance cube) throws Exception {
        cubeManager.updateCubeDropSegments(cube, cube.getSegments());
    }

    protected ExecutableState waitForJob(String jobId) {
        while (true) {
            AbstractExecutable job = jobService.getJob(jobId);
            if (job.getStatus() == ExecutableState.SUCCEED || job.getStatus() == ExecutableState.ERROR) {
                return job.getStatus();
            } else {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void deployMetadata(String localMetaData) throws IOException {
        // install metadata to hbase
        new ResourceTool().reset(config());
        new ResourceTool().copy(KylinConfig.createInstanceFromUri(localMetaData), config());

        // update cube desc signature.
        for (CubeInstance cube : CubeManager.getInstance(config()).listAllCubes()) {
            CubeDescManager.getInstance(config()).updateCubeDesc(cube.getDescriptor());//enforce signature updating
        }
    }

    private static KylinConfig config() {
        return KylinConfig.getInstanceFromEnv();
    }
}

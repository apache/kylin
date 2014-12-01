/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.job;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.quartz.SchedulerException;

import com.google.common.collect.Lists;
import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.util.ClasspathUtil;
import com.kylinolap.common.util.HBaseMetadataTestCase;
import com.kylinolap.common.util.JsonUtil;
import com.kylinolap.cube.CubeBuildTypeEnum;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.cube.CubeSegment;
import com.kylinolap.cube.exception.CubeIntegrityException;
import com.kylinolap.job.constant.JobStatusEnum;
import com.kylinolap.job.engine.JobEngineConfig;
import com.kylinolap.job.exception.InvalidJobInstanceException;

/**
 * @author ysong1
 */
public class BuildCubeWithEngineTest extends HBaseMetadataTestCase {

    protected JobManager jobManager;
    protected JobEngineConfig engineConfig;

    @BeforeClass
    public static void beforeClass() throws Exception {
        ClasspathUtil.addClasspath(new File(SANDBOX_TEST_DATA).getAbsolutePath());
    }

    @Before
    public void before() throws Exception {
        this.createTestMetadata();

        DeployUtil.initCliWorkDir();
        DeployUtil.deployMetadata();
        DeployUtil.overrideJobJarLocations();
        DeployUtil.overrideJobConf(SANDBOX_TEST_DATA);

        engineConfig = new JobEngineConfig(KylinConfig.getInstanceFromEnv());
        jobManager = new JobManager("Build_Test_Cube_Engine", engineConfig);
        jobManager.deleteAllJobs();
    }

    @After
    public void after() throws IOException {
        // jobManager.deleteAllJobs();
        this.cleanupTestMetadata();
    }

    @Test
    public void testCubes() throws Exception {

        // start job schedule engine
        jobManager.startJobEngine(10);

//        testSimpleLeftJoinCube();
        
        // keep this order.
        testLeftJoinCube();
        testInnerJoinCube();

        jobManager.stopJobEngine();
    }

    /**
     * For cube test_kylin_cube_with_slr_empty, we will create 2 segments For
     * cube test_kylin_cube_without_slr_empty, since it doesn't support
     * incremental build, we will create only 1 segment (full build)
     *
     * @throws Exception
     */
    private void testInnerJoinCube() throws Exception {
        DeployUtil.prepareTestData("inner", "test_kylin_cube_with_slr_empty");

        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
        f.setTimeZone(TimeZone.getTimeZone("GMT"));
        long dateStart;
        long dateEnd;

        ArrayList<String> jobs = new ArrayList<String>();

        // this cube's start date is 0, end date is 20501112000000
        dateStart = 0;
        dateEnd = f.parse("2013-01-01").getTime();
        jobs.addAll(this.submitJob("test_kylin_cube_with_slr_empty", dateStart, dateEnd, CubeBuildTypeEnum.BUILD));

        // this cube doesn't support incremental build, always do full build
        jobs.addAll(this.submitJob("test_kylin_cube_without_slr_empty", 0, 0, CubeBuildTypeEnum.BUILD));

        waitCubeBuilt(jobs);

        // then submit a incremental job, start date is 20130101000000, end date
        // is 20220101000000
        jobs.clear();
        dateStart = f.parse("2013-01-01").getTime();
        dateEnd = f.parse("2022-01-01").getTime();
        jobs.addAll(this.submitJob("test_kylin_cube_with_slr_empty", dateStart, dateEnd, CubeBuildTypeEnum.BUILD));
        waitCubeBuilt(jobs);
    }

    /**
     * For cube test_kylin_cube_without_slr_left_join_empty, it is using
     * update_insert, we will create 2 segments, and then merge these 2 segments
     * into a larger segment For cube test_kylin_cube_with_slr_left_join_empty,
     * we will create only 1 segment
     *
     * @throws Exception
     */
    private void testLeftJoinCube() throws Exception {
        DeployUtil.prepareTestData("left", "test_kylin_cube_with_slr_left_join_empty");

        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
        f.setTimeZone(TimeZone.getTimeZone("GMT"));
        long dateStart;
        long dateEnd;

        ArrayList<String> jobs = new ArrayList<String>();

        // this cube's start date is 0, end date is 20501112000000
        CubeManager cubeMgr = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
        dateStart = cubeMgr.getCube("test_kylin_cube_with_slr_left_join_empty").getDescriptor().getCubePartitionDesc().getPartitionDateStart();
        dateEnd = f.parse("2050-11-12").getTime();
        jobs.addAll(this.submitJob("test_kylin_cube_with_slr_left_join_empty", dateStart, dateEnd, CubeBuildTypeEnum.BUILD));

        // this cube's start date is 0, end date is 20120601000000
        dateStart = cubeMgr.getCube("test_kylin_cube_without_slr_left_join_empty").getDescriptor().getCubePartitionDesc().getPartitionDateStart();
        dateEnd = f.parse("2012-06-01").getTime();
        jobs.addAll(this.submitJob("test_kylin_cube_without_slr_left_join_empty", dateStart, dateEnd, CubeBuildTypeEnum.BUILD));

        waitCubeBuilt(jobs);

        jobs.clear();
        // then submit a update_insert job, start date is 20120101000000, end
        // date is 20220101000000
        dateStart = f.parse("2012-03-01").getTime();
        dateEnd = f.parse("2022-01-01").getTime();
        jobs.addAll(this.submitJob("test_kylin_cube_without_slr_left_join_empty", dateStart, dateEnd, CubeBuildTypeEnum.BUILD));

        waitCubeBuilt(jobs);

        jobs.clear();

        // final submit a merge job, start date is 0, end date is 20220101000000
        //dateEnd = f.parse("2022-01-01").getTime();
        //jobs.addAll(this.submitJob("test_kylin_cube_without_slr_left_join_empty", 0, dateEnd, CubeBuildTypeEnum.MERGE));
        //waitCubeBuilt(jobs);
    }

    @SuppressWarnings("unused")
    private void testSimpleLeftJoinCube() throws Exception {
        DeployUtil.prepareTestData("left", "test_kylin_cube_with_slr_left_join_empty");
        
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
        f.setTimeZone(TimeZone.getTimeZone("GMT"));
        long dateStart;
        long dateEnd;
        
        ArrayList<String> jobs = new ArrayList<String>();
        
        // this cube's start date is 0, end date is 20501112000000
        CubeManager cubeMgr = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
        dateStart = cubeMgr.getCube("test_kylin_cube_with_slr_left_join_empty").getDescriptor().getCubePartitionDesc().getPartitionDateStart();
        dateEnd = f.parse("2050-11-12").getTime();
        jobs.addAll(this.submitJob("test_kylin_cube_with_slr_left_join_empty", dateStart, dateEnd, CubeBuildTypeEnum.BUILD));
        
        // this cube's start date is 0, end date is 20501112000000
        dateStart = cubeMgr.getCube("test_kylin_cube_without_slr_left_join_empty").getDescriptor().getCubePartitionDesc().getPartitionDateStart();
        dateEnd = f.parse("2050-11-12").getTime();
        jobs.addAll(this.submitJob("test_kylin_cube_without_slr_left_join_empty", dateStart, dateEnd, CubeBuildTypeEnum.BUILD));
        
        waitCubeBuilt(jobs);
    }
    
    protected void waitCubeBuilt(List<String> jobs) throws Exception {

        boolean allFinished = false;
        while (!allFinished) {
            // sleep for 1 minutes
            Thread.sleep(60 * 1000L);

            allFinished = true;
            for (String job : jobs) {
                JobInstance savedJob = jobManager.getJob(job);
                JobStatusEnum jobStatus = savedJob.getStatus();
                if (jobStatus.getCode() <= JobStatusEnum.RUNNING.getCode()) {
                    allFinished = false;
                    break;
                }
            }
        }

        for (String job : jobs)
            assertEquals("Job fail - " + job, JobStatusEnum.FINISHED, jobManager.getJob(job).getStatus());
    }

    protected List<String> submitJob(String cubename, long startDate, long endDate, CubeBuildTypeEnum jobType) throws SchedulerException, IOException, InvalidJobInstanceException, CubeIntegrityException {

        CubeManager cubeMgr = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
        CubeInstance cube = cubeMgr.getCube(cubename);
        CubeManager.getInstance(this.getTestConfig()).loadCubeCache(cube);

        System.out.println(JsonUtil.writeValueAsIndentString(cube));
        List<CubeSegment> newSegments = cubeMgr.allocateSegments(cube, jobType, startDate, endDate);
        System.out.println(JsonUtil.writeValueAsIndentString(cube));

        List<String> jobUuids = Lists.newArrayList();
        List<JobInstance> jobs = Lists.newArrayList();
        for (CubeSegment seg : newSegments) {
            String uuid = seg.getUuid();
            jobUuids.add(uuid);
            jobs.add(jobManager.createJob(cubename, seg.getName(), uuid, jobType));
            seg.setLastBuildJobID(uuid);
        }
        cubeMgr.updateCube(cube);
        for (JobInstance job: jobs) {
            // submit job to store
            jobManager.submitJob(job);
        }
        return jobUuids;
    }
}
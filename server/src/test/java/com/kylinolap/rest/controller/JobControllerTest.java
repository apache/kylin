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

package com.kylinolap.rest.controller;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.cube.CubeDescManager;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.cube.model.CubeDesc;
import com.kylinolap.job.JobDAO;
import com.kylinolap.metadata.MetadataManager;
import com.kylinolap.rest.exception.InternalErrorException;
import com.kylinolap.rest.exception.NotFoundException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import com.kylinolap.job.JobInstance;
import com.kylinolap.rest.request.JobBuildRequest;
import com.kylinolap.rest.request.JobListRequest;
import com.kylinolap.rest.service.CubeService;
import com.kylinolap.rest.service.JobService;
import com.kylinolap.rest.service.ServiceTestBase;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author xduo
 */
public class JobControllerTest extends ServiceTestBase {

    private JobController jobSchedulerController;
    private CubeController cubeController;
    @Autowired
    JobService jobService;

    @Autowired
    CubeService cubeService;
    private static final String CUBE_NAME = "new_job_controller";


    @Before
    public void setup() throws Exception {
        super.setUp();

        jobSchedulerController = new JobController();
        jobSchedulerController.setJobService(jobService);
        cubeController = new CubeController();
        cubeController.setJobService(jobService);
        cubeController.setCubeService(cubeService);

    }

    @Test
    public void testBasics() throws IOException {

        KylinConfig testConfig = getTestConfig();
        CubeManager cubeManager = CubeManager.getInstance(testConfig);
        if (cubeManager.getCube(CUBE_NAME) != null) {
            cubeManager.dropCube(CUBE_NAME, false);
        }
        CubeDescManager cubeDescManager = CubeDescManager.getInstance(testConfig);
        CubeDesc cubeDesc = cubeDescManager.getCubeDesc("test_kylin_cube_with_slr_left_join_desc");
        CubeInstance cube = cubeManager.createCube(CUBE_NAME, "DEFAULT", cubeDesc, "test");
        assertNotNull(cube);

        JobListRequest jobRequest = new JobListRequest();
        Assert.assertNotNull(jobSchedulerController.list(jobRequest));

        JobBuildRequest jobBuildRequest = new JobBuildRequest();
        jobBuildRequest.setBuildType("BUILD");
        jobBuildRequest.setStartTime(0L);
        jobBuildRequest.setEndTime(new Date().getTime());
        JobInstance job = cubeController.rebuild(CUBE_NAME, jobBuildRequest);


        Assert.assertNotNull(jobSchedulerController.get(job.getId()));
        Map<String, String> output = jobSchedulerController.getStepOutput(job.getId(), 0);
        Assert.assertNotNull(output);
        JobDAO.getInstance(testConfig).deleteJob(job);

        // jobSchedulerController.cancel(job.getId());
    }

    @Test(expected = RuntimeException.class)
    public void testResume() throws IOException {
        JobBuildRequest jobBuildRequest = new JobBuildRequest();
        jobBuildRequest.setBuildType("BUILD");
        jobBuildRequest.setStartTime(20130331080000L);
        jobBuildRequest.setEndTime(20131212080000L);
        JobInstance job = cubeController.rebuild(CUBE_NAME, jobBuildRequest);

        jobSchedulerController.resume(job.getId());
    }
}

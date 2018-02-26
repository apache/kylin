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

package org.apache.kylin.rest.controller;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.job.JobInstance;
import org.apache.kylin.job.dao.ExecutableDao;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.rest.request.JobBuildRequest;
import org.apache.kylin.rest.request.JobListRequest;
import org.apache.kylin.rest.service.CubeService;
import org.apache.kylin.rest.service.JobService;
import org.apache.kylin.rest.service.ServiceTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.io.IOException;
import java.util.Date;

import static org.junit.Assert.assertNotNull;

/**
 * @author xduo
 */
public class JobControllerTest extends ServiceTestBase {

    private JobController jobSchedulerController;
    private CubeController cubeController;
    @Autowired
    @Qualifier("jobService")
    JobService jobService;

    @Autowired
    @Qualifier("cubeMgmtService")
    CubeService cubeService;
    private static final String CUBE_NAME = "new_job_controller";

    private CubeManager cubeManager;
    private CubeDescManager cubeDescManager;
    private ExecutableDao executableDAO;

    @Before
    public void setup() throws Exception {
        super.setup();

        jobSchedulerController = new JobController();
        jobSchedulerController.setJobService(jobService);
        cubeController = new CubeController();
        cubeController.setJobService(jobService);
        cubeController.setCubeService(cubeService);

        KylinConfig testConfig = getTestConfig();
        cubeManager = CubeManager.getInstance(testConfig);
        cubeDescManager = CubeDescManager.getInstance(testConfig);
        executableDAO = ExecutableDao.getInstance(testConfig);

    }

    @After
    public void tearDown() throws Exception {
        if (cubeManager.getCube(CUBE_NAME) != null) {
            cubeManager.dropCube(CUBE_NAME, false);
        }
    }

    @Test
    public void testBasics() throws IOException, PersistentException {
        CubeDesc cubeDesc = cubeDescManager.getCubeDesc("test_kylin_cube_with_slr_left_join_desc");
        CubeInstance cube = cubeManager.createCube(CUBE_NAME, "DEFAULT", cubeDesc, "test");
        assertNotNull(cube);

        JobListRequest jobRequest = new JobListRequest();
        jobRequest.setTimeFilter(4);
        Assert.assertNotNull(jobSchedulerController.list(jobRequest));

        jobRequest.setJobSearchMode("ALL");
        Assert.assertNotNull(jobSchedulerController.list(jobRequest));


        jobRequest.setJobSearchMode("");
        Assert.assertNotNull(jobSchedulerController.list(jobRequest));


        jobRequest.setJobSearchMode("wrong-input");
        Assert.assertNotNull(jobSchedulerController.list(jobRequest));

        JobBuildRequest jobBuildRequest = new JobBuildRequest();
        jobBuildRequest.setBuildType("BUILD");
        jobBuildRequest.setStartTime(0L);
        jobBuildRequest.setEndTime(new Date().getTime());
        JobInstance job = cubeController.rebuild(CUBE_NAME, jobBuildRequest);

        Assert.assertNotNull(jobSchedulerController.get(job.getId()));
        executableDAO.deleteJob(job.getId());
        if (cubeManager.getCube(CUBE_NAME) != null) {
            cubeManager.dropCube(CUBE_NAME, false);
        }

        // jobSchedulerController.cancel(job.getId());
    }

    @Test(expected = RuntimeException.class)
    public void testResume() throws IOException {
        JobBuildRequest jobBuildRequest = new JobBuildRequest();
        jobBuildRequest.setBuildType("BUILD");
        jobBuildRequest.setStartTime(20130331080000L);
        jobBuildRequest.setEndTime(20131212080000L);

        // Yang: how to rebuild a cube does not exists?!
        JobInstance job = cubeController.rebuild(CUBE_NAME, jobBuildRequest);

        jobSchedulerController.resume(job.getId());
    }
}

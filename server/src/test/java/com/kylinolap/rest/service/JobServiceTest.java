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

package com.kylinolap.rest.service;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.kylinolap.cube.project.ProjectInstance;
import com.kylinolap.job.exception.JobException;

/**
 * @author xduo
 * 
 */
public class JobServiceTest extends ServiceTestBase {

    @Autowired
    JobService jobService;

    @Test
    public void testBasics() throws JobException, IOException {
        Assert.assertNotNull(jobService.getJobManager());
        Assert.assertNotNull(jobService.getConfig());
        Assert.assertNotNull(jobService.getKylinConfig());
        Assert.assertNotNull(jobService.getMetadataManager());
        Assert.assertNotNull(jobService.getOLAPDataSource(ProjectInstance.DEFAULT_PROJECT_NAME));
        Assert.assertNull(jobService.getJobInstance("job_not_exist"));
        Assert.assertNotNull(jobService.listAllJobs(null, null, null));
    }
}

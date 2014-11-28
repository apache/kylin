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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.util.LocalFileMetadataTestCase;
import com.kylinolap.cube.CubeBuildTypeEnum;
import com.kylinolap.job.JobInstance.JobStep;
import com.kylinolap.job.constant.JobStatusEnum;
import com.kylinolap.job.constant.JobStepStatusEnum;
import com.kylinolap.job.engine.JobEngineConfig;

import java.util.UUID;

/**
 * @author ysong1
 * 
 */
public class JobInstanceTest extends LocalFileMetadataTestCase {
    @Before
    public void before() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testJobInstanceStatus() throws Exception {
        KylinConfig kylinCfg = KylinConfig.getInstanceFromEnv();
        JobManager jobManager = new JobManager("JobInstanceTest", new JobEngineConfig(kylinCfg));

        JobInstance jobInstance = jobManager.createJob("test_kylin_cube_with_slr_1_new_segment", "20130331080000_20131212080000", UUID.randomUUID().toString(), CubeBuildTypeEnum.BUILD);
        // initial job status should be PENDING
        assertEquals(JobStatusEnum.PENDING, jobInstance.getStatus());

        // if a step fails, job status should be ERROR
        jobInstance.getSteps().get(3).setStatus(JobStepStatusEnum.ERROR);
        assertEquals(JobStatusEnum.ERROR, jobInstance.getStatus());

        // then resume job, job status should be NEW
        jobInstance.getSteps().get(0).setStatus(JobStepStatusEnum.FINISHED);
        jobInstance.getSteps().get(1).setStatus(JobStepStatusEnum.FINISHED);
        jobInstance.getSteps().get(2).setStatus(JobStepStatusEnum.FINISHED);
        jobInstance.getSteps().get(3).setStatus(JobStepStatusEnum.PENDING);
        assertEquals(JobStatusEnum.PENDING, jobInstance.getStatus());

        // running job
        jobInstance.getSteps().get(0).setStatus(JobStepStatusEnum.FINISHED);
        jobInstance.getSteps().get(1).setStatus(JobStepStatusEnum.FINISHED);
        jobInstance.getSteps().get(2).setStatus(JobStepStatusEnum.FINISHED);
        jobInstance.getSteps().get(3).setStatus(JobStepStatusEnum.RUNNING);
        assertEquals(JobStatusEnum.RUNNING, jobInstance.getStatus());

        // kill job
        jobInstance.getSteps().get(0).setStatus(JobStepStatusEnum.FINISHED);
        jobInstance.getSteps().get(1).setStatus(JobStepStatusEnum.FINISHED);
        jobInstance.getSteps().get(2).setStatus(JobStepStatusEnum.FINISHED);
        jobInstance.getSteps().get(3).setStatus(JobStepStatusEnum.DISCARDED);
        assertEquals(JobStatusEnum.DISCARDED, jobInstance.getStatus());

        // finish job
        for (JobStep step : jobInstance.getSteps()) {
            step.setStatus(JobStepStatusEnum.FINISHED);
        }
        assertEquals(JobStatusEnum.FINISHED, jobInstance.getStatus());

        // finish job
        for (JobStep step : jobInstance.getSteps()) {
            step.setStatus(JobStepStatusEnum.NEW);
        }
        assertEquals(JobStatusEnum.NEW, jobInstance.getStatus());

        // default
        jobInstance.getSteps().get(3).setStatus(JobStepStatusEnum.WAITING);
        assertEquals(JobStatusEnum.RUNNING, jobInstance.getStatus());
    }

}

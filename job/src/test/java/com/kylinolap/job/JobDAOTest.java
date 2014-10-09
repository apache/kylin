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

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.kylinolap.common.util.LocalFileMetadataTestCase;
import com.kylinolap.cube.CubeBuildTypeEnum;
import com.kylinolap.job.exception.InvalidJobInstanceException;

/** 
* @author George Song (ysong1)
* 
*/
public class JobDAOTest extends LocalFileMetadataTestCase {

    @Before
    public void setup() throws Exception {
        createTestMetadata();
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void test() throws IOException, InvalidJobInstanceException {

        String uuid = "132432cb-8c68-42d8-aa3a-504151b39d1b";
        JobDAO service = JobDAO.getInstance(getTestConfig());
        JobInstance job = createDumbJobInstance(uuid);
        assertEquals(0, job.getLastModified());
        service.updateJobInstance(job);

        // test read
        JobInstance job2 = service.getJob(uuid);
        //assertEquals(JobStatusEnum.PENDING, job2.getStatus());
        assertTrue(job2.getLastModified() > 0);

        // test modify 
        job2.setRelatedCube("abc");
        service.updateJobInstance(job2);
        JobInstance job3 = service.getJob(uuid);
        assertEquals("abc", job3.getRelatedCube());
        assertTrue(job3.getLastModified() > 0);

        // test delete
        service.deleteJob(job2);
        JobInstance job4 = service.getJob(uuid);
        assertNull(job4);
    }

    @Test
    public void testOutput() throws IOException, InvalidJobInstanceException {
        String uuid = "132432cb-8c68-42d8-aa3a-504151b39d1b";
        int seq = 1;
        String s = "this is output";
        JobDAO service = JobDAO.getInstance(getTestConfig());
        service.saveJobOutput(uuid, seq, s);

        // test read
        JobStepOutput output2 = service.getJobOutput(uuid, seq);
        assertTrue(output2.getLastModified() > 0);
        assertEquals(s, output2.getOutput());

    }

    private JobInstance createDumbJobInstance(String uuid) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(SerializationFeature.INDENT_OUTPUT, true);

            JobInstance jobInstance = new JobInstance();
            jobInstance.setUuid(uuid);
            jobInstance.setType(CubeBuildTypeEnum.BUILD);
            jobInstance.setRelatedCube("test_kylin_cube_with_slr".toUpperCase());
            jobInstance.setName("Dummy_Job");
            //jobInstance.setStatus(JobStatusEnum.PENDING);

            return jobInstance;
        } catch (Exception e) {
            return null;
        }
    }
}

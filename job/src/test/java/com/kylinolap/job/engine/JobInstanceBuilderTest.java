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
package com.kylinolap.job.engine;

import static org.junit.Assert.*;

import java.text.SimpleDateFormat;
import java.util.TimeZone;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.util.JsonUtil;
import com.kylinolap.common.util.LocalFileMetadataTestCase;
import com.kylinolap.cube.CubeBuildTypeEnum;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.cube.CubeSegment;
import com.kylinolap.cube.project.ProjectManager;
import com.kylinolap.dict.DictionaryManager;
import com.kylinolap.job.JobInstance;
import com.kylinolap.job.JobInstance.JobStep;
import com.kylinolap.job.JobManager;
import com.kylinolap.job.constant.JobStepCmdTypeEnum;
import com.kylinolap.metadata.MetadataManager;

/**
 * @author George Song (ysong1)
 * 
 */
public class JobInstanceBuilderTest extends LocalFileMetadataTestCase {

    @Before
    public void before() throws Exception {
        this.createTestMetadata();
        MetadataManager.removeInstance(this.getTestConfig());
        CubeManager.removeInstance(this.getTestConfig());
        ProjectManager.removeInstance(this.getTestConfig());
        DictionaryManager.removeInstance(this.getTestConfig());
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testCreateSteps() throws Exception {
        // create a new cube
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
        f.setTimeZone(TimeZone.getTimeZone("GMT"));

        long dateEnd = f.parse("2013-11-12").getTime();

        JobManager jobManager = new JobManager("JobInstanceBuilderTest", new JobEngineConfig(KylinConfig.getInstanceFromEnv()));
        String cubeName = "test_kylin_cube_with_slr_empty";
        CubeManager cubeManager = CubeManager.getInstance(this.getTestConfig());
        CubeInstance cube = cubeManager.getCube(cubeName);

        // initial segment
        CubeSegment segment = cubeManager.allocateSegments(cube, CubeBuildTypeEnum.BUILD, 0, dateEnd).get(0);

        JobInstance jobInstance = jobManager.createJob(cubeName, segment.getName(), UUID.randomUUID().toString(), CubeBuildTypeEnum.BUILD);

        String actual = JsonUtil.writeValueAsIndentString(jobInstance);
        System.out.println(actual);

        assertEquals(13, jobInstance.getSteps().size());

        assertTrue(jobInstance.getSteps().get(3).getExecCmd().contains(JobEngineConfig.HADOOP_JOB_CONF_FILENAME + ".xml"));

        JobStep jobStep;
        // check each step
        jobStep = jobInstance.getSteps().get(0);
        assertEquals(JobStepCmdTypeEnum.SHELL_CMD_HADOOP, jobStep.getCmdType());
        assertEquals(false, jobStep.isRunAsync());

        jobStep = jobInstance.getSteps().get(1);
        assertEquals(JobStepCmdTypeEnum.JAVA_CMD_HADOOP_FACTDISTINCT, jobStep.getCmdType());
        assertEquals(true, jobStep.isRunAsync());

        jobStep = jobInstance.getSteps().get(2);
        assertEquals(JobStepCmdTypeEnum.JAVA_CMD_HADOOP_NO_MR_DICTIONARY, jobStep.getCmdType());
        assertEquals(false, jobStep.isRunAsync());

        jobStep = jobInstance.getSteps().get(3);
        assertEquals(JobStepCmdTypeEnum.JAVA_CMD_HADOOP_BASECUBOID, jobStep.getCmdType());
        assertEquals(true, jobStep.isRunAsync());

        jobStep = jobInstance.getSteps().get(4);
        assertEquals(JobStepCmdTypeEnum.JAVA_CMD_HADOOP_NDCUBOID, jobStep.getCmdType());
        assertEquals(true, jobStep.isRunAsync());

        jobStep = jobInstance.getSteps().get(5);
        assertEquals(JobStepCmdTypeEnum.JAVA_CMD_HADOOP_NDCUBOID, jobStep.getCmdType());
        assertEquals(true, jobStep.isRunAsync());

        jobStep = jobInstance.getSteps().get(6);
        assertEquals(JobStepCmdTypeEnum.JAVA_CMD_HADOOP_NDCUBOID, jobStep.getCmdType());
        assertEquals(true, jobStep.isRunAsync());

        jobStep = jobInstance.getSteps().get(7);
        assertEquals(JobStepCmdTypeEnum.JAVA_CMD_HADOOP_NDCUBOID, jobStep.getCmdType());
        assertEquals(true, jobStep.isRunAsync());

        jobStep = jobInstance.getSteps().get(8);
        assertEquals(JobStepCmdTypeEnum.JAVA_CMD_HADOOP_NDCUBOID, jobStep.getCmdType());
        assertEquals(true, jobStep.isRunAsync());

        jobStep = jobInstance.getSteps().get(9);
        assertEquals(JobStepCmdTypeEnum.JAVA_CMD_HADOOP_RANGEKEYDISTRIBUTION, jobStep.getCmdType());
        assertEquals(true, jobStep.isRunAsync());

        jobStep = jobInstance.getSteps().get(10);
        assertEquals(JobStepCmdTypeEnum.JAVA_CMD_HADDOP_NO_MR_CREATEHTABLE, jobStep.getCmdType());
        assertEquals(false, jobStep.isRunAsync());

        jobStep = jobInstance.getSteps().get(11);
        assertEquals(JobStepCmdTypeEnum.JAVA_CMD_HADOOP_CONVERTHFILE, jobStep.getCmdType());
        assertEquals(true, jobStep.isRunAsync());

        jobStep = jobInstance.getSteps().get(12);
        assertEquals(JobStepCmdTypeEnum.JAVA_CMD_HADOOP_NO_MR_BULKLOAD, jobStep.getCmdType());
        assertEquals(false, jobStep.isRunAsync());
    }

    @Test
    public void testCreateMergeSteps() throws Exception {

        JobManager jobManager = new JobManager("JobInstanceBuilderTest", new JobEngineConfig(KylinConfig.getInstanceFromEnv()));
        String cubeName = "test_kylin_cube_with_slr_ready_2_segments";
        CubeManager cubeManager = CubeManager.getInstance(this.getTestConfig());
        CubeInstance cube = cubeManager.getCube(cubeName);

        // initial segment
        CubeSegment segment = CubeManager.getInstance(this.getTestConfig()).allocateSegments(cube, CubeBuildTypeEnum.MERGE, 1384240200000L, 1386835200000L).get(0);

        JobInstance jobInstance = jobManager.createJob(cubeName, segment.getName(), UUID.randomUUID().toString(), CubeBuildTypeEnum.MERGE);

        String actual = JsonUtil.writeValueAsIndentString(jobInstance);
        System.out.println(actual);

        assertEquals(5, jobInstance.getSteps().size());

        JobStep jobStep;
        // check each step
        jobStep = jobInstance.getSteps().get(0);
        assertEquals(JobStepCmdTypeEnum.JAVA_CMD_HADOOP_MERGECUBOID, jobStep.getCmdType());
        assertEquals(true, jobStep.isRunAsync());

        jobStep = jobInstance.getSteps().get(1);
        assertEquals(JobStepCmdTypeEnum.JAVA_CMD_HADOOP_RANGEKEYDISTRIBUTION, jobStep.getCmdType());
        assertEquals(true, jobStep.isRunAsync());

        jobStep = jobInstance.getSteps().get(2);
        assertEquals(JobStepCmdTypeEnum.JAVA_CMD_HADDOP_NO_MR_CREATEHTABLE, jobStep.getCmdType());
        assertEquals(false, jobStep.isRunAsync());

        jobStep = jobInstance.getSteps().get(3);
        assertEquals(JobStepCmdTypeEnum.JAVA_CMD_HADOOP_CONVERTHFILE, jobStep.getCmdType());
        assertEquals(true, jobStep.isRunAsync());

        jobStep = jobInstance.getSteps().get(4);
        assertEquals(JobStepCmdTypeEnum.JAVA_CMD_HADOOP_NO_MR_BULKLOAD, jobStep.getCmdType());
        assertEquals(false, jobStep.isRunAsync());
    }
}

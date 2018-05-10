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

//package org.apache.kylin.storage.hbase.lookup;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.kylin.common.util.LocalFileMetadataTestCase;
//import org.apache.kylin.cube.CubeSegment;
//import org.apache.kylin.engine.mr.CubingJob;
//import org.apache.kylin.job.engine.JobEngineConfig;
//import org.junit.After;
//import org.junit.Before;
//import org.junit.Rule;
//import org.junit.Test;
//import org.powermock.api.mockito.PowerMockito;
//import org.powermock.core.classloader.annotations.PrepareForTest;
//import org.powermock.modules.junit4.rule.PowerMockRule;
//
//@PrepareForTest({ LookupTableToHFileJob.class, Job.class})
//public class LookupTableToHFileJobTest extends LocalFileMetadataTestCase {
//
//    @Rule
//    public PowerMockRule rule = new PowerMockRule();
//
//    @Before
//    public void setup() throws Exception {
//        createTestMetadata();
//    }
//
//    @After
//    public void after() throws Exception {
//        cleanupTestMetadata();
//    }
//
//    @Test
//    public void testRun() throws Exception {
//        String cubeName = "test_kylin_cube_with_slr_1_new_segment";
//        String segmentID = "198va32a-a33e-4b69-83dd-0bb8b1f8c53b";
//        CubeInstance cubeInstance = CubeManager.getInstance(getTestConfig()).getCube(cubeName);
//        CubeSegment cubeSegment = cubeInstance.getSegmentById(segmentID);
//
//        Configuration conf = HadoopUtil.getCurrentConfiguration();
//        conf.set("fs.defaultFS", "file:///");
//        conf.set("mapreduce.framework.name", "local");
//        conf.set("mapreduce.application.framework.path", "");
//        conf.set("fs.file.impl.disable.cache", "true");
//
//        FileSystem localFileSystem = new LocalFileSystem();
//        URI uri = URI.create("file:///");
//        localFileSystem.initialize(uri, conf);
//
//        Job mockedJob = createMockMRJob(conf);
//        PowerMockito.stub(PowerMockito.method(Job.class, "getInstance", Configuration.class, String.class))
//                .toReturn(mockedJob);
//        PowerMockito.stub(PowerMockito.method(Job.class, "getInstance", Configuration.class, String.class))
//                .toReturn(mockedJob);
//
//        StringBuilder cmd = new StringBuilder();
//        JobBuilderSupport.appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME,
//                "Build_Lookup_Table_For_Segment_20130331080000_20131212080000_Step");
//        JobBuilderSupport.appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME,
//                cubeName);
//        JobBuilderSupport.appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_ID,
//                segmentID);
//        JobBuilderSupport.appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, getOutputPath());
//        JobBuilderSupport.appendExecCmdParameters(cmd, BatchConstants.ARG_TABLE_NAME, "EDW.TEST_SITES");
//
//        LookupTableToHFileJob job = new LookupTableToHFileJob();
//        job.setConf(conf);
//        job.setAsync(true);
//
//        String[] args = cmd.toString().trim().split("\\s+");
//        job.run(args);
//    }
//
//    private String getOutputPath() {
//        return "_tmp_output";
//    }
//
//    private CubingJob createMockCubingJob(CubeSegment cubeSeg) {
//        JobEngineConfig jobEngineConfig = new JobEngineConfig(getTestConfig());
//        CubingJob cubingJob = CubingJob.createBuildJob(cubeSeg, "unitTest", jobEngineConfig);
//
//        return cubingJob;
//    }
//
//    private Job createMockMRJob(Configuration conf) throws Exception {
//        Job job = PowerMockito.mock(Job.class);
//        PowerMockito.when(job.getConfiguration()).thenReturn(conf);
//        PowerMockito.doNothing().when(job).submit();
//        return job;
//    }

//}

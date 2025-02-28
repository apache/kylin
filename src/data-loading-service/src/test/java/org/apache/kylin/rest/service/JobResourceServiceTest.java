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

package org.apache.kylin.rest.service;

import java.util.Map;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.dao.ExecutableOutputPO;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.dao.JobInfoDao;
import org.apache.kylin.job.domain.JobInfo;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.job.util.JobInfoUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.test.util.ReflectionTestUtils;

public class JobResourceServiceTest extends NLocalFileMetadataTestCase {

    @InjectMocks
    private JobResourceService jobResourceService = Mockito.spy(new JobResourceService());

    @Mock
    private JobInfoService jobInfoService;

    @Mock
    private JobInfoDao jobInfoDao;

    @Before
    public void setUp() {
        JobContextUtil.cleanUp();
        MockitoAnnotations.initMocks(this);
        ReflectionTestUtils.setField(jobResourceService, "jobInfoService", jobInfoService);
        createTestMetadata();
    }

    @After
    public void tearDown() {
        JobContextUtil.cleanUp();
        cleanupTestMetadata();
    }

    @Test
    public void testAdjustJobResource() throws Exception {
        String queue = "test_build_queue";
        ExecutablePO po = new ExecutablePO();

        ExecutablePO subTask = new ExecutablePO();
        ExecutableOutputPO subTaskOutput = new ExecutableOutputPO();
        subTaskOutput.setStatus(JobStatusEnum.RUNNING.name());
        subTask.setOutput(subTaskOutput);
        Map<String, String> info = Maps.newHashMap();
        info.put(ExecutableConstants.QUEUE_NAME, queue);

        info.put(ExecutableConstants.MEMORY, "1024");
        subTaskOutput.setInfo(info);

        po.setTasks(Lists.newArrayList(subTask));

        try (MockedStatic<JobContextUtil> theMock = Mockito.mockStatic(JobContextUtil.class)) {
            theMock.when(() -> JobContextUtil.getJobInfoDao(Mockito.any())).thenReturn(jobInfoDao);
            JobResourceService.JobResource resource = new JobResourceService.JobResource();
            resource.setQueue(queue);

            Assert.assertEquals(0, jobResourceService.adjustJobResource(resource).getCores());
            resource.setCores(1);
            resource.setMemory(2048);
            Assert.assertEquals(1, jobResourceService.adjustJobResource(resource).getCores());

            resource.setMemory(-2048);
            Assert.assertEquals(0, jobResourceService.adjustJobResource(resource).getMemory());

            Mockito.doReturn(Lists.newArrayList(po)).when(jobInfoDao).getExecutablePoByStatus(Mockito.any(),
                    Mockito.any(), Mockito.any());
            Assert.assertEquals(0, jobResourceService.adjustJobResource(resource).getMemory());
            info.put(ExecutableConstants.CORES, "2");
            Assert.assertEquals(-1024, jobResourceService.adjustJobResource(resource).getMemory());

            resource.setCores(-1);
            resource.setMemory(-1024);
            Assert.assertEquals(-1024, jobResourceService.adjustJobResource(resource).getMemory());
        }

    }

    @Test
    public void testGetQueueNames() {
        try (MockedStatic<JobContextUtil> theMock = Mockito.mockStatic(JobContextUtil.class)) {
            theMock.when(() -> JobContextUtil.getJobInfoDao(Mockito.any())).thenReturn(jobInfoDao);
            Assert.assertEquals(0, jobResourceService.getQueueNames().size());
            JobInfo jobInfoNotTasks = new JobInfo();
            jobInfoNotTasks.setUpdateTime(System.currentTimeMillis());
            jobInfoNotTasks.setJobContent(JobInfoUtil.serializeExecutablePO(new ExecutablePO()));

            Mockito.doReturn(Lists.newArrayList(jobInfoNotTasks)).when(jobInfoDao)
                    .getJobInfoListByFilter(Mockito.any());
            Assert.assertEquals(0, jobResourceService.getQueueNames().size());
            testGetQueueNamesMock();
            Assert.assertEquals(1, jobResourceService.getQueueNames().size());
        }
    }

    private void testGetQueueNamesMock() {
        JobInfo jobInfo = new JobInfo();
        ExecutablePO po = new ExecutablePO();
        ExecutablePO subTask = new ExecutablePO();
        ExecutableOutputPO subTaskOutput = new ExecutableOutputPO();
        subTaskOutput.setStatus(JobStatusEnum.FINISHED.name());
        subTask.setOutput(subTaskOutput);
        Map<String, String> info = Maps.newHashMap();
        info.put(ExecutableConstants.QUEUE_NAME, "test_build_queue");
        subTaskOutput.setInfo(info);
        po.setTasks(Lists.newArrayList(subTask));
        jobInfo.setUpdateTime(System.currentTimeMillis());
        jobInfo.setJobContent(JobInfoUtil.serializeExecutablePO(po));
        Mockito.doReturn(Lists.newArrayList(jobInfo)).when(jobInfoDao).getJobInfoListByFilter(Mockito.any());
    }

}

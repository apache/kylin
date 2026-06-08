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

package org.apache.kylin.job.dao;

import java.util.List;

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.job.domain.JobInfo;
import org.apache.kylin.job.mapper.JobInfoMapper;
import org.apache.kylin.job.rest.JobMapperFilter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

public class JobInfoDaoTest {

    private JobInfoMapper jobInfoMapper;
    private JobInfoDao jobInfoDao;

    @Before
    public void setUp() {
        jobInfoMapper = Mockito.mock(JobInfoMapper.class);
        jobInfoDao = new JobInfoDao();
        ReflectionTestUtils.setField(jobInfoDao, "jobInfoMapper", jobInfoMapper);
    }

    private JobInfo mockJobInfo(String jobId) {
        JobInfo jobInfo = new JobInfo();
        jobInfo.setJobId(jobId);
        return jobInfo;
    }

    @Test
    public void testGetJobInfoListByProjectFilterReturnsEmptyWhenNoProjectScope() {
        JobMapperFilter filter = JobMapperFilter.builder().build();

        List<JobInfo> result = jobInfoDao.getJobInfoListByProjectFilter(filter);

        Assert.assertTrue(result.isEmpty());
        // must short-circuit without hitting the mapper to avoid a cross-project scan
        Mockito.verify(jobInfoMapper, Mockito.never()).selectByJobFilter(ArgumentMatchers.any());
    }

    @Test
    public void testGetJobInfoListByProjectFilterReturnsEmptyWhenProjectIsBlank() {
        JobMapperFilter filter = JobMapperFilter.builder().project("   ").build();

        List<JobInfo> result = jobInfoDao.getJobInfoListByProjectFilter(filter);

        Assert.assertTrue(result.isEmpty());
        Mockito.verify(jobInfoMapper, Mockito.never()).selectByJobFilter(ArgumentMatchers.any());
    }

    @Test
    public void testGetJobInfoListByProjectFilterDelegatesWhenSingleProjectPresent() {
        JobMapperFilter filter = JobMapperFilter.builder().project("default").build();
        List<JobInfo> expected = Lists.newArrayList(mockJobInfo("job-1"));
        Mockito.when(jobInfoMapper.selectByJobFilter(filter)).thenReturn(expected);

        List<JobInfo> result = jobInfoDao.getJobInfoListByProjectFilter(filter);

        Assert.assertSame(expected, result);
        Mockito.verify(jobInfoMapper, Mockito.times(1)).selectByJobFilter(filter);
    }

    @Test
    public void testGetJobInfoListByProjectFilterDelegatesWhenProjectsPresent() {
        JobMapperFilter filter = JobMapperFilter.builder().projects(Lists.newArrayList("p1", "p2")).build();
        List<JobInfo> expected = Lists.newArrayList(mockJobInfo("job-1"), mockJobInfo("job-2"));
        Mockito.when(jobInfoMapper.selectByJobFilter(filter)).thenReturn(expected);

        List<JobInfo> result = jobInfoDao.getJobInfoListByProjectFilter(filter);

        Assert.assertEquals(2, result.size());
        Mockito.verify(jobInfoMapper, Mockito.times(1)).selectByJobFilter(filter);
    }

    @Test
    public void testGetJobInfoListByFilterAlwaysDelegates() {
        JobMapperFilter filter = JobMapperFilter.builder().build();
        List<JobInfo> expected = Lists.newArrayList(mockJobInfo("job-1"));
        Mockito.when(jobInfoMapper.selectByJobFilter(filter)).thenReturn(expected);

        List<JobInfo> result = jobInfoDao.getJobInfoListByFilter(filter);

        Assert.assertSame(expected, result);
        Mockito.verify(jobInfoMapper, Mockito.times(1)).selectByJobFilter(filter);
    }
}

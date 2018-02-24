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

import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.job.constant.JobTimeFilterEnum;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.exception.JobException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.job.execution.Output;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.query.QueryConnection;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

/**
 * @author xduo
 */
public class JobServiceTest extends ServiceTestBase {

    @Autowired
    @Qualifier("jobService")
    JobService jobService;

    @Autowired
    @Qualifier("cacheService")
    private CacheService cacheService;

    @Test
    public void testBasics() throws JobException, IOException, SQLException {
        Assert.assertNotNull(jobService.getConfig());
        Assert.assertNotNull(jobService.getConfig());
        Assert.assertNotNull(jobService.getDataModelManager());
        Assert.assertNotNull(QueryConnection.getConnection(ProjectInstance.DEFAULT_PROJECT_NAME));
        Assert.assertNull(jobService.getJobInstance("job_not_exist"));
        Assert.assertNotNull(jobService.searchJobs(null, null, null, 0, 0, JobTimeFilterEnum.ALL, JobService.JobSearchMode.ALL));
    }

    @Test
    public void testExceptionOnLostJobOutput() {
        ExecutableManager manager = ExecutableManager.getInstance(jobService.getConfig());
        AbstractExecutable executable = new TestJob();
        manager.addJob(executable);
        List<CubingJob> jobs = jobService.innerSearchCubingJobs("cube", "jobName",
                Collections.<ExecutableState> emptySet(), 0, Long.MAX_VALUE, Collections.<String, Output> emptyMap(),
                true, "project");
        Assert.assertEquals(0, jobs.size());
    }

    public static class TestJob extends CubingJob {

        public TestJob() {
            super();
        }

        @Override
        protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
            return new ExecuteResult(ExecuteResult.State.SUCCEED, "");
        }
    }
}

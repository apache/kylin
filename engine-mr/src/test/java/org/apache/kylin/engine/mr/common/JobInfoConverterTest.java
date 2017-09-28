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

package org.apache.kylin.engine.mr.common;

import java.util.Map;

import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.job.JobInstance;
import org.apache.kylin.job.constant.JobStepStatusEnum;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.job.execution.Output;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Maps;

public class JobInfoConverterTest {
    @Test
    public void testParseToJobInstance() {
        TestJob task = new TestJob();
        JobInstance instance = JobInfoConverter.parseToJobInstanceQuietly(task, Maps.<String, Output> newHashMap());
        // no exception thrown is expected
        Assert.assertTrue(instance == null);
    }

    @Test
    public void testParseToJobStep() {
        TestJob task = new TestJob();
        JobInstance.JobStep step = JobInfoConverter.parseToJobStep(task, 0, null);
        Assert.assertEquals(step.getStatus(), JobStepStatusEnum.PENDING);

        step = JobInfoConverter.parseToJobStep(task, 0, new TestOutput());
        Assert.assertEquals(step.getStatus(), JobStepStatusEnum.FINISHED);
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

    public static class TestOutput implements Output {

        @Override
        public Map<String, String> getExtra() {
            Map<String, String> extra = Maps.newHashMap();
            extra.put("testkey", "testval");
            return extra;
        }

        @Override
        public String getVerboseMsg() {
            return null;
        }

        @Override
        public ExecutableState getState() {
            return ExecutableState.SUCCEED;
        }

        @Override
        public long getLastModified() {
            return 0;
        }
    }
}

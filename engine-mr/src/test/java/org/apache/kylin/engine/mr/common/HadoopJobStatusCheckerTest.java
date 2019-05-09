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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.kylin.job.constant.JobStepStatusEnum;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Unit tests for class {@link HadoopJobStatusChecker}.
 *
 * @see HadoopJobStatusChecker
 */
public class HadoopJobStatusCheckerTest {

    @Test
    public void testCheckStatusWithNullJob() {
        StringBuilder stringBuilder = new StringBuilder();
        JobStepStatusEnum jobStepStatusEnum = HadoopJobStatusChecker.checkStatus(null, stringBuilder);

        assertEquals(JobStepStatusEnum.WAITING, jobStepStatusEnum);
        assertEquals("Skip status check with empty job id..\n", stringBuilder.toString());

        assertFalse(jobStepStatusEnum.isRunable());
        assertEquals(32, jobStepStatusEnum.getCode());

        assertFalse(jobStepStatusEnum.isComplete());
    }

    @Test
    public void testCheckStatusWithEmptyJobId() throws IOException {
        Job job = Job.getInstance(new Configuration(false));
        StringBuilder stringBuilder = new StringBuilder();
        JobStepStatusEnum jobStepStatusEnum = HadoopJobStatusChecker.checkStatus(job, stringBuilder);

        assertEquals(JobStepStatusEnum.WAITING, jobStepStatusEnum);
        assertEquals("Skip status check with empty job id..\n", stringBuilder.toString());

        assertFalse(jobStepStatusEnum.isRunable());
        assertEquals(32, jobStepStatusEnum.getCode());

        assertFalse(jobStepStatusEnum.isComplete());
    }

}
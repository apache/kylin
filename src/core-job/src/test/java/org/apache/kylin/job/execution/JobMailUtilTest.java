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

package org.apache.kylin.job.execution;

import java.util.HashMap;
import java.util.Map;

import org.apache.kylin.common.mail.MailNotificationType;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.mail.JobMailUtil;
import org.apache.kylin.job.util.JobContextUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import lombok.val;

public class JobMailUtilTest extends NLocalFileMetadataTestCase {
    private static final String DEFAULT_PROJECT = "default";
    private static final String MAIL_TITLE_JOB_ERROR = "[Kylin System Notification]-[Job Error]";
    private static final String MAIL_TITLE_JOB_FINISHED = "[Kylin System Notification]-[Job Finished]";
    private static final String MAIL_TITLE_JOB_DISCARDED = "[Kylin System Notification]-[Job Discarded]";
    private static final String MAIL_TITLE_JOB_LOAD_EMPTY_DATA = "[Kylin System Notification]-[Job Load Empty Data]";

    @Before
    public void setup() throws Exception {
        JobContextUtil.cleanUp();
        createTestMetadata();
        JobContextUtil.getJobInfoDao(getTestConfig());
    }

    @After
    public void after() throws Exception {
        JobContextUtil.cleanUp();
        cleanupTestMetadata();
    }

    @Test
    public void testCreateMail() {
        DefaultExecutableOnModel job = new DefaultExecutableOnModel();
        job.setName("test_job1");
        job.setSubmitter("test_submitter1");
        job.setProject(DEFAULT_PROJECT);
        job.setTargetSubject("model_test1");
        job.setId("jobId");

        val step = new SucceedTestExecutable();
        step.setName("step");
        job.addTask(step);

        // test job load empty data
        Pair<String, String> mail = JobMailUtil.createMail(MailNotificationType.JOB_LOAD_EMPTY_DATA, job);
        Assert.assertNotNull(mail.getSecond());
        Assert.assertEquals(MAIL_TITLE_JOB_LOAD_EMPTY_DATA, mail.getFirst());

        // test job finished
        mail = JobMailUtil.createMail(MailNotificationType.JOB_FINISHED, job);
        Assert.assertNotNull(mail.getSecond());
        Assert.assertEquals(MAIL_TITLE_JOB_FINISHED, mail.getFirst());

        // test job discarded
        mail = JobMailUtil.createMail(MailNotificationType.JOB_DISCARDED, job);
        Assert.assertNotNull(mail.getSecond());
        Assert.assertEquals(MAIL_TITLE_JOB_DISCARDED, mail.getFirst());

        // test create mail failed
        DefaultExecutableOnModel job2 = new DefaultExecutableOnModel();
        job2.setProject(DEFAULT_PROJECT);
        mail = JobMailUtil.createMail(MailNotificationType.JOB_FINISHED, job2);
        Assert.assertNull(mail);
    }

    @Test
    public void testCreateMailForJobError() {
        val manager = ExecutableManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        DefaultExecutableOnModel job = new DefaultExecutableOnModel();
        job.setName("test_job2");
        job.setSubmitter("test_submitter2");
        job.setProject(DEFAULT_PROJECT);
        job.setTargetSubject("model_test2");
        job.setJobType(JobTypeEnum.INDEX_BUILD);

        // test no tasks
        Pair<String, String> mail = JobMailUtil.createMail(MailNotificationType.JOB_ERROR, job);
        Assert.assertNull(mail);

        SucceedTestExecutable step2 = new SucceedTestExecutable();
        step2.setName("step2");
        job.addTask(step2);
        manager.addJob(job);

        // test task is not error
        mail = JobMailUtil.createMail(MailNotificationType.JOB_ERROR, job);
        Assert.assertNull(mail);

        manager.updateJobOutput(job.getJobId(), ExecutableState.PENDING);
        manager.updateJobOutput(step2.getId(), ExecutableState.PENDING);
        manager.updateJobOutput(step2.getId(), ExecutableState.ERROR);
        DefaultExecutableOnModel job2 = (DefaultExecutableOnModel) manager.getJob(job.getId());
        mail = JobMailUtil.createMail(MailNotificationType.JOB_ERROR, job2);
        Assert.assertNotNull(mail.getSecond());
        Assert.assertEquals(MAIL_TITLE_JOB_ERROR, mail.getFirst());
        Assert.assertTrue(mail.getSecond().contains("test_job"));

        Map<String, String> info = new HashMap<String, String>() {
            {
                put(ExecutableConstants.YARN_APP_ID, "application_111");
            }
        };
        manager.updateJobOutput(step2.getId(), ExecutableState.ERROR, info);
        DefaultExecutableOnModel job3 = (DefaultExecutableOnModel) manager.getJob(job.getId());
        mail = JobMailUtil.createMail(MailNotificationType.JOB_ERROR, job3);
        Assert.assertNotNull(mail.getSecond());
        Assert.assertEquals(MAIL_TITLE_JOB_ERROR, mail.getFirst());
        Assert.assertTrue(mail.getSecond().contains("step2"));
        Assert.assertTrue(mail.getSecond().contains("application_111"));
    }
}

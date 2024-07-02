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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.mail.MailNotificationType;
import org.apache.kylin.common.util.LogOutputTestCase;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.project.NProjectManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JobStatusChangedTest extends LogOutputTestCase {
    String project = "default";
    KylinConfig config;

    @Before
    public void setUp() throws Exception {
        JobContextUtil.cleanUp();
        createTestMetadata();
        getTestConfig().setMetadataUrl(
                "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1;MODE=MYSQL,username=sa,password=");
        config = KylinConfig.getInstanceFromEnv();
        NProjectManager prjMgr = NProjectManager.getInstance(config);
        prjMgr.createProject(project, "", "", Maps.newLinkedHashMap());
    }

    @Test
    public void testJobStatusChanged() {
        DefaultExecutableOnModel job = new DefaultExecutableOnModel();
        job.setProject(project);
        job.setTargetSubject("model_test");

        // test kylin.job.notification-enabled = false
        boolean notified = job.onStatusChange(MailNotificationType.JOB_ERROR);
        Assert.assertFalse(notified);

        overwriteSystemProp("kylin.job.notification-enabled", "true");

        // test job state needs to be notified, but it is not configured
        notified = job.onStatusChange(MailNotificationType.JOB_FINISHED);
        Assert.assertFalse(notified);

        overwriteSystemProp("kylin.job.notification-enable-states", "ERROR,DISCARDED,FINISHED");
        notified = job.onStatusChange(MailNotificationType.JOB_FINISHED);
        Assert.assertTrue(containsLog("user list is empty, not need to notify users."));
        Assert.assertFalse(notified);

        overwriteSystemProp("kylin.job.notification-admin-emails", "test@user");
        notified = job.onStatusChange(MailNotificationType.JOB_DISCARDED);
        Assert.assertTrue(containsLog("mail content is null, not need to notify users."));
        Assert.assertFalse(notified);

        // test exception
        job.setName("test_job1");
        job.setSubmitter("test_submitter1");
        notified = job.onStatusChange(MailNotificationType.JOB_DISCARDED);
        Assert.assertTrue(containsLog("notify user [Kylin System Notification]-[Job Discarded] failed!"));
        Assert.assertFalse(notified);
    }
}

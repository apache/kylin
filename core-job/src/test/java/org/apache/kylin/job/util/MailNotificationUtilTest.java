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

package org.apache.kylin.job.util;

import org.apache.kylin.job.execution.ExecutableState;
import org.junit.Assert;
import org.junit.Test;

import org.apache.kylin.shaded.com.google.common.collect.Maps;

public class MailNotificationUtilTest {

    @Test
    public void testGetMailTitle() {
        String[] titleParts = new String[] { "JOB", "SUCCEED" };
        Assert.assertEquals("[" + titleParts[0] + "]-[" + titleParts[1] + "]",
                MailNotificationUtil.getMailTitle(titleParts));
    }

    @Test
    public void testHasMailNotification() {
        Assert.assertTrue(MailNotificationUtil.hasMailNotification(ExecutableState.DISCARDED));
        Assert.assertTrue(MailNotificationUtil.hasMailNotification(ExecutableState.ERROR));
        Assert.assertTrue(MailNotificationUtil.hasMailNotification(ExecutableState.SUCCEED));
        Assert.assertFalse(MailNotificationUtil.hasMailNotification(ExecutableState.RUNNING));
        Assert.assertFalse(MailNotificationUtil.hasMailNotification(ExecutableState.STOPPED));
        Assert.assertFalse(MailNotificationUtil.hasMailNotification(ExecutableState.READY));
    }

    @Test
    public void testGetMailContent() {
        Assert.assertFalse(
                MailNotificationUtil.getMailContent(ExecutableState.DISCARDED, Maps.<String, Object>newHashMap())
                        .startsWith("Cannot find email template for"));
        Assert.assertFalse(MailNotificationUtil.getMailContent(ExecutableState.ERROR, Maps.<String, Object>newHashMap())
                .startsWith("Cannot find email template for"));
        Assert.assertFalse(
                MailNotificationUtil.getMailContent(ExecutableState.SUCCEED, Maps.<String, Object>newHashMap())
                        .startsWith("Cannot find email template for"));
        Assert.assertTrue(
                MailNotificationUtil.getMailContent(ExecutableState.RUNNING, Maps.<String, Object>newHashMap())
                        .startsWith("Cannot find email template for"));
        Assert.assertTrue(
                MailNotificationUtil.getMailContent(ExecutableState.STOPPED, Maps.<String, Object>newHashMap())
                        .startsWith("Cannot find email template for"));
        Assert.assertTrue(MailNotificationUtil.getMailContent(ExecutableState.READY, Maps.<String, Object>newHashMap())
                .startsWith("Cannot find email template for"));
    }
}

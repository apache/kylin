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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.kylin.shaded.com.google.common.collect.Maps;

public class MailNotificationUtilTest {

    @Test
    void testGetMailTitle() {
        String[] titleParts = new String[] { "JOB", "SUCCEED" };
        Assertions.assertEquals("[" + titleParts[0] + "]-[" + titleParts[1] + "]",
                MailNotificationUtil.getMailTitle(titleParts));
    }

    @Test
    void testHasMailNotification() {
        Assertions.assertTrue(MailNotificationUtil.hasMailNotification(ExecutableState.DISCARDED));
        Assertions.assertTrue(MailNotificationUtil.hasMailNotification(ExecutableState.ERROR));
        Assertions.assertTrue(MailNotificationUtil.hasMailNotification(ExecutableState.SUCCEED));
        Assertions.assertFalse(MailNotificationUtil.hasMailNotification(ExecutableState.RUNNING));
        Assertions.assertFalse(MailNotificationUtil.hasMailNotification(ExecutableState.STOPPED));
        Assertions.assertFalse(MailNotificationUtil.hasMailNotification(ExecutableState.READY));
    }

    @Test
    void testGetMailContent() {
        Assertions.assertFalse(
                MailNotificationUtil.getMailContent(ExecutableState.DISCARDED, Maps.<String, Object>newHashMap())
                        .startsWith("Cannot find email template for"));
        Assertions.assertFalse(MailNotificationUtil.getMailContent(ExecutableState.ERROR, Maps.<String, Object>newHashMap())
                .startsWith("Cannot find email template for"));
        Assertions.assertFalse(
                MailNotificationUtil.getMailContent(ExecutableState.SUCCEED, Maps.<String, Object>newHashMap())
                        .startsWith("Cannot find email template for"));
        Assertions.assertTrue(
                MailNotificationUtil.getMailContent(ExecutableState.RUNNING, Maps.<String, Object>newHashMap())
                        .startsWith("Cannot find email template for"));
        Assertions.assertTrue(
                MailNotificationUtil.getMailContent(ExecutableState.STOPPED, Maps.<String, Object>newHashMap())
                        .startsWith("Cannot find email template for"));
        Assertions.assertTrue(MailNotificationUtil.getMailContent(ExecutableState.READY, Maps.<String, Object>newHashMap())
                .startsWith("Cannot find email template for"));
    }
}

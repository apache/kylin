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

package org.apache.kylin.common.notify.util;

import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

public class MailNotificationUtilTest {

    @Test
    public void testGetMailTitle() {
        String[] titleParts = new String[] { "JOB", "SUCCEED" };
        Assert.assertEquals("[" + titleParts[0] + "]-[" + titleParts[1] + "]",
                MailNotificationUtil.getMailTitle(titleParts));
    }

    @Test
    public void testHasMailNotification() {
        Assert.assertTrue(MailNotificationUtil.hasMailNotification(Notify.DISCARDED));
        Assert.assertTrue(MailNotificationUtil.hasMailNotification(Notify.ERROR));
        Assert.assertTrue(MailNotificationUtil.hasMailNotification(Notify.SUCCEED));
    }

    @Test
    public void testGetMailContent() {
        Assert.assertFalse(
                MailNotificationUtil.getMailContent(Notify.DISCARDED, Maps.<String, Object>newHashMap())
                        .startsWith("Cannot find email template for"));
        Assert.assertFalse(MailNotificationUtil.getMailContent(Notify.ERROR, Maps.<String, Object>newHashMap())
                .startsWith("Cannot find email template for"));
        Assert.assertFalse(
                MailNotificationUtil.getMailContent(Notify.SUCCEED, Maps.<String, Object>newHashMap())
                        .startsWith("Cannot find email template for"));
    }
}

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

package org.apache.kylin.common.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.mail.EmailException;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.mail.MailService;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class MailServiceTest {

    @Test
    public void testSendEmail() {
        KylinConfig mockConfig = Mockito.mock(KylinConfig.class);
        Mockito.when(mockConfig.isStarttlsEnabled()).thenReturn(true);
        Mockito.when(mockConfig.getMailUsername()).thenReturn("test@user");
        Mockito.when(mockConfig.getMailPassword()).thenReturn("test_password");
        Mockito.when(mockConfig.getSmtpPort()).thenReturn("25");
        Mockito.when(mockConfig.getMailSender()).thenReturn("test@user");

        MailService mailservice = new MailService(mockConfig);
        List<String> receivers = new ArrayList<>(1);
        receivers.add("foobar@foobar.com");
        boolean sent = sendTestEmail(mailservice, receivers, true);
        Assert.assertFalse(sent);

        Mockito.when(mockConfig.isStarttlsEnabled()).thenReturn(false);
        Mockito.when(mockConfig.getMailUsername()).thenReturn("");
        Mockito.when(mockConfig.getMailPassword()).thenReturn("ENC(test_password)");
        Mockito.when(mockConfig.getMailSender()).thenReturn("test@sender");

        mailservice = new MailService(mockConfig);
        sent = sendTestEmail(mailservice, Lists.newArrayList(), false);
        Assert.assertFalse(sent);
    }

    private boolean sendTestEmail(MailService mailservice, List<String> receivers, boolean isHtmlMsg) {
        try {
            return mailservice.sendMail(receivers, "A test email from Kylin", "Hello!", isHtmlMsg);
        } catch (EmailException e) {
            return false;
        }
    }
}

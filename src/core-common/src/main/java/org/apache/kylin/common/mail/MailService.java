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

package org.apache.kylin.common.mail;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.commons.mail.Email;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.EncryptUtil;

/**
 * @author xduo
 */
public class MailService {
    private boolean starttlsEnabled;
    private String host;
    private String port;
    private String username;
    private String password;
    private String sender;

    public MailService(KylinConfig config) {
        this(config.isStarttlsEnabled(), config.getMailHost(), config.getSmtpPort(), config.getMailUsername(),
                config.getMailPassword(), config.getMailSender());
    }

    private MailService(boolean starttlsEnabled, String host, String port, String username, String password,
            String sender) {
        this.starttlsEnabled = starttlsEnabled;
        this.host = host;
        this.port = port;
        this.username = username;
        if (EncryptUtil.isEncrypted(password)) {
            password = EncryptUtil.decryptPassInKylin(password);
        }
        this.password = password;
        this.sender = sender;
    }

    /**
     * @param receivers
     * @param subject
     * @param content
     * @return true or false indicating whether the email was delivered successfully
     * @throws IOException
     */
    public boolean sendMail(List<String> receivers, String subject, String content) throws EmailException {
        return sendMail(receivers, subject, content, true);
    }

    /**
     * @param receivers
     * @param subject
     * @param content
     * @return true or false indicating whether the email was delivered successfully
     * @throws IOException
     */
    public boolean sendMail(List<String> receivers, String subject, String content, boolean isHtmlMsg)
            throws EmailException {
        Email email = new HtmlEmail();
        email.setHostName(host);
        email.setStartTLSEnabled(starttlsEnabled);
        if (starttlsEnabled) {
            if (!port.equals("25")) {
                email.setSSLOnConnect(true);
                email.setSSLCheckServerIdentity(true);
            }
            email.setSslSmtpPort(port);
        } else {
            email.setSmtpPort(Integer.parseInt(port));
        }

        if (username != null && username.trim().length() > 0) {
            email.setAuthentication(username, password);
        }

        for (String receiver : receivers) {
            email.addTo(receiver);
        }

        email.setFrom(sender);
        email.setSubject(subject);
        email.setCharset(StandardCharsets.UTF_8.toString());
        if (isHtmlMsg) {
            ((HtmlEmail) email).setHtmlMsg(content);
        } else {
            ((HtmlEmail) email).setTextMsg(content);
        }
        email.send();
        email.getMailSession();

        return true;
    }
}

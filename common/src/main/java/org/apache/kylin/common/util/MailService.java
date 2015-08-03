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

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.mail.Email;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;
import org.apache.kylin.common.KylinConfig;

/**
 * @author xduo
 */
public class MailService {

    private Boolean enabled = Boolean.TRUE;
    private String host;
    private String username;
    private String password;
    private String sender;

    private static final Log logger = LogFactory.getLog(MailService.class);

    public MailService() {
        this(KylinConfig.getInstanceFromEnv());
    }

    public MailService(KylinConfig config) {
        enabled = "true".equalsIgnoreCase(config.getProperty(KylinConfig.MAIL_ENABLED, "false"));
        host = config.getProperty(KylinConfig.MAIL_HOST, "");
        username = config.getProperty(KylinConfig.MAIL_USERNAME, "");
        password = config.getProperty(KylinConfig.MAIL_PASSWORD, "");
        sender = config.getProperty(KylinConfig.MAIL_SENDER, "");

        if (enabled) {
            if (host.isEmpty())
                throw new RuntimeException("mail service host is empty");
        }
    }

    /**
     * @param receivers
     * @param subject
     * @param content
     * @return true or false indicating whether the email was delivered successfully
     * @throws IOException
     */
    public boolean sendMail(List<String> receivers, String subject, String content) throws IOException {

        if (!enabled) {
            logger.info("Email service is disabled; this mail will not be delivered: " + subject);
            logger.info("To enable mail service, set 'mail.enabled=true' in kylin.properties");
            return false;
        }

        Email email = new HtmlEmail();
        email.setHostName(host);
        if (username != null && username.trim().length() > 0) {
            email.setAuthentication(username, password);
        }

        //email.setDebug(true);
        try {
            for (String receiver : receivers) {
                email.addTo(receiver);
            }

            email.setFrom(sender);
            email.setSubject(subject);
            email.setCharset("UTF-8");
            ((HtmlEmail) email).setHtmlMsg(content);
            email.send();
            email.getMailSession();

        } catch (EmailException e) {
            logger.error(e.getLocalizedMessage(), e);
            return false;
        }

        return true;
    }
}

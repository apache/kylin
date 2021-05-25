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

package org.apache.kylin.common.notify;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.mail.Email;
import org.apache.commons.mail.HtmlEmail;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.notify.util.MailNotificationUtil;
import org.apache.kylin.common.notify.util.NotificationConstants;
import org.apache.kylin.common.util.Pair;
import org.slf4j.LoggerFactory;

/**
 * @author xduo
 */
public class MailService extends NotifyServiceBase {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(MailService.class);

    private Boolean enabled = Boolean.TRUE;
    private Boolean starttlsEnabled = Boolean.FALSE;
    private String host;
    private String port;
    private String username;
    private String password;
    private String sender;

    public MailService(NotificationContext notificationContext) {
        super(notificationContext);
        KylinConfig kylinConfig = getNotificationContext().getConfig();
        this.enabled = kylinConfig.isNotificationEnabled();
        this.starttlsEnabled = kylinConfig.isStarttlsEnabled();
        this.host = kylinConfig.getMailHost();
        this.port = kylinConfig.getSmtpPort();
        this.username = kylinConfig.getMailUsername();
        this.password = kylinConfig.getMailPassword();
        this.sender = kylinConfig.getMailSender();
    }

    /**
     * @param receivers
     * @param subject
     * @param content
     * @return true or false indicating whether the email was delivered successfully
     * @throws IOException
     */
    private boolean sendMail(List<String> receivers, String subject, String content) {
        return sendMail(receivers, subject, content, getNotificationContext().isHtmlMsg());
    }

    /**
     * @param receivers
     * @param subject
     * @param content
     * @return true or false indicating whether the email was delivered successfully
     * @throws IOException
     */
    private boolean sendMail(List<String> receivers, String subject, String content, boolean isHtmlMsg) {

        if (!enabled) {
            logger.info("Email service is disabled; this mail will not be delivered: " + subject);
            logger.info("To enable mail service, set 'kylin.job.notification-enabled=true' in kylin.properties");
            return false;
        }

        if (host.isEmpty()) {
            logger.warn("mail service host is empty");
            return false;
        }

        Email email = new HtmlEmail();
        email.setHostName(host);
        email.setStartTLSEnabled(starttlsEnabled);
        email.setSSLOnConnect(starttlsEnabled);
        if (starttlsEnabled) {
            email.setSslSmtpPort(port);
        } else {
            email.setSmtpPort(Integer.parseInt(port));
        }
        
        if (username != null && !username.trim().isEmpty()) {
            email.setAuthentication(username, password);
        }

        for (String receiver : receivers) {
            try {
                email.addTo(receiver);
            } catch (Exception e) {
                logger.error("add " + receiver + " to send to mailbox list failed, " +
                        "this will not affect sending to the valid mailbox", e);
            }
        }

        // List of valid recipients is empty
        if (email.getToAddresses().isEmpty()) {
            logger.error("No valid send to mailbox, please check");
            return false;
        }

        // List of valid recipients is not empty
        try {
            email.setFrom(sender);
            email.setSubject(subject);
            email.setCharset("UTF-8");
            if (isHtmlMsg) {
                ((HtmlEmail) email).setHtmlMsg(content);
            } else {
                ((HtmlEmail) email).setTextMsg(content);
            }
            email.send();
            email.getMailSession();

            return true;
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return false;
        }
    }

    public boolean sendNotification() {
        List<String> receivers = getNotificationContext().getReceivers().get(NotificationConstants.NOTIFY_EMAIL_LIST);
        if (CollectionUtils.isEmpty(receivers)) {
            logger.warn("no need to send email, receivers is empty");
            return false;
        } else {
            logger.info("prepare to send email to:{}", receivers);
            if (getNotificationContext().isHtmlMsg()) {
                Pair<String[], Map<String, Object>> content = getNotificationContext().getContent();
                String contentEmail = MailNotificationUtil.getMailContent(getNotificationContext().getState(), content.getSecond());
                String title = MailNotificationUtil.getMailTitle(content.getFirst());
                return sendMail(receivers, title, contentEmail);
            } else {
                return sendMail(receivers, getNotificationContext().getSubject(), getNotificationContext().getInfo());
            }
        }
    }
}

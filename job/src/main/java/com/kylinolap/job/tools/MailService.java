/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.job.tools;

import org.apache.commons.mail.Email;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;

import java.io.IOException;
import java.util.List;

/**
 * @author xduo
 */
public class MailService {

    private String host = "atom.corp.ebay.com";
    private String username = "_kylin_ldap";
    private String password;
    private String sender = "DL-eBay-Kylin@corp.ebay.com";

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getSender() {
        return sender;
    }

    public void setSender(String sender) {
        this.sender = sender;
    }

    public void sendMail(List<String> receivers, String subject, String content) throws IOException {

        Email email = new HtmlEmail();
        email.setHostName(host);
        email.setDebug(true);
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

            System.out.println("!!");
        } catch (EmailException e) {
            e.printStackTrace();
        }
    }
}

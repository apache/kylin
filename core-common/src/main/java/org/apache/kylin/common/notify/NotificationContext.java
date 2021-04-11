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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * All basic information of the notification.
 */
public class NotificationContext {
    private KylinConfig config;

    /**
     * Notification recipient，include email and dingTalk
     */
    private Map<String, List<String>> receivers;

    /**
     * state of job
     */
    private String state;

    /**
     * content of notification, include title and detail
     */
    private Pair<String[], Map<String, Object>> content;

    /**
     * Send in HTML
     */
    private boolean isHtmlMsg = true;

    /**
     * when isHtmlMsg is false, subject and info will be sended as text
     */
    private String subject = "";

    private String info = "";

    public NotificationContext(KylinConfig config, Map<String, List<String>> receivers, String state, Pair<String[], Map<String, Object>> content) {
        this.config = config;
        this.receivers = receivers;
        this.state = state;
        this.content = content;
    }

    public NotificationContext(KylinConfig config, Map<String, List<String>> receivers, String subject, String info, boolean isHtmlMsg) {
        this.config = config;
        this.receivers = receivers;
        this.subject = subject;
        this.info = info;
        this.isHtmlMsg = isHtmlMsg;
    }

    public KylinConfig getConfig() {
        return config;
    }

    public void setConfig(KylinConfig config) {
        this.config = config;
    }

    public Map<String, List<String>> getReceivers() {
        return receivers;
    }

    public void setReceivers(Map<String, List<String>> receivers) {
        this.receivers = receivers;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public Pair<String[], Map<String, Object>> getContent() {
        return content;
    }

    public void setContent(Pair<String[], Map<String, Object>> content) {
        this.content = content;
    }

    public boolean isHtmlMsg() {
        return isHtmlMsg;
    }

    public void setHtmlMsg(boolean htmlMsg) {
        isHtmlMsg = htmlMsg;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getInfo() {
        return info;
    }

    public void setInfo(String info) {
        this.info = info;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NotificationContext that = (NotificationContext) o;
        return isHtmlMsg == that.isHtmlMsg &&
                Objects.equals(config, that.config) &&
                Objects.equals(receivers, that.receivers) &&
                Objects.equals(state, that.state) &&
                Objects.equals(content, that.content) &&
                Objects.equals(subject, that.subject) &&
                Objects.equals(info, that.info);
    }

    @Override
    public int hashCode() {
        return Objects.hash(config, receivers, state, content, isHtmlMsg, subject, info);
    }
}

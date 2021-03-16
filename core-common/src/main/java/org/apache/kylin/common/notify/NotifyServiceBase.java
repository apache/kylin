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
import java.util.concurrent.Callable;

public abstract class NotifyServiceBase implements Callable<Boolean> {
    
    private NotificationContext notificationContext;

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
     * only when send email and isHtmlMsg is false, subject and info will be used
     */
    private String subject;

    private String info;

    public abstract boolean sendNotification();

    public NotifyServiceBase(NotificationContext notificationContext) {
        this.notificationContext = notificationContext;
        this.config = notificationContext.getConfig();
        this.receivers = notificationContext.getReceivers();
        this.state = notificationContext.getState();
        this.content = notificationContext.getContent();
        this.subject = notificationContext.getSubject();
        this.info = notificationContext.getInfo();
    }

    @Override
    public Boolean call() throws Exception {
        return sendNotification();
    }

    public KylinConfig getConfig() {
        return config;
    }

    public Map<String, List<String>> getReceivers() {
        return receivers;
    }

    public String getState() {
        return state;
    }

    public Pair<String[], Map<String, Object>> getContent() {
        return content;
    }

    public NotificationContext getNotificationContext() {
        return notificationContext;
    }

    public String getSubject() {
        return subject;
    }

    public String getInfo() {
        return info;
    }
}

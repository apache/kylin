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

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.notify.util.NotificationConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Entity class for notification sending.
 */
public class NotificationTransmitter {
    private static final Logger logger = LoggerFactory.getLogger(NotificationTransmitter.class);

    private List<NotifyServiceBase> notifyServices = new ArrayList<>();

    private ExecutorService pool = Executors.newCachedThreadPool();

    private List<Future> futureList = new ArrayList<>();

    public NotificationTransmitter(NotificationContext notificationInfo) {
        if (!CollectionUtils.isEmpty(notificationInfo.getReceivers().get(NotificationConstants.NOTIFY_EMAIL_LIST))) {
            logger.info("email notification add to NotificationTransmitter");
            notifyServices.add(new MailService(notificationInfo));
        }

        if (!CollectionUtils.isEmpty(notificationInfo.getReceivers().get(NotificationConstants.NOTIFY_DINGTALK_LIST))) {
            logger.info("dingTalk notification add to NotificationTransmitter");
            notifyServices.add(new DingTalkService(notificationInfo));
        }
    }

    public boolean sendNotification() {
        boolean res = Boolean.TRUE;

        for (Callable callable : notifyServices) {
            Future f = pool.submit(callable);
            futureList.add(f);
        }

        for (Future f : futureList) {
            try {
                if (f.get(1, TimeUnit.MINUTES).toString().equalsIgnoreCase("false")) {
                    res = false;
                }
            } catch (Exception e) {
                res = false;
                logger.error("push notification for email or dingtalk occurred exception", e);
            }
        }

        pool.shutdown();

        return res;
    }

}

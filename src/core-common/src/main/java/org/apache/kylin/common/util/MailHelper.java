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

import java.math.BigDecimal;
import java.time.Clock;
import java.time.LocalDate;
import java.util.Collections;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class MailHelper {

    protected static final Logger logger = LoggerFactory.getLogger(MailHelper.class);

    public static List<String> getOverCapacityMailingUsers(KylinConfig kylinConfig) {
        final String[] overCapacityMailingUsers = kylinConfig.getOverCapacityMailingList();
        return getAllNotifyUserList(overCapacityMailingUsers);
    }

    public static List<String> getAllNotifyUserList(String[] notifyUsers) {
        List<String> users = Lists.newArrayList();
        if (null != notifyUsers) {
            Collections.addAll(users, notifyUsers);
        }
        return users;
    }

    public static Pair<String, String> formatNotifications(BasicEmailNotificationContent content) {
        if (content == null) {
            return null;
        }
        String title = content.getEmailTitle();
        String body = content.getEmailBody();
        return Pair.newPair(title, body);
    }

    public static boolean notifyUser(KylinConfig kylinConfig, BasicEmailNotificationContent content,
            List<String> users) {
        try {
            if (users.isEmpty()) {
                logger.debug("no need to send email, user list is empty.");
                return false;
            }
            final Pair<String, String> email = MailHelper.formatNotifications(content);
            return doSendMail(kylinConfig, users, email);
        } catch (Exception e) {
            logger.error("error send email", e);
            return false;
        }
    }

    public static boolean doSendMail(KylinConfig kylinConfig, List<String> users, Pair<String, String> email) {
        if (email == null) {
            logger.warn("no need to send email, content is null");
            return false;
        }
        logger.info("prepare to send email to:{}", users);
        logger.info("notify list:{}", users);
        return new MailService(kylinConfig).sendMail(users, email.getFirst(), email.getSecond());
    }

    public static BasicEmailNotificationContent creatContentForCapacityUsage(Long licenseVolume, Long currentCapacity) {
        BasicEmailNotificationContent content = new BasicEmailNotificationContent();
        content.setIssue("Over capacity threshold");
        content.setTime(LocalDate.now(Clock.systemDefaultZone()).toString());
        content.setJobType("CHECK_USAGE");
        content.setProject("NULL");

        String readableCurrentCapacity = SizeConvertUtil.getReadableFileSize(currentCapacity);
        String readableLicenseVolume = SizeConvertUtil.getReadableFileSize(licenseVolume);
        double overCapacityThreshold = KylinConfig.getInstanceFromEnv().getOverCapacityThreshold() * 100;
        content.setConclusion(BasicEmailNotificationContent.CONCLUSION_FOR_OVER_CAPACITY_THRESHOLD
                .replaceAll("\\$\\{volume_used\\}", readableCurrentCapacity)
                .replaceAll("\\$\\{volume_total\\}", readableLicenseVolume)
                .replaceAll("\\$\\{capacity_threshold\\}", BigDecimal.valueOf(overCapacityThreshold).toString()));
        content.setSolution(BasicEmailNotificationContent.SOLUTION_FOR_OVER_CAPACITY_THRESHOLD);
        return content;
    }

    public static boolean notifyUserForOverCapacity(Long licenseVolume, Long currentCapacity) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        List<String> users = getOverCapacityMailingUsers(kylinConfig);
        return notifyUser(kylinConfig, creatContentForCapacityUsage(licenseVolume, currentCapacity), users);
    }
}

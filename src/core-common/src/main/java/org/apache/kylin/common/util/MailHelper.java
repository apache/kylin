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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.base.Joiner;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MailHelper {

    public static final String OVER_CAPACITY_THRESHOLD = "OVER_CAPACITY_THRESHOLD";
    public static final String CAPACITY = "CAPACITY";
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

    public static boolean notifyUser(KylinConfig kylinConfig, Pair<String, String> mail, List<String> users) {
        try {
            if (users.isEmpty()) {
                logger.debug("no need to send email, user list is empty.");
                return false;
            }
            return doSendMail(kylinConfig, users, mail);
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

    public static Pair<String, String> creatContentForCapacityUsage(Long licenseVolume, Long currentCapacity, String resourceName) {

        String readableCurrentCapacity = SizeConvertUtil.getReadableFileSize(currentCapacity);
        String readableLicenseVolume = SizeConvertUtil.getReadableFileSize(licenseVolume);
        double overCapacityThreshold = KylinConfig.getInstanceFromEnv().getOverCapacityThreshold() * 100;
        KylinConfig env = KylinConfig.getInstanceFromEnv();

        Map<String, Object> dataMap = Maps.newHashMap();
        dataMap.put("resource_name", resourceName);
        dataMap.put("volume_used", readableCurrentCapacity);
        dataMap.put("volume_total", readableLicenseVolume);
        dataMap.put("capacity_threshold", BigDecimal.valueOf(overCapacityThreshold).toString());
        dataMap.put("env_name", KylinConfig.getInstanceFromEnv().getDeployEnv());
        String title = getMailTitle(CAPACITY,
                OVER_CAPACITY_THRESHOLD,
                env.getMetadataUrlPrefix(),
                env.getDeployEnv());
        String content = MailTemplateProvider.getInstance().buildMailContent(OVER_CAPACITY_THRESHOLD, dataMap);
        return Pair.newPair(title, content);
    }


    public static String getMailTitle(String... titleParts) {
        return "[" + Joiner.on("]-[").join(titleParts) + "]";
    }
}

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

package org.apache.kylin.metadata.sourceusage.mail;

import java.math.BigDecimal;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.mail.MailNotificationCreator;
import org.apache.kylin.common.mail.MailNotificationType;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.SizeConvertUtil;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import freemarker.template.Template;

public class SourceUsageMailUtil {

    private static final Logger logger = LoggerFactory.getLogger(SourceUsageMailUtil.class);

    private SourceUsageMailUtil() {
        throw new IllegalStateException("Utility class");
    }

    private static Map<String, Object> createMailContent(Long licenseVolume, Long currentCapacity) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        String readableCurrentCapacity = SizeConvertUtil.getReadableFileSize(currentCapacity);
        String readableLicenseVolume = SizeConvertUtil.getReadableFileSize(licenseVolume);
        double overCapacityThreshold = kylinConfig.getOverCapacityThreshold() * 100;

        Map<String, Object> data = Maps.newHashMap();
        data.put("volume_used", readableCurrentCapacity);
        data.put("volume_total", readableLicenseVolume);
        data.put("capacity_threshold", BigDecimal.valueOf(overCapacityThreshold).toString());

        return data;
    }

    public static Pair<String, String> createMail(MailNotificationType notificationType, Long licenseVolume,
            Long currentCapacity) {
        String mailTitle = MailNotificationCreator.createTitle(notificationType);
        Map<String, Object> data = createMailContent(licenseVolume, currentCapacity);

        String mailContent = null;
        try {
            Template mailTemplate = MailNotificationCreator.MailTemplate
                    .getTemplate(notificationType.getCorrespondingTemplateName());
            mailContent = MailNotificationCreator.createContent(mailTemplate, data);
        } catch (Exception e) {
            logger.error("create mail [{}] failed!", notificationType.getDisplayName(), e);
        }

        return Pair.newPair(mailTitle, mailContent);
    }
}

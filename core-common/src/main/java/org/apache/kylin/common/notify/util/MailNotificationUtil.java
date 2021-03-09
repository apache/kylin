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

package org.apache.kylin.common.notify.util;

import org.apache.kylin.shaded.com.google.common.base.Joiner;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Map;

public class MailNotificationUtil {

    private static String localHostName;

    private static String[] emailTemps;

    static {
        try {
            localHostName = InetAddress.getLocalHost().getCanonicalHostName();
            emailTemps = new String[]{
                    NotificationConstant.ERROR,
                    NotificationConstant.DISCARDED,
                    NotificationConstant.SUCCEED,
                    NotificationConstant.MIGRATION_REQUEST,
                    NotificationConstant.MIGRATION_REJECTED,
                    NotificationConstant.MIGRATION_APPROVED,
                    NotificationConstant.MIGRATION_COMPLETED,
                    NotificationConstant.MIGRATION_FAILED,
                    NotificationConstant.METADATA_PERSIST_FAIL
            };
        } catch (UnknownHostException e) {
            localHostName = "UNKNOWN";
        }
    }

    private static String getMailTemplateKey(String state) {
        switch (state) {
            case NotificationConstant.ERROR:
                return NotificationConstant.JOB_ERROR;
            case NotificationConstant.DISCARDED:
                return NotificationConstant.JOB_DISCARD;
            case NotificationConstant.SUCCEED:
                return NotificationConstant.JOB_SUCCEED;
            case NotificationConstant.MIGRATION_REQUEST:
                return NotificationConstant.MIGRATION_REQUEST;
            case NotificationConstant.MIGRATION_REJECTED:
                return NotificationConstant.MIGRATION_REJECTED;
            case NotificationConstant.MIGRATION_APPROVED:
                return NotificationConstant.MIGRATION_APPROVED;
            case NotificationConstant.MIGRATION_COMPLETED:
                return NotificationConstant.MIGRATION_COMPLETED;
            case NotificationConstant.MIGRATION_FAILED:
                return NotificationConstant.MIGRATION_FAILED;
            case NotificationConstant.METADATA_PERSIST_FAIL:
                return NotificationConstant.METADATA_PERSIST_FAIL;
            default:
                return null;
        }
    }


    private MailNotificationUtil() {
        throw new IllegalStateException("Class MailNotificationUtil is an utility class !");
    }

    public static String getLocalHostName() {
        return localHostName;
    }

    public static String getMailContent(String key, Map<String, Object> dataMap) {
        return MailTemplateProvider.getInstance().buildMailContent(getMailTemplateKey(key), dataMap);
    }

    public static String getMailTitle(String... titleParts) {
        return "[" + Joiner.on("]-[").join(titleParts) + "]";
    }

    public static boolean hasMailNotification(String state) {
        return Arrays.asList(emailTemps).contains(state);
    }
}

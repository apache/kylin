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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Map;

import org.apache.kylin.shaded.com.google.common.base.Joiner;

public class MailNotificationUtil {

    private static String localHostName;

    private static String[] emailTemps;

    static {
        try {
            localHostName = InetAddress.getLocalHost().getCanonicalHostName();
            emailTemps = new String[]{
                    NotificationConstants.JOB_ERROR,
                    NotificationConstants.JOB_DISCARDED,
                    NotificationConstants.JOB_SUCCEED,
                    NotificationConstants.JOB_MIGRATION_REQUEST,
                    NotificationConstants.JOB_MIGRATION_REJECTED,
                    NotificationConstants.JOB_MIGRATION_APPROVED,
                    NotificationConstants.JOB_MIGRATION_COMPLETED,
                    NotificationConstants.JOB_MIGRATION_FAILED,
                    NotificationConstants.JOB_METADATA_PERSIST_FAIL
            };
        } catch (UnknownHostException e) {
            localHostName = "UNKNOWN";
        }
    }

    private static String getMailTemplateKey(String state) {
        switch (state) {
            case NotificationConstants.JOB_ERROR:
                return NotificationConstants.JOB_ERROR_NOTIFICATION_TEMP;
            case NotificationConstants.JOB_DISCARDED:
                return NotificationConstants.JOB_DISCARD_NOTIFICATION_TEMP;
            case NotificationConstants.JOB_SUCCEED:
                return NotificationConstants.JOB_SUCCEED_NOTIFICATION_TEMP;
            case NotificationConstants.JOB_MIGRATION_REQUEST:
                return NotificationConstants.JOB_MIGRATION_REQUEST_TEMP;
            case NotificationConstants.JOB_MIGRATION_REJECTED:
                return NotificationConstants.JOB_MIGRATION_REJECTED_TEMP;
            case NotificationConstants.JOB_MIGRATION_APPROVED:
                return NotificationConstants.JOB_MIGRATION_APPROVED_TEMP;
            case NotificationConstants.JOB_MIGRATION_COMPLETED:
                return NotificationConstants.JOB_MIGRATION_COMPLETED_TEMP;
            case NotificationConstants.JOB_MIGRATION_FAILED:
                return NotificationConstants.JOB_MIGRATION_FAILED_TEMP;
            case NotificationConstants.JOB_METADATA_PERSIST_FAIL:
                return NotificationConstants.JOB_METADATA_PERSIST_FAIL_TEMP;
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

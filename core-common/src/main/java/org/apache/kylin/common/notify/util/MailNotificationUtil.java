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
                    Notify.ERROR,
                    Notify.DISCARDED,
                    Notify.SUCCEED,
                    Notify.MIGRATION_REQUEST,
                    Notify.MIGRATION_REJECTED,
                    Notify.MIGRATION_APPROVED,
                    Notify.MIGRATION_COMPLETED,
                    Notify.MIGRATION_FAILED,
                    Notify.METADATA_PERSIST_FAIL
            };
        } catch (UnknownHostException e) {
            localHostName = "UNKNOWN";
        }
    }

    private static String getMailTemplateKey(String state) {
        switch (state) {
            case Notify.ERROR:
                return Notify.JOB_ERROR;
            case Notify.DISCARDED:
                return Notify.JOB_DISCARD;
            case Notify.SUCCEED:
                return Notify.JOB_SUCCEED;
            case Notify.MIGRATION_REQUEST:
                return Notify.MIGRATION_REQUEST;
            case Notify.MIGRATION_REJECTED:
                return Notify.MIGRATION_REJECTED;
            case Notify.MIGRATION_APPROVED:
                return Notify.MIGRATION_APPROVED;
            case Notify.MIGRATION_COMPLETED:
                return Notify.MIGRATION_COMPLETED;
            case Notify.MIGRATION_FAILED:
                return Notify.MIGRATION_FAILED;
            case Notify.METADATA_PERSIST_FAIL:
                return Notify.METADATA_PERSIST_FAIL;
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

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
import org.apache.kylin.shaded.com.google.common.base.Strings;

import java.util.Map;

public class DingTalkNotificationUtil {

    public static String TITLEFORMAT = "<font color=%s size=3>%s</font>  \n";

    public static String CONTENTFORMAT = " **%s :** %s  \n";
    
    public static String getContent(String key, String title, Map<String, Object> content) {
        return buildContent(key, title, content);
    }

    public static String getTitle(String... titleParts) {
        return "[KYLIN]-[" + Joiner.on("]-[").join(titleParts) + "]";
    }

    private static String buildContent(String state, String title, Map<String, Object> content) {
       return buildDingTalkContent(state, title, content);
    }

    private static String buildDingTalkContent(String state, String title, Map<String, Object> data) {
        StringBuilder sb = new StringBuilder(Strings.lenientFormat(TITLEFORMAT, titleColor(state), title));
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            sb.append(Strings.lenientFormat(CONTENTFORMAT, entry.getKey(), entry.getValue()));
        }

        return sb.toString();
    }

    private static String titleColor(String state) {
        switch (state) {
            case NotificationConstants.JOB_SUCCEED:
            case NotificationConstants.JOB_MIGRATION_COMPLETED:
                return "#5cb85c";
            case NotificationConstants.JOB_DISCARDED:
                return "#607D8B";
            case NotificationConstants.JOB_ERROR:
            case NotificationConstants.JOB_METADATA_PERSIST_FAIL:
            case NotificationConstants.JOB_MIGRATION_FAILED:
            case NotificationConstants.JOB_MIGRATION_REJECTED:
                return "#d9534f";
            case NotificationConstants.JOB_MIGRATION_APPROVED:
            case NotificationConstants.JOB_MIGRATION_REQUEST:
                return "#337ab7";
            default:
                return "#000000";
        }
    }

}
